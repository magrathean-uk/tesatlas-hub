// SPDX-License-Identifier: AGPL-3.0-only

//! The egress gate: one chokepoint every Tesla send asks for a permit.
//!
//! The gate is closed by default and opened only by the window controller with
//! a verified grant. It also checks the clock itself, so it stops issuing
//! permits at the deadline even when no supervisor tick has run. Once it sees
//! either deadline reached it latches closed and cancels every cancellable
//! permit, so a later clock rollback cannot reopen it; only the controller
//! opens it again, for a new window. A refused request is never retried: each
//! attempt needs a new permit.
//!
//! A window whose grant carries a drive grace is armed with it. At the signed
//! end the gate refuses and cancels as at any deadline but does not latch; the
//! controller takes one fresh reading and either enters the grace, which moves
//! the deadline to the signed grace end once, or closes the window. A held
//! permit never moves: one granted before the signed end ends there.
//!
//! Local writes made under a window's authority (an owner sign-in's stored
//! pair, the cars its discovery configures) go through the write fence
//! ([`write_under_authority`]): the authority is checked and the write counted
//! under one lock, so a close that has cancelled the permits sees every write
//! that passed its check still running, and every later write refuses. The
//! supervisor waits for those writes only up to its handback deadline.

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    EXCHANGE_GRACE_SECONDS,
    clock::{SystemClock, WindowClock},
    state::GraceJournal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Legacy,
    Fleet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitKind {
    /// One request.
    Request,
    /// A stream holds its permit for its life and is cancelled at close.
    Stream,
    /// A token exchange: it may finish within the grace after the deadline.
    TokenExchange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum GateRefusal {
    #[error("window_closed")]
    WindowClosed,
}

impl GateRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::WindowClosed => "window_closed",
        }
    }
}

#[derive(Debug)]
struct OpenWindow {
    deadline_wall: i64,
    deadline_mono: u64,
    /// The window's drive grace, when its grant carries one.
    grace: Option<GraceArm>,
}

impl OpenWindow {
    fn reached(&self, wall: i64, mono: u64) -> bool {
        wall >= self.deadline_wall || mono >= self.deadline_mono
    }
}

/// A grant's drive grace as the controller arms the gate with it: the hard
/// grace end on both clocks, and where its journal lives.
#[derive(Debug, Clone)]
pub(super) struct GraceArm {
    pub(super) end_wall: i64,
    pub(super) end_mono: u64,
    pub(super) journal: PathBuf,
}

impl GraceArm {
    fn before_end(&self, wall: i64, mono: u64) -> bool {
        wall < self.end_wall && mono < self.end_mono
    }

    /// Journals the end of `grace` at `wall` before the gate acts on it. A
    /// journal that cannot record the end is removed, so a restart finds no
    /// grace to resume.
    fn record_end(&self, grace: &GraceJournal, wall: i64) {
        let ended = GraceJournal {
            ended_at: Some(wall),
            ..grace.clone()
        };
        if ended.write(&self.journal).is_err() {
            let _ = std::fs::remove_file(&self.journal);
        }
    }
}

#[derive(Debug, Default)]
struct GateState {
    open: Option<OpenWindow>,
    cancellable: Vec<CancellationToken>,
    /// The tokens of token-exchange permits: a close leaves them running for
    /// the grace, and only [`EgressGate::lapse_exchanges`] cancels them.
    exchanges: Vec<CancellationToken>,
    /// The wall time at which the gate saw a deadline reached; cleared only
    /// by `open`.
    expiry_observed: Option<i64>,
    /// When the gate saw the signed end of a window armed with a drive
    /// grace: it refuses every permit until the controller enters the grace
    /// or closes the window. Cleared only by `open` and the grace's entry.
    signed_end_seen: Option<i64>,
    /// The journal of the drive grace the open window entered; cleared only
    /// by `open`.
    grace: Option<GraceJournal>,
}

impl GateState {
    /// Stops issuing permits and cancels every request and stream permit,
    /// then journals the end of an open window's drive grace at `wall`, still
    /// under the gate's lock: no permit is issued in between, and no held
    /// one runs on while the journal is written.
    fn shut(&mut self, wall: i64) {
        let window = self.open.take();
        self.cancel_requests();
        if let (Some(window), Some(grace)) = (window, self.grace.as_ref())
            && let Some(arm) = window.grace.as_ref()
        {
            arm.record_end(grace, wall);
        }
    }

    /// Latches the gate closed at the deadline it saw at `wall`.
    fn latch(&mut self, wall: i64) {
        self.expiry_observed.get_or_insert(wall);
        self.shut(wall);
    }

    fn cancel_requests(&mut self) {
        for token in self.cancellable.drain(..) {
            token.cancel();
        }
    }

    /// Whether the open window authorises a send at (`wall`, `mono`), one
    /// reading the caller took. At its deadline the gate latches closed: the
    /// expiry is recorded and every request and stream permit cancelled, so
    /// a later clock rollback cannot reopen it. At the signed end of a window
    /// armed with a drive grace, still before the grace end, the gate only
    /// records that end once, refuses and cancels (every held request and
    /// stream permit ends at the signed end) and waits for the controller,
    /// which takes one fresh reading of the drive. A reading at or past the
    /// grace end latches the gate then too, whatever a later reading says.
    fn admit(&mut self, wall: i64, mono: u64) -> bool {
        let Some(window) = self.open.as_ref() else {
            return false;
        };
        if self.grace.is_none()
            && let Some(arm) = window.grace.as_ref()
        {
            if !arm.before_end(wall, mono) {
                self.latch(wall);
                return false;
            }
            if self.signed_end_seen.is_some() {
                return false;
            }
            if window.reached(wall, mono) {
                self.signed_end_seen = Some(wall);
                self.cancel_requests();
                return false;
            }
            return true;
        }
        if !window.reached(wall, mono) {
            return true;
        }
        self.latch(wall);
        false
    }

    /// Whether an armed window may enter its grace at (`wall`, `mono`): open
    /// and before the grace end. A reading at or past the grace end latches.
    fn admit_grace_entry(&mut self, wall: i64, mono: u64) -> bool {
        let Some(arm) = self.open.as_ref().and_then(|window| window.grace.as_ref()) else {
            return false;
        };
        if arm.before_end(wall, mono) {
            return true;
        }
        self.latch(wall);
        false
    }
}

#[derive(Debug)]
struct GateInner {
    clock: Arc<dyn WindowClock>,
    state: Mutex<GateState>,
    exchanges: Arc<AtomicUsize>,
    /// Every permit not yet dropped, of any kind.
    held: Arc<AtomicUsize>,
    /// The grant sequence of the window last opened, plus one; zero before any
    /// (M04-T11: a collector run's intake latencies are that window's).
    window_seq: AtomicU64,
}

impl GateInner {
    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().expect("gate lock")
    }

    /// Applies one reading of the gate's clock, taken by the caller: latches
    /// the gate if it shows a deadline passed. The reading is used as taken,
    /// never read again.
    fn observe(&self, wall: i64, mono: u64) {
        let _ = self.lock().admit(wall, mono);
    }
}

/// A cloneable handle; clones share one gate.
#[derive(Debug, Clone)]
pub struct EgressGate {
    inner: Arc<GateInner>,
}

/// Counts a held permit while it lives.
#[derive(Debug)]
struct ExchangeGuard(Arc<AtomicUsize>);

impl ExchangeGuard {
    fn count(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for ExchangeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Permission for one send. Request timeouts must be cut to `time_left`.
#[derive(Debug)]
pub struct Permit {
    provider: Provider,
    kind: PermitKind,
    time_left: Duration,
    cancel: CancellationToken,
    /// The gate of a request or stream permit granted in a drive grace: an
    /// end the permit sees on its own deadlines is reported to it.
    in_grace: Option<Arc<GateInner>>,
    _exchange: Option<ExchangeGuard>,
    _held: ExchangeGuard,
}

impl Permit {
    pub fn provider(&self) -> Provider {
        self.provider
    }

    pub fn kind(&self) -> PermitKind {
        self.kind
    }

    pub fn time_left(&self) -> Duration {
        self.time_left
    }

    /// Cancelled when the window closes (never for a token exchange).
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The holder found one of the permit's own deadlines passed at the
    /// reading (`wall`, `mono`) of the gate's clock. A permit granted in a
    /// drive grace hands that reading to the gate, so an end it saw (a
    /// forward wall step past the grace end) is journalled at that reading
    /// before the holder stops, even if the clock is corrected before the
    /// gate hears of it. The permit's deadline never moves; nothing else
    /// happens for any other permit.
    pub fn deadline_seen(&self, wall: i64, mono: u64) {
        if let Some(gate) = &self.in_grace {
            gate.observe(wall, mono);
        }
    }
}

impl EgressGate {
    pub fn new(clock: Arc<dyn WindowClock>) -> Self {
        Self {
            inner: Arc::new(GateInner {
                clock,
                state: Mutex::new(GateState::default()),
                exchanges: Arc::new(AtomicUsize::new(0)),
                held: Arc::new(AtomicUsize::new(0)),
                window_seq: AtomicU64::new(0),
            }),
        }
    }

    /// The process-wide gate on the system clock, closed until a window opens.
    pub fn process_wide() -> &'static EgressGate {
        static GATE: OnceLock<EgressGate> = OnceLock::new();
        GATE.get_or_init(|| EgressGate::new(Arc::new(SystemClock::new())))
    }

    /// Asks for permission to send. Called immediately before every send.
    /// Both providers ride the same window: a Fleet send, like a Legacy one,
    /// needs an open window and stops at its deadline (M09-T08).
    pub fn permit(&self, provider: Provider, kind: PermitKind) -> Result<Permit, GateRefusal> {
        let mut state = self.inner.lock();
        let wall = self.inner.clock.wall();
        let mono = self.inner.clock.mono();
        if !state.admit(wall, mono) {
            return Err(GateRefusal::WindowClosed);
        }
        let (deadline_wall, deadline_mono) = state
            .open
            .as_ref()
            .map(|window| (window.deadline_wall, window.deadline_mono))
            .ok_or(GateRefusal::WindowClosed)?;
        let left_wall = (deadline_wall - wall) as u64;
        let left_mono = deadline_mono - mono;
        let mut left = left_wall.min(left_mono);
        let cancel = CancellationToken::new();
        let (exchange, in_grace) = if kind == PermitKind::TokenExchange {
            left += EXCHANGE_GRACE_SECONDS as u64;
            state.exchanges.retain(|token| !token.is_cancelled());
            state.exchanges.push(cancel.clone());
            (Some(ExchangeGuard::count(&self.inner.exchanges)), None)
        } else {
            state.cancellable.retain(|token| !token.is_cancelled());
            state.cancellable.push(cancel.clone());
            (None, state.grace.is_some().then(|| self.inner.clone()))
        };
        Ok(Permit {
            provider,
            kind,
            time_left: Duration::from_secs(left),
            cancel,
            in_grace,
            _exchange: exchange,
            _held: ExchangeGuard::count(&self.inner.held),
        })
    }

    /// Ends the grace of every token exchange still holding its permit: each
    /// is cancelled, so one still sending is cut and every later write under
    /// it refuses (each checks its permit through the write fence). A write
    /// already past its check finishes; the supervisor waits for it only up
    /// to its handback deadline (design 2.7, M02-T13 round 2).
    pub fn lapse_exchanges(&self) {
        for token in self.inner.lock().exchanges.drain(..) {
            token.cancel();
        }
    }

    /// Whether a token exchange permitted before the deadline still holds its
    /// permit: sending, or storing its answer.
    pub fn exchange_in_flight(&self) -> bool {
        self.inner.exchanges.load(Ordering::SeqCst) > 0
    }

    /// Whether any permit of this gate, of any kind, is still held (a holder
    /// may still be inside a write it began under that permit).
    pub fn permits_held(&self) -> bool {
        self.inner.held.load(Ordering::SeqCst) > 0
    }

    pub fn is_open(&self) -> bool {
        self.inner.lock().open.is_some()
    }

    /// When (wall time) the gate saw a deadline reached since it was last
    /// opened, if it has.
    pub fn expiry_observed(&self) -> Option<i64> {
        self.inner.lock().expiry_observed
    }

    /// The grant sequence of the window the gate was last opened for, read without
    /// the gate's lock. A permit granted between two equal reads belongs to it:
    /// sequences only grow.
    pub fn window_seq(&self) -> Option<u64> {
        self.inner.window_seq.load(Ordering::SeqCst).checked_sub(1)
    }

    pub(super) fn open(&self, seq: u64, deadline_wall: i64, deadline_mono: u64) {
        let mut state = self.inner.lock();
        self.inner
            .window_seq
            .store(seq.saturating_add(1), Ordering::SeqCst);
        state.open = Some(OpenWindow {
            deadline_wall,
            deadline_mono,
            grace: None,
        });
        state.expiry_observed = None;
        state.signed_end_seen = None;
        state.grace = None;
    }

    /// Arms the open window with its grant's drive grace: at the signed end
    /// the gate waits for the controller instead of latching. A no-op on a
    /// shut gate.
    pub(super) fn arm_drive_grace(&self, arm: GraceArm) {
        if let Some(window) = self.inner.lock().open.as_mut() {
            window.grace = Some(arm);
        }
    }

    /// When the gate saw the signed end of a window armed with a drive grace,
    /// while it waits for the controller.
    pub(super) fn signed_end_seen(&self) -> Option<i64> {
        self.inner.lock().signed_end_seen
    }

    /// Enters the drive grace the controller decided on, already journalled:
    /// the window's deadline becomes the grace end, once. False when the gate
    /// has shut or latched, the window is not armed or is already in its
    /// grace, or either clock has reached the grace end (the gate then
    /// latches).
    pub(super) fn enter_drive_grace(&self, journal: GraceJournal) -> bool {
        let wall = self.inner.clock.wall();
        let mono = self.inner.clock.mono();
        let mut state = self.inner.lock();
        if state.grace.is_some() || state.expiry_observed.is_some() {
            return false;
        }
        if !state.admit_grace_entry(wall, mono) {
            return false;
        }
        let Some(window) = state.open.as_mut() else {
            return false;
        };
        let Some((end_wall, end_mono)) = window
            .grace
            .as_ref()
            .map(|arm| (arm.end_wall, arm.end_mono))
        else {
            return false;
        };
        window.deadline_wall = end_wall;
        window.deadline_mono = end_mono;
        state.signed_end_seen = None;
        state.grace = Some(journal);
        true
    }

    /// Reopens a window resumed in its drive grace: armed, entered, and open
    /// until the grace end.
    pub(super) fn open_in_drive_grace(&self, seq: u64, arm: GraceArm, journal: GraceJournal) {
        self.open(seq, arm.end_wall, arm.end_mono);
        let mut state = self.inner.lock();
        if let Some(window) = state.open.as_mut() {
            window.grace = Some(arm);
        }
        state.grace = Some(journal);
    }

    /// Stops issuing permits and cancels every request and stream permit.
    pub(super) fn close(&self) {
        let wall = self.inner.clock.wall();
        self.inner.lock().shut(wall);
    }

    /// Latches the gate closed if the reading (`wall`, `mono`) shows a
    /// deadline passed (called each controller tick with its own reading).
    pub(super) fn observe(&self, wall: i64, mono: u64) {
        self.inner.observe(wall, mono);
    }
}

/// The process-wide write fence (M02-T13 round 2): how many writes that
/// passed their authority check are still running.
static WRITE_FENCE: Mutex<usize> = Mutex::new(0);

fn write_fence() -> MutexGuard<'static, usize> {
    WRITE_FENCE.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Ends a counted write, also when it unwinds.
struct FencedWrite;

impl Drop for FencedWrite {
    fn drop(&mut self) {
        let mut writing = write_fence();
        *writing = writing.saturating_sub(1);
    }
}

/// Runs one local write under a window's authority: `live` (the holder's
/// permit check) is asked and the write counted under the fence's lock, so
/// once a close has cancelled the permit no write begins, and a close that
/// reads [`writes_in_progress`] after cancelling sees every write that passed
/// its check. `None` when the authority has ended: nothing was written.
/// Each call is one mutation (one commit or one file publication); a caller
/// that writes several times calls this before each.
pub fn write_under_authority<T>(live: &dyn Fn() -> bool, write: impl FnOnce() -> T) -> Option<T> {
    {
        let mut writing = write_fence();
        if !live() {
            return None;
        }
        *writing += 1;
    }
    let _counted = FencedWrite;
    Some(write())
}

/// How many writes that passed their authority check are still running, in
/// this process.
pub fn writes_in_progress() -> usize {
    *write_fence()
}

#[cfg(test)]
mod fleet_admission_tests {
    //! M09-T08: the gate no longer parks Fleet; a Fleet send is window-gated
    //! exactly as a Legacy one.

    use std::sync::atomic::{AtomicI64, AtomicU64};

    use super::*;
    use crate::collection::window::refusal::Refusal;

    #[derive(Debug, Default)]
    struct StepClock {
        wall: AtomicI64,
        mono: AtomicU64,
    }

    impl StepClock {
        fn advance(&self, seconds: u64) {
            self.wall.fetch_add(seconds as i64, Ordering::SeqCst);
            self.mono.fetch_add(seconds, Ordering::SeqCst);
        }
    }

    impl WindowClock for StepClock {
        fn wall(&self) -> i64 {
            self.wall.load(Ordering::SeqCst)
        }
        fn mono(&self) -> u64 {
            self.mono.load(Ordering::SeqCst)
        }
        fn mono_boot_scoped(&self) -> bool {
            true
        }
        fn boot_time(&self) -> Result<i64, Refusal> {
            Ok(0)
        }
    }

    fn gate() -> (Arc<StepClock>, EgressGate) {
        let clock = Arc::new(StepClock::default());
        clock.advance(1_000);
        (clock.clone(), EgressGate::new(clock))
    }

    #[test]
    fn fleet_admission_window_gating_refuses_fleet_outside_a_window() {
        let (_, gate) = gate();
        for kind in [PermitKind::Request, PermitKind::Stream, PermitKind::TokenExchange] {
            assert_eq!(gate.permit(Provider::Fleet, kind).unwrap_err(), GateRefusal::WindowClosed);
        }
    }

    #[test]
    fn fleet_admission_window_gating_permits_fleet_inside_and_stops_at_the_deadline() {
        let (clock, gate) = gate();
        gate.open(1, 1_000 + 600, 1_000 + 600);
        let permit = gate.permit(Provider::Fleet, PermitKind::Request).expect("Fleet inside the window");
        assert_eq!(permit.provider(), Provider::Fleet);
        assert_eq!(permit.time_left(), Duration::from_secs(600));
        let stream = gate.permit(Provider::Fleet, PermitKind::Stream).expect("Fleet stream inside the window");
        clock.advance(600);
        assert_eq!(gate.permit(Provider::Fleet, PermitKind::Request).unwrap_err(), GateRefusal::WindowClosed);
        assert!(stream.is_cancelled(), "the deadline cancels a held Fleet stream");
        // Latched: a clock rollback does not reopen the gate.
        clock.wall.fetch_sub(300, Ordering::SeqCst);
        assert_eq!(gate.permit(Provider::Fleet, PermitKind::Request).unwrap_err(), GateRefusal::WindowClosed);
    }

    #[test]
    fn fleet_admission_window_gating_cuts_fleet_at_close() {
        let (_, gate) = gate();
        gate.open(1, 1_000 + 600, 1_000 + 600);
        let permit = gate.permit(Provider::Fleet, PermitKind::Request).expect("Fleet inside the window");
        gate.close();
        assert!(permit.is_cancelled());
        assert_eq!(gate.permit(Provider::Fleet, PermitKind::Request).unwrap_err(), GateRefusal::WindowClosed);
    }
}
