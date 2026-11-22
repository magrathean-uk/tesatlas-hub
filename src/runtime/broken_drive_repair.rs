// SPDX-License-Identifier: AGPL-3.0-only

//! The monthly broken-drive repair scheduler in the serving process (M03-T28a).
//!
//! Per vehicle, the schedule keeps one record
//! (`hub_metadata['drive_repair_schedule:<vehicle>']`, see
//! [`crate::db::DriveRepairSchedule`]); a restart repeats nothing that is not
//! due and skips nothing that is overdue. The rules, in the order a tick
//! applies them:
//!
//! - Automatic runs are off until a stored `automatic = true` says
//!   otherwise (the real-phone witness is recorded first). With them off
//!   nothing runs and nothing is written.
//! - A vehicle is scheduled only once an import has recorded its candidate
//!   index; its first run is due then, and every run after it 30 days after
//!   the previous pass completed.
//! - A pass that finds the schedule overdue (after a wake, or a forward
//!   clock jump) runs once and schedules the next 30 days from then: missed
//!   months are not replayed.
//! - A clock found behind the recorded high-water mark has jumped back: the
//!   schedule is re-anchored to it, nothing becomes due early, and the next
//!   run is at most 30 days from the present.
//! - An import, a hand-back, a backup, an open window or a service that is
//!   not ready defers a due run: nothing runs, nothing is recorded and the
//!   schedule looks again shortly. A deferral is not a failure.
//! - A failed pass is retried after 1 hour, 6 hours, then daily, until one
//!   completes.
//! - A pass that changes nothing persists its last and next run and
//!   publishes nothing.
//!
//! One pass at a time: a second request while one runs is deferred. A pass
//! reads its record under the pass slot, so a timer step never runs on a
//! record a manual pass has meanwhile replaced, and its times come from the
//! clock when it starts and when it finishes (REV-CX-T28). Before it runs a
//! pass is written down as a failed attempt with its backoff, so an outcome
//! the store does not take is neither replayed at once nor loses its
//! backoff; the outcome itself is kept and written again before the
//! vehicle's next step.
//!
//! While a pass runs it holds what it must not overlap (a [`PassHold`]): the
//! window lock, so no window opens and no import is admitted, and the
//! maintenance turn, so no backup starts. It starts only while no import
//! publication is pending for any vehicle and the store's coverage holds
//! every hand-back. Undo and the republication after a protection run under
//! the same slot and holds ([`RepairScheduler::run_exclusive`]), and every
//! manual job is joined when the service stops.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use uuid::Uuid;

use crate::{
    db::{DriveRepairSchedule, HubStore, StoreError},
    import::source_db::importer::{DriveRepairApplyOutcome, apply_drive_repairs},
    runtime::backup_schedule::wall_ms,
};

#[cfg(test)]
mod tests;

const MS_PER_HOUR: i64 = 60 * 60 * 1000;
const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;

/// How long after a completed pass the next one is due.
pub const INTERVAL_MS: i64 = 30 * MS_PER_DAY;
/// The waits after the first, second and every later failed pass in a row.
pub const BACKOFF_MS: [i64; 3] = [MS_PER_HOUR, 6 * MS_PER_HOUR, MS_PER_DAY];
/// How long a deferred run waits before the schedule looks again.
pub const DEFER_RECHECK_MS: i64 = 5 * 60 * 1000;
/// A clock this far behind the high-water mark has jumped back; less is a
/// small correction.
pub const CLOCK_SLACK_MS: i64 = 60 * 1000;
/// The high-water mark is written at most this often while nothing else is.
const HIGH_WATER_WRITE_MS: i64 = MS_PER_HOUR;
/// The timer never sleeps longer than this, so a wake from sleep, a newly
/// recorded index and a changed on/off are noticed within it.
const LONGEST_WAIT_MS: i64 = 5 * 60 * 1000;
const SHORTEST_WAIT_MS: i64 = 1000;

/// Why a due run waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferral {
    /// An import is running or its publication is pending.
    Import,
    /// A hand-back is closing or its head is being published.
    HandBack,
    /// A backup is running.
    Backup,
    /// A collection window is open, pending or closing.
    Window,
    /// The service is not ready (listener not bound, or keys not restored).
    NotReady,
    /// Another pass holds the slot.
    Busy,
}

impl Deferral {
    pub fn code(self) -> &'static str {
        match self {
            Self::Import => "import",
            Self::HandBack => "hand_back",
            Self::Backup => "backup",
            Self::Window => "window",
            Self::NotReady => "not_ready",
            Self::Busy => "busy",
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutcome {
    /// The pass completed and changed nothing; nothing was published.
    NoChange,
    /// A batch was activated and its head is served.
    Repaired { batch_id: i64 },
    /// The pass did not start; nothing is recorded.
    Deferred(Deferral),
    /// The pass failed; the code is stored, never a message.
    Failed(String),
}

/// One vehicle's pass. The service's is [`StoreRepairRunner`]; tests supply
/// others.
pub trait RepairPassRunner: Send + Sync {
    fn run_pass(&self, vehicle: Uuid, now_ms: i64) -> PassOutcome;
}

/// What a running pass holds until it ends, so that nothing it must not
/// overlap starts meanwhile (the service's: the window lock and the
/// maintenance turn). Dropping it releases them.
#[derive(Default)]
pub struct PassHold(Vec<Box<dyn std::any::Any + Send>>);

impl PassHold {
    /// Holds nothing.
    pub fn none() -> Self {
        Self::default()
    }

    /// Also holds `held` until the pass ends.
    pub fn with(mut self, held: impl std::any::Any + Send) -> Self {
        self.0.push(Box::new(held));
        self
    }
}

/// Admits a pass: the first reason it must wait now, or what it holds while
/// it runs.
pub type DeferralCheck = Box<dyn Fn() -> Result<PassHold, Deferral> + Send + Sync>;

/// Unix milliseconds now; the service's is [`wall_ms`], tests inject one.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// The code a pass is stored with while it runs: an outcome that never
/// reaches the record leaves it, with its backoff.
pub const INTERRUPTED: &str = "interrupted";

/// One turn shared by the repair passes and the encrypted backups, so
/// neither starts while the other runs (a backup's archive is written after
/// it releases the publication gate). A pass only tries for it and defers; a
/// backup waits for it.
#[derive(Clone, Default)]
pub struct MaintenanceExclusion(Arc<(Mutex<bool>, Condvar)>);

/// Holds the maintenance turn; dropping it frees the turn.
pub struct MaintenanceTurn(Arc<(Mutex<bool>, Condvar)>);

impl MaintenanceExclusion {
    /// The turn if it is free now.
    pub fn try_take(&self) -> Option<MaintenanceTurn> {
        let mut busy = lock(&self.0.0);
        if *busy {
            return None;
        }
        *busy = true;
        Some(MaintenanceTurn(Arc::clone(&self.0)))
    }

    /// The turn, once it is free.
    pub fn take(&self) -> MaintenanceTurn {
        let mut busy = lock(&self.0.0);
        while *busy {
            busy = self.0.1.wait(busy).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        *busy = true;
        MaintenanceTurn(Arc::clone(&self.0))
    }
}

impl Drop for MaintenanceTurn {
    fn drop(&mut self) {
        *lock(&self.0.0) = false;
        self.0.1.notify_all();
    }
}

/// What one step of one vehicle did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VehicleStep {
    /// Automatic runs are off for the vehicle: nothing was read or written
    /// beyond its record.
    Off,
    NotDue { wait_ms: i64 },
    Deferred(Deferral),
    Ran(PassOutcome),
}

/// One timer step across the indexed vehicles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tick {
    /// How long until the schedule next needs a look.
    pub wait_ms: i64,
    pub steps: Vec<(Uuid, VehicleStep)>,
}

struct Shared {
    store: HubStore,
    runner: Arc<dyn RepairPassRunner>,
    deferral: DeferralCheck,
    clock: Clock,
    /// The one pass slot.
    running: AtomicBool,
    /// Serialises a pass's start (its last look at the stored on/off and its
    /// attempt record) against the operator's on/off.
    start: Mutex<()>,
    /// Set once the service's API listener is bound.
    ready: AtomicBool,
    cancel: AtomicBool,
    /// Finished records the store did not take, written again before the
    /// vehicle's next step.
    unsaved: Mutex<HashMap<Uuid, DriveRepairSchedule>>,
    /// Manual jobs (Check now, Undo, a protection's republication), joined
    /// when the service stops.
    jobs: Mutex<Vec<JoinHandle<()>>>,
    wake: Condvar,
    wake_lock: Mutex<()>,
    /// Tests: outcome writes to refuse.
    #[cfg(test)]
    refuse_outcome_writes: std::sync::atomic::AtomicUsize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Holds the one pass slot; dropping it frees the slot.
struct PassSlot<'a>(&'a AtomicBool);

impl<'a> PassSlot<'a> {
    fn take(flag: &'a AtomicBool) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| Self(flag))
    }
}

impl Drop for PassSlot<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// The wait after `failures` failed passes in a row (at least one).
pub fn backoff_ms(failures: u32) -> i64 {
    let index = usize::try_from(failures.saturating_sub(1)).unwrap_or(usize::MAX);
    BACKOFF_MS[index.min(BACKOFF_MS.len() - 1)]
}

/// Re-anchors `record` to `now_ms` when the clock is behind the recorded
/// high-water mark, and advances the mark otherwise. Returns whether the
/// record changed. Pure.
///
/// A jump back never makes a run due early: every recorded time is capped at
/// the present, and the next run at what is left of its wait from it: the
/// backoff after a failed pass, one interval otherwise. The mark is written
/// only when it moved by an hour, so ticks write nothing between passes.
pub fn settle_clock(record: &mut DriveRepairSchedule, now_ms: i64) -> bool {
    if now_ms.saturating_add(CLOCK_SLACK_MS) < record.high_water_ms {
        let wait = if record.failures > 0 { backoff_ms(record.failures) } else { INTERVAL_MS };
        record.high_water_ms = now_ms;
        record.last_run_ms = record.last_run_ms.map(|last| last.min(now_ms));
        record.last_attempt_ms = record.last_attempt_ms.map(|last| last.min(now_ms));
        record.next_run_ms = record
            .next_run_ms
            .map(|next| next.min(now_ms.saturating_add(wait)));
        return true;
    }
    if now_ms >= record.high_water_ms.saturating_add(HIGH_WATER_WRITE_MS) || record.high_water_ms == 0 {
        record.high_water_ms = now_ms;
        return true;
    }
    false
}

/// When the next pass is due: the recorded time, or at once for a vehicle
/// that has an index but no recorded run (its first import just completed).
pub fn due_at_ms(record: &DriveRepairSchedule, now_ms: i64) -> i64 {
    record.next_run_ms.unwrap_or(now_ms)
}

/// Records a pass as started, before it runs: a failed attempt at
/// `started_ms` with its backoff, so an outcome that never reaches the
/// record (a crash, a store that stays busy) is not replayed at once and
/// keeps the backoff. Pure.
pub fn record_start(record: &mut DriveRepairSchedule, started_ms: i64) {
    record.failures = record.failures.saturating_add(1);
    record.last_attempt_ms = Some(started_ms);
    record.next_run_ms = Some(started_ms.saturating_add(backoff_ms(record.failures)));
    record.last_failure = Some(INTERRUPTED.to_owned());
}

/// Records a finished pass in `record`: started at `started_ms` and finished
/// at `finished_ms`, the next run is 30 days after a completed pass
/// finished, or the backoff for the failures so far after a failed one.
/// Returns false for an outcome that records nothing (a deferral). Pure.
pub fn record_outcome(
    record: &mut DriveRepairSchedule,
    outcome: &PassOutcome,
    started_ms: i64,
    finished_ms: i64,
) -> bool {
    let finished_ms = finished_ms.max(started_ms);
    match outcome {
        PassOutcome::Deferred(_) => return false,
        PassOutcome::NoChange | PassOutcome::Repaired { .. } => {
            record.last_run_ms = Some(started_ms);
            record.last_attempt_ms = Some(started_ms);
            record.next_run_ms = Some(finished_ms.saturating_add(INTERVAL_MS));
            record.high_water_ms = record.high_water_ms.max(finished_ms);
            record.failures = 0;
            record.last_failure = None;
            record.last_outcome = Some(
                if matches!(outcome, PassOutcome::NoChange) { "no_change" } else { "repaired" }
                    .to_owned(),
            );
        }
        PassOutcome::Failed(code) => {
            record.failures = record.failures.saturating_add(1);
            record.last_attempt_ms = Some(started_ms);
            record.next_run_ms = Some(finished_ms.saturating_add(backoff_ms(record.failures)));
            record.high_water_ms = record.high_water_ms.max(finished_ms);
            record.last_failure = Some(code.clone());
        }
    }
    true
}

impl Shared {
    /// Writes `record` over the stored one, keeping the stored on/off (the
    /// operator's). Called under the pass slot only.
    fn persist(&self, vehicle: Uuid, record: &DriveRepairSchedule) -> Result<(), StoreError> {
        self.store
            .update_drive_repair_schedule(vehicle, |stored| {
                let automatic = stored.automatic;
                *stored = record.clone();
                stored.automatic = automatic;
            })
            .map(|_| ())
    }

    /// [`Self::persist`] for a finished pass; a record the store does not
    /// take is kept and written again before the vehicle's next step.
    fn persist_outcome(&self, vehicle: Uuid, record: &DriveRepairSchedule) {
        #[cfg(test)]
        let refused = self
            .refuse_outcome_writes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1))
            .is_ok();
        #[cfg(not(test))]
        let refused = false;
        let written = if refused { Err(StoreError::PublicationGateBusy) } else { self.persist(vehicle, record) };
        match written {
            Ok(()) => {
                lock(&self.unsaved).remove(&vehicle);
            }
            Err(error) => {
                tracing::warn!(%vehicle, %error, "the repair schedule record could not be written; it is kept and written again");
                lock(&self.unsaved).insert(vehicle, record.clone());
            }
        }
    }

    /// Writes a finished record the store refused earlier. False while it
    /// still does not take it: the vehicle then waits.
    fn flush_unsaved(&self, vehicle: Uuid) -> bool {
        let Some(record) = lock(&self.unsaved).get(&vehicle).cloned() else {
            return true;
        };
        match self.persist(vehicle, &record) {
            Ok(()) => {
                lock(&self.unsaved).remove(&vehicle);
                true
            }
            Err(error) => {
                tracing::warn!(%vehicle, %error, "the repair schedule record still could not be written");
                false
            }
        }
    }

    /// Whether a pass may start now, and what it holds while it runs: a
    /// stopping or not yet ready service, the service's own deferrals (taken
    /// first, so their holds are in place before the store is read), an
    /// import publication pending for any vehicle (a publication resumes
    /// them all), and a hand-back the store's coverage lacks.
    fn admit(&self) -> Result<PassHold, Deferral> {
        if self.cancel.load(Ordering::SeqCst) || !self.ready.load(Ordering::SeqCst) {
            return Err(Deferral::NotReady);
        }
        let hold = (self.deferral)()?;
        match self.store.import_publication_intents() {
            Ok(intents) if !intents.is_empty() => return Err(Deferral::Import),
            Ok(_) => {}
            Err(_) => return Err(Deferral::NotReady),
        }
        match self.store.collection_coverage_unreconciled() {
            Ok(false) => {}
            Ok(true) => return Err(Deferral::HandBack),
            Err(_) => return Err(Deferral::NotReady),
        }
        Ok(hold)
    }

    /// One vehicle. `forced` is a manual check: it skips the on/off and the
    /// due time, and still honours every deferral. Everything from the read
    /// of the record to the write of the outcome happens under the slot.
    fn step(&self, vehicle: Uuid, forced: bool, clock: &dyn Fn() -> i64) -> Result<VehicleStep, StoreError> {
        let Some(_slot) = PassSlot::take(&self.running) else {
            return Ok(VehicleStep::Deferred(Deferral::Busy));
        };
        if !self.flush_unsaved(vehicle) {
            return Ok(VehicleStep::Deferred(Deferral::NotReady));
        }
        let now_ms = clock();
        let mut record = self.store.drive_repair_schedule(vehicle)?;
        if !forced && record.automatic != Some(true) {
            return Ok(VehicleStep::Off);
        }
        let settled = settle_clock(&mut record, now_ms);
        let due = due_at_ms(&record, now_ms);
        let write_settled = |record: &DriveRepairSchedule| {
            if settled && let Err(error) = self.persist(vehicle, record) {
                tracing::warn!(%vehicle, %error, "the repair schedule clock could not be written");
            }
        };
        if !forced && now_ms < due {
            write_settled(&record);
            return Ok(VehicleStep::NotDue { wait_ms: due - now_ms });
        }
        let hold = match self.admit() {
            Ok(hold) => hold,
            Err(reason) => {
                write_settled(&record);
                return Ok(VehicleStep::Deferred(reason));
            }
        };
        {
            // The last look at the on/off, and the attempt on record, before
            // the pass starts; the operator's on/off waits for this.
            let _start = lock(&self.start);
            if !forced && self.store.drive_repair_schedule(vehicle)?.automatic != Some(true) {
                write_settled(&record);
                return Ok(VehicleStep::Off);
            }
            let mut started = record.clone();
            record_start(&mut started, now_ms);
            self.persist(vehicle, &started)?;
        }
        let outcome = self.runner.run_pass(vehicle, now_ms);
        drop(hold);
        // A pass the runner could not start records nothing: the record
        // before the attempt goes back.
        let mut finished = record;
        record_outcome(&mut finished, &outcome, now_ms, clock());
        self.persist_outcome(vehicle, &finished);
        Ok(match outcome {
            PassOutcome::Deferred(reason) => VehicleStep::Deferred(reason),
            other => VehicleStep::Ran(other),
        })
    }

    fn tick(&self, clock: &dyn Fn() -> i64) -> Tick {
        let vehicles = match self.store.drive_repair_indexed_vehicles() {
            Ok(vehicles) => vehicles,
            Err(error) => {
                tracing::warn!(%error, "the repair schedule could not list vehicles");
                return Tick { wait_ms: DEFER_RECHECK_MS, steps: Vec::new() };
            }
        };
        let mut wait_ms = LONGEST_WAIT_MS;
        let mut steps = Vec::new();
        for vehicle in vehicles {
            if self.cancel.load(Ordering::SeqCst) {
                break;
            }
            let step = match self.step(vehicle, false, clock) {
                Ok(step) => step,
                Err(error) => {
                    tracing::warn!(%vehicle, %error, "the repair schedule could not be read");
                    wait_ms = wait_ms.min(DEFER_RECHECK_MS);
                    continue;
                }
            };
            match &step {
                VehicleStep::Off => {}
                VehicleStep::NotDue { wait_ms: wait } => wait_ms = wait_ms.min(*wait),
                VehicleStep::Deferred(_) => wait_ms = wait_ms.min(DEFER_RECHECK_MS),
                VehicleStep::Ran(outcome) => {
                    tracing::info!(%vehicle, ?outcome, "scheduled repair pass finished");
                    if let Ok(record) = self.store.drive_repair_schedule(vehicle)
                        && let Some(next) = record.next_run_ms
                    {
                        wait_ms = wait_ms.min(next.saturating_sub(clock()));
                    }
                }
            }
            steps.push((vehicle, step));
        }
        Tick { wait_ms: wait_ms.clamp(SHORTEST_WAIT_MS, LONGEST_WAIT_MS), steps }
    }

    fn timer(&self, startup_delay: Duration) {
        self.sleep(startup_delay);
        while !self.cancel.load(Ordering::SeqCst) {
            let tick = self.tick(&*self.clock);
            self.sleep(Duration::from_millis(u64::try_from(tick.wait_ms).unwrap_or(1)));
        }
    }

    fn sleep(&self, wait: Duration) {
        let guard = lock(&self.wake_lock);
        if self.cancel.load(Ordering::SeqCst) {
            return;
        }
        drop(
            self.wake
                .wait_timeout(guard, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
}

/// The service's repair schedule. Dropping it stops the timer and joins
/// every manual job.
pub struct RepairScheduler {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

/// The first look after a start waits this long, so start-up settles first.
pub const STARTUP_DELAY: Duration = Duration::from_secs(2 * 60);

impl RepairScheduler {
    /// Starts the schedule's timer thread. `deferral` holds due runs back
    /// (an import, a hand-back, a backup, an open window, a service not yet
    /// ready) and returns what a pass holds while it runs; it is asked only
    /// when a run is due. Nothing runs before [`Self::mark_ready`]. With
    /// automatic runs off for every vehicle the timer only reads records.
    pub fn start(
        store: HubStore,
        runner: Arc<dyn RepairPassRunner>,
        deferral: DeferralCheck,
    ) -> std::io::Result<Self> {
        Self::start_with(store, runner, deferral, Some(STARTUP_DELAY), Arc::new(wall_ms))
    }

    fn start_with(
        store: HubStore,
        runner: Arc<dyn RepairPassRunner>,
        deferral: DeferralCheck,
        timer_delay: Option<Duration>,
        clock: Clock,
    ) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            store,
            runner,
            deferral,
            clock,
            running: AtomicBool::new(false),
            start: Mutex::new(()),
            ready: AtomicBool::new(false),
            cancel: AtomicBool::new(false),
            unsaved: Mutex::new(HashMap::new()),
            jobs: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            wake_lock: Mutex::new(()),
            #[cfg(test)]
            refuse_outcome_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let thread = match timer_delay {
            Some(delay) => {
                let timer = Arc::clone(&shared);
                Some(
                    std::thread::Builder::new()
                        .name("hub-drive-repair-schedule".to_owned())
                        .spawn(move || timer.timer(delay))?,
                )
            }
            None => None,
        };
        Ok(Self { shared, thread: Mutex::new(thread) })
    }

    /// The service is ready (its API listener is bound): passes may start.
    pub fn mark_ready(&self) {
        self.shared.ready.store(true, Ordering::SeqCst);
        self.kick();
    }

    /// A manual check of one vehicle (T28b's Check now): the same pass and
    /// the same record as a scheduled one, ignoring the on/off and the due
    /// time and honouring every deferral.
    pub fn run_now(&self, vehicle: Uuid) -> Result<VehicleStep, StoreError> {
        if self.shared.cancel.load(Ordering::SeqCst) {
            return Ok(VehicleStep::Deferred(Deferral::NotReady));
        }
        let step = self.shared.step(vehicle, true, &*self.shared.clock);
        self.kick();
        step
    }

    /// Runs `work` as a pass of its own (Undo, the republication after a
    /// protection): under the one pass slot, after every deferral a
    /// scheduled pass honours, holding what it holds. A stopping service
    /// starts none.
    pub fn run_exclusive<R>(&self, work: impl FnOnce() -> R) -> Result<R, Deferral> {
        let Some(_slot) = PassSlot::take(&self.shared.running) else {
            return Err(Deferral::Busy);
        };
        let _hold = self.shared.admit()?;
        Ok(work())
    }

    /// Sets the stored on/off. A pass that has not yet started when this
    /// returns sees it.
    pub fn set_automatic(&self, vehicle: Uuid, on: bool) -> Result<DriveRepairSchedule, StoreError> {
        let written = {
            let _start = lock(&self.shared.start);
            self.shared.store.set_drive_repair_automatic(vehicle, on)?
        };
        self.kick();
        Ok(written)
    }

    /// Starts a manual job on its own thread; the service's stop joins it.
    /// A stopping service starts none.
    pub fn spawn_job(&self, work: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
        let mut jobs = lock(&self.shared.jobs);
        if self.shared.cancel.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("the service is stopping"));
        }
        jobs.retain(|job| !job.is_finished());
        let job = std::thread::Builder::new()
            .name("hub-drive-repair-job".to_owned())
            .spawn(work)?;
        jobs.push(job);
        Ok(())
    }

    /// Wakes the timer, for example after the stored on/off changed.
    pub fn kick(&self) {
        let _guard = lock(&self.shared.wake_lock);
        self.shared.wake.notify_all();
    }

    /// The vehicle's stored record, for status.
    pub fn status(&self, vehicle: Uuid) -> Result<DriveRepairSchedule, StoreError> {
        self.shared.store.drive_repair_schedule(vehicle)
    }

    /// Steps the timer once at `now_ms` (tests drive time through this).
    #[cfg(test)]
    fn tick(&self, now_ms: i64) -> Tick {
        self.shared.tick(&move || now_ms)
    }

    /// Steps the timer once on its own clock (tests).
    #[cfg(test)]
    fn tick_on_clock(&self) -> Tick {
        self.shared.tick(&*self.shared.clock)
    }

    /// Quiesces the schedule at once: the timer stops and no new pass or
    /// job starts. Does not wait.
    pub fn request_stop(&self) {
        self.shared.cancel.store(true, Ordering::SeqCst);
        let _guard = lock(&self.shared.wake_lock);
        self.shared.wake.notify_all();
    }

    /// Stops the timer and waits for it and every manual job only within
    /// what is left of the service's shutdown budget. A pass or job still
    /// finishing keeps its handle here (a later stop or the drop joins it)
    /// and the timeout is reported.
    pub fn stop_within(
        &self,
        budget: &crate::runtime::shutdown::ShutdownBudget,
    ) -> Result<(), crate::runtime::shutdown::ShutdownTimedOut> {
        use crate::runtime::shutdown::join_within;
        self.request_stop();
        let timer = lock(&self.thread).take();
        if let Some(handle) = timer
            && let Err((handle, timeout)) = join_within(handle, budget, "drive repair schedule")
        {
            *lock(&self.thread) = Some(handle);
            return Err(timeout);
        }
        let jobs = std::mem::take(&mut *lock(&self.shared.jobs));
        let mut jobs = jobs.into_iter();
        while let Some(job) = jobs.next() {
            if let Err((job, timeout)) = join_within(job, budget, "drive repair job") {
                let mut kept = lock(&self.shared.jobs);
                kept.push(job);
                kept.extend(jobs);
                return Err(timeout);
            }
        }
        Ok(())
    }
}

impl Drop for RepairScheduler {
    fn drop(&mut self) {
        let budget = crate::runtime::shutdown::ShutdownBudget::process();
        if budget.is_stopping() {
            let _ = self.stop_within(&budget);
        } else {
            self.request_stop();
            let timer = lock(&self.thread).take();
            if let Some(handle) = timer {
                let _ = handle.join();
            }
            let jobs = std::mem::take(&mut *lock(&self.shared.jobs));
            for job in jobs {
                let _ = job.join();
            }
        }
    }
}

/// Takes the window lock exclusively and the maintenance turn for a pass:
/// an import (which holds the lock shared for its whole run) or a running
/// backup defers it; once held, no window opens, no import is admitted and
/// no backup starts until the pass ends.
pub fn hold_window_and_turn(
    data_dir: &std::path::Path,
    maintenance: &MaintenanceExclusion,
) -> Result<PassHold, Deferral> {
    use crate::collection::window::{FileLock, LockMode};
    let lock_path = crate::runtime::operator::window_dir(data_dir).join("window.lock");
    let window = match FileLock::acquire(&lock_path, LockMode::Exclusive, false) {
        Ok(Some(lock)) => lock,
        Ok(None) => return Err(Deferral::Import),
        Err(_) => return Err(Deferral::NotReady),
    };
    let turn = maintenance.try_take().ok_or(Deferral::Backup)?;
    Ok(PassHold::none().with(window).with(turn))
}

/// The service's deferral check: the first reason a due run must wait, or
/// what the pass holds while it runs.
///
/// The window controller's import guard answers an open, pending or closing
/// window and an unsynced hand-back; then the window lock and the
/// maintenance turn are taken for the pass ([`hold_window_and_turn`]).
/// `not_ready` comes from the recovery gate.
pub fn service_deferral(
    window: Arc<crate::collection::window::WindowController>,
    data_dir: PathBuf,
    maintenance: MaintenanceExclusion,
    not_ready: Box<dyn Fn() -> bool + Send + Sync>,
) -> DeferralCheck {
    use crate::collection::window::Refusal;
    Box::new(move || {
        if not_ready() {
            return Err(Deferral::NotReady);
        }
        match window.import_guard() {
            Ok(guard) => drop(guard),
            Err(Refusal::HandbackPending) => return Err(Deferral::HandBack),
            Err(Refusal::WindowActive) => return Err(Deferral::Window),
            Err(_) => return Err(Deferral::NotReady),
        }
        if window.coverage_unreconciled() {
            return Err(Deferral::HandBack);
        }
        hold_window_and_turn(&data_dir, &maintenance)
    })
}

/// What a repair publication needs: the cursor key (only loaded when it
/// exists; a Hub restored without it is not ready) and the publication
/// gate, taken without waiting.
pub(crate) enum PublicationInputs {
    Ready(crate::protocol::CursorKey, crate::db::PublicationGate),
    Deferred(Deferral),
    Failed(&'static str),
}

pub(crate) fn publication_inputs(store: &HubStore, data_dir: &std::path::Path) -> PublicationInputs {
    let key_path = crate::legacy_owner_credentials::cursor_key_path(data_dir);
    if std::fs::symlink_metadata(key_path).is_err() {
        return PublicationInputs::Deferred(Deferral::NotReady);
    }
    let cursor_key = match crate::legacy_owner_credentials::load_or_create_cursor_key(data_dir) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(%error, "the repair publication could not load the cursor key");
            return PublicationInputs::Failed("cursor_key");
        }
    };
    match store.try_acquire_publication_gate() {
        Ok(gate) => PublicationInputs::Ready(cursor_key, gate),
        Err(StoreError::PublicationGateBusy) => PublicationInputs::Deferred(Deferral::HandBack),
        Err(error) => {
            tracing::warn!(%error, "the repair publication could not take the gate");
            PublicationInputs::Failed("store")
        }
    }
}

/// Publishes the vehicle's head again when it is refused while no repair
/// batch is publishing (a protection superseded an active component and
/// changed its layer). The answer is `served`, `withheld`, `none` (no head),
/// or the deferral's or failure's code.
pub(crate) fn republish_head(store: &HubStore, data_dir: &std::path::Path, vehicle: Uuid) -> String {
    let (cursor_key, gate) = match publication_inputs(store, data_dir) {
        PublicationInputs::Ready(key, gate) => (key, gate),
        PublicationInputs::Deferred(reason) => return reason.code().to_owned(),
        PublicationInputs::Failed(code) => return code.to_owned(),
    };
    match crate::import::source_db::importer::republish_drive_repair_head(store, &cursor_key, &gate, vehicle) {
        Ok(Some(true)) => "served",
        Ok(Some(false)) => "withheld",
        Ok(None) => "none",
        Err(error) => {
            tracing::warn!(%vehicle, %error, "the repaired head could not be published again");
            "failed"
        }
    }
    .to_owned()
}

/// Runs the repair pass against the service's store: plan from the stored
/// index, then activate the next batch and publish its head once, under the
/// publication gate. The cursor key is only loaded when it exists; a Hub
/// restored without it is not ready.
pub struct StoreRepairRunner {
    store: HubStore,
    data_dir: PathBuf,
}

impl StoreRepairRunner {
    pub fn new(store: HubStore, data_dir: PathBuf) -> Self {
        Self { store, data_dir }
    }
}

impl RepairPassRunner for StoreRepairRunner {
    fn run_pass(&self, vehicle: Uuid, now_ms: i64) -> PassOutcome {
        let failed = |code: &str, error: &dyn std::fmt::Display| {
            tracing::warn!(%vehicle, code, %error, "repair pass failed");
            PassOutcome::Failed(code.to_owned())
        };
        match self.store.import_publication_holds() {
            Ok(holds) if holds.pending(vehicle) => return PassOutcome::Deferred(Deferral::Import),
            Ok(_) => {}
            Err(error) => return failed("store", &error),
        }
        let (cursor_key, gate) = match publication_inputs(&self.store, &self.data_dir) {
            PublicationInputs::Ready(key, gate) => (key, gate),
            PublicationInputs::Deferred(reason) => return PassOutcome::Deferred(reason),
            PublicationInputs::Failed(code) => return PassOutcome::Failed(code.to_owned()),
        };
        match self.store.plan_drive_repairs(vehicle, now_ms) {
            Ok(report) if !report.index_present => return PassOutcome::Deferred(Deferral::Import),
            Ok(_) => {}
            Err(error) => return failed("plan", &error),
        }
        match apply_drive_repairs(&self.store, &cursor_key, &gate, vehicle, now_ms) {
            Ok(DriveRepairApplyOutcome::Idle(_)) => PassOutcome::NoChange,
            Ok(DriveRepairApplyOutcome::Served { batch_id, .. }) => PassOutcome::Repaired { batch_id },
            Ok(DriveRepairApplyOutcome::Refused(reason)) => {
                match self.store.import_publication_holds() {
                    Ok(holds) if holds.pending(vehicle) => PassOutcome::Deferred(Deferral::Import),
                    _ => failed("refused", &reason),
                }
            }
            Ok(DriveRepairApplyOutcome::Compensated { batch_id, .. }) => {
                failed("compensated", &format!("batch {batch_id:?} was not served"))
            }
            Err(error) => failed("apply", &error),
        }
    }
}
