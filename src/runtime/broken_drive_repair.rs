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
//! One pass at a time: a second request while one runs is deferred.

use std::{
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

/// Holds scheduled runs back: the first reason a run must wait now, if any.
pub type DeferralCheck = Box<dyn Fn() -> Option<Deferral> + Send + Sync>;

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
    running: AtomicBool,
    cancel: AtomicBool,
    wake: Condvar,
    wake_lock: Mutex<()>,
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
/// the present, and the next run at one interval from it. The mark is
/// written only when it moved by an hour, so ticks write nothing between
/// passes.
pub fn settle_clock(record: &mut DriveRepairSchedule, now_ms: i64) -> bool {
    if now_ms.saturating_add(CLOCK_SLACK_MS) < record.high_water_ms {
        record.high_water_ms = now_ms;
        record.last_run_ms = record.last_run_ms.map(|last| last.min(now_ms));
        record.last_attempt_ms = record.last_attempt_ms.map(|last| last.min(now_ms));
        record.next_run_ms = record
            .next_run_ms
            .map(|next| next.min(now_ms.saturating_add(INTERVAL_MS)));
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

/// Records a finished pass in `record`: the next run 30 days from `now_ms`
/// after a completed pass, or after the backoff for the failures so far.
/// Returns false for an outcome that records nothing (a deferral). Pure.
pub fn record_outcome(record: &mut DriveRepairSchedule, outcome: &PassOutcome, now_ms: i64) -> bool {
    match outcome {
        PassOutcome::Deferred(_) => return false,
        PassOutcome::NoChange | PassOutcome::Repaired { .. } => {
            record.last_run_ms = Some(now_ms);
            record.last_attempt_ms = Some(now_ms);
            record.next_run_ms = Some(now_ms.saturating_add(INTERVAL_MS));
            record.failures = 0;
            record.last_failure = None;
            record.last_outcome = Some(
                if matches!(outcome, PassOutcome::NoChange) { "no_change" } else { "repaired" }
                    .to_owned(),
            );
        }
        PassOutcome::Failed(code) => {
            record.failures = record.failures.saturating_add(1);
            record.last_attempt_ms = Some(now_ms);
            record.next_run_ms = Some(now_ms.saturating_add(backoff_ms(record.failures)));
            record.last_failure = Some(code.clone());
        }
    }
    true
}

impl Shared {
    fn persist(&self, vehicle: Uuid, record: &DriveRepairSchedule) {
        let written = self.store.update_drive_repair_schedule(vehicle, |stored| {
            // The on/off is the operator's: keep whatever is stored now.
            let automatic = stored.automatic;
            *stored = record.clone();
            stored.automatic = automatic;
        });
        if let Err(error) = written {
            tracing::warn!(%vehicle, %error, "the repair schedule record could not be written");
        }
    }

    /// One vehicle. `forced` is a manual check: it skips the on/off and the
    /// due time, and still honours every deferral.
    fn step(&self, vehicle: Uuid, now_ms: i64, forced: bool) -> Result<VehicleStep, StoreError> {
        let mut record = self.store.drive_repair_schedule(vehicle)?;
        if !forced && record.automatic != Some(true) {
            return Ok(VehicleStep::Off);
        }
        let mut dirty = settle_clock(&mut record, now_ms);
        let due = due_at_ms(&record, now_ms);
        if !forced && now_ms < due {
            if dirty {
                self.persist(vehicle, &record);
            }
            return Ok(VehicleStep::NotDue { wait_ms: due - now_ms });
        }
        if let Some(reason) = (self.deferral)() {
            if dirty {
                self.persist(vehicle, &record);
            }
            return Ok(VehicleStep::Deferred(reason));
        }
        let Some(_slot) = PassSlot::take(&self.running) else {
            return Ok(VehicleStep::Deferred(Deferral::Busy));
        };
        let outcome = self.runner.run_pass(vehicle, now_ms);
        dirty |= record_outcome(&mut record, &outcome, now_ms);
        if dirty {
            self.persist(vehicle, &record);
        }
        Ok(match outcome {
            PassOutcome::Deferred(reason) => VehicleStep::Deferred(reason),
            other => VehicleStep::Ran(other),
        })
    }

    fn tick(&self, now_ms: i64) -> Tick {
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
            let step = match self.step(vehicle, now_ms, false) {
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
                        wait_ms = wait_ms.min(next.saturating_sub(now_ms));
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
            let tick = self.tick(wall_ms());
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

/// The service's repair schedule. Dropping it stops the timer.
pub struct RepairScheduler {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

/// The first look after a start waits this long, so start-up settles first.
pub const STARTUP_DELAY: Duration = Duration::from_secs(2 * 60);

impl RepairScheduler {
    /// Starts the schedule's timer thread. `deferral` holds due runs back
    /// (an import, a hand-back, a backup, an open window, a service not yet
    /// ready); it is asked only when a run is due. With automatic runs off
    /// for every vehicle the timer only reads records.
    pub fn start(
        store: HubStore,
        runner: Arc<dyn RepairPassRunner>,
        deferral: DeferralCheck,
    ) -> std::io::Result<Self> {
        Self::start_with(store, runner, deferral, Some(STARTUP_DELAY))
    }

    fn start_with(
        store: HubStore,
        runner: Arc<dyn RepairPassRunner>,
        deferral: DeferralCheck,
        timer_delay: Option<Duration>,
    ) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            store,
            runner,
            deferral,
            running: AtomicBool::new(false),
            cancel: AtomicBool::new(false),
            wake: Condvar::new(),
            wake_lock: Mutex::new(()),
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

    /// A manual check of one vehicle (T28b's Check now): the same pass and
    /// the same record as a scheduled one, ignoring the on/off and the due
    /// time and honouring every deferral.
    pub fn run_now(&self, vehicle: Uuid) -> Result<VehicleStep, StoreError> {
        if self.shared.cancel.load(Ordering::SeqCst) {
            return Ok(VehicleStep::Deferred(Deferral::NotReady));
        }
        let step = self.shared.step(vehicle, wall_ms(), true);
        self.kick();
        step
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
        self.shared.tick(now_ms)
    }

    /// Quiesces the schedule at once: the timer stops and no new pass
    /// starts. Does not wait.
    pub fn request_stop(&self) {
        self.shared.cancel.store(true, Ordering::SeqCst);
        let _guard = lock(&self.shared.wake_lock);
        self.shared.wake.notify_all();
    }

    /// Stops the timer and waits for it only within what is left of the
    /// service's shutdown budget. A pass still finishing keeps the handle
    /// here (a later stop or the drop joins it) and the timeout is reported.
    pub fn stop_within(
        &self,
        budget: &crate::runtime::shutdown::ShutdownBudget,
    ) -> Result<(), crate::runtime::shutdown::ShutdownTimedOut> {
        self.request_stop();
        let handle = lock(&self.thread).take();
        let Some(handle) = handle else {
            return Ok(());
        };
        match crate::runtime::shutdown::join_within(handle, budget, "drive repair schedule") {
            Ok(_) => Ok(()),
            Err((handle, timeout)) => {
                *lock(&self.thread) = Some(handle);
                Err(timeout)
            }
        }
    }
}

impl Drop for RepairScheduler {
    fn drop(&mut self) {
        let budget = crate::runtime::shutdown::ShutdownBudget::process();
        if budget.is_stopping() {
            let _ = self.stop_within(&budget);
        } else {
            self.request_stop();
            if let Some(handle) = lock(&self.thread).take() {
                let _ = handle.join();
            }
        }
    }
}

/// The service's deferral check: the first reason a due run must wait.
///
/// The window controller's import guard answers an open, pending or closing
/// window and an unsynced hand-back; an exclusive try-lock on the window lock
/// answers an import, which holds it shared for its whole run. `backup_running`
/// and `not_ready` come from the backup schedule and the recovery gate.
pub fn service_deferral(
    window: Arc<crate::collection::window::WindowController>,
    data_dir: PathBuf,
    backup_running: Box<dyn Fn() -> bool + Send + Sync>,
    not_ready: Box<dyn Fn() -> bool + Send + Sync>,
) -> DeferralCheck {
    use crate::collection::window::{FileLock, LockMode, Refusal};
    Box::new(move || {
        if not_ready() {
            return Some(Deferral::NotReady);
        }
        match window.import_guard() {
            Ok(guard) => drop(guard),
            Err(Refusal::HandbackPending) => return Some(Deferral::HandBack),
            Err(Refusal::WindowActive) => return Some(Deferral::Window),
            Err(_) => return Some(Deferral::NotReady),
        }
        let lock_path = crate::runtime::operator::window_dir(&data_dir).join("window.lock");
        match FileLock::acquire(&lock_path, LockMode::Exclusive, false) {
            Ok(Some(_probe)) => {}
            Ok(None) => return Some(Deferral::Import),
            Err(_) => return Some(Deferral::NotReady),
        }
        backup_running().then_some(Deferral::Backup)
    })
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
        let key_path = crate::legacy_owner_credentials::cursor_key_path(&self.data_dir);
        if std::fs::symlink_metadata(key_path).is_err() {
            return PassOutcome::Deferred(Deferral::NotReady);
        }
        let cursor_key = match crate::legacy_owner_credentials::load_or_create_cursor_key(&self.data_dir) {
            Ok(key) => key,
            Err(error) => return failed("cursor_key", &error),
        };
        let gate = match self.store.try_acquire_publication_gate() {
            Ok(gate) => gate,
            Err(StoreError::PublicationGateBusy) => return PassOutcome::Deferred(Deferral::HandBack),
            Err(error) => return failed("store", &error),
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
