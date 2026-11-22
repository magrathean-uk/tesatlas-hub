// SPDX-License-Identifier: AGPL-3.0-only

// M03-T28a: the monthly repair schedule, driven by an injected clock (every
// tick names its time) and a scripted pass runner, in temporary stores with
// synthetic data. One test runs the service's own runner on a store with
// nothing planned. No network, no vehicle traffic.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::*;
use crate::import::broken_drive_repair::BrokenDriveCandidateIndex;

const HOUR: i64 = MS_PER_HOUR;
const DAY: i64 = MS_PER_DAY;
const T0: i64 = 1_800_000_000_000;

/// Answers from a script (a completed pass with no change when it is empty)
/// and counts the passes.
#[derive(Default)]
struct Script {
    outcomes: Mutex<VecDeque<PassOutcome>>,
    passes: AtomicUsize,
    at: Mutex<Vec<(Uuid, i64)>>,
}

impl Script {
    fn with(outcomes: Vec<PassOutcome>) -> Arc<Self> {
        Arc::new(Self { outcomes: Mutex::new(outcomes.into()), ..Self::default() })
    }

    fn count(&self) -> usize {
        self.passes.load(Ordering::SeqCst)
    }
}

impl RepairPassRunner for Script {
    fn run_pass(&self, vehicle: Uuid, now_ms: i64) -> PassOutcome {
        self.passes.fetch_add(1, Ordering::SeqCst);
        self.at.lock().unwrap().push((vehicle, now_ms));
        self.outcomes.lock().unwrap().pop_front().unwrap_or(PassOutcome::NoChange)
    }
}

/// What a test sets to hold due runs back.
#[derive(Default)]
struct Hold(Mutex<Option<Deferral>>);

impl Hold {
    fn set(&self, reason: Option<Deferral>) {
        *self.0.lock().unwrap() = reason;
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: HubStore,
    vehicle: Uuid,
    script: Arc<Script>,
    hold: Arc<Hold>,
    schedule: RepairScheduler,
}

fn index_for(store: &HubStore, vehicle: Uuid) {
    store
        .record_drive_repair_index(
            Uuid::new_v4(),
            vehicle,
            &BrokenDriveCandidateIndex { car_id: 1, rows: Vec::new() },
            T0,
        )
        .expect("index");
}

/// Admits every pass and holds nothing.
fn free() -> DeferralCheck {
    Box::new(|| Ok(PassHold::none()))
}

/// A ready scheduler without a timer, on the wall clock.
fn ready(store: &HubStore, runner: Arc<dyn RepairPassRunner>, deferral: DeferralCheck) -> RepairScheduler {
    let schedule =
        RepairScheduler::start_with(store.clone(), runner, deferral, None, Arc::new(wall_ms)).expect("scheduler");
    schedule.mark_ready();
    schedule
}

fn schedule_on(store: &HubStore, script: &Arc<Script>, hold: &Arc<Hold>) -> RepairScheduler {
    let held = Arc::clone(hold);
    ready(
        store,
        script.clone(),
        Box::new(move || match *held.0.lock().unwrap() {
            Some(reason) => Err(reason),
            None => Ok(PassHold::none()),
        }),
    )
}

/// A store whose vehicle has completed an import (its index is recorded).
fn fixture(outcomes: Vec<PassOutcome>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = HubStore::initialize(dir.path().join("data")).expect("store");
    let vehicle = Uuid::new_v4();
    index_for(&store, vehicle);
    let script = Script::with(outcomes);
    let hold = Arc::new(Hold::default());
    let schedule = schedule_on(&store, &script, &hold);
    Fixture { _dir: dir, store, vehicle, script, hold, schedule }
}

impl Fixture {
    fn on(&self) {
        self.store.set_drive_repair_automatic(self.vehicle, true).expect("on");
    }

    fn record(&self) -> DriveRepairSchedule {
        self.store.drive_repair_schedule(self.vehicle).expect("record")
    }

    fn step(&self, now_ms: i64) -> VehicleStep {
        let tick = self.schedule.tick(now_ms);
        tick.steps
            .into_iter()
            .find(|(vehicle, _)| *vehicle == self.vehicle)
            .map(|(_, step)| step)
            .expect("the indexed vehicle is stepped")
    }

    fn ran(step: &VehicleStep) -> bool {
        matches!(step, VehicleStep::Ran(_))
    }
}

fn metadata_has_schedule(store: &HubStore, vehicle: Uuid) -> bool {
    let connection = store.open().expect("connection");
    connection
        .query_row(
            "SELECT COUNT(*) FROM hub_metadata WHERE key = ?1",
            [format!("drive_repair_schedule:{vehicle}")],
            |row| row.get::<_, i64>(0),
        )
        .expect("count")
        > 0
}

#[test]
fn broken_drive_repair_schedule_is_off_until_a_stored_on_and_then_honours_the_stored_value() {
    let fixture = fixture(Vec::new());
    // No record at all: off, whatever the time, and nothing is written.
    for months in [0, 1, 5, 40] {
        assert_eq!(fixture.step(T0 + months * 31 * DAY), VehicleStep::Off);
    }
    assert_eq!(fixture.script.count(), 0);
    assert!(!metadata_has_schedule(&fixture.store, fixture.vehicle));
    assert_eq!(fixture.record(), DriveRepairSchedule::default());
    // Stored off: still nothing.
    fixture.store.set_drive_repair_automatic(fixture.vehicle, false).unwrap();
    assert_eq!(fixture.step(T0 + 100 * DAY), VehicleStep::Off);
    assert_eq!(fixture.script.count(), 0);
    // Stored on: the first run is due at once.
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0 + 101 * DAY)));
    assert_eq!(fixture.script.count(), 1);
    // Switched off again: the next due time passes without a run.
    fixture.store.set_drive_repair_automatic(fixture.vehicle, false).unwrap();
    assert_eq!(fixture.step(T0 + 101 * DAY + INTERVAL_MS + 5 * DAY), VehicleStep::Off);
    assert_eq!(fixture.script.count(), 1);
}

#[test]
fn broken_drive_repair_schedule_first_run_is_due_after_the_first_completed_import() {
    let dir = tempfile::tempdir().unwrap();
    let store = HubStore::initialize(dir.path().join("data")).expect("store");
    let vehicle = Uuid::new_v4();
    store.set_drive_repair_automatic(vehicle, true).unwrap();
    let script = Script::with(Vec::new());
    let schedule = schedule_on(&store, &script, &Arc::new(Hold::default()));
    // On, but no import has recorded an index: no vehicle is scheduled.
    assert!(schedule.tick(T0).steps.is_empty());
    assert_eq!(script.count(), 0);
    // The import completes: the very next look runs the first pass.
    index_for(&store, vehicle);
    let tick = schedule.tick(T0 + HOUR);
    assert_eq!(tick.steps, vec![(vehicle, VehicleStep::Ran(PassOutcome::NoChange))]);
    assert_eq!(script.count(), 1);
    assert_eq!(store.drive_repair_schedule(vehicle).unwrap().next_run_ms, Some(T0 + HOUR + INTERVAL_MS));
}

#[test]
fn broken_drive_repair_schedule_runs_every_thirty_days() {
    let fixture = fixture(Vec::new());
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0)));
    assert_eq!(fixture.record().next_run_ms, Some(T0 + 30 * DAY));
    assert_eq!(fixture.step(T0 + 30 * DAY - 1), VehicleStep::NotDue { wait_ms: 1 });
    assert_eq!(fixture.step(T0 + DAY), VehicleStep::NotDue { wait_ms: 29 * DAY });
    assert_eq!(fixture.script.count(), 1);
    assert!(Fixture::ran(&fixture.step(T0 + 30 * DAY)));
    assert_eq!(fixture.record().next_run_ms, Some(T0 + 60 * DAY));
    assert_eq!(fixture.step(T0 + 60 * DAY - 1), VehicleStep::NotDue { wait_ms: 1 });
    assert_eq!(fixture.script.count(), 2);
}

#[test]
fn broken_drive_repair_schedule_catches_up_once_after_a_wake_or_forward_jump() {
    let fixture = fixture(Vec::new());
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0)));
    // The Mac sleeps through four months: one catch-up pass, not four.
    let woke = T0 + 120 * DAY;
    assert!(Fixture::ran(&fixture.step(woke)));
    assert_eq!(fixture.script.count(), 2);
    assert_eq!(fixture.record().next_run_ms, Some(woke + 30 * DAY));
    assert_eq!(fixture.step(woke + 1), VehicleStep::NotDue { wait_ms: 30 * DAY - 1 });
    assert_eq!(fixture.step(woke + 29 * DAY), VehicleStep::NotDue { wait_ms: DAY });
    assert_eq!(fixture.script.count(), 2);
}

#[test]
fn broken_drive_repair_schedule_a_backward_jump_never_runs_early_and_reanchors_to_the_high_water_mark() {
    let fixture = fixture(Vec::new());
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0)));
    // A forward jump of a year makes the next pass due; it runs once.
    let wrong = T0 + 365 * DAY;
    assert!(Fixture::ran(&fixture.step(wrong)));
    assert_eq!(fixture.record().high_water_ms, wrong);
    assert_eq!(fixture.record().next_run_ms, Some(wrong + 30 * DAY));
    // The clock is corrected: no run, the record is re-anchored to the
    // present and the next pass is at most one interval away.
    let now = T0 + 45 * DAY;
    assert_eq!(fixture.step(now), VehicleStep::NotDue { wait_ms: 30 * DAY });
    let record = fixture.record();
    assert_eq!(record.high_water_ms, now);
    assert_eq!(record.last_run_ms, Some(now.min(wrong)));
    assert_eq!(record.next_run_ms, Some(now + 30 * DAY));
    assert_eq!(fixture.script.count(), 2);
    assert_eq!(fixture.step(now + 30 * DAY - 1), VehicleStep::NotDue { wait_ms: 1 });
    assert!(Fixture::ran(&fixture.step(now + 30 * DAY)));
    assert_eq!(fixture.script.count(), 3);
}

#[test]
fn broken_drive_repair_schedule_a_small_clock_correction_writes_and_changes_nothing() {
    let fixture = fixture(Vec::new());
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0)));
    let before = fixture.record();
    assert_eq!(fixture.step(T0 - 30_000), VehicleStep::NotDue { wait_ms: 30 * DAY + 30_000 });
    assert_eq!(fixture.record(), before);
}

#[test]
fn broken_drive_repair_schedule_defers_for_an_import_hand_back_backup_window_or_unready_service() {
    for reason in [
        Deferral::Import,
        Deferral::HandBack,
        Deferral::Backup,
        Deferral::Window,
        Deferral::NotReady,
    ] {
        let fixture = fixture(Vec::new());
        fixture.on();
        fixture.hold.set(Some(reason));
        let tick = fixture.schedule.tick(T0);
        assert_eq!(tick.steps, vec![(fixture.vehicle, VehicleStep::Deferred(reason))], "{reason:?}");
        assert_eq!(tick.wait_ms, DEFER_RECHECK_MS);
        assert_eq!(fixture.script.count(), 0, "{reason:?}");
        // A deferral is not a failure and moves nothing: no failure, no
        // backoff, no last run, no next run.
        let record = fixture.record();
        assert_eq!((record.failures, record.last_run_ms, record.next_run_ms), (0, None, None));
        assert_eq!(record.last_failure, None);
        // Still deferred a day later; then it clears and the run happens.
        assert_eq!(fixture.step(T0 + DAY), VehicleStep::Deferred(reason));
        assert_eq!(fixture.script.count(), 0);
        fixture.hold.set(None);
        assert!(Fixture::ran(&fixture.step(T0 + DAY + DEFER_RECHECK_MS)));
        assert_eq!(fixture.script.count(), 1);
    }
}

#[test]
fn broken_drive_repair_schedule_defers_a_pass_the_runner_cannot_start_and_records_nothing() {
    let fixture = fixture(vec![PassOutcome::Deferred(Deferral::Import), PassOutcome::Deferred(Deferral::HandBack)]);
    fixture.on();
    assert_eq!(fixture.step(T0), VehicleStep::Deferred(Deferral::Import));
    assert_eq!(fixture.step(T0 + 10), VehicleStep::Deferred(Deferral::HandBack));
    let record = fixture.record();
    assert_eq!((record.failures, record.last_run_ms, record.next_run_ms), (0, None, None));
    assert!(Fixture::ran(&fixture.step(T0 + 20)));
}

#[test]
fn broken_drive_repair_schedule_backs_off_one_hour_six_hours_then_daily() {
    let failed = || PassOutcome::Failed("refused".to_owned());
    let fixture = fixture(vec![failed(), failed(), failed(), failed(), PassOutcome::NoChange]);
    fixture.on();
    let mut now = T0;
    for (index, wait) in [HOUR, 6 * HOUR, DAY, DAY].into_iter().enumerate() {
        assert_eq!(fixture.step(now), VehicleStep::Ran(failed()), "failure {}", index + 1);
        let record = fixture.record();
        assert_eq!(record.failures, u32::try_from(index + 1).unwrap());
        assert_eq!(record.next_run_ms, Some(now + wait));
        assert_eq!(record.last_failure.as_deref(), Some("refused"));
        assert_eq!(record.last_run_ms, None, "a failed pass is not a completed one");
        assert_eq!(record.last_attempt_ms, Some(now));
        assert_eq!(fixture.step(now + wait - 1), VehicleStep::NotDue { wait_ms: 1 });
        now += wait;
    }
    assert_eq!(fixture.script.count(), 4);
    // A pass that completes clears the backoff and waits the full month.
    assert_eq!(fixture.step(now), VehicleStep::Ran(PassOutcome::NoChange));
    let record = fixture.record();
    assert_eq!((record.failures, record.last_failure.clone()), (0, None));
    assert_eq!(record.next_run_ms, Some(now + 30 * DAY));
    assert_eq!(record.last_run_ms, Some(now));
}

#[test]
fn broken_drive_repair_schedule_a_pass_with_no_change_persists_last_and_next_and_publishes_nothing() {
    // The service's own runner, with a cursor key and nothing planned.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = HubStore::initialize(&data).expect("store");
    crate::legacy_owner_credentials::load_or_create_cursor_key(&data).expect("cursor key");
    let vehicle = Uuid::new_v4();
    index_for(&store, vehicle);
    store.set_drive_repair_automatic(vehicle, true).unwrap();
    let count = |sql: &str| -> i64 {
        store.open().unwrap().query_row(sql, [], |row| row.get(0)).unwrap()
    };
    let published = |name: &str| count(&format!("SELECT COUNT(*) FROM hub_metadata WHERE key LIKE '{name}%'"));
    let before = (
        count("SELECT COUNT(*) FROM sync_mutations"),
        published("physical_v3_"),
        published("drive_repair_layer:"),
        store.drive_repair_summary(vehicle).unwrap(),
    );
    let schedule = ready(&store, Arc::new(StoreRepairRunner::new(store.clone(), data)), free());
    let tick = schedule.tick(T0);
    assert_eq!(tick.steps, vec![(vehicle, VehicleStep::Ran(PassOutcome::NoChange))]);
    let record = store.drive_repair_schedule(vehicle).unwrap();
    assert_eq!(record.last_run_ms, Some(T0));
    assert_eq!(record.next_run_ms, Some(T0 + 30 * DAY));
    assert_eq!(record.last_outcome.as_deref(), Some("no_change"));
    assert_eq!(record.automatic, Some(true));
    let after = (
        count("SELECT COUNT(*) FROM sync_mutations"),
        published("physical_v3_"),
        published("drive_repair_layer:"),
        store.drive_repair_summary(vehicle).unwrap(),
    );
    assert_eq!(before, after, "no row, journal entry, head or layer changed");
    // A second look inside the month does nothing at all.
    assert_eq!(schedule.tick(T0 + DAY).steps, vec![(vehicle, VehicleStep::NotDue { wait_ms: 29 * DAY })]);
}

#[test]
fn broken_drive_repair_schedule_the_service_runner_waits_for_the_cursor_key() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = HubStore::initialize(&data).expect("store");
    let vehicle = Uuid::new_v4();
    index_for(&store, vehicle);
    let runner = StoreRepairRunner::new(store.clone(), data.clone());
    assert_eq!(runner.run_pass(vehicle, T0), PassOutcome::Deferred(Deferral::NotReady));
    assert!(!crate::legacy_owner_credentials::cursor_key_path(&data).exists(), "no key is created");
}

#[test]
fn broken_drive_repair_schedule_the_stored_on_off_is_not_overwritten_by_a_pass() {
    // The operator switches off while a pass runs: the pass's record write
    // keeps the new value.
    struct SwitchesOff {
        store: HubStore,
    }
    impl RepairPassRunner for SwitchesOff {
        fn run_pass(&self, vehicle: Uuid, _now_ms: i64) -> PassOutcome {
            self.store.set_drive_repair_automatic(vehicle, false).expect("off");
            PassOutcome::NoChange
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let store = HubStore::initialize(dir.path().join("data")).expect("store");
    let vehicle = Uuid::new_v4();
    index_for(&store, vehicle);
    store.set_drive_repair_automatic(vehicle, true).unwrap();
    let schedule = ready(&store, Arc::new(SwitchesOff { store: store.clone() }), free());
    assert!(Fixture::ran(&schedule.tick(T0).steps[0].1));
    let record = store.drive_repair_schedule(vehicle).unwrap();
    assert_eq!(record.automatic, Some(false));
    assert_eq!(record.next_run_ms, Some(T0 + 30 * DAY));
}

#[test]
fn broken_drive_repair_schedule_a_restart_repeats_nothing_that_is_not_due_and_skips_nothing_overdue() {
    let fixture = fixture(Vec::new());
    fixture.on();
    assert!(Fixture::ran(&fixture.step(T0)));
    // A second scheduler on the same store is the restarted service.
    let script = Script::with(Vec::new());
    let restarted = schedule_on(&fixture.store, &script, &fixture.hold);
    let tick = restarted.tick(T0 + 10 * DAY);
    assert_eq!(tick.steps, vec![(fixture.vehicle, VehicleStep::NotDue { wait_ms: 20 * DAY })]);
    assert_eq!(script.count(), 0);
    assert!(Fixture::ran(&restarted.tick(T0 + 31 * DAY).steps[0].1));
    assert_eq!(script.count(), 1);
}

#[test]
fn broken_drive_repair_schedule_a_manual_check_ignores_the_switch_and_due_time_but_not_a_deferral() {
    let fixture = fixture(Vec::new());
    // Off, never run: a manual check still runs and records its times.
    assert_eq!(fixture.schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Ran(PassOutcome::NoChange));
    let record = fixture.record();
    assert_eq!(record.automatic, None, "a manual check does not switch automatic runs on");
    assert!(record.last_run_ms.is_some() && record.next_run_ms.is_some());
    assert_eq!(fixture.script.count(), 1);
    // Not yet due: a manual check runs anyway; a deferral holds it back.
    assert!(Fixture::ran(&fixture.schedule.run_now(fixture.vehicle).unwrap()));
    fixture.hold.set(Some(Deferral::Window));
    assert_eq!(fixture.schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Deferred(Deferral::Window));
    assert_eq!(fixture.script.count(), 2);
}

#[test]
fn broken_drive_repair_schedule_schedules_each_vehicle_on_its_own_record() {
    let fixture = fixture(Vec::new());
    let other = Uuid::new_v4();
    index_for(&fixture.store, other);
    fixture.on();
    let tick = fixture.schedule.tick(T0);
    let mut steps = tick.steps;
    steps.sort_by_key(|(vehicle, _)| *vehicle);
    let mut expected = vec![
        (fixture.vehicle, VehicleStep::Ran(PassOutcome::NoChange)),
        (other, VehicleStep::Off),
    ];
    expected.sort_by_key(|(vehicle, _)| *vehicle);
    assert_eq!(steps, expected);
    assert!(!metadata_has_schedule(&fixture.store, other));
}

#[test]
fn broken_drive_repair_schedule_pure_rules_are_exact() {
    assert_eq!([backoff_ms(1), backoff_ms(2), backoff_ms(3), backoff_ms(4), backoff_ms(900)], [HOUR, 6 * HOUR, DAY, DAY, DAY]);
    assert_eq!(backoff_ms(0), HOUR);
    let mut record = DriveRepairSchedule::default();
    assert!(!record_outcome(&mut record, &PassOutcome::Deferred(Deferral::Backup), T0, T0));
    assert_eq!(record, DriveRepairSchedule::default());
    assert!(settle_clock(&mut record, T0), "the first look sets the mark");
    assert!(!settle_clock(&mut record, T0 + HOUR - 1), "the mark moves by the hour");
    assert!(settle_clock(&mut record, T0 + HOUR));
    assert_eq!(due_at_ms(&record, T0 + 5), T0 + 5, "no recorded run: due at once");
}

// ---------------------------------------------------------------------------
// REV-CX-T28 repair round: one test per finding, named after it.
// ---------------------------------------------------------------------------

/// A test clock the runner can move on.
fn test_clock(start: i64) -> (Arc<std::sync::atomic::AtomicI64>, Clock) {
    let now = Arc::new(std::sync::atomic::AtomicI64::new(start));
    let read = Arc::clone(&now);
    (now, Arc::new(move || read.load(Ordering::SeqCst)))
}

fn data_dir_of(store: &HubStore) -> std::path::PathBuf {
    store.database_path().parent().expect("data dir").to_path_buf()
}

/// P2 "Repair passes release the window lock and only sample backup state":
/// the window lock and the maintenance turn are held for the whole pass and
/// released after it; a held turn or window lock defers the pass.
#[test]
fn t28_fix_repair_passes_hold_the_window_lock_and_the_backup_turn() {
    use crate::collection::window::{FileLock, LockMode};
    struct Probe {
        lock_path: std::path::PathBuf,
        maintenance: MaintenanceExclusion,
        seen: Mutex<Option<(bool, bool)>>,
    }
    impl RepairPassRunner for Probe {
        fn run_pass(&self, _: Uuid, _: i64) -> PassOutcome {
            let window_free = FileLock::acquire(&self.lock_path, LockMode::Exclusive, false)
                .expect("lock")
                .is_some();
            let turn_free = self.maintenance.try_take().is_some();
            *self.seen.lock().unwrap() = Some((window_free, turn_free));
            PassOutcome::NoChange
        }
    }
    let fixture = fixture(Vec::new());
    let data = data_dir_of(&fixture.store);
    let window = crate::runtime::operator::window_dir(&data);
    std::fs::create_dir_all(&window).expect("window dir");
    std::fs::set_permissions(&window, <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700)).expect("mode");
    let lock_path = window.join("window.lock");
    let maintenance = MaintenanceExclusion::default();
    let probe = Arc::new(Probe { lock_path: lock_path.clone(), maintenance: maintenance.clone(), seen: Mutex::default() });
    let admitted = maintenance.clone();
    let schedule = ready(
        &fixture.store,
        probe.clone(),
        Box::new(move || hold_window_and_turn(&data, &admitted)),
    );
    assert_eq!(schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Ran(PassOutcome::NoChange));
    assert_eq!(*probe.seen.lock().unwrap(), Some((false, false)), "no window and no backup can start mid-pass");
    assert!(FileLock::acquire(&lock_path, LockMode::Exclusive, false).unwrap().is_some(), "released after");
    assert!(maintenance.try_take().is_some(), "released after");
    let backup = maintenance.take();
    assert_eq!(schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Deferred(Deferral::Backup));
    drop(backup);
    let import = FileLock::acquire(&lock_path, LockMode::Shared, false).unwrap().expect("shared lock");
    assert_eq!(schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Deferred(Deferral::Import));
    drop(import);
    // A backup waits for the turn a pass holds, and starts once it is free.
    let pass = maintenance.try_take().expect("turn");
    let waiter = {
        let maintenance = maintenance.clone();
        std::thread::spawn(move || drop(maintenance.take()))
    };
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!waiter.is_finished(), "the backup waits for the pass");
    drop(pass);
    waiter.join().expect("backup started");
}

/// P2 "A closed ledger can admit repairs before hand-back reaches the
/// catalogue": a hand-back the store's coverage lacks is a hand-back
/// deferral, not a failed pass.
#[test]
fn t28_fix_a_closed_ledger_waits_for_the_hand_back_to_reach_the_catalogue() {
    let fixture = fixture(Vec::new());
    fixture.on();
    crate::db::mark_coverage_unreconciled(&data_dir_of(&fixture.store), 41);
    assert_eq!(fixture.step(T0), VehicleStep::Deferred(Deferral::HandBack));
    assert_eq!(fixture.script.count(), 0);
    let record = fixture.record();
    assert_eq!((record.failures, record.last_attempt_ms, record.next_run_ms), (0, None, None));
    assert_eq!(fixture.schedule.run_exclusive(|| ()), Err(Deferral::HandBack));
}

/// P2 "A repair can enter publication with an import pending for another
/// vehicle": any vehicle's pending import publication defers every pass and
/// every exclusive job.
#[test]
fn t28_fix_an_import_pending_for_another_vehicle_defers_the_pass() {
    let fixture = fixture(Vec::new());
    fixture.on();
    let other = Uuid::new_v4();
    fixture
        .store
        .open()
        .unwrap()
        .execute(
            "INSERT INTO hub_metadata(key, value) VALUES (?1, '{}')",
            [format!("import_publication_intent:{other}")],
        )
        .expect("intent");
    assert_eq!(fixture.step(T0), VehicleStep::Deferred(Deferral::Import));
    assert_eq!(fixture.schedule.run_now(fixture.vehicle).unwrap(), VehicleStep::Deferred(Deferral::Import));
    assert_eq!(fixture.schedule.run_exclusive(|| ()), Err(Deferral::Import));
    assert_eq!(fixture.script.count(), 0);
}

/// P2 "The timer can repeat a completed manual pass or start after Off": a
/// Check now arriving while the timer admits its pass is busy, so one pass
/// runs; an Off acknowledged there stops the timer's pass.
#[test]
fn t28_fix_the_timer_never_repeats_a_manual_pass_or_starts_after_off() {
    use std::sync::{OnceLock, atomic::AtomicBool};
    for off in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = HubStore::initialize(dir.path().join("data")).expect("store");
        let vehicle = Uuid::new_v4();
        index_for(&store, vehicle);
        store.set_drive_repair_automatic(vehicle, true).unwrap();
        let script = Script::with(Vec::new());
        let cell: Arc<OnceLock<Arc<RepairScheduler>>> = Arc::default();
        let inner: Arc<Mutex<Option<VehicleStep>>> = Arc::default();
        let fired = Arc::new(AtomicBool::new(false));
        let (cell_in, inner_in, fired_in) = (Arc::clone(&cell), Arc::clone(&inner), Arc::clone(&fired));
        let schedule = Arc::new(ready(
            &store,
            script.clone(),
            Box::new(move || {
                if !fired_in.swap(true, Ordering::SeqCst)
                    && let Some(schedule) = cell_in.get()
                {
                    if off {
                        schedule.set_automatic(vehicle, false).expect("off");
                    } else {
                        *inner_in.lock().unwrap() = Some(schedule.run_now(vehicle).expect("check now"));
                    }
                }
                Ok(PassHold::none())
            }),
        ));
        assert!(cell.set(Arc::clone(&schedule)).is_ok());
        let step = schedule.tick(T0).steps.remove(0).1;
        if off {
            assert_eq!(step, VehicleStep::Off, "an acknowledged Off stops the pass");
            assert_eq!(script.count(), 0);
            assert_eq!(store.drive_repair_schedule(vehicle).unwrap().automatic, Some(false));
        } else {
            assert_eq!(*inner.lock().unwrap(), Some(VehicleStep::Deferred(Deferral::Busy)));
            assert_eq!(step, VehicleStep::Ran(PassOutcome::NoChange));
            assert_eq!(script.count(), 1, "one pass, not two");
            let record = store.drive_repair_schedule(vehicle).unwrap();
            assert_eq!(record.next_run_ms, Some(T0 + INTERVAL_MS));
        }
    }
}

/// P2 "Clock rollback and long passes distort the retry interval": a jump
/// back keeps a failed pass's retry, and a long pass is scheduled from its
/// end; each vehicle starts on the clock, not the tick's first sample.
#[test]
fn t28_fix_clock_rollback_keeps_the_retry_and_long_passes_count_from_their_end() {
    let year_ahead = T0 + 365 * DAY;
    let mut record = DriveRepairSchedule {
        high_water_ms: year_ahead,
        last_attempt_ms: Some(year_ahead),
        next_run_ms: Some(year_ahead + HOUR),
        failures: 1,
        last_failure: Some("refused".to_owned()),
        ..DriveRepairSchedule::default()
    };
    assert!(settle_clock(&mut record, T0));
    assert_eq!(record.next_run_ms, Some(T0 + HOUR), "a one-hour retry stays one hour");

    /// Each pass takes two hours; the first fails.
    struct Slow {
        clock: Arc<std::sync::atomic::AtomicI64>,
        script: Arc<Script>,
    }
    impl RepairPassRunner for Slow {
        fn run_pass(&self, vehicle: Uuid, now_ms: i64) -> PassOutcome {
            self.clock.fetch_add(2 * HOUR, Ordering::SeqCst);
            self.script.run_pass(vehicle, now_ms)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let store = HubStore::initialize(dir.path().join("data")).expect("store");
    let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
    for vehicle in [first, second] {
        index_for(&store, vehicle);
        store.set_drive_repair_automatic(vehicle, true).unwrap();
    }
    let (now, clock) = test_clock(T0);
    let script = Script::with(vec![PassOutcome::Failed("refused".to_owned())]);
    let schedule = RepairScheduler::start_with(
        store.clone(),
        Arc::new(Slow { clock: Arc::clone(&now), script: script.clone() }),
        free(),
        None,
        clock,
    )
    .expect("scheduler");
    schedule.mark_ready();
    schedule.tick_on_clock();
    let failed = store.drive_repair_schedule(first).unwrap();
    assert_eq!(failed.last_attempt_ms, Some(T0));
    assert_eq!(failed.next_run_ms, Some(T0 + 2 * HOUR + HOUR), "the retry waits an hour after the pass ended");
    let completed = store.drive_repair_schedule(second).unwrap();
    assert_eq!(completed.last_run_ms, Some(T0 + 2 * HOUR), "the second vehicle starts when the first ends");
    assert_eq!(completed.next_run_ms, Some(T0 + 4 * HOUR + INTERVAL_MS));
    assert_eq!(*script.at.lock().unwrap(), vec![(first, T0), (second, T0 + 2 * HOUR)]);
}

/// P2 "Failed schedule persistence can replay completed work": the attempt
/// is on record before the pass, so an outcome the store refuses is neither
/// replayed at once (here or after a restart) nor loses its backoff; this
/// service writes it before the vehicle's next step.
#[test]
fn t28_fix_failed_schedule_persistence_neither_replays_nor_loses_the_backoff() {
    let fixture = fixture(vec![PassOutcome::Repaired { batch_id: 3 }]);
    fixture.on();
    fixture.schedule.shared.refuse_outcome_writes.store(1, Ordering::SeqCst);
    assert_eq!(fixture.step(T0), VehicleStep::Ran(PassOutcome::Repaired { batch_id: 3 }));
    let record = fixture.record();
    assert_eq!(record.next_run_ms, Some(T0 + HOUR), "the attempt's backoff is on record");
    assert_eq!((record.failures, record.last_failure.as_deref()), (1, Some(INTERRUPTED)));
    let script = Script::with(Vec::new());
    let restarted = schedule_on(&fixture.store, &script, &fixture.hold);
    assert_eq!(
        restarted.tick(T0 + 1).steps,
        vec![(fixture.vehicle, VehicleStep::NotDue { wait_ms: HOUR - 1 })],
        "a restart does not replay the pass at once"
    );
    assert_eq!(fixture.step(T0 + 2), VehicleStep::NotDue { wait_ms: INTERVAL_MS - 2 });
    let record = fixture.record();
    assert_eq!((record.last_run_ms, record.failures, record.last_failure.clone()), (Some(T0), 0, None));
    assert_eq!(record.last_outcome.as_deref(), Some("repaired"));
    assert_eq!(fixture.script.count() + script.count(), 1);
}

/// P2 "Accepted manual repair jobs can outlive shutdown": the stop joins
/// every manual job, and once stopping no job or exclusive pass starts.
#[test]
fn t28_fix_manual_jobs_are_joined_at_stop_and_none_starts_after_it() {
    let fixture = fixture(Vec::new());
    let (release, released) = std::sync::mpsc::channel::<()>();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = Arc::clone(&finished);
    fixture
        .schedule
        .spawn_job(move || {
            let _ = released.recv();
            done.store(true, Ordering::SeqCst);
        })
        .expect("job");
    let schedule = Arc::new(fixture.schedule);
    let stopping = Arc::clone(&schedule);
    let stop = std::thread::spawn(move || {
        let budget = crate::runtime::shutdown::ShutdownBudget::system();
        budget.begin();
        stopping.stop_within(&budget)
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(!stop.is_finished(), "the stop waits for the accepted job");
    release.send(()).expect("release");
    assert!(stop.join().expect("stop").is_ok());
    assert!(finished.load(Ordering::SeqCst));
    assert!(schedule.spawn_job(|| ()).is_err(), "no job starts once stopping");
    let mut ran = false;
    assert_eq!(schedule.run_exclusive(|| ran = true), Err(Deferral::NotReady));
    assert!(!ran, "an accepted Undo whose worker runs after the stop does nothing");
}

/// P2 "Repair work can start before the API is ready": nothing runs until
/// the service marks the scheduler ready once its listener is bound.
#[test]
fn t28_fix_repair_work_waits_until_the_api_is_ready() {
    let dir = tempfile::tempdir().unwrap();
    let store = HubStore::initialize(dir.path().join("data")).expect("store");
    let vehicle = Uuid::new_v4();
    index_for(&store, vehicle);
    store.set_drive_repair_automatic(vehicle, true).unwrap();
    let script = Script::with(Vec::new());
    let schedule =
        RepairScheduler::start_with(store.clone(), script.clone(), free(), None, Arc::new(wall_ms)).expect("scheduler");
    assert_eq!(schedule.run_now(vehicle).unwrap(), VehicleStep::Deferred(Deferral::NotReady));
    assert_eq!(schedule.tick(T0).steps, vec![(vehicle, VehicleStep::Deferred(Deferral::NotReady))]);
    assert_eq!(schedule.run_exclusive(|| ()), Err(Deferral::NotReady));
    assert_eq!(script.count(), 0);
    schedule.mark_ready();
    assert_eq!(schedule.run_now(vehicle).unwrap(), VehicleStep::Ran(PassOutcome::NoChange));
}
