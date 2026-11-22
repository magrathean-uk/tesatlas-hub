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

fn schedule_on(store: &HubStore, script: &Arc<Script>, hold: &Arc<Hold>) -> RepairScheduler {
    let held = Arc::clone(hold);
    RepairScheduler::start_with(
        store.clone(),
        script.clone(),
        Box::new(move || *held.0.lock().unwrap()),
        None,
    )
    .expect("scheduler")
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
    let schedule = RepairScheduler::start_with(
        store.clone(),
        Arc::new(StoreRepairRunner::new(store.clone(), data)),
        Box::new(|| None),
        None,
    )
    .expect("scheduler");
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
    let schedule = RepairScheduler::start_with(
        store.clone(),
        Arc::new(SwitchesOff { store: store.clone() }),
        Box::new(|| None),
        None,
    )
    .expect("scheduler");
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
    assert!(!record_outcome(&mut record, &PassOutcome::Deferred(Deferral::Backup), T0));
    assert_eq!(record, DriveRepairSchedule::default());
    assert!(settle_clock(&mut record, T0), "the first look sets the mark");
    assert!(!settle_clock(&mut record, T0 + HOUR - 1), "the mark moves by the hour");
    assert!(settle_clock(&mut record, T0 + HOUR));
    assert_eq!(due_at_ms(&record, T0 + 5), T0 + 5, "no recorded run: due at once");
}
