// SPDX-License-Identifier: AGPL-3.0-only

//! M03-T27c: a repair batch is activated, published, undone and compensated
//! end to end. Each step is checked through the packs a fresh and an
//! existing 1.4 client are served, against the Hub's own reads of its
//! materialised rows.

use rusqlite::OptionalExtension;

use super::*;
use crate::db::{DriveProtectionReason, DriveRepairEntity, DriveRepairTarget};
use crate::hub_pack::ProjectionFloat64BitsV2_2;
use crate::import::broken_drive_repair::{
    BrokenDriveCandidateIndex, GapEvidence, RepairInput, RepairSample, StateCoverage,
};

const NOW: i64 = T + 100 * HOUR;

/// One broken drive: fragments `first` and `first + 1` with one sample
/// each, a minute apart from `start`, then their destination `first + 2`.
#[derive(Debug, Clone, Copy)]
struct Run {
    first: i64,
    start: i64,
    odometer: f64,
}

const RUNS: [Run; 2] = [
    Run { first: 20, start: T, odometer: 100.0 },
    Run { first: 30, start: T + 2 * HOUR, odometer: 200.0 },
];

impl Run {
    fn fragments(self) -> [i64; 2] {
        [self.first, self.first + 1]
    }

    fn destination(self) -> i64 {
        self.first + 2
    }

    /// (position, parent, instant, odometer): the fragment samples, then
    /// the destination's first and last.
    fn samples(self) -> [(i64, i64, i64, f64); 4] {
        let d = self.destination();
        [
            (self.first * 100, self.first, self.start, self.odometer),
            ((self.first + 1) * 100, self.first + 1, self.start + MINUTE, self.odometer + 0.5),
            (d * 100, d, self.start + 2 * MINUTE, self.odometer + 1.5),
            (d * 100 + 1, d, self.start + 32 * MINUTE, self.odometer + 10.5),
        ]
    }

    fn end(self) -> i64 {
        self.start + 32 * MINUTE
    }
}

fn km(value: f64) -> Option<ProjectionFloat64BitsV2_2> {
    Some(ProjectionFloat64BitsV2_2::from_f64(value))
}

fn physical_drive(id: i64, start: i64, end: Option<(i64, f64)>) -> SourceDbDrivePhysicalV2_2 {
    SourceDbDrivePhysicalV2_2 {
        id: i32::try_from(id).expect("drive id"),
        car_id: 1,
        start_date_pg_us: pg_us(start),
        end_date_pg_us: end.map(|(end, _)| pg_us(end)),
        start_position_id: None,
        end_position_id: None,
        start_address_id: None,
        end_address_id: None,
        start_geofence_id: None,
        end_geofence_id: None,
        outside_temp_avg_e1: None,
        inside_temp_avg_e1: None,
        speed_max: end.map(|_| 50),
        power_max: None,
        power_min: None,
        start_ideal_range_km_e2: None,
        end_ideal_range_km_e2: None,
        start_rated_range_km_e2: None,
        end_rated_range_km_e2: None,
        start_km: end.and_then(|(_, start_km)| km(start_km)),
        end_km: end.and_then(|(_, start_km)| km(start_km + 9.0)),
        distance: end.and_then(|_| km(9.0)),
        duration_min: end.map(|(end, _)| i16::try_from((end - start) / MINUTE).expect("minutes")),
        ascent: None,
        descent: None,
    }
}

fn physical_position(id: i64, drive: i64, at: i64, odometer: f64) -> SourceDbPositionPhysicalV2_2 {
    SourceDbPositionPhysicalV2_2 {
        id: i32::try_from(id).expect("position id"),
        car_id: 1,
        drive_id: Some(i32::try_from(drive).expect("drive id")),
        date_pg_us: pg_us(at),
        latitude_e6: ProjectionFixedNumericV2_2::Finite(47_500_000),
        longitude_e6: ProjectionFixedNumericV2_2::Finite(19_000_000),
        elevation: None,
        speed: Some(40),
        power: None,
        odometer: km(odometer),
        ideal_battery_range_km_e2: None,
        est_battery_range_km_e2: None,
        rated_battery_range_km_e2: None,
        battery_level: None,
        usable_battery_level: None,
        battery_heater: None,
        battery_heater_on: None,
        battery_heater_no_power: None,
        outside_temp_e1: None,
        inside_temp_e1: None,
        fan_status: None,
        driver_temp_setting_e1: None,
        passenger_temp_setting_e1: None,
        is_climate_on: None,
        is_rear_defroster_on: None,
        is_front_defroster_on: None,
        tpms_pressure_fl_e1: None,
        tpms_pressure_fr_e1: None,
        tpms_pressure_rl_e1: None,
        tpms_pressure_rr_e1: None,
    }
}

fn materialised_destination(run: Run) -> ProjectionDrive {
    ProjectionDrive {
        id: run.destination(),
        car_id: CAR,
        optimized_at_ms: None,
        start_date_ms: run.start + 2 * MINUTE,
        end_date_ms: run.end(),
        distance_km: Some(9.0),
        duration_min: Some(30),
        efficiency: None,
        outside_temp_avg: None,
        inside_temp_avg: None,
        speed_max: Some(50),
        power_max: None,
        power_min: None,
        start_ideal_range_km: None,
        end_ideal_range_km: None,
        start_address: None,
        end_address: None,
        start_geofence: None,
        end_geofence: None,
        start_latitude: None,
        start_longitude: None,
        end_latitude: None,
        end_longitude: None,
        start_soc: None,
        end_soc: None,
        start_rated_range_km: None,
        end_rated_range_km: None,
        ascent: None,
        descent: None,
    }
}

fn materialised_sample(id: i64, drive: Option<i64>, at: i64, odometer: f64) -> ProjectionPosition {
    serde_json::from_value(serde_json::json!({
        "id": id, "drive_id": drive, "car_id": CAR, "date_ms": at,
        "latitude": 47.5, "longitude": 19.0, "speed": 40, "odometer": odometer
    }))
    .expect("materialised sample")
}

fn sample(id: i64, at: i64, odometer: f64) -> RepairSample {
    RepairSample {
        position_id: id,
        date_ms: at,
        odometer_km: Some(odometer),
        speed_kmh: Some(40),
    }
}

/// The candidate index rows of one run, as an import captures them.
fn index_rows(run: Run) -> Vec<RepairInput> {
    let samples = run.samples();
    let mut rows: Vec<RepairInput> = run
        .fragments()
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let (position, _, at, odometer) = samples[i];
            let s = sample(position, at, odometer);
            RepairInput {
                drive_id: id,
                car_id: CAR,
                start_ms: at,
                end_ms: None,
                distance_km: None,
                duration_min: None,
                sample_count: 1,
                first_sample: Some(s),
                last_sample: Some(s),
                previous_drive_id: Some(id - 1),
                previous_unclosed: i > 0,
                next_drive_id: Some(id + 1),
                gap_to_next: Some(GapEvidence {
                    charge_between: false,
                    state: StateCoverage::Online,
                }),
            }
        })
        .collect();
    let (first, _, at, odometer) = samples[2];
    let (last, _, end, end_odometer) = samples[3];
    rows.push(RepairInput {
        drive_id: run.destination(),
        car_id: CAR,
        start_ms: at,
        end_ms: Some(end),
        distance_km: Some(9.0),
        duration_min: Some(30),
        sample_count: 2,
        first_sample: Some(sample(first, at, odometer)),
        last_sample: Some(sample(last, end, end_odometer)),
        previous_drive_id: Some(run.destination() - 1),
        previous_unclosed: true,
        next_drive_id: None,
        gap_to_next: None,
    });
    rows
}

fn raw(fixture: &Fixture) -> Connection {
    let connection = Connection::open(fixture.store.database_path()).expect("raw catalogue");
    connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("foreign keys off");
    connection
}

/// The materialised and catalogued rows an import of the runs leaves: the
/// destinations with their samples, the fragment samples detached, and the
/// fragments themselves (unclosed) not stored as journeys.
fn seed_imported(fixture: &Fixture) {
    let connection = raw(fixture);
    let vehicle = fixture.binding.vehicle_id.to_string();
    for run in RUNS {
        connection
            .execute(
                "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    vehicle,
                    run.destination(),
                    CAR,
                    serde_json::to_string(&materialised_destination(run)).expect("drive json")
                ],
            )
            .expect("destination");
        connection
            .execute(
                "INSERT INTO import_projection_rows(vehicle_id, entity, entity_id) VALUES (?1, 'drive', ?2)",
                params![vehicle, run.destination()],
            )
            .expect("catalogued destination");
        for (id, parent, at, odometer) in run.samples() {
            let parent = (parent == run.destination()).then_some(parent);
            let row = materialised_sample(id, parent, at, odometer);
            connection
                .execute(
                    "INSERT INTO materialised_positions(vehicle_id, position_id, drive_id, car_id,
                         position_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![vehicle, id, parent, CAR, serde_json::to_string(&row).expect("json")],
                )
                .expect("sample");
            connection
                .execute(
                    "INSERT INTO import_projection_rows(vehicle_id, entity, entity_id)
                     VALUES (?1, 'position', ?2)",
                    params![vehicle, id],
                )
                .expect("catalogued sample");
        }
    }
}

/// The raw physical stage of the base drive and the runs.
fn stage(root: &Path, name: &str) -> OwnedSourceDbStage {
    let mut stage = SourceDbStage::create_physical_v3(
        root.join(name),
        SourceDbStageLimits {
            max_rows: 4_096,
            max_stage_bytes: 8 * 1024 * 1024,
            minimum_free_bytes: 0,
        },
    )
    .expect("physical stage");
    crate::sync::physical_fragments::tests::seed_roots(&mut stage);
    let mut drives = vec![physical_drive(10, T - 5 * HOUR, Some((T - 5 * HOUR + 10 * MINUTE, 10.0)))];
    let mut positions = Vec::new();
    for run in RUNS {
        for (i, id) in run.fragments().into_iter().enumerate() {
            drives.push(physical_drive(id, run.start + i as i64 * MINUTE, None));
        }
        drives.push(physical_drive(
            run.destination(),
            run.start + 2 * MINUTE,
            Some((run.end(), run.odometer + 1.5)),
        ));
        for (id, parent, at, odometer) in run.samples() {
            positions.push(physical_position(id, parent, at, odometer));
        }
    }
    for row in &drives {
        stage.insert(SourceDbStageTable::Drives, i64::from(row.id), row).expect("drive");
    }
    for row in &positions {
        stage.insert(SourceDbStageTable::Positions, i64::from(row.id), row).expect("position");
    }
    stage.seal().expect("seal");
    OwnedSourceDbStage::new(stage)
}

/// Every materialised drive as the store holds it: the kept source drives.
fn stored_drives(fixture: &Fixture) -> Vec<ProjectionDrive> {
    let connection = raw(fixture);
    let mut statement = connection
        .prepare("SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1 ORDER BY drive_id")
        .expect("drives");
    statement
        .query_map([fixture.binding.vehicle_id.to_string()], |row| row.get::<_, String>(0))
        .expect("drives")
        .map(|json| serde_json::from_str(&json.expect("drive")).expect("drive json"))
        .collect()
}

type HubDrives = BTreeMap<i64, (i64, i64)>;
type HubPositions = BTreeMap<i64, (Option<i64>, i64)>;

/// The Hub's own reads: its stored drives and their positions.
fn hub_reads(fixture: &Fixture) -> (HubDrives, HubPositions) {
    let drives = stored_drives(fixture)
        .into_iter()
        .map(|drive| (drive.id, (drive.start_date_ms, drive.end_date_ms)))
        .collect();
    let connection = raw(fixture);
    let mut statement = connection
        .prepare("SELECT position_id, drive_id, position_json FROM materialised_positions WHERE vehicle_id = ?1")
        .expect("positions");
    let positions = statement
        .query_map([fixture.binding.vehicle_id.to_string()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, String>(2)?))
        })
        .expect("positions")
        .map(|row| {
            let (id, drive, json) = row.expect("position");
            let position: ProjectionPosition = serde_json::from_str(&json).expect("position json");
            assert_eq!(position.drive_id, drive, "the column and the JSON agree");
            (id, (drive, position.date_ms))
        })
        .collect();
    (drives, positions)
}

/// A 1.4 mirror as the Hub reads the same rows: unclosed drives (start
/// equals end in the mirror) are not journeys, and their samples detached.
fn hub_view(mirror: &PhysicalMirror) -> (HubDrives, HubPositions) {
    let drives: HubDrives = mirror
        .drives
        .iter()
        .filter(|(_, (start, end))| start != end)
        .map(|(id, span)| (*id, *span))
        .collect();
    let positions = mirror
        .positions
        .iter()
        .map(|(id, (drive, at))| (*id, (drive.filter(|drive| drives.contains_key(drive)), *at)))
        .collect();
    (drives, positions)
}

struct Setup {
    fixture: Fixture,
    _root: tempfile::TempDir,
    env: Paired,
    existing: PhysicalClient,
    /// The planned components, by run.
    keys: Vec<[u8; 32]>,
    /// The Hub's reads before any repair.
    before: (HubDrives, HubPositions),
}

/// A vehicle whose import left the two runs and kept its raw stage, with a
/// served head an existing 1.4 client holds, and both runs planned.
fn setup() -> Setup {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    seed_imported(&fixture);
    let drives = stored_drives(&fixture);
    publish_physical_route(
        &fixture.store,
        &fixture.cursor_key,
        fixture.binding.clone(),
        Some(stage(root.path(), "base")),
        &fixture.gate,
        None,
        Some((&drives, &[])),
    )
    .expect("physical publication")
    .expect("physical head");
    let env = paired(&fixture);
    let existing = PhysicalClient::bootstrap(&env, &fixture).expect("served before any repair");
    let vehicle = fixture.binding.vehicle_id;
    let rows: Vec<RepairInput> = RUNS.into_iter().flat_map(index_rows).collect();
    let ids: Vec<i64> = rows.iter().map(|row| row.drive_id).collect();
    fixture
        .store
        .record_drive_repair_index(fixture.binding.account_id, vehicle, &BrokenDriveCandidateIndex { car_id: CAR, rows }, NOW)
        .expect("index");
    assert_eq!(
        fixture.store.attest_unedited_drive_repair_candidates(vehicle, &ids, NOW).expect("attest"),
        ids.len() as u64
    );
    let plan = fixture.store.plan_drive_repairs(vehicle, NOW).expect("plan");
    assert_eq!(plan.planned_keys.len(), 2, "both runs are planned: {:?}", plan.counts);
    let before = hub_reads(&fixture);
    Setup {
        fixture,
        _root: root,
        env,
        existing,
        keys: plan.planned_keys,
        before,
    }
}

fn apply_bounded(setup: &Setup, limit: usize) -> DriveRepairApplyOutcome {
    let fixture = &setup.fixture;
    apply_drive_repairs_bounded(
        &fixture.store,
        &fixture.cursor_key,
        &fixture.gate,
        fixture.binding.vehicle_id,
        NOW,
        limit,
    )
    .expect("apply")
}

fn apply(setup: &Setup) -> DriveRepairApplyOutcome {
    let fixture = &setup.fixture;
    apply_drive_repairs(&fixture.store, &fixture.cursor_key, &fixture.gate, fixture.binding.vehicle_id, NOW)
        .expect("apply")
}

fn undo(setup: &Setup, target: DriveRepairTarget<'_>) -> DriveRepairApplyOutcome {
    let fixture = &setup.fixture;
    undo_drive_repairs(&fixture.store, &fixture.cursor_key, &fixture.gate, fixture.binding.vehicle_id, target, NOW)
        .expect("undo")
}

fn hook(point: DriveRepairHookPoint, run: impl FnOnce(&HubStore) -> Result<(), SourceDbImportError> + 'static) {
    DRIVE_REPAIR_HOOKS.with(|hooks| hooks.borrow_mut().push((point, Box::new(run))));
}

fn crash(_: &HubStore) -> Result<(), SourceDbImportError> {
    Err(StoreError::ImportGenerationConflict.into())
}

fn mutations(fixture: &Fixture) -> i64 {
    raw(fixture)
        .query_row(
            "SELECT COUNT(*) FROM sync_mutations WHERE vehicle_id = ?1",
            [fixture.binding.vehicle_id.to_string()],
            |row| row.get(0),
        )
        .expect("mutations")
}

/// The vehicle's served head.
fn current_head(fixture: &Fixture) -> Uuid {
    fixture
        .store
        .pending_physical_v3_control_admission_for_vehicle(fixture.binding.vehicle_id)
        .expect("head")
        .expect("a served head")
        .snapshot_id
}

fn served(fixture: &Fixture) -> bool {
    fixture
        .store
        .physical_v3_current_head_served(fixture.binding.vehicle_id)
        .expect("served")
        == Some(true)
}

/// The heads recorded with a repair layer, by snapshot.
fn layered_heads(fixture: &Fixture) -> BTreeMap<String, crate::db::PhysicalV3RepairLayer> {
    raw(fixture)
        .query_row(
            "SELECT value FROM hub_metadata WHERE key = ?1",
            [format!("physical_v3_repair_layer:{}", fixture.binding.vehicle_id)],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("layer records")
        .map(|value| serde_json::from_str(&value).expect("records"))
        .unwrap_or_default()
}

/// A fresh client and the existing one (after `changes-since`) hold the
/// same rows, and those are the Hub's own reads.
fn assert_agree(setup: &mut Setup) {
    let fresh = PhysicalClient::bootstrap(&setup.env, &setup.fixture).expect("the head is served");
    let kind = setup.existing.sync(&setup.env, &setup.fixture);
    assert!(
        matches!(kind.as_str(), "physical_changed_set" | "no_op"),
        "the existing client follows incrementally: {kind}"
    );
    assert_eq!(setup.existing.mirror, fresh.mirror, "the existing client ends where a fresh one does");
    assert_eq!(hub_view(&fresh.mirror), hub_reads(&setup.fixture), "clients and Hub reads agree");
}

fn keys_of(activation: &crate::db::DriveRepairActivation) -> Vec<[u8; 32]> {
    let mut keys = activation.activated.clone();
    keys.sort_unstable();
    keys
}

fn sorted(mut keys: Vec<[u8; 32]>) -> Vec<[u8; 32]> {
    keys.sort_unstable();
    keys
}

/// One batch joins both runs: every fragment sample under its destination,
/// the destination starting at its first fragment, no row added or lost,
/// one sync mutation per changed row; both clients and the Hub agree.
#[test]
fn broken_drive_repair_apply_joins_samples_keeps_totals_and_clients_agree() {
    let mut setup = setup();
    let fixture = &setup.fixture;
    let journal = mutations(fixture);
    let DriveRepairApplyOutcome::Served { activation, .. } = apply(&setup) else {
        panic!("the batch's head is not served");
    };
    assert_eq!(keys_of(&activation), sorted(setup.keys.clone()));
    assert_eq!(activation.changed_rows, 6, "per run: the destination and two samples");
    assert_eq!(mutations(fixture) - journal, 6, "one sync mutation per changed row");
    let (drives, positions) = hub_reads(fixture);
    assert_eq!(drives.keys().collect::<Vec<_>>(), setup.before.0.keys().collect::<Vec<_>>());
    assert_eq!(positions.keys().collect::<Vec<_>>(), setup.before.1.keys().collect::<Vec<_>>());
    for run in RUNS {
        assert_eq!(drives[&run.destination()], (run.start, run.end()), "starts at its first fragment");
        for (id, _, at, _) in run.samples() {
            assert_eq!(positions[&id], (Some(run.destination()), at), "sample {id} joined");
        }
    }
    let destination = stored_drives(fixture)
        .into_iter()
        .find(|drive| drive.id == RUNS[0].destination())
        .expect("destination");
    assert_eq!(destination.distance_km, Some(10.5), "the odometer span from the first fragment");
    assert_eq!(destination.duration_min, Some(32));
    let summary = fixture.store.drive_repair_summary(fixture.binding.vehicle_id).expect("summary");
    assert_eq!((summary.planned, summary.active), (0, 2));
    assert!(served(fixture));
    assert_agree(&mut setup);
    assert!(
        setup.existing.mirror.drives.keys().all(|id| !RUNS.iter().any(|run| run.fragments().contains(id))),
        "the fragments left the clients"
    );
}

/// The changed-set bound: each batch takes what fits and publishes one
/// head; the rest waits for the next batch.
#[test]
fn broken_drive_repair_apply_one_head_per_batch_bounded_by_the_changed_set_limit() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    let (first, second) = {
        let mut order = setup.keys.clone();
        // Effective start order: run 0 starts first.
        let planned = setup.fixture.store.drive_repair_activation_candidates(vehicle, 1_000).expect("candidates");
        order.sort_by_key(|key| planned.iter().position(|c| c.component_key == *key));
        (order[0], order[1])
    };
    let DriveRepairApplyOutcome::Served { activation, .. } = apply_bounded(&setup, 5) else {
        panic!("first batch not served");
    };
    assert_eq!(activation.activated, vec![first], "five changed rows fit one run");
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!((summary.planned, summary.active), (1, 1), "the second run waits");
    let heads = layered_heads(&setup.fixture);
    assert_eq!(heads.len(), 1, "one head for the batch");
    assert_eq!(heads[&current_head(&setup.fixture).to_string()].components, vec![hex::encode(first)]);
    assert_agree(&mut setup);

    let DriveRepairApplyOutcome::Served { activation, .. } = apply_bounded(&setup, 5) else {
        panic!("second batch not served");
    };
    assert_eq!(activation.activated, vec![second]);
    let heads = layered_heads(&setup.fixture);
    assert_eq!(heads.len(), 2, "one more head for the second batch");
    assert_eq!(
        heads[&current_head(&setup.fixture).to_string()].components,
        sorted(vec![first, second]).iter().map(hex::encode).collect::<Vec<_>>()
    );
    assert_agree(&mut setup);
    assert!(matches!(apply_bounded(&setup, 5), DriveRepairApplyOutcome::Idle(_)), "nothing waits");
}

/// A protection recorded after activation supersedes its component, which
/// is compensated; undoing the batch then restores the other run, leaving
/// the superseded one and the protection as they are.
#[test]
fn broken_drive_repair_apply_undo_restores_fragments_and_keeps_newer_protection() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    let DriveRepairApplyOutcome::Served { batch_id, .. } = apply(&setup) else {
        panic!("batch not served");
    };
    assert_agree(&mut setup);

    // The owner protects the second run's last sample.
    let protected = RUNS[1].destination() * 100 + 1;
    let journal = mutations(&setup.fixture);
    setup
        .fixture
        .store
        .record_drive_edit_protection(vehicle, DriveRepairEntity::Position, protected, DriveProtectionReason::Protect, NOW)
        .expect("protection");
    assert_eq!(mutations(&setup.fixture) - journal, 3, "the second run's rows are restored");
    let (drives, positions) = hub_reads(&setup.fixture);
    assert_eq!(drives[&RUNS[1].destination()], setup.before.0[&RUNS[1].destination()]);
    for (id, _, _, _) in RUNS[1].samples() {
        assert_eq!(positions[&id], setup.before.1[&id], "sample {id} restored");
    }
    assert!(!served(&setup.fixture), "the layered head is refused once the layer changed");
    publish_physical_after_handback(&setup.fixture.store, &setup.fixture.cursor_key, &setup.fixture.gate)
        .expect("publication");
    assert_agree(&mut setup);

    let DriveRepairApplyOutcome::Compensated { compensation, served: is_served, .. } =
        undo(&setup, DriveRepairTarget::Batch(batch_id))
    else {
        panic!("undo did not compensate");
    };
    assert!(is_served);
    assert_eq!((compensation.components, compensation.restored_rows), (1, 3));
    assert_eq!(hub_reads(&setup.fixture), setup.before, "every row is as the import left it");
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!(
        (summary.active, summary.undone, summary.superseded, summary.suppressed, summary.protected),
        (0, 1, 1, 1, 1),
        "the protection and the superseded run stay"
    );
    assert_agree(&mut setup);
    for run in RUNS {
        for id in run.fragments() {
            assert!(setup.existing.mirror.drives.contains_key(&id), "fragment {id} is back");
        }
    }
    // A second undo restores nothing more.
    let DriveRepairApplyOutcome::Compensated { compensation, .. } =
        undo(&setup, DriveRepairTarget::Batch(batch_id))
    else {
        panic!("second undo");
    };
    assert_eq!(compensation.restored_rows, 0);
}

/// A head that cannot be published after activation (pre-rows that no
/// longer match, or a kept base that cannot be read) is compensated: the
/// rows are restored, the components wait, and the old head is served.
#[test]
fn broken_drive_repair_apply_withheld_or_stale_pre_rows_compensate_and_serve_the_old_head() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    let old = current_head(&setup.fixture);
    let journal = mutations(&setup.fixture);

    // Stale pre-rows: the recorded destination row is not the stage's.
    let stale = setup.keys[0];
    hook(DriveRepairHookPoint::AfterActivation, move |store| {
        let connection = Connection::open(store.database_path()).expect("raw");
        let json: String = connection
            .query_row(
                "SELECT before_json FROM drive_repair_changes WHERE component_key = ?1",
                [stale.as_slice()],
                |row| row.get(0),
            )
            .expect("pre-rows");
        let mut pre: crate::import::broken_drive_repair::layer::ComponentPreRows =
            serde_json::from_str(&json).expect("pre-rows json");
        pre.destination.speed_max = Some(99);
        connection
            .execute(
                "UPDATE drive_repair_changes SET before_json = ?2 WHERE component_key = ?1",
                params![stale.as_slice(), serde_json::to_string(&pre).expect("json")],
            )
            .expect("stale pre-rows");
        Ok(())
    });
    let DriveRepairApplyOutcome::Compensated { compensation, served: is_served, batch_id } = apply(&setup) else {
        panic!("stale pre-rows were not compensated");
    };
    assert!(batch_id.is_some() && is_served);
    assert_eq!((compensation.components, compensation.restored_rows), (2, 6));
    assert_eq!(mutations(&setup.fixture) - journal, 12, "six rows written, six restored");
    assert_eq!(current_head(&setup.fixture), old, "the old head is served again");
    assert_eq!(hub_reads(&setup.fixture), setup.before);
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!((summary.planned, summary.active), (2, 0), "the components wait");
    assert_agree(&mut setup);

    // Withheld: the kept source sessions cannot be read at publication.
    hook(DriveRepairHookPoint::AfterActivation, |store| {
        let vehicle = store.physical_v3_kept_vehicles().expect("kept")[0];
        let kept = store.physical_v3_kept(vehicle).expect("kept").expect("record");
        std::fs::write(kept_source_path(store, vehicle, &kept), b"damaged").expect("damage");
        Ok(())
    });
    let DriveRepairApplyOutcome::Compensated { served: is_served, .. } = apply(&setup) else {
        panic!("a withheld head was not compensated");
    };
    assert!(is_served);
    assert_eq!(current_head(&setup.fixture), old);
    assert_eq!(hub_reads(&setup.fixture), setup.before);
    assert_agree(&mut setup);
}

/// A protection or a changed attestation between the dry run and the
/// activation transaction refuses the component: nothing is written.
#[test]
fn broken_drive_repair_apply_protection_during_planning_refuses() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    let old = current_head(&setup.fixture);
    let journal = mutations(&setup.fixture);
    let fragment = RUNS[0].first;
    hook(DriveRepairHookPoint::BeforeActivation, move |store| {
        store
            .record_drive_edit_protection(vehicle, DriveRepairEntity::Drive, fragment, DriveProtectionReason::Edit, NOW)
            .expect("protection");
        Ok(())
    });
    let DriveRepairApplyOutcome::Idle(activation) = apply_bounded(&setup, 5) else {
        panic!("a protected component was activated");
    };
    assert!(activation.activated.is_empty() && activation.batch_id.is_none());

    // The other run's attestation is renewed: its protection digest moved.
    let destination = RUNS[1].destination();
    hook(DriveRepairHookPoint::BeforeActivation, move |store| {
        store.attest_unedited_drive_repair_candidates(vehicle, &[destination], NOW).expect("attest");
        Ok(())
    });
    let DriveRepairApplyOutcome::Idle(activation) = apply_bounded(&setup, 5) else {
        panic!("a changed protection digest was activated");
    };
    assert_eq!(activation.superseded.len(), 1, "revalidation refuses it");
    assert_eq!(mutations(&setup.fixture), journal, "nothing was written");
    assert_eq!(hub_reads(&setup.fixture), setup.before);
    assert_eq!(current_head(&setup.fixture), old);
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!((summary.planned, summary.active, summary.superseded), (0, 0, 2));
    assert_agree(&mut setup);
}

/// A crash right after the activation commit leaves the batch publishing
/// and the head refused, never stale; a crash after the publication leaves
/// the head served. The next call settles either without a second head.
#[test]
fn broken_drive_repair_apply_crash_after_activation_or_publication_converges_on_restart() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    hook(DriveRepairHookPoint::AfterActivation, crash);
    apply_drive_repairs_bounded(&setup.fixture.store, &setup.fixture.cursor_key, &setup.fixture.gate, vehicle, NOW, 5)
        .expect_err("the crash");
    let batch = setup.fixture.store.drive_repair_publishing_batch(vehicle).expect("batch").expect("publishing");
    assert!(!served(&setup.fixture), "the old head is refused, never served stale");
    assert_eq!(
        PhysicalClient::bootstrap(&setup.env, &setup.fixture).err(),
        Some(axum::http::StatusCode::NOT_ACCEPTABLE)
    );
    let DriveRepairApplyOutcome::Served { batch_id, activation } = apply_bounded(&setup, 5) else {
        panic!("restart did not settle the batch");
    };
    assert_eq!(batch_id, batch);
    assert!(activation.activated.is_empty(), "the restart activates nothing more");
    assert_eq!(setup.fixture.store.drive_repair_publishing_batch(vehicle).expect("batch"), None);
    assert_agree(&mut setup);

    hook(DriveRepairHookPoint::AfterPublication, crash);
    apply_drive_repairs_bounded(&setup.fixture.store, &setup.fixture.cursor_key, &setup.fixture.gate, vehicle, NOW, 5)
        .expect_err("the crash");
    assert!(served(&setup.fixture), "the published head is served");
    let head = current_head(&setup.fixture);
    let heads = layered_heads(&setup.fixture).len();
    let DriveRepairApplyOutcome::Served { activation, .. } = apply_bounded(&setup, 5) else {
        panic!("restart did not settle the second batch");
    };
    assert!(activation.activated.is_empty());
    assert_eq!(current_head(&setup.fixture), head, "no second head");
    assert_eq!(layered_heads(&setup.fixture).len(), heads);
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!((summary.planned, summary.active), (0, 2));
    assert_agree(&mut setup);
}

/// Replaying an activation with the same pre-rows, or applying again,
/// attaches nothing twice and journals nothing more.
#[test]
fn broken_drive_repair_apply_replay_never_attaches_twice() {
    let mut setup = setup();
    let fixture = &setup.fixture;
    let vehicle = fixture.binding.vehicle_id;
    let candidates = fixture.store.drive_repair_activation_candidates(vehicle, 1_000).expect("candidates");
    let pre_rows = drive_repair_dry_run_pre_rows(&fixture.store, &fixture.gate, vehicle, &candidates)
        .expect("dry run")
        .expect("plannable");
    let first = fixture.store.activate_drive_repairs(&fixture.gate, vehicle, &pre_rows, NOW).expect("activate");
    assert_eq!((first.activated.len(), first.changed_rows), (2, 6));
    let journal = mutations(fixture);
    let reads = hub_reads(fixture);
    let replay = fixture.store.activate_drive_repairs(&fixture.gate, vehicle, &pre_rows, NOW).expect("replay");
    assert_eq!((replay.activated.len(), replay.changed_rows, replay.batch_id), (0, 0, None));
    assert_eq!(mutations(fixture), journal);
    assert_eq!(hub_reads(fixture), reads);

    assert!(matches!(apply(&setup), DriveRepairApplyOutcome::Served { .. }), "the batch settles");
    assert!(matches!(apply(&setup), DriveRepairApplyOutcome::Idle(_)));
    assert_eq!(mutations(&setup.fixture), journal, "settling and replaying journal nothing");
    assert_eq!(hub_reads(&setup.fixture), reads);
    assert_agree(&mut setup);
}

/// An import whose publication is pending holds every repair change.
#[test]
fn broken_drive_repair_apply_pending_import_intent_refuses() {
    let mut setup = setup();
    let vehicle = setup.fixture.binding.vehicle_id;
    let key = format!("import_publication_intent:{vehicle}");
    raw(&setup.fixture)
        .execute("INSERT INTO hub_metadata(key, value) VALUES (?1, 'pending')", [&key])
        .expect("intent");
    let journal = mutations(&setup.fixture);
    assert!(matches!(apply(&setup), DriveRepairApplyOutcome::Refused(reason) if reason.contains("import")));
    let first = setup.keys[0];
    assert!(matches!(
        undo(&setup, DriveRepairTarget::Component(&first)),
        DriveRepairApplyOutcome::Refused(_)
    ));
    assert_eq!(mutations(&setup.fixture), journal);
    let summary = setup.fixture.store.drive_repair_summary(vehicle).expect("summary");
    assert_eq!((summary.planned, summary.active, summary.undone), (2, 0, 0));

    raw(&setup.fixture).execute("DELETE FROM hub_metadata WHERE key = ?1", [&key]).expect("resolved");
    assert!(matches!(apply(&setup), DriveRepairApplyOutcome::Served { .. }));
    assert_agree(&mut setup);
}
