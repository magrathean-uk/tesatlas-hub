// SPDX-License-Identifier: AGPL-3.0-only

//! M03-T27d: active repairs at every import commit. A vehicle imports two
//! broken drives, keeps its raw stage, and both runs are repaired and served.
//! A later import then commits over them: an unchanged one keeps every
//! repair in Hub reads with the same layer and no extra head; a source-closed
//! fragment or a deleted destination supersedes its component and restores
//! its rows inside that same commit; a run interrupted between its commit and
//! its physical publication resumes to a head with the surviving layer.
//! Applying and undoing a repair leaves every source digest as it was.

use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use super::*;
use crate::db::DriveRepairTarget;
use crate::hub_pack::{ProjectionFixedNumericV2_2, ProjectionFloat64BitsV2_2};
use crate::import::broken_drive_repair::{
    BrokenDriveCandidateIndex, GapEvidence, RepairInput, RepairSample, StateCoverage,
};
use crate::source_db_projection::{
    SourceDbDrive, SourceDbDrivePhysicalV2_2, SourceDbPosition, SourceDbPositionPhysicalV2_2,
};
use crate::source_db_stage::{OwnedSourceDbStage, SourceDbStage, SourceDbStageLimits, SourceDbStageTable};

const T: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;
const HOUR: i64 = 60 * MINUTE;
const CAR: i64 = 1;
const NOW: i64 = T + 100 * HOUR;
const PG_EPOCH_US: i64 = 946_684_800_000_000;

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

/// What a later import finds at the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Unchanged,
    /// The first run's first fragment gained an end.
    ClosedFragment,
    /// The second run's destination, and its own samples, are gone.
    DeletedDestination,
}

impl Source {
    fn closed(self, id: i64) -> Option<i64> {
        (self == Source::ClosedFragment && id == RUNS[0].fragments()[0]).then(|| RUNS[0].start + 30_000)
    }

    fn deleted(self, id: i64) -> bool {
        self == Source::DeletedDestination && id == RUNS[1].destination()
    }
}

fn pg_us(ms: i64) -> i64 {
    ms * 1_000 - PG_EPOCH_US
}

fn km(value: f64) -> Option<ProjectionFloat64BitsV2_2> {
    Some(ProjectionFloat64BitsV2_2::from_f64(value))
}

// ---- The source ---------------------------------------------------------

fn source_car() -> SourceDbCar {
    SourceDbCar {
        id: CAR,
        eid: 88,
        vid: Some(99),
        vin: Some("5YJTESTVIN1234567".into()),
        name: Some("Road car".into()),
        model: Some("Model 3".into()),
        trim_badging: None,
        marketing_name: None,
        exterior_color: None,
        wheel_type: None,
        spoiler_type: None,
        efficiency_wh_per_km: Some(0.145),
        settings: Default::default(),
    }
}

fn source_drive(id: i64, start_ms: i64, end_ms: Option<i64>) -> SourceDbDrive {
    SourceDbDrive {
        id,
        car_id: CAR,
        start_date_ms: start_ms,
        end_date_ms: end_ms,
        outside_temp_avg: None,
        speed_max: Some(50),
        power_max: None,
        power_min: None,
        start_ideal_range_km: None,
        end_ideal_range_km: None,
        start_rated_range_km: end_ms.map(|_| 300.0),
        end_rated_range_km: end_ms.map(|_| 280.0),
        start_km: end_ms.map(|_| 10.0),
        end_km: end_ms.map(|_| 19.0),
        distance_km: end_ms.map(|_| 9.0),
        duration_min: end_ms.map(|end| (end - start_ms) / MINUTE),
        start_address_id: None,
        end_address_id: None,
        start_geofence_id: None,
        end_geofence_id: None,
        start_position_id: None,
        end_position_id: None,
        ascent: None,
        descent: None,
        inside_temp_avg: None,
    }
}

fn source_position(id: i64, drive_id: i64, date_ms: i64) -> SourceDbPosition {
    serde_json::from_value(serde_json::json!({
        "id": id, "car_id": CAR, "drive_id": drive_id, "date_ms": date_ms,
        "latitude": 47.5, "longitude": 19.0
    }))
    .expect("source position")
}

/// The base drive long before the runs, then the runs.
fn source_history(source: Source) -> SourceDbHistory {
    let mut drives = vec![source_drive(10, T - 5 * HOUR, Some(T - 5 * HOUR + 10 * MINUTE))];
    let mut positions = Vec::new();
    for run in RUNS {
        for (i, id) in run.fragments().into_iter().enumerate() {
            drives.push(source_drive(id, run.start + i as i64 * MINUTE, source.closed(id)));
        }
        if !source.deleted(run.destination()) {
            drives.push(source_drive(run.destination(), run.start + 2 * MINUTE, Some(run.end())));
        }
        for (id, parent, at, _) in run.samples() {
            if !source.deleted(parent) {
                positions.push(source_position(id, parent, at));
            }
        }
    }
    SourceDbHistory {
        cars: vec![source_car()],
        drives,
        positions,
        charging_processes: vec![],
        charges: vec![],
        addresses: vec![],
        geofences: vec![],
        states: vec![],
        updates: vec![],
    }
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

/// The raw physical stage of `source`, sealed.
fn stage(root: &Path, name: &str, source: Source) -> OwnedSourceDbStage {
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
            let start = run.start + i as i64 * MINUTE;
            drives.push(physical_drive(id, start, source.closed(id).map(|end| (end, run.odometer))));
        }
        if !source.deleted(run.destination()) {
            drives.push(physical_drive(
                run.destination(),
                run.start + 2 * MINUTE,
                Some((run.end(), run.odometer + 1.5)),
            ));
        }
        for (id, parent, at, odometer) in run.samples() {
            if !source.deleted(parent) {
                positions.push(physical_position(id, parent, at, odometer));
            }
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

/// The candidate index an import of `source` captures.
fn index(source: Source) -> BrokenDriveCandidateIndex {
    let rows = RUNS
        .into_iter()
        .flat_map(index_rows)
        .filter(|row| !source.deleted(row.drive_id))
        .map(|mut row| {
            if let Some(end) = source.closed(row.drive_id) {
                row.end_ms = Some(end);
                row.distance_km = Some(9.0);
                row.duration_min = Some(0);
            }
            row
        })
        .collect();
    BrokenDriveCandidateIndex { car_id: CAR, rows }
}

// ---- The Hub ------------------------------------------------------------

struct Fixture {
    _temporary: tempfile::TempDir,
    root: tempfile::TempDir,
    store: HubStore,
    binding: ProjectionBinding,
    car: ProjectionCar,
    gate: PublicationGate,
    cursor_key: CursorKey,
    /// The drives the base import materialised.
    drives: Vec<ProjectionDrive>,
    /// The planned components, by run.
    keys: Vec<[u8; 32]>,
    /// The Hub's rows before any repair.
    before: Rows,
}

type Rows = (BTreeMap<i64, ProjectionDrive>, BTreeMap<i64, ProjectionPosition>);

fn base_fingerprint() -> Sha256Digest {
    Sha256Digest::of_bytes(b"broken-drive-repair-import-base")
}

fn state_limits() -> SourceDbProjectionStateLimits {
    SourceDbProjectionStateLimits {
        max_rows: 64,
        max_state_bytes: 256 * 1024,
        max_changed_payload_bytes: 256 * 1024,
        minimum_free_bytes: 0,
    }
}

fn begin_run(fixture: &Fixture, at_ms: i64) -> Uuid {
    let binding = &fixture.binding;
    let run_id = fixture
        .store
        .begin_import_generation(binding.account_id, binding.vehicle_id, CAR, at_ms)
        .expect("generation");
    fixture
        .store
        .stage_import_generation_session(
            run_id,
            &SourceDbOpenSession {
                car_id: CAR,
                ..Default::default()
            },
        )
        .expect("session");
    run_id
}

/// Every entity of `projected` recorded into a sealed capture; a
/// successor's first notes its source sessions, as the importer does.
fn capture(
    fixture: &Fixture,
    run_id: Uuid,
    successor: bool,
    projected: &SourceDbProjection,
) -> SourceDbProjectionStateCapture {
    use crate::import::source_db::seam::SessionRow;
    let binding = &fixture.binding;
    let mut capture = direct_projection_state_capture(
        &fixture.store,
        &fixture.gate,
        run_id,
        binding.vehicle_id,
        binding.account_id,
        CAR,
        successor,
        state_limits(),
    )
    .expect("capture");
    if successor {
        let drives: Vec<SessionRow> = projected
            .snapshot
            .drives
            .iter()
            .map(|drive| SessionRow {
                id: drive.id,
                start_ms: drive.start_date_ms,
                end_ms: Some(drive.end_date_ms),
            })
            .collect();
        capture.note_seam_source_sessions(&drives, &[]);
    }
    for car in &projected.snapshot.cars {
        capture.record_car(car).expect("car");
    }
    for drive in &projected.snapshot.drives {
        capture.record_drive(drive).expect("drive");
    }
    for position in &projected.snapshot.positions {
        capture.record_position(position).expect("position");
    }
    capture.seal().expect("seal");
    capture
}

/// The first import: a direct base of the unchanged source.
fn import_base(fixture: &Fixture, projected: &SourceDbProjection) {
    let run_id = begin_run(fixture, T + 10 * HOUR);
    let state = capture(fixture, run_id, false, projected).into_state();
    let sequence = fixture
        .store
        .reserve_next_full_snapshot_sequence(&fixture.gate, fixture.binding.vehicle_id)
        .expect("base sequence");
    let request = ProjectionPackRequest {
        pack_id: Uuid::new_v4(),
        snapshot_id: Uuid::new_v4(),
        ordinal: 0,
        binding: fixture.binding.clone(),
        sequence: SequenceRange {
            from_exclusive: sequence,
            to_inclusive: sequence,
        },
        snapshot: &projected.snapshot,
    };
    let pack = ProjectionPackWriter::new(fixture.store.packs_dir())
        .write_full_snapshot_with_states_and_updates(&request, &[], &[])
        .expect("base pack");
    let manifest = request
        .signed_manifest_with_states_and_updates(&pack, &[], &[], &fixture.cursor_key)
        .expect("base manifest");
    fixture
        .store
        .finalize_import_generation_with_projection_state_and_materialisation(
            run_id,
            fixture.binding.account_id,
            fixture.binding.vehicle_id,
            CAR,
            T + 10 * HOUR,
            &manifest,
            base_fingerprint(),
            &[],
            &fixture.binding,
            &state,
            &fixture.car,
            &projected.snapshot.drives,
            false,
        )
        .expect("base");
}

fn raw(fixture: &Fixture) -> Connection {
    let connection = Connection::open(fixture.store.database_path()).expect("raw catalogue");
    connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("foreign keys off");
    connection
}

fn materialised_sample(id: i64, drive: Option<i64>, at: i64, odometer: f64) -> ProjectionPosition {
    serde_json::from_value(serde_json::json!({
        "id": id, "drive_id": drive, "car_id": CAR, "date_ms": at,
        "latitude": 47.5, "longitude": 19.0, "speed": 40, "odometer": odometer
    }))
    .expect("materialised sample")
}

/// The retained samples the Hub holds: each fragment's detached, each
/// destination's under it.
fn seed_samples(fixture: &Fixture) {
    let connection = raw(fixture);
    let vehicle = fixture.binding.vehicle_id.to_string();
    for run in RUNS {
        for (id, parent, at, odometer) in run.samples() {
            let parent = (parent == run.destination()).then_some(parent);
            let row = materialised_sample(id, parent, at, odometer);
            connection
                .execute(
                    "INSERT OR REPLACE INTO materialised_positions(vehicle_id, position_id, drive_id,
                         car_id, position_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![vehicle, id, parent, CAR, serde_json::to_string(&row).expect("json")],
                )
                .expect("sample");
        }
    }
}

/// Every materialised drive and position as the Hub reads them.
fn rows(fixture: &Fixture) -> Rows {
    let connection = raw(fixture);
    let vehicle = fixture.binding.vehicle_id.to_string();
    let mut statement = connection
        .prepare("SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1")
        .expect("drives");
    let drives = statement
        .query_map([&vehicle], |row| row.get::<_, String>(0))
        .expect("drives")
        .map(|json| {
            let drive: ProjectionDrive = serde_json::from_str(&json.expect("drive")).expect("drive json");
            (drive.id, drive)
        })
        .collect();
    let mut statement = connection
        .prepare("SELECT drive_id, position_json FROM materialised_positions WHERE vehicle_id = ?1")
        .expect("positions");
    let positions = statement
        .query_map([&vehicle], |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, String>(1)?)))
        .expect("positions")
        .map(|row| {
            let (drive, json) = row.expect("position");
            let position: ProjectionPosition = serde_json::from_str(&json).expect("position json");
            assert_eq!(position.drive_id, drive, "the column and the JSON agree");
            (position.id, position)
        })
        .collect();
    (drives, positions)
}

/// A vehicle whose base import left both runs, whose raw stage is kept
/// under a served head, with both runs planned.
fn setup() -> Fixture {
    let temporary = crate::private_tempdir().expect("temporary store");
    let root = crate::private_tempdir().expect("stage root");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let source = store
        .register_source(
            &SourceDescriptor::new(crate::db::IMPORT_SOURCE_KIND, "broken-drive-repair-import"),
            T,
        )
        .expect("source");
    let vehicle = store
        .register_vehicle(&VehicleDescriptor::new(source.source_id, "broken-drive-repair-car"), T)
        .expect("vehicle");
    let binding = ProjectionBinding {
        installation_id: store.installation_id().expect("installation"),
        account_id: source.source_id,
        vehicle_id: vehicle.vehicle_id,
        generation: source.generation,
        selected_car_id: CAR,
    };
    let gate = store.try_acquire_publication_gate().expect("gate");
    let projected = project_vehicle(&source_history(Source::Unchanged), CAR).expect("base projection");
    let mut fixture = Fixture {
        _temporary: temporary,
        root,
        store,
        binding,
        car: projected.snapshot.cars[0].clone(),
        gate,
        cursor_key: CursorKey::from_bytes([43; 32]),
        drives: projected.snapshot.drives.clone(),
        keys: Vec::new(),
        before: Default::default(),
    };
    assert_eq!(
        fixture.drives.iter().map(|drive| drive.id).collect::<Vec<_>>(),
        vec![10, RUNS[0].destination(), RUNS[1].destination()],
        "the fragments are not journeys"
    );
    import_base(&fixture, &projected);
    seed_samples(&fixture);
    publish_physical_route(
        &fixture.store,
        &fixture.cursor_key,
        fixture.binding.clone(),
        Some(stage(fixture.root.path(), "base", Source::Unchanged)),
        &fixture.gate,
        None,
        Some((&fixture.drives, &[])),
    )
    .expect("physical publication")
    .expect("physical head");
    let vehicle_id = fixture.binding.vehicle_id;
    let index = index(Source::Unchanged);
    let ids: Vec<i64> = index.rows.iter().map(|row| row.drive_id).collect();
    fixture
        .store
        .record_drive_repair_index(fixture.binding.account_id, vehicle_id, &index, NOW)
        .expect("index");
    assert_eq!(
        fixture.store.attest_unedited_drive_repair_candidates(vehicle_id, &ids, NOW).expect("attest"),
        ids.len() as u64
    );
    let plan = fixture.store.plan_drive_repairs(vehicle_id, NOW).expect("plan");
    assert_eq!(plan.planned_keys.len(), 2, "both runs are planned: {:?}", plan.counts);
    fixture.keys = plan.planned_keys;
    fixture.before = rows(&fixture);
    fixture
}

fn apply(fixture: &Fixture) -> DriveRepairApplyOutcome {
    apply_drive_repairs(&fixture.store, &fixture.cursor_key, &fixture.gate, fixture.binding.vehicle_id, NOW)
        .expect("apply")
}

/// [`setup`] with both runs repaired and their head served.
fn repaired() -> Fixture {
    let fixture = setup();
    let DriveRepairApplyOutcome::Served { activation, .. } = apply(&fixture) else {
        panic!("the batch's head is not served");
    };
    assert_eq!(activation.activated.len(), 2);
    assert!(served(&fixture));
    fixture
}

fn served(fixture: &Fixture) -> bool {
    fixture
        .store
        .physical_v3_current_head_served(fixture.binding.vehicle_id)
        .expect("served")
        == Some(true)
}

fn current_head(fixture: &Fixture) -> Uuid {
    fixture
        .store
        .pending_physical_v3_control_admission_for_vehicle(fixture.binding.vehicle_id)
        .expect("head")
        .expect("a head")
        .snapshot_id
}

/// The vehicle's layer digest `L`.
fn layer(fixture: &Fixture) -> Option<String> {
    raw(fixture)
        .query_row(
            "SELECT value FROM hub_metadata WHERE key = ?1",
            [format!("drive_repair_layer:{}", fixture.binding.vehicle_id)],
            |row| row.get(0),
        )
        .optional()
        .expect("layer")
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

fn mutations(fixture: &Fixture) -> i64 {
    raw(fixture)
        .query_row(
            "SELECT COUNT(*) FROM sync_mutations WHERE vehicle_id = ?1",
            [fixture.binding.vehicle_id.to_string()],
            |row| row.get(0),
        )
        .expect("mutations")
}

/// The latest journalled (operation, payload) of one entity.
fn latest_mutation(fixture: &Fixture, entity: &str, id: i64) -> Option<(String, String)> {
    raw(fixture)
        .query_row(
            "SELECT operation, payload_json FROM sync_mutations
              WHERE vehicle_id = ?1 AND entity = ?2 AND entity_id = ?3
              ORDER BY revision DESC LIMIT 1",
            params![fixture.binding.vehicle_id.to_string(), entity, id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .expect("latest mutation")
}

fn staged_captures(fixture: &Fixture) -> i64 {
    raw(fixture)
        .query_row(
            "SELECT COUNT(*) FROM hub_metadata WHERE key LIKE 'drive_repair_import_capture:%'",
            [],
            |row| row.get(0),
        )
        .expect("captures")
}

fn active_keys(fixture: &Fixture) -> Vec<[u8; 32]> {
    let connection = raw(fixture);
    let mut statement = connection
        .prepare(
            "SELECT component_key FROM drive_repair_changes
              WHERE vehicle_id = ?1 AND state = 'active' ORDER BY component_key",
        )
        .expect("active");
    statement
        .query_map([fixture.binding.vehicle_id.to_string()], |row| row.get::<_, Vec<u8>>(0))
        .expect("active")
        .map(|key| <[u8; 32]>::try_from(key.expect("key").as_slice()).expect("32 bytes"))
        .collect()
}

/// The component of `run`: the planned key whose members name its
/// destination.
fn key_of(fixture: &Fixture, run: Run) -> [u8; 32] {
    let key: Vec<u8> = raw(fixture)
        .query_row(
            "SELECT component_key FROM drive_repair_changes WHERE vehicle_id = ?1 AND destination_drive_id = ?2",
            params![fixture.binding.vehicle_id.to_string(), run.destination()],
            |row| row.get(0),
        )
        .expect("component");
    let key = <[u8; 32]>::try_from(key.as_slice()).expect("32 bytes");
    assert!(fixture.keys.contains(&key));
    key
}

/// The planned successor of `source`, committed as the importer commits
/// it, with the run's candidate capture handed to the commit. Returns the
/// stage when `capture_publication` recorded the issue #60 capture first.
fn commit_successor(fixture: &Fixture, source: Source, capture_publication: bool) -> Option<OwnedSourceDbStage> {
    let projected = project_vehicle(&source_history(source), CAR).expect("projection");
    let mut planned = plan_direct_successor(
        &fixture.store,
        &fixture.gate,
        &fixture.cursor_key,
        fixture.binding.vehicle_id,
        CAR,
        &projected.snapshot.drives,
        &projected.snapshot.charges,
    )
    .expect("plan ahead of the packs");
    let run_id = begin_run(fixture, NOW + HOUR);
    let mut kept = None;
    if capture_publication {
        let mut stage = stage(fixture.root.path(), "successor", source);
        capture_import_publication(
            &fixture.store,
            &fixture.gate,
            run_id,
            &fixture.binding,
            stage.stage_mut(),
            &projected.snapshot.drives,
            &projected.snapshot.charges,
            &updates_source(),
        )
        .expect("capture before the commit");
        kept = Some(stage);
    }
    fixture
        .store
        .stage_import_drive_repair_capture(run_id, Some(&index(source)))
        .expect("candidate capture handed to the commit");
    let state = capture(fixture, run_id, true, &projected);
    let base_snapshot_id = fixture
        .store
        .v2_head(fixture.binding.vehicle_id)
        .expect("head")
        .expect("base")
        .0;
    publish_planned_direct_successor(
        &fixture.store,
        &fixture.gate,
        &fixture.cursor_key,
        PlannedSuccessorRun {
            run_id,
            source_id: fixture.binding.account_id,
            imported_at_ms: NOW + HOUR,
            base_snapshot_id,
            minimum_free_bytes: 0,
            fingerprint: Sha256Digest::of_bytes(format!("successor-{run_id}").as_bytes()),
            geofences: &[],
            materialised_car: &fixture.car,
            materialised_drives: &projected.snapshot.drives,
            materialised_charges: &projected.snapshot.charges,
        },
        &fixture.binding,
        &fixture.car,
        state,
        &mut planned,
    )
    .expect("successor commit");
    kept
}

/// The schema-2.2 source facts of the selected car.
fn updates_source() -> DirectUpdatesSourceV2_2 {
    DirectUpdatesSourceV2_2 {
        postgres_snapshot_sha256: crate::updates_logical::hex_sha256(Uuid::new_v4().as_bytes()),
        schema: crate::source_db_reader::SourceDbSchemaInfo {
            observed_migration_version: crate::source_db_schema::MAX_VALIDATED_MIGRATION,
            observed_migration_count: crate::source_db_schema::SOURCE_DB_V4_MIGRATION_COUNT,
            minimum_supported_migration_version: crate::source_db_schema::MIN_SUPPORTED_MIGRATION,
            maximum_validated_migration_version: crate::source_db_schema::MAX_VALIDATED_MIGRATION,
            pinned_source_revision: crate::source_db_schema::SOURCE_DB_V4_SOURCE_REVISION,
            pinned_migration_set_sha256: crate::source_db_schema::SOURCE_DB_V4_MIGRATION_SET_SHA256,
            fingerprint: crate::updates_logical::hex_sha256(b"broken-drive-repair-import-schema"),
        },
        global_settings: crate::source_db_projection::SourceDbSettingsPhysicalV2_2 {
            id: 1,
            unit_of_length: crate::hub_pack::ProjectionUnitOfLengthV2_2::Kilometers,
            unit_of_temperature: crate::hub_pack::ProjectionUnitOfTemperatureV2_2::Celsius,
            unit_of_pressure: crate::hub_pack::ProjectionUnitOfPressureV2_2::Bar,
            preferred_range: crate::hub_pack::ProjectionPreferredRangeV2_2::Rated,
            base_url: None,
            grafana_url: None,
            language: "en".into(),
            theme_mode: "system".into(),
            inserted_at_pg_us: 0,
            updated_at_pg_us: 0,
        },
        car: crate::source_db_projection::SourceDbCarPhysicalV2_2 {
            id: 1,
            eid: 88,
            vid: 99,
            vin: Some("5YJTESTVIN1234567".into()),
            name: Some("Road car".into()),
            model: Some("Model 3".into()),
            efficiency: Some(0.145),
            trim_badging: Some("74d".into()),
            marketing_name: Some("LR AWD".into()),
            exterior_color: Some("Pearl White".into()),
            wheel_type: Some("Apollo".into()),
            spoiler_type: Some("None".into()),
            display_priority: 0,
            inserted_at_pg_us: 0,
            updated_at_pg_us: 0,
            settings_id: 1,
        },
        car_settings: crate::source_db_projection::SourceDbCarSettingsPhysicalV2_2 {
            id: 1,
            suspend_min: 21,
            suspend_after_idle_min: 15,
            req_not_unlocked: false,
            free_supercharging: false,
            use_streaming_api: true,
            enabled: true,
            lfp_battery: false,
        },
        updates: Vec::new(),
    }
}

/// Asserts the component of `run` was superseded with its rows restored to
/// the pre-image, journalled as the last word on each restored row.
fn assert_superseded_and_restored(fixture: &Fixture, run: Run, now: &Rows) {
    let key = key_of(fixture, run);
    assert!(!active_keys(fixture).contains(&key), "the run's component left `active`");
    for id in run.fragments() {
        let sample = id * 100;
        let pre = &fixture.before.1[&sample];
        assert_eq!(now.1.get(&sample), Some(pre), "fragment sample {sample} is detached again");
        let pre_json = serde_json::to_string(pre).expect("json");
        assert_eq!(
            latest_mutation(fixture, "position", sample),
            Some(("upsert".into(), pre_json)),
            "the restored sample {sample} is journalled"
        );
    }
}

/// Asserts the component of `run` is still active with its after-image.
fn assert_kept(fixture: &Fixture, run: Run, repaired: &Rows, now: &Rows) {
    assert!(active_keys(fixture).contains(&key_of(fixture, run)), "the run stays repaired");
    assert_eq!(now.0.get(&run.destination()), repaired.0.get(&run.destination()));
    for (id, ..) in run.samples() {
        assert_eq!(now.1.get(&id), repaired.1.get(&id), "sample {id} stays joined");
    }
}

// ---- Tests --------------------------------------------------------------

/// An unchanged re-import, direct (its materialisation reconciled again)
/// and staged (none), keeps both repairs in Hub reads, the same `L` and the
/// same served head, journals nothing and leaves no staged capture.
#[test]
fn broken_drive_repair_import_unchanged_direct_and_staged_reimports_keep_the_repair() {
    let fixture = repaired();
    let vehicle = fixture.binding.vehicle_id;
    let repaired_rows = rows(&fixture);
    assert_ne!(repaired_rows.0, fixture.before.0, "the repair changed the destinations");
    let layer_before = layer(&fixture).expect("L");
    let head = current_head(&fixture);
    let heads = layered_heads(&fixture);
    let journal = mutations(&fixture);
    let check = |at: &str| {
        assert_eq!(rows(&fixture), repaired_rows, "{at}: Hub reads keep both repairs");
        assert_eq!(layer(&fixture).as_deref(), Some(layer_before.as_str()), "{at}: the same L");
        assert_eq!(current_head(&fixture), head, "{at}: no new head");
        assert!(served(&fixture), "{at}: the repaired head is still served");
        assert_eq!(layered_heads(&fixture), heads, "{at}: no head recorded");
        assert_eq!(mutations(&fixture), journal, "{at}: nothing journalled");
        assert_eq!(active_keys(&fixture).len(), 2, "{at}: both runs stay active");
        assert_eq!(staged_captures(&fixture), 0, "{at}: the commit took its capture");
    };

    let run = begin_run(&fixture, NOW + HOUR);
    fixture
        .store
        .stage_import_drive_repair_capture(run, Some(&index(Source::Unchanged)))
        .expect("capture");
    assert_eq!(staged_captures(&fixture), 1);
    fixture
        .store
        .promote_import_generation_with_materialisation(
            run,
            fixture.binding.account_id,
            vehicle,
            CAR,
            NOW + HOUR,
            &fixture.car,
            &fixture.drives,
        )
        .expect("unchanged direct re-import");
    check("direct");

    let run = begin_run(&fixture, NOW + 2 * HOUR);
    fixture
        .store
        .stage_import_drive_repair_capture(run, Some(&index(Source::Unchanged)))
        .expect("capture");
    fixture
        .store
        .promote_import_generation(run, fixture.binding.account_id, vehicle, CAR, NOW + 2 * HOUR)
        .expect("unchanged staged re-import");
    check("staged");

    // The index recorded after the commit changes nothing either.
    let report = fixture
        .store
        .record_drive_repair_index(fixture.binding.account_id, vehicle, &index(Source::Unchanged), NOW + 2 * HOUR)
        .expect("index");
    assert_eq!((report.changed, report.removed, report.superseded_components), (0, 0, 0));
    check("after the index");

    // An aborted run's staged capture goes with it.
    let run = begin_run(&fixture, NOW + 3 * HOUR);
    fixture
        .store
        .stage_import_drive_repair_capture(run, Some(&index(Source::Unchanged)))
        .expect("capture");
    fixture.store.abort_import_generation(run).expect("abort");
    assert_eq!(staged_captures(&fixture), 0);
}

/// A fragment closed at the source supersedes its component inside the
/// successor's commit: before the index is recorded, the run's rows are the
/// pre-image again, journalled, the other run stays repaired and `L` covers
/// it alone. The index recorded afterwards supersedes nothing more.
#[test]
fn broken_drive_repair_import_closed_fragment_supersedes_and_compensates_in_the_commit() {
    let fixture = repaired();
    let vehicle = fixture.binding.vehicle_id;
    let repaired_rows = rows(&fixture);
    let layer_before = layer(&fixture).expect("L");
    let (closed, kept) = (RUNS[0], RUNS[1]);

    commit_successor(&fixture, Source::ClosedFragment, false);

    let now = rows(&fixture);
    assert_superseded_and_restored(&fixture, closed, &now);
    assert_eq!(now.0.get(&closed.destination()), fixture.before.0.get(&closed.destination()));
    let destination = serde_json::to_string(&fixture.before.0[&closed.destination()]).expect("json");
    assert_eq!(
        latest_mutation(&fixture, "drive", closed.destination()),
        Some(("upsert".into(), destination)),
        "the destination's pre-image is the journal's last word"
    );
    assert!(now.0.contains_key(&closed.fragments()[0]), "the closed fragment is a journey now");
    assert_kept(&fixture, kept, &repaired_rows, &now);
    assert_eq!(active_keys(&fixture), vec![key_of(&fixture, kept)]);
    let layer_after = layer(&fixture).expect("L of the surviving run");
    assert_ne!(layer_after, layer_before);
    assert_eq!(staged_captures(&fixture), 0);

    let report = fixture
        .store
        .record_drive_repair_index(fixture.binding.account_id, vehicle, &index(Source::ClosedFragment), NOW + HOUR)
        .expect("index");
    assert_eq!((report.changed, report.superseded_components), (1, 0), "{report:?}");
    assert_eq!(layer(&fixture), Some(layer_after));
}

/// A destination deleted at the source supersedes its component inside the
/// successor's commit: its fragment samples are detached again, the deleted
/// destination is not served repaired, and the other run stays repaired.
#[test]
fn broken_drive_repair_import_deleted_destination_supersedes_and_compensates_in_the_commit() {
    let fixture = repaired();
    let repaired_rows = rows(&fixture);
    let (kept, deleted) = (RUNS[0], RUNS[1]);

    commit_successor(&fixture, Source::DeletedDestination, false);

    let now = rows(&fixture);
    assert_superseded_and_restored(&fixture, deleted, &now);
    assert_ne!(
        now.0.get(&deleted.destination()),
        repaired_rows.0.get(&deleted.destination()),
        "the deleted destination is not served repaired"
    );
    assert_kept(&fixture, kept, &repaired_rows, &now);
    assert_eq!(active_keys(&fixture), vec![key_of(&fixture, kept)]);
    assert!(layer(&fixture).is_some());
    assert_eq!(staged_captures(&fixture), 0);
}

/// Issue #60: a successor that closes a fragment commits (superseding that
/// run in its commit) and ends before its physical publication. The older
/// head is refused; the restart publishes from the capture alone a head
/// recorded with the surviving layer, which is served. A second restart
/// has nothing to finish.
#[test]
fn broken_drive_repair_import_design_later_60_crash_resumes_to_the_surviving_layer() {
    let fixture = repaired();
    let vehicle = fixture.binding.vehicle_id;
    let repaired_rows = rows(&fixture);
    let head = current_head(&fixture);
    let kept = RUNS[1];

    let stage = commit_successor(&fixture, Source::ClosedFragment, true);
    drop(stage);
    // The run ends here: its physical publication never ran.
    let surviving = layer(&fixture).expect("surviving layer");
    assert_eq!(active_keys(&fixture), vec![key_of(&fixture, kept)]);
    assert!(!served(&fixture), "the older head is refused once the import committed");

    let outcomes = resume_import_publications(&fixture.store, &fixture.cursor_key, &fixture.gate).expect("resume");
    assert!(
        matches!(outcomes.as_slice(), [(id, ImportPublicationOutcome::Finished(Some(_)))] if *id == vehicle),
        "{outcomes:?}"
    );
    assert!(served(&fixture), "the resumed head is served");
    let resumed = current_head(&fixture);
    assert_ne!(resumed, head);
    let recorded = layered_heads(&fixture);
    let record = recorded.get(&resumed.to_string()).expect("the resumed head records its layer");
    assert_eq!(record.layer, surviving);
    assert_eq!(record.components, vec![hex::encode(key_of(&fixture, kept))]);
    assert_kept(&fixture, kept, &repaired_rows, &rows(&fixture));
    assert!(
        resume_import_publications(&fixture.store, &fixture.cursor_key, &fixture.gate)
            .expect("second resume")
            .is_empty()
    );
    assert!(served(&fixture));
}

/// The source digests an import compares (its fingerprint, the projection
/// state rows and the candidate index) are the same after a repair applies
/// and after it is undone.
#[test]
fn broken_drive_repair_import_source_digests_are_unaffected_by_apply_and_undo() {
    let fixture = setup();
    let vehicle = fixture.binding.vehicle_id;
    let digests = |fixture: &Fixture| {
        let connection = raw(fixture);
        let key = vehicle.to_string();
        let mut statement = connection
            .prepare(
                "SELECT entity_ordinal, entity_id, projection_sha256 FROM import_projection_state_rows
                  WHERE vehicle_id = ?1 ORDER BY entity_ordinal, entity_id",
            )
            .expect("state rows");
        let state: Vec<(i64, i64, Vec<u8>)> = statement
            .query_map([&key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("state rows")
            .collect::<Result<_, _>>()
            .expect("state rows");
        let mut statement = connection
            .prepare("SELECT drive_id, input_digest FROM drive_repair_inputs WHERE vehicle_id = ?1 ORDER BY drive_id")
            .expect("inputs");
        let inputs: Vec<(i64, Vec<u8>)> = statement
            .query_map([&key], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("inputs")
            .collect::<Result<_, _>>()
            .expect("inputs");
        let capture: Vec<u8> = connection
            .query_row(
                "SELECT capture_digest FROM drive_repair_index_heads WHERE vehicle_id = ?1",
                [&key],
                |row| row.get(0),
            )
            .expect("index head");
        let fingerprint = fixture.store.source_fingerprint_matches(vehicle, base_fingerprint()).expect("fingerprint");
        (fingerprint, state, inputs, capture)
    };
    let before = digests(&fixture);
    assert!(before.0, "the base fingerprint is recorded");
    assert!(!before.1.is_empty() && !before.2.is_empty());

    let DriveRepairApplyOutcome::Served { batch_id, .. } = apply(&fixture) else {
        panic!("the batch's head is not served");
    };
    assert_ne!(rows(&fixture), fixture.before, "the repair changed Hub reads");
    assert_eq!(digests(&fixture), before, "applying changed no source digest");

    let outcome = undo_drive_repairs(
        &fixture.store,
        &fixture.cursor_key,
        &fixture.gate,
        vehicle,
        DriveRepairTarget::Batch(batch_id),
        NOW,
    )
    .expect("undo");
    assert!(matches!(outcome, DriveRepairApplyOutcome::Compensated { .. }), "{outcome:?}");
    assert_eq!(rows(&fixture), fixture.before, "every row is as the import left it");
    assert_eq!(digests(&fixture), before, "undoing changed no source digest");
}
