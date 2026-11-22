// SPDX-License-Identifier: AGPL-3.0-only

// The builders of `repair_journey_fixture` (see its declaration in
// physical_route.rs); included into that module, so outer comments only.

use std::path::Path;

use rusqlite::{Connection, params};
use uuid::Uuid;

use super::{pg_us, publish_physical_route};
use crate::{
    db::{HubStore, IMPORT_SOURCE_KIND, SourceDescriptor, VehicleDescriptor},
    hub_pack::{
        ProjectionDrive, ProjectionFixedNumericV2_2, ProjectionFloat64BitsV2_2, ProjectionPosition,
        ProjectionPreferredRangeV2_2, ProjectionUnitOfLengthV2_2, ProjectionUnitOfPressureV2_2,
        ProjectionUnitOfTemperatureV2_2,
    },
    import::broken_drive_repair::{
        BrokenDriveCandidateIndex, GapEvidence, RepairInput, RepairSample, StateCoverage,
    },
    source_db_projection::{
        SourceDbCarPhysicalV2_2, SourceDbCarSettingsPhysicalV2_2, SourceDbDrivePhysicalV2_2,
        SourceDbPositionPhysicalV2_2, SourceDbSettingsPhysicalV2_2,
    },
    source_db_stage::{OwnedSourceDbStage, SourceDbStage, SourceDbStageLimits, SourceDbStageTable},
};

const MINUTE: i64 = 60_000;
const HOUR: i64 = 60 * MINUTE;
const CAR: i64 = 1;

/// The journey's first broken drive starts here (14 November 2023), long
/// enough ago that every destination is stable at the wall clock.
pub const JOURNEY_T: i64 = 1_700_000_000_000;

/// One broken drive: fragments `first` and `first + 1` with one sample
/// each, a minute apart from `start`, then their destination `first + 2`.
#[derive(Debug, Clone, Copy)]
pub struct Run {
    pub first: i64,
    pub start: i64,
    pub odometer: f64,
}

/// Two broken drives two hours apart, the first starting at `t`.
pub const fn runs(t: i64) -> [Run; 2] {
    [
        Run { first: 20, start: t, odometer: 100.0 },
        Run { first: 30, start: t + 2 * HOUR, odometer: 200.0 },
    ]
}

impl Run {
    pub fn fragments(self) -> [i64; 2] {
        [self.first, self.first + 1]
    }

    pub fn destination(self) -> i64 {
        self.first + 2
    }

    /// (position, parent, instant, odometer): the fragment samples, then
    /// the destination's first and last.
    pub fn samples(self) -> [(i64, i64, i64, f64); 4] {
        let d = self.destination();
        [
            (self.first * 100, self.first, self.start, self.odometer),
            ((self.first + 1) * 100, self.first + 1, self.start + MINUTE, self.odometer + 0.5),
            (d * 100, d, self.start + 2 * MINUTE, self.odometer + 1.5),
            (d * 100 + 1, d, self.start + 32 * MINUTE, self.odometer + 10.5),
        ]
    }

    pub fn end(self) -> i64 {
        self.start + 32 * MINUTE
    }
}

fn km(value: f64) -> Option<ProjectionFloat64BitsV2_2> {
    Some(ProjectionFloat64BitsV2_2::from_f64(value))
}

pub(crate) fn physical_drive(id: i64, start: i64, end: Option<(i64, f64)>) -> SourceDbDrivePhysicalV2_2 {
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

pub(crate) fn physical_position(id: i64, drive: i64, at: i64, odometer: f64) -> SourceDbPositionPhysicalV2_2 {
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
        row_digest: [0; 32],
    }
}

/// The candidate index rows of one run, as an import captures them.
pub fn index_rows(run: Run) -> Vec<RepairInput> {
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

/// A connection for seeding rows without their import parent chain.
fn raw(store: &HubStore) -> rusqlite::Result<Connection> {
    let connection = Connection::open(store.database_path())?;
    connection.execute_batch("PRAGMA foreign_keys = OFF;")?;
    Ok(connection)
}

/// The materialised and catalogued rows an import of `runs` leaves: the
/// destinations with their samples, the fragment samples detached, and the
/// fragments themselves (unclosed) not stored as journeys.
pub fn seed_imported(store: &HubStore, vehicle: Uuid, runs: &[Run]) {
    let connection = raw(store).expect("raw catalogue");
    let vehicle = vehicle.to_string();
    for &run in runs {
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

/// The raw physical stage of a base drive five hours before the first run,
/// then `runs`.
pub(crate) fn stage(root: &Path, name: &str, runs: &[Run]) -> OwnedSourceDbStage {
    let mut stage = SourceDbStage::create_physical_v3(
        root.join(name),
        SourceDbStageLimits {
            max_rows: 4_096,
            max_stage_bytes: 8 * 1024 * 1024,
            minimum_free_bytes: 0,
        },
    )
    .expect("physical stage");
    seed_roots(&mut stage);
    let base = runs.first().map_or(0, |run| run.start) - 5 * HOUR;
    let mut drives = vec![physical_drive(10, base, Some((base + 10 * MINUTE, 10.0)))];
    let mut positions = Vec::new();
    for &run in runs {
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

/// Every materialised drive of the vehicle as the store holds it: the kept
/// source drives.
pub fn stored_drives(store: &HubStore, vehicle: Uuid) -> Vec<ProjectionDrive> {
    let connection = raw(store).expect("raw catalogue");
    let mut statement = connection
        .prepare("SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1 ORDER BY drive_id")
        .expect("drives");
    statement
        .query_map([vehicle.to_string()], |row| row.get::<_, String>(0))
        .expect("drives")
        .map(|json| serde_json::from_str(&json.expect("drive")).expect("drive json"))
        .collect()
}

/// The selected car's roots in a physical stage: the global and car
/// settings and the car itself (car 1).
pub fn seed_roots(stage: &mut SourceDbStage) {
    let settings = SourceDbSettingsPhysicalV2_2 {
        id: 1,
        unit_of_length: ProjectionUnitOfLengthV2_2::Kilometers,
        unit_of_temperature: ProjectionUnitOfTemperatureV2_2::Celsius,
        unit_of_pressure: ProjectionUnitOfPressureV2_2::Bar,
        preferred_range: ProjectionPreferredRangeV2_2::Rated,
        base_url: None,
        grafana_url: None,
        language: "en".into(),
        theme_mode: "system".into(),
        inserted_at_pg_us: 0,
        updated_at_pg_us: 0,
    };
    let car_settings = SourceDbCarSettingsPhysicalV2_2 {
        id: 9,
        suspend_min: 21,
        suspend_after_idle_min: 15,
        req_not_unlocked: false,
        free_supercharging: false,
        use_streaming_api: true,
        enabled: true,
        lfp_battery: false,
    };
    let car = SourceDbCarPhysicalV2_2 {
        id: 1,
        eid: 100,
        vid: 200,
        vin: Some("physical-vin".into()),
        name: Some("physical-car".into()),
        model: Some("3".into()),
        efficiency: Some(0.153),
        trim_badging: None,
        marketing_name: None,
        exterior_color: None,
        wheel_type: None,
        spoiler_type: None,
        display_priority: 1,
        inserted_at_pg_us: 0,
        updated_at_pg_us: 0,
        settings_id: car_settings.id,
    };
    stage
        .insert(SourceDbStageTable::GlobalSettings, settings.id, &settings)
        .expect("settings root");
    stage
        .insert(
            SourceDbStageTable::CarSettings,
            car_settings.id,
            &car_settings,
        )
        .expect("car settings root");
    stage
        .insert(SourceDbStageTable::Cars, i64::from(car.id), &car)
        .expect("car root");
}

/// Seeds the Hub data folder `data_dir` as an import of the two runs at
/// [`JOURNEY_T`] leaves it, and returns the vehicle: a registered import
/// vehicle, its materialised rows, its raw stage kept (built under
/// `stage_root`) and a served 1.4 head signed with the folder's own cursor
/// key, then the candidate index. Nothing is attested or planned: the
/// owner's controls do that. Run it before the service starts.
pub fn seed_repair_journey(data_dir: &Path, stage_root: &Path) -> Uuid {
    let store = HubStore::initialize(data_dir).expect("store");
    let source = store
        .register_source(&SourceDescriptor::new(IMPORT_SOURCE_KIND, "repair-journey"), JOURNEY_T)
        .expect("import source");
    let vehicle = store
        .register_vehicle(&VehicleDescriptor::new(source.source_id, "1"), JOURNEY_T)
        .expect("import vehicle")
        .vehicle_id;
    let binding = store.v2_projection_binding(vehicle).expect("projection binding");
    let cursor_key =
        crate::legacy_owner_credentials::load_or_create_cursor_key(data_dir).expect("cursor key");
    let gate = store.try_acquire_publication_gate().expect("publication gate");
    let runs = runs(JOURNEY_T);
    seed_imported(&store, vehicle, &runs);
    let drives = stored_drives(&store, vehicle);
    publish_physical_route(
        &store,
        &cursor_key,
        binding.clone(),
        Some(stage(stage_root, "base", &runs)),
        &gate,
        None,
        Some((&drives, &[])),
    )
    .expect("physical publication")
    .expect("physical head");
    let rows = runs.into_iter().flat_map(index_rows).collect();
    store
        .record_drive_repair_index(
            binding.account_id,
            vehicle,
            &BrokenDriveCandidateIndex { car_id: CAR, rows },
            JOURNEY_T + 100 * HOUR,
        )
        .expect("candidate index");
    vehicle
}

/// The vehicle's current physical head in the data folder and whether it is
/// served; `None` without one. Read while the service is stopped.
pub fn current_head(data_dir: &Path, vehicle: Uuid) -> Option<(Uuid, bool)> {
    let store = HubStore::initialize(data_dir).expect("store");
    let head = store
        .pending_physical_v3_control_admission_for_vehicle(vehicle)
        .expect("head")?
        .snapshot_id;
    let served = store.physical_v3_current_head_served(vehicle).expect("served") == Some(true);
    Some((head, served))
}
