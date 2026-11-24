// SPDX-License-Identifier: AGPL-3.0-only

// M03-T27b: with a component active, the raw-stage head carries the repair
// and is bound to the layer digest it was planned with; a ledger change
// refuses the old head until a new one is planned. An empty ledger leaves
// heads, inputs and the store's metadata as they were.

use std::collections::BTreeMap;

use super::*;
use crate::db::PhysicalV3RepairLayer;
use crate::hub_pack::{ProjectionFixedNumericV2_2, ProjectionFloat64BitsV2_2};
use crate::import::source_db::seam::SeamPackPlan;
use crate::physical_publication::PhysicalV3Publication;
use crate::source_db_projection::{SourceDbDrivePhysicalV2_2, SourceDbPositionPhysicalV2_2};
use crate::source_db_stage::{OwnedSourceDbStage, SourceDbStageError, SourceDbStageLimits};

const PG_EPOCH_US: i64 = 946_684_800_000_000;
const T: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;
const DESTINATION: i64 = 30;
const KEY: [u8; 32] = [0x27; 32];

struct Fixture {
    temporary: tempfile::TempDir,
    store: HubStore,
    binding: ProjectionBinding,
    gate: PublicationGate,
    cursor_key: CursorKey,
}

impl Fixture {
    fn root(&self) -> &std::path::Path {
        self.temporary.path()
    }
}

fn pg(ms: i64) -> i64 {
    ms * 1_000 - PG_EPOCH_US
}

/// A drive row; a closed one has its end, odometer span, distance and
/// duration, an open one (a structural fragment) none of them.
fn drive_row(id: i32, start_ms: i64, end_ms: Option<i64>, start_km: f64) -> SourceDbDrivePhysicalV2_2 {
    let closed = end_ms.is_some();
    let km = |value: f64| closed.then(|| ProjectionFloat64BitsV2_2::from_f64(value));
    SourceDbDrivePhysicalV2_2 {
        id,
        car_id: 1,
        start_date_pg_us: pg(start_ms),
        end_date_pg_us: end_ms.map(pg),
        start_position_id: None,
        end_position_id: None,
        start_address_id: None,
        end_address_id: None,
        start_geofence_id: None,
        end_geofence_id: None,
        outside_temp_avg_e1: None,
        inside_temp_avg_e1: None,
        speed_max: closed.then_some(50),
        power_max: None,
        power_min: None,
        start_ideal_range_km_e2: None,
        end_ideal_range_km_e2: None,
        start_rated_range_km_e2: None,
        end_rated_range_km_e2: None,
        start_km: km(start_km),
        end_km: km(start_km + 9.0),
        distance: km(9.0),
        duration_min: closed.then_some(30),
        ascent: None,
        descent: None,
    }
}

fn position_row(id: i32, drive: i32, at_ms: i64, odometer: f64) -> SourceDbPositionPhysicalV2_2 {
    SourceDbPositionPhysicalV2_2 {
        id,
        car_id: 1,
        drive_id: Some(drive),
        date_pg_us: pg(at_ms),
        latitude_e6: ProjectionFixedNumericV2_2::Finite(47_500_000),
        longitude_e6: ProjectionFixedNumericV2_2::Finite(19_000_000),
        elevation: None,
        speed: Some(40),
        power: None,
        odometer: Some(ProjectionFloat64BitsV2_2::from_f64(odometer)),
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

/// Fragments 10 and 20 with one sample each, their destination 30 and an
/// unrelated drive 40.
fn drives() -> Vec<SourceDbDrivePhysicalV2_2> {
    vec![
        drive_row(10, T, None, 0.0),
        drive_row(20, T + MINUTE, None, 0.0),
        drive_row(30, T + 3 * MINUTE, Some(T + 33 * MINUTE), 103.0),
        drive_row(40, T + 120 * MINUTE, Some(T + 150 * MINUTE), 200.0),
    ]
}

fn positions() -> Vec<SourceDbPositionPhysicalV2_2> {
    vec![
        position_row(100, 10, T, 100.0),
        position_row(200, 20, T + MINUTE, 100.5),
        position_row(300, 30, T + 3 * MINUTE, 103.0),
        position_row(301, 30, T + 33 * MINUTE, 112.0),
        position_row(400, 40, T + 120 * MINUTE, 200.0),
    ]
}

fn materialised_drive() -> ProjectionDrive {
    ProjectionDrive {
        id: DESTINATION,
        car_id: 1,
        optimized_at_ms: None,
        start_date_ms: T + 3 * MINUTE,
        end_date_ms: T + 33 * MINUTE,
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

fn materialised_position(id: i64, drive: Option<i64>, at_ms: i64, odometer: f64) -> ProjectionPosition {
    ProjectionPosition {
        id,
        drive_id: drive,
        car_id: 1,
        date_ms: at_ms,
        latitude: 47.5,
        longitude: 19.0,
        speed: Some(40),
        power: None,
        battery_level: None,
        usable_battery_level: None,
        elevation: None,
        odometer: Some(odometer),
        ideal_battery_range_km: None,
        est_battery_range_km: None,
        rated_battery_range_km: None,
        fan_status: None,
        driver_temp_setting: None,
        passenger_temp_setting: None,
        is_climate_on: None,
        is_rear_defroster_on: None,
        is_front_defroster_on: None,
        inside_temp: None,
        outside_temp: None,
        battery_heater: None,
        battery_heater_on: None,
        battery_heater_no_power: None,
        tpms_pressure_fl: None,
        tpms_pressure_fr: None,
        tpms_pressure_rl: None,
        tpms_pressure_rr: None,
    }
}

/// A raw connection for seeding rows without their import parent chain.
fn raw(fixture: &Fixture) -> rusqlite::Connection {
    let connection =
        rusqlite::Connection::open(fixture.store.database_path()).expect("raw catalogue connection");
    connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("foreign keys off");
    connection
}

/// A registered vehicle without the seam whose store materialises the
/// destination and the detached fragment samples.
fn fixture() -> Fixture {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path().join("hub")).expect("store");
    let binding = crate::sync::physical_fragments::tests::registered_admission_binding(&store);
    let gate = store.try_acquire_publication_gate().expect("publication gate");
    let fixture = Fixture {
        temporary,
        store,
        binding,
        gate,
        cursor_key: CursorKey::from_bytes([0x27; 32]),
    };
    let connection = raw(&fixture);
    let vehicle = fixture.binding.vehicle_id.to_string();
    connection
        .execute(
            "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
             VALUES (?1, ?2, 1, ?3)",
            rusqlite::params![
                vehicle,
                DESTINATION,
                serde_json::to_string(&materialised_drive()).expect("drive json")
            ],
        )
        .expect("materialised destination");
    for position in [
        materialised_position(100, None, T, 100.0),
        materialised_position(200, None, T + MINUTE, 100.5),
        materialised_position(300, Some(DESTINATION), T + 3 * MINUTE, 103.0),
        materialised_position(301, Some(DESTINATION), T + 33 * MINUTE, 112.0),
    ] {
        connection
            .execute(
                "INSERT INTO materialised_positions(vehicle_id, position_id, drive_id, car_id,
                     position_json) VALUES (?1, ?2, ?3, 1, ?4)",
                rusqlite::params![
                    vehicle,
                    position.id,
                    position.drive_id,
                    serde_json::to_string(&position).expect("position json")
                ],
            )
            .expect("materialised position");
    }
    fixture
}

/// A sealed physical stage of the rows, after `edit`.
fn stage(
    root: &std::path::Path,
    name: &str,
    edit: impl FnOnce(&mut Vec<SourceDbDrivePhysicalV2_2>, &mut Vec<SourceDbPositionPhysicalV2_2>),
) -> OwnedSourceDbStage {
    let mut drives = drives();
    let mut positions = positions();
    edit(&mut drives, &mut positions);
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
    for row in &drives {
        stage
            .insert(SourceDbStageTable::Drives, i64::from(row.id), row)
            .expect("drive");
    }
    for row in &positions {
        stage
            .insert(SourceDbStageTable::Positions, i64::from(row.id), row)
            .expect("position");
    }
    stage.seal().expect("seal");
    OwnedSourceDbStage::new(stage)
}

/// The vehicle's raw-stage publication, as an import without the seam runs it.
fn publish_with(
    fixture: &Fixture,
    name: &str,
    edit: impl FnOnce(&mut Vec<SourceDbDrivePhysicalV2_2>, &mut Vec<SourceDbPositionPhysicalV2_2>),
) -> Option<PhysicalV3Publication> {
    publish_physical_route(
        &fixture.store,
        &fixture.cursor_key,
        fixture.binding.clone(),
        Some(stage(fixture.root(), name, edit)),
        &fixture.gate,
        None,
        None,
    )
    .expect("physical publication")
}

fn publish(fixture: &Fixture, name: &str) -> Option<PhysicalV3Publication> {
    publish_with(fixture, name, |_, _| {})
}

/// Activates the one component (fragments 10 and 20 into 30); returns `L`.
fn activate(fixture: &Fixture) -> String {
    fixture
        .store
        .insert_active_drive_repair_for_test(
            fixture.binding.vehicle_id,
            &KEY,
            DESTINATION,
            &[10, 20, DESTINATION],
            &[100, 200, 300, 301],
            T,
        )
        .expect("activate")
        .expect("an active layer")
}

/// The snapshot of the served control head, if one is served.
fn served(fixture: &Fixture) -> Option<Uuid> {
    fixture
        .store
        .pending_physical_v3_control_admission_for_vehicle(fixture.binding.vehicle_id)
        .expect("served head read")
        .map(|admission| admission.snapshot_id)
}

fn metadata(fixture: &Fixture, key: &str) -> Option<String> {
    use rusqlite::OptionalExtension;
    raw(fixture)
        .query_row("SELECT value FROM hub_metadata WHERE key = ?1", [key], |row| row.get(0))
        .optional()
        .expect("metadata read")
}

fn layer_records(fixture: &Fixture) -> BTreeMap<String, PhysicalV3RepairLayer> {
    let key = format!("physical_v3_repair_layer:{}", fixture.binding.vehicle_id);
    metadata(fixture, &key)
        .map(|value| serde_json::from_str(&value).expect("layer records"))
        .unwrap_or_default()
}

type StageRows = (
    BTreeMap<i32, SourceDbDrivePhysicalV2_2>,
    BTreeMap<i32, SourceDbPositionPhysicalV2_2>,
);

fn rows(stage: &mut OwnedSourceDbStage) -> StageRows {
    let mut rows = StageRows::default();
    stage
        .stage_mut()
        .edit_sealed(|edit| -> Result<(), SourceDbStageError> {
            rows.0 = edit
                .rows::<SourceDbDrivePhysicalV2_2>(SourceDbStageTable::Drives)?
                .into_iter()
                .map(|(_, row)| (row.id, row))
                .collect();
            rows.1 = edit
                .rows::<SourceDbPositionPhysicalV2_2>(SourceDbStageTable::Positions)?
                .into_iter()
                .map(|(_, row)| (row.id, row))
                .collect();
            Ok(())
        })
        .expect("stage rows");
    rows
}

/// One active component: the layer joins the fragments to the destination
/// on the raw stage, the first application records the pre-rows, and the
/// published head is that stage, recorded with its layer and served.
#[test]
fn broken_drive_repair_publish_layers_the_raw_stage_head_and_records_its_pre_rows() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let layer = activate(&fixture);
    let mut owned = stage(fixture.root(), "applied", |_, _| {});
    let applied = apply_drive_repair_layer(&fixture.store, &fixture.gate, &fixture.binding, owned.stage_mut())
        .expect("layer applied")
        .expect("an active layer");
    assert_eq!(applied.layer, layer);
    assert_eq!(applied.components, vec![hex::encode(KEY)]);
    let digest = owned.stage_mut().sealed_content_digest().expect("layered digest");
    let (layered, samples) = rows(&mut owned);
    assert_eq!(layered.keys().copied().collect::<Vec<_>>(), vec![30, 40], "fragments removed");
    let destination = &layered[&30];
    assert_eq!(destination.start_date_pg_us, pg(T));
    assert_eq!(destination.end_date_pg_us, Some(pg(T + 33 * MINUTE)));
    assert_eq!(destination.duration_min, Some(33));
    assert_eq!(destination.start_km.map(|km| f64::from_bits(km.0)), Some(100.0));
    assert_eq!(destination.distance.map(|km| f64::from_bits(km.0)), Some(12.0));
    for id in [100, 200, 300, 301] {
        assert_eq!(samples[&id].drive_id, Some(30), "position {id} under the destination");
    }
    assert_eq!(samples[&400].drive_id, Some(40));

    let ledger = fixture.store.drive_repair_active_layer(vehicle).expect("ledger").expect("active");
    let before = ledger.components[0].before.clone().expect("pre-rows recorded");
    assert_eq!(before.destination, drives()[2]);
    assert_eq!(before.fragments, drives()[..2].to_vec());
    assert_eq!(
        before.samples.iter().map(|row| (row.id, row.drive_id)).collect::<Vec<_>>(),
        vec![(100, Some(10)), (200, Some(20))]
    );
    assert_eq!(before.destination_samples.outside_temp, 0);
    assert_eq!(before.materialised_destination, materialised_drive());
    assert_eq!(
        before.materialised_samples.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![100, 200]
    );

    let publication = publish(&fixture, "published").expect("layered head");
    let snapshot = publication.admission.snapshot_id;
    assert_eq!(snapshot, crate::physical_publication::physical_v3_snapshot_id(digest, &fixture.binding));
    assert_eq!(fixture.store.physical_v3_repair_layer(vehicle, snapshot).expect("record"), Some(applied));
    assert_eq!(served(&fixture), Some(snapshot));
}

/// The served check: a head without a record is served on an empty ledger
/// and refused once a component is active; a head with a record is served
/// only while its `L` is the vehicle's, until a new head is planned.
#[test]
fn broken_drive_repair_publish_served_check_binds_each_head_to_its_layer() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let plain = publish(&fixture, "plain").expect("plain head").admission.snapshot_id;
    assert_eq!(fixture.store.physical_v3_repair_layer(vehicle, plain).expect("record"), None);
    assert_eq!(served(&fixture), Some(plain), "no record, empty ledger: served");

    activate(&fixture);
    assert_eq!(served(&fixture), None, "no record, active ledger: refused");
    assert_eq!(fixture.store.physical_v3_current_head_served(vehicle).expect("served"), Some(false));
    let layered = publish(&fixture, "layered").expect("layered head").admission.snapshot_id;
    assert_ne!(layered, plain);
    assert_eq!(served(&fixture), Some(layered));

    assert!(fixture.store.undo_drive_repair_component(vehicle, &KEY, T).expect("undo"));
    assert_eq!(metadata(&fixture, &format!("drive_repair_layer:{vehicle}")), None);
    assert_eq!(served(&fixture), None, "the old layer's head is refused after a ledger change");
    let replanned = publish_with(&fixture, "replanned", |drives, _| drives[3].speed_max = Some(60))
        .expect("replanned head")
        .admission
        .snapshot_id;
    assert_eq!(served(&fixture), Some(replanned));
}

/// After a ledger change the hand-back's early answer has no record to
/// answer with, so it plans the head again; the layer enters the inputs
/// digest only while a component is active.
#[test]
fn broken_drive_repair_publish_early_answer_replans_after_a_ledger_change() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let head = publish(&fixture, "plain").expect("plain head").admission.snapshot_id;
    let plan = crate::db::PhysicalV3HandbackPlan {
        snapshot_id: head.to_string(),
        span: None,
        inputs: "recorded inputs".into(),
        prepared: true,
    };
    fixture
        .store
        .record_physical_v3_handback_plan(&fixture.gate, vehicle, Some(&plan))
        .expect("record plan");
    assert_eq!(fixture.store.physical_v3_handback_plan(vehicle).expect("plan"), Some(plan.clone()));

    let layer = activate(&fixture);
    assert_eq!(fixture.store.physical_v3_handback_plan(vehicle).expect("plan"), None);
    let kept = crate::db::PhysicalV3Kept {
        stage: "kept stage".into(),
        source: "kept source".into(),
        archive: None,
    };
    let packs = SeamPackPlan::default();
    let stored = crate::db::ImportSeamStoredRows::default();
    let answer =
        serve_unchanged_head(&fixture.store, &fixture.gate, vehicle, &kept, &fixture.binding, &packs, &stored)
            .expect("early answer");
    assert!(answer.is_none(), "the hand-back plans the head again");

    let inputs = handback_plan_inputs("kept stage", &fixture.binding, &packs, None, &[0; 32], T);
    assert_eq!(handback_plan_inputs_layered(inputs.clone(), None), inputs);
    assert_ne!(handback_plan_inputs_layered(inputs.clone(), Some(layer.as_str())), inputs);

    fixture
        .store
        .set_drive_repair_state_for_test(vehicle, &KEY, "undone")
        .expect("deactivate");
    assert_eq!(fixture.store.physical_v3_handback_plan(vehicle).expect("plan"), Some(plan));
}

/// A second application compares the pre-rows by digest: unchanged rows
/// plan the same head; a changed destination row of a settled repair
/// supersedes its component and the head is planned without it, never
/// withheld (REV-CX-RECHECK-T27 P2).
#[test]
fn broken_drive_repair_publish_changed_pre_rows_supersede_the_component() {
    let fixture = fixture();
    activate(&fixture);
    let first = publish(&fixture, "first").expect("layered head").admission.snapshot_id;
    let again = publish(&fixture, "again").expect("same head").admission.snapshot_id;
    assert_eq!(again, first);

    let changed = |drives: &mut Vec<SourceDbDrivePhysicalV2_2>, _: &mut Vec<SourceDbPositionPhysicalV2_2>| {
        drives[2].speed_max = Some(90);
    };
    let mut owned = stage(fixture.root(), "changed-direct", changed);
    let applied = apply_drive_repair_layer(&fixture.store, &fixture.gate, &fixture.binding, owned.stage_mut())
        .expect("the stale component does not refuse the plan");
    assert_eq!(applied, None, "the stale component left the layer");
    assert!(fixture.store.drive_repair_active_layer(fixture.binding.vehicle_id).expect("ledger").is_none());
    let head = publish_with(&fixture, "changed", changed).expect("the head is published").admission.snapshot_id;
    assert_ne!(head, first);
    assert_eq!(served(&fixture), Some(head));
}

/// The frozen head's entry survives the pruning of expired heads' entries.
#[test]
fn broken_drive_repair_publish_frozen_entry_survives_pruning_of_expired_heads() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let layer = activate(&fixture);
    let frozen = publish(&fixture, "frozen").expect("layered head").admission.snapshot_id;
    let base = fixture
        .store
        .freeze_physical_v3_base(&fixture.gate, vehicle, T)
        .expect("freeze the served layered head");
    assert_eq!(base.snapshot_id, frozen);
    let next = publish_with(&fixture, "next", |drives, _| drives[3].speed_max = Some(60))
        .expect("next head")
        .admission
        .snapshot_id;

    // The frozen head and a stale entry are no longer held.
    let connection = raw(&fixture);
    connection
        .execute(
            "DELETE FROM retained_physical_v3_admissions WHERE vehicle_id = ?1 AND snapshot_id = ?2",
            rusqlite::params![vehicle.to_string(), frozen.to_string()],
        )
        .expect("expire the frozen head");
    let stale = Uuid::from_u128(0x57a1e);
    let mut records = layer_records(&fixture);
    records.insert(stale.to_string(), records[&next.to_string()].clone());
    connection
        .execute(
            "UPDATE hub_metadata SET value = ?2 WHERE key = ?1",
            rusqlite::params![
                format!("physical_v3_repair_layer:{vehicle}"),
                serde_json::to_string(&records).expect("records json")
            ],
        )
        .expect("stale entry");

    let last = publish_with(&fixture, "last", |drives, _| drives[3].speed_max = Some(70))
        .expect("last head")
        .admission
        .snapshot_id;
    let records = layer_records(&fixture);
    assert!(records.contains_key(&frozen.to_string()), "the frozen head's entry survives");
    assert!(!records.contains_key(&stale.to_string()), "an expired head's entry is pruned");
    assert!(records.contains_key(&next.to_string()) && records.contains_key(&last.to_string()));
    assert_eq!(
        fixture.store.physical_v3_repair_layer(vehicle, frozen).expect("record").map(|record| record.layer),
        Some(layer)
    );
}

/// An empty ledger leaves the stage, the head, the inputs digest and the
/// store's metadata exactly as before the layer existed.
#[test]
fn broken_drive_repair_publish_empty_ledger_keeps_heads_and_inputs_byte_identical() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let mut owned = stage(fixture.root(), "untouched", |_, _| {});
    let digest = owned.stage_mut().sealed_content_digest().expect("digest");
    let applied = apply_drive_repair_layer(&fixture.store, &fixture.gate, &fixture.binding, owned.stage_mut())
        .expect("no layer");
    assert_eq!(applied, None);
    assert_eq!(owned.stage_mut().sealed_content_digest().expect("digest"), digest);

    let head = publish(&fixture, "plain").expect("plain head").admission.snapshot_id;
    assert_eq!(head, crate::physical_publication::physical_v3_snapshot_id(digest, &fixture.binding));
    assert_eq!(metadata(&fixture, &format!("drive_repair_layer:{vehicle}")), None);
    assert_eq!(metadata(&fixture, &format!("physical_v3_repair_layer:{vehicle}")), None);

    let packs = SeamPackPlan::default();
    let inputs = handback_plan_inputs("kept stage", &fixture.binding, &packs, Some((T, T + MINUTE)), &[7; 32], T);
    assert_eq!(handback_plan_inputs_layered(inputs.clone(), None), inputs);

    // A component that leaves the ledger again leaves no layer key behind.
    activate(&fixture);
    assert!(fixture.store.undo_drive_repair_component(vehicle, &KEY, T).expect("undo"));
    assert_eq!(metadata(&fixture, &format!("drive_repair_layer:{vehicle}")), None);
}

/// REV-CX-T27 P2: with the ledger empty again, a source that now supplies
/// exactly a held repaired head's rows gives that very snapshot. The held
/// head (current and frozen) keeps its recorded layer as its provenance and
/// is served under the empty ledger.
#[test]
fn broken_drive_repair_publish_p2_byte_identical_reimport_keeps_the_held_head_record() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let layer = activate(&fixture);
    let held = publish(&fixture, "repaired").expect("layered head").admission.snapshot_id;
    let mut probe = stage(fixture.root(), "probe", |_, _| {});
    apply_drive_repair_layer(&fixture.store, &fixture.gate, &fixture.binding, probe.stage_mut())
        .expect("layer applied")
        .expect("an active layer");
    let (effective_drives, effective_positions) = rows(&mut probe);
    let base = fixture.store.freeze_physical_v3_base(&fixture.gate, vehicle, T).expect("freeze");
    assert_eq!(base.snapshot_id, held);

    // The component leaves the ledger; the source corrected the rows itself.
    assert_eq!(fixture.store.set_drive_repair_state_for_test(vehicle, &KEY, "superseded").expect("superseded"), None);
    let corrected = |drives: &mut Vec<SourceDbDrivePhysicalV2_2>, positions: &mut Vec<SourceDbPositionPhysicalV2_2>| {
        *drives = effective_drives.values().cloned().collect();
        *positions = effective_positions.values().cloned().collect();
    };
    let mut same = stage(fixture.root(), "same", corrected);
    let digest = same.stage_mut().sealed_content_digest().expect("digest");
    assert_eq!(
        crate::physical_publication::physical_v3_snapshot_id(digest, &fixture.binding),
        held,
        "the corrected source gives the held head's snapshot"
    );
    let _ = publish_with(&fixture, "corrected", corrected);
    assert_eq!(
        fixture.store.physical_v3_repair_layer(vehicle, held).expect("record").map(|record| record.layer),
        Some(layer),
        "the held head keeps its recorded layer"
    );
    assert_eq!(served(&fixture), Some(held), "served under the empty ledger");
}

#[test]
fn pg4_layer_activated_during_build_rejects_the_old_basis() {
    let fixture = fixture();
    let vehicle = fixture.binding.vehicle_id;
    let layer = activate(&fixture);
    let old = publish_physical_route(
        &fixture.store, &fixture.cursor_key, fixture.binding.clone(),
        Some(stage(fixture.root(), "pg4-repair", |_, _| {})),
        &fixture.gate, None, Some((&[], &[])),
    ).expect("publish repaired base").expect("head").admission;
    assert_eq!(served(&fixture), Some(old.snapshot_id));
    fixture.store.set_drive_repair_state_for_test(vehicle, &KEY, "undone")
        .expect("remove the active layer");
    assert_eq!(served(&fixture), None);
    assert_eq!(fixture.store.plan_basis(vehicle).unwrap().repair_layer, None);

    let Fixture { temporary, store, binding: _, gate, cursor_key } = fixture;
    drop(gate);
    let lock = HandbackRunFile::open(store.database_path().parent().unwrap()).expect("run lock");
    let run = lock.try_acquire().unwrap().expect("lock held");
    let stop = std::sync::atomic::AtomicBool::new(false);
    let clock = HandbackGateClock::new(&stop);
    let competing = store.clone();
    let expected_layer = layer.clone();
    let (send, recv) = std::sync::mpsc::channel();
    AFTER_HANDBACK_BUILD.with(|hook| hook.replace(Some(Box::new(move || {
        let _gate = competing.try_acquire_publication_gate().expect("repair gate");
        assert_eq!(
            competing.set_drive_repair_state_for_test(vehicle, &KEY, "active").unwrap(),
            Some(expected_layer.clone()),
        );
        send.send(()).unwrap();
    }))));
    let result = publish_physical_handback(&store, &cursor_key, &run, &clock);
    AFTER_HANDBACK_BUILD.with(|hook| hook.replace(None));
    assert_eq!(recv.try_iter().count(), 1, "the old basis completed BUILD once");
    assert_eq!(result.unwrap(), vec![(vehicle, HandbackPhysicalOutcome::Current)]);
    assert_eq!(store.plan_basis(vehicle).unwrap().repair_layer, Some(layer));
    assert_eq!(
        store.pending_physical_v3_control_admission_for_vehicle(vehicle).unwrap().unwrap(),
        old,
        "only the head bound to the now-active layer is served",
    );
    assert!(!store.has_handback_candidates().unwrap());
    drop(temporary);
}
