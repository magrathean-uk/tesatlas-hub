// SPDX-License-Identifier: AGPL-3.0-only

// M03-T27e: repairs survive backup, restore and reclaim. Plain and encrypted
// backups carry the repair ledger, the layer digest `L`, the head-layer map
// and the pre-rows; the restored Hub serves the repaired head and can undo;
// reclaim, repair and the census leave them alone; the summary counts show
// active and undone repairs without values. Synthetic rows in temporary
// stores; no network and no real credential.

use std::collections::BTreeMap;

use super::*;
use crate::{
    data_recovery::{
        EncryptedBackupRequest,
        archive_crypto::KdfParams,
        backup_admission::{VolumeTrust, test_seam},
        create_encrypted_backup,
        encrypted_backup::clear_hook,
        restore_encrypted_backup,
    },
    db::{DriveRepairTarget, PhysicalV3RepairLayer},
    hub_pack::{
        ProjectionDrive, ProjectionFixedNumericV2_2, ProjectionFloat64BitsV2_2, ProjectionPosition,
    },
    import::broken_drive_repair::{
        PlannedComponent,
        layer::{ComponentPreRows, DestinationSampleCounts, layer_component},
    },
    source_db_projection::{SourceDbDrivePhysicalV2_2, SourceDbPositionPhysicalV2_2},
};

const PASSWORD: &str = "correct horse battery staple";
const TEST_KDF: KdfParams = KdfParams {
    memory_kib: 64,
    passes: 1,
    lanes: 1,
};
const PG_EPOCH_US: i64 = 946_684_800_000_000;
const T: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;
const DESTINATION: i64 = 30;
const KEY: [u8; 32] = [0x27; 32];
const OTHER_KEY: [u8; 32] = [0x28; 32];

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

fn materialised_position(id: i64, at_ms: i64, odometer: f64) -> ProjectionPosition {
    ProjectionPosition {
        id,
        drive_id: None,
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

/// Fragments 10 and 20 with one sample each, joined into destination 30.
fn pre_rows() -> ComponentPreRows {
    ComponentPreRows {
        destination: drive_row(30, T + 3 * MINUTE, Some(T + 33 * MINUTE), 103.0),
        fragments: vec![drive_row(10, T, None, 0.0), drive_row(20, T + MINUTE, None, 0.0)],
        samples: vec![position_row(100, 10, T, 100.0), position_row(200, 20, T + MINUTE, 100.5)],
        destination_samples: DestinationSampleCounts {
            outside_temp: 0,
            inside_temp: 0,
        },
        materialised_destination: materialised_drive(),
        materialised_samples: vec![
            materialised_position(100, T, 100.0),
            materialised_position(200, T + MINUTE, 100.5),
        ],
    }
}

fn planned(pre: &ComponentPreRows) -> PlannedComponent {
    PlannedComponent {
        component_key: KEY,
        input_digest: [0; 32],
        destination_drive_id: DESTINATION,
        fragment_drive_ids: vec![10, 20],
        sample_position_ids: vec![100, 200],
        effective_start_ms: pre.fragments[0].start_date_pg_us,
        changed_rows: 0,
        members: Vec::new(),
    }
}

struct Hub {
    temporary: tempfile::TempDir,
    store: HubStore,
    data: PathBuf,
    vehicle: Uuid,
    heads: Vec<Uuid>,
    layer: PhysicalV3RepairLayer,
    pre: ComponentPreRows,
}

fn raw(data: &Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(data.join("hub.sqlite")).expect("raw catalogue");
    connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("foreign keys off");
    connection
}

fn metadata(data: &Path, key: &str) -> Option<String> {
    use rusqlite::OptionalExtension;
    raw(data)
        .query_row("SELECT value FROM hub_metadata WHERE key = ?1", [key], |row| row.get(0))
        .optional()
        .expect("metadata read")
}

/// Every drive repair ledger table, its rows as strings, in a stable order.
fn ledger(data: &Path) -> BTreeMap<String, Vec<String>> {
    let connection = raw(data);
    let mut dump = BTreeMap::new();
    for table in [
        "drive_repair_changes",
        "drive_repair_change_members",
        "drive_repair_batches",
        "drive_repair_suppressions",
        "drive_repair_inputs",
        "drive_edit_protection",
        "drive_repair_index_heads",
    ] {
        let mut statement = connection.prepare(&format!("SELECT * FROM {table}")).expect("ledger table");
        let columns = statement.column_count();
        let mut rows: Vec<String> = statement
            .query_map([], |row| {
                let mut text = String::new();
                for column in 0..columns {
                    let value: rusqlite::types::Value = row.get(column)?;
                    text.push_str(&format!("{value:?}|"));
                }
                Ok(text)
            })
            .expect("ledger rows")
            .map(|row| row.expect("ledger row"))
            .collect();
        rows.sort();
        dump.insert(table.to_owned(), rows);
    }
    dump
}

fn stored_drive(store: &HubStore, vehicle: Uuid) -> ProjectionDrive {
    let json: String = raw(store.database_path().parent().unwrap())
        .query_row(
            "SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1 AND drive_id = ?2",
            rusqlite::params![vehicle.to_string(), DESTINATION],
            |row| row.get(0),
        )
        .expect("materialised destination");
    serde_json::from_str(&json).expect("drive json")
}

/// A Hub with one repaired destination: the active ledger row with its
/// pre-rows, the materialised after-image, a current head published through
/// the production path and recorded with the layer it was planned with.
/// `retained` adds an earlier head, still held, also recorded with it.
async fn repaired_hub(retained: bool) -> Hub {
    clear_hook();
    let (temporary, store) = create_fixture();
    let data = store.database_path().parent().unwrap().to_path_buf();
    let key = crate::legacy_owner_credentials::load_or_create_cursor_key(&data).expect("key");
    let binding = crate::sync::physical_fragments::tests::registered_admission_binding(&store);
    let vehicle = binding.vehicle_id;
    let pre = pre_rows();
    let after = layer_component(&planned(&pre), &pre, None).expect("after-image").after_image;

    let connection = raw(&data);
    connection
        .execute(
            "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
             VALUES (?1, ?2, 1, ?3)",
            rusqlite::params![
                vehicle.to_string(),
                DESTINATION,
                serde_json::to_string(&after.destination).expect("drive json")
            ],
        )
        .expect("materialised destination");
    for sample in &after.samples {
        connection
            .execute(
                "INSERT INTO materialised_positions(vehicle_id, position_id, drive_id, car_id,
                     position_json) VALUES (?1, ?2, ?3, 1, ?4)",
                rusqlite::params![
                    vehicle.to_string(),
                    sample.id,
                    sample.drive_id,
                    serde_json::to_string(sample).expect("position json")
                ],
            )
            .expect("materialised position");
    }
    drop(connection);

    let now = current_epoch_ms().unwrap();
    store
        .insert_active_drive_repair_for_test(vehicle, &KEY, DESTINATION, &[10, 20, DESTINATION], &[100, 200], T)
        .expect("activate")
        .expect("an active layer");
    let layer = store
        .drive_repair_active_layer(vehicle)
        .expect("ledger")
        .expect("active")
        .layer;
    {
        let gate = store.try_acquire_publication_gate().expect("gate");
        assert!(store.record_drive_repair_pre_rows(&gate, vehicle, &KEY, &pre).expect("pre-rows"));
    }
    let mut heads = Vec::new();
    for changed in if retained { vec![false, true] } else { vec![true] } {
        let (stage, ..) = crate::sync::prepared_map::month_stage_fixture(temporary.path(), now, changed);
        let admission = crate::physical_publication::publish_sealed_physical_v3_stage(
            &store,
            &key,
            binding.clone(),
            stage,
        )
        .await
        .expect("head")
        .admission;
        let gate = store.try_acquire_publication_gate().expect("gate");
        store
            .record_physical_v3_repair_layer(&gate, vehicle, admission.snapshot_id, Some(&layer))
            .expect("head layer");
        heads.push(admission.snapshot_id);
    }
    Hub {
        temporary,
        store,
        data,
        vehicle,
        heads,
        layer,
        pre,
    }
}

impl Hub {
    fn current(&self) -> Uuid {
        *self.heads.last().unwrap()
    }

    fn plain_backup_and_restore(&self) -> PathBuf {
        let backup = self.temporary.path().join("plain-backup");
        create_data_backup(&self.store, &backup).expect("plain backup");
        let restored = self.temporary.path().join("plain-restored");
        restore_data_backup(&backup, &restored).expect("plain restore");
        restored
    }

    fn encrypted_backup_and_restore(&self) -> PathBuf {
        test_seam::force(Some(VolumeTrust::Posix));
        let destination = self.temporary.path().join("encrypted-destination");
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = EncryptedBackupRequest::new(&destination, PASSWORD);
        request.kdf = TEST_KDF;
        request.chunk_bytes = 4096;
        let report = create_encrypted_backup(&self.store, &request).expect("encrypted backup");
        let fresh_host = self.temporary.path().join("fresh-host");
        fs::create_dir(&fresh_host).unwrap();
        fs::set_permissions(&fresh_host, fs::Permissions::from_mode(0o700)).unwrap();
        let restored = fresh_host.join("hub-data");
        restore_encrypted_backup(&report.path, PASSWORD, &restored).expect("encrypted restore");
        restored
    }

    /// What the restored Hub must hold exactly as this one does.
    fn assert_carried(&self, restored: &Path) {
        assert_eq!(ledger(restored), ledger(&self.data), "the ledger tables");
        let layer_key = format!("drive_repair_layer:{}", self.vehicle);
        assert_eq!(metadata(restored, &layer_key), Some(self.layer.layer.clone()));
        let heads_key = format!("physical_v3_repair_layer:{}", self.vehicle);
        assert_eq!(metadata(restored, &heads_key), metadata(&self.data, &heads_key));
        let store = HubStore::initialize(restored).expect("restored store");
        assert_eq!(
            store.drive_repair_active_layer(self.vehicle).expect("ledger"),
            self.store.drive_repair_active_layer(self.vehicle).expect("ledger")
        );
        let layer = store.drive_repair_active_layer(self.vehicle).unwrap().unwrap();
        assert_eq!(layer.components[0].before.as_ref(), Some(&self.pre), "the pre-rows");
        for head in &self.heads {
            assert_eq!(
                store.physical_v3_repair_layer(self.vehicle, *head).expect("record"),
                Some(self.layer.clone())
            );
        }
        assert_eq!(
            store.drive_repair_summary(self.vehicle).unwrap(),
            self.store.drive_repair_summary(self.vehicle).unwrap()
        );
    }

    /// The restored Hub serves the repaired head, and an undo restores the
    /// pre-image and withdraws the head the old layer planned.
    fn assert_serves_and_undoes(&self, restored: &Path) {
        let store = HubStore::initialize(restored).expect("restored store");
        let served = store
            .pending_physical_v3_control_admission_for_vehicle(self.vehicle)
            .expect("served read")
            .map(|admission| admission.snapshot_id);
        assert_eq!(served, Some(self.current()), "the repaired head is served");
        assert_eq!(store.physical_v3_current_head_served(self.vehicle).expect("served"), Some(true));

        let gate = store.try_acquire_publication_gate().expect("gate");
        let report = store
            .undo_drive_repairs(&gate, self.vehicle, DriveRepairTarget::Component(&KEY), T + MINUTE)
            .expect("undo");
        assert_eq!(report.components, 1);
        assert_eq!(report.restored_rows, 3, "the destination and both samples");
        assert_eq!(stored_drive(&store, self.vehicle), self.pre.materialised_destination);
        assert_eq!(store.drive_repair_summary(self.vehicle).unwrap().active, 0);
        assert_eq!(store.drive_repair_summary(self.vehicle).unwrap().undone, 1);
        assert_eq!(metadata(restored, &format!("drive_repair_layer:{}", self.vehicle)), None);
        assert_eq!(
            store.physical_v3_current_head_served(self.vehicle).expect("served"),
            Some(false),
            "the head of the undone layer is refused"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_plain_restore_carries_the_ledger_and_serves_the_head() {
    let hub = repaired_hub(false).await;
    let restored = hub.plain_backup_and_restore();
    hub.assert_carried(&restored);
    hub.assert_serves_and_undoes(&restored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_encrypted_restore_carries_the_ledger_and_serves_the_head() {
    let hub = repaired_hub(false).await;
    let restored = hub.encrypted_backup_and_restore();
    hub.assert_carried(&restored);
    hub.assert_serves_and_undoes(&restored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_restore_keeps_the_layer_of_a_retained_head() {
    let hub = repaired_hub(true).await;
    assert_eq!(hub.heads.len(), 2);
    let restored = hub.plain_backup_and_restore();
    hub.assert_carried(&restored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_reclaim_repair_and_census_keep_what_a_held_head_names() {
    let hub = repaired_hub(true).await;
    let before = (
        ledger(&hub.data),
        metadata(&hub.data, &format!("drive_repair_layer:{}", hub.vehicle)),
        metadata(&hub.data, &format!("physical_v3_repair_layer:{}", hub.vehicle)),
    );
    hub.store.storage_census().expect("census");
    hub.store.reclaim_storage().expect("reclaim");
    hub.store.repair().expect("repair");
    hub.store.reclaim_storage().expect("second reclaim");
    let after = (
        ledger(&hub.data),
        metadata(&hub.data, &format!("drive_repair_layer:{}", hub.vehicle)),
        metadata(&hub.data, &format!("physical_v3_repair_layer:{}", hub.vehicle)),
    );
    assert_eq!(after, before);
    let layer = hub.store.drive_repair_active_layer(hub.vehicle).unwrap().unwrap();
    assert_eq!(layer.components[0].before.as_ref(), Some(&hub.pre), "the pre-rows");
    for head in &hub.heads {
        assert_eq!(
            hub.store.physical_v3_repair_layer(hub.vehicle, *head).unwrap(),
            Some(hub.layer.clone())
        );
    }
    assert_eq!(
        hub.store.physical_v3_current_head_served(hub.vehicle).unwrap(),
        Some(true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_maintenance_after_restore_keeps_the_restored_repair() {
    let hub = repaired_hub(false).await;
    let restored = hub.plain_backup_and_restore();
    let store = HubStore::initialize(&restored).expect("restored store");
    store.storage_census().expect("census");
    store.reclaim_storage().expect("reclaim");
    store.repair().expect("repair");
    assert_eq!(ledger(&restored), ledger(&hub.data));
    assert_eq!(
        store.physical_v3_repair_layer(hub.vehicle, hub.current()).unwrap(),
        Some(hub.layer.clone())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_summary_counts_active_and_undone_without_values() {
    let hub = repaired_hub(false).await;
    hub.store
        .insert_active_drive_repair_for_test(hub.vehicle, &OTHER_KEY, 31, &[11, 21, 31], &[101, 201], T)
        .expect("second component")
        .expect("a layer");
    hub.store
        .set_drive_repair_state_for_test(hub.vehicle, &OTHER_KEY, "undone")
        .expect("undone");
    let summary = hub.store.drive_repair_summary(hub.vehicle).expect("summary");
    assert_eq!((summary.active, summary.undone), (1, 1));
    let restored = hub.plain_backup_and_restore();
    let store = HubStore::initialize(&restored).expect("restored store");
    assert_eq!(store.drive_repair_summary(hub.vehicle).unwrap(), summary);
    let shown = format!("{summary:?}");
    assert!(!shown.contains("47.5") && !shown.contains("19.0"), "counts only: {shown}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_drive_repair_recovery_backup_tolerates_a_transient_import_capture_key() {
    let hub = repaired_hub(false).await;
    raw(&hub.data)
        .execute(
            "INSERT INTO hub_metadata(key, value) VALUES(?1, 'capture')",
            [format!("drive_repair_import_capture:{}", Uuid::new_v4())],
        )
        .expect("capture key");
    let restored = hub.plain_backup_and_restore();
    let store = HubStore::initialize(&restored).expect("restored store");
    assert_eq!(
        store.drive_repair_active_layer(hub.vehicle).unwrap(),
        hub.store.drive_repair_active_layer(hub.vehicle).unwrap()
    );
    assert_eq!(
        store.physical_v3_repair_layer(hub.vehicle, hub.current()).unwrap(),
        Some(hub.layer.clone())
    );
}
