// SPDX-License-Identifier: AGPL-3.0-only

// Issue #106, task b: a fresh compatibility base folded from the served
// lineage. Each case builds a real catalogued lineage (an imported base,
// import successors and collector deltas through the store's own paths),
// asks for a candidate and compares its decoded packs with an independent
// applier over the typed rows the lineage was written from. The catalogue
// must be byte-for-byte the same after every case. This file is
// `include!`d into a test module of `v2_lineage_rebase.rs`.

use std::collections::{BTreeMap, BTreeSet};

use crate::hub_pack::{
    ProjectionCarSettingsPatch, ProjectionCharge, ProjectionChargeSample,
    ProjectionDeltaPackRequest, ProjectionState, ProjectionUpdate,
    signed_full_snapshot_manifest,
};
use crate::protocol::{CursorClaims, HUB_PROJECTION_SCHEMA_V1, ProtocolVersion};

type FoldTestRow = serde_json::Map<String, serde_json::Value>;

fn fold_test_key() -> CursorKey {
    CursorKey::from_bytes([67; 32])
}

fn fold_test_cursor(binding: &ProjectionBinding, sequence: u64) -> OpaqueCursor {
    OpaqueCursor::issue(
        &fold_test_key(),
        CursorClaims {
            protocol: ProtocolVersion { major: 1, minor: 0 },
            schema: HUB_PROJECTION_SCHEMA_V2,
            installation_id: binding.installation_id,
            account_id: binding.account_id,
            vehicle_id: binding.vehicle_id,
            generation: binding.generation,
            sequence,
        },
    )
    .expect("fixture cursor")
}

fn fold_test_settings() -> ProjectionCarSettings {
    ProjectionCarSettings {
        suspend_min: 33,
        lfp_battery: true,
        ..ProjectionCarSettings::new_live()
    }
}

fn fold_test_car(car_id: i64, name: &str) -> ProjectionCar {
    ProjectionCar {
        id: car_id,
        name: name.into(),
        model: "Model 3".into(),
        vin: None,
        source_eid: None,
        source_vid: None,
        trim_badging: None,
        marketing_name: None,
        exterior_color: None,
        wheel_type: None,
        spoiler_type: None,
        source_model_text: None,
        firmware_version: Some("2024.8.7".into()),
        efficiency_wh_per_km: Some(0.153),
        settings: fold_test_settings(),
    }
}

fn fold_test_drive(id: i64, car_id: i64) -> ProjectionDrive {
    let start = 1_700_000_000_000 + id * 3_600_000;
    ProjectionDrive {
        id,
        car_id,
        optimized_at_ms: None,
        start_date_ms: start,
        end_date_ms: start + 600_000,
        distance_km: Some(12.5 + id as f64 * 0.1),
        duration_min: Some(10),
        efficiency: Some(0.17),
        outside_temp_avg: Some(11.25),
        inside_temp_avg: None,
        speed_max: Some(90),
        power_max: None,
        power_min: None,
        start_ideal_range_km: None,
        end_ideal_range_km: None,
        start_address: Some("Start".into()),
        end_address: None,
        start_geofence: None,
        end_geofence: None,
        start_latitude: Some(51.5 + id as f64 * 0.001),
        start_longitude: Some(-0.12),
        end_latitude: Some(51.6),
        end_longitude: Some(-0.13),
        start_soc: Some(80),
        end_soc: Some(75),
        start_rated_range_km: Some(300.5),
        end_rated_range_km: Some(280.25),
        ascent: None,
        descent: None,
    }
}

fn fold_test_position(id: i64, drive_id: Option<i64>, car_id: i64) -> ProjectionPosition {
    ProjectionPosition {
        id,
        drive_id,
        car_id,
        date_ms: 1_700_000_000_000 + id * 1_000,
        latitude: 51.5 + id as f64 * 0.0001,
        longitude: -0.12 - id as f64 * 0.0001,
        speed: Some(50),
        power: Some(12.0),
        battery_level: Some(70),
        usable_battery_level: Some(69),
        elevation: Some(30),
        odometer: Some(10_000.0 + id as f64 * 0.3),
        ideal_battery_range_km: Some(250.5),
        est_battery_range_km: None,
        rated_battery_range_km: Some(260.75),
        fan_status: None,
        driver_temp_setting: None,
        passenger_temp_setting: None,
        is_climate_on: Some(true),
        is_rear_defroster_on: None,
        is_front_defroster_on: None,
        inside_temp: Some(20.5),
        outside_temp: Some(9.75),
        battery_heater: None,
        battery_heater_on: None,
        battery_heater_no_power: None,
        tpms_pressure_fl: None,
        tpms_pressure_fr: None,
        tpms_pressure_rl: None,
        tpms_pressure_rr: None,
    }
}

fn fold_test_charge(id: i64, car_id: i64) -> ProjectionCharge {
    let start = 1_700_100_000_000 + id * 3_600_000;
    ProjectionCharge {
        id,
        car_id,
        start_date_ms: start,
        end_date_ms: Some(start + 1_800_000),
        charge_energy_added: Some(20.25 + id as f64 * 0.1),
        charge_energy_used_kwh: None,
        start_ideal_range_km: None,
        end_ideal_range_km: None,
        cost: None,
        fast_charger_type: None,
        billing_type: None,
        cost_per_unit: None,
        session_fee: None,
        start_latitude: None,
        start_longitude: None,
        start_battery_level: Some(40),
        end_battery_level: Some(80),
        duration_min: Some(30),
        address: Some("Home".into()),
        location_name: None,
        geofence: None,
        is_dc: Some(false),
        charge_rate_km_per_hour: Some(45.5),
        max_charger_power_kw: Some(11.0),
        outside_temp_avg: Some(8.5),
        start_rated_range_km: Some(150.0),
        end_rated_range_km: Some(300.0),
    }
}

fn fold_test_sample(id: i64, charge_process_id: i64) -> ProjectionChargeSample {
    ProjectionChargeSample {
        id,
        charge_process_id,
        timestamp_ms: 1_700_100_000_000 + id * 60_000,
        battery_level: Some(50),
        usable_battery_level: Some(49),
        charge_energy_added_kwh: Some(1.5 + id as f64 * 0.01),
        charger_power_kw: Some(11.0),
        charger_voltage: Some(230.0),
        charger_actual_current: Some(16.0),
        charger_pilot_current: Some(16.0),
        charger_phases: Some(3),
        ideal_range_km: None,
        rated_range_km: Some(200.0),
        outside_temp_c: Some(8.0),
        battery_heater_on: Some(false),
        battery_heater: None,
        battery_heater_no_power: None,
        not_enough_power_to_heat: None,
        fast_charger_present: Some(false),
        fast_charger_brand: None,
        fast_charger_type: None,
        charge_cable: Some("IEC".into()),
    }
}

fn fold_test_state(id: i64, car_id: i64) -> ProjectionState {
    let start = 1_700_200_000_000 + id * 1_000;
    ProjectionState {
        id,
        car_id,
        state: "online".into(),
        start_date_ms: start,
        end_date_ms: Some(start + 500),
    }
}

fn fold_test_update(id: i64, car_id: i64) -> ProjectionUpdate {
    let start = 1_700_300_000_000 + id * 1_000;
    ProjectionUpdate {
        id,
        car_id,
        start_date_ms: start,
        end_date_ms: start + 500,
        version: "2024.8.7".into(),
    }
}

/// One base pack's typed content.
#[derive(Clone)]
struct FoldTestFragment {
    snapshot: ProjectionSnapshot,
    states: Vec<ProjectionState>,
    updates: Vec<ProjectionUpdate>,
}

fn fold_test_fragment(
    car: ProjectionCar,
    drives: Vec<ProjectionDrive>,
    positions: Vec<ProjectionPosition>,
    charges: Vec<ProjectionCharge>,
    charge_samples: Vec<ProjectionChargeSample>,
) -> FoldTestFragment {
    FoldTestFragment {
        snapshot: ProjectionSnapshot {
            cars: vec![car],
            drives,
            positions,
            charges,
            charge_samples,
        },
        states: Vec::new(),
        updates: Vec::new(),
    }
}

/// The three-pack imported base: a drive repeated as context across packs,
/// a loose position (schema 2.1 only), charges with samples, states and
/// updates (schema 2.1 only).
fn fold_test_base_fragments(car_id: i64, schema_2_0: bool) -> Vec<FoldTestFragment> {
    let car = fold_test_car(car_id, "Fold fixture");
    let mut first = fold_test_fragment(
        car.clone(),
        vec![fold_test_drive(1, car_id)],
        (1..=3)
            .map(|id| fold_test_position(id, Some(1), car_id))
            .collect(),
        Vec::new(),
        Vec::new(),
    );
    if !schema_2_0 {
        first
            .snapshot
            .positions
            .push(fold_test_position(21, None, car_id));
    }
    let second = fold_test_fragment(
        car.clone(),
        vec![fold_test_drive(1, car_id), fold_test_drive(2, car_id)],
        vec![
            fold_test_position(4, Some(1), car_id),
            fold_test_position(5, Some(1), car_id),
            fold_test_position(6, Some(2), car_id),
            fold_test_position(7, Some(2), car_id),
        ],
        Vec::new(),
        Vec::new(),
    );
    let mut third = fold_test_fragment(
        car,
        Vec::new(),
        Vec::new(),
        vec![fold_test_charge(1, car_id), fold_test_charge(2, car_id)],
        vec![
            fold_test_sample(1, 1),
            fold_test_sample(2, 1),
            fold_test_sample(3, 2),
        ],
    );
    if !schema_2_0 {
        third.states = vec![fold_test_state(1, car_id), fold_test_state(2, car_id)];
        third.updates = vec![fold_test_update(1, car_id)];
    }
    vec![first, second, third]
}

/// Catalogues `fragments` as the vehicle's imported base through the store's
/// import finaliser and returns the served lineage.
fn fold_test_imported_base(
    store: &HubStore,
    schema_2_0: bool,
    fragments: impl FnOnce(i64) -> Vec<FoldTestFragment>,
) -> (ProjectionBinding, LineageManifestV2) {
    let source = store
        .register_source(&SourceDescriptor::new("source_db_import", "fold-fixture"), 1_000)
        .expect("fixture source");
    let vehicle = store
        .register_vehicle(
            &VehicleDescriptor::new(source.source_id, "10").with_tesla_identity(Some(70), None),
            1_001,
        )
        .expect("fixture vehicle");
    let binding = store
        .v2_projection_binding(vehicle.vehicle_id)
        .expect("fixture binding");
    let snapshot_id = Uuid::new_v4();
    let range = SequenceRange {
        from_exclusive: 1,
        to_inclusive: 1,
    };
    let writer = ProjectionPackWriter::new(store.packs_dir());
    let mut packs = Vec::new();
    for (ordinal, fragment) in fragments(binding.selected_car_id).iter().enumerate() {
        let request = ProjectionPackRequest {
            pack_id: Uuid::new_v4(),
            snapshot_id,
            ordinal: ordinal as u32,
            binding: binding.clone(),
            sequence: range,
            snapshot: &fragment.snapshot,
        };
        let built = if schema_2_0 {
            writer.write_full_snapshot(&request)
        } else {
            writer.write_full_snapshot_with_states_and_updates(
                &request,
                &fragment.states,
                &fragment.updates,
            )
        }
        .expect("fixture base pack");
        packs.push(built);
    }
    let total_rows = packs.iter().map(|pack| pack.metadata.row_count).sum();
    let manifest =
        signed_full_snapshot_manifest(&binding, snapshot_id, range, &packs, total_rows, &fold_test_key())
            .expect("fixture base manifest");
    if schema_2_0 {
        fold_test_catalogue_schema_2_0_base(store, &manifest, &binding);
    } else {
        store
            .finalize_import_snapshot_with_binding(
                &manifest,
                Sha256Digest::of_bytes(b"fold-fixture-base"),
                &[],
                &binding,
            )
            .expect("fixture base catalogue");
    }
    let lineage = store
        .lineage_manifest_for_vehicle(vehicle.vehicle_id)
        .expect("fixture lineage")
        .expect("fixture lineage catalogued");
    (binding, lineage)
}

/// Catalogues a schema-2.0 base as an earlier Hub left it: today's
/// finalisers catalogue only 2.1 bases (`validate_immutable_v2_base_binding`),
/// but a store upgraded from a 2.0 Hub still serves one. The rows are the
/// ones `publish_manifest_in_transaction` writes for a typed base, with the
/// binding through the store's own recorder.
fn fold_test_catalogue_schema_2_0_base(
    store: &HubStore,
    manifest: &SyncManifest,
    binding: &ProjectionBinding,
) {
    assert_eq!(manifest.schema, HUB_PROJECTION_SCHEMA_V1);
    let mut connection = store.open().unwrap();
    let transaction = connection.transaction().unwrap();
    let snapshot_id = manifest.snapshot_id.to_string();
    let vehicle_id = manifest.vehicle_id.to_string();
    let head_sequence = i64::try_from(manifest.head_sequence).unwrap();
    let base_digest = manifest.chunks[0].sha256.to_string();
    transaction
        .execute(
            "INSERT INTO sync_manifests(snapshot_id, vehicle_id, head_sequence, manifest_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot_id,
                vehicle_id,
                head_sequence,
                serde_json::to_vec(manifest).unwrap()
            ],
        )
        .unwrap();
    for pack in &manifest.chunks {
        transaction
            .execute(
                "INSERT INTO sync_packs(sha256, snapshot_id, ordinal, relative_path,
                    compressed_bytes, uncompressed_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    pack.sha256.to_string(),
                    snapshot_id,
                    i64::from(pack.ordinal),
                    pack.relative_path,
                    i64::try_from(pack.compressed_bytes).unwrap(),
                    i64::try_from(pack.uncompressed_bytes).unwrap()
                ],
            )
            .unwrap();
    }
    transaction
        .execute(
            "INSERT INTO sync_bases(vehicle_id, snapshot_id, base_sequence, base_digest, packs_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                vehicle_id,
                snapshot_id,
                head_sequence,
                base_digest,
                serde_json::to_vec(&manifest.chunks).unwrap()
            ],
        )
        .unwrap();
    record_immutable_v2_base_binding_values_in_transaction(
        &transaction,
        manifest.vehicle_id,
        manifest.snapshot_id,
        binding,
    )
    .expect("fixture base binding");
    transaction
        .execute(
            "INSERT INTO sync_heads(vehicle_id, base_snapshot_id, head_sequence, head_digest,
                terminal_cursor)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                vehicle_id,
                snapshot_id,
                head_sequence,
                base_digest,
                serde_json::to_string(&manifest.terminal_cursor).unwrap()
            ],
        )
        .unwrap();
    transaction.commit().unwrap();
}

fn fold_test_delta(binding: &ProjectionBinding) -> ProjectionDelta {
    ProjectionDelta {
        binding: binding.clone(),
        sequence: SequenceRange {
            from_exclusive: 0,
            to_inclusive: 1,
        },
        parent_digest: Sha256Digest::of_bytes(b"placeholder"),
        cars: Vec::new(),
        car_settings: Vec::new(),
        drives: Vec::new(),
        positions: Vec::new(),
        charges: Vec::new(),
        charge_samples: Vec::new(),
        states: Vec::new(),
        updates: Vec::new(),
        tombstones: Vec::new(),
    }
}

fn fold_test_tombstone(entity: ProjectionDeltaEntity, id: i64, car_id: i64) -> ProjectionTombstone {
    ProjectionTombstone {
        entity,
        id,
        car_id,
    }
}

/// Writes `rows` as the next delta pack of `lineage` and returns its entry.
fn fold_test_written_delta(
    store: &HubStore,
    lineage: &LineageManifestV2,
    rows: &ProjectionDelta,
) -> LineageDelta {
    let payload = ProjectionDelta {
        sequence: SequenceRange {
            from_exclusive: lineage.head_sequence,
            to_inclusive: lineage.head_sequence + 1,
        },
        parent_digest: lineage.head_digest,
        ..rows.clone()
    };
    let written = ProjectionPackWriter::new(store.packs_dir())
        .write_delta(&ProjectionDeltaPackRequest {
            pack_id: Uuid::new_v4(),
            snapshot_id: lineage.base.snapshot_id,
            ordinal: (lineage.base.packs.len() + lineage.deltas.len()) as u32,
            delta: &payload,
        })
        .expect("fixture delta pack");
    LineageDelta {
        from_sequence: payload.sequence.from_exclusive,
        to_sequence: payload.sequence.to_inclusive,
        parent_chain_digest: lineage.head_digest,
        chain_digest: canonical_delta_chain_digest(lineage.head_digest, written.metadata.sha256),
        pack_digest: written.metadata.sha256,
        pack: written.metadata,
    }
}

/// Publishes `rows` through the import successor path.
fn fold_test_import_delta(
    store: &HubStore,
    binding: &ProjectionBinding,
    lineage: &LineageManifestV2,
    rows: &ProjectionDelta,
) -> LineageManifestV2 {
    let delta = fold_test_written_delta(store, lineage, rows);
    store
        .finalize_import_delta_successor(
            binding.vehicle_id,
            &delta,
            &fold_test_key(),
            &fold_test_cursor(binding, delta.to_sequence),
            Sha256Digest::of_bytes(&delta.to_sequence.to_le_bytes()),
            &[],
        )
        .expect("fixture import successor");
    store
        .lineage_manifest_for_vehicle(binding.vehicle_id)
        .unwrap()
        .unwrap()
}

/// Publishes `rows` through the collector's claim path, as one queued change.
fn fold_test_collector_delta(
    store: &HubStore,
    binding: &ProjectionBinding,
    lineage: &LineageManifestV2,
    rows: &ProjectionDelta,
) -> LineageManifestV2 {
    let connection = store.open().unwrap();
    let revision: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(revision), 0) + 1 FROM sync_mutations WHERE vehicle_id = ?1",
            params![binding.vehicle_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO sync_mutations
                (vehicle_id, revision, entity, entity_id, car_id, operation, payload_json)
             VALUES (?1, ?2, 'car_setting', ?3, ?3, 'upsert', '{}')",
            params![binding.vehicle_id.to_string(), revision, binding.selected_car_id],
        )
        .unwrap();
    drop(connection);
    let claim = store
        .claim_sync_mutations(binding.vehicle_id, 1_800_000_000_000, 1)
        .unwrap()
        .expect("fixture claim");
    let delta = fold_test_written_delta(store, lineage, rows);
    store
        .commit_v2_delta_claim(
            &claim,
            &delta,
            &fold_test_key(),
            &fold_test_cursor(binding, delta.to_sequence),
        )
        .expect("fixture collector delta");
    store
        .lineage_manifest_for_vehicle(binding.vehicle_id)
        .unwrap()
        .unwrap()
}

/// The independent applier: rows by (table, key) as JSON objects, built
/// from the typed inputs alone.
#[derive(Debug, Default, PartialEq)]
struct FoldTestMirror(BTreeMap<(String, i64), FoldTestRow>);

fn fold_test_json<V: serde::Serialize>(value: &V, drop: &[&str]) -> FoldTestRow {
    let serde_json::Value::Object(mut row) = serde_json::to_value(value).unwrap() else {
        panic!("typed rows serialise as objects");
    };
    for key in drop {
        row.remove(*key);
    }
    for value in row.values_mut() {
        if let serde_json::Value::Bool(flag) = value {
            *value = serde_json::json!(i64::from(*flag));
        }
    }
    row
}

impl FoldTestMirror {
    fn put<V: serde::Serialize>(&mut self, table: &str, key: i64, value: &V, drop: &[&str]) {
        self.0
            .insert((table.to_owned(), key), fold_test_json(value, drop));
    }

    fn car(&mut self, car: &ProjectionCar, with_settings: bool) {
        self.put("cars", car.id, car, &["settings", "source_model_text"]);
        if with_settings {
            self.settings(car.id, &car.settings);
        }
    }

    fn settings(&mut self, car_id: i64, settings: &ProjectionCarSettings) {
        let mut row = fold_test_json(settings, &[]);
        row.insert("car_id".into(), serde_json::json!(car_id));
        self.0.insert(("car_settings".into(), car_id), row);
    }

    fn base(&mut self, fragments: &[FoldTestFragment], schema_2_0: bool) {
        for fragment in fragments {
            for car in &fragment.snapshot.cars {
                self.car(car, !schema_2_0);
            }
            for row in &fragment.snapshot.drives {
                self.put("drives", row.id, row, &[]);
            }
            for row in &fragment.snapshot.positions {
                self.put("positions", row.id, row, &[]);
            }
            for row in &fragment.snapshot.charges {
                self.put("charges", row.id, row, &[]);
            }
            for row in &fragment.snapshot.charge_samples {
                self.put("charge_samples", row.id, row, &[]);
            }
            for row in &fragment.states {
                self.put("states", row.id, row, &[]);
            }
            for row in &fragment.updates {
                self.put("updates", row.id, row, &[]);
            }
        }
    }

    fn delta(&mut self, delta: &ProjectionDelta) {
        for car in &delta.cars {
            self.car(car, true);
        }
        for patch in &delta.car_settings {
            self.settings(patch.car_id, &patch.settings);
        }
        for row in &delta.drives {
            self.put("drives", row.id, row, &[]);
        }
        for row in &delta.positions {
            self.put("positions", row.id, row, &[]);
        }
        for row in &delta.charges {
            self.put("charges", row.id, row, &[]);
        }
        for row in &delta.charge_samples {
            self.put("charge_samples", row.id, row, &[]);
        }
        for row in &delta.states {
            self.put("states", row.id, row, &[]);
        }
        for row in &delta.updates {
            self.put("updates", row.id, row, &[]);
        }
        for tombstone in &delta.tombstones {
            let table = match tombstone.entity {
                ProjectionDeltaEntity::Drive => "drives",
                ProjectionDeltaEntity::Position => "positions",
                ProjectionDeltaEntity::Charge => "charges",
                ProjectionDeltaEntity::ChargeSample => "charge_samples",
                ProjectionDeltaEntity::State => "states",
                ProjectionDeltaEntity::Update => "updates",
                other => panic!("no tombstone fixture for {other:?}"),
            };
            self.0.remove(&(table.to_owned(), tombstone.id));
        }
    }

    fn count(&self, table: &str) -> usize {
        self.0.keys().filter(|(name, _)| name == table).count()
    }
}

const FOLD_TEST_TABLES: [(&str, &str); 8] = [
    ("cars", "id"),
    ("car_settings", "car_id"),
    ("drives", "id"),
    ("charges", "id"),
    ("positions", "id"),
    ("charge_samples", "id"),
    ("states", "id"),
    ("updates", "id"),
];

/// Decodes every candidate pack as a client would and merges the rows; a
/// row repeated across packs must be identical.
fn fold_test_decoded(candidate: &V2LineageRebaseCandidate) -> FoldTestMirror {
    let scratch = crate::private_tempdir().expect("decode directory");
    let mut mirror = FoldTestMirror::default();
    for (index, pack) in candidate.packs.iter().enumerate() {
        assert_eq!(pack.metadata.schema, HUB_PROJECTION_SCHEMA_V2);
        let bytes = zstd::stream::decode_all(File::open(&pack.path).expect("candidate pack"))
            .expect("candidate pack decodes");
        let path = scratch.path().join(format!("{index}.sqlite"));
        fs::write(&path, bytes).unwrap();
        let connection = Connection::open(&path).unwrap();
        for (table, key) in FOLD_TEST_TABLES {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table}"))
                .unwrap();
            let names = statement
                .column_names()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let mut rows = statement.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let mut decoded = FoldTestRow::new();
                for (index, name) in names.iter().enumerate() {
                    let value = match row.get_ref(index).unwrap() {
                        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                        rusqlite::types::ValueRef::Integer(value) => serde_json::json!(value),
                        rusqlite::types::ValueRef::Real(value) => serde_json::Value::Number(
                            serde_json::Number::from_f64(value).expect("finite"),
                        ),
                        rusqlite::types::ValueRef::Text(value) => {
                            serde_json::json!(std::str::from_utf8(value).unwrap())
                        }
                        rusqlite::types::ValueRef::Blob(_) => panic!("no blob columns"),
                    };
                    decoded.insert(name.clone(), value);
                }
                let id = decoded[key].as_i64().expect("integer key");
                if let Some(held) = mirror.0.get(&(table.to_owned(), id)) {
                    assert_eq!(held, &decoded, "repeated {table} {id} differs across packs");
                }
                mirror.0.insert((table.to_owned(), id), decoded);
            }
        }
    }
    mirror
}

/// Every catalogue row, so a case can prove it changed nothing.
fn fold_test_catalogue(store: &HubStore) -> Vec<String> {
    let connection = store.open_read_only_connection().unwrap();
    let tables = connection
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut dump = Vec::new();
    for table in tables {
        let mut statement = connection.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
        let width = statement.column_count();
        let mut rows = statement
            .query_map([], |row| {
                (0..width)
                    .map(|index| row.get::<_, rusqlite::types::Value>(index))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .map(|row| format!("{table}: {:?}", row.unwrap()))
            .collect::<Vec<_>>();
        rows.sort();
        dump.extend(rows);
    }
    dump
}

/// The content-addressed packs on disk, and any fold scratch left behind.
fn fold_test_files(store: &HubStore) -> (BTreeSet<String>, Vec<String>) {
    let names = |directory: PathBuf| -> Vec<String> {
        fs::read_dir(directory)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    let packs = names(store.packs_dir().join("sha256")).into_iter().collect();
    let scratch = names(store.packs_dir().join(".staging"))
        .into_iter()
        .filter(|name| name.contains("lineage-fold"))
        .collect();
    (packs, scratch)
}

fn fold_test_build(
    store: &HubStore,
    vehicle_id: Uuid,
    limits: V2LineageRebaseLimits,
) -> Result<V2LineageRebaseCandidate, StoreError> {
    let gate = store.try_acquire_publication_gate().expect("publication gate");
    store.build_v2_lineage_rebase(&gate, vehicle_id, &fold_test_key(), limits, &|| false)
}

fn fold_test_discard(store: &HubStore, candidate: V2LineageRebaseCandidate) {
    let gate = store.try_acquire_publication_gate().expect("publication gate");
    store
        .discard_v2_lineage_rebase(&gate, candidate)
        .expect("discard candidate");
}

/// A refusal leaves the catalogue, the served packs and the staging area as
/// they were.
fn fold_test_assert_refused_unchanged(
    store: &HubStore,
    lineage: &LineageManifestV2,
    catalogue: &[String],
    files: &(BTreeSet<String>, Vec<String>),
) {
    assert_eq!(fold_test_catalogue(store), catalogue);
    assert_eq!(&fold_test_files(store), files);
    assert!(fold_test_files(store).1.is_empty());
    assert_eq!(
        store
            .lineage_manifest_for_vehicle_with_verification(
                lineage.vehicle_id,
                LineagePackVerification::MetadataOnly,
            )
            .unwrap()
            .as_ref(),
        Some(lineage)
    );
}

/// A successful build: the candidate equals `expected`, is signed for a
/// marker after the served head, changed no catalogue row and its packs go
/// away on discard.
fn fold_test_assert_candidate(
    store: &HubStore,
    lineage: &LineageManifestV2,
    limits: V2LineageRebaseLimits,
    expected: &FoldTestMirror,
) -> FoldTestMirror {
    let catalogue = fold_test_catalogue(store);
    let files = fold_test_files(store);
    let candidate = fold_test_build(store, lineage.vehicle_id, limits).expect("fresh base");
    assert_eq!(candidate.vehicle_id, lineage.vehicle_id);
    assert_eq!(
        candidate.binding,
        store.v2_projection_binding(lineage.vehicle_id).unwrap()
    );
    assert_eq!(&candidate.expected, lineage);
    candidate.manifest.validate().expect("valid signed base manifest");
    assert_eq!(candidate.manifest.chunks.len(), candidate.packs.len());
    assert!(candidate.manifest.head_sequence > lineage.head_sequence);
    assert_eq!(candidate.manifest.base_sequence, candidate.manifest.head_sequence);
    assert!(candidate.packs.len() <= limits.base_pack_limit());
    let decoded = fold_test_decoded(&candidate);
    assert_eq!(&decoded, expected);
    for (table, count) in &candidate.rows.0 {
        assert_eq!(*count as usize, expected.count(table), "{table} rows");
    }
    assert_eq!(fold_test_catalogue(store), catalogue);
    assert!(fold_test_files(store).1.is_empty());
    fold_test_discard(store, candidate);
    assert_eq!(fold_test_files(store), files);
    decoded
}

fn fold_test_small_packs() -> V2LineageRebaseLimits {
    V2LineageRebaseLimits {
        rows_per_pack: 4,
        ..V2LineageRebaseLimits::default()
    }
}

#[test]
fn design_later_106_b_fold_equals_an_independent_applier_with_imported_history() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let mut fragments = Vec::new();
    let (binding, lineage) = fold_test_imported_base(&store, false, |car_id| {
        fragments = fold_test_base_fragments(car_id, false);
        fragments.clone()
    });
    let car_id = binding.selected_car_id;
    let mut expected = FoldTestMirror::default();
    expected.base(&fragments, false);
    assert!(expected.count("positions") > 0 && expected.count("charges") > 0);
    // The imported base alone, in one pack and in packs of four rows.
    fold_test_assert_candidate(&store, &lineage, V2LineageRebaseLimits::default(), &expected);
    fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);

    let mut successor = fold_test_delta(&binding);
    successor.positions = vec![fold_test_position(30, Some(2), car_id)];
    successor.charge_samples = vec![fold_test_sample(31, 2)];
    let lineage = fold_test_import_delta(&store, &binding, &lineage, &successor);
    expected.delta(&successor);
    let mut collected = fold_test_delta(&binding);
    collected.states = vec![fold_test_state(3, car_id)];
    let lineage = fold_test_collector_delta(&store, &binding, &lineage, &collected);
    expected.delta(&collected);
    assert_eq!(lineage.deltas.len(), 2);
    let decoded =
        fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);
    // Imported positions and charges are in the fresh base.
    assert!(decoded.0.contains_key(&("positions".to_owned(), 21)));
    assert!(decoded.0.contains_key(&("positions".to_owned(), 30)));
    assert!(decoded.0.contains_key(&("charges".to_owned(), 1)));
    assert!(decoded.0.contains_key(&("charge_samples".to_owned(), 31)));
}

#[test]
fn design_later_106_b_upserts_and_tombstones_across_import_and_collector_deltas() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let mut fragments = Vec::new();
    let (binding, lineage) = fold_test_imported_base(&store, false, |car_id| {
        fragments = fold_test_base_fragments(car_id, false);
        fragments.clone()
    });
    let car_id = binding.selected_car_id;
    let mut expected = FoldTestMirror::default();
    expected.base(&fragments, false);

    // Import successor: change a drive, add a position, remove one, add a state.
    let mut first = fold_test_delta(&binding);
    let mut changed_drive = fold_test_drive(1, car_id);
    changed_drive.distance_km = Some(99.75);
    first.drives = vec![changed_drive];
    first.positions = vec![fold_test_position(10, Some(1), car_id)];
    first.states = vec![fold_test_state(3, car_id)];
    first.tombstones = vec![fold_test_tombstone(ProjectionDeltaEntity::Position, 2, car_id)];
    let lineage = fold_test_import_delta(&store, &binding, &lineage, &first);
    expected.delta(&first);

    // Collector delta: remove a drive with its positions and an update,
    // patch the settings, change a position.
    let mut second = fold_test_delta(&binding);
    second.car_settings = vec![ProjectionCarSettingsPatch {
        car_id,
        settings: ProjectionCarSettings {
            suspend_min: 44,
            ..fold_test_settings()
        },
    }];
    let mut changed_position = fold_test_position(3, Some(1), car_id);
    changed_position.speed = Some(99);
    second.positions = vec![changed_position];
    second.tombstones = vec![
        fold_test_tombstone(ProjectionDeltaEntity::Position, 6, car_id),
        fold_test_tombstone(ProjectionDeltaEntity::Position, 7, car_id),
        fold_test_tombstone(ProjectionDeltaEntity::Drive, 2, car_id),
        fold_test_tombstone(ProjectionDeltaEntity::Update, 1, car_id),
    ];
    let lineage = fold_test_collector_delta(&store, &binding, &lineage, &second);
    expected.delta(&second);

    // Collector delta: bring a removed position back changed, change a
    // sample and rename the car. A delta car row carries its settings, so
    // the rename is the last mention of the settings and replaces the patch.
    let mut third = fold_test_delta(&binding);
    let mut returned = fold_test_position(2, Some(1), car_id);
    returned.latitude = 51.25;
    third.positions = vec![returned];
    let mut sample = fold_test_sample(3, 2);
    sample.charger_power_kw = Some(7.4);
    third.charge_samples = vec![sample];
    let mut renamed = fold_test_car(car_id, "Fold fixture renamed");
    renamed.settings.suspend_min = 55;
    third.cars = vec![renamed];
    let after_patch = fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);
    assert_eq!(after_patch.0[&("car_settings".to_owned(), car_id)]["suspend_min"], 44);
    let lineage = fold_test_collector_delta(&store, &binding, &lineage, &third);
    expected.delta(&third);

    assert!(!expected.0.contains_key(&("drives".to_owned(), 2)));
    assert!(!expected.0.contains_key(&("updates".to_owned(), 1)));
    assert_eq!(expected.0[&("car_settings".to_owned(), car_id)]["suspend_min"], 55);
    let decoded = fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);
    assert_eq!(decoded.0[&("car_settings".to_owned(), car_id)]["suspend_min"], 55);
    assert_eq!(decoded.0[&("positions".to_owned(), 2)]["latitude"], 51.25);
    assert_eq!(decoded.0[&("cars".to_owned(), car_id)]["name"], "Fold fixture renamed");
}

#[test]
fn rev_cx_iss106_5_a_schema_2_0_base_without_settings_is_refused_not_amended() {
    // A schema-2.0 base has no settings table and no delta set them: a client
    // holds no settings row, and a schema-2.1 base cannot omit one, so the
    // exact fold is not representable and the head is refused.
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (_, lineage) =
        fold_test_imported_base(&store, true, |car_id| fold_test_base_fragments(car_id, true));
    assert_eq!(lineage.schema, HUB_PROJECTION_SCHEMA_V1);
    assert_eq!(lineage.base.packs.len(), 3);
    let catalogue = fold_test_catalogue(&store);
    let files = fold_test_files(&store);
    let error = fold_test_build(&store, lineage.vehicle_id, fold_test_small_packs())
        .expect_err("no settings row is invented");
    assert!(matches!(error, StoreError::LineageRebaseInexact(_)), "{error:?}");
    assert!(error.refuses_lineage_rebase());
    fold_test_assert_refused_unchanged(&store, &lineage, &catalogue, &files);
}

#[test]
fn rev_cx_iss106_4_a_history_that_fits_at_a_larger_pack_target_is_rebased() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let mut fragments = Vec::new();
    let (_, lineage) = fold_test_imported_base(&store, false, |car_id| {
        fragments = vec![fold_test_fragment(
            fold_test_car(car_id, "Fold fixture"),
            Vec::new(),
            (1..=20)
                .map(|id| fold_test_position(id, None, car_id))
                .collect(),
            Vec::new(),
            Vec::new(),
        )];
        fragments.clone()
    });
    // At three rows a pack the twenty loose positions need ten packs, more
    // than the seven a fresh base may hold; at six rows they need four.
    let limits = V2LineageRebaseLimits {
        protocol: ProtocolLimits {
            max_chunks: 16,
            ..ProtocolLimits::default()
        },
        rows_per_pack: 3,
    };
    assert_eq!(limits.base_pack_limit(), 7);
    let mut expected = FoldTestMirror::default();
    expected.base(&fragments, false);
    fold_test_assert_candidate(&store, &lineage, limits, &expected);
}

#[test]
fn design_later_106_b_a_conflicting_context_row_is_refused() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (_, lineage) = fold_test_imported_base(&store, false, |car_id| {
        let mut fragments = fold_test_base_fragments(car_id, false);
        // The second pack repeats drive 1 as context, but not identically.
        fragments[1].snapshot.drives[0].distance_km = Some(77.0);
        fragments
    });
    let catalogue = fold_test_catalogue(&store);
    let files = fold_test_files(&store);
    let error = fold_test_build(&store, lineage.vehicle_id, V2LineageRebaseLimits::default())
        .expect_err("conflicting context rows");
    assert!(
        matches!(&error, StoreError::LineageRebaseInexact(message) if message.contains("drives 1")),
        "{error:?}"
    );
    assert!(error.refuses_lineage_rebase());
    fold_test_assert_refused_unchanged(&store, &lineage, &catalogue, &files);
}

#[test]
fn design_later_106_b_lowered_limits_give_oversized() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (_, lineage) =
        fold_test_imported_base(&store, false, |car_id| fold_test_base_fragments(car_id, false));
    let unlimited = V2LineageRebaseLimits {
        rows_per_pack: 3,
        ..V2LineageRebaseLimits::default()
    };
    let candidate = fold_test_build(&store, lineage.vehicle_id, unlimited).expect("fresh base");
    let needed = candidate.packs.len();
    fold_test_discard(&store, candidate);
    assert!(needed > 1);
    // The row ceiling is the target too, so no larger target is tried.
    let lowered = V2LineageRebaseLimits {
        protocol: ProtocolLimits {
            max_chunks: 10,
            max_rows_per_pack: 3,
            ..ProtocolLimits::default()
        },
        rows_per_pack: 3,
    };
    assert_eq!(lowered.base_pack_limit(), 1);
    let catalogue = fold_test_catalogue(&store);
    let files = fold_test_files(&store);
    let error = fold_test_build(&store, lineage.vehicle_id, lowered).expect_err("oversized");
    assert!(
        matches!(error, StoreError::LineageRebaseOversized { needed: n, limit: 1 } if n == needed),
        "{error:?}"
    );
    assert!(error.refuses_lineage_rebase());
    assert!(error.to_string().contains("previous head stays served"));
    fold_test_assert_refused_unchanged(&store, &lineage, &catalogue, &files);
}

#[test]
fn design_later_106_b_a_damaged_or_missing_pack_is_refused() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (binding, lineage) =
        fold_test_imported_base(&store, false, |car_id| fold_test_base_fragments(car_id, false));
    let mut rows = fold_test_delta(&binding);
    rows.states = vec![fold_test_state(3, binding.selected_car_id)];
    let lineage = fold_test_import_delta(&store, &binding, &lineage, &rows);
    let catalogue = fold_test_catalogue(&store);
    let files = fold_test_files(&store);

    // A delta pack with one byte changed, at its recorded length.
    let delta_path = store
        .packs_dir()
        .join("sha256")
        .join(format!("{}.sqlite.zst", lineage.deltas[0].pack.sha256));
    let original = fs::read(&delta_path).unwrap();
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 0x5a;
    fs::set_permissions(&delta_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&delta_path, &damaged).unwrap();
    let error = fold_test_build(&store, lineage.vehicle_id, V2LineageRebaseLimits::default())
        .expect_err("damaged pack");
    assert!(matches!(error, StoreError::LineageRebaseDamaged(_)), "{error:?}");
    assert!(error.refuses_lineage_rebase());
    fold_test_assert_refused_unchanged(&store, &lineage, &catalogue, &files);
    fs::write(&delta_path, &original).unwrap();

    // A missing base pack.
    let base_path = store
        .packs_dir()
        .join("sha256")
        .join(format!("{}.sqlite.zst", lineage.base.packs[1].sha256));
    let aside = temporary.path().join("aside.sqlite.zst");
    fs::rename(&base_path, &aside).unwrap();
    let error = fold_test_build(&store, lineage.vehicle_id, V2LineageRebaseLimits::default())
        .expect_err("missing pack");
    assert!(matches!(error, StoreError::LineageRebaseDamaged(_)), "{error:?}");
    assert_eq!(fold_test_catalogue(&store), catalogue);
    assert!(fold_test_files(&store).1.is_empty());
    fs::rename(&aside, &base_path).unwrap();

    // The restored lineage folds again; a stop request is transient.
    let gate = store.try_acquire_publication_gate().unwrap();
    let stopped = store
        .build_v2_lineage_rebase(
            &gate,
            lineage.vehicle_id,
            &fold_test_key(),
            V2LineageRebaseLimits::default(),
            &|| true,
        )
        .expect_err("stopped");
    assert!(matches!(stopped, StoreError::LineageRebaseStopped));
    assert!(!stopped.refuses_lineage_rebase());
    drop(gate);
    let candidate = fold_test_build(&store, lineage.vehicle_id, V2LineageRebaseLimits::default())
        .expect("restored lineage folds");
    fold_test_discard(&store, candidate);
    fold_test_assert_refused_unchanged(&store, &lineage, &catalogue, &files);
}

#[test]
fn design_later_106_b_the_post_takeover_state_folds_the_same() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let mut fragments = Vec::new();
    let (binding, lineage) = fold_test_imported_base(&store, false, |car_id| {
        fragments = fold_test_base_fragments(car_id, false);
        fragments.clone()
    });
    let mut rows = fold_test_delta(&binding);
    rows.positions = vec![fold_test_position(40, None, binding.selected_car_id)];
    let lineage = fold_test_collector_delta(&store, &binding, &lineage, &rows);
    let mut expected = FoldTestMirror::default();
    expected.base(&fragments, false);
    expected.delta(&rows);

    // Before the takeover: a kept directory and a #60 capture record exist.
    let kept = store.packs_dir().join(PHYSICAL_V3_KEPT_DIRECTORY);
    fs::create_dir_all(&kept).unwrap();
    fs::write(kept.join("capture.kept"), b"kept capture").unwrap();
    let vehicle = binding.vehicle_id;
    {
        let connection = store.open().unwrap();
        for key in [
            format!("import_publication_intent:{vehicle}"),
            format!("physical_v3_import_revision:{vehicle}"),
        ] {
            connection
                .execute(
                    "INSERT INTO hub_metadata(key, value) VALUES (?1, '{}')",
                    params![key],
                )
                .unwrap();
        }
    }
    let before = fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);

    // After the takeover both are gone; the fold is the same.
    fs::remove_dir_all(&kept).unwrap();
    store
        .open()
        .unwrap()
        .execute(
            "DELETE FROM hub_metadata WHERE key IN (?1, ?2)",
            params![
                format!("import_publication_intent:{vehicle}"),
                format!("physical_v3_import_revision:{vehicle}"),
            ],
        )
        .unwrap();
    let after = fold_test_assert_candidate(&store, &lineage, fold_test_small_packs(), &expected);
    assert_eq!(before, after);
}
