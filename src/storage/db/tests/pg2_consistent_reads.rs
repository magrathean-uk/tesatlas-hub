// SPDX-License-Identifier: AGPL-3.0-only

//! PG-2: the hand-back's reads of the store are consistent and take no write
//! lock. `import_seam_stored_rows` reads its four tables in one snapshot, a
//! seam dry run reads in a DEFERRED transaction (SQLite's write lock only
//! when its plan changes the store), and the plan basis is one store call.
//! Synthetic data only.

use std::{cell::Cell, rc::Rc, time::Duration};

use super::*;
use crate::db::hub_ids::HUB_ID_BASE;

const T0: i64 = 1_760_000_000_000;

fn pg2_vehicle(store: &HubStore) -> Uuid {
    let source = store
        .register_source(
            &SourceDescriptor::new(crate::db::IMPORT_SOURCE_KIND, "pg2"),
            1_000,
        )
        .expect("import source");
    store
        .register_vehicle(&VehicleDescriptor::new(source.source_id, "pg2"), 1_001)
        .expect("vehicle")
        .vehicle_id
}

fn pg2_json<T: serde::de::DeserializeOwned + serde::Serialize>(value: serde_json::Value) -> String {
    serde_json::to_string(&serde_json::from_value::<T>(value).expect("typed row")).expect("json")
}

/// A Hub-range drive and a position of it, committed together.
fn pg2_commit_drive(connection: &Connection, vehicle: Uuid, offset: i64) {
    let id = HUB_ID_BASE + offset;
    let drive = pg2_json::<crate::hub_pack::ProjectionDrive>(serde_json::json!({
        "id": id, "car_id": 1, "start_date_ms": T0 + offset, "end_date_ms": T0 + offset + 1_000
    }));
    let position = pg2_json::<crate::hub_pack::ProjectionPosition>(serde_json::json!({
        "id": id, "drive_id": id, "car_id": 1, "date_ms": T0 + offset + 500,
        "latitude": 10.0, "longitude": 20.0
    }));
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .expect("collector commit begins");
    connection
        .execute(
            "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
             VALUES (?1, ?2, 1, ?3)",
            params![vehicle.to_string(), id, drive],
        )
        .expect("drive");
    connection
        .execute(
            "INSERT INTO materialised_positions(vehicle_id, position_id, drive_id, car_id, position_json)
             VALUES (?1, ?2, ?2, 1, ?3)",
            params![vehicle.to_string(), id, position],
        )
        .expect("position");
    connection.execute_batch("COMMIT").expect("collector commit");
}

/// A Hub-range charge and a sample of it, committed together.
fn pg2_commit_charge(connection: &Connection, vehicle: Uuid, offset: i64) {
    let id = HUB_ID_BASE + offset;
    let charge = pg2_json::<crate::hub_pack::ProjectionCharge>(serde_json::json!({
        "id": id, "car_id": 1, "start_date_ms": T0 + offset, "end_date_ms": null,
        "charge_energy_added": null
    }));
    let sample = pg2_json::<crate::hub_pack::ProjectionChargeSample>(serde_json::json!({
        "id": id, "charge_process_id": id, "timestamp_ms": T0 + offset + 500
    }));
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .expect("collector commit begins");
    connection
        .execute(
            "INSERT INTO materialised_charges(vehicle_id, charge_id, car_id, charge_json)
             VALUES (?1, ?2, 1, ?3)",
            params![vehicle.to_string(), id, charge],
        )
        .expect("charge");
    connection
        .execute(
            "INSERT INTO materialised_charge_samples(vehicle_id, sample_id, charge_id, sample_json)
             VALUES (?1, ?2, ?2, ?3)",
            params![vehicle.to_string(), id, sample],
        )
        .expect("sample");
    connection.execute_batch("COMMIT").expect("collector commit");
}

/// Every position and sample of the result belongs to a drive or charge of it.
fn pg2_is_consistent(rows: &ImportSeamStoredRows) -> bool {
    let drives: Vec<i64> = rows.drives.iter().map(|drive| drive.id).collect();
    let charges: Vec<i64> = rows.charges.iter().map(|charge| charge.id).collect();
    rows.positions
        .iter()
        .all(|position| position.drive_id.is_none_or(|id| drives.contains(&id)))
        && rows
            .charge_samples
            .iter()
            .all(|sample| charges.contains(&sample.charge_process_id))
}

#[test]
fn pg2_stored_rows_drives_and_positions_come_from_one_snapshot() {
    let directory = crate::private_tempdir().expect("tempdir");
    let store = HubStore::initialize(directory.path()).expect("store");
    let vehicle = pg2_vehicle(&store);
    pg2_commit_drive(&store.open().expect("writer"), vehicle, 1);

    let writer = store.open().expect("collector connection");
    let ran = Rc::new(Cell::new(0));
    let seen = Rc::clone(&ran);
    set_import_seam_stored_rows_hook(Some(Box::new(move |after| {
        if after == "drives" {
            seen.set(seen.get() + 1);
            pg2_commit_drive(&writer, vehicle, 2);
        }
    })));
    let rows = store.import_seam_stored_rows(vehicle).expect("stored rows");
    set_import_seam_stored_rows_hook(None);

    assert_eq!(ran.get(), 1, "the writer committed between the reads");
    assert!(pg2_is_consistent(&rows), "no position without its drive: {rows:?}");
    assert_eq!(rows.drives.len(), 1, "the drive committed mid-read is not seen");
    assert_eq!(rows.positions.len(), 1, "nor is its position");
    let later = store.import_seam_stored_rows(vehicle).expect("stored rows");
    assert_eq!((later.drives.len(), later.positions.len()), (2, 2));
}

#[test]
fn pg2_stored_rows_charge_samples_never_lack_their_charge() {
    let directory = crate::private_tempdir().expect("tempdir");
    let store = HubStore::initialize(directory.path()).expect("store");
    let vehicle = pg2_vehicle(&store);
    pg2_commit_charge(&store.open().expect("writer"), vehicle, 1);

    let writer = store.open().expect("collector connection");
    let ran = Rc::new(Cell::new(0));
    let seen = Rc::clone(&ran);
    set_import_seam_stored_rows_hook(Some(Box::new(move |after| {
        if after == "positions" {
            seen.set(seen.get() + 1);
            pg2_commit_charge(&writer, vehicle, 2);
        }
    })));
    let rows = store.import_seam_stored_rows(vehicle).expect("stored rows");
    set_import_seam_stored_rows_hook(None);

    assert_eq!(ran.get(), 1);
    assert!(
        pg2_is_consistent(&rows),
        "no charge sample without its charging process: {rows:?}"
    );
    assert_eq!((rows.charges.len(), rows.charge_samples.len()), (1, 1));
}

/// A second connection that must never wait for a lock.
fn pg2_collector_connection(store: &HubStore) -> Connection {
    let connection = store.open().expect("collector connection");
    connection.busy_timeout(Duration::ZERO).expect("no wait");
    connection
}

fn pg2_is_busy(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(failure, _)
        if failure.code == rusqlite::ErrorCode::DatabaseBusy)
}

#[test]
fn pg2_collector_commit_succeeds_while_a_dry_run_reads() {
    let directory = crate::private_tempdir().expect("tempdir");
    let store = HubStore::initialize(directory.path()).expect("store");
    let vehicle = pg2_vehicle(&store);
    let gate = store.try_acquire_publication_gate().expect("gate");

    let collector = pg2_collector_connection(&store);
    let committed = Rc::new(Cell::new(0));
    let seen = Rc::clone(&committed);
    set_import_seam_dry_run_hook(Some(Box::new(move |stage| {
        if stage == SeamDryRunStage::Planned {
            pg2_commit_drive(&collector, vehicle, seen.get() + 1);
            seen.set(seen.get() + 1);
        }
    })));
    let run = store.dry_run_import_seam_ahead_of_packs(&gate, vehicle, 1, &[], &[]);
    set_import_seam_dry_run_hook(None);

    let run = run.expect("a dry run holds no write lock while it plans");
    assert_eq!(committed.get(), 1, "the collector committed during the dry run");
    // The collector's drive is the Hub's own: this plan restates nothing.
    assert!(run.ahead.restated_drives.is_empty());
    assert_eq!(store.import_seam_stored_rows(vehicle).expect("rows").drives.len(), 1);
}

#[test]
fn pg2_dry_run_that_changes_the_store_writes_nothing_and_survives_a_stale_snapshot() {
    let directory = crate::private_tempdir().expect("tempdir");
    let store = HubStore::initialize(directory.path()).expect("store");
    let vehicle = pg2_vehicle(&store);
    let gate = store.try_acquire_publication_gate().expect("gate");
    // A Hub drive outside any coverage loses to the source's side of the
    // seam, so the plan changes the store (a tombstone) and must write.
    pg2_commit_drive(&store.open().expect("writer"), vehicle, 1);
    let before = store.import_seam_stored_rows(vehicle).expect("rows");

    let collector = pg2_collector_connection(&store);
    let planned = Rc::new(Cell::new(0));
    let upgraded = Rc::new(Cell::new(0));
    let (planned_seen, upgraded_seen) = (Rc::clone(&planned), Rc::clone(&upgraded));
    set_import_seam_dry_run_hook(Some(Box::new(move |stage| match stage {
        SeamDryRunStage::Planned => {
            planned_seen.set(planned_seen.get() + 1);
            // The first attempt's snapshot goes stale before it can write.
            if planned_seen.get() == 1 {
                pg2_commit_drive(&collector, vehicle, 2);
            }
        }
        SeamDryRunStage::Upgraded => {
            upgraded_seen.set(upgraded_seen.get() + 1);
            let error = collector
                .execute_batch("BEGIN IMMEDIATE")
                .expect_err("the upgraded dry run holds the write lock");
            assert!(pg2_is_busy(&error), "{error:?}");
        }
    })));
    let run = store.dry_run_import_seam_ahead_of_packs(&gate, vehicle, 1, &[], &[]);
    set_import_seam_dry_run_hook(None);

    let run = run.expect("dry run");
    assert!(run.ahead.changed_the_store, "the plan tombstones the Hub drive");
    assert_eq!(planned.get(), 2, "the stale snapshot was read again");
    assert_eq!(upgraded.get(), 1, "only the fresh snapshot wrote");
    let after = store.import_seam_stored_rows(vehicle).expect("rows");
    assert_eq!(after.drives.len(), before.drives.len() + 1, "only the collector's drive was added");
    pg2_collector_connection(&store)
        .execute_batch("BEGIN IMMEDIATE; COMMIT")
        .expect("the write lock is released");
}

#[test]
fn pg2_plan_basis_matches_the_separate_reads_on_a_quiet_store() {
    let directory = crate::private_tempdir().expect("tempdir");
    let store = HubStore::initialize(directory.path()).expect("store");
    let vehicle = pg2_vehicle(&store);

    let separate = |store: &HubStore| {
        let connection = store.open_read_only_connection().expect("read");
        PlanBasis {
            kept: store.physical_v3_kept(vehicle).expect("kept"),
            import_intent: store.import_publication_intent(vehicle).expect("intent"),
            import_publication_pending: store
                .import_publication_holds()
                .expect("holds")
                .pending(vehicle),
            import_revision: store.physical_v3_import_revision(vehicle).expect("revision"),
            coverage_unreconciled: store.collection_coverage_unreconciled().expect("coverage"),
            closed_coverage_digest: closed_coverage_digest_for_vehicle(
                &connection,
                &vehicle.to_string(),
            )
            .expect("digest"),
            head: store
                .pending_physical_v3_admission_for_vehicle(vehicle)
                .expect("head")
                .map(|admission| PlanBasisHead {
                    snapshot_id: admission.snapshot_id,
                    head_sequence: admission.head_sequence,
                    receipt_id: admission.receipt_id,
                    blocked: false,
                    retained_receipt_id: None,
                }),
            vehicle_active: store.vehicle_is_active(vehicle).expect("active"),
            binding: store.v2_projection_binding(vehicle).ok(),
        }
    };

    let bare = store.plan_basis(vehicle).expect("basis");
    assert_eq!(bare, separate(&store));
    assert!(bare.kept.is_none() && bare.import_intent.is_none() && bare.head.is_none());
    assert!(bare.vehicle_active && !bare.import_publication_pending);

    let gate = store.try_acquire_publication_gate().expect("gate");
    let kept = PhysicalV3Kept {
        stage: "a".repeat(64),
        source: "b".repeat(64),
        archive: None,
    };
    store
        .record_physical_v3_kept(&gate, vehicle, Some(&kept))
        .expect("kept record");
    let recorded = store.plan_basis(vehicle).expect("basis");
    assert_eq!(recorded.kept.as_ref(), Some(&kept));
    assert_eq!(recorded, separate(&store));
    assert_ne!(recorded, bare, "a kept record changes the basis");
}
