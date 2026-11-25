// SPDX-License-Identifier: AGPL-3.0-only

// The import's final report: the checked source range and the per-route
// counts (M08-T41, DEC-246).

use super::*;
use crate::import::source_db::seam::{InheritedCopy, SeamLink, SeamPackPlan, SeamPlan};

fn drive(id: i64, start: i64, end: i64) -> ProjectionDrive {
    serde_json::from_value(serde_json::json!({
        "id": id, "car_id": 1, "start_date_ms": start, "end_date_ms": end,
        "ascent": 1, "descent": 1,
    }))
    .unwrap()
}

fn charge(id: i64, start: i64, end: Option<i64>) -> ProjectionCharge {
    serde_json::from_value(serde_json::json!({
        "id": id, "car_id": 1, "start_date_ms": start, "end_date_ms": end,
    }))
    .unwrap()
}

fn plan() -> SeamPackPlan {
    let mut seam = SeamPlan::default();
    seam.drives.imported = vec![1, 2];
    seam.drives.hub_kept = vec![9];
    seam.drives.inherited = vec![(9, InheritedCopy::Stored)];
    seam.drives.skipped = vec![SeamLink { loser_id: 3, winner_id: 9 }];
    seam.charges.imported = vec![5];
    seam.charges.skipped = vec![SeamLink { loser_id: 9, winner_id: 50 }];
    SeamPackPlan::for_plan(&Default::default(), &seam)
}

#[test]
fn import_summary_range_spans_drives_and_charges() {
    let basis = SourceDbImportBasis::of(
        &[drive(1, 2_000, 3_000), drive(2, 500, 800)],
        &[charge(1, 1_000, Some(9_000)), charge(2, 4_000, None)],
    );
    assert_eq!(basis.range_ms, Some((500, 9_000)));
    assert_eq!(basis.sessions, 4);
}

#[test]
fn import_summary_range_is_none_for_an_empty_source() {
    let basis = SourceDbImportBasis::of(&[], &[]);
    assert_eq!(basis, SourceDbImportBasis::default());
    let json = SourceDbImportSummary::new(basis, SourceDbRouteCounts::default(), Some(SourceDbRouteCounts::default())).to_json();
    assert!(json["checkedFromMs"].is_null() && json["checkedToMs"].is_null());
}

#[test]
fn import_summary_counts_packed_kept_and_withheld_from_the_seam() {
    let temporary = crate::private_tempdir().unwrap();
    let mut stage = SourceDbStage::create_physical_v3(
        temporary.path(),
        crate::physical_fragments::tests::stage_limits(),
    )
    .unwrap();
    for (table, ids) in [
        (SourceDbStageTable::Drives, &[1, 2, 3, 4, 9][..]),
        (SourceDbStageTable::ChargingProcesses, &[5, 9][..]),
    ] {
        for &id in ids {
            let source = serde_json::json!({"start_date_pg_us": pg_us(id), "end_date_pg_us": null});
            stage.insert(table, id, &source).unwrap();
        }
    }
    stage.seal().unwrap();
    let seam = plan();
    let (checked, physical) = physical_import_report_facts(&stage, Some(&seam)).unwrap();
    let materialised = SourceDbImportBasis { sessions: 6, range_ms: None };
    let legacy = SourceDbRouteCounts::legacy(materialised, checked, Some(&seam));
    assert_eq!(checked.sessions, 7);
    assert_eq!(legacy, SourceDbRouteCounts { packed: 3, kept: 1, withheld: 3 });
    assert_eq!(physical, SourceDbRouteCounts { packed: 4, kept: 1, withheld: 2 });
}

#[test]
fn import_summary_without_a_seam_packs_every_session() {
    let basis = SourceDbImportBasis::of(&[drive(1, 1, 2)], &[charge(1, 3, Some(4))]);
    let legacy = SourceDbRouteCounts::legacy(basis, basis, None);
    assert_eq!(legacy, SourceDbRouteCounts { packed: 2, kept: 0, withheld: 0 });
}

#[test]
fn import_summary_withholds_a_physical_route_that_is_not_served() {
    let basis = SourceDbImportBasis { sessions: 6, range_ms: Some((1, 2)) };
    let legacy = SourceDbRouteCounts::legacy(basis, basis, Some(&plan()));
    let summary = SourceDbImportSummary::new(basis, legacy, None);
    assert_eq!(summary.legacy.packed, 3);
    assert_eq!(summary.physical, SourceDbRouteCounts { packed: 0, kept: 0, withheld: 6 });
}

#[test]
fn import_summary_json_holds_only_a_range_and_counts() {
    let basis = SourceDbImportBasis { sessions: 6, range_ms: Some((10, 20)) };
    let legacy = SourceDbRouteCounts::legacy(basis, basis, Some(&plan()));
    let physical = SourceDbRouteCounts { packed: 4, kept: 0, withheld: 2 };
    let json = SourceDbImportSummary::new(basis, legacy, Some(physical)).to_json();
    assert_eq!(json["checkedFromMs"], 10);
    assert_eq!(json["checkedToMs"], 20);
    assert_eq!(json["routes"]["legacy"]["packed"], 3);
    assert_eq!(json["routes"]["legacy"]["kept"], 1);
    assert_eq!(json["routes"]["physical"]["withheld"], 2);
    let keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, ["checkedFromMs", "checkedToMs", "routes"]);
    assert_eq!(json["routes"].as_object().unwrap().len(), 2);
}

#[test]
fn import_summary_first_import_reports_closed_and_open_drives_by_route() {
    use crate::import::source_db::importer::repair_journey_fixture::{physical_drive, seed_roots};
    use crate::source_db_projection::SourceDbDrive;
    use crate::source_db_stage::OwnedSourceDbStage;

    const START: i64 = 1_760_000_000_000;
    const END: i64 = START + 60_000;
    const OPEN: i64 = END + 60_000;
    let temp = crate::private_tempdir().unwrap();
    let store = HubStore::initialize(temp.path()).unwrap();
    let key = CursorKey::from_bytes([74; 32]);
    let mut stage = SourceDbStage::create_physical_v3(
        store.imports_dir(),
        crate::physical_fragments::tests::stage_limits(),
    )
    .unwrap();
    seed_roots(&mut stage);
    for row in [physical_drive(1, START, Some((END, 10.0))), physical_drive(2, OPEN, None)] {
        stage.insert(SourceDbStageTable::Drives, i64::from(row.id), &row).unwrap();
    }
    stage.seal().unwrap();
    let car = stage.get(SourceDbStageTable::Cars, 1).unwrap().unwrap();
    let settings = stage.page(SourceDbStageTable::GlobalSettings, 0, 1).unwrap().rows.remove(0).value;
    let car_settings = stage.page(SourceDbStageTable::CarSettings, 0, 1).unwrap().rows.remove(0).value;
    let updates_v2_2 = DirectUpdatesSourceV2_2 {
        postgres_snapshot_sha256: crate::updates_logical::hex_sha256(b"first-import-report"),
        schema: crate::source_db_reader::SourceDbSchemaInfo {
            observed_migration_version: crate::source_db_schema::MAX_VALIDATED_MIGRATION,
            observed_migration_count: crate::source_db_schema::SOURCE_DB_V4_MIGRATION_COUNT,
            minimum_supported_migration_version: crate::source_db_schema::MIN_SUPPORTED_MIGRATION,
            maximum_validated_migration_version: crate::source_db_schema::MAX_VALIDATED_MIGRATION,
            pinned_source_revision: crate::source_db_schema::SOURCE_DB_V4_SOURCE_REVISION,
            pinned_migration_set_sha256: crate::source_db_schema::SOURCE_DB_V4_MIGRATION_SET_SHA256,
            fingerprint: crate::updates_logical::hex_sha256(b"first-import-report-schema"),
        },
        global_settings: settings,
        car,
        car_settings,
        updates: Vec::new(),
    };
    let source_car = serde_json::from_value(serde_json::json!({
        "id": 1, "eid": updates_v2_2.car.eid, "vid": updates_v2_2.car.vid, "vin": updates_v2_2.car.vin
    }))
    .unwrap();
    let source_drive = |id, start, end| {
        serde_json::from_value::<SourceDbDrive>(serde_json::json!({
            "id": id, "car_id": 1, "start_date_ms": start, "end_date_ms": end
        }))
        .unwrap()
    };
    let history = SourceDbHistory {
        cars: vec![source_car],
        drives: vec![source_drive(1, START, Some(END)), source_drive(2, OPEN, None)],
        positions: vec![],
        charging_processes: vec![],
        charges: vec![],
        addresses: vec![],
        geofences: vec![],
        states: vec![],
        updates: vec![],
    };
    let projected = project_vehicle(&history, 1).unwrap();
    let request = SourceDbImportRequest {
        new_import_source: false,
        source_key: "synthetic-first-import-report".into(),
        scope: SourceDbImportScope::Selected(1),
        imported_at_ms: OPEN + 60_000,
    };
    let report = publish_history(&store, &key, &request, &history).expect("first legacy publication");
    assert!(!report.skipped);
    assert_eq!(report.projection.completed_drives, 1);
    assert_eq!(report.projection.skipped_open_drives, 1);
    let binding = store.v2_projection_binding(report.vehicle_id).unwrap();
    let publication_gate = store.try_acquire_publication_gate().unwrap();
    let source_basis = SourceDbImportBasis::of(&projected.snapshot.drives, &projected.snapshot.charges);
    let imported = finish_selected_schema_22_publication(
        &store,
        &key,
        CapturedSourceDbImport {
            report,
            binding,
            updates_v2_2,
            atomic_schema_22: None,
            physical_stage: Some(OwnedSourceDbStage::new(stage)),
            physical_seam: None,
            physical_source: Some((projected.snapshot.drives, projected.snapshot.charges)),
            source_basis,
            publication_gate,
            publication: None,
        },
    )
    .expect("selected publication succeeds");
    let physical = imported.physical_v3.expect("physical 1.4 head");
    assert_eq!(physical.kind, crate::physical_publication::PhysicalV3PublicationKind::FirstHead);
    assert_eq!(
        imported.summary.to_json(),
        serde_json::json!({
            "checkedFromMs": START,
            "checkedToMs": OPEN,
            "routes": {
                "legacy": {"packed": 1, "kept": 0, "withheld": 1},
                "physical": {"packed": 2, "kept": 0, "withheld": 0}
            }
        }),
    );
}
