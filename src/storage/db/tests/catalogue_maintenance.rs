// SPDX-License-Identifier: AGPL-3.0-only

//! M08-T29: the storage census counts pack files by class and the bytes no
//! reference, download, recovery path or backup pin needs, and reclamation
//! removes exactly those bytes, resumably, leaving every kept pack unchanged.

use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const SOURCE: &str = "storage-census";
const GRACE: i64 = RETIRED_LINEAGE_PACK_DELETE_GRACE_MS;
const RETENTION: i64 = RETIRED_LINEAGE_PACK_RETENTION_MS;

fn pack_object_hashes(store: &HubStore) -> BTreeMap<String, String> {
    let mut hashes = BTreeMap::new();
    for entry in std::fs::read_dir(store.packs_dir().join("sha256")).expect("pack objects") {
        let entry = entry.expect("pack object entry");
        if entry.file_type().expect("pack object type").is_file() {
            let bytes = std::fs::read(entry.path()).expect("pack object bytes");
            hashes.insert(
                entry.file_name().to_string_lossy().into_owned(),
                hex::encode(Sha256::digest(&bytes)),
            );
        }
    }
    hashes
}

fn head_candidate(
    temporary: &std::path::Path,
    store: &HubStore,
    name: &str,
    chunks: u32,
    head_sequence: u64,
) -> (
    ProjectionBinding,
    crate::sync::physical_fragments::StagedPhysicalProjectionV3,
) {
    crate::sync::physical_fragments::tests::public_admission_candidate_fixture_for_source_and_sequence(
        &temporary.join(name),
        store,
        &CursorKey::from_bytes([29; 32]),
        chunks,
        SOURCE,
        head_sequence,
    )
}

fn chunk_files(
    store: &HubStore,
    admission: &PendingPhysicalV3Admission,
) -> Vec<(std::path::PathBuf, u64)> {
    admission
        .manifest
        .chunks
        .iter()
        .map(|chunk| {
            (
                store
                    .packs_dir()
                    .join("sha256")
                    .join(format!("{}.sqlite.zst", chunk.sha256)),
                chunk.compressed_bytes,
            )
        })
        .collect()
}

fn write_unreferenced_pack(store: &HubStore, fill: u8, bytes: usize) -> String {
    let digest = hex::encode([fill; 32]);
    std::fs::write(
        store
            .packs_dir()
            .join("sha256")
            .join(format!("{digest}.sqlite.zst")),
        vec![fill; bytes],
    )
    .expect("unreferenced pack");
    digest
}

fn retained_receipts(store: &HubStore) -> Vec<String> {
    store
        .open()
        .expect("catalogue")
        .prepare("SELECT receipt_id FROM retained_physical_v3_admissions ORDER BY head_sequence")
        .expect("retained query")
        .query_map([], |row| row.get(0))
        .expect("retained rows")
        .collect::<Result<_, _>>()
        .expect("retained receipts")
}

/// Three public heads: the first expired past its delete grace, the second
/// inside its retention, the third current; the vehicle's kept import stage
/// and source sessions; one orphan pack, one orphan pack a live backup pins,
/// and one stage copy an interrupted hand-back left.
struct PublicFixture {
    _temporary: tempfile::TempDir,
    store: HubStore,
    expired: PendingPhysicalV3Admission,
    retained: PendingPhysicalV3Admission,
    current: PendingPhysicalV3Admission,
    orphan_bytes: u64,
    pinned_bytes: u64,
    kept_paths: Vec<std::path::PathBuf>,
    stray_copy: std::path::PathBuf,
    now_ms: i64,
}

const KEPT_STAGE_BYTES: u64 = 500;
const KEPT_SOURCE_BYTES: u64 = 50;
const STRAY_COPY_BYTES: u64 = 900;

/// The kept stage and source sessions of `vehicle_id`, a stray stage copy,
/// and one file of another shape that the census does not count.
fn write_kept_stages(
    store: &HubStore,
    vehicle_id: Uuid,
) -> (Vec<std::path::PathBuf>, std::path::PathBuf) {
    let kept = store.packs_dir().join("physical-kept");
    let staging = kept.join(".staging");
    std::fs::create_dir_all(&staging).expect("kept stages");
    let stage = staging.join(format!("{vehicle_id}.sqlite"));
    let source = kept.join(format!("{vehicle_id}.source.json"));
    let stray = staging.join(format!("{}.sqlite", Uuid::from_u128(0x2929)));
    std::fs::write(&stage, vec![1; KEPT_STAGE_BYTES as usize]).expect("kept stage");
    std::fs::write(&source, vec![2; KEPT_SOURCE_BYTES as usize]).expect("kept source");
    std::fs::write(&stray, vec![3; STRAY_COPY_BYTES as usize]).expect("stray copy");
    std::fs::write(staging.join("other-shape.txt"), b"not counted").expect("other file");
    // The legacy record that names the pair.
    let digest = "ab".repeat(32);
    let gate = store.try_acquire_publication_gate().expect("gate");
    store
        .record_physical_v3_kept(
            &gate,
            vehicle_id,
            Some(&PhysicalV3Kept { stage: digest.clone(), source: digest, archive: None }),
        )
        .expect("kept record");
    (vec![stage, source], stray)
}

fn public_fixture() -> PublicFixture {
    let (temporary, store) = try_test_store().expect("store");
    let first_retained_at = 1_000;
    let second_retained_at = first_retained_at + RETENTION + GRACE + 10;
    let now_ms = second_retained_at + 10;
    let (binding, first) = head_candidate(temporary.path(), &store, "first", 2, 1);
    let gate = store.try_acquire_publication_gate().expect("gate");
    let expired = store
        .stage_pending_physical_v3_admission(&gate, first)
        .expect("first head");
    let (second_binding, second) = head_candidate(temporary.path(), &store, "second", 3, 2);
    assert_eq!(second_binding, binding);
    let retained = store
        .rotate_and_activate_physical_v3_admission_with_delta_at(
            &gate,
            second,
            None,
            first_retained_at,
        )
        .expect("second head");
    let (_, third) = head_candidate(temporary.path(), &store, "third", 4, 3);
    let current = store
        .rotate_and_activate_physical_v3_admission_with_delta_at(
            &gate,
            third,
            None,
            second_retained_at,
        )
        .expect("third head");
    drop(gate);
    let (kept_paths, stray_copy) = write_kept_stages(&store, binding.vehicle_id);
    write_unreferenced_pack(&store, 0x0a, 700);
    let pinned = write_unreferenced_pack(&store, 0x0b, 300);
    store
        .open()
        .expect("catalogue")
        .execute(
            "INSERT INTO backup_pack_pins (pin_id, sha256, expires_at_ms) VALUES (?1, ?2, ?3)",
            params![Uuid::from_u128(29).to_string(), pinned, now_ms + 60_000],
        )
        .expect("backup pin");
    PublicFixture {
        _temporary: temporary,
        store,
        expired,
        retained,
        current,
        orphan_bytes: 700,
        pinned_bytes: 300,
        kept_paths,
        stray_copy,
        now_ms,
    }
}

fn bytes_of(files: &[(std::path::PathBuf, u64)]) -> u64 {
    files.iter().map(|(_, bytes)| bytes).sum()
}

#[test]
fn storage_census_counts_each_class_and_only_unneeded_bytes_as_reclaimable() {
    let fixture = public_fixture();
    let store = &fixture.store;
    let current = chunk_files(store, &fixture.current);
    let retained = chunk_files(store, &fixture.retained);
    let expired = chunk_files(store, &fixture.expired);
    let hashes_before = pack_object_hashes(store);

    let census = store.storage_census_at(fixture.now_ms).expect("census");

    assert_eq!(
        census.current_heads,
        StorageCensusClass {
            files: 4,
            bytes: bytes_of(&current),
            reclaimable_files: 0,
            reclaimable_bytes: 0,
        }
    );
    assert_eq!(
        census.retained_prior_heads,
        StorageCensusClass {
            files: 5,
            bytes: bytes_of(&retained) + bytes_of(&expired),
            reclaimable_files: 2,
            reclaimable_bytes: bytes_of(&expired),
        }
    );
    assert_eq!(
        census.import_bases,
        StorageCensusClass {
            files: 2,
            bytes: KEPT_STAGE_BYTES + KEPT_SOURCE_BYTES,
            reclaimable_files: 0,
            reclaimable_bytes: 0,
        }
    );
    assert_eq!(
        census.orphans,
        StorageCensusClass {
            files: 3,
            bytes: fixture.orphan_bytes + fixture.pinned_bytes + STRAY_COPY_BYTES,
            reclaimable_files: 2,
            reclaimable_bytes: fixture.orphan_bytes + STRAY_COPY_BYTES,
        }
    );
    assert_eq!(census.deltas, StorageCensusClass::default());
    assert_eq!(census.prepared_maps, StorageCensusClass::default());
    assert_eq!(census.total.files, 14);
    assert_eq!(
        census.total.reclaimable_bytes,
        bytes_of(&expired) + fixture.orphan_bytes + STRAY_COPY_BYTES
    );
    // Inside the delete grace an expired head is still needed: a download
    // that began before expiry may still be reading it.
    let inside_grace = store
        .storage_census_at(1_000 + RETENTION + GRACE - 1)
        .expect("census inside the grace");
    assert_eq!(inside_grace.retained_prior_heads.reclaimable_files, 0);

    // Counts and sizes only: no digest, path or identifier in the output.
    let printed = serde_json::to_string(&census).expect("census JSON");
    assert!(!printed.contains('/'));
    for name in hashes_before.keys() {
        assert!(!printed.contains(name.trim_end_matches(".sqlite.zst")));
    }
    assert!(!printed.contains(&fixture.current.vehicle_id.to_string()));
    // Read-only: nothing is removed or rewritten.
    assert_eq!(pack_object_hashes(store), hashes_before);
    assert_eq!(retained_receipts(store).len(), 2);
}

#[test]
fn storage_reclaim_removes_exactly_the_census_bytes_and_keeps_every_needed_pack() {
    let fixture = public_fixture();
    let store = &fixture.store;
    let expired = chunk_files(store, &fixture.expired);
    let census = store.storage_census_at(fixture.now_ms).expect("census");
    let mut kept = pack_object_hashes(store);
    for (path, _) in &expired {
        kept.remove(path.file_name().unwrap().to_str().unwrap());
    }
    kept.remove(&format!("{}.sqlite.zst", hex::encode([0x0a; 32])));
    let kept_stage_bytes = fixture
        .kept_paths
        .iter()
        .map(|path| std::fs::read(path).expect("kept file"))
        .collect::<Vec<_>>();

    let report = store.reclaim_storage_at(fixture.now_ms).expect("reclaim");

    assert_eq!(report.before, census);
    assert_eq!(report.reclaimed_files, census.total.reclaimable_files);
    assert_eq!(report.reclaimed_bytes, census.total.reclaimable_bytes);
    assert_eq!(report.after.total.reclaimable_bytes, 0);
    assert_eq!(report.after.total.bytes, census.total.bytes - report.reclaimed_bytes);
    // Every kept pack is byte for byte what it was.
    assert_eq!(pack_object_hashes(store), kept);
    assert_eq!(
        fixture
            .kept_paths
            .iter()
            .map(|path| std::fs::read(path).expect("kept file after reclaim"))
            .collect::<Vec<_>>(),
        kept_stage_bytes
    );
    assert!(!fixture.stray_copy.exists());
    assert!(
        store
            .packs_dir()
            .join("physical-kept/.staging/other-shape.txt")
            .is_file()
    );
    assert_eq!(retained_receipts(store), vec![fixture.retained.receipt_id.clone()]);
    assert_eq!(
        store
            .pending_physical_v3_admission_for_vehicle(fixture.current.vehicle_id)
            .expect("current head after reclaim"),
        Some(fixture.current.clone())
    );
    assert!(
        store
            .retained_physical_v3_admission_for_receipt_at(
                fixture.current.vehicle_id,
                &fixture.retained.receipt_id,
                fixture.now_ms,
                true,
            )
            .expect("retained head after reclaim")
            .is_some()
    );
    // A second run finds nothing left.
    let again = store.reclaim_storage_at(fixture.now_ms).expect("second reclaim");
    assert_eq!((again.reclaimed_files, again.reclaimed_bytes), (0, 0));
    assert_eq!(again.after, report.after);
}

#[test]
fn storage_reclaim_resumes_after_an_interrupted_run() {
    let fixture = public_fixture();
    let store = &fixture.store;
    let expired = chunk_files(store, &fixture.expired);
    let census = store.storage_census_at(fixture.now_ms).expect("census");
    // A run that stopped after its first removal, before the rows went.
    std::fs::remove_file(&expired[0].0).expect("first removal");
    let partial = store.storage_census_at(fixture.now_ms).expect("partial census");
    assert_eq!(
        partial.total.reclaimable_bytes,
        census.total.reclaimable_bytes - expired[0].1
    );
    assert_eq!(retained_receipts(store).len(), 2);

    let report = store.reclaim_storage_at(fixture.now_ms).expect("resumed reclaim");

    assert_eq!(report.reclaimed_files, census.total.reclaimable_files - 1);
    assert_eq!(
        report.reclaimed_bytes,
        census.total.reclaimable_bytes - expired[0].1
    );
    assert_eq!(report.after.total.reclaimable_files, 0);
    assert_eq!(retained_receipts(store), vec![fixture.retained.receipt_id.clone()]);
    assert!(expired.iter().all(|(path, _)| !path.exists()));
}

#[test]
fn storage_reclaim_keeps_a_blocked_rotations_predecessor_for_its_recovery() {
    let (temporary, store) = try_test_store().expect("store");
    let (_, first) = head_candidate(temporary.path(), &store, "first", 2, 1);
    let gate = store.try_acquire_publication_gate().expect("gate");
    let predecessor = store
        .stage_pending_physical_v3_admission(&gate, first)
        .expect("first head");
    let (_, second) = head_candidate(temporary.path(), &store, "second", 3, 2);
    let blocked = store
        .rotate_pending_physical_v3_admission_at(&gate, second, 1_000)
        .expect("blocked rotation");
    drop(gate);
    // The last moment of the predecessor's 24-hour retention.
    let now_ms = 1_000 + RETENTION - 1;
    let hashes = pack_object_hashes(&store);
    let census = store.storage_census_at(now_ms).expect("census");
    assert_eq!(census.current_heads.files, 3);
    assert_eq!(census.retained_prior_heads.files, 2);
    assert_eq!(census.total.reclaimable_files, 0);

    let report = store.reclaim_storage_at(now_ms).expect("reclaim");

    assert_eq!((report.reclaimed_files, report.reclaimed_bytes), (0, 0));
    assert_eq!(pack_object_hashes(&store), hashes);
    assert_eq!(retained_receipts(&store), vec![predecessor.receipt_id.clone()]);
    match store
        .physical_v3_publication_state_for_vehicle_at(blocked.vehicle_id, now_ms)
        .expect("recovery state after reclaim")
    {
        PhysicalV3PublicationState::Blocked { current, retained } => {
            assert_eq!(current, blocked);
            assert_eq!(retained.admission, predecessor);
        }
        _ => panic!("the blocked rotation is no longer recoverable"),
    }
}

/// Lab run: the census of a store copy named by `TESATLAS_HUB_CENSUS_STORE`,
/// opened read-only and printed as counts and sizes only.
#[test]
#[ignore = "lab: needs a store copy"]
fn storage_census_of_a_lab_store_copy() {
    let Some(data_dir) = std::env::var_os("TESATLAS_HUB_CENSUS_STORE") else {
        return;
    };
    let store = HubStore::open_read_only(&data_dir).expect("store copy");
    let census = store.storage_census().expect("census");
    println!("{}", serde_json::to_string(&census).expect("census JSON"));
    // With TESATLAS_HUB_CENSUS_RECLAIM set, the copy is then reclaimed and
    // the receipt printed.
    if std::env::var_os("TESATLAS_HUB_CENSUS_RECLAIM").is_some() {
        drop(store);
        let store = HubStore::initialize(&data_dir).expect("writable store copy");
        let report = store.reclaim_storage().expect("reclaim");
        println!("{}", serde_json::to_string(&report).expect("reclaim JSON"));
    }
}

#[test]
fn storage_census_and_reclaim_refuse_a_linked_pack_directory_and_leave_outside_files() {
    for linked in ["sha256", "physical-kept", "physical-kept/.staging"] {
        let fixture = public_fixture();
        let store = &fixture.store;
        let outside = tempfile::tempdir().expect("outside directory");
        let stray = Uuid::from_u128(0x2930);
        let outside_files = [
            outside.path().join(format!("{}.sqlite.zst", "ab".repeat(32))),
            outside.path().join(format!("{stray}.sqlite")),
            outside.path().join(format!("{stray}.source.json")),
        ];
        for path in &outside_files {
            std::fs::write(path, b"outside the store").expect("outside file");
        }
        let link = store.packs_dir().join(linked);
        if std::fs::symlink_metadata(&link).is_ok() {
            std::fs::rename(&link, link.with_extension("moved")).expect("move aside");
        }
        std::os::unix::fs::symlink(outside.path(), &link).expect("directory link");

        assert!(store.storage_census_at(fixture.now_ms).is_err(), "{linked}");
        assert!(store.reclaim_storage_at(fixture.now_ms).is_err(), "{linked}");
        assert!(outside_files.iter().all(|path| path.exists()), "{linked}");
    }
}

const BASE_BYTES: usize = 1 << 20;

fn kept_record(archive: Option<(Uuid, u64)>) -> PhysicalV3Kept {
    let digest = "cd".repeat(32);
    PhysicalV3Kept {
        stage: digest.clone(),
        source: digest.clone(),
        archive: archive.map(|(generation, bytes)| PhysicalV3KeptArchive {
            generation,
            format: 1,
            digest,
            bytes,
            decoded_bytes: bytes * 10,
        }),
    }
}

fn put_kept_record(store: &HubStore, vehicle_id: Uuid, record: &PhysicalV3Kept) {
    let gate = store.try_acquire_publication_gate().expect("gate");
    store
        .record_physical_v3_kept(&gate, vehicle_id, Some(record))
        .expect("kept record");
}

fn kept_dir(store: &HubStore) -> std::path::PathBuf {
    let kept = store.packs_dir().join("physical-kept");
    std::fs::create_dir_all(kept.join(".staging")).expect("kept directory");
    kept
}

fn kept_write(path: &std::path::Path, bytes: usize) {
    std::fs::write(path, vec![7; bytes]).expect("kept file");
}

#[test]
fn storage_reclaim_decides_kept_ownership_from_references_not_vehicles() {
    let (_temporary, store) = try_test_store().expect("store");
    let kept = kept_dir(&store);
    let (live, orphan, unreferenced) =
        (Uuid::from_u128(0x71), Uuid::from_u128(0x72), Uuid::from_u128(0x73));
    let (generation, old_generation) = (Uuid::from_u128(0x81), Uuid::from_u128(0x82));
    // `unreferenced` has no kept record whatever the vehicles table says.
    let (record, named) = compact_replay_base(&store, live, generation);
    put_kept_record(&store, live, &record);
    let freed = [
        kept.join(format!("{live}.{old_generation}.kept")),
        kept.join(format!("{live}.{old_generation}.source.json")),
        kept.join(format!("{live}.{old_generation}.kept.partial")),
        kept.join(format!("{live}.{old_generation}.source.{}.tmp", Uuid::from_u128(0x90))),
        kept.join(format!("{orphan}.{generation}.kept")),
        kept.join(format!("{unreferenced}.source.json")),
        kept.join(".staging").join(format!("{orphan}.sqlite")),
        kept.join(".staging").join(format!("{unreferenced}.sqlite")),
        // A stale legacy stage beside an archive record.
        kept.join(".staging").join(format!("{live}.sqlite")),
    ];
    for path in &freed {
        kept_write(path, BASE_BYTES / 4);
    }
    let freed_bytes = (BASE_BYTES / 4 * freed.len()) as u64;
    let named_bytes: u64 = named.iter().map(|file| std::fs::metadata(file).unwrap().len()).sum();

    let census = store.storage_census_at(1_000).expect("census");
    assert_eq!(census.import_bases.files, 2);
    assert_eq!(census.import_bases.bytes, named_bytes);
    assert_eq!(census.orphans.files, freed.len() as u64);
    assert_eq!(census.orphans.reclaimable_bytes, freed_bytes);
    assert_eq!(census.refused, StorageCensusClass::default());

    let report = store.reclaim_storage_at(1_000).expect("reclaim");
    assert_eq!(report.reclaimed_files, freed.len() as u64);
    assert_eq!(report.reclaimed_bytes, freed_bytes);
    assert_eq!(report.after.total.bytes, named_bytes);
    assert!(freed.iter().all(|path| !path.exists()));
    assert!(named.iter().all(|path| path.is_file()));
    let again = store.reclaim_storage_at(1_000).expect("second reclaim");
    assert_eq!((again.reclaimed_files, again.reclaimed_bytes), (0, 0));
    assert_eq!(again.after, report.after);
}

#[test]
fn storage_reclaim_frees_exactly_the_orphans_bytes_of_two_equal_bases() {
    let (_temporary, store) = try_test_store().expect("store");
    let kept = kept_dir(&store);
    let (live, orphan) = (Uuid::from_u128(0x71), Uuid::from_u128(0x72));
    put_kept_record(&store, live, &kept_record(None));
    kept_write(&kept.join(".staging").join(format!("{live}.sqlite")), BASE_BYTES);
    kept_write(&kept.join(".staging").join(format!("{orphan}.sqlite")), BASE_BYTES);
    let before = store.storage_census_at(1_000).expect("census").total.bytes;
    let report = store.reclaim_storage_at(1_000).expect("reclaim");
    assert_eq!(report.reclaimed_bytes, BASE_BYTES as u64);
    assert_eq!(report.after.total.bytes, before - BASE_BYTES as u64);
    assert!(kept.join(".staging").join(format!("{live}.sqlite")).is_file());
}

#[test]
fn storage_reclaim_refuses_links_damaged_records_and_a_wrong_sized_archive() {
    let (_temporary, store) = try_test_store().expect("store");
    let kept = kept_dir(&store);
    let outside = tempfile::tempdir().expect("outside");
    let target = outside.path().join("target");
    kept_write(&target, 64);
    let (good, damaged, hard) =
        (Uuid::from_u128(0x71), Uuid::from_u128(0x72), Uuid::from_u128(0x73));
    let generation = Uuid::from_u128(0x81);
    // A wrong-sized archive of a valid record, a damaged record's files, a
    // symlink and a hard link of an unowned name.
    put_kept_record(&store, good, &kept_record(Some((generation, 5))));
    let archive = kept.join(format!("{good}.{generation}.kept"));
    kept_write(&archive, 99);
    store
        .open()
        .expect("catalogue")
        .execute(
            "INSERT INTO hub_metadata(key, value) VALUES (?1, 'not json')",
            params![format!("physical_v3_kept:{damaged}")],
        )
        .expect("damaged record");
    let damaged_file = kept.join(format!("{damaged}.source.json"));
    kept_write(&damaged_file, 40);
    let symlink = kept.join(".staging").join(format!("{}.sqlite", Uuid::from_u128(0x74)));
    std::os::unix::fs::symlink(&target, &symlink).expect("symlink");
    let first = kept.join(format!("{hard}.source.json"));
    kept_write(&first, 30);
    let second = outside.path().join("second-name");
    std::fs::hard_link(&first, &second).expect("hard link");

    let census = store.storage_census_at(1_000).expect("census");
    assert_eq!(census.refused.files, 4);
    assert_eq!(census.total.reclaimable_bytes, 0);
    let report = store.reclaim_storage_at(1_000).expect("reclaim");
    assert_eq!((report.reclaimed_files, report.reclaimed_bytes), (0, 0));
    for path in [&archive, &damaged_file, &first, &second, &target] {
        assert!(path.is_file(), "{path:?}");
    }
    assert!(std::fs::symlink_metadata(&symlink).is_ok());
}

#[test]
fn storage_reclaim_keeps_replay_base_on_catalogue_query_error() {
    let (_temporary, store) = try_test_store().expect("store");
    let kept = kept_dir(&store);
    let vehicle = Uuid::from_u128(0x71);
    let generation = Uuid::from_u128(0x81);
    let archive = kept.join(format!("{vehicle}.{generation}.kept"));
    let source = kept.join(format!("{vehicle}.{generation}.source.json"));
    let archive_bytes = b"synthetic archive";
    let source_bytes = br#"{"synthetic":true}"#;
    std::fs::write(&archive, archive_bytes).expect("write archive");
    std::fs::write(&source, source_bytes).expect("write source");
    put_kept_record(&store, vehicle, &kept_record(Some((generation, archive_bytes.len() as u64))));
    assert_eq!(store.storage_census_at(1_000).expect("healthy census").import_bases.files, 2);

    // SQLite accepts these bytes as TEXT, but the kept-record query cannot decode them.
    let updated = store
        .open()
        .expect("catalogue")
        .execute(
            "UPDATE hub_metadata SET value = CAST(X'80' AS TEXT) WHERE key = ?1",
            params![format!("physical_v3_kept:{vehicle}")],
        )
        .expect("damage kept record");
    assert_eq!(updated, 1);

    let error = store.reclaim_storage_at(1_000).expect_err("reclaim must return the query error");
    assert!(
        matches!(&error, StoreError::Query(rusqlite::Error::Utf8Error(1, _))),
        "unexpected reclaim error: {error:?}"
    );
    assert_eq!(std::fs::read(&archive).expect("retained archive").as_slice(), archive_bytes);
    assert_eq!(std::fs::read(&source).expect("retained source").as_slice(), source_bytes);
}

#[test]
fn storage_reclaim_holds_every_kept_file_when_a_record_names_no_vehicle() {
    let (_temporary, store) = try_test_store().expect("store");
    let kept = kept_dir(&store);
    let stray = kept.join(".staging").join(format!("{}.sqlite", Uuid::from_u128(0x72)));
    kept_write(&stray, 500);
    store
        .open()
        .expect("catalogue")
        .execute(
            "INSERT INTO hub_metadata(key, value) VALUES ('physical_v3_kept:not-a-vehicle', '{}')",
            [],
        )
        .expect("unknown record");
    let report = store.reclaim_storage_at(1_000).expect("reclaim");
    assert_eq!(report.reclaimed_bytes, 0);
    assert_eq!(report.after.refused.files, 1);
    assert!(stray.is_file());
}

#[test]
fn storage_reclaim_refuses_a_differently_cased_conflicting_kept_key() {
    for (case, archive) in [("archive", true), ("legacy", false)] {
        let (_temporary, store) = try_test_store().expect("store");
        let kept = kept_dir(&store);
        let live = Uuid::from_u128(0xabcdefab_cdef_4abc_8def_abcdefabcdef);
        let generation = Uuid::from_u128(1);
        let rival = Uuid::from_u128(2);
        let live_record = kept_record(archive.then_some((generation, BASE_BYTES as u64)));
        put_kept_record(&store, live, &live_record);
        let paths: Vec<_> = if archive {
            vec![
                kept.join(format!("{live}.{generation}.kept")),
                kept.join(format!("{live}.{generation}.source.json")),
            ]
        } else {
            vec![
                kept.join(format!("{live}.source.json")),
                kept.join(".staging").join(format!("{live}.sqlite")),
            ]
        };
        for path in &paths {
            kept_write(path, BASE_BYTES);
        }
        let alias = kept_record(Some((rival, BASE_BYTES as u64)));
        store
            .open()
            .expect("catalogue")
            .execute(
                "INSERT INTO hub_metadata(key, value) VALUES (?1, ?2)",
                [
                    format!("physical_v3_kept:{}", live.to_string().to_uppercase()),
                    serde_json::to_string(&alias).expect("record"),
                ],
            )
            .expect("alias record");
        let report = store.reclaim_storage_at(1_000).expect("reclaim");
        assert_eq!(report.reclaimed_bytes, 0, "{case}");
        assert!(report.after.refused.files >= 1, "{case}");
        for path in &paths {
            assert!(path.is_file(), "{case}: {}", path.display());
        }
    }
}

fn compact_replay_base(
    store: &HubStore,
    vehicle: Uuid,
    generation: Uuid,
) -> (PhysicalV3Kept, [std::path::PathBuf; 2]) {
    use std::os::unix::fs::PermissionsExt;

    let kept = kept_dir(store);
    for directory in [&kept, &kept.join(".staging")] {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut stage = crate::source_db_stage::SourceDbStage::create_physical_v3(
        store.imports_dir(),
        crate::sync::physical_fragments::tests::stage_limits(),
    )
    .unwrap();
    crate::sync::physical_fragments::tests::seed_roots(&mut stage);
    stage.seal().unwrap();
    let archive_name = format!("{vehicle}.{generation}.kept");
    let archive = stage
        .write_kept_archive(
            &kept,
            &archive_name,
            crate::source_db_stage::KeptArchiveIdentity { vehicle_id: vehicle, binding: [17; 32] },
        )
        .unwrap();
    stage.discard().unwrap();
    let source = kept.join(format!("{vehicle}.{generation}.source.json"));
    let source_bytes = replay_source(store, vehicle);
    std::fs::write(&source, &source_bytes).unwrap();
    let record = PhysicalV3Kept {
        stage: archive.logical.to_string(),
        source: Sha256Digest::of_bytes(&source_bytes).to_string(),
        archive: Some(PhysicalV3KeptArchive {
            generation,
            format: archive.format,
            digest: archive.digest.to_string(),
            bytes: archive.bytes,
            decoded_bytes: archive.decoded_bytes,
        }),
    };
    (record, [kept.join(archive_name), source])
}

fn replay_source(store: &HubStore, vehicle: Uuid) -> Vec<u8> {
    let binding = ProjectionBinding {
        installation_id: store.installation_id().unwrap(),
        account_id: Uuid::from_u128(0x46),
        vehicle_id: vehicle,
        generation: 1,
        selected_car_id: 1,
    };
    serde_json::to_vec(&serde_json::json!({"binding": binding, "drives": [], "charges": []})).unwrap()
}

#[test]
fn storage_reclaim_holds_replay_generations_until_the_current_pair_is_valid() {
    let (_temporary, store) = try_test_store().unwrap();
    let vehicle = Uuid::from_u128(0x71);
    let (_, retained) = compact_replay_base(&store, vehicle, Uuid::from_u128(0x80));
    let retained_bytes = retained.each_ref().map(|path| std::fs::read(path).unwrap());
    let (record, current) = compact_replay_base(&store, vehicle, Uuid::from_u128(0x81));
    let current_bytes = current.each_ref().map(|path| std::fs::read(path).unwrap());
    put_kept_record(&store, vehicle, &record);
    let assert_held = |case| {
        let report = store.reclaim_storage_at(1_000).unwrap();
        assert_eq!(report.reclaimed_files, 0, "{case}");
        assert!(report.after.refused.files >= 2, "{case}");
        for (path, bytes) in retained.iter().zip(&retained_bytes) {
            assert_eq!(&std::fs::read(path).unwrap(), bytes, "{case}");
        }
    };

    for path in &current {
        std::fs::remove_file(path).unwrap();
    }
    assert_held("recorded generation is missing");
    std::fs::write(&current[0], &current_bytes[0]).unwrap();
    assert_held("recorded source is missing");

    let mut wrong_source = current_bytes[1].clone();
    wrong_source[0] ^= 1;
    std::fs::write(&current[1], &wrong_source).unwrap();
    assert_held("same-size source does not match the catalogue");
    std::fs::write(&current[1], &current_bytes[1]).unwrap();
    std::fs::write(&current[0], &current_bytes[0][..current_bytes[0].len() - 1]).unwrap();
    assert_held("current archive has the wrong size");
    let mut wrong_archive = current_bytes[0].clone();
    *wrong_archive.last_mut().unwrap() ^= 1;
    std::fs::write(&current[0], &wrong_archive).unwrap();
    assert_held("same-size archive does not match the catalogue");
    std::fs::write(&current[0], &current_bytes[0]).unwrap();

    let mut unknown_archive = serde_json::to_value(&record).unwrap();
    let fields = unknown_archive.as_object_mut().unwrap();
    let archive = fields.remove("archive").unwrap();
    fields.insert("archive_v2".into(), archive);
    store
        .open()
        .unwrap()
        .execute(
            "UPDATE hub_metadata SET value = ?1 WHERE key = ?2",
            params![unknown_archive.to_string(), format!("physical_v3_kept:{vehicle}")],
        )
        .unwrap();
    assert_held("unknown archive field with no legacy base");

    put_kept_record(&store, vehicle, &record);
    let report = store.reclaim_storage_at(1_000).unwrap();
    assert_eq!(report.reclaimed_files, 2);
    assert_eq!(report.after.import_bases.files, 2);
    for (path, bytes) in current.iter().zip(&current_bytes) {
        assert_eq!(&std::fs::read(path).unwrap(), bytes);
    }
    assert!(retained.iter().all(|path| !path.exists()));
}

#[test]
fn storage_reclaim_recognises_a_readable_legacy_replay_pair() {
    use crate::source_db_stage::SourceDbStage;

    let (_temporary, store) = try_test_store().unwrap();
    let vehicle = Uuid::from_u128(0x72);
    let (_, obsolete) = compact_replay_base(&store, vehicle, Uuid::from_u128(0x81));
    let kept = kept_dir(&store);
    let mut stage = SourceDbStage::create_physical_v3(
        &kept,
        crate::sync::physical_fragments::tests::stage_limits(),
    )
    .unwrap();
    crate::sync::physical_fragments::tests::seed_roots(&mut stage);
    stage.seal().unwrap();
    let stage_digest = stage.sealed_logical_digest().unwrap();
    let created = stage.path().to_path_buf();
    drop(stage);
    let legacy_stage = kept.join(".staging").join(format!("{vehicle}.sqlite"));
    std::fs::rename(created, &legacy_stage).unwrap();
    let legacy_source = kept.join(format!("{vehicle}.source.json"));
    let source_bytes = replay_source(&store, vehicle);
    std::fs::write(&legacy_source, &source_bytes).unwrap();
    let record = PhysicalV3Kept {
        stage: stage_digest.to_string(),
        source: Sha256Digest::of_bytes(&source_bytes).to_string(),
        archive: None,
    };

    // A missing compact replacement must leave the surviving legacy pair.
    let missing = kept_record(Some((Uuid::from_u128(0x82), 20)));
    put_kept_record(&store, vehicle, &missing);
    let held = store.reclaim_storage_at(1_000).unwrap();
    assert_eq!(held.reclaimed_files, 0);
    assert!(legacy_stage.is_file());
    assert_eq!(std::fs::read(&legacy_source).unwrap(), source_bytes);

    put_kept_record(&store, vehicle, &record);
    let mut ambiguous = serde_json::to_value(&record).unwrap();
    ambiguous["archive_v2"] = serde_json::json!({"generation": Uuid::from_u128(0x81)});
    store
        .open()
        .unwrap()
        .execute(
            "UPDATE hub_metadata SET value = ?1 WHERE key = ?2",
            params![ambiguous.to_string(), format!("physical_v3_kept:{vehicle}")],
        )
        .unwrap();
    let held = store.reclaim_storage_at(1_000).unwrap();
    assert_eq!(held.reclaimed_files, 0);
    assert!(obsolete.iter().all(|path| path.is_file()));

    put_kept_record(&store, vehicle, &record);
    let report = store.reclaim_storage_at(1_000).unwrap();
    assert_eq!(report.reclaimed_files, 2);
    assert_eq!(report.after.import_bases.files, 2);
    assert!(obsolete.iter().all(|path| !path.exists()));
    let opened = SourceDbStage::open_sealed(&legacy_stage).unwrap();
    assert_eq!(opened.sealed_logical_digest().unwrap(), stage_digest);
    assert_eq!(std::fs::read(legacy_source).unwrap(), source_bytes);
}
