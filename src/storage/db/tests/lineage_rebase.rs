// SPDX-License-Identifier: AGPL-3.0-only

// Issue #106, task a: a compatibility lineage at the packs a client can hold
// keeps the collector run alive and asks for a fresh base. This file is
// `include!`d into a test module of the collector (its replay is private
// there); the lineage rig is the one of the import-successor limit test.

use crate::hub_pack::{ProjectionCarSettings, ProjectionDelta, ProjectionSnapshot};
use crate::protocol::{LineageManifestV2, ProtocolVersion, SyncManifest};
use crate::db::{V2LineageRebaseRequest, V2LineageRebaseState};

fn rebase_test_cursor_key() -> CursorKey {
    CursorKey::from_bytes([61; 32])
}

fn rebase_test_cursor(binding: &ProjectionBinding, sequence: u64) -> OpaqueCursor {
    OpaqueCursor::issue(
        &rebase_test_cursor_key(),
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

fn rebase_test_car(car_id: i64) -> ProjectionCar {
    ProjectionCar {
        id: car_id,
        name: "Rebase request fixture".into(),
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
        firmware_version: None,
        efficiency_wh_per_km: None,
        settings: ProjectionCarSettings::default(),
    }
}

/// An imported compatibility base of one pack, as the import path leaves it.
fn rebase_test_imported_base(
    store: &HubStore,
) -> (crate::db::VehicleRecord, ProjectionBinding, LineageManifestV2) {
    let source = store
        .register_source(&SourceDescriptor::new("source_db_import", "rebase-fixture"), 1_000)
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
    let snapshot = ProjectionSnapshot {
        cars: vec![rebase_test_car(binding.selected_car_id)],
        drives: Vec::new(),
        positions: Vec::new(),
        charges: Vec::new(),
        charge_samples: Vec::new(),
    };
    let request = ProjectionPackRequest {
        pack_id: Uuid::new_v4(),
        snapshot_id: Uuid::new_v4(),
        ordinal: 0,
        binding: binding.clone(),
        sequence: SequenceRange {
            from_exclusive: 1,
            to_inclusive: 1,
        },
        snapshot: &snapshot,
    };
    let pack = ProjectionPackWriter::new(store.packs_dir())
        .write_full_snapshot_with_states_and_updates(&request, &[], &[])
        .expect("fixture base pack");
    let manifest: SyncManifest = request
        .signed_manifest_with_states_and_updates(&pack, &[], &[], &rebase_test_cursor_key())
        .expect("fixture base manifest");
    store
        .finalize_import_snapshot_with_binding(
            &manifest,
            Sha256Digest::of_bytes(b"rebase-fixture-base"),
            &[],
            &binding,
        )
        .expect("fixture base catalogue");
    let lineage = store
        .lineage_manifest_for_vehicle(vehicle.vehicle_id)
        .expect("fixture lineage lookup")
        .expect("fixture lineage catalogue");
    (vehicle, binding, lineage)
}

/// One more imported delta on the lineage, through the typed writer and the
/// import finalizer.
fn rebase_test_grow(
    store: &HubStore,
    vehicle_id: Uuid,
    binding: &ProjectionBinding,
    prior: &mut LineageManifestV2,
) {
    let payload = ProjectionDelta {
        binding: binding.clone(),
        sequence: SequenceRange {
            from_exclusive: prior.head_sequence,
            to_inclusive: prior.head_sequence + 1,
        },
        parent_digest: prior.head_digest,
        cars: vec![rebase_test_car(binding.selected_car_id)],
        car_settings: Vec::new(),
        drives: Vec::new(),
        positions: Vec::new(),
        charges: Vec::new(),
        charge_samples: Vec::new(),
        states: Vec::new(),
        updates: Vec::new(),
        tombstones: Vec::new(),
    };
    let written = ProjectionPackWriter::new(store.packs_dir())
        .write_delta(&ProjectionDeltaPackRequest {
            pack_id: Uuid::new_v4(),
            snapshot_id: prior.base.snapshot_id,
            ordinal: (prior.base.packs.len() + prior.deltas.len()) as u32,
            delta: &payload,
        })
        .expect("real typed successor pack");
    let delta = LineageDelta {
        from_sequence: payload.sequence.from_exclusive,
        to_sequence: payload.sequence.to_inclusive,
        parent_chain_digest: prior.head_digest,
        chain_digest: canonical_delta_chain_digest(prior.head_digest, written.metadata.sha256),
        pack_digest: written.metadata.sha256,
        pack: written.metadata,
    };
    let cursor = rebase_test_cursor(binding, delta.to_sequence);
    store
        .finalize_import_delta_successor(
            vehicle_id,
            &delta,
            &rebase_test_cursor_key(),
            &cursor,
            Sha256Digest::of_bytes(&delta.to_sequence.to_le_bytes()),
            &[],
        )
        .expect("successor through the aggregate limit");
    prior.head_sequence = delta.to_sequence;
    prior.head_digest = delta.chain_digest;
    prior.terminal_cursor = cursor;
    prior.deltas.push(delta);
}

fn rebase_test_grow_to(
    store: &HubStore,
    vehicle_id: Uuid,
    binding: &ProjectionBinding,
    prior: &mut LineageManifestV2,
    packs: usize,
) {
    while prior.base.packs.len() + prior.deltas.len() < packs {
        rebase_test_grow(store, vehicle_id, binding, prior);
    }
}

/// A queued change of the car's settings: one unpublished mutation. `flip`
/// sets `lfp_battery`, a column every upsert writes, so a call whose `flip`
/// differs from the previous one is a change. `suspend_min` would not do: the
/// default settings mark it resolved, and an upsert then keeps the stored value.
#[track_caller]
fn rebase_test_queue_mutation(store: &HubStore, vehicle_id: Uuid, binding: &ProjectionBinding, flip: bool) {
    let caller = std::panic::Location::caller();
    store
        .upsert_car_settings(
            vehicle_id,
            binding.selected_car_id,
            &ProjectionCarSettings {
                lfp_battery: flip,
                ..ProjectionCarSettings::default()
            },
        )
        .expect("queued settings mutation");
    assert!(
        store
            .has_unpublished_sync_mutations(vehicle_id)
            .expect("pending lookup"),
        "the change from {caller} (lfp_battery {flip}) is queued, not published"
    );
}

/// The replay at `now_ms` of the claimed export outbox row, as the run does it
/// (no streams, so compaction is not deferred).
fn rebase_test_replay(
    store: &HubStore,
    now_ms: i64,
) -> Result<usize, CollectorError> {
    let publication_gate = store.try_acquire_publication_gate().expect("gate");
    replay_export_outbox_blocking(
        store,
        &publication_gate,
        &rebase_test_cursor_key(),
        &[],
        now_ms,
        false,
        None,
    )
}

/// Removes the collector's span rows of the vehicle, so its published deltas
/// read as imported ones and the compaction planner finds no plan.
fn rebase_test_forget_live_spans(store: &HubStore, vehicle_id: Uuid) {
    store
        .open()
        .expect("catalogue")
        .execute(
            "DELETE FROM sync_live_delta_spans WHERE vehicle_id = ?1",
            params![vehicle_id.to_string()],
        )
        .expect("span rows removed");
}

fn rebase_test_outbox_error(store: &HubStore, vehicle_id: Uuid) -> Option<String> {
    store
        .open()
        .expect("catalogue")
        .query_row(
            "SELECT last_error FROM export_outbox WHERE vehicle_id = ?1",
            params![vehicle_id.to_string()],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .expect("outbox lookup")
        .flatten()
}

fn rebase_test_now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("clock range")
        + 3_600_000
}

fn rebase_test_request(store: &HubStore, vehicle_id: Uuid) -> Option<V2LineageRebaseRequest> {
    store
        .v2_lineage_rebase_request(vehicle_id)
        .expect("request lookup")
}

#[test]
fn design_later_106_a_full_lineage_replay_returns_ok_keeps_the_change_and_requests_a_rebase() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, mut lineage) = rebase_test_imported_base(&store);
    let limit = ProtocolLimits::default().max_chunks;
    assert_eq!(limit, 512);
    rebase_test_grow_to(&store, vehicle.vehicle_id, &binding, &mut lineage, limit);
    rebase_test_queue_mutation(&store, vehicle.vehicle_id, &binding, true);
    let head = store
        .v2_head(vehicle.vehicle_id)
        .expect("head")
        .expect("served head");
    let before = store
        .lineage_manifest_for_vehicle(vehicle.vehicle_id)
        .expect("lineage")
        .expect("catalogue");
    assert_eq!(before, lineage);

    let replayed = rebase_test_replay(&store, rebase_test_now_ms());

    assert!(
        matches!(replayed, Ok(0)),
        "a full lineage is not a fault of the run: {replayed:?}"
    );
    assert_eq!(
        rebase_test_outbox_error(&store, vehicle.vehicle_id).as_deref(),
        Some("lineage_capacity_exhausted")
    );
    let request = rebase_test_request(&store, vehicle.vehicle_id).expect("a rebase request");
    assert_eq!(request.head_digest, head.2.to_string());
    assert_eq!(request.state, V2LineageRebaseState::Requested);
    assert_eq!(request.format, 1);
    assert_eq!(
        store.v2_head(vehicle.vehicle_id).expect("head"),
        Some(head),
        "the old head keeps serving"
    );
    assert_eq!(
        store
            .lineage_manifest_for_vehicle(vehicle.vehicle_id)
            .expect("lineage"),
        Some(before),
        "no lineage row changed"
    );
    assert!(
        store
            .has_unpublished_sync_mutations(vehicle.vehicle_id)
            .expect("pending lookup"),
        "the change stays queued"
    );
    assert!(
        store
            .claim_sync_mutations(vehicle.vehicle_id, rebase_test_now_ms(), 100)
            .expect("claim")
            .is_some(),
        "the claim was released, so the change can be claimed again"
    );
}

#[test]
fn design_later_106_a_the_slots_before_full_request_at_the_trigger_and_still_publish() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, mut lineage) = rebase_test_imported_base(&store);
    let trigger = ProtocolLimits::default().max_chunks - 8;
    // Below the trigger the lineage is not asked about at all.
    rebase_test_grow_to(&store, vehicle.vehicle_id, &binding, &mut lineage, trigger - 1);
    rebase_test_queue_mutation(&store, vehicle.vehicle_id, &binding, true);
    assert!(matches!(rebase_test_replay(&store, rebase_test_now_ms()), Ok(1)));
    assert_eq!(rebase_test_request(&store, vehicle.vehicle_id), None);
    let published = store
        .v2_lineage_pack_count(vehicle.vehicle_id)
        .expect("pack count");
    assert_eq!(published, trigger);
    // From the trigger on, with no compaction plan, each replay asks for a
    // rebase and still publishes into the slots left.
    for step in 0..7usize {
        // 504 to 510 packs before the publication: 505 to 511 after it.
        let packs = store
            .v2_lineage_pack_count(vehicle.vehicle_id)
            .expect("pack count");
        assert!(packs < 512, "a slot is left for the publication");
        // The design's case is a head with no compaction plan. The replay's
        // own deltas are collector-owned, so from the second one on the
        // planner would merge them (a 2-delta compaction, which holds the
        // count) instead: clear their span rows so the published suffix reads
        // as imported, as the deltas before the trigger do.
        rebase_test_forget_live_spans(&store, vehicle.vehicle_id);
        assert!(
            store
                .plan_live_delta_compaction(vehicle.vehicle_id)
                .expect("compaction plan")
                .is_none(),
            "step {step}: no compaction plan"
        );
        // The change before the loop set `lfp_battery`, so step 0 clears it and
        // each later step flips it: an equal value is not a change and queues
        // nothing.
        rebase_test_queue_mutation(&store, vehicle.vehicle_id, &binding, step % 2 == 1);
        let head = store
            .v2_head(vehicle.vehicle_id)
            .expect("head")
            .expect("served head");
        let replayed = rebase_test_replay(&store, rebase_test_now_ms());
        assert!(matches!(replayed, Ok(1)), "step {step} at {packs} packs: {replayed:?}");
        assert_eq!(
            store
                .v2_lineage_pack_count(vehicle.vehicle_id)
                .expect("pack count"),
            packs + 1,
            "the delta published"
        );
        let request = rebase_test_request(&store, vehicle.vehicle_id).expect("a rebase request");
        assert_eq!(request.head_digest, head.2.to_string(), "it names the head it saw");
        assert_eq!(request.state, V2LineageRebaseState::Requested);
        assert!(
            !store
                .has_unpublished_sync_mutations(vehicle.vehicle_id)
                .expect("pending lookup")
        );
    }
}

#[test]
fn design_later_106_a_a_refused_record_for_the_same_head_is_kept() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, mut lineage) = rebase_test_imported_base(&store);
    let head = store
        .v2_head(vehicle.vehicle_id)
        .expect("head")
        .expect("served head");
    let now = 5_000;
    let first = store
        .request_v2_lineage_rebase(vehicle.vehicle_id, head.2, "no_compaction_plan", now)
        .expect("request");
    assert_eq!(first.state, V2LineageRebaseState::Requested);
    assert_eq!(first.next_attempt_ms, now);
    // The same head asks again: the stored row is returned as it is.
    let again = store
        .request_v2_lineage_rebase(vehicle.vehicle_id, head.2, "other_reason", now + 99)
        .expect("idempotent request");
    assert_eq!(again, first);
    assert!(
        store
            .refuse_v2_lineage_rebase(vehicle.vehicle_id, head.2, "oversized")
            .expect("refusal")
    );
    let refused = rebase_test_request(&store, vehicle.vehicle_id).expect("record");
    assert_eq!(refused.state, V2LineageRebaseState::Refused);
    // The collector asks again at the trigger with a lowered limit: nothing changes.
    let small = ProtocolLimits {
        max_chunks: 8,
        ..ProtocolLimits::default()
    };
    compact_v2_lineage_if_needed_with_limits(&store, &rebase_test_cursor_key(), vehicle.vehicle_id, small)
        .expect("slots remain, so the compaction check is not an error");
    assert_eq!(
        rebase_test_request(&store, vehicle.vehicle_id),
        Some(refused.clone()),
        "a refused record for the same head is kept"
    );
    // A different head replaces it with a fresh, due request.
    rebase_test_grow(&store, vehicle.vehicle_id, &binding, &mut lineage);
    compact_v2_lineage_if_needed_with_limits(&store, &rebase_test_cursor_key(), vehicle.vehicle_id, small)
        .expect("slots remain");
    let moved = store
        .v2_head(vehicle.vehicle_id)
        .expect("head")
        .expect("served head");
    let replaced = rebase_test_request(&store, vehicle.vehicle_id).expect("record");
    assert_eq!(replaced.head_digest, moved.2.to_string());
    assert_eq!(replaced.state, V2LineageRebaseState::Requested);
    assert_ne!(replaced.head_digest, refused.head_digest);
}

#[test]
fn design_later_106_a_a_request_is_due_until_deferred_and_never_when_refused() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, _, _) = rebase_test_imported_base(&store);
    let head = store
        .v2_head(vehicle.vehicle_id)
        .expect("head")
        .expect("served head");
    let due = |now_ms: i64| crate::db::v2_lineage_rebase_due(temporary.path(), now_ms);
    assert!(!due(10_000), "no request, nothing due");
    store
        .request_v2_lineage_rebase(vehicle.vehicle_id, head.2, "no_compaction_plan", 10_000)
        .expect("request");
    assert!(!due(9_999), "not before its time");
    assert!(due(10_000));
    assert_eq!(store.defer_due_v2_lineage_rebases(10_000).expect("defer"), 1);
    assert_eq!(
        rebase_test_request(&store, vehicle.vehicle_id).map(|r| (r.attempts, r.next_attempt_ms)),
        Some((1, 11_000))
    );
    assert!(!due(10_500), "the first deferral waits one second");
    assert!(due(11_000));
    assert_eq!(store.defer_due_v2_lineage_rebases(11_000).expect("defer"), 1);
    assert_eq!(
        rebase_test_request(&store, vehicle.vehicle_id).map(|r| r.next_attempt_ms),
        Some(13_000),
        "the delay doubles"
    );
    assert_eq!(
        store.defer_due_v2_lineage_rebases(11_000).expect("defer"),
        0,
        "a request that is not due is not moved"
    );
    assert!(
        store
            .refuse_v2_lineage_rebase(vehicle.vehicle_id, head.2, "oversized")
            .expect("refusal")
    );
    assert!(!due(i64::MAX / 2), "a refused head is never due");
}

// Issue #106, task c: the swap and the recovery end to end. The rebase step
// is the one the hand-back publication thread runs under the gate.

use crate::db::{V2LineageRebaseLimits, V2LineageRebaseOutcome};

/// Limits under which a lineage of `RECOVERY_TEST_PACKS` packs is at the
/// compaction trigger (16 - 8) and a fresh base may hold 7 packs.
fn recovery_test_limits() -> V2LineageRebaseLimits {
    V2LineageRebaseLimits {
        protocol: ProtocolLimits {
            max_chunks: 16,
            ..ProtocolLimits::default()
        },
        ..V2LineageRebaseLimits::default()
    }
}

const RECOVERY_TEST_PACKS: usize = 9;

/// The rebase step as the publication thread runs it, under the gate.
fn recovery_test_attempt(
    store: &HubStore,
    now_ms: i64,
    limits: V2LineageRebaseLimits,
    stop: &dyn Fn() -> bool,
) -> Vec<(Uuid, V2LineageRebaseOutcome)> {
    let gate = store.try_acquire_publication_gate().expect("gate");
    store
        .rebase_due_v2_lineages(&gate, &rebase_test_cursor_key(), now_ms, limits, stop)
        .expect("rebase step")
}

/// A store restart: the old handle goes, the read-write opening sequence
/// (migrations, staging sweep, recovery) runs again.
fn recovery_test_restart(store: HubStore, path: &std::path::Path) -> HubStore {
    drop(store);
    HubStore::initialize(path).expect("store reopens")
}

fn recovery_test_lineage(store: &HubStore, vehicle_id: Uuid) -> LineageManifestV2 {
    store
        .lineage_manifest_for_vehicle(vehicle_id)
        .expect("lineage verifies")
        .expect("served lineage")
}

fn recovery_test_head(store: &HubStore, vehicle_id: Uuid) -> Sha256Digest {
    store
        .v2_head(vehicle_id)
        .expect("head")
        .expect("served head")
        .2
}

/// Every row of `table`, in key order, for before/after comparisons.
fn recovery_test_rows(store: &HubStore, table: &str) -> Vec<String> {
    let connection = store.open().expect("catalogue");
    let mut statement = connection
        .prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2, 3"))
        .expect("table");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|index| row.get::<_, rusqlite::types::Value>(index))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| format!("{values:?}"))
        })
        .expect("rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows read")
}

/// The tables a swap must leave as they are.
const RECOVERY_TEST_KEPT_TABLES: [&str; 7] = [
    "collection_coverage",
    "import_seam_links",
    "materialised_drives",
    "materialised_positions",
    "materialised_charges",
    "sync_mutations",
    "sync_mutation_sequences",
];

fn recovery_test_kept(store: &HubStore) -> Vec<Vec<String>> {
    RECOVERY_TEST_KEPT_TABLES
        .iter()
        .map(|table| recovery_test_rows(store, table))
        .collect()
}

/// A Hub-range drive and a seam decision: the vehicle has the Hub's own
/// collection, so the unplanned-seam guard refuses an import base on it.
fn recovery_test_hub_rows(store: &HubStore, vehicle_id: Uuid, car_id: i64) {
    let drive_id = crate::db::hub_ids::HUB_ID_BASE + 1;
    let connection = store.open().expect("catalogue");
    connection
        .execute(
            "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
             VALUES (?1, ?2, ?3, '{}')",
            params![vehicle_id.to_string(), drive_id, car_id],
        )
        .expect("hub drive");
    connection
        .execute(
            "INSERT INTO import_seam_links(vehicle_id, entity, loser_id, winner_id, decided_seq)
             VALUES (?1, 'drive', 5, ?2, 1)",
            params![vehicle_id.to_string(), drive_id],
        )
        .expect("seam decision");
}

fn recovery_test_files(store: &HubStore) -> usize {
    std::fs::read_dir(store.packs_dir().join("sha256"))
        .map(|entries| entries.count())
        .unwrap_or(0)
}

fn recovery_test_outbox(store: &HubStore, vehicle_id: Uuid) -> Option<(i64, Option<String>)> {
    store
        .open()
        .expect("catalogue")
        .query_row(
            "SELECT next_attempt_ms, last_error FROM export_outbox WHERE vehicle_id = ?1",
            params![vehicle_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .expect("outbox lookup")
}

/// Delta 1 on the new base `rebased` (no deltas yet) published the queued
/// change once: one delta, from the base, nothing pending, and a second
/// replay publishes nothing.
fn recovery_test_delta_one_once(store: &HubStore, vehicle_id: Uuid, rebased: &LineageManifestV2) {
    let replayed = rebase_test_replay(store, rebase_test_now_ms());
    assert!(matches!(replayed, Ok(1)), "delta 1 publishes: {replayed:?}");
    let lineage = recovery_test_lineage(store, vehicle_id);
    assert_eq!(lineage.base, rebased.base, "on the new base");
    assert_eq!(lineage.deltas.len(), 1, "exactly one delta");
    assert_eq!(lineage.deltas[0].from_sequence, rebased.head_sequence);
    assert_eq!(lineage.deltas[0].parent_chain_digest, rebased.head_digest);
    assert!(
        !store
            .has_unpublished_sync_mutations(vehicle_id)
            .expect("pending lookup"),
        "the reclaimed change is published"
    );
    let again = rebase_test_replay(store, rebase_test_now_ms());
    assert!(matches!(again, Ok(0)), "nothing publishes twice: {again:?}");
    assert_eq!(recovery_test_lineage(store, vehicle_id), lineage);
}

/// A lineage of `RECOVERY_TEST_PACKS` imported packs, a due request naming
/// its head, and one queued change.
fn recovery_test_small_rig(
    store: &HubStore,
) -> (crate::db::VehicleRecord, ProjectionBinding, LineageManifestV2) {
    let (vehicle, binding, mut lineage) = rebase_test_imported_base(store);
    rebase_test_grow_to(store, vehicle.vehicle_id, &binding, &mut lineage, RECOVERY_TEST_PACKS);
    rebase_test_queue_mutation(store, vehicle.vehicle_id, &binding, true);
    let head = recovery_test_head(store, vehicle.vehicle_id);
    store
        .request_v2_lineage_rebase(vehicle.vehicle_id, head, "no_compaction_plan", 1_000)
        .expect("request");
    (vehicle, binding, lineage)
}

#[test]
fn design_later_106_c_fill_refuse_restart_rebase_and_delta_one_publishes_once() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let path = temporary.path();
    let mut store = HubStore::initialize(path).expect("store");
    let (vehicle, binding, mut lineage) = rebase_test_imported_base(&store);
    let vehicle_id = vehicle.vehicle_id;
    let limit = ProtocolLimits::default().max_chunks;
    // Fill: imports up to the last slot, then one collector delta into it,
    // so the full lineage ends in a collector span and holds a published
    // change ("other mutations").
    rebase_test_grow_to(&store, vehicle_id, &binding, &mut lineage, limit - 1);
    rebase_test_queue_mutation(&store, vehicle_id, &binding, true);
    assert!(matches!(rebase_test_replay(&store, rebase_test_now_ms()), Ok(1)));
    assert_eq!(store.v2_lineage_pack_count(vehicle_id).expect("packs"), limit);
    recovery_test_hub_rows(&store, vehicle_id, binding.selected_car_id);
    rebase_test_queue_mutation(&store, vehicle_id, &binding, false);
    let full = recovery_test_lineage(&store, vehicle_id);

    // Crash table, before the request: a restart keeps the old head and the
    // queued change; the next replay refuses and asks.
    store = recovery_test_restart(store, path);
    assert_eq!(recovery_test_lineage(&store, vehicle_id), full);
    assert!(matches!(rebase_test_replay(&store, rebase_test_now_ms()), Ok(0)));
    assert_eq!(
        rebase_test_outbox_error(&store, vehicle_id).as_deref(),
        Some("lineage_capacity_exhausted")
    );
    let request = rebase_test_request(&store, vehicle_id).expect("request");
    assert_eq!(request.head_digest, full.head_digest.to_string());
    assert_eq!(request.state, V2LineageRebaseState::Requested);
    let kept = recovery_test_kept(&store);
    assert!(store.unplanned_import_seam_refused(vehicle_id).expect("guard"));

    // Crash table, request written: a restart finds it due and the step
    // rebases.
    store = recovery_test_restart(store, path);
    let now = rebase_test_now_ms();
    assert!(crate::db::v2_lineage_rebase_due(path, now), "the start spawns the step");
    let outcomes = recovery_test_attempt(&store, now, V2LineageRebaseLimits::default(), &|| false);
    assert_eq!(outcomes.len(), 1);
    let V2LineageRebaseOutcome::Rebased { head_sequence } = outcomes[0].1 else {
        panic!("rebased: {outcomes:?}");
    };
    assert_eq!(outcomes[0].0, vehicle_id);
    assert!(head_sequence > full.head_sequence);
    let rebased = recovery_test_lineage(&store, vehicle_id);
    assert_eq!(rebased.head_sequence, head_sequence);
    assert_eq!(rebased.base.sequence, head_sequence);
    assert!(rebased.deltas.is_empty(), "the new head is the base alone");
    assert_eq!(rebased.base.packs.len(), 1);
    assert_ne!(rebased.base.snapshot_id, full.base.snapshot_id);
    assert_eq!(
        (rebased.installation_id, rebased.account_id, rebased.generation),
        (full.installation_id, full.account_id, full.generation),
        "the binding is unchanged"
    );
    assert_eq!(
        store.v2_projection_binding(vehicle_id).expect("binding"),
        binding
    );
    // The old lineage is a checkpoint until it expires, its packs kept.
    for (sequence, digest) in [(full.head_sequence, full.head_digest), (full.base.sequence, full.base.digest)] {
        assert!(
            store
                .retired_lineage_contains_checkpoint(vehicle_id, sequence, digest)
                .expect("checkpoint")
        );
    }
    for pack in full.base.packs.iter().chain(full.deltas.iter().map(|delta| &delta.pack)) {
        assert!(
            store.pack_sha256_is_retained(&pack.sha256.to_string()).expect("retention"),
            "old pack {} is retained",
            pack.sha256
        );
    }
    assert_eq!(recovery_test_kept(&store), kept, "coverage, seam, rows and mutations unchanged");
    assert!(store.has_unpublished_sync_mutations(vehicle_id).expect("pending"));
    assert_eq!(rebase_test_request(&store, vehicle_id), None, "the request is gone");
    assert_eq!(recovery_test_outbox(&store, vehicle_id), Some((0, None)), "the outbox is due");
    assert!(
        store.unplanned_import_seam_refused(vehicle_id).expect("guard"),
        "an import base replacement on this vehicle is still refused"
    );

    // Crash table, swap committed before delta 1: a restart serves the new
    // base and the collector publishes delta 1 once.
    store = recovery_test_restart(store, path);
    assert_eq!(recovery_test_lineage(&store, vehicle_id), rebased);
    assert!(store.has_unpublished_sync_mutations(vehicle_id).expect("pending"));
    recovery_test_delta_one_once(&store, vehicle_id, &rebased);
    assert!(
        !crate::db::v2_lineage_rebase_due(path, i64::MAX / 2),
        "nothing is asked again"
    );

    // Expiry ends the checkpoint.
    store
        .open()
        .expect("catalogue")
        .execute(
            "UPDATE sync_retired_lineages SET retired_at_ms = 0, expires_at_ms = 1",
            [],
        )
        .expect("expire");
    assert!(
        !store
            .retired_lineage_contains_checkpoint(vehicle_id, full.head_sequence, full.head_digest)
            .expect("checkpoint")
    );
}

#[test]
fn design_later_106_c_a_crash_inside_the_swap_or_build_reopens_to_the_old_lineage_and_retries() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let path = temporary.path();
    let mut store = HubStore::initialize(path).expect("store");
    let (vehicle, _, old) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    let kept = recovery_test_kept(&store);
    let limits = recovery_test_limits();
    {
        let gate = store.try_acquire_publication_gate().expect("gate");
        let candidate = store
            .build_v2_lineage_rebase(&gate, vehicle_id, &rebase_test_cursor_key(), limits, &|| false)
            .expect("candidate");
        // The swap runs to its last statement and does not commit.
        store
            .rolled_back_v2_lineage_rebase(&gate, &candidate)
            .expect("the swap itself succeeds");
        // The candidate packs stay behind unreferenced, as after a crash.
        drop(candidate);
    }
    // A fold scratch file a crash left in the staging directory.
    let scratch = store
        .packs_dir()
        .join(".staging")
        .join(format!("{}.lineage-fold.sqlite.tmp", Uuid::new_v4()));
    std::fs::write(&scratch, b"interrupted fold").expect("scratch");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o600)).expect("mode");
    }
    store = recovery_test_restart(store, path);
    assert!(!scratch.exists(), "the opening sweeps the fold scratch");
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old, "the old lineage is complete");
    assert_eq!(recovery_test_kept(&store), kept);
    let request = rebase_test_request(&store, vehicle_id).expect("request");
    assert_eq!(request.state, V2LineageRebaseState::Requested);
    assert!(store.has_unpublished_sync_mutations(vehicle_id).expect("pending"));

    let outcomes = recovery_test_attempt(&store, rebase_test_now_ms(), limits, &|| false);
    assert!(
        matches!(outcomes.as_slice(), [(id, V2LineageRebaseOutcome::Rebased { .. })] if *id == vehicle_id),
        "{outcomes:?}"
    );
    let rebased = recovery_test_lineage(&store, vehicle_id);
    assert!(rebased.deltas.is_empty());
    assert_eq!(recovery_test_kept(&store), kept);
    recovery_test_delta_one_once(&store, vehicle_id, &rebased);
}

#[test]
fn design_later_106_c_a_stop_during_the_build_discards_and_defers() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, _, old) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    let files = recovery_test_files(&store);
    let now = rebase_test_now_ms();
    // The stop arrives once the build has started.
    let asked = std::cell::Cell::new(0);
    let stop = || {
        asked.set(asked.get() + 1);
        asked.get() > 2
    };
    let outcomes = recovery_test_attempt(&store, now, recovery_test_limits(), &stop);
    assert!(
        matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Deferred(_))]),
        "{outcomes:?}"
    );
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
    assert_eq!(recovery_test_files(&store), files, "no candidate pack is left");
    let request = rebase_test_request(&store, vehicle_id).expect("request");
    assert_eq!(request.state, V2LineageRebaseState::Requested);
    assert_eq!((request.attempts, request.next_attempt_ms), (1, now + 1_000));
    assert!(
        recovery_test_attempt(&store, now + 999, recovery_test_limits(), &|| false).is_empty(),
        "not before its delay"
    );
    let outcomes = recovery_test_attempt(&store, now + 1_000, recovery_test_limits(), &|| false);
    assert!(
        matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Rebased { .. })]),
        "{outcomes:?}"
    );
    let rebased = recovery_test_lineage(&store, vehicle_id);
    recovery_test_delta_one_once(&store, vehicle_id, &rebased);
}

#[test]
fn design_later_106_c_a_head_moved_between_prepare_and_swap_refuses_and_retries() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, mut lineage) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    let limits = recovery_test_limits();
    let first = recovery_test_head(&store, vehicle_id);
    {
        let gate = store.try_acquire_publication_gate().expect("gate");
        let candidate = store
            .build_v2_lineage_rebase(&gate, vehicle_id, &rebase_test_cursor_key(), limits, &|| false)
            .expect("candidate");
        drop(gate);
        // Another publication moves the head before the swap.
        rebase_test_grow(&store, vehicle_id, &binding, &mut lineage);
        let gate = store.try_acquire_publication_gate().expect("gate");
        let moved = recovery_test_lineage(&store, vehicle_id);
        assert!(matches!(
            store.commit_v2_lineage_rebase(&gate, &candidate),
            Err(StoreError::LineageCatalogConflict)
        ));
        assert_eq!(recovery_test_lineage(&store, vehicle_id), moved, "nothing changed");
        store.discard_v2_lineage_rebase(&gate, candidate).expect("discard");
    }
    // The lost swap was transient: the request, still naming the first head,
    // is deferred and retried on the head that serves now.
    let now = rebase_test_now_ms();
    assert_eq!(store.defer_due_v2_lineage_rebases(now).expect("defer"), 1);
    let request = rebase_test_request(&store, vehicle_id).expect("request");
    assert_eq!(request.head_digest, first.to_string());
    let outcomes = recovery_test_attempt(&store, now + 1_000, limits, &|| false);
    assert!(
        matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Rebased { .. })]),
        "{outcomes:?}"
    );
    let rebased = recovery_test_lineage(&store, vehicle_id);
    assert!(
        store
            .retired_lineage_contains_checkpoint(vehicle_id, lineage.head_sequence, lineage.head_digest)
            .expect("checkpoint"),
        "the moved head is the one retired"
    );
    recovery_test_delta_one_once(&store, vehicle_id, &rebased);
}

#[test]
fn design_later_106_c_a_pre_takeover_import_succeeds_on_the_new_base_and_unchanged_answers_unchanged() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, old) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    // The import's per-row state, as a digest-state import leaves it.
    let connection = store.open().expect("catalogue");
    connection
        .execute(
            "INSERT INTO import_projection_state_heads(
                vehicle_id, source_id, base_snapshot_id, selected_car_id, head_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                vehicle_id.to_string(),
                binding.account_id.to_string(),
                old.base.snapshot_id.to_string(),
                binding.selected_car_id,
                i64::try_from(old.head_sequence).expect("sequence"),
            ],
        )
        .expect("state head");
    connection
        .execute(
            "INSERT INTO import_projection_state_rows(
                vehicle_id, entity_ordinal, entity_id, car_id, projection_sha256)
             VALUES (?1, 1, 7, ?2, ?3)",
            params![vehicle_id.to_string(), binding.selected_car_id, vec![7_u8; 32]],
        )
        .expect("state row");
    drop(connection);
    let state_rows = recovery_test_rows(&store, "import_projection_state_rows");
    let inventory = store
        .source_db_import_projection_inventory(vehicle_id, binding.account_id, binding.selected_car_id)
        .expect("inventory before");
    // The last import's fingerprint (the rig's successor to the head).
    let fingerprint = Sha256Digest::of_bytes(&old.head_sequence.to_le_bytes());
    assert!(store.source_fingerprint_matches(vehicle_id, fingerprint).expect("match"));

    let outcomes = recovery_test_attempt(&store, rebase_test_now_ms(), recovery_test_limits(), &|| false);
    let [(_, V2LineageRebaseOutcome::Rebased { head_sequence })] = outcomes.as_slice() else {
        panic!("rebased: {outcomes:?}");
    };
    let mut rebased = recovery_test_lineage(&store, vehicle_id);
    // An unchanged import answers unchanged, with the new base.
    assert!(store.source_fingerprint_matches(vehicle_id, fingerprint).expect("match"));
    let manifest = store
        .manifest_for_snapshot_fingerprint(vehicle_id, fingerprint)
        .expect("lookup")
        .expect("the fingerprint names the served base");
    assert_eq!(manifest.snapshot_id, rebased.base.snapshot_id);
    assert_eq!(manifest.head_sequence, *head_sequence);
    // The import's state moved to the new base, its rows unchanged.
    assert_eq!(recovery_test_rows(&store, "import_projection_state_rows"), state_rows);
    let head: (String, i64) = store
        .open()
        .expect("catalogue")
        .query_row(
            "SELECT base_snapshot_id, head_sequence FROM import_projection_state_heads
              WHERE vehicle_id = ?1",
            params![vehicle_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("state head kept");
    assert_eq!(
        head,
        (rebased.base.snapshot_id.to_string(), i64::try_from(*head_sequence).expect("sequence"))
    );
    assert_eq!(
        store
            .source_db_import_projection_inventory(vehicle_id, binding.account_id, binding.selected_car_id)
            .expect("inventory after"),
        inventory
    );
    // A changed import publishes its successor on the new base.
    rebase_test_grow(&store, vehicle_id, &binding, &mut rebased);
    let grown = recovery_test_lineage(&store, vehicle_id);
    assert_eq!(grown.base, rebased.base);
    assert_eq!(grown.deltas.len(), 1);
    assert_eq!(grown.deltas[0].from_sequence, *head_sequence);
}

#[test]
fn design_later_106_c_a_pending_import_publication_or_staging_generation_withholds() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let (vehicle, binding, old) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    let limits = recovery_test_limits();
    let now = rebase_test_now_ms();
    let intent_key = format!("import_publication_intent:{vehicle_id}");
    let set_intent = |present: bool| {
        let connection = store.open().expect("catalogue");
        if present {
            connection
                .execute(
                    "INSERT INTO hub_metadata(key, value) VALUES (?1, '{\"pending\":true}')",
                    params![intent_key],
                )
                .expect("intent");
        } else {
            connection
                .execute("DELETE FROM hub_metadata WHERE key = ?1", params![intent_key])
                .expect("intent removed");
        }
    };
    // A pending #60 publication intent withholds the rebase, and the swap.
    set_intent(true);
    let outcomes = recovery_test_attempt(&store, now, limits, &|| false);
    assert!(matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Withheld)]), "{outcomes:?}");
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
    assert_eq!(rebase_test_request(&store, vehicle_id).map(|r| r.attempts), Some(1));
    {
        set_intent(false);
        let gate = store.try_acquire_publication_gate().expect("gate");
        let candidate = store
            .build_v2_lineage_rebase(&gate, vehicle_id, &rebase_test_cursor_key(), limits, &|| false)
            .expect("candidate");
        set_intent(true);
        assert!(matches!(
            store.commit_v2_lineage_rebase(&gate, &candidate),
            Err(StoreError::LineageCatalogConflict)
        ));
        store.discard_v2_lineage_rebase(&gate, candidate).expect("discard");
        assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
        set_intent(false);
    }
    // So does an import generation that is still staging.
    store
        .open()
        .expect("catalogue")
        .execute(
            "INSERT INTO import_generations(run_id, source_id, vehicle_id, car_id, status, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, 'staging', 1)",
            params![
                Uuid::new_v4().to_string(),
                binding.account_id.to_string(),
                vehicle_id.to_string(),
                binding.selected_car_id
            ],
        )
        .expect("staging generation");
    let outcomes = recovery_test_attempt(&store, now + 1_000, limits, &|| false);
    assert!(matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Withheld)]), "{outcomes:?}");
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
    store
        .open()
        .expect("catalogue")
        .execute("DELETE FROM import_generations", [])
        .expect("generation ends");
    let outcomes = recovery_test_attempt(&store, now + 3_000, limits, &|| false);
    assert!(matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Rebased { .. })]), "{outcomes:?}");
}

#[test]
fn design_later_106_c_an_oversized_refusal_is_not_retried_after_restart() {
    let temporary = crate::private_tempdir().expect("temporary store");
    let path = temporary.path();
    let mut store = HubStore::initialize(path).expect("store");
    let (vehicle, _, old) = recovery_test_small_rig(&store);
    let vehicle_id = vehicle.vehicle_id;
    let head = recovery_test_head(&store, vehicle_id);
    let files = recovery_test_files(&store);
    // At 9 chunks the trigger is 1 pack and a fresh base may hold none.
    let small = ProtocolLimits {
        max_chunks: 9,
        ..ProtocolLimits::default()
    };
    let limits = V2LineageRebaseLimits {
        protocol: small,
        ..V2LineageRebaseLimits::default()
    };
    let now = rebase_test_now_ms();
    let outcomes = recovery_test_attempt(&store, now, limits, &|| false);
    assert!(
        matches!(outcomes.as_slice(), [(_, V2LineageRebaseOutcome::Refused("lineage_rebase_oversized"))]),
        "{outcomes:?}"
    );
    let refused = rebase_test_request(&store, vehicle_id).expect("record");
    assert_eq!(refused.state, V2LineageRebaseState::Refused);
    assert_eq!(refused.head_digest, head.to_string());
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
    assert_eq!(recovery_test_files(&store), files, "no candidate pack is left");

    store = recovery_test_restart(store, path);
    assert!(!crate::db::v2_lineage_rebase_due(path, i64::MAX / 2));
    assert!(recovery_test_attempt(&store, i64::MAX / 2, limits, &|| false).is_empty());
    // The collector asks again for the same head: the refusal stays. (The
    // lineage is full at these limits, so the check itself reports that.)
    let _full = compact_v2_lineage_if_needed_with_limits(
        &store,
        &rebase_test_cursor_key(),
        vehicle_id,
        small,
    );
    assert_eq!(rebase_test_request(&store, vehicle_id), Some(refused));
    assert_eq!(recovery_test_lineage(&store, vehicle_id), old);
}
