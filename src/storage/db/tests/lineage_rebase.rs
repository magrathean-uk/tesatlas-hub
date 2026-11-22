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
