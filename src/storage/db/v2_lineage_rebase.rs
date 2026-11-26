// SPDX-License-Identifier: AGPL-3.0-only

// The request for a fresh compatibility base (issue #106). A vehicle's 1.x
// compatibility lineage that has no collector-owned suffix left to compact
// cannot take another delta once it holds all the packs a client can hold.
// The collector records one request per vehicle, naming the head it saw, and
// keeps serving that head; a later step folds the lineage into one base. This
// file holds the request only: one `hub_metadata` row per vehicle, so it needs
// no schema step and travels with backups. This file is `include!`d into
// `db.rs`.

/// The `hub_metadata` key prefix of a vehicle's rebase request.
const V2_LINEAGE_REBASE_PREFIX: &str = "v2_lineage_rebase:";
/// The `hub_metadata` key prefix of the mark a swap leaves when the lineage it
/// retired held collector deltas: the fresh base holds their rows, and the
/// mark stands in for their spans as the vehicle's live history.
const V2_LINEAGE_REBASE_LIVE_PREFIX: &str = "v2_lineage_rebase_live:";
const V2_LINEAGE_REBASE_FORMAT: u32 = 1;
/// A transient failure waits one second, then doubles, up to one hour.
const V2_LINEAGE_REBASE_FIRST_DELAY_MS: i64 = 1_000;
const V2_LINEAGE_REBASE_LONGEST_DELAY_MS: i64 = 60 * 60 * 1_000;

fn v2_lineage_rebase_key(vehicle_id: Uuid) -> String {
    format!("{V2_LINEAGE_REBASE_PREFIX}{vehicle_id}")
}

/// Whether the request still waits for an attempt, or the head was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum V2LineageRebaseState {
    /// To be tried at `next_attempt_ms`.
    Requested,
    /// This head cannot be rebased (oversized, inexact or damaged); it is not
    /// tried again until the head changes.
    Refused,
}

/// The stored request: the head digest it names, its state and the earliest
/// time of the next attempt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct V2LineageRebaseRequest {
    pub(crate) format: u32,
    /// The served head's digest (hex) at the time of the request.
    pub(crate) head_digest: String,
    pub(crate) state: V2LineageRebaseState,
    /// Why the collector asked, or why the head was refused.
    pub(crate) reason: String,
    pub(crate) next_attempt_ms: i64,
    /// Transient failures so far; the next delay doubles with it.
    #[serde(default)]
    pub(crate) attempts: u32,
}

impl V2LineageRebaseRequest {
    fn is_due(&self, now_ms: i64) -> bool {
        self.state == V2LineageRebaseState::Requested && self.next_attempt_ms <= now_ms
    }

    /// Moves the next attempt out by the next doubled delay.
    fn defer(&mut self, now_ms: i64) {
        let delay = V2_LINEAGE_REBASE_FIRST_DELAY_MS
            .checked_shl(self.attempts.min(16))
            .unwrap_or(V2_LINEAGE_REBASE_LONGEST_DELAY_MS)
            .min(V2_LINEAGE_REBASE_LONGEST_DELAY_MS);
        self.attempts = self.attempts.saturating_add(1);
        self.next_attempt_ms = now_ms.saturating_add(delay);
    }
}

fn v2_lineage_rebase_request_in(
    connection: &Connection,
    vehicle_id: Uuid,
) -> Result<Option<V2LineageRebaseRequest>, StoreError> {
    Ok(metadata_value_in(connection, &v2_lineage_rebase_key(vehicle_id))?
        .and_then(|value| serde_json::from_str::<V2LineageRebaseRequest>(&value).ok())
        .filter(|request| request.format == V2_LINEAGE_REBASE_FORMAT))
}

/// Whether the vehicle has collector history in its served lineage: a live
/// delta span, or the mark of a swap that folded such deltas into its base
/// (REV-CX-ISS106 finding 6). Either fences the import base against
/// replacement.
fn v2_live_delta_history_in(connection: &Connection, vehicle_key: &str) -> Result<bool, StoreError> {
    let spans: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sync_live_delta_spans WHERE vehicle_id = ?1)",
            params![vehicle_key],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)?;
    if spans {
        return Ok(true);
    }
    Ok(metadata_value_in(connection, &format!("{V2_LINEAGE_REBASE_LIVE_PREFIX}{vehicle_key}"))?.is_some())
}

fn put_v2_lineage_rebase_request_in(
    connection: &Connection,
    vehicle_id: Uuid,
    request: &V2LineageRebaseRequest,
) -> Result<(), StoreError> {
    let value = serde_json::to_string(request)
        .map_err(|error| StoreError::SyncMutation(format!("rebase request encoding: {error}")))?;
    put_metadata_in(connection, &v2_lineage_rebase_key(vehicle_id), &value)
}

impl HubStore {
    /// Records that the vehicle's served head needs a fresh base. Idempotent:
    /// a request, or a refusal, that already names this head digest is left
    /// as it is; one that names another head (or does not read back) is
    /// replaced by a new request that is due at `now_ms`. It changes no
    /// served lineage.
    pub(crate) fn request_v2_lineage_rebase(
        &self,
        vehicle_id: Uuid,
        head_digest: Sha256Digest,
        reason: &str,
        now_ms: i64,
    ) -> Result<V2LineageRebaseRequest, StoreError> {
        validate_timestamp("rebase request now_ms", now_ms)?;
        let head_digest = head_digest.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        if let Some(existing) = v2_lineage_rebase_request_in(&transaction, vehicle_id)?
            && existing.head_digest == head_digest
        {
            return Ok(existing);
        }
        let request = V2LineageRebaseRequest {
            format: V2_LINEAGE_REBASE_FORMAT,
            head_digest,
            state: V2LineageRebaseState::Requested,
            reason: reason.chars().filter(|c| !c.is_control()).take(64).collect(),
            next_attempt_ms: now_ms,
            attempts: 0,
        };
        put_v2_lineage_rebase_request_in(&transaction, vehicle_id, &request)?;
        transaction.commit().map_err(StoreError::Query)?;
        Ok(request)
    }

    /// Marks the request that names `head_digest` refused for `reason`, so it
    /// is not tried again until the head changes. A request for another head
    /// is left as it is. The step that attempts a rebase calls this.
    pub(crate) fn refuse_v2_lineage_rebase(
        &self,
        vehicle_id: Uuid,
        head_digest: Sha256Digest,
        reason: &str,
    ) -> Result<bool, StoreError> {
        let head_digest = head_digest.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let Some(mut request) = v2_lineage_rebase_request_in(&transaction, vehicle_id)? else {
            return Ok(false);
        };
        if request.head_digest != head_digest {
            return Ok(false);
        }
        request.state = V2LineageRebaseState::Refused;
        request.reason = reason.chars().filter(|c| !c.is_control()).take(64).collect();
        put_v2_lineage_rebase_request_in(&transaction, vehicle_id, &request)?;
        transaction.commit().map_err(StoreError::Query)?;
        Ok(true)
    }

    /// The vehicle's stored request, when there is one that reads back.
    #[cfg(test)]
    pub(crate) fn v2_lineage_rebase_request(
        &self,
        vehicle_id: Uuid,
    ) -> Result<Option<V2LineageRebaseRequest>, StoreError> {
        let connection = self.open_read_only_connection()?;
        v2_lineage_rebase_request_in(&connection, vehicle_id)
    }

    /// Pushes the next attempt of every request that is due out by its next
    /// doubled delay and returns how many it moved. The publication thread
    /// calls it when it cannot attempt the requests at all (no cursor key, a
    /// busy gate), so a due request is asked for again at a growing interval,
    /// not at every supervisor step.
    pub(crate) fn defer_due_v2_lineage_rebases(&self, now_ms: i64) -> Result<usize, StoreError> {
        validate_timestamp("rebase request now_ms", now_ms)?;
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let mut deferred = 0;
        for (vehicle_id, mut request) in v2_lineage_rebase_requests_in(&transaction)? {
            if !request.is_due(now_ms) {
                continue;
            }
            request.defer(now_ms);
            put_v2_lineage_rebase_request_in(&transaction, vehicle_id, &request)?;
            deferred += 1;
        }
        transaction.commit().map_err(StoreError::Query)?;
        Ok(deferred)
    }
}

/// Every request that reads back, by vehicle.
fn v2_lineage_rebase_requests_in(
    connection: &Connection,
) -> Result<Vec<(Uuid, V2LineageRebaseRequest)>, StoreError> {
    let mut statement = connection
        .prepare("SELECT key, value FROM hub_metadata WHERE key LIKE 'v2_lineage_rebase:%' ORDER BY key")
        .map_err(StoreError::Query)?;
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
        .map_err(StoreError::Query)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Query)?;
    Ok(rows
        .into_iter()
        .filter_map(|(key, value)| {
            let vehicle_id = key.strip_prefix(V2_LINEAGE_REBASE_PREFIX)?.parse().ok()?;
            let request = serde_json::from_str::<V2LineageRebaseRequest>(&value).ok()?;
            (request.format == V2_LINEAGE_REBASE_FORMAT).then_some((vehicle_id, request))
        })
        .collect())
}

/// Whether the store in `data_dir` holds a request that is due at `now_ms`.
/// It opens the catalogue read-only and writes nothing; a store that is
/// absent, not readable or without the table has none, and so has one whose
/// catalogue path the store's own admission refuses (a symlink, a foreign
/// owner or mode), as the read-only opener does (REV-CX-ISS106 finding 7).
pub fn v2_lineage_rebase_due(data_dir: &Path, now_ms: i64) -> bool {
    let database = data_dir.join("hub.sqlite");
    if !matches!(admit_catalogue_paths(&database), Ok(CatalogueAdmission::Admitted)) {
        return false;
    }
    let Ok(connection) = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return false;
    };
    if connection.busy_timeout(Duration::from_secs(1)).is_err() {
        return false;
    }
    v2_lineage_rebase_requests_in(&connection)
        .is_ok_and(|requests| requests.iter().any(|(_, request)| request.is_due(now_ms)))
}

// The fresh base itself (issue #106, task b). Under the publication gate the
// served lineage is folded with the client application rule (base packs,
// then every delta in sequence order; the last mention of a key wins and a
// tombstone removes it; repeated base rows must be identical) into a scratch
// file, written as self-contained schema-2.1 packs, read back against the
// fold and signed. Nothing served changes: the catalogue is only read, and
// the candidate's packs are unreferenced until a swap catalogues them.

/// The rows of one fresh-base pack unless the receiver profile is lower.
const V2_LINEAGE_REBASE_ROWS_PER_PACK: u64 = 100_000;

/// The receiver profile a fresh base must fit and its target rows per pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2LineageRebaseLimits {
    pub(crate) protocol: ProtocolLimits,
    pub(crate) rows_per_pack: u64,
}

impl Default for V2LineageRebaseLimits {
    fn default() -> Self {
        Self {
            protocol: ProtocolLimits::default(),
            rows_per_pack: V2_LINEAGE_REBASE_ROWS_PER_PACK,
        }
    }
}

impl V2LineageRebaseLimits {
    /// The most base packs that leave the new lineage below the collector's
    /// compaction trigger (`max_chunks - 8`) with one change after it.
    pub(crate) fn base_pack_limit(&self) -> usize {
        let trigger = self
            .protocol
            .max_chunks
            .saturating_sub(self.protocol.max_chunks.min(8));
        trigger.saturating_sub(1)
    }
}

/// A verified fresh base for one vehicle, not yet served. `expected` is the
/// served lineage it was folded from, for the swap's compare-and-swap; the
/// packs are unreferenced until that swap, and
/// [`HubStore::discard_v2_lineage_rebase`] removes those this build created.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct V2LineageRebaseCandidate {
    pub(crate) vehicle_id: Uuid,
    pub(crate) expected: LineageManifestV2,
    pub(crate) binding: ProjectionBinding,
    /// Signed full-snapshot manifest of the new base, sequence
    /// `{sequence, sequence}`; ISS106c reserves that marker in the swap.
    pub(crate) manifest: SyncManifest,
    pub(crate) packs: Vec<crate::hub_pack::BuiltProjectionPack>,
    /// Rows per table of the fold the packs reproduce.
    pub(crate) rows: crate::hub_pack::LineageFoldCounts,
}

impl From<crate::hub_pack::LineageFoldError> for StoreError {
    fn from(error: crate::hub_pack::LineageFoldError) -> Self {
        match error {
            crate::hub_pack::LineageFoldError::Damaged(message) => {
                StoreError::LineageRebaseDamaged(message)
            }
            crate::hub_pack::LineageFoldError::Inexact(message) => {
                StoreError::LineageRebaseInexact(message)
            }
            crate::hub_pack::LineageFoldError::Scratch(error) => StoreError::LineageRebasePack(error),
            crate::hub_pack::LineageFoldError::Stopped => StoreError::LineageRebaseStopped,
        }
    }
}

impl StoreError {
    /// Whether this failure refuses the head for good (oversized, inexact or
    /// damaged), so the request is marked refused; every other failure of a
    /// rebase attempt is transient and the request is deferred.
    pub(crate) fn refuses_lineage_rebase(&self) -> bool {
        matches!(
            self,
            StoreError::LineageRebaseOversized { .. }
                | StoreError::LineageRebaseInexact(..)
                | StoreError::LineageRebaseDamaged(..)
        )
    }
}

/// A pack the writer refuses for its content cannot hold the fold as it is;
/// any other writer failure (space, I/O) is worth retrying.
fn lineage_rebase_pack_error(error: ProjectionPackError) -> StoreError {
    match error {
        ProjectionPackError::Invalid(_)
        | ProjectionPackError::TooManyRows
        | ProjectionPackError::Protocol(_) => StoreError::LineageRebaseInexact(error.to_string()),
        other => StoreError::LineageRebasePack(other),
    }
}

/// A served pack that is not on disk, or not its catalogued length, is
/// damage of the head wherever a rebase meets it (REV-CX-ISS106 finding 8).
fn lineage_rebase_missing_pack(error: StoreError) -> StoreError {
    match error {
        StoreError::LineagePackNotReady => {
            StoreError::LineageRebaseDamaged("a served pack is missing".to_owned())
        }
        other => other,
    }
}

/// A writer refusal at a target above the requested one: the pack outgrew a
/// byte or row ceiling, so no larger cut fits either.
fn lineage_rebase_pack_capacity_error(error: &ProjectionPackError) -> bool {
    use crate::protocol::ProtocolError;
    match error {
        ProjectionPackError::TooManyRows => true,
        ProjectionPackError::Protocol(error) => matches!(
            error,
            ProtocolError::CompressedSizeOutOfBounds(_)
                | ProtocolError::UncompressedSizeOutOfBounds(_)
                | ProtocolError::RowCountOutOfBounds(_)
                | ProtocolError::PackTooLarge
        ),
        ProjectionPackError::Insert(error)
        | ProjectionPackError::Commit(error)
        | ProjectionPackError::FinalizeSqlite(error) => {
            error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull)
        }
        _ => false,
    }
}

impl HubStore {
    /// Folds the vehicle's served compatibility lineage into a verified,
    /// signed candidate base whose decoded rows equal the fold, or refuses:
    /// `LineageRebaseOversized` (more packs than [`V2LineageRebaseLimits::
    /// base_pack_limit`]), `LineageRebaseInexact` (repeated base rows that
    /// differ, a fold that is not one car's representable history, or a
    /// readback that differs) or `LineageRebaseDamaged` (a served pack that is
    /// missing or fails its digest, length or layout). `stop` is asked between
    /// packs; it ends the build with `LineageRebaseStopped`. The caller holds
    /// the publication gate; nothing in the catalogue is written, and on any
    /// failure the packs this build created are removed.
    pub(crate) fn build_v2_lineage_rebase(
        &self,
        publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        cursor_key: &CursorKey,
        limits: V2LineageRebaseLimits,
        stop: &dyn Fn() -> bool,
    ) -> Result<V2LineageRebaseCandidate, StoreError> {
        let expected = self
            .lineage_manifest_for_vehicle_with_verification(
                vehicle_id,
                LineagePackVerification::MetadataOnly,
            )
            .map_err(lineage_rebase_missing_pack)?
            .ok_or(StoreError::LineageCatalogConflict)?;
        let binding = self.v2_projection_binding(vehicle_id)?;
        if expected.vehicle_id != binding.vehicle_id
            || expected.installation_id != binding.installation_id
            || expected.account_id != binding.account_id
            || expected.generation != binding.generation
        {
            return Err(StoreError::LineageCatalogConflict);
        }
        if expected.schema != crate::protocol::HUB_PROJECTION_SCHEMA_V1
            && expected.schema != HUB_PROJECTION_SCHEMA_V2
        {
            return Err(StoreError::LineageRebaseInexact(format!(
                "schema {}.{} has no typed fold",
                expected.schema.major, expected.schema.minor
            )));
        }
        let mut fold = crate::hub_pack::LineageFold::create(&self.packs_dir, binding.clone())?;
        let content_path = |pack: &TransportPack| {
            self.packs_dir
                .join("sha256")
                .join(format!("{}.sqlite.zst", pack.sha256))
        };
        for pack in &expected.base.packs {
            if stop() {
                return Err(StoreError::LineageRebaseStopped);
            }
            self.verify_lineage_pack_metadata(pack).map_err(|_| {
                StoreError::LineageRebaseDamaged(format!("pack {} is not readable", pack.sha256))
            })?;
            fold.apply_base_pack(pack, &content_path(pack))?;
        }
        for delta in &expected.deltas {
            if stop() {
                return Err(StoreError::LineageRebaseStopped);
            }
            self.verify_lineage_pack_metadata(&delta.pack).map_err(|_| {
                StoreError::LineageRebaseDamaged(format!(
                    "pack {} is not readable",
                    delta.pack.sha256
                ))
            })?;
            fold.apply_delta_pack(&delta.pack, &content_path(&delta.pack))?;
        }
        let rows = fold.finish()?;
        let sequence = self.next_full_snapshot_sequence_preview(vehicle_id)?;
        let range = SequenceRange {
            from_exclusive: sequence,
            to_inclusive: sequence,
        };
        let snapshot_id = Uuid::new_v4();
        let writer = ProjectionPackWriter::with_limits(self.packs_dir.clone(), limits.protocol);
        let pack_limit = limits.base_pack_limit();
        let mut packs: Vec<crate::hub_pack::BuiltProjectionPack> = Vec::new();
        // Cuts the fold at `target` rows a pack and writes the first
        // `write_limit` fragments; the rest are counted, so a refusal names
        // the exact number of packs. Returns the fragments the cut needs.
        // `oversized` is the refusal a capacity failure of a raised target
        // becomes.
        let cut = |target: u64,
                   write_limit: usize,
                   oversized: Option<usize>,
                   packs: &mut Vec<crate::hub_pack::BuiltProjectionPack>|
         -> Result<usize, StoreError> {
            let mut needed = 0_usize;
            fold.for_each_fragment(target, |fragment| {
                if stop() {
                    return Err(StoreError::LineageRebaseStopped);
                }
                needed += 1;
                if needed > write_limit {
                    return Ok(());
                }
                let request = ProjectionPackRequest {
                    pack_id: Uuid::new_v4(),
                    snapshot_id,
                    ordinal: u32::try_from(needed - 1).map_err(|_| StoreError::SequenceTooLarge)?,
                    binding: binding.clone(),
                    sequence: range,
                    snapshot: &fragment.snapshot,
                };
                let built = writer
                    .write_full_snapshot_with_states_and_updates(
                        &request,
                        &fragment.states,
                        &fragment.updates,
                    )
                    .map_err(|error| {
                        if let Some((digest, path)) = error.installed_object()
                            && let Err(cleanup) =
                                self.remove_unretained_pack(publication_gate, digest, path)
                        {
                            tracing::warn!(%vehicle_id, error = %cleanup, "failed rebase pack left for repair");
                        }
                        match oversized {
                            Some(needed) if lineage_rebase_pack_capacity_error(&error) => {
                                StoreError::LineageRebaseOversized {
                                    needed,
                                    limit: pack_limit,
                                }
                            }
                            _ => lineage_rebase_pack_error(error),
                        }
                    })?;
                packs.push(built);
                Ok(())
            })?;
            Ok(needed)
        };
        // A history that needs more packs than a base may hold at the
        // requested target is cut again at doubled targets, up to the
        // receiver's row ceiling, as the importer's fragmenter does; the
        // raised cuts only count until one fits (REV-CX-ISS106 finding 4).
        let ceiling = limits.protocol.max_rows_per_pack.max(limits.rows_per_pack);
        let mut needed = 0_usize;
        let written = (|| -> Result<(), StoreError> {
            let mut target = limits.rows_per_pack;
            needed = cut(target, pack_limit, None, &mut packs)?;
            if needed <= pack_limit || target >= ceiling {
                return Ok(());
            }
            let requested = needed;
            self.remove_rebase_candidate_packs(publication_gate, &packs)?;
            packs.clear();
            while needed > pack_limit && target < ceiling {
                target = target.saturating_mul(2).min(ceiling);
                needed = cut(target, 0, None, &mut packs)?;
            }
            if needed <= pack_limit {
                needed = cut(target, pack_limit, Some(requested), &mut packs)?;
            }
            Ok(())
        })();
        let verified = written
            .and_then(|()| {
                if needed > pack_limit {
                    return Err(StoreError::LineageRebaseOversized {
                        needed,
                        limit: pack_limit,
                    });
                }
                let located = packs
                    .iter()
                    .map(|pack| (pack.metadata.clone(), pack.path.clone()))
                    .collect::<Vec<_>>();
                fold.verify_readback(&located, stop)?;
                let total_rows = packs.iter().try_fold(0_u64, |total, pack| {
                    total
                        .checked_add(pack.metadata.row_count)
                        .ok_or(StoreError::SequenceTooLarge)
                })?;
                crate::hub_pack::signed_full_snapshot_manifest_with_limits(
                    &binding,
                    snapshot_id,
                    range,
                    &packs,
                    total_rows,
                    cursor_key,
                    limits.protocol,
                )
                .map_err(|error| match error {
                    ProjectionPackError::TooManyChunks
                    | ProjectionPackError::Protocol(
                        crate::protocol::ProtocolError::LineageAggregateLimitExceeded,
                    ) => StoreError::LineageRebaseOversized {
                        needed,
                        limit: pack_limit,
                    },
                    other => lineage_rebase_pack_error(other),
                })
            });
        match verified {
            Ok(manifest) => Ok(V2LineageRebaseCandidate {
                vehicle_id,
                expected,
                binding,
                manifest,
                packs,
                rows,
            }),
            Err(error) => {
                if let Err(cleanup) = self.remove_rebase_candidate_packs(publication_gate, &packs) {
                    tracing::warn!(%vehicle_id, error = %cleanup, "fresh base candidate packs left for startup repair");
                }
                Err(error)
            }
        }
    }

    /// Removes the packs a candidate created (never one it reused) while the
    /// caller holds the publication gate. Nothing served changes.
    pub(crate) fn discard_v2_lineage_rebase(
        &self,
        publication_gate: &PublicationGate,
        candidate: V2LineageRebaseCandidate,
    ) -> Result<(), StoreError> {
        self.remove_rebase_candidate_packs(publication_gate, &candidate.packs)
    }

    fn remove_rebase_candidate_packs(
        &self,
        publication_gate: &PublicationGate,
        packs: &[crate::hub_pack::BuiltProjectionPack],
    ) -> Result<(), StoreError> {
        let mut first_error = None;
        for pack in packs.iter().filter(|pack| pack.may_remove_unpublished_file()) {
            if let Err(error) =
                self.remove_unretained_pack(publication_gate, pack.metadata.sha256, &pack.path)
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// The full-snapshot marker the next reservation would hand out, read
    /// without reserving it. Under the publication gate no other reservation
    /// can take it; the swap reserves it in its own transaction.
    pub(crate) fn next_full_snapshot_sequence_preview(
        &self,
        vehicle_id: Uuid,
    ) -> Result<u64, StoreError> {
        next_full_snapshot_sequence_in(&self.open_read_only_connection()?, vehicle_id)
    }
}

/// The full-snapshot marker the next reservation would hand out in
/// `connection`'s catalogue, as `next_full_snapshot_sequence_while_gated`
/// computes it.
fn next_full_snapshot_sequence_in(connection: &Connection, vehicle_id: Uuid) -> Result<u64, StoreError> {
    let next_counter: Option<i64> = connection
        .query_row(
            "SELECT next_sequence FROM vehicle_snapshot_sequences WHERE vehicle_id = ?1",
            params![vehicle_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    let catalog_head: Option<i64> = connection
        .query_row(
            "SELECT MAX(head_sequence) FROM (
                SELECT head_sequence FROM sync_manifests WHERE vehicle_id = ?1
                UNION ALL
                SELECT head_sequence FROM sync_heads WHERE vehicle_id = ?1
             )",
            params![vehicle_id.to_string()],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)?;
    catalog_head
        .unwrap_or(0)
        .max(next_counter.unwrap_or(1).saturating_sub(1))
        .checked_add(1)
        .and_then(|sequence| u64::try_from(sequence).ok())
        .filter(|sequence| *sequence >= 1)
        .ok_or(StoreError::SequenceExhausted)
}

// The swap and the attempt (issue #106, task c). Under the publication gate
// the hand-back publication thread takes each due request, builds the fresh
// base (task b) and swaps it in one IMMEDIATE transaction: compare-and-swap
// on the folded lineage, the old lineage into an expiring checkpoint, the new
// base catalogued with the same binding, the import's projection state kept
// on the new base, the outbox made due and the request removed. The queued
// changes then publish as delta 1 on the new base through the collector's
// usual path; the swap consumes none of them.

/// What one attempt at a due rebase request did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum V2LineageRebaseOutcome {
    /// The fresh base serves at this head sequence; the old lineage is an
    /// expiring checkpoint.
    Rebased { head_sequence: u64 },
    /// An import publication is pending or an import generation is staging
    /// for the vehicle; the request was deferred.
    Withheld,
    /// The served head cannot be rebased (the reason code); it is not tried
    /// again until the head changes.
    Refused(&'static str),
    /// A transient failure (I/O, free space, stop, a head that moved); the
    /// request was deferred.
    Deferred(String),
    /// Another request replaced the one being settled; it is left unchanged.
    Superseded,
    /// The vehicle needs no fresh base any more (no served head, or one below
    /// the compaction trigger); the request was removed.
    Dropped,
}

/// The reason code a refusal records.
fn v2_lineage_rebase_refusal_code(error: &StoreError) -> &'static str {
    match error {
        StoreError::LineageRebaseOversized { .. } => "lineage_rebase_oversized",
        StoreError::LineageRebaseInexact(..) => "lineage_rebase_inexact",
        _ => "lineage_rebase_damaged",
    }
}

/// Whether an import holds the vehicle's lineage: a pending import
/// publication (issue #60) or an import generation still staging. Either may
/// publish on the served base, so the rebase waits for it.
fn v2_lineage_rebase_withheld_in(connection: &Connection, vehicle_id: Uuid) -> Result<bool, StoreError> {
    let holds = import_publication_holds_in(connection)?;
    if holds.all || holds.pending(vehicle_id) {
        return Ok(true);
    }
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM import_generations
                            WHERE vehicle_id = ?1 AND status = 'staging')",
            params![vehicle_id.to_string()],
            |row| row.get(0),
        )
        .map_err(StoreError::ImportGeneration)
}

/// The import projection state tables a swap keeps, as (table, temporary
/// copy). Every one cascades from `sync_bases`; heads before their rows.
const V2_LINEAGE_REBASE_IMPORT_STATE: [(&str, &str); 5] = [
    (
        crate::db::import_table_names::import_projection_heads_table!(),
        "v2_rebase_import_heads",
    ),
    (
        crate::db::import_table_names::import_projection_state_heads_table!(),
        "v2_rebase_import_state_heads",
    ),
    (
        crate::db::import_table_names::import_projection_state_bridges_table!(),
        "v2_rebase_import_state_bridges",
    ),
    (
        crate::db::import_table_names::import_projection_rows_table!(),
        "v2_rebase_import_rows",
    ),
    (
        crate::db::import_table_names::import_projection_state_rows_table!(),
        "v2_rebase_import_state_rows",
    ),
];

/// Copies the vehicle's import projection state into connection-private
/// TEMP tables before the retirement's cascade removes it.
fn copy_rebased_import_state_in_transaction(
    transaction: &Transaction<'_>,
    vehicle_key: &str,
) -> Result<(), StoreError> {
    for (table, copy) in V2_LINEAGE_REBASE_IMPORT_STATE {
        transaction
            .execute_batch(&format!(
                "DROP TABLE IF EXISTS temp.{copy};
                 CREATE TEMP TABLE {copy} AS SELECT * FROM main.\"{table}\" WHERE 0;"
            ))
            .map_err(StoreError::LineageCatalog)?;
        transaction
            .execute(
                &format!("INSERT INTO temp.{copy} SELECT * FROM main.\"{table}\" WHERE vehicle_id = ?1"),
                params![vehicle_key],
            )
            .map_err(StoreError::LineageCatalog)?;
    }
    Ok(())
}

/// Restores the copied import projection state on the new base: the heads
/// and the bridge name the new snapshot and head sequence, the per-row state
/// is unchanged. A later import of the same source then diffs against the
/// same rows and finds what it already published.
fn restore_rebased_import_state_in_transaction(
    transaction: &Transaction<'_>,
    snapshot_id: Uuid,
    head_sequence: i64,
) -> Result<(), StoreError> {
    let snapshot = snapshot_id.to_string();
    let [heads, state_heads, bridges, rows, state_rows] = V2_LINEAGE_REBASE_IMPORT_STATE;
    for (table, copy) in [heads, state_heads] {
        transaction
            .execute(
                &format!(
                    "INSERT INTO main.\"{table}\"(
                        vehicle_id, source_id, base_snapshot_id, selected_car_id, head_sequence)
                     SELECT vehicle_id, source_id, ?1, selected_car_id, ?2 FROM temp.{copy}"
                ),
                params![snapshot, head_sequence],
            )
            .map_err(StoreError::LineageCatalog)?;
    }
    let (table, copy) = bridges;
    transaction
        .execute(
            &format!(
                "INSERT INTO main.\"{table}\"(
                    vehicle_id, base_snapshot_id, head_sequence, algorithm,
                    legacy_fingerprint_sha256, logical_fingerprint_sha256)
                 SELECT vehicle_id, ?1, ?2, algorithm,
                        legacy_fingerprint_sha256, logical_fingerprint_sha256
                   FROM temp.{copy}"
            ),
            params![snapshot, head_sequence],
        )
        .map_err(StoreError::LineageCatalog)?;
    for (table, copy) in [rows, state_rows] {
        transaction
            .execute_batch(&format!("INSERT INTO main.\"{table}\" SELECT * FROM temp.{copy};"))
            .map_err(StoreError::LineageCatalog)?;
    }
    for (_, copy) in V2_LINEAGE_REBASE_IMPORT_STATE {
        transaction
            .execute_batch(&format!("DROP TABLE temp.{copy};"))
            .map_err(StoreError::LineageCatalog)?;
    }
    Ok(())
}

/// The swap, as one step of `transaction`; commits nothing. Returns the new
/// head sequence. Every refusal leaves the transaction to roll back.
fn swap_v2_lineage_rebase_in_transaction(
    transaction: &Transaction<'_>,
    candidate: &V2LineageRebaseCandidate,
) -> Result<u64, StoreError> {
    let vehicle_id = candidate.vehicle_id;
    let vehicle_key = vehicle_id.to_string();
    let manifest = &candidate.manifest;
    let expected = &candidate.expected;
    if manifest.vehicle_id != vehicle_id
        || expected.vehicle_id != vehicle_id
        || candidate.binding.vehicle_id != vehicle_id
        || manifest.snapshot_id == expected.base.snapshot_id
        || manifest.head_sequence <= expected.head_sequence
    {
        return Err(StoreError::LineageCatalogConflict);
    }
    if v2_lineage_rebase_withheld_in(transaction, vehicle_id)? {
        return Err(StoreError::LineageCatalogConflict);
    }
    // The marker the build signed must still be the next one; reserve it.
    let sequence = manifest.head_sequence;
    if next_full_snapshot_sequence_in(transaction, vehicle_id)? != sequence {
        return Err(StoreError::LineageCatalogConflict);
    }
    let head_sequence = i64::try_from(sequence).map_err(|_| StoreError::SequenceTooLarge)?;
    transaction
        .execute(
            "INSERT INTO vehicle_snapshot_sequences (vehicle_id, next_sequence)
             VALUES (?1, ?2)
             ON CONFLICT(vehicle_id) DO UPDATE SET
                next_sequence = MAX(vehicle_snapshot_sequences.next_sequence, excluded.next_sequence)",
            params![
                vehicle_key.as_str(),
                head_sequence.checked_add(1).ok_or(StoreError::SequenceExhausted)?
            ],
        )
        .map_err(StoreError::Query)?;
    copy_rebased_import_state_in_transaction(transaction, &vehicle_key)?;
    // The spans of folded collector deltas go with them; their mark stays.
    let live_history = v2_live_delta_history_in(transaction, &vehicle_key)?;
    // Compare-and-swap on the folded lineage, then its checkpoint.
    retire_rebased_lineage_in_transaction(transaction, expected, &candidate.binding)?;
    if live_history {
        put_metadata_in(
            transaction,
            &format!("{V2_LINEAGE_REBASE_LIVE_PREFIX}{vehicle_key}"),
            "1",
        )?;
    }
    publish_manifest_in_transaction(transaction, manifest, Some(&candidate.binding))?;
    restore_rebased_import_state_in_transaction(transaction, manifest.snapshot_id, head_sequence)?;
    // The source fingerprint an import recorded stays current on the base
    // that holds its rows.
    transaction
        .execute(
            "UPDATE snapshot_fingerprints SET snapshot_id = ?2, head_sequence = ?3
              WHERE vehicle_id = ?1 AND snapshot_id = ?4",
            params![
                vehicle_key.as_str(),
                manifest.snapshot_id.to_string(),
                head_sequence,
                expected.base.snapshot_id.to_string(),
            ],
        )
        .map_err(StoreError::PublishManifest)?;
    // The queued changes are due at once, as delta 1 on the new base.
    transaction
        .execute(
            "UPDATE export_outbox SET next_attempt_ms = 0, attempts = 0, last_error = NULL
              WHERE vehicle_id = ?1",
            params![vehicle_key.as_str()],
        )
        .map_err(StoreError::Query)?;
    transaction
        .execute(
            "DELETE FROM hub_metadata WHERE key = ?1",
            params![v2_lineage_rebase_key(vehicle_id)],
        )
        .map_err(StoreError::Query)?;
    Ok(sequence)
}

impl HubStore {
    /// Serves `candidate` in place of the lineage it was folded from, in one
    /// IMMEDIATE transaction, and returns the new head sequence. It refuses
    /// with `LineageCatalogConflict`, changing nothing, when the served
    /// lineage, its binding or the next full-snapshot marker is no longer
    /// what the build saw, or when an import holds the vehicle. The caller
    /// holds the publication gate and discards the candidate on a refusal.
    pub(crate) fn commit_v2_lineage_rebase(
        &self,
        _publication_gate: &PublicationGate,
        candidate: &V2LineageRebaseCandidate,
    ) -> Result<u64, StoreError> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let sequence = swap_v2_lineage_rebase_in_transaction(&transaction, candidate)?;
        transaction.commit().map_err(StoreError::LineageCatalog)?;
        Ok(sequence)
    }

    /// The swap run to its last statement and rolled back, as a crash before
    /// its commit leaves the catalogue.
    #[cfg(test)]
    pub(crate) fn rolled_back_v2_lineage_rebase(
        &self,
        _publication_gate: &PublicationGate,
        candidate: &V2LineageRebaseCandidate,
    ) -> Result<u64, StoreError> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let sequence = swap_v2_lineage_rebase_in_transaction(&transaction, candidate)?;
        transaction.rollback().map_err(StoreError::LineageCatalog)?;
        Ok(sequence)
    }

    /// Whether an import base publication on the vehicle is refused by the
    /// unplanned-seam guard (design M03-SEAM-R3).
    #[cfg(test)]
    pub(crate) fn unplanned_import_seam_refused(&self, vehicle_id: Uuid) -> Result<bool, StoreError> {
        let connection = self.open()?;
        match refuse_unplanned_import_seam_in_transaction(&connection, &vehicle_id.to_string()) {
            Ok(()) => Ok(false),
            Err(StoreError::ImportGenerationConflict) => Ok(true),
            Err(other) => Err(other),
        }
    }

    /// Attempts every rebase request that is due at `now_ms`, one vehicle
    /// after another, under the caller's publication gate. A refusal of the
    /// head (oversized, inexact, damaged) marks the request refused; any
    /// transient failure defers it by the doubled delay. `stop` is asked
    /// between vehicles and inside each build.
    pub(crate) fn rebase_due_v2_lineages(
        &self,
        publication_gate: &PublicationGate,
        cursor_key: &CursorKey,
        now_ms: i64,
        limits: V2LineageRebaseLimits,
        stop: &dyn Fn() -> bool,
    ) -> Result<Vec<(Uuid, V2LineageRebaseOutcome)>, StoreError> {
        validate_timestamp("rebase request now_ms", now_ms)?;
        let due = v2_lineage_rebase_requests_in(&self.open_read_only_connection()?)?
            .into_iter()
            .filter(|(_, request)| request.is_due(now_ms))
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(due.len());
        for (vehicle_id, request) in due {
            if stop() {
                break;
            }
            let outcome = self.rebase_v2_lineage(
                publication_gate,
                vehicle_id,
                &request,
                cursor_key,
                now_ms,
                limits,
                stop,
            )?;
            outcomes.push((vehicle_id, outcome));
        }
        Ok(outcomes)
    }

    #[allow(clippy::too_many_arguments)]
    fn rebase_v2_lineage(
        &self,
        publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        request: &V2LineageRebaseRequest,
        cursor_key: &CursorKey,
        now_ms: i64,
        limits: V2LineageRebaseLimits,
        stop: &dyn Fn() -> bool,
    ) -> Result<V2LineageRebaseOutcome, StoreError> {
        if v2_lineage_rebase_withheld_in(&self.open_read_only_connection()?, vehicle_id)? {
            self.defer_v2_lineage_rebase(vehicle_id, now_ms)?;
            return Ok(V2LineageRebaseOutcome::Withheld);
        }
        let Some((_, _, head)) = self.v2_head(vehicle_id)? else {
            self.drop_v2_lineage_rebase(vehicle_id, &request.head_digest)?;
            return Ok(V2LineageRebaseOutcome::Dropped);
        };
        let packs = match self.v2_lineage_pack_count(vehicle_id).map_err(lineage_rebase_missing_pack) {
            Ok(packs) => packs,
            Err(error) => {
                if error.refuses_lineage_rebase() && request.head_digest != head.to_string() {
                    self.retarget_v2_lineage_rebase(vehicle_id, request, head)?;
                }
                return self.settle_failed_v2_lineage_rebase(vehicle_id, head, now_ms, error);
            }
        };
        let trigger = limits
            .protocol
            .max_chunks
            .saturating_sub(limits.protocol.max_chunks.min(8));
        if packs < trigger {
            self.drop_v2_lineage_rebase(vehicle_id, &request.head_digest)?;
            return Ok(V2LineageRebaseOutcome::Dropped);
        }
        if request.head_digest != head.to_string() {
            // The head moved since the request (a lost swap, or more
            // publications into the slots left); the lineage still needs a
            // fresh base, so the request now names the served head.
            self.retarget_v2_lineage_rebase(vehicle_id, request, head)?;
        }
        let candidate =
            match self.build_v2_lineage_rebase(publication_gate, vehicle_id, cursor_key, limits, stop) {
                Ok(candidate) => candidate,
                Err(error) => return self.settle_failed_v2_lineage_rebase(vehicle_id, head, now_ms, error),
            };
        // A stop during the readback's last pack, or after it, still wins
        // over the swap (REV-CX-ISS106 finding 2).
        let committed = if stop() {
            Err(StoreError::LineageRebaseStopped)
        } else if candidate.expected.head_digest == head {
            self.commit_v2_lineage_rebase(publication_gate, &candidate)
        } else {
            Err(StoreError::LineageCatalogConflict)
        };
        match committed {
            Ok(head_sequence) => Ok(V2LineageRebaseOutcome::Rebased { head_sequence }),
            Err(error) => {
                if let Err(cleanup) = self.discard_v2_lineage_rebase(publication_gate, candidate) {
                    tracing::warn!(%vehicle_id, error = %cleanup, "fresh base candidate packs left for startup repair");
                }
                self.settle_failed_v2_lineage_rebase(vehicle_id, head, now_ms, error)
            }
        }
    }

    /// Records a failed attempt: refused for the head when the failure
    /// refuses it, otherwise deferred.
    fn settle_failed_v2_lineage_rebase(
        &self,
        vehicle_id: Uuid,
        head: Sha256Digest,
        now_ms: i64,
        error: StoreError,
    ) -> Result<V2LineageRebaseOutcome, StoreError> {
        if error.refuses_lineage_rebase() {
            let code = v2_lineage_rebase_refusal_code(&error);
            tracing::warn!(%vehicle_id, %error, "the compatibility lineage cannot take a fresh base");
            if self.refuse_v2_lineage_rebase(vehicle_id, head, code)? {
                return Ok(V2LineageRebaseOutcome::Refused(code));
            }
            return Ok(V2LineageRebaseOutcome::Superseded);
        }
        tracing::info!(%vehicle_id, %error, "the fresh compatibility base waits for another attempt");
        self.defer_v2_lineage_rebase(vehicle_id, now_ms)?;
        Ok(V2LineageRebaseOutcome::Deferred(error.to_string()))
    }

    /// Defers the vehicle's request, when it is still a request, by its next
    /// doubled delay.
    fn defer_v2_lineage_rebase(&self, vehicle_id: Uuid, now_ms: i64) -> Result<(), StoreError> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        if let Some(mut request) = v2_lineage_rebase_request_in(&transaction, vehicle_id)?
            && request.state == V2LineageRebaseState::Requested
        {
            request.defer(now_ms);
            put_v2_lineage_rebase_request_in(&transaction, vehicle_id, &request)?;
        }
        transaction.commit().map_err(StoreError::Query)
    }

    /// Makes the vehicle's request, still naming `request`'s head, name the
    /// served head instead; its state, reason and schedule are kept.
    fn retarget_v2_lineage_rebase(
        &self,
        vehicle_id: Uuid,
        request: &V2LineageRebaseRequest,
        head: Sha256Digest,
    ) -> Result<(), StoreError> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        if let Some(mut stored) = v2_lineage_rebase_request_in(&transaction, vehicle_id)?
            && stored.head_digest == request.head_digest
        {
            stored.head_digest = head.to_string();
            put_v2_lineage_rebase_request_in(&transaction, vehicle_id, &stored)?;
        }
        transaction.commit().map_err(StoreError::Query)
    }

    /// Removes the vehicle's request when it still names `head_digest`.
    fn drop_v2_lineage_rebase(&self, vehicle_id: Uuid, head_digest: &str) -> Result<(), StoreError> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        if v2_lineage_rebase_request_in(&transaction, vehicle_id)?
            .is_some_and(|stored| stored.head_digest == head_digest)
        {
            transaction
                .execute(
                    "DELETE FROM hub_metadata WHERE key = ?1",
                    params![v2_lineage_rebase_key(vehicle_id)],
                )
                .map_err(StoreError::Query)?;
        }
        transaction.commit().map_err(StoreError::Query)
    }
}

#[cfg(test)]
mod lineage_rebase_fold_tests {
    use super::*;

    include!("tests/lineage_rebase_fold.rs");

    #[test]
    fn rebase_failed_install_removes_new_object() {
        let temporary = crate::private_tempdir().expect("temporary store");
        let store = HubStore::initialize(temporary.path()).expect("store");
        let (_, lineage) = fold_test_imported_base(&store, false, |car_id| {
            fold_test_base_fragments(car_id, false)
        });
        let files = fold_test_files(&store);

        let gate = store.try_acquire_publication_gate().expect("publication gate");
        let fault = crate::durability_fault::inject(
            crate::durability_fault::DurabilityFaultPoint::PackFinalDirectoryFsync,
        );
        let error = store
            .build_v2_lineage_rebase(
                &gate,
                lineage.vehicle_id,
                &fold_test_key(),
                V2LineageRebaseLimits::default(),
                &|| false,
            )
            .expect_err("directory sync failure");
        drop(fault);

        assert!(matches!(
            error,
            StoreError::LineageRebasePack(ProjectionPackError::InstalledThenFailed { .. })
        ));
        assert_eq!(fold_test_files(&store), files);
        assert_eq!(
            store.lineage_manifest_for_vehicle(lineage.vehicle_id).unwrap().as_ref(),
            Some(&lineage)
        );
    }

    #[test]
    fn stale_rebase_request_is_settled_for_current_damaged_head() {
        let temporary = crate::private_tempdir().expect("temporary store");
        let store = HubStore::initialize(temporary.path()).expect("store");
        let (_, lineage) = fold_test_imported_base(&store, false, |car_id| {
            fold_test_base_fragments(car_id, false)
        });
        let now_ms = 2_000;
        let stale = V2LineageRebaseRequest {
            format: V2_LINEAGE_REBASE_FORMAT,
            head_digest: Sha256Digest::of_bytes(b"older head").to_string(),
            state: V2LineageRebaseState::Requested,
            reason: "fixture".to_owned(),
            next_attempt_ms: now_ms,
            attempts: 0,
        };
        let mut connection = store.open().expect("catalogue");
        let transaction = connection.transaction().expect("transaction");
        put_v2_lineage_rebase_request_in(&transaction, lineage.vehicle_id, &stale)
            .expect("stale request");
        transaction.commit().expect("request commit");
        fs::remove_file(
            store
                .packs_dir()
                .join("sha256")
                .join(format!("{}.sqlite.zst", lineage.base.packs[0].sha256)),
        )
        .expect("missing served pack");

        let gate = store.try_acquire_publication_gate().expect("publication gate");
        store
            .rebase_due_v2_lineages(
                &gate,
                &fold_test_key(),
                now_ms,
                V2LineageRebaseLimits::default(),
                &|| false,
            )
            .expect("rebase step");
        let settled = store
            .v2_lineage_rebase_request(lineage.vehicle_id)
            .expect("request read")
            .expect("request retained");
        assert_eq!(settled.head_digest, lineage.head_digest.to_string());
        assert_eq!(settled.state, V2LineageRebaseState::Refused);
        assert!(!settled.is_due(now_ms));

        let replacement = store
            .request_v2_lineage_rebase(
                lineage.vehicle_id,
                Sha256Digest::of_bytes(b"replacement head"),
                "replacement",
                now_ms,
            )
            .expect("replacement request");
        let outcome = store
            .settle_failed_v2_lineage_rebase(
                lineage.vehicle_id,
                lineage.head_digest,
                now_ms,
                StoreError::LineageRebaseDamaged("fixture".to_owned()),
            )
            .expect("superseded settlement");
        assert_eq!(outcome, V2LineageRebaseOutcome::Superseded);
        assert_eq!(
            store
                .v2_lineage_rebase_request(lineage.vehicle_id)
                .expect("request read"),
            Some(replacement)
        );
    }
}
