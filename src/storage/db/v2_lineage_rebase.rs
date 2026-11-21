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
}

fn v2_lineage_rebase_request_in(
    connection: &Connection,
    vehicle_id: Uuid,
) -> Result<Option<V2LineageRebaseRequest>, StoreError> {
    Ok(metadata_value_in(connection, &v2_lineage_rebase_key(vehicle_id))?
        .and_then(|value| serde_json::from_str::<V2LineageRebaseRequest>(&value).ok())
        .filter(|request| request.format == V2_LINEAGE_REBASE_FORMAT))
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
    #[cfg_attr(not(test), allow(dead_code))]
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn v2_lineage_rebase_request(
        &self,
        vehicle_id: Uuid,
    ) -> Result<Option<V2LineageRebaseRequest>, StoreError> {
        let connection = self.open_read_only_connection()?;
        v2_lineage_rebase_request_in(&connection, vehicle_id)
    }

    /// Pushes the next attempt of every request that is due out by its next
    /// doubled delay and returns how many it moved. The step that attempts a
    /// rebase calls this for a transient failure; until that step exists the
    /// supervisor's publication thread calls it, so a due request is asked
    /// for again at a growing interval, not at every supervisor step.
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
            let delay = V2_LINEAGE_REBASE_FIRST_DELAY_MS
                .checked_shl(request.attempts.min(16))
                .unwrap_or(V2_LINEAGE_REBASE_LONGEST_DELAY_MS)
                .min(V2_LINEAGE_REBASE_LONGEST_DELAY_MS);
            request.attempts = request.attempts.saturating_add(1);
            request.next_attempt_ms = now_ms.saturating_add(delay);
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
/// absent, not readable or without the table has none.
pub fn v2_lineage_rebase_due(data_dir: &Path, now_ms: i64) -> bool {
    let database = data_dir.join("hub.sqlite");
    if !database.is_file() {
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
        }
    }
}

impl StoreError {
    /// Whether this failure refuses the head for good (oversized, inexact or
    /// damaged), so the request is marked refused; every other failure of a
    /// rebase attempt is transient and the request is deferred.
    #[cfg_attr(not(test), allow(dead_code))]
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
    #[cfg_attr(not(test), allow(dead_code))]
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
            .map_err(|error| match error {
                StoreError::LineagePackNotReady => {
                    StoreError::LineageRebaseDamaged("a served pack is missing".to_owned())
                }
                other => other,
            })?
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
        let mut needed = 0_usize;
        let written = fold.for_each_fragment(limits.rows_per_pack, |fragment| {
            if stop() {
                return Err(StoreError::LineageRebaseStopped);
            }
            needed += 1;
            if needed > pack_limit {
                // Count the rest without writing, so the refusal names the
                // exact number of packs this history needs.
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
                .map_err(lineage_rebase_pack_error)?;
            packs.push(built);
            Ok(())
        });
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
                fold.verify_readback(&located)?;
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
    #[cfg_attr(not(test), allow(dead_code))]
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
    fn next_full_snapshot_sequence_preview(&self, vehicle_id: Uuid) -> Result<u64, StoreError> {
        let connection = self.open_read_only_connection()?;
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
}

#[cfg(test)]
mod lineage_rebase_fold_tests {
    use super::*;

    include!("tests/lineage_rebase_fold.rs");
}
