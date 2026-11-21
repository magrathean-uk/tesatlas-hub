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
