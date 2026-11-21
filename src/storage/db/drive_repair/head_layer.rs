// SPDX-License-Identifier: AGPL-3.0-only

// Planned physical heads bound to the active repair layer (M03-T27b).
//
// The layer digest `L` covers the repair policy version and the sorted keys
// of the vehicle's active components. It is kept in `hub_metadata` under
// `drive_repair_layer:<vehicle>`, rewritten in every transaction that moves a
// component into or out of `active`, and absent while none is. Each planned
// head records the `L` it was planned with under
// `physical_v3_repair_layer:<vehicle>` (snapshot -> layer and component
// keys); a head is served only while that `L` is the vehicle's. A head
// without a record was planned with no layer, so a store without repairs
// serves exactly as before and writes neither key.

/// The `hub_metadata` key of a vehicle's active repair layer digest.
fn drive_repair_layer_key(vehicle: &str) -> String {
    format!("drive_repair_layer:{vehicle}")
}

/// The `hub_metadata` key of the layer each planned head was planned with.
fn physical_v3_repair_layer_key(vehicle_id: Uuid) -> String {
    format!("physical_v3_repair_layer:{vehicle_id}")
}

/// The repair layer one physical head was planned with.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhysicalV3RepairLayer {
    /// The layer digest `L`, lower-case hex.
    pub(crate) layer: String,
    /// The active component keys it covers, lower-case hex, sorted.
    pub(crate) components: Vec<String>,
}

/// One active component as a head producer applies it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActiveDriveRepair {
    pub(crate) component_key: [u8; 32],
    pub(crate) destination_drive_id: i64,
    /// The member drives other than the destination: its fragments.
    pub(crate) fragment_drive_ids: Vec<i64>,
    /// Every member position: fragment and destination samples.
    pub(crate) position_ids: Vec<i64>,
    /// The pre-rows its first application recorded, once it has run.
    pub(crate) before: Option<crate::import::broken_drive_repair::layer::ComponentPreRows>,
}

/// A vehicle's active ledger: its layer and the components it covers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActiveDriveRepairLayer {
    pub(crate) layer: PhysicalV3RepairLayer,
    pub(crate) components: Vec<ActiveDriveRepair>,
}

/// The layer of the component `keys`; `None` for no key.
fn drive_repair_layer_digest(keys: &[[u8; 32]]) -> Option<PhysicalV3RepairLayer> {
    if keys.is_empty() {
        return None;
    }
    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();
    let mut hasher = Sha256::new();
    hasher.update(b"tesatlas-hub/drive-repair-layer/v1\0");
    hasher.update(REPAIR_POLICY_VERSION.to_be_bytes());
    hasher.update((keys.len() as u64).to_be_bytes());
    for key in &keys {
        hasher.update(key);
    }
    Some(PhysicalV3RepairLayer {
        layer: hex::encode(hasher.finalize()),
        components: keys.iter().map(hex::encode).collect(),
    })
}

/// The keys of the vehicle's active components, in key order.
fn active_drive_repair_keys_in(
    connection: &Connection,
    vehicle: &str,
) -> Result<Vec<[u8; 32]>, StoreError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT component_key FROM drive_repair_changes
              WHERE vehicle_id = ?1 AND state = 'active' ORDER BY component_key",
        )
        .map_err(StoreError::Query)?;
    let keys = statement
        .query_map(params![vehicle], |row| row.get::<_, Vec<u8>>(0))
        .map_err(StoreError::Query)?
        .map(|key| {
            let key = key.map_err(StoreError::Query)?;
            <[u8; 32]>::try_from(key.as_slice())
                .map_err(|_| StoreError::DriveRepairRecord("component key"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(keys)
}

/// Rewrites the vehicle's layer digest from its active components in the
/// caller's transaction, or removes it when none is active. Every write that
/// moves a component into or out of `active` calls this.
fn write_drive_repair_layer_in(
    connection: &Connection,
    vehicle: &str,
) -> Result<Option<String>, StoreError> {
    let layer = drive_repair_layer_digest(&active_drive_repair_keys_in(connection, vehicle)?)
        .map(|layer| layer.layer);
    match &layer {
        Some(digest) => connection.execute(
            "INSERT INTO hub_metadata(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![drive_repair_layer_key(vehicle), digest],
        ),
        None => connection.execute(
            "DELETE FROM hub_metadata WHERE key = ?1",
            params![drive_repair_layer_key(vehicle)],
        ),
    }
    .map_err(StoreError::Query)?;
    Ok(layer)
}

/// The vehicle's layer digest `L`, while a component is active.
fn drive_repair_layer_in(
    connection: &Connection,
    vehicle_id: Uuid,
) -> Result<Option<String>, StoreError> {
    connection
        .query_row(
            "SELECT value FROM hub_metadata WHERE key = ?1",
            params![drive_repair_layer_key(&vehicle_id.to_string())],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)
}

/// The layer each recorded head was planned with; none recorded is an empty
/// map.
fn physical_v3_repair_layers_in(
    connection: &Connection,
    vehicle_id: Uuid,
) -> Result<std::collections::BTreeMap<String, PhysicalV3RepairLayer>, StoreError> {
    let value: Option<String> = connection
        .query_row(
            "SELECT value FROM hub_metadata WHERE key = ?1",
            params![physical_v3_repair_layer_key(vehicle_id)],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    match value {
        Some(value) => {
            serde_json::from_str(&value).map_err(|_| StoreError::PhysicalV3AdmissionConflict)
        }
        None => Ok(std::collections::BTreeMap::new()),
    }
}

/// Whether `snapshot_id` was planned with the vehicle's current repair layer.
/// A head without a record was planned with none.
fn physical_v3_repair_layer_serves_in(
    connection: &Connection,
    vehicle_id: Uuid,
    snapshot_id: Uuid,
) -> Result<bool, StoreError> {
    let current = drive_repair_layer_in(connection, vehicle_id)?;
    let recorded = physical_v3_repair_layers_in(connection, vehicle_id)?
        .remove(&snapshot_id.to_string())
        .map(|recorded| recorded.layer);
    Ok(recorded == current)
}

/// Whether the store still holds the head `snapshot` as current or retained.
fn physical_v3_snapshot_held_in(
    connection: &Connection,
    vehicle_id: Uuid,
    snapshot: &str,
) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_physical_v3_admissions
                            WHERE vehicle_id = ?1 AND snapshot_id = ?2)
                 OR EXISTS(SELECT 1 FROM retained_physical_v3_admissions
                            WHERE vehicle_id = ?1 AND snapshot_id = ?2)",
            params![vehicle_id.to_string(), snapshot],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)
}

impl HubStore {
    /// The vehicle's active repair layer and its components, or `None` while
    /// no component is active. Read in one transaction; the stored `L` must
    /// be the digest of the active keys.
    pub(crate) fn drive_repair_active_layer(
        &self,
        vehicle_id: Uuid,
    ) -> Result<Option<ActiveDriveRepairLayer>, StoreError> {
        let connection = self.open_read_only_connection()?;
        let transaction = connection.unchecked_transaction().map_err(StoreError::Query)?;
        let vehicle = vehicle_id.to_string();
        let stored = drive_repair_layer_in(&transaction, vehicle_id)?;
        let keys = active_drive_repair_keys_in(&transaction, &vehicle)?;
        let layer = drive_repair_layer_digest(&keys);
        if stored.as_deref() != layer.as_ref().map(|layer| layer.layer.as_str()) {
            return Err(StoreError::DriveRepairRecord("repair layer digest"));
        }
        let Some(layer) = layer else {
            return Ok(None);
        };
        let mut components = Vec::with_capacity(keys.len());
        for key in keys {
            let (destination_drive_id, before): (i64, Option<String>) = transaction
                .query_row(
                    "SELECT destination_drive_id, before_json FROM drive_repair_changes
                      WHERE vehicle_id = ?1 AND component_key = ?2",
                    params![vehicle, key.as_slice()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(StoreError::Query)?;
            let mut statement = transaction
                .prepare_cached(
                    "SELECT entity, entity_id FROM drive_repair_change_members
                      WHERE vehicle_id = ?1 AND component_key = ?2 ORDER BY entity, entity_id",
                )
                .map_err(StoreError::Query)?;
            let members = statement
                .query_map(params![vehicle, key.as_slice()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(StoreError::Query)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::Query)?;
            let before = before
                .map(|json| {
                    serde_json::from_str(&json)
                        .map_err(|_| StoreError::DriveRepairRecord("repair pre-rows"))
                })
                .transpose()?;
            components.push(ActiveDriveRepair {
                component_key: key,
                destination_drive_id,
                fragment_drive_ids: members
                    .iter()
                    .filter(|(entity, id)| entity == "drive" && *id != destination_drive_id)
                    .map(|(_, id)| *id)
                    .collect(),
                position_ids: members
                    .iter()
                    .filter(|(entity, _)| entity == "position")
                    .map(|(_, id)| *id)
                    .collect(),
                before,
            });
        }
        Ok(Some(ActiveDriveRepairLayer { layer, components }))
    }

    /// The materialised pre-rows of a component's first application: its
    /// destination drive and its fragment samples, in `sample_ids` order, as
    /// the store holds them. `None` when the store lacks one of them.
    pub(crate) fn drive_repair_materialised_pre_rows(
        &self,
        vehicle_id: Uuid,
        destination_drive_id: i64,
        sample_ids: &[i64],
    ) -> Result<Option<(ProjectionDrive, Vec<ProjectionPosition>)>, StoreError> {
        let connection = self.open_read_only_connection()?;
        let vehicle = vehicle_id.to_string();
        let drive: Option<String> = connection
            .query_row(
                "SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1 AND drive_id = ?2",
                params![vehicle, destination_drive_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)?;
        let Some(drive) = drive else {
            return Ok(None);
        };
        let drive: ProjectionDrive =
            serde_json::from_str(&drive).map_err(StoreError::DeserializeLifecycleRow)?;
        let mut samples = Vec::with_capacity(sample_ids.len());
        for &id in sample_ids {
            let json: Option<String> = connection
                .query_row(
                    "SELECT position_json FROM materialised_positions
                      WHERE vehicle_id = ?1 AND position_id = ?2",
                    params![vehicle, id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(StoreError::Query)?;
            let Some(json) = json else {
                return Ok(None);
            };
            samples.push(serde_json::from_str(&json).map_err(StoreError::DeserializeLifecycleRow)?);
        }
        Ok(Some((drive, samples)))
    }

    /// Records an active component's pre-rows at its first application in
    /// `drive_repair_changes.before_json`; later applications compare by
    /// digest. Returns whether they were written: never over recorded ones.
    pub(crate) fn record_drive_repair_pre_rows(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        component_key: &[u8; 32],
        pre: &crate::import::broken_drive_repair::layer::ComponentPreRows,
    ) -> Result<bool, StoreError> {
        let json = serde_json::to_string(pre).map_err(StoreError::SerializeLifecycleRow)?;
        let connection = self.open()?;
        let changed = connection
            .execute(
                "UPDATE drive_repair_changes SET before_json = ?3
                  WHERE vehicle_id = ?1 AND component_key = ?2 AND state = 'active'
                    AND before_json IS NULL",
                params![vehicle_id.to_string(), component_key.as_slice(), json],
            )
            .map_err(StoreError::Query)?;
        Ok(changed == 1)
    }

    /// Records the repair layer `snapshot_id` was planned with, before it is
    /// published, so a crash leaves at most an unused entry. With `layer`
    /// none the head was planned with no layer and gets no entry. Entries of
    /// heads the store no longer holds are pruned as the planned list is,
    /// except the frozen base's, whose layer stays its recorded one. A store
    /// without any entry and a head without a layer write nothing.
    pub(crate) fn record_physical_v3_repair_layer(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        snapshot_id: Uuid,
        layer: Option<&PhysicalV3RepairLayer>,
    ) -> Result<(), StoreError> {
        // A store without repairs takes no write lock here.
        if layer.is_none()
            && physical_v3_repair_layers_in(&self.open_read_only_connection()?, vehicle_id)?
                .is_empty()
        {
            return Ok(());
        }
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let layers = physical_v3_repair_layers_in(&transaction, vehicle_id)?;
        if layers.is_empty() && layer.is_none() {
            return Ok(());
        }
        // A frozen record that does not read back keeps every entry.
        let (prune, frozen) = match frozen_physical_base_in(&transaction, vehicle_id) {
            Ok(base) => (true, base.map(|base| base.snapshot_id.to_string())),
            Err(_) => (false, None),
        };
        let new = snapshot_id.to_string();
        let mut kept = std::collections::BTreeMap::new();
        for (id, entry) in layers {
            if id == new {
                continue;
            }
            if !prune
                || frozen.as_deref() == Some(id.as_str())
                || physical_v3_snapshot_held_in(&transaction, vehicle_id, &id)?
            {
                kept.insert(id, entry);
            }
        }
        if let Some(layer) = layer {
            kept.insert(new, layer.clone());
        }
        let key = physical_v3_repair_layer_key(vehicle_id);
        if kept.is_empty() {
            transaction.execute("DELETE FROM hub_metadata WHERE key = ?1", params![key])
        } else {
            let value = serde_json::to_string(&kept).map_err(StoreError::SerializeLifecycleRow)?;
            transaction.execute(
                "INSERT INTO hub_metadata(key, value) VALUES(?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )
        }
        .map_err(StoreError::Query)?;
        transaction.commit().map_err(StoreError::LifecycleWrite)
    }

    /// The repair layer the vehicle's head `snapshot_id` was planned with,
    /// if it was planned with one: the frozen head's layer is its record.
    #[cfg_attr(not(test), allow(dead_code, reason = "the takeover lane task adds the caller"))]
    pub(crate) fn physical_v3_repair_layer(
        &self,
        vehicle_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Option<PhysicalV3RepairLayer>, StoreError> {
        let connection = self.open_read_only_connection()?;
        Ok(physical_v3_repair_layers_in(&connection, vehicle_id)?.remove(&snapshot_id.to_string()))
    }

    /// Stands in for M03-T27c's activation: records one active component
    /// with its members and rewrites the vehicle's layer.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_active_drive_repair_for_test(
        &self,
        vehicle_id: Uuid,
        component_key: &[u8; 32],
        destination_drive_id: i64,
        drive_ids: &[i64],
        position_ids: &[i64],
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        let vehicle = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Query)?;
        let zero = [0_u8; 32];
        transaction
            .execute(
                "INSERT INTO drive_repair_batches(vehicle_id, policy_version, state, totals_json,
                     capture_digest, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 'complete', '{}', ?3, ?4, ?4)",
                params![vehicle, REPAIR_POLICY_VERSION, zero.as_slice(), now_ms],
            )
            .map_err(StoreError::Query)?;
        let batch_id = transaction.last_insert_rowid();
        transaction
            .execute(
                "INSERT INTO drive_repair_changes(vehicle_id, component_key, batch_id,
                     policy_version, destination_drive_id, effective_start_ms, input_digest,
                     protection_digest, state, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, 'active', ?6)",
                params![
                    vehicle,
                    component_key.as_slice(),
                    batch_id,
                    REPAIR_POLICY_VERSION,
                    destination_drive_id,
                    now_ms,
                    zero.as_slice()
                ],
            )
            .map_err(StoreError::Query)?;
        let members = drive_ids
            .iter()
            .map(|&id| ("drive", id))
            .chain(position_ids.iter().map(|&id| ("position", id)));
        for (entity, id) in members {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO drive_repair_change_members(vehicle_id, entity,
                         entity_id, component_key) VALUES (?1, ?2, ?3, ?4)",
                    params![vehicle, entity, id, component_key.as_slice()],
                )
                .map_err(StoreError::Query)?;
        }
        let layer = write_drive_repair_layer_in(&transaction, &vehicle)?;
        transaction.commit().map_err(StoreError::Query)?;
        Ok(layer)
    }

    /// Moves one component to `state` and rewrites the vehicle's layer.
    #[cfg(test)]
    pub(crate) fn set_drive_repair_state_for_test(
        &self,
        vehicle_id: Uuid,
        component_key: &[u8; 32],
        state: &str,
    ) -> Result<Option<String>, StoreError> {
        let vehicle = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Query)?;
        transaction
            .execute(
                "UPDATE drive_repair_changes SET state = ?3
                  WHERE vehicle_id = ?1 AND component_key = ?2",
                params![vehicle, component_key.as_slice(), state],
            )
            .map_err(StoreError::Query)?;
        let layer = write_drive_repair_layer_in(&transaction, &vehicle)?;
        transaction.commit().map_err(StoreError::Query)?;
        Ok(layer)
    }
}
