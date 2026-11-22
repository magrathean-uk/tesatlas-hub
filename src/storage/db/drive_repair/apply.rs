// SPDX-License-Identifier: AGPL-3.0-only

// Activation, undo and compensation of repair components (M03-T27c).
//
// Activation is one transaction under the publication gate: the planned
// components are revalidated (planned again from the stored index, their
// protection digest unchanged, their materialised rows still the pre-rows
// the dry run read), then each one's state, recorded pre-rows, materialised
// after-image with one `sync_mutations` row per changed row, and the
// vehicle's layer `L` are written together, and the batch waits in
// `publishing` until its head is settled. Compensation (undo, supersession
// of an active component, a failed publication) restores the materialised
// pre-image from the recorded pre-rows in the transaction that moves the
// component out of `active`, row by row and only where the store still holds
// the component's after-image, so a replay never writes a row twice.

/// The changed-row bound of one activation batch: the sum of its
/// components' changed rows (two per fragment, and the destination). A
/// component that would cross it waits for the next batch.
pub(crate) const MAX_BATCH_CHANGED_ROWS: usize = 500;

/// What one activation transaction did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DriveRepairActivation {
    /// The batch the activated components joined, while one did.
    pub(crate) batch_id: Option<i64>,
    /// The vehicle's layer `L` after the transaction.
    pub(crate) layer: Option<String>,
    pub(crate) activated: Vec<[u8; 32]>,
    /// Refused by revalidation; they leave the plan.
    pub(crate) superseded: Vec<[u8; 32]>,
    /// Still planned: their materialised rows are not the dry run's.
    pub(crate) waiting: Vec<[u8; 32]>,
    /// Materialised rows written, one `sync_mutations` row each.
    pub(crate) changed_rows: u64,
    /// An import's publication is pending: nothing was written.
    pub(crate) import_pending: bool,
}

/// What one compensation restored.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DriveRepairCompensation {
    /// Components that left `active`.
    pub(crate) components: u64,
    /// Materialised rows restored, one `sync_mutations` row each.
    pub(crate) restored_rows: u64,
}

/// How a `publishing` batch was settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveRepairSettlement {
    /// The batch was not `publishing`: another call settled it.
    NotPublishing,
    /// Its head is served with its surviving components.
    Complete,
    /// The head is served but no component of the batch is active any more:
    /// an import superseded and compensated them.
    Superseded,
    /// Its head was not served: its active components were compensated and
    /// wait in the plan.
    Failed(DriveRepairCompensation),
}

/// The components an undo names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code, reason = "M03-T28a adds the operator caller"))]
pub(crate) enum DriveRepairTarget<'a> {
    Component(&'a [u8; 32]),
    Batch(i64),
}

fn component_key_of(bytes: Vec<u8>) -> Result<[u8; 32], StoreError> {
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| StoreError::DriveRepairRecord("component key"))
}

/// The member drives and positions of one component, in id order.
fn component_members_in(
    connection: &Connection,
    vehicle: &str,
    key: &[u8; 32],
) -> Result<(Vec<i64>, Vec<i64>), StoreError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT entity, entity_id FROM drive_repair_change_members
              WHERE vehicle_id = ?1 AND component_key = ?2 ORDER BY entity, entity_id",
        )
        .map_err(StoreError::Query)?;
    let rows = statement
        .query_map(params![vehicle, key.as_slice()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(StoreError::Query)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Query)?;
    let (drives, positions): (Vec<_>, Vec<_>) =
        rows.into_iter().partition(|(entity, _)| entity == "drive");
    Ok((
        drives.into_iter().map(|(_, id)| id).collect(),
        positions.into_iter().map(|(_, id)| id).collect(),
    ))
}

/// The materialised destination and samples as the store holds them now.
fn drive_repair_materialised_rows_in(
    connection: &Connection,
    vehicle: &str,
    destination_drive_id: i64,
    sample_ids: &[i64],
) -> Result<Option<(ProjectionDrive, Vec<ProjectionPosition>)>, StoreError> {
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
    let drive = serde_json::from_str(&drive).map_err(StoreError::DeserializeLifecycleRow)?;
    let mut samples = Vec::with_capacity(sample_ids.len());
    for &id in sample_ids {
        let Some(sample) = materialised_position_in(connection, vehicle, id)? else {
            return Ok(None);
        };
        samples.push(sample);
    }
    Ok(Some((drive, samples)))
}

fn materialised_position_in(
    connection: &Connection,
    vehicle: &str,
    position_id: i64,
) -> Result<Option<ProjectionPosition>, StoreError> {
    let json: Option<String> = connection
        .query_row(
            "SELECT position_json FROM materialised_positions WHERE vehicle_id = ?1 AND position_id = ?2",
            params![vehicle, position_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    json.map(|json| serde_json::from_str(&json).map_err(StoreError::DeserializeLifecycleRow))
        .transpose()
}

/// Writes one materialised sample with its parent and journals it.
fn write_repair_sample_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    sample: &ProjectionPosition,
) -> Result<(), StoreError> {
    let json = serde_json::to_string(sample).map_err(StoreError::SerializeLifecycleRow)?;
    transaction
        .execute(
            "UPDATE materialised_positions SET drive_id = ?3, position_json = ?4
              WHERE vehicle_id = ?1 AND position_id = ?2",
            params![vehicle_id.to_string(), sample.id, sample.drive_id, json],
        )
        .map_err(StoreError::LifecycleWrite)?;
    record_sync_mutation_in_transaction(
        transaction,
        vehicle_id,
        "position",
        sample.id,
        sample.car_id,
        "upsert",
        &json,
    )
}

/// Writes one materialised destination drive and journals it.
fn write_repair_drive_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    drive: &ProjectionDrive,
) -> Result<bool, StoreError> {
    if !upsert_imported_drive_in_transaction(transaction, &vehicle_id.to_string(), drive)? {
        return Ok(false);
    }
    let json = serde_json::to_string(drive).map_err(StoreError::SerializeLifecycleRow)?;
    record_sync_mutation_in_transaction(transaction, vehicle_id, "drive", drive.id, drive.car_id, "upsert", &json)?;
    Ok(true)
}

/// The component identity the layer reads, from its pre-rows.
fn planned_component_of_pre_rows(
    key: &[u8; 32],
    pre: &crate::import::broken_drive_repair::layer::ComponentPreRows,
) -> crate::import::broken_drive_repair::PlannedComponent {
    crate::import::broken_drive_repair::PlannedComponent {
        component_key: *key,
        input_digest: [0; 32],
        destination_drive_id: i64::from(pre.destination.id),
        fragment_drive_ids: pre.fragments.iter().map(|row| i64::from(row.id)).collect(),
        sample_position_ids: pre.samples.iter().map(|row| i64::from(row.id)).collect(),
        effective_start_ms: 0,
        changed_rows: 0,
        members: Vec::new(),
    }
}

/// A drive without its location labels: a fence save or a reverse geocode
/// relabels a stored drive in place, and a label is no repair field.
fn drive_without_labels(drive: &ProjectionDrive) -> ProjectionDrive {
    ProjectionDrive {
        start_address: None,
        end_address: None,
        start_geofence: None,
        end_geofence: None,
        ..drive.clone()
    }
}

/// Whether `stored` is the repair's after-image `after`, apart from labels.
fn repaired_drive_holds(stored: &ProjectionDrive, after: &ProjectionDrive) -> bool {
    drive_without_labels(stored) == drive_without_labels(after)
}

/// The pre-repair destination to restore over `stored`: its end labels are
/// the stored ones (a repair keeps the end), and its start label is the
/// current fences' answer for its own start point when a label changed
/// since the repair, otherwise the one it had.
fn restored_destination_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    pre: &ProjectionDrive,
    after: &ProjectionDrive,
    stored: &ProjectionDrive,
) -> Result<ProjectionDrive, StoreError> {
    let mut restored = ProjectionDrive {
        end_address: stored.end_address.clone(),
        end_geofence: stored.end_geofence.clone(),
        ..pre.clone()
    };
    if stored.start_geofence != after.start_geofence
        && let (Some(latitude), Some(longitude)) = (pre.start_latitude, pre.start_longitude)
    {
        let fences = load_geofence_fences(transaction, vehicle_id)?;
        if let Some(name) = crate::lifecycle::match_geofence_name(latitude, longitude, &fences) {
            restored.start_geofence = Some(name);
        }
    }
    Ok(restored)
}

/// Whether the import's recorded projection of the entity is `value`: the
/// source itself now holds that row, so it is no repair's after-image.
fn source_projects_in<T: serde::Serialize>(
    transaction: &Transaction<'_>,
    vehicle: &str,
    entity: SourceDbProjectionStateEntity,
    id: i64,
    car_id: i64,
    value: &T,
) -> Result<bool, StoreError> {
    let recorded: Option<Vec<u8>> = transaction
        .query_row(
            concat!(
                "SELECT projection_sha256 FROM ",
                crate::db::import_table_names::import_projection_state_rows_table!(),
                " WHERE vehicle_id = ?1 AND entity_ordinal = ?2 AND entity_id = ?3"
            ),
            params![vehicle, i64::from(entity.ordinal()), id],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    let Some(recorded) = recorded else {
        return Ok(false);
    };
    let digest = canonical_payload_and_digest(entity, id, car_id, value)
        .map_err(StoreError::SourceDbProjectionState)?
        .1;
    Ok(recorded.as_slice() == digest.as_bytes().as_slice())
}

/// Restores one component's materialised pre-image from its recorded
/// pre-rows: each row the store still holds as the component's after-image
/// (a drive apart from its location labels) takes its pre-row again, with
/// one `sync_mutations` row. A row that no longer holds the after-image, or,
/// with `source_rows`, that the import's source now projects as it is, is
/// left as it is. Returns the rows restored.
fn compensate_component_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    key: &[u8; 32],
    source_rows: bool,
) -> Result<u64, StoreError> {
    let vehicle = vehicle_id.to_string();
    let before: Option<String> = transaction
        .query_row(
            "SELECT before_json FROM drive_repair_changes WHERE vehicle_id = ?1 AND component_key = ?2",
            params![vehicle, key.as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?
        .flatten();
    let Some(before) = before else {
        return Ok(0);
    };
    let pre: crate::import::broken_drive_repair::layer::ComponentPreRows =
        serde_json::from_str(&before).map_err(|_| StoreError::DriveRepairRecord("repair pre-rows"))?;
    let planned = planned_component_of_pre_rows(key, &pre);
    let Ok(layered) = crate::import::broken_drive_repair::layer::layer_component(&planned, &pre, None) else {
        return Ok(0);
    };
    let after = layered.after_image;
    let mut restored = 0_u64;
    let stored = drive_repair_materialised_rows_in(transaction, &vehicle, after.destination.id, &[])?;
    if let Some((stored, _)) = stored
        && repaired_drive_holds(&stored, &after.destination)
        && !(source_rows
            && source_projects_in(
                transaction,
                &vehicle,
                SourceDbProjectionStateEntity::Drive,
                stored.id,
                stored.car_id,
                &stored,
            )?)
    {
        let destination = restored_destination_in(
            transaction,
            vehicle_id,
            &pre.materialised_destination,
            &after.destination,
            &stored,
        )?;
        if write_repair_drive_in(transaction, vehicle_id, &destination)? {
            restored += 1;
        }
    }
    for (after_sample, pre_sample) in after.samples.iter().zip(&pre.materialised_samples) {
        if materialised_position_in(transaction, &vehicle, after_sample.id)?.as_ref() == Some(after_sample)
            && !(source_rows
                && source_projects_in(
                    transaction,
                    &vehicle,
                    SourceDbProjectionStateEntity::Position,
                    after_sample.id,
                    after_sample.car_id,
                    after_sample,
                )?)
        {
            write_repair_sample_in(transaction, vehicle_id, pre_sample)?;
            restored += 1;
        }
    }
    Ok(restored)
}

/// Compensates the active components among `keys` (see
/// [`compensate_component_in`]) and marks the export once when a row
/// changed. The caller moves them out of `active` and rewrites `L`.
fn compensate_components_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    keys: &[[u8; 32]],
) -> Result<DriveRepairCompensation, StoreError> {
    let mut report = DriveRepairCompensation::default();
    for key in keys {
        report.components += 1;
        report.restored_rows += compensate_component_in(transaction, vehicle_id, key, false)?;
    }
    if report.restored_rows > 0 {
        mark_export_dirty_in_transaction(transaction, vehicle_id)?;
    }
    Ok(report)
}

/// The active components of `vehicle` that name the entity.
fn active_components_of(
    transaction: &Transaction<'_>,
    vehicle: &str,
    entity: &str,
    entity_id: i64,
) -> Result<Vec<[u8; 32]>, StoreError> {
    let mut statement = transaction
        .prepare_cached(
            "SELECT component_key FROM drive_repair_changes
              WHERE vehicle_id = ?1 AND state = 'active'
                AND component_key IN (
                    SELECT component_key FROM drive_repair_change_members
                     WHERE vehicle_id = ?1 AND entity = ?2 AND entity_id = ?3)
              ORDER BY component_key",
        )
        .map_err(StoreError::Query)?;
    let keys = statement
        .query_map(params![vehicle, entity, entity_id], |row| row.get::<_, Vec<u8>>(0))
        .map_err(StoreError::Query)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Query)?;
    keys.into_iter().map(component_key_of).collect()
}

/// Compensates the active components naming the entity before a
/// supersession moves them out of `active` (M03-T27c).
fn compensate_superseded_components_in(
    transaction: &Transaction<'_>,
    vehicle: &str,
    entity: &str,
    entity_id: i64,
) -> Result<(), StoreError> {
    let keys = active_components_of(transaction, vehicle, entity, entity_id)?;
    if keys.is_empty() {
        return Ok(());
    }
    let vehicle_id =
        Uuid::parse_str(vehicle).map_err(|_| StoreError::DriveRepairRecord("vehicle id"))?;
    compensate_components_in(transaction, vehicle_id, &keys)?;
    Ok(())
}

/// The components a planning pass would plan from the stored index now, by
/// key: the revalidation of activation.
fn replan_drive_repairs_in(
    transaction: &Transaction<'_>,
    vehicle: &str,
    now_ms: i64,
) -> Result<HashMap<[u8; 32], crate::import::broken_drive_repair::PlannedComponent>, StoreError> {
    let mut eligibility = StoreRepairEligibility {
        transaction,
        vehicle: vehicle.to_owned(),
        coverage: coverage_intervals_for_vehicle(transaction, vehicle).ok(),
    };
    let mut planner = ComponentPlanner::new(vehicle.to_owned(), now_ms);
    let mut outcomes = Vec::new();
    let mut cursor = None;
    loop {
        let page = drive_repair_index_page_in(transaction, vehicle, cursor, CANDIDATE_PAGE_ROWS)?;
        let full = page.len() == CANDIDATE_PAGE_ROWS;
        for row in page {
            cursor = Some((row.start_ms, row.drive_id));
            outcomes.extend(planner.push(row, &mut eligibility)?);
        }
        if !full {
            break;
        }
    }
    outcomes.extend(planner.finish().1);
    Ok(outcomes
        .into_iter()
        .filter_map(|outcome| match outcome {
            PlanOutcome::Planned(component) => Some((component.component_key, component)),
            PlanOutcome::Refused { .. } => None,
        })
        .collect())
}

fn supersede_planned_in(
    transaction: &Transaction<'_>,
    vehicle: &str,
    key: &[u8; 32],
    now_ms: i64,
) -> Result<(), StoreError> {
    transaction
        .execute(
            "UPDATE drive_repair_changes SET state = 'superseded', updated_at_ms = ?3
              WHERE vehicle_id = ?1 AND component_key = ?2 AND state = 'planned'",
            params![vehicle, key.as_slice(), now_ms],
        )
        .map_err(StoreError::Query)?;
    Ok(())
}

impl HubStore {
    /// The planned components the next activation takes, in effective start
    /// order, while their summed changed rows stay within
    /// `max_changed_rows`; the first always fits. The rest wait.
    pub(crate) fn drive_repair_activation_candidates(
        &self,
        vehicle_id: Uuid,
        max_changed_rows: usize,
    ) -> Result<Vec<ActiveDriveRepair>, StoreError> {
        let connection = self.open_read_only_connection()?;
        let transaction = connection.unchecked_transaction().map_err(StoreError::Query)?;
        let vehicle = vehicle_id.to_string();
        let planned = {
            let mut statement = transaction
                .prepare(
                    "SELECT component_key, destination_drive_id FROM drive_repair_changes
                      WHERE vehicle_id = ?1 AND state = 'planned'
                      ORDER BY effective_start_ms, component_key",
                )
                .map_err(StoreError::Query)?;
            statement
                .query_map(params![vehicle], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)))
                .map_err(StoreError::Query)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::Query)?
        };
        let mut chosen = Vec::new();
        let mut rows = 0_usize;
        for (key, destination_drive_id) in planned {
            let key = component_key_of(key)?;
            let (drives, position_ids) = component_members_in(&transaction, &vehicle, &key)?;
            let fragment_drive_ids: Vec<i64> =
                drives.into_iter().filter(|id| *id != destination_drive_id).collect();
            let changed = fragment_drive_ids.len() * 2 + 1;
            if !chosen.is_empty() && rows + changed > max_changed_rows {
                break;
            }
            rows += changed;
            chosen.push(ActiveDriveRepair {
                component_key: key,
                destination_drive_id,
                fragment_drive_ids,
                position_ids,
                before: None,
            });
        }
        Ok(chosen)
    }

    /// Activates the components of `prepared`, each with the pre-rows the
    /// dry run of its head read, in one transaction (see the module notes).
    /// A component no longer planned is skipped, so a replay attaches
    /// nothing twice. The batch is left `publishing` with its `L`.
    pub(crate) fn activate_drive_repairs(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        prepared: &[([u8; 32], crate::import::broken_drive_repair::layer::ComponentPreRows)],
        now_ms: i64,
    ) -> Result<DriveRepairActivation, StoreError> {
        let vehicle = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let mut report = DriveRepairActivation::default();
        if import_publication_holds_in(&transaction)?.pending(vehicle_id) {
            report.import_pending = true;
            return Ok(report);
        }
        let replanned = replan_drive_repairs_in(&transaction, &vehicle, now_ms)?;
        let capture: Vec<u8> = transaction
            .query_row(
                "SELECT capture_digest FROM drive_repair_index_heads WHERE vehicle_id = ?1",
                params![vehicle],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)?
            .unwrap_or_else(|| vec![0; 32]);
        transaction
            .execute(
                "INSERT INTO drive_repair_batches(vehicle_id, policy_version, state, totals_json,
                     capture_digest, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 'publishing', '{}', ?3, ?4, ?4)",
                params![vehicle, REPAIR_POLICY_VERSION, capture, now_ms],
            )
            .map_err(StoreError::Query)?;
        let batch_id = transaction.last_insert_rowid();
        for (key, pre) in prepared {
            let recorded: Option<(String, Vec<u8>)> = transaction
                .query_row(
                    "SELECT state, protection_digest FROM drive_repair_changes
                      WHERE vehicle_id = ?1 AND component_key = ?2",
                    params![vehicle, key.as_slice()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(StoreError::Query)?;
            let Some((state, protection)) = recorded else {
                continue;
            };
            if state != "planned" {
                continue;
            }
            let Some(planned) = replanned.get(key) else {
                supersede_planned_in(&transaction, &vehicle, key, now_ms)?;
                report.superseded.push(*key);
                continue;
            };
            if protection_digest(&transaction, &vehicle, &planned.members)?.as_slice() != protection {
                supersede_planned_in(&transaction, &vehicle, key, now_ms)?;
                report.superseded.push(*key);
                continue;
            }
            let fragments_materialised = planned.fragment_drive_ids.iter().try_fold(false, |any, &id| {
                drive_repair_materialised_rows_in(&transaction, &vehicle, id, &[]).map(|row| any || row.is_some())
            })?;
            if fragments_materialised {
                supersede_planned_in(&transaction, &vehicle, key, now_ms)?;
                report.superseded.push(*key);
                continue;
            }
            // An import keeps its history samples in its packs: a sample
            // the store does not materialise stays so, and only one it holds
            // must still be the dry run's.
            let destination_current =
                drive_repair_materialised_rows_in(&transaction, &vehicle, planned.destination_drive_id, &[])?
                    .is_some_and(|(drive, _)| drive == pre.materialised_destination);
            let mut held = Vec::with_capacity(planned.sample_position_ids.len());
            let mut samples_current = planned.sample_position_ids.len() == pre.materialised_samples.len();
            for (&id, pre_sample) in planned.sample_position_ids.iter().zip(&pre.materialised_samples) {
                match materialised_position_in(&transaction, &vehicle, id)? {
                    Some(stored) if stored == *pre_sample => held.push(true),
                    Some(_) => samples_current = false,
                    None => held.push(false),
                }
            }
            if !destination_current || !samples_current {
                report.waiting.push(*key);
                continue;
            }
            let layered = match crate::import::broken_drive_repair::layer::layer_component(planned, pre, None) {
                Ok(layered) => layered,
                Err(error) => {
                    tracing::info!(%vehicle_id, component = %hex::encode(key), %error,
                        "a planned repair cannot be layered; it leaves the plan");
                    supersede_planned_in(&transaction, &vehicle, key, now_ms)?;
                    report.superseded.push(*key);
                    continue;
                }
            };
            let before = serde_json::to_string(pre).map_err(StoreError::SerializeLifecycleRow)?;
            transaction
                .execute(
                    "UPDATE drive_repair_changes SET state = 'active', before_json = ?3,
                         batch_id = ?4, updated_at_ms = ?5
                      WHERE vehicle_id = ?1 AND component_key = ?2 AND state = 'planned'",
                    params![vehicle, key.as_slice(), before, batch_id, now_ms],
                )
                .map_err(StoreError::Query)?;
            let after = layered.after_image;
            if write_repair_drive_in(&transaction, vehicle_id, &after.destination)? {
                report.changed_rows += 1;
            }
            for (sample, _) in after.samples.iter().zip(&held).filter(|(_, held)| **held) {
                write_repair_sample_in(&transaction, vehicle_id, sample)?;
                report.changed_rows += 1;
            }
            report.activated.push(*key);
        }
        if report.activated.is_empty() {
            transaction
                .execute("DELETE FROM drive_repair_batches WHERE batch_id = ?1", params![batch_id])
                .map_err(StoreError::Query)?;
            transaction.commit().map_err(StoreError::LifecycleWrite)?;
            report.layer = drive_repair_layer_in(&self.open_read_only_connection()?, vehicle_id)?;
            return Ok(report);
        }
        if report.changed_rows > 0 {
            mark_export_dirty_in_transaction(&transaction, vehicle_id)?;
        }
        report.layer = write_drive_repair_layer_in(&transaction, &vehicle)?;
        let expected = serde_json::json!({
            "layer": report.layer,
            "components": report.activated.iter().map(hex::encode).collect::<Vec<_>>(),
        });
        transaction
            .execute(
                "UPDATE drive_repair_batches SET expected_heads_json = ?2 WHERE batch_id = ?1",
                params![batch_id, expected.to_string()],
            )
            .map_err(StoreError::Query)?;
        transaction.commit().map_err(StoreError::LifecycleWrite)?;
        report.batch_id = Some(batch_id);
        Ok(report)
    }

    /// The vehicle's batch whose activation committed and whose head is not
    /// settled yet, if any.
    pub(crate) fn drive_repair_publishing_batch(&self, vehicle_id: Uuid) -> Result<Option<i64>, StoreError> {
        self.open_read_only_connection()?
            .query_row(
                "SELECT batch_id FROM drive_repair_batches
                  WHERE vehicle_id = ?1 AND state = 'publishing' ORDER BY batch_id LIMIT 1",
                params![vehicle_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)
    }

    /// Settles a `publishing` batch once its publication ran. With the
    /// vehicle's head `served` and a component of the batch still active, the
    /// batch is complete: a served head carries every active component. With
    /// it served and none left active (an import superseded them meanwhile),
    /// the batch is cancelled. Otherwise its still-active components are
    /// compensated and planned again and the batch fails, in one transaction
    /// that rewrites `L`, so the head served before the activation is served
    /// again. Recorded pre-rows stay: a retained or frozen head that carried
    /// a component still names them.
    pub(crate) fn settle_drive_repair_batch(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        batch_id: i64,
        served: bool,
        now_ms: i64,
    ) -> Result<DriveRepairSettlement, StoreError> {
        let vehicle = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let state: Option<String> = transaction
            .query_row(
                "SELECT state FROM drive_repair_batches WHERE batch_id = ?1 AND vehicle_id = ?2",
                params![batch_id, vehicle],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)?;
        if state.as_deref() != Some("publishing") {
            return Ok(DriveRepairSettlement::NotPublishing);
        }
        let active = batch_components_in(&transaction, &vehicle, batch_id, &["active"])?;
        let (outcome, settlement) = if served && !active.is_empty() {
            ("complete", DriveRepairSettlement::Complete)
        } else if served {
            ("cancelled", DriveRepairSettlement::Superseded)
        } else {
            let report = compensate_components_in(&transaction, vehicle_id, &active)?;
            transaction
                .execute(
                    "UPDATE drive_repair_changes SET state = 'planned', updated_at_ms = ?3
                      WHERE vehicle_id = ?1 AND batch_id = ?2 AND state = 'active'",
                    params![vehicle, batch_id, now_ms],
                )
                .map_err(StoreError::Query)?;
            write_drive_repair_layer_in(&transaction, &vehicle)?;
            ("failed", DriveRepairSettlement::Failed(report))
        };
        transaction
            .execute(
                "UPDATE drive_repair_batches SET state = ?2, updated_at_ms = ?3 WHERE batch_id = ?1",
                params![batch_id, outcome, now_ms],
            )
            .map_err(StoreError::Query)?;
        transaction.commit().map_err(StoreError::LifecycleWrite)?;
        Ok(settlement)
    }

    /// The vehicles with a batch whose activation committed and whose head
    /// is not settled yet, ascending.
    pub(crate) fn drive_repair_publishing_vehicles(&self) -> Result<Vec<Uuid>, StoreError> {
        let connection = self.open_read_only_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT DISTINCT vehicle_id FROM drive_repair_batches
                  WHERE state = 'publishing' ORDER BY vehicle_id",
            )
            .map_err(StoreError::Query)?;
        let vehicles = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(StoreError::Query)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Query)?;
        vehicles
            .iter()
            .map(|vehicle| Uuid::parse_str(vehicle).map_err(|_| StoreError::DriveRepairRecord("vehicle id")))
            .collect()
    }

    /// Undoes a component or every component of a batch in one transaction:
    /// each active one is compensated, each planned or active one becomes
    /// `undone` and is suppressed until an explicit retry, and `L` is
    /// rewritten. Superseded and earlier undone components, and every
    /// protection, stay as they are.
    pub(crate) fn undo_drive_repairs(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        target: DriveRepairTarget<'_>,
        now_ms: i64,
    ) -> Result<DriveRepairCompensation, StoreError> {
        let vehicle = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Begin)?;
        let keys = match target {
            DriveRepairTarget::Component(key) => vec![*key],
            DriveRepairTarget::Batch(batch_id) => {
                batch_components_in(&transaction, &vehicle, batch_id, &["planned", "active"])?
            }
        };
        let report = undo_components_in(&transaction, vehicle_id, &keys, now_ms)?;
        transaction.commit().map_err(StoreError::LifecycleWrite)?;
        Ok(report)
    }
}

/// The components of a batch in one of `states`, in key order.
fn batch_components_in(
    transaction: &Transaction<'_>,
    vehicle: &str,
    batch_id: i64,
    states: &[&str],
) -> Result<Vec<[u8; 32]>, StoreError> {
    let mut statement = transaction
        .prepare_cached(
            "SELECT component_key, state FROM drive_repair_changes
              WHERE vehicle_id = ?1 AND batch_id = ?2 ORDER BY component_key",
        )
        .map_err(StoreError::Query)?;
    let rows = statement
        .query_map(params![vehicle, batch_id], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)))
        .map_err(StoreError::Query)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Query)?;
    rows.into_iter()
        .filter(|(_, state)| states.contains(&state.as_str()))
        .map(|(key, _)| component_key_of(key))
        .collect()
}

/// Undoes `keys` in the caller's transaction (see
/// [`HubStore::undo_drive_repairs`]).
fn undo_components_in(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    keys: &[[u8; 32]],
    now_ms: i64,
) -> Result<DriveRepairCompensation, StoreError> {
    let vehicle = vehicle_id.to_string();
    let mut active = Vec::new();
    for key in keys {
        let state: Option<String> = transaction
            .query_row(
                "SELECT state FROM drive_repair_changes WHERE vehicle_id = ?1 AND component_key = ?2",
                params![vehicle, key.as_slice()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)?;
        if state.as_deref() == Some("active") {
            active.push(*key);
        }
    }
    let mut report = compensate_components_in(transaction, vehicle_id, &active)?;
    report.components = 0;
    let mut changed = false;
    for key in keys {
        let undone = transaction
            .execute(
                "UPDATE drive_repair_changes SET state = 'undone', updated_at_ms = ?3
                  WHERE vehicle_id = ?1 AND component_key = ?2 AND state IN ('planned', 'active')",
                params![vehicle, key.as_slice(), now_ms],
            )
            .map_err(StoreError::Query)?;
        report.components += undone as u64;
        changed |= undone == 1;
        transaction
            .execute(
                "INSERT OR IGNORE INTO drive_repair_suppressions(vehicle_id, component_key,
                     reason, recorded_at_ms) VALUES (?1, ?2, 'undo', ?3)",
                params![vehicle, key.as_slice(), now_ms],
            )
            .map_err(StoreError::Query)?;
    }
    if changed {
        write_drive_repair_layer_in(transaction, &vehicle)?;
    }
    Ok(report)
}
