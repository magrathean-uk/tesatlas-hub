// SPDX-License-Identifier: AGPL-3.0-only

// The negotiated 1.4 route's physical publication (M03-T21, DEC-117). For a
// vehicle with imported history and Hub-collected data the raw physical
// stage is planned before it is published: the seam's decisions are applied
// to the source rows and the store's kept Hub rows are added in physical
// form, so a 1.4 client holds exactly the sessions and children the store
// holds. A head the importer cannot plan is withheld, and 1.4 clients are
// refused as before (M03-T19).

/// PostgreSQL's epoch in Unix microseconds.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Why a vehicle's physical 1.4 target was not planned.
#[derive(Debug)]
enum PhysicalPlanError {
    /// The target cannot be planned: the reason names the row and field.
    Unplannable(String),
    Stage(crate::source_db_stage::SourceDbStageError),
    Store(StoreError),
}

impl From<crate::source_db_stage::SourceDbStageError> for PhysicalPlanError {
    fn from(error: crate::source_db_stage::SourceDbStageError) -> Self {
        Self::Stage(error)
    }
}

impl From<StoreError> for PhysicalPlanError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Publishes the schema-2.2 updates and the physical 1.4 head of one
/// selected-car import after its legacy publication has committed, under the
/// publication gate the import held throughout.
fn finish_selected_schema_22_publication(
    store: &HubStore,
    cursor_key: &CursorKey,
    captured: CapturedSourceDbImport,
) -> Result<SourceDbSelectedImportReport, SourceDbImportError> {
    let CapturedSourceDbImport {
        report,
        binding,
        updates_v2_2,
        atomic_schema_22,
        physical_stage,
        physical_seam,
        physical_source,
        source_basis,
        publication_gate,
        publication,
    } = captured;
    let vehicle_id = report.vehicle_id;
    let legacy_snapshot_id = report.snapshot_id;
    // Observe the schema-2.2 head only after this command's legacy transaction
    // has committed. The same retained gate has covered source capture and the
    // legacy commit, so no other publisher can interleave before this check.
    let updates_schema_22 = if let Some(publication) = atomic_schema_22 {
        publication
    } else {
        let expected_schema_22_head =
            production_updates_head(store, vehicle_id).map_err(|source| {
                SourceDbImportError::Schema22PostCommit {
                    vehicle_id,
                    legacy_snapshot_id,
                    source,
                }
            })?;

        publish_production_updates_schema_22_with_gate(
            store,
            cursor_key,
            &binding,
            updates_v2_2,
            &publication_gate,
            &expected_schema_22_head,
            Some((legacy_snapshot_id, report.sequence)),
        )
        .map_err(|source| SourceDbImportError::Schema22PostCommit {
            vehicle_id,
            legacy_snapshot_id,
            source,
        })?
    };
    // The negotiated 1.4 route's head, planned with the seam the lineage's
    // publication proved (M03-T21).
    let physical_target_expected = physical_stage.is_some();
    import_publication_crash_point("schema_22");
    // Issue #60: a captured import adopts its capture and publishes the head
    // stamped with its revision; without a capture there is no stage either.
    let physical_v3 = match publication {
        Some(intent) => finish_import_physical(
            store,
            cursor_key,
            &publication_gate,
            &intent,
            physical_stage,
            physical_seam.as_ref(),
        )?,
        None => publish_physical_route(
            store,
            cursor_key,
            binding,
            physical_stage,
            &publication_gate,
            physical_seam.as_ref(),
            physical_source
                .as_ref()
                .map(|(drives, charges)| (drives.as_slice(), charges.as_slice())),
        )?,
    };
    if physical_target_expected && physical_v3.is_none() {
        return Err(SourceDbImportError::PhysicalHeadWithheld {
            vehicle_id,
            legacy_snapshot_id,
        });
    }
    let summary = SourceDbImportSummary::new(source_basis, physical_seam.as_ref(), physical_v3.is_some());
    Ok(SourceDbSelectedImportReport {
        import: report,
        updates_schema_22,
        physical_v3,
        summary,
    })
}

/// Publishes one import's physical 1.4 head after its planned lineage. A
/// vehicle without the seam is published as captured. A vehicle with any of
/// it is planned with `seam`, the pack plan the lineage's publication
/// proved; the head is recorded as planned before it is published, so the
/// store serves it. Without a plan, or with a row the physical schema cannot
/// hold, the head is withheld and the stage discarded.
///
/// With `keep` (an import's source sessions), the raw stage and those
/// sessions are kept first, so a later handback can plan the head again
/// without another import (M03-T22).
fn publish_physical_route(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: Option<crate::source_db_stage::OwnedSourceDbStage>,
    publication_gate: &PublicationGate,
    seam: Option<&crate::import::source_db::seam::SeamPackPlan>,
    keep: Option<(&[ProjectionDrive], &[ProjectionCharge])>,
) -> Result<Option<crate::physical_publication::PhysicalV3Publication>, SourceDbImportError> {
    publish_planned_physical_route(
        store,
        cursor_key,
        binding,
        stage,
        publication_gate,
        seam,
        keep,
        None,
    )
}

/// As [`publish_physical_route`]; `kept_stage` is the recorded digest of
/// the kept stage `stage` is a copy of, when a handback publishes it. With
/// it, or with `keep`, a planned head is recorded with the inputs its plan
/// was a function of, so a later handback that would plan the same head
/// serves it again without planning it (M08-T19).
#[allow(clippy::too_many_arguments)]
fn publish_planned_physical_route(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: Option<crate::source_db_stage::OwnedSourceDbStage>,
    publication_gate: &PublicationGate,
    seam: Option<&crate::import::source_db::seam::SeamPackPlan>,
    keep: Option<(&[ProjectionDrive], &[ProjectionCharge])>,
    kept_stage: Option<&str>,
) -> Result<Option<crate::physical_publication::PhysicalV3Publication>, SourceDbImportError> {
    publish_planned_physical_route_at_revision(
        store,
        cursor_key,
        binding,
        stage,
        publication_gate,
        seam,
        keep,
        kept_stage,
        None,
    )
}

/// As [`publish_planned_physical_route`]; with `revision` the head is
/// stamped as that import revision's before it is published (issue #60),
/// so it is served while that import is the vehicle's revision.
#[allow(clippy::too_many_arguments)]
fn publish_planned_physical_route_at_revision(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: Option<crate::source_db_stage::OwnedSourceDbStage>,
    publication_gate: &PublicationGate,
    seam: Option<&crate::import::source_db::seam::SeamPackPlan>,
    keep: Option<(&[ProjectionDrive], &[ProjectionCharge])>,
    kept_stage: Option<&str>,
    revision: Option<Uuid>,
) -> Result<Option<crate::physical_publication::PhysicalV3Publication>, SourceDbImportError> {
    let Some(mut stage) = stage else {
        return Ok(None);
    };
    let vehicle_id = binding.vehicle_id;
    let kept_stage = match keep {
        Some((drives, charges)) => keep_physical_source(
            store,
            publication_gate,
            &binding,
            stage.stage_mut(),
            drives,
            charges,
        ),
        None => kept_stage.map(str::to_owned),
    };
    let planned = store.import_seam_present(vehicle_id)?;
    let mut facts = None;
    if planned {
        let outcome = match seam {
            Some(packs) => plan_physical_stage(store, &binding, stage.stage_mut(), packs),
            None => Err(PhysicalPlanError::Unplannable(
                "this import reached the physical publication without a seam plan".to_owned(),
            )),
        };
        match outcome {
            Ok(planned) => facts = Some(planned),
            Err(PhysicalPlanError::Unplannable(reason)) => {
                drop(stage);
                tracing::warn!(
                    %vehicle_id,
                    %reason,
                    "physical 1.4 target withheld: it cannot be planned with the import seam, \
                     so 1.4 clients are refused until an import can plan it"
                );
                return Ok(None);
            }
            Err(PhysicalPlanError::Stage(error)) => {
                return Err(crate::physical_publication::SourceDbPhysicalPublicationError::from(error).into());
            }
            Err(PhysicalPlanError::Store(error)) => return Err(error.into()),
        }
    }
    // M03-T27b: the active repair layer is the plan's last step, on the pure
    // imported rows, and the head is bound to the layer it was planned with.
    let layer = match apply_drive_repair_layer(store, publication_gate, &binding, stage.stage_mut()) {
        Ok(layer) => layer,
        Err(PhysicalPlanError::Unplannable(reason)) => {
            drop(stage);
            tracing::warn!(
                %vehicle_id,
                %reason,
                "physical 1.4 target withheld: its repair layer cannot be applied, so 1.4 \
                 clients are refused until a plan can apply it"
            );
            return Ok(None);
        }
        Err(PhysicalPlanError::Stage(error)) => {
            return Err(crate::physical_publication::SourceDbPhysicalPublicationError::from(error).into());
        }
        Err(PhysicalPlanError::Store(error)) => return Err(error.into()),
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_millis()).ok())
        .ok_or(crate::physical_publication::SourceDbPhysicalPublicationError::Clock)?;
    // The head is bound to the coverage its plan was made with (M03-T22).
    let coverage = seam.map(|packs| {
        crate::db::coverage_digest(packs.coverage().intervals())
    });
    let record = |snapshot_id: Uuid| {
        if let (true, Some(coverage)) = (planned, coverage.as_deref()) {
            store.record_physical_v3_seam_planned_with_coverage(
                publication_gate,
                vehicle_id,
                snapshot_id,
                coverage,
            )?;
        }
        if let Some(run_id) = revision {
            store.stamp_physical_v3_import_head(publication_gate, vehicle_id, snapshot_id, run_id)?;
        }
        store.record_physical_v3_repair_layer(publication_gate, vehicle_id, snapshot_id, layer.as_ref())?;
        Ok(())
    };
    let inputs = match (seam, &facts, kept_stage.as_deref()) {
        (Some(packs), Some(facts), Some(kept_stage)) => Some((
            facts.span,
            handback_plan_inputs_layered(
                handback_plan_inputs(kept_stage, &binding, packs, facts.span, &facts.stored, now_ms),
                layer.as_ref().map(|layer| layer.layer.as_str()),
            ),
        )),
        _ => None,
    };
    let (publication, prepared) = crate::physical_publication::publish_recorded_physical_v3_stage_reporting_month(
        store,
        cursor_key,
        binding,
        stage.take(),
        publication_gate,
        now_ms,
        &record,
    )?;
    if let Some((span, inputs)) = inputs {
        let recorded = crate::db::PhysicalV3HandbackPlan {
            snapshot_id: publication.admission.snapshot_id.to_string(),
            span,
            inputs,
            prepared,
        };
        if let Err(error) =
            store.record_physical_v3_handback_plan(publication_gate, vehicle_id, Some(&recorded))
        {
            tracing::warn!(
                %vehicle_id,
                %error,
                "the 1.4 head's plan inputs were not recorded; the next handback plans it again"
            );
        }
    }
    Ok(Some(publication))
}

/// What one plan of a physical stage read besides the stage, the seam and
/// the binding (M08-T19): the instants its source children span, in Unix
/// milliseconds as the seam decides them, and a digest of the store's kept
/// Hub rows it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PhysicalPlanFacts {
    span: Option<(i64, i64)>,
    stored: [u8; 32],
}

/// A digest of the store's kept Hub rows as [`plan_physical_stage`] reads
/// them. Each row was read from its stored JSON, so its JSON form here
/// reads back as the same row: equal digests mean equal rows.
fn stored_rows_digest(
    stored: &crate::db::ImportSeamStoredRows,
) -> Result<[u8; 32], PhysicalPlanError> {
    let mut hash = Sha256::new();
    hash.update(b"teslatlas-hub physical plan stored rows v1\n");
    let mut part = |name: &str, json: serde_json::Result<Vec<u8>>| {
        let json = json.map_err(|error| PhysicalPlanError::Unplannable(error.to_string()))?;
        hash.update(format!("{name} {}\n", json.len()));
        hash.update(&json);
        Ok::<(), PhysicalPlanError>(())
    };
    part("drives", serde_json::to_vec(&stored.drives))?;
    part("charges", serde_json::to_vec(&stored.charges))?;
    part("positions", serde_json::to_vec(&stored.positions))?;
    part("charge_samples", serde_json::to_vec(&stored.charge_samples))?;
    Ok(hash.finalize().into())
}

/// The inputs a handback's plan of the kept stage is a function of
/// (M08-T19), as one digest: the kept stage's recorded content digest, the
/// binding, the seam within the span of the stage's source children, that
/// span, the store's kept Hub rows, and the calendar month (UTC) whose
/// previous month the optional prepared map covers.
fn handback_plan_inputs(
    kept_stage: &str,
    binding: &ProjectionBinding,
    packs: &crate::import::source_db::seam::SeamPackPlan,
    span: Option<(i64, i64)>,
    stored: &[u8; 32],
    now_ms: i64,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"teslatlas-hub handback plan inputs v1\n");
    hash.update(format!("stage {kept_stage}\n"));
    hash.update(binding.installation_id.as_bytes());
    hash.update(binding.account_id.as_bytes());
    hash.update(binding.vehicle_id.as_bytes());
    hash.update(binding.generation.to_be_bytes());
    hash.update(binding.selected_car_id.to_be_bytes());
    hash.update(packs.digest_within(span));
    match span {
        Some((first, last)) => hash.update(format!("span {first} {last}\n")),
        None => hash.update(b"span -\n"),
    }
    hash.update(stored);
    let month = time::OffsetDateTime::from_unix_timestamp(now_ms.div_euclid(1_000))
        .map(|at| format!("{:04}-{:02}", at.year(), u8::from(at.month())))
        .unwrap_or_default();
    hash.update(format!("month {month}\n"));
    Sha256Digest::from_bytes(hash.finalize().into()).to_string()
}

/// [`handback_plan_inputs`] of a head planned with the repair layer `layer`
/// (M03-T27b). `L` is hashed in only while a component is active, so the
/// inputs of a store without repairs keep their digest, and a head planned
/// with a layer is never early-answered without it.
fn handback_plan_inputs_layered(inputs: String, layer: Option<&str>) -> String {
    let Some(layer) = layer else {
        return inputs;
    };
    let mut hash = Sha256::new();
    hash.update(b"teslatlas-hub handback plan inputs with repair layer v1\n");
    hash.update(format!("inputs {inputs}\nlayer {layer}\n"));
    Sha256Digest::from_bytes(hash.finalize().into()).to_string()
}

/// Applies the vehicle's active repair layer to its planned stage in place
/// (M03-T27b): each active component's destination takes its layered row,
/// its fragment drives go and their samples are reparented to it, from the
/// pure imported rows the stage holds. Members exclude seam links and
/// collection coverage, so the seam never touched them. The first
/// application records each component's pre-rows; a later one that finds
/// pre-rows whose digest differs from the recorded ones supersedes that
/// component (unless its batch is still publishing, which refuses the plan
/// for its settlement to compensate). Returns the layer applied, or `None`
/// with the stage untouched while no component is active.
fn apply_drive_repair_layer(
    store: &HubStore,
    publication_gate: &PublicationGate,
    binding: &ProjectionBinding,
    stage: &mut SourceDbStage,
) -> Result<Option<crate::db::PhysicalV3RepairLayer>, PhysicalPlanError> {
    let vehicle_id = binding.vehicle_id;
    let Some(active) = store.drive_repair_active_layer(vehicle_id)? else {
        return Ok(None);
    };
    let mut stale = Vec::new();
    let first =
        layer_drive_repair_components_noting_stale(store, vehicle_id, &active.components, stage, Some(&mut stale))?;
    let mut layer = Some(active.layer);
    if !stale.is_empty() {
        // REV-CX-RECHECK-T27 P2: the source changed under these components
        // where their evidence does not look. They are superseded and
        // compensated, and the head is planned with the layer left, which
        // must be exactly the components the stage now carries.
        stale.sort_unstable();
        let refusal = || {
            PhysicalPlanError::Unplannable(format!(
                "repair components {}: the pre-rows differ from the recorded digest",
                stale.iter().map(hex::encode).collect::<Vec<_>>().join(", ")
            ))
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_millis()).ok())
            .ok_or_else(refusal)?;
        let mut superseded = store.supersede_stale_drive_repairs(publication_gate, vehicle_id, &stale, now_ms)?;
        superseded.sort_unstable();
        if superseded != stale {
            return Err(refusal());
        }
        layer = store.drive_repair_active_layer(vehicle_id)?.map(|active| active.layer);
        let mut carried: Vec<String> = active
            .components
            .iter()
            .map(|component| component.component_key)
            .filter(|key| !stale.contains(key))
            .map(hex::encode)
            .collect();
        carried.sort_unstable();
        if layer.as_ref().map_or(&[][..], |layer| layer.components.as_slice()) != carried.as_slice() {
            return Err(refusal());
        }
    }
    for (key, pre) in &first {
        store.record_drive_repair_pre_rows(publication_gate, vehicle_id, key, pre)?;
    }
    Ok(layer)
}

/// Layers `components` on the planned stage in place (see
/// [`apply_drive_repair_layer`]) and returns the pre-rows of each one
/// without recorded pre-rows, as its first application reads them; the
/// activation's dry run reads them so (M03-T27c).
fn layer_drive_repair_components(
    store: &HubStore,
    vehicle_id: Uuid,
    components: &[crate::db::ActiveDriveRepair],
    stage: &mut SourceDbStage,
) -> Result<
    Vec<([u8; 32], crate::import::broken_drive_repair::layer::ComponentPreRows)>,
    PhysicalPlanError,
> {
    layer_drive_repair_components_noting_stale(store, vehicle_id, components, stage, None)
}

/// [`layer_drive_repair_components`]; with `stale`, a component whose
/// recorded pre-rows differ from the stage's is noted there and left off
/// the stage instead of refusing the plan.
fn layer_drive_repair_components_noting_stale(
    store: &HubStore,
    vehicle_id: Uuid,
    components: &[crate::db::ActiveDriveRepair],
    stage: &mut SourceDbStage,
    mut stale: Option<&mut Vec<[u8; 32]>>,
) -> Result<
    Vec<([u8; 32], crate::import::broken_drive_repair::layer::ComponentPreRows)>,
    PhysicalPlanError,
> {
    use crate::import::broken_drive_repair::PlannedComponent;
    use crate::import::broken_drive_repair::layer::{
        ComponentPreRows, DestinationSampleCounts, LayerError, layer_component,
    };
    use crate::source_db_projection::{SourceDbDrivePhysicalV2_2, SourceDbPositionPhysicalV2_2};
    use crate::source_db_stage::StageChildTable;

    let refused = |key: &[u8; 32], why: String| {
        PhysicalPlanError::Unplannable(format!("repair component {}: {why}", hex::encode(key)))
    };
    let watched: HashSet<i64> = components
        .iter()
        .flat_map(|component| {
            component
                .fragment_drive_ids
                .iter()
                .copied()
                .chain([component.destination_drive_id])
        })
        .collect();
    let mut children: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut first: Vec<([u8; 32], ComponentPreRows)> = Vec::new();
    stage.edit_sealed(|edit| -> Result<(), PhysicalPlanError> {
        edit.for_each_child(StageChildTable::Positions, &mut |id, parent, _| {
            if let Some(parent) = parent.filter(|parent| watched.contains(parent)) {
                children.entry(parent).or_default().push(id);
            }
        })?;
        let children_of = |drive: i64| children.get(&drive).map_or(&[][..], Vec::as_slice);
        for component in components {
            let key = &component.component_key;
            let drive = |id: i64| -> Result<SourceDbDrivePhysicalV2_2, PhysicalPlanError> {
                edit.get(SourceDbStageTable::Drives, id)?
                    .ok_or_else(|| refused(key, format!("drive {id} is not in the planned stage")))
            };
            let position = |id: i64| -> Result<SourceDbPositionPhysicalV2_2, PhysicalPlanError> {
                edit.get(SourceDbStageTable::Positions, id)?.ok_or_else(|| {
                    refused(key, format!("position {id} is not in the planned stage"))
                })
            };
            let destination = drive(component.destination_drive_id)?;
            let mut fragments = component
                .fragment_drive_ids
                .iter()
                .map(|&id| drive(id))
                .collect::<Result<Vec<_>, _>>()?;
            fragments.sort_by_key(|row| (row.start_date_pg_us, row.id));
            let mut samples = Vec::with_capacity(fragments.len());
            for fragment in &fragments {
                let held = children_of(i64::from(fragment.id));
                let [sample] = held else {
                    return Err(refused(
                        key,
                        format!("fragment drive {} has {} samples, not one", fragment.id, held.len()),
                    ));
                };
                if !component.position_ids.contains(sample) {
                    return Err(refused(key, format!("position {sample} is not a member")));
                }
                samples.push(position(*sample)?);
            }
            let mut destination_samples = DestinationSampleCounts {
                outside_temp: 0,
                inside_temp: 0,
            };
            for &id in children_of(component.destination_drive_id) {
                let row = position(id)?;
                destination_samples.outside_temp += u32::from(row.outside_temp_e1.is_some());
                destination_samples.inside_temp += u32::from(row.inside_temp_e1.is_some());
            }
            let sample_ids: Vec<i64> = samples.iter().map(|row| i64::from(row.id)).collect();
            // A later application takes the materialised half from the
            // record: the store may already hold the after-image.
            let (materialised_destination, materialised_samples, recorded) = match &component.before {
                Some(before) => (
                    before.materialised_destination.clone(),
                    before.materialised_samples.clone(),
                    Some(before.digest()),
                ),
                None => {
                    let Some((destination, stored)) = store.drive_repair_materialised_pre_rows(
                        vehicle_id,
                        component.destination_drive_id,
                        &sample_ids,
                    )?
                    else {
                        return Err(refused(key, "the store lacks its materialised destination".into()));
                    };
                    // Planning admits only samples the store materialises,
                    // so its reads follow the head (REV-CX-RECHECK-T27 P1).
                    let materialised = stored
                        .into_iter()
                        .zip(&samples)
                        .map(|(stored, row)| {
                            stored.ok_or_else(|| refused(key, format!("position {} is not materialised", row.id)))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    (destination, materialised, None)
                }
            };
            let pre = ComponentPreRows {
                destination,
                fragments,
                samples,
                destination_samples,
                materialised_destination,
                materialised_samples,
            };
            // The layer reads the component's ids only.
            let planned = PlannedComponent {
                component_key: *key,
                input_digest: [0; 32],
                destination_drive_id: component.destination_drive_id,
                fragment_drive_ids: pre.fragments.iter().map(|row| i64::from(row.id)).collect(),
                sample_position_ids: sample_ids,
                effective_start_ms: 0,
                changed_rows: 0,
                members: Vec::new(),
            };
            let layered = match layer_component(&planned, &pre, recorded.as_ref()) {
                Ok(layered) => layered,
                Err(LayerError::PreRowsDigestMismatch) if stale.is_some() => {
                    stale.as_mut().expect("checked").push(*key);
                    continue;
                }
                Err(error) => return Err(refused(key, error.to_string())),
            };
            for &id in &layered.effective.removed_drive_ids {
                edit.delete(SourceDbStageTable::Drives, i64::from(id))?;
            }
            edit.put(
                SourceDbStageTable::Drives,
                component.destination_drive_id,
                &layered.effective.destination,
            )?;
            for sample in &layered.effective.samples {
                edit.put(SourceDbStageTable::Positions, i64::from(sample.id), sample)?;
            }
            if component.before.is_none() {
                first.push((*key, pre));
            }
        }
        Ok(())
    })?;
    Ok(first)
}

/// Plans the sealed physical stage of one vehicle with the import seam, in
/// place (design M03-T21, section 1). Source drives and charging processes
/// the plan does not pack go, packed ones take the widened bounds; source
/// positions and charges (samples) inside coverage go, others take the
/// parent standing for a skipped one; relation rows no remaining session
/// names go. Then the store's kept Hub rows are written in physical form.
fn plan_physical_stage(
    store: &HubStore,
    binding: &ProjectionBinding,
    stage: &mut SourceDbStage,
    packs: &crate::import::source_db::seam::SeamPackPlan,
) -> Result<PhysicalPlanFacts, PhysicalPlanError> {
    use crate::import::source_db::seam::{ChildRow, PackedChild, PackedSession};
    use crate::source_db_projection::{
        SourceDbChargingProcessPhysicalV2_2, SourceDbDrivePhysicalV2_2,
        SourceDbPositionPhysicalV2_2,
    };
    use crate::source_db_stage::StageChildTable;

    let stored = store.import_seam_stored_rows(binding.vehicle_id)?;
    let stored_digest = stored_rows_digest(&stored)?;
    let mut span: Option<(i64, i64)> = None;
    let car_id = i16::try_from(binding.selected_car_id)
        .map_err(|_| PhysicalPlanError::Unplannable("the selected car id is out of range".into()))?;
    let packed = |session: Option<PackedSession>| matches!(session, Some(PackedSession::Keep { .. }));
    let mut omitted_samples = 0_u64;
    stage.edit_sealed(|edit| -> Result<(), PhysicalPlanError> {
        for (id, mut drive) in edit.rows::<SourceDbDrivePhysicalV2_2>(SourceDbStageTable::Drives)? {
            match packs.drive(id) {
                Some(PackedSession::Skip) => {
                    edit.delete(SourceDbStageTable::Drives, id)?;
                    continue;
                }
                Some(PackedSession::Keep { bounds: Some((start_ms, end_ms)) }) => {
                    drive.start_date_pg_us = pg_us(start_ms);
                    if let Some(end_ms) = end_ms {
                        drive.end_date_pg_us = Some(pg_us(end_ms));
                    }
                    edit.put(SourceDbStageTable::Drives, id, &drive)?;
                }
                _ => {}
            }
        }
        for (id, mut process) in
            edit.rows::<SourceDbChargingProcessPhysicalV2_2>(SourceDbStageTable::ChargingProcesses)?
        {
            match packs.charge(id) {
                Some(PackedSession::Skip) => {
                    edit.delete(SourceDbStageTable::ChargingProcesses, id)?;
                    continue;
                }
                Some(PackedSession::Keep { bounds: Some((start_ms, end_ms)) }) => {
                    process.start_date_pg_us = pg_us(start_ms);
                    if let Some(end_ms) = end_ms {
                        process.end_date_pg_us = Some(pg_us(end_ms));
                    }
                    edit.put(SourceDbStageTable::ChargingProcesses, id, &process)?;
                }
                _ => {}
            }
        }

        // Source children: by instant, under the parent the plan gives them.
        for table in [StageChildTable::Positions, StageChildTable::Charges] {
            let mut changes: Vec<(i64, Option<Option<i64>>)> = Vec::new();
            edit.for_each_child(table, &mut |id, parent, at_pg_us| {
                let child = ChildRow {
                    id,
                    at_ms: unix_ms(at_pg_us),
                    parent,
                };
                span = Some(span.map_or((child.at_ms, child.at_ms), |(first, last)| {
                    (first.min(child.at_ms), last.max(child.at_ms))
                }));
                let decided = match table {
                    StageChildTable::Positions => packs.position(child),
                    StageChildTable::Charges => packs.charge_sample(child),
                };
                match decided {
                    PackedChild::Skip => changes.push((id, None)),
                    PackedChild::Keep { parent: Some(planned) } if Some(planned) != parent => {
                        changes.push((id, Some(Some(planned))));
                    }
                    PackedChild::Keep { .. } => {}
                }
            })?;
            for (id, change) in changes {
                match (table, change) {
                    (StageChildTable::Positions, None) => {
                        edit.delete(SourceDbStageTable::Positions, id)?;
                    }
                    (StageChildTable::Charges, None) => {
                        edit.delete(SourceDbStageTable::Charges, id)?;
                    }
                    (StageChildTable::Positions, Some(parent)) => {
                        let mut row: SourceDbPositionPhysicalV2_2 = edit
                            .get(SourceDbStageTable::Positions, id)?
                            .ok_or(StoreError::ImportGenerationConflict)?;
                        row.drive_id = parent.map(|parent| id32(parent, "position parent")).transpose()?;
                        edit.put(SourceDbStageTable::Positions, id, &row)?;
                    }
                    (StageChildTable::Charges, Some(parent)) => {
                        let mut row: crate::source_db_projection::SourceDbChargePhysicalV2_2 = edit
                            .get(SourceDbStageTable::Charges, id)?
                            .ok_or(StoreError::ImportGenerationConflict)?;
                        let parent = parent.ok_or(StoreError::ImportGenerationConflict)?;
                        row.charging_process_id = id32(parent, "charge sample parent")?;
                        edit.put(SourceDbStageTable::Charges, id, &row)?;
                    }
                }
            }
        }

        // The Hub's rows the store keeps.
        let mut ends: HashMap<i64, ((i64, i64), (i64, i64))> = HashMap::new();
        let mut hub_positions: Vec<(i64, i64)> = Vec::with_capacity(stored.positions.len());
        for position in &stored.positions {
            hub_positions.push((position.date_ms, position.id));
            if let Some(drive) = position.drive_id {
                let at = (position.date_ms, position.id);
                ends.entry(drive)
                    .and_modify(|(first, last)| {
                        *first = (*first).min(at);
                        *last = (*last).max(at);
                    })
                    .or_insert((at, at));
            }
            edit.put(
                SourceDbStageTable::Positions,
                position.id,
                &physical_position(position, car_id)?,
            )?;
        }
        hub_positions.sort_unstable();
        for drive in stored.drives.iter().filter(|drive| !packed(packs.drive(drive.id))) {
            let ends = ends.get(&drive.id).copied();
            edit.put(
                SourceDbStageTable::Drives,
                drive.id,
                &physical_drive(drive, car_id, ends.map(|(first, last)| (first.1, last.1)))?,
            )?;
        }
        for charge in stored.charges.iter().filter(|charge| !packed(packs.charge(charge.id))) {
            let start = charge.start_date_ms;
            let nearest = hub_positions
                .iter()
                .min_by_key(|(at, id)| (at.abs_diff(start), *at, *id))
                .map(|(_, id)| *id);
            let position_id = match nearest {
                Some(id) => id,
                None => edit.nearest_position(pg_us(start))?.ok_or_else(|| {
                    PhysicalPlanError::Unplannable(format!(
                        "Hub charge {} has no position to stand at",
                        charge.id
                    ))
                })?,
            };
            edit.put(
                SourceDbStageTable::ChargingProcesses,
                charge.id,
                &physical_charge(charge, car_id, position_id)?,
            )?;
        }
        for sample in &stored.charge_samples {
            match physical_charge_sample(sample)? {
                Some(row) => edit.put(SourceDbStageTable::Charges, sample.id, &row)?,
                None => omitted_samples += 1,
            }
        }

        // Relation rows no remaining drive or charging process names go:
        // the physical head holds only referenced ones.
        let mut addresses = HashSet::new();
        let mut geofences = HashSet::new();
        for (_, drive) in edit.rows::<SourceDbDrivePhysicalV2_2>(SourceDbStageTable::Drives)? {
            addresses.extend([drive.start_address_id, drive.end_address_id].into_iter().flatten());
            geofences.extend([drive.start_geofence_id, drive.end_geofence_id].into_iter().flatten());
        }
        for (_, process) in
            edit.rows::<SourceDbChargingProcessPhysicalV2_2>(SourceDbStageTable::ChargingProcesses)?
        {
            addresses.extend(process.address_id);
            geofences.extend(process.geofence_id);
        }
        for (table, referenced) in [
            (SourceDbStageTable::Addresses, &addresses),
            (SourceDbStageTable::Geofences, &geofences),
        ] {
            for (id, _) in edit.rows::<serde_json::Value>(table)? {
                if !i32::try_from(id).is_ok_and(|id| referenced.contains(&id)) {
                    edit.delete(table, id)?;
                }
            }
        }
        Ok(())
    })?;
    if omitted_samples > 0 {
        tracing::warn!(
            vehicle_id = %binding.vehicle_id,
            omitted_samples,
            "Hub charge samples without charger power, energy added or ideal range have no \
             physical form and are left out of the 1.4 target"
        );
    }
    Ok(PhysicalPlanFacts {
        span,
        stored: stored_digest,
    })
}

fn pg_us(ms: i64) -> i64 {
    ms.saturating_mul(1_000).saturating_sub(PG_EPOCH_OFFSET_US)
}

/// Unix milliseconds of a PostgreSQL instant, truncated as the source reader
/// truncates them.
fn unix_ms(pg_us: i64) -> i64 {
    pg_us.saturating_add(PG_EPOCH_OFFSET_US) / 1_000
}

fn unplannable(what: &str, id: i64) -> PhysicalPlanError {
    PhysicalPlanError::Unplannable(format!("{what} of row {id} is out of the physical range"))
}

fn id32(id: i64, what: &str) -> Result<i32, PhysicalPlanError> {
    i32::try_from(id).map_err(|_| unplannable(what, id))
}

fn fixed(value: Option<f64>, scale: i32) -> Option<crate::hub_pack::ProjectionFixedNumericV2_2> {
    use crate::hub_pack::ProjectionFixedNumericV2_2;
    value.map(|value| {
        if value.is_finite() {
            ProjectionFixedNumericV2_2::Finite((value * 10_f64.powi(scale)).round() as i64)
        } else {
            ProjectionFixedNumericV2_2::NaN
        }
    })
}

fn small(value: Option<i64>, what: &str, id: i64) -> Result<Option<i16>, PhysicalPlanError> {
    value
        .map(|value| i16::try_from(value).map_err(|_| unplannable(what, id)))
        .transpose()
}

fn small_f(value: Option<f64>, what: &str, id: i64) -> Result<Option<i16>, PhysicalPlanError> {
    small(value.map(|value| value.round() as i64), what, id)
}

fn bits(value: Option<f64>) -> Option<crate::hub_pack::ProjectionFloat64BitsV2_2> {
    value.map(crate::hub_pack::ProjectionFloat64BitsV2_2::from_f64)
}

/// A stored drive in physical form, with its first and last Hub position.
/// The Hub holds no address or geofence row ids, so those references are
/// null.
fn physical_drive(
    drive: &ProjectionDrive,
    car_id: i16,
    ends: Option<(i64, i64)>,
) -> Result<crate::source_db_projection::SourceDbDrivePhysicalV2_2, PhysicalPlanError> {
    let id = drive.id;
    Ok(crate::source_db_projection::SourceDbDrivePhysicalV2_2 {
        id: id32(id, "drive id")?,
        car_id,
        start_date_pg_us: pg_us(drive.start_date_ms),
        end_date_pg_us: Some(pg_us(drive.end_date_ms)),
        start_position_id: ends.map(|(first, _)| id32(first, "drive start position")).transpose()?,
        end_position_id: ends.map(|(_, last)| id32(last, "drive end position")).transpose()?,
        start_address_id: None,
        end_address_id: None,
        start_geofence_id: None,
        end_geofence_id: None,
        outside_temp_avg_e1: fixed(drive.outside_temp_avg, 1),
        inside_temp_avg_e1: fixed(drive.inside_temp_avg, 1),
        speed_max: small(drive.speed_max, "drive speed_max", id)?,
        power_max: small_f(drive.power_max, "drive power_max", id)?,
        power_min: small_f(drive.power_min, "drive power_min", id)?,
        start_ideal_range_km_e2: fixed(drive.start_ideal_range_km, 2),
        end_ideal_range_km_e2: fixed(drive.end_ideal_range_km, 2),
        start_rated_range_km_e2: fixed(drive.start_rated_range_km, 2),
        end_rated_range_km_e2: fixed(drive.end_rated_range_km, 2),
        start_km: None,
        end_km: None,
        distance: bits(drive.distance_km),
        duration_min: small(drive.duration_min, "drive duration_min", id)?,
        ascent: small(drive.ascent, "drive ascent", id)?,
        descent: small(drive.descent, "drive descent", id)?,
    })
}

/// A stored Hub position in physical form.
fn physical_position(
    position: &crate::hub_pack::ProjectionPosition,
    car_id: i16,
) -> Result<crate::source_db_projection::SourceDbPositionPhysicalV2_2, PhysicalPlanError> {
    use crate::hub_pack::ProjectionFixedNumericV2_2;
    let id = position.id;
    let coordinate = |value: f64| fixed(Some(value), 6).unwrap_or(ProjectionFixedNumericV2_2::NaN);
    Ok(crate::source_db_projection::SourceDbPositionPhysicalV2_2 {
        id: id32(id, "position id")?,
        car_id,
        drive_id: position
            .drive_id
            .map(|drive| id32(drive, "position drive"))
            .transpose()?,
        date_pg_us: pg_us(position.date_ms),
        latitude_e6: coordinate(position.latitude),
        longitude_e6: coordinate(position.longitude),
        elevation: small(position.elevation, "position elevation", id)?,
        speed: small(position.speed, "position speed", id)?,
        power: small_f(position.power, "position power", id)?,
        odometer: bits(position.odometer),
        ideal_battery_range_km_e2: fixed(position.ideal_battery_range_km, 2),
        est_battery_range_km_e2: fixed(position.est_battery_range_km, 2),
        rated_battery_range_km_e2: fixed(position.rated_battery_range_km, 2),
        battery_level: small(position.battery_level, "position battery_level", id)?,
        usable_battery_level: small(position.usable_battery_level, "position usable_battery_level", id)?,
        battery_heater: position.battery_heater,
        battery_heater_on: position.battery_heater_on,
        battery_heater_no_power: position.battery_heater_no_power,
        outside_temp_e1: fixed(position.outside_temp, 1),
        inside_temp_e1: fixed(position.inside_temp, 1),
        fan_status: position
            .fan_status
            .map(|value| i32::try_from(value).map_err(|_| unplannable("position fan_status", id)))
            .transpose()?,
        driver_temp_setting_e1: fixed(position.driver_temp_setting, 1),
        passenger_temp_setting_e1: fixed(position.passenger_temp_setting, 1),
        is_climate_on: position.is_climate_on,
        is_rear_defroster_on: position.is_rear_defroster_on,
        is_front_defroster_on: position.is_front_defroster_on,
        tpms_pressure_fl_e1: fixed(position.tpms_pressure_fl, 1),
        tpms_pressure_fr_e1: fixed(position.tpms_pressure_fr, 1),
        tpms_pressure_rl_e1: fixed(position.tpms_pressure_rl, 1),
        tpms_pressure_rr_e1: fixed(position.tpms_pressure_rr, 1),
    })
}

/// A stored charge in physical form, standing at `position_id`.
fn physical_charge(
    charge: &ProjectionCharge,
    car_id: i16,
    position_id: i64,
) -> Result<crate::source_db_projection::SourceDbChargingProcessPhysicalV2_2, PhysicalPlanError> {
    let id = charge.id;
    Ok(crate::source_db_projection::SourceDbChargingProcessPhysicalV2_2 {
        id: id32(id, "charge id")?,
        car_id,
        position_id: id32(position_id, "charge position")?,
        address_id: None,
        geofence_id: None,
        start_date_pg_us: pg_us(charge.start_date_ms),
        end_date_pg_us: charge.end_date_ms.map(pg_us),
        charge_energy_added_e2: fixed(charge.charge_energy_added, 2),
        charge_energy_used_e2: fixed(charge.charge_energy_used_kwh, 2),
        start_ideal_range_km_e2: fixed(charge.start_ideal_range_km, 2),
        end_ideal_range_km_e2: fixed(charge.end_ideal_range_km, 2),
        start_rated_range_km_e2: fixed(charge.start_rated_range_km, 2),
        end_rated_range_km_e2: fixed(charge.end_rated_range_km, 2),
        start_battery_level: small(charge.start_battery_level, "charge start_battery_level", id)?,
        end_battery_level: small(charge.end_battery_level, "charge end_battery_level", id)?,
        duration_min: small(charge.duration_min, "charge duration_min", id)?,
        outside_temp_avg_e1: fixed(charge.outside_temp_avg, 1),
        cost_e2: fixed(charge.cost, 2),
    })
}

/// A stored Hub charge sample in physical form; `None` when it lacks a value
/// the physical schema requires (charger power, energy added, ideal range).
fn physical_charge_sample(
    sample: &crate::hub_pack::ProjectionChargeSample,
) -> Result<Option<crate::source_db_projection::SourceDbChargePhysicalV2_2>, PhysicalPlanError> {
    use crate::hub_pack::ProjectionFixedNumericV2_2;
    let (Some(energy), Some(power), Some(range)) = (
        sample.charge_energy_added_kwh,
        sample.charger_power_kw,
        sample.ideal_range_km,
    ) else {
        return Ok(None);
    };
    let id = sample.id;
    Ok(Some(crate::source_db_projection::SourceDbChargePhysicalV2_2 {
        id: id32(id, "charge sample id")?,
        charging_process_id: id32(sample.charge_process_id, "charge sample parent")?,
        date_pg_us: pg_us(sample.timestamp_ms),
        battery_heater: sample.battery_heater,
        battery_heater_on: sample.battery_heater_on,
        battery_heater_no_power: sample.battery_heater_no_power,
        battery_level: small(sample.battery_level, "charge sample battery_level", id)?,
        usable_battery_level: small(sample.usable_battery_level, "charge sample usable_battery_level", id)?,
        charge_energy_added_e2: fixed(Some(energy), 2).unwrap_or(ProjectionFixedNumericV2_2::NaN),
        charger_actual_current: small_f(sample.charger_actual_current, "charge sample current", id)?,
        charger_phases: small(sample.charger_phases, "charge sample phases", id)?,
        charger_pilot_current: small_f(sample.charger_pilot_current, "charge sample pilot current", id)?,
        charger_power: small_f(Some(power), "charge sample power", id)?.unwrap_or_default(),
        charger_voltage: small_f(sample.charger_voltage, "charge sample voltage", id)?,
        conn_charge_cable: sample.charge_cable.clone(),
        fast_charger_present: sample.fast_charger_present,
        fast_charger_brand: sample.fast_charger_brand.clone(),
        fast_charger_type: sample.fast_charger_type.clone(),
        ideal_battery_range_km_e2: fixed(Some(range), 2).unwrap_or(ProjectionFixedNumericV2_2::NaN),
        rated_battery_range_km_e2: fixed(sample.rated_range_km, 2),
        not_enough_power_to_heat: sample.not_enough_power_to_heat,
        outside_temp_e1: fixed(sample.outside_temp_c, 1),
    }))
}

// The raw stage and source sessions kept for a handback, and the handback's
// publication (M03-T22).
include!("physical_kept.rs");

// Applying and undoing repair batches end to end (M03-T27c).
include!("broken_drive_repair_apply.rs");

// The raw-stage head carries the active repair layer (M03-T27b).
#[cfg(test)]
mod broken_drive_repair_publish_tests {
    include!("broken_drive_repair_publish_tests.rs");
}

/// A vehicle ready for a broken-drive repair (M03-T27c, REV-CX-T28): two
/// broken drives imported with their raw physical stage kept and a served
/// 1.4 head, plus the candidate index the import recorded. The importer's
/// apply tests and the real-binary repair journey
/// (`tests/broken_drive_repair_journey.rs`) build this state from the same
/// builders. Compiled only for tests and the test-only `journey-doubles`
/// feature, so the production library has none of it.
#[cfg(any(test, feature = "journey-doubles"))]
pub mod repair_journey_fixture {
    include!("repair_journey_fixture.rs");
}
