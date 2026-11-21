// SPDX-License-Identifier: AGPL-3.0-only

// Applying and undoing repair batches end to end (M03-T27c). The caller
// holds the publication gate. An activation commits the batch's state, its
// materialised after-image with its sync journal and the layer `L` in one
// store transaction, then publishes once through the handback publication,
// which plans the vehicle's head again because its `L` changed. The batch
// is complete once that head is served; otherwise it is compensated and the
// head served before it is served again. Undo compensates the same way. A
// batch an interrupted call left `publishing` is settled by the next call
// before anything else is activated. No scheduler or operator calls these
// yet (M03-T28a).

/// What one apply or undo call did with a vehicle's repairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DriveRepairApplyOutcome {
    /// Nothing changed: an import's publication is pending, or the head
    /// cannot be planned now. The reason says which.
    Refused(String),
    /// Nothing was activated; revalidation may have superseded components.
    Idle(crate::db::DriveRepairActivation),
    /// The batch's head is served.
    Served {
        batch_id: i64,
        activation: crate::db::DriveRepairActivation,
    },
    /// Components left `active` and their rows were restored; `served`
    /// says whether the vehicle's head is served after the publication.
    Compensated {
        batch_id: Option<i64>,
        compensation: crate::db::DriveRepairCompensation,
        served: bool,
    },
}

/// Points at which tests interrupt or interleave an apply call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveRepairHookPoint {
    BeforeActivation,
    AfterActivation,
    AfterPublication,
}

#[cfg(test)]
type DriveRepairHook = Box<dyn FnOnce(&HubStore) -> Result<(), SourceDbImportError>>;

#[cfg(test)]
thread_local! {
    static DRIVE_REPAIR_HOOKS: std::cell::RefCell<Vec<(DriveRepairHookPoint, DriveRepairHook)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Runs the hook a test set for `point`, once; an `Err` stands for a crash.
/// Outside tests nothing runs.
fn drive_repair_hook(store: &HubStore, point: DriveRepairHookPoint) -> Result<(), SourceDbImportError> {
    #[cfg(test)]
    {
        let hook = DRIVE_REPAIR_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            let found = hooks.iter().position(|(at, _)| *at == point);
            found.map(|index| hooks.remove(index).1)
        });
        hook.map_or(Ok(()), |hook| hook(store))
    }
    #[cfg(not(test))]
    {
        let _ = (store, point);
        Ok(())
    }
}

/// Activates the vehicle's next batch of planned repairs and publishes its
/// head once (see the module notes). Refuses while an import's publication
/// is pending.
#[cfg_attr(not(test), allow(dead_code, reason = "M03-T28a adds the scheduler and operator callers"))]
pub(crate) fn apply_drive_repairs(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    now_ms: i64,
) -> Result<DriveRepairApplyOutcome, SourceDbImportError> {
    apply_drive_repairs_bounded(
        store,
        cursor_key,
        publication_gate,
        vehicle_id,
        now_ms,
        crate::db::MAX_BATCH_CHANGED_ROWS,
    )
}

/// [`apply_drive_repairs`] with the batch's changed-row bound.
fn apply_drive_repairs_bounded(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    now_ms: i64,
    max_changed_rows: usize,
) -> Result<DriveRepairApplyOutcome, SourceDbImportError> {
    if store.import_publication_holds()?.pending(vehicle_id) {
        return Ok(import_pending());
    }
    if let Some(batch_id) = store.drive_repair_publishing_batch(vehicle_id)? {
        return settle_drive_repair_batch(
            store,
            cursor_key,
            publication_gate,
            vehicle_id,
            batch_id,
            crate::db::DriveRepairActivation::default(),
            now_ms,
        );
    }
    let candidates = store.drive_repair_activation_candidates(vehicle_id, max_changed_rows)?;
    if candidates.is_empty() {
        return Ok(DriveRepairApplyOutcome::Idle(crate::db::DriveRepairActivation::default()));
    }
    let pre_rows =
        match drive_repair_dry_run_pre_rows(store, publication_gate, vehicle_id, &candidates)? {
            Ok(pre_rows) => pre_rows,
            Err(reason) => return Ok(DriveRepairApplyOutcome::Refused(reason)),
        };
    drive_repair_hook(store, DriveRepairHookPoint::BeforeActivation)?;
    let activation = store.activate_drive_repairs(publication_gate, vehicle_id, &pre_rows, now_ms)?;
    if activation.import_pending {
        return Ok(import_pending());
    }
    let Some(batch_id) = activation.batch_id else {
        return Ok(DriveRepairApplyOutcome::Idle(activation));
    };
    drive_repair_hook(store, DriveRepairHookPoint::AfterActivation)?;
    settle_drive_repair_batch(
        store,
        cursor_key,
        publication_gate,
        vehicle_id,
        batch_id,
        activation,
        now_ms,
    )
}

fn import_pending() -> DriveRepairApplyOutcome {
    DriveRepairApplyOutcome::Refused(
        "an import's publication is pending; repairs wait until it is resolved".into(),
    )
}

/// Publishes a `publishing` batch's head once and settles the batch: served,
/// it is complete; otherwise it is compensated and the vehicle's head is
/// published again from the restored ledger, which serves the head before
/// the activation when it is still the current one.
fn settle_drive_repair_batch(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    batch_id: i64,
    activation: crate::db::DriveRepairActivation,
    now_ms: i64,
) -> Result<DriveRepairApplyOutcome, SourceDbImportError> {
    publish_physical_after_handback(store, cursor_key, publication_gate)?;
    drive_repair_hook(store, DriveRepairHookPoint::AfterPublication)?;
    let served = store.physical_v3_current_head_served(vehicle_id)? == Some(true);
    let compensation =
        store.settle_drive_repair_batch(publication_gate, vehicle_id, batch_id, served, now_ms)?;
    if served {
        return Ok(DriveRepairApplyOutcome::Served {
            batch_id,
            activation,
        });
    }
    tracing::warn!(
        %vehicle_id,
        batch_id,
        "the repair batch's head was not served; its rows were restored and its components wait"
    );
    publish_physical_after_handback(store, cursor_key, publication_gate)?;
    Ok(DriveRepairApplyOutcome::Compensated {
        batch_id: Some(batch_id),
        compensation,
        served: store.physical_v3_current_head_served(vehicle_id)? == Some(true),
    })
}

/// Undoes a component or a batch: compensated and suppressed in one store
/// transaction, then the head is published once. Refuses while an import's
/// publication is pending. A batch left `publishing` is settled first.
#[cfg_attr(not(test), allow(dead_code, reason = "M03-T28a adds the operator caller"))]
pub(crate) fn undo_drive_repairs(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    target: crate::db::DriveRepairTarget<'_>,
    now_ms: i64,
) -> Result<DriveRepairApplyOutcome, SourceDbImportError> {
    if store.import_publication_holds()?.pending(vehicle_id) {
        return Ok(import_pending());
    }
    if let Some(batch_id) = store.drive_repair_publishing_batch(vehicle_id)? {
        settle_drive_repair_batch(
            store,
            cursor_key,
            publication_gate,
            vehicle_id,
            batch_id,
            crate::db::DriveRepairActivation::default(),
            now_ms,
        )?;
    }
    let compensation = store.undo_drive_repairs(publication_gate, vehicle_id, target, now_ms)?;
    publish_physical_after_handback(store, cursor_key, publication_gate)?;
    Ok(DriveRepairApplyOutcome::Compensated {
        batch_id: match target {
            crate::db::DriveRepairTarget::Batch(batch_id) => Some(batch_id),
            crate::db::DriveRepairTarget::Component(_) => None,
        },
        compensation,
        served: store.physical_v3_current_head_served(vehicle_id)? == Some(true),
    })
}
