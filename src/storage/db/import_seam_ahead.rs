// SPDX-License-Identifier: AGPL-3.0-only

/// What [`HubStore::apply_import_seam_ahead_of_packs`] hands the importer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportSeamAhead {
    /// The part of the plan the packs carry; publication proves it again by
    /// its digest.
    pub(crate) packs: crate::import::source_db::seam::SeamPackPlan,
    /// Packed source drives whose stored row is not yet the planned row (a
    /// drive skipped before and now packed, or one the plan widened): the
    /// packs carry them even when the source did not change them.
    pub(crate) restated_drives: Vec<i64>,
    /// Packed source charges the packs carry even when the source did not
    /// change them: a charge skipped before and now packed, or one the plan
    /// widened. The store holds no source charge to compare with.
    pub(crate) restated_charges: Vec<i64>,
    /// The plan changed the Hub's side of the store, so its sync mutations
    /// must be published before any pack that relies on them.
    pub(crate) changed_the_store: bool,
}

/// What a routing decision for a direct successor rests on (DEC-103): the
/// seam the packs would carry, whether the vehicle had seam history before
/// it, and how many of the vehicle's sync mutations would then wait for
/// publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportSeamDryRun {
    pub(crate) ahead: ImportSeamAhead,
    /// Coverage in the store, a window still open, a Hub-range row, a seam
    /// link or a live delta span: such a vehicle's import base is never
    /// replaced.
    pub(crate) seam_history: bool,
    pub(crate) unpublished: u64,
}

impl HubStore {
    /// Applies the Hub's side of the import seam ahead of the packs (design
    /// M03-SEAM-R3, section 4), for drives and charges alike: Hub sessions
    /// that lost are removed with a tombstone, an earlier import's copy of a skipped or stale source drive
    /// goes with a tombstone, Hub positions outside coverage go with a
    /// tombstone, kept Hub positions follow their drive's winner, kept Hub
    /// drives widen, charges and charge samples follow the same rules, and
    /// the decisions are recorded in `import_seam_links`.
    /// The winning source drives are not written here: publication writes
    /// them in the transaction that publishes the packs carrying them.
    ///
    /// The caller publishes the resulting sync mutations before it writes any
    /// pack, so a client never holds a replacement next to the row it
    /// replaces. The returned plan is the one publication must find again.
    ///
    /// With `decided`, the seam is applied only when the store still gives
    /// the dry run a routing decision rested on (DEC-103): the same seam
    /// history, plan and waiting sync mutations, worked out again in the
    /// transaction that applies it. Otherwise the transaction is rolled back
    /// and `None` comes back: nothing changed, and the caller decides again.
    ///
    /// The importer no longer applies the seam ahead of its packs (M03-T19):
    /// it stages it ([`HubStore::stage_import_seam_ahead_of_packs`]) and
    /// publication applies it. This remains for the store's own tests.
    #[cfg(test)]
    pub(crate) fn apply_import_seam_ahead_of_packs(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        car_id: i64,
        drives: &[ProjectionDrive],
        charges: &[crate::hub_pack::ProjectionCharge],
        decided: Option<&ImportSeamDryRun>,
    ) -> Result<Option<ImportSeamAhead>, StoreError> {
        let run = self.import_seam_ahead_of_packs(
            vehicle_id,
            car_id,
            drives,
            charges,
            false,
            |_, _, run| Ok(decided.is_none_or(|decided| decided == run)),
        )?;
        Ok(run.committed.then_some(run.run.ahead))
    }

    /// The seam [`HubStore::apply_import_seam_ahead_of_packs`] would apply,
    /// worked out from one snapshot without writing (DEC-103, PG-2), with
    /// what a routing decision rests on. Nothing in the store changes, and
    /// SQLite's write lock is never taken.
    pub(crate) fn dry_run_import_seam_ahead_of_packs(
        &self,
        _publication_gate: &PublicationGate,
        vehicle_id: Uuid,
        car_id: i64,
        drives: &[ProjectionDrive],
        charges: &[crate::hub_pack::ProjectionCharge],
    ) -> Result<ImportSeamDryRun, StoreError> {
        self.import_seam_ahead_of_packs(vehicle_id, car_id, drives, charges, false, |_, _, _| {
            Ok(false)
        })
        .map(|run| run.run)
    }

    /// The seam ahead of the packs, with the vehicle's seam history before it
    /// and its unpublished sync mutations after it, applied only when
    /// `commit` accepts what it found. `commit` reads the snapshot the plan
    /// was made from and the sync mutations applying it would journal (in
    /// revision order, after every waiting one).
    ///
    /// The plan, what applying it would change and, with `stored_rows`, the
    /// stored rows the applied store would hold are all worked out in one
    /// DEFERRED read transaction without writing (PG-2): planning never
    /// holds SQLite's write lock, so a collector commit is not held up. Only
    /// an accepted plan that changes the store takes the lock, to apply it;
    /// the journal it wrote must be the one worked out, and the applied
    /// store, planned again, must give the packs the same decisions, or the
    /// transaction rolls back with [`StoreError::ImportGenerationConflict`].
    /// A snapshot that went stale before that upgrade is planned again, a
    /// bounded number of times; the last attempt then plans under IMMEDIATE,
    /// holding the write lock from its first read, so writers that never
    /// take the publication gate cannot leave the plan unapplied.
    fn import_seam_ahead_of_packs(
        &self,
        vehicle_id: Uuid,
        car_id: i64,
        drives: &[ProjectionDrive],
        charges: &[crate::hub_pack::ProjectionCharge],
        stored_rows: bool,
        mut commit: impl FnMut(&Transaction<'_>, &[SyncMutation], &ImportSeamDryRun) -> Result<bool, StoreError>,
    ) -> Result<SeamRun, StoreError> {
        if vehicle_id.is_nil() || car_id <= 0 {
            return Err(StoreError::InvalidImportGeneration);
        }
        let mut previous_id = 0_i64;
        for drive in drives {
            if drive.id <= previous_id
                || drive.id >= crate::db::hub_ids::HUB_ID_BASE
                || drive.car_id != car_id
            {
                return Err(StoreError::InvalidImportGeneration);
            }
            previous_id = drive.id;
        }
        validate_import_seam_charges(charges, car_id)?;
        for _ in 0..SEAM_SNAPSHOT_ATTEMPTS {
            if let Some(done) = self.import_seam_ahead_once(
                TransactionBehavior::Deferred,
                vehicle_id,
                drives,
                charges,
                stored_rows,
                &mut commit,
            )? {
                return Ok(done);
            }
        }
        // Every snapshot went stale: the last resort holds the write lock
        // from its first read, so no other commit can intervene.
        self.import_seam_ahead_once(
            TransactionBehavior::Immediate,
            vehicle_id,
            drives,
            charges,
            stored_rows,
            &mut commit,
        )?
        .ok_or(StoreError::ImportGenerationConflict)
    }

    /// One attempt of [`HubStore::import_seam_ahead_of_packs`] in a
    /// transaction of `behavior`; `None` when its snapshot went stale before
    /// an accepted plan could write (never under IMMEDIATE).
    fn import_seam_ahead_once(
        &self,
        behavior: TransactionBehavior,
        vehicle_id: Uuid,
        drives: &[ProjectionDrive],
        charges: &[crate::hub_pack::ProjectionCharge],
        stored_rows: bool,
        commit: &mut impl FnMut(&Transaction<'_>, &[SyncMutation], &ImportSeamDryRun) -> Result<bool, StoreError>,
    ) -> Result<Option<SeamRun>, StoreError> {
        let vehicle_key = vehicle_id.to_string();
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(behavior)
            .map_err(StoreError::Begin)?;
        let seam_history = import_seam_history_in_connection(&transaction, &vehicle_key)?;
        let prior = durable_prior_imported_drive_ids_in_transaction(&transaction, &vehicle_key)?;
        let (coverage, before) =
            import_seam_plan_in_transaction(&transaction, vehicle_id, drives, charges, &prior)?;
        #[cfg(test)]
        run_import_seam_dry_run_hook(SeamDryRunStage::Planned);
        let changed_the_store = before.changes_the_store();
        let packs = crate::import::source_db::seam::SeamPackPlan::for_plan(&coverage, &before);
        let effects = if changed_the_store {
            seam_effects_in_transaction(&transaction, vehicle_id, &before)?
        } else {
            SeamEffects::default()
        };
        let restated_drives = restated_import_drives_in_transaction(
            &transaction,
            &vehicle_key,
            drives,
            &packs,
            &effects.drives,
        )?;
        let restated_charges = restated_import_charges(&before, &packs);
        let waiting: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM sync_mutations WHERE vehicle_id = ?1 AND published = 0",
                params![vehicle_key.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Query)?;
        let unpublished = u64::try_from(waiting)
            .ok()
            .and_then(|waiting| waiting.checked_add(u64::try_from(effects.journal.len()).ok()?))
            .ok_or(StoreError::InvalidStoredSequence)?;
        let stored = if stored_rows {
            Some(import_seam_stored_rows_in_transaction(&transaction, vehicle_id, &effects)?)
        } else {
            None
        };
        let run = ImportSeamDryRun {
            ahead: ImportSeamAhead {
                packs,
                restated_drives,
                restated_charges,
                changed_the_store,
            },
            seam_history,
            unpublished,
        };
        #[cfg(test)]
        run_import_seam_dry_run_hook(SeamDryRunStage::Simulated);
        if !commit(&transaction, &effects.journal, &run)? {
            transaction.rollback().map_err(StoreError::LifecycleWrite)?;
            return Ok(Some(SeamRun {
                run,
                committed: false,
                stored,
            }));
        }
        if changed_the_store {
            if !upgrade_seam_snapshot_to_write(&transaction)? {
                return Ok(None);
            }
            #[cfg(test)]
            run_import_seam_dry_run_hook(SeamDryRunStage::Upgraded);
            apply_import_seam_hub_side_in_transaction(&transaction, vehicle_id, &before)?;
            apply_import_seam_to_charges_in_transaction(
                &transaction,
                vehicle_id,
                &before,
                charges,
                false,
            )?;
            // What was applied is what the plan was accepted for: the same
            // journal, so the same rows.
            if seam_journal_in_transaction(&transaction, vehicle_id, &effects)? != effects.journal {
                return Err(StoreError::ImportGenerationConflict);
            }
            // Planned again, the applied store must give the packs the
            // same decisions, or publication could serve a seam the applied
            // store would refuse: the transaction rolls back (dropped
            // uncommitted) and the next run plans again.
            let (coverage_after, after) =
                import_seam_plan_in_transaction(&transaction, vehicle_id, drives, charges, &prior)?;
            if crate::import::source_db::seam::SeamPackPlan::for_plan(&coverage_after, &after)
                .digest()
                != run.ahead.packs.digest()
            {
                return Err(StoreError::ImportGenerationConflict);
            }
        }
        transaction.commit().map_err(StoreError::LifecycleWrite)?;
        Ok(Some(SeamRun {
            run,
            committed: true,
            stored,
        }))
    }

    /// Refuses ([`StoreError::ImportGenerationConflict`]) a publication path
    /// that does not plan the seam while the vehicle has any of it: coverage,
    /// a Hub-range row or a recorded seam decision.
    pub(crate) fn refuse_unplanned_import_seam(&self, vehicle_id: Uuid) -> Result<(), StoreError> {
        let connection = self.open()?;
        refuse_unplanned_import_seam_in_transaction(&connection, &vehicle_id.to_string())
    }
}

/// What one seam run found: the plan with what a routing decision rests on,
/// whether it was applied, and the stored rows the applied store holds when
/// they were asked for.
#[derive(Debug)]
struct SeamRun {
    run: ImportSeamDryRun,
    committed: bool,
    stored: Option<ImportSeamStoredRows>,
}

/// How often a seam run plans again after its snapshot went stale before
/// an accepted plan could write.
const SEAM_SNAPSHOT_ATTEMPTS: usize = 5;

/// Takes the write lock a seam run needs to apply an accepted plan. `false`
/// when another writer committed since the snapshot began (the snapshot is
/// stale, and the whole run is repeated).
fn upgrade_seam_snapshot_to_write(transaction: &Transaction<'_>) -> Result<bool, StoreError> {
    match transaction.execute("DELETE FROM hub_metadata WHERE 0", []) {
        Ok(_) => Ok(true),
        Err(rusqlite::Error::SqliteFailure(error, _))
            if error.code == rusqlite::ErrorCode::DatabaseBusy =>
        {
            Ok(false)
        }
        Err(error) => Err(StoreError::Query(error)),
    }
}

/// Rows of one materialised table a seam plan changes, by id: `Some` with
/// the row's car (a charge sample's: its charging process) and JSON after
/// the change, `None` for a removed row.
type SeamRowChanges = HashMap<i64, Option<(Option<i64>, String)>>;

/// What applying a seam plan changes (PG-2), worked out from the snapshot
/// the plan was made from without writing, statement for statement as
/// [`apply_import_seam_hub_side_in_transaction`] and
/// [`apply_import_seam_to_charges_in_transaction`] (without source copies)
/// change it: the rows of the four materialised tables, and the sync
/// mutations journalled, in revision order. Applying the plan must journal
/// exactly these; every changed row journals its new JSON or a tombstone.
#[derive(Debug, Default)]
struct SeamEffects {
    drives: SeamRowChanges,
    positions: SeamRowChanges,
    charges: SeamRowChanges,
    charge_samples: SeamRowChanges,
    vehicle_id: Uuid,
    /// The revision the first journalled mutation takes.
    first_revision: i64,
    journal: Vec<SyncMutation>,
}

impl SeamEffects {
    /// Journals one mutation at the next revision, as the apply does.
    fn record(&mut self, entity: &str, entity_id: i64, car_id: i64, operation: &str, payload_json: String) {
        let revision = self.first_revision + i64::try_from(self.journal.len()).unwrap_or(i64::MAX);
        self.journal.push(SyncMutation {
            vehicle_id: self.vehicle_id,
            revision,
            entity: entity.to_owned(),
            entity_id,
            car_id,
            operation: operation.to_owned(),
            payload_json,
        });
    }
}

/// One stored row as a seam plan being worked out sees it: its change when
/// it has one, the snapshot's row otherwise. `sql` selects the car (or a
/// sample's charge) and the JSON by vehicle and id.
fn seam_row(
    connection: &Connection,
    changes: &SeamRowChanges,
    sql: &str,
    vehicle_key: &str,
    id: i64,
) -> Result<Option<(Option<i64>, String)>, StoreError> {
    if let Some(changed) = changes.get(&id) {
        return Ok(changed.clone());
    }
    connection
        .query_row(sql, params![vehicle_key, id], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()
        .map_err(StoreError::Query)
}

const SEAM_DRIVE_ROW: &str =
    "SELECT car_id, drive_json FROM materialised_drives WHERE vehicle_id = ?1 AND drive_id = ?2";
const SEAM_POSITION_ROW: &str = "SELECT car_id, position_json FROM materialised_positions
  WHERE vehicle_id = ?1 AND position_id = ?2";
const SEAM_CHARGE_ROW: &str =
    "SELECT car_id, charge_json FROM materialised_charges WHERE vehicle_id = ?1 AND charge_id = ?2";
const SEAM_SAMPLE_ROW: &str = "SELECT charge_id, sample_json FROM materialised_charge_samples
  WHERE vehicle_id = ?1 AND sample_id = ?2";

/// A stored car, as the apply reads it: a row without one is refused.
fn seam_car(row: Option<(Option<i64>, String)>) -> Result<Option<(i64, String)>, StoreError> {
    match row {
        None => Ok(None),
        Some((Some(car_id), json)) => Ok(Some((car_id, json))),
        Some((None, _)) => Err(StoreError::InvalidLifecycleSession),
    }
}

/// `json` with `field` set to `value` (and `end_date_ms` when given), as the
/// apply rewrites a stored row.
fn seam_rewritten(json: &str, fields: &[(&str, i64)]) -> Result<String, StoreError> {
    let mut value: Value =
        serde_json::from_str(json).map_err(|_| StoreError::InvalidLifecycleSession)?;
    let Some(object) = value.as_object_mut() else {
        return Err(StoreError::InvalidLifecycleSession);
    };
    for (field, to) in fields {
        object.insert((*field).into(), Value::from(*to));
    }
    Ok(value.to_string())
}

/// Works out [`SeamEffects`] of `plan` from the transaction's snapshot.
fn seam_effects_in_transaction(
    connection: &Connection,
    vehicle_id: Uuid,
    plan: &crate::import::source_db::seam::SeamPlan,
) -> Result<SeamEffects, StoreError> {
    use crate::db::hub_ids::is_hub_id;
    let vehicle_key = vehicle_id.to_string();
    let key = vehicle_key.as_str();
    let first_revision: Option<i64> = connection
        .query_row(
            "SELECT next_revision FROM sync_mutation_sequences WHERE vehicle_id = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    let mut effects = SeamEffects {
        vehicle_id,
        first_revision: first_revision.unwrap_or(1),
        ..SeamEffects::default()
    };

    // The Hub's side: drives that lost or whose source went, kept Hub drives
    // widened, Hub positions outside coverage removed, kept ones moved.
    let drive_plan = &plan.drives;
    let gone = drive_plan
        .superseded
        .iter()
        .map(|link| (link.loser_id, false))
        .chain(drive_plan.skipped.iter().map(|link| (link.loser_id, true)))
        .chain(drive_plan.stale.iter().map(|id| (*id, true)));
    for (id, source) in gone {
        if source && is_hub_id(id) {
            return Err(StoreError::InvalidImportGeneration);
        }
        if let Some((car_id, _)) = seam_car(seam_row(connection, &effects.drives, SEAM_DRIVE_ROW, key, id)?)? {
            effects.drives.insert(id, None);
            effects.record("drive", id, car_id, "tombstone", "{}".into());
        }
    }
    for bounds in &drive_plan.widened {
        if drive_plan.hub_kept.binary_search(&bounds.id).is_err() {
            continue;
        }
        let Some((car_id, json)) =
            seam_car(seam_row(connection, &effects.drives, SEAM_DRIVE_ROW, key, bounds.id)?)?
        else {
            continue;
        };
        let mut fields = vec![("start_date_ms", bounds.start_ms)];
        fields.extend(bounds.end_ms.map(|end_ms| ("end_date_ms", end_ms)));
        let widened = seam_rewritten(&json, &fields)?;
        if widened == json {
            continue;
        }
        effects.drives.insert(bounds.id, Some((Some(car_id), widened.clone())));
        effects.record("drive", bounds.id, car_id, "upsert", widened);
    }
    for id in &plan.positions.hub_removed {
        if let Some((car_id, _)) =
            seam_car(seam_row(connection, &effects.positions, SEAM_POSITION_ROW, key, *id)?)?
        {
            effects.positions.insert(*id, None);
            effects.record("position", *id, car_id, "tombstone", "{}".into());
        }
    }
    for moved in &plan.positions.moved_hub {
        let Some((car_id, json)) =
            seam_car(seam_row(connection, &effects.positions, SEAM_POSITION_ROW, key, moved.id)?)?
        else {
            continue;
        };
        let moved_json = seam_rewritten(&json, &[("drive_id", moved.to)])?;
        effects.positions.insert(moved.id, Some((Some(car_id), moved_json.clone())));
        effects.record("position", moved.id, car_id, "upsert", moved_json);
    }

    // Charges and their samples, the same way.
    let charge_plan = &plan.charges;
    let gone = charge_plan
        .superseded
        .iter()
        .map(|link| (link.loser_id, false))
        .chain(charge_plan.skipped.iter().map(|link| (link.loser_id, true)))
        .chain(charge_plan.stale.iter().map(|id| (*id, true)));
    for (id, source) in gone {
        if source && is_hub_id(id) {
            return Err(StoreError::InvalidImportGeneration);
        }
        if let Some((car_id, _)) = seam_car(seam_row(connection, &effects.charges, SEAM_CHARGE_ROW, key, id)?)? {
            effects.charges.insert(id, None);
            effects.record("charge", id, car_id, "tombstone", "{}".into());
        }
    }
    let widened: HashMap<i64, (i64, Option<i64>)> = charge_plan
        .widened
        .iter()
        .map(|bounds| (bounds.id, (bounds.start_ms, bounds.end_ms)))
        .collect();
    for id in &charge_plan.hub_kept {
        let Some((start_ms, end_ms)) = widened.get(id).copied() else {
            continue;
        };
        let Some((car_id, json)) =
            seam_car(seam_row(connection, &effects.charges, SEAM_CHARGE_ROW, key, *id)?)?
        else {
            continue;
        };
        let mut fields = vec![("start_date_ms", start_ms)];
        fields.extend(end_ms.map(|end_ms| ("end_date_ms", end_ms)));
        let widened_json = seam_rewritten(&json, &fields)?;
        if widened_json == json {
            continue;
        }
        effects.charges.insert(*id, Some((Some(car_id), widened_json.clone())));
        effects.record("charge", *id, car_id, "upsert", widened_json);
    }
    // A sample's car is its charging process's, else the vehicle's car.
    let sample_car = |effects: &SeamEffects, id: i64| -> Result<Option<(i64, String)>, StoreError> {
        let Some((charge_id, json)) =
            seam_row(connection, &effects.charge_samples, SEAM_SAMPLE_ROW, key, id)?
        else {
            return Ok(None);
        };
        let charge_car = match charge_id {
            Some(charge_id) => seam_row(connection, &effects.charges, SEAM_CHARGE_ROW, key, charge_id)?
                .and_then(|(car_id, _)| car_id),
            None => None,
        };
        let car_id = match charge_car {
            Some(car_id) => Some(car_id),
            None => connection
                .query_row(
                    "SELECT (SELECT car_id FROM materialised_cars WHERE vehicle_id = ?1)",
                    params![key],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .map_err(StoreError::Query)?,
        };
        Ok(car_id.map(|car_id| (car_id, json)))
    };
    for id in &plan.charge_samples.hub_removed {
        let Some((car_id, _)) = sample_car(&effects, *id)? else {
            continue;
        };
        effects.charge_samples.insert(*id, None);
        effects.record("charge_sample", *id, car_id, "tombstone", "{}".into());
    }
    for moved in &plan.charge_samples.moved_hub {
        let Some((car_id, json)) = sample_car(&effects, moved.id)? else {
            continue;
        };
        let moved_json = seam_rewritten(&json, &[("charge_process_id", moved.to)])?;
        effects.charge_samples.insert(moved.id, Some((Some(moved.to), moved_json.clone())));
        effects.record("charge_sample", moved.id, car_id, "upsert", moved_json);
    }
    Ok(effects)
}

/// The sync mutations an applied seam journalled: the vehicle's from the
/// revision the plan's effects begin at, in revision order.
fn seam_journal_in_transaction(
    connection: &Connection,
    vehicle_id: Uuid,
    effects: &SeamEffects,
) -> Result<Vec<SyncMutation>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT revision, entity, entity_id, car_id, operation, payload_json
               FROM sync_mutations WHERE vehicle_id = ?1 AND revision >= ?2
              ORDER BY revision",
        )
        .map_err(StoreError::Query)?;
    statement
        .query_map(params![vehicle_id.to_string(), effects.first_revision], |row| {
            Ok(SyncMutation {
                vehicle_id,
                revision: row.get(0)?,
                entity: row.get(1)?,
                entity_id: row.get(2)?,
                car_id: row.get(3)?,
                operation: row.get(4)?,
                payload_json: row.get(5)?,
            })
        })
        .map_err(StoreError::Query)?
        .collect::<Result<_, _>>()
        .map_err(StoreError::Query)
}

/// Where a seam run is when a test hook runs.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeamDryRunStage {
    /// The plan has been read, no write has been attempted.
    Planned,
    /// Everything the run returns has been worked out, still without a
    /// write; `commit` decides next.
    Simulated,
    /// An accepted plan changes the store and the write lock is held.
    Upgraded,
}

#[cfg(test)]
type SeamDryRunHook = Box<dyn FnMut(SeamDryRunStage)>;

#[cfg(test)]
thread_local! {
    static IMPORT_SEAM_DRY_RUN_HOOK: std::cell::RefCell<Option<SeamDryRunHook>> =
        std::cell::RefCell::new(None);
}

/// Test seam: run `hook` on this thread at each stage of a seam run.
#[cfg(test)]
pub(crate) fn set_import_seam_dry_run_hook(hook: Option<SeamDryRunHook>) {
    IMPORT_SEAM_DRY_RUN_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(test)]
fn run_import_seam_dry_run_hook(stage: SeamDryRunStage) {
    // Taken out while it runs, so the hook may use the store.
    if let Some(mut hook) = IMPORT_SEAM_DRY_RUN_HOOK.with(|slot| slot.borrow_mut().take()) {
        hook(stage);
        IMPORT_SEAM_DRY_RUN_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(hook);
            }
        });
    }
}

/// The source drive ids the prior import listed, from its durable state.
fn durable_prior_imported_drive_ids_in_transaction(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<HashSet<i64>, StoreError> {
    durable_prior_imported_ids_in_connection(
        connection,
        vehicle_key,
        SourceDbProjectionStateEntity::Drive,
    )
}

/// The source ids of one entity the prior import listed, from its durable
/// state.
fn durable_prior_imported_ids_in_connection(
    connection: &Connection,
    vehicle_key: &str,
    entity: SourceDbProjectionStateEntity,
) -> Result<HashSet<i64>, StoreError> {
    let mut statement = connection
        .prepare(concat!(
            "SELECT entity_id
               FROM ",
            crate::db::import_table_names::import_projection_state_rows_table!(),
            "
              WHERE vehicle_id = ?1 AND entity_ordinal = ?2
              ORDER BY entity_id ASC"
        ))
        .map_err(StoreError::LineageCatalog)?;
    let rows = statement
        .query_map(
            params![vehicle_key, i64::from(entity.ordinal())],
            |row| row.get(0),
        )
        .map_err(StoreError::LineageCatalog)?;
    rows.collect::<Result<HashSet<i64>, _>>()
        .map_err(StoreError::LineageCatalog)
}

/// Plans the import seam for one vehicle's drives, positions, charges and
/// charge samples from the store, as publication applies it (design
/// M03-SEAM-R3, section 4): the coverage, the store's side of the drives as
/// [`import_seam_hub_drives_in_connection`] reads it, the stored charges and
/// Hub-range samples, with the open charge the Hub owns and the inherited
/// marks (see `import_seam_hub_charges_in_connection`), the source
/// drives and charges and the recorded links. Reads only. Both the planner
/// ahead of the packs and publication plan through this one reader.
fn import_seam_plan_in_transaction(
    connection: &Connection,
    vehicle_id: Uuid,
    drives: &[ProjectionDrive],
    charges: &[crate::hub_pack::ProjectionCharge],
    prior_imported_drive_ids: &HashSet<i64>,
) -> Result<
    (
        crate::import::source_db::seam::HubCoverage,
        crate::import::source_db::seam::SeamPlan,
    ),
    StoreError,
> {
    use crate::import::source_db::seam::{
        HubCoverage, SeamLinks, SessionRow, SourceRows, plan_seam,
    };

    let vehicle_key = vehicle_id.to_string();
    let coverage = HubCoverage::for_store(connection, &vehicle_key)
        .map_err(|_| StoreError::ImportGenerationConflict)?;
    let mut hub = import_seam_hub_drives_in_connection(
        connection,
        &vehicle_key,
        &coverage,
        prior_imported_drive_ids,
    )?
    .rows;
    let prior_imported_charge_ids = durable_prior_imported_ids_in_connection(
        connection,
        &vehicle_key,
        SourceDbProjectionStateEntity::Charge,
    )?;
    import_seam_hub_charges_in_connection(
        connection,
        &vehicle_key,
        &coverage,
        &prior_imported_charge_ids,
        &mut hub,
    )?;
    let source = SourceRows {
        drives: drives
            .iter()
            .map(|drive| SessionRow {
                id: drive.id,
                start_ms: drive.start_date_ms,
                end_ms: Some(drive.end_date_ms),
            })
            .collect(),
        charges: import_seam_source_charges(charges),
        ..SourceRows::default()
    };
    let links = SeamLinks {
        drives: seam_links_in_connection(connection, &vehicle_key, "drive")?,
        charges: seam_links_in_connection(connection, &vehicle_key, "charge")?,
    };
    let plan = plan_seam(&coverage, &hub, &source, &links);
    Ok((coverage, plan))
}

/// The lifecycle's open drive when the Hub owns it (M03-T18): a drive still
/// open in the lifecycle state whose id is in the Hub's range or that holds a
/// Hub-range position in `lifecycle_open_rows`. It weighs as a Hub session
/// from its start to its last evidence (its latest fold or open position),
/// with its Hub-range open positions as its children.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SeamOpenDrive {
    session: crate::import::source_db::seam::SessionRow,
    /// Hub-range open positions: id to instant.
    positions: std::collections::BTreeMap<i64, i64>,
}

/// The store's side of the drive seam of one vehicle (M03-T18).
struct SeamHubDrives {
    /// What the plan weighs: the Hub's drives (Hub-range, inherited, and the
    /// open drive the Hub owns), the Hub-range positions (stored and open) and
    /// the source drives earlier imports stored.
    rows: crate::import::source_db::seam::HubRows,
    /// Every stored drive with a start.
    stored: std::collections::BTreeMap<i64, StoredSeamDrive>,
    /// The stored Hub-range positions.
    hub_positions: std::collections::BTreeMap<i64, StoredSeamPosition>,
    /// The open drive taking part in the seam, if any.
    open: Option<SeamOpenDrive>,
    /// The drive ids weighed on the Hub's side.
    hub_side: HashSet<i64>,
    /// The drives a stored Hub-range position belongs to.
    held_by_hub_position: HashSet<i64>,
}

/// Reads the store's side of the drive seam of one vehicle, for the planner
/// ahead of the packs and for publication alike (M03-T18).
///
/// A stored drive is the Hub's when its id is in the Hub's range, or it is an
/// inherited session (one id on both sides): the prior import did not list it,
/// a Hub-range position belongs to it, or a recorded self-link says the seam
/// kept its stored copy. Every other stored drive is an earlier import's. When
/// the vehicle has coverage, the lifecycle's open drive the Hub owns takes part
/// too ([`SeamOpenDrive`]), so the positions of a drive still open when the
/// window was handed back are weighed like any other Hub position.
fn import_seam_hub_drives_in_connection(
    connection: &Connection,
    vehicle_key: &str,
    coverage: &crate::import::source_db::seam::HubCoverage,
    prior_imported_drive_ids: &HashSet<i64>,
) -> Result<SeamHubDrives, StoreError> {
    use crate::db::hub_ids::{HUB_ID_BASE, HUB_ID_MAX, is_hub_id};
    use crate::import::source_db::seam::{ChildRow, HubRows, SessionRow};

    let stored: std::collections::BTreeMap<i64, StoredSeamDrive> = {
        let mut statement = connection
            .prepare(
                "SELECT drive_id, car_id,
                        CAST(json_extract(drive_json, '$.start_date_ms') AS INTEGER),
                        CAST(json_extract(drive_json, '$.end_date_ms') AS INTEGER)
                   FROM materialised_drives WHERE vehicle_id = ?1 ORDER BY drive_id ASC",
            )
            .map_err(StoreError::Query)?;
        let rows = statement
            .query_map(params![vehicle_key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(StoreError::Query)?;
        let mut stored = std::collections::BTreeMap::new();
        for row in rows {
            let (id, car_id, start_ms, end_ms) = row.map_err(StoreError::Query)?;
            // A stored drive without a start cannot be weighed; it stays as
            // it is and is never planned away.
            let Some(start_ms) = start_ms else { continue };
            stored.insert(
                id,
                StoredSeamDrive {
                    car_id,
                    start_ms,
                    end_ms,
                },
            );
        }
        stored
    };
    let hub_positions: std::collections::BTreeMap<i64, StoredSeamPosition> = {
        let mut statement = connection
            .prepare(
                "SELECT position_id, car_id, drive_id,
                        CAST(json_extract(position_json, '$.date_ms') AS INTEGER)
                   FROM materialised_positions
                  WHERE vehicle_id = ?1 AND position_id BETWEEN ?2 AND ?3
                  ORDER BY position_id ASC",
            )
            .map_err(StoreError::Query)?;
        let rows = statement
            .query_map(params![vehicle_key, HUB_ID_BASE, HUB_ID_MAX], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(StoreError::Query)?;
        let mut positions = std::collections::BTreeMap::new();
        for row in rows {
            let (id, car_id, drive_id, at_ms) = row.map_err(StoreError::Query)?;
            // A Hub position without an instant cannot be placed; it stays.
            let Some(at_ms) = at_ms else { continue };
            positions.insert(
                id,
                StoredSeamPosition {
                    car_id,
                    at_ms,
                    drive_id,
                },
            );
        }
        positions
    };
    let marked = seam_inherited_in_connection(connection, vehicle_key, "drive")?;
    let open = if coverage.intervals().is_empty() {
        None
    } else {
        import_seam_open_drive_in_connection(connection, vehicle_key)?
            .filter(|open| !stored.contains_key(&open.session.id))
    };

    let held_by_hub_position: HashSet<i64> = hub_positions
        .values()
        .filter_map(|position| position.drive_id)
        .collect();
    let mut rows = HubRows::default();
    let mut hub_side = HashSet::new();
    for (id, drive) in &stored {
        let inherited = !prior_imported_drive_ids.contains(id)
            || held_by_hub_position.contains(id)
            || marked.contains(id);
        if is_hub_id(*id) || inherited {
            rows.drives.push(SessionRow {
                id: *id,
                start_ms: drive.start_ms,
                end_ms: drive.end_ms,
            });
            hub_side.insert(*id);
        } else {
            rows.imported_drives.push(*id);
        }
    }
    rows.positions = hub_positions
        .iter()
        .map(|(id, position)| ChildRow {
            id: *id,
            at_ms: position.at_ms,
            parent: position.drive_id,
        })
        .collect();
    if let Some(open) = &open {
        rows.drives.push(open.session);
        hub_side.insert(open.session.id);
        rows.positions
            .extend(open.positions.iter().map(|(id, at_ms)| ChildRow {
                id: *id,
                at_ms: *at_ms,
                parent: Some(open.session.id),
            }));
    }
    Ok(SeamHubDrives {
        rows,
        stored,
        hub_positions,
        open,
        hub_side,
        held_by_hub_position,
    })
}

/// The lifecycle's open drive of one vehicle when the Hub owns it (see
/// [`SeamOpenDrive`]), whatever the coverage. Reads only.
fn import_seam_open_drive_in_connection(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<Option<SeamOpenDrive>, StoreError> {
    use crate::db::hub_ids::{HUB_ID_BASE, HUB_ID_MAX, is_hub_id};

    let state: Option<Vec<u8>> = connection
        .query_row(
            "SELECT open_session_json FROM vehicle_lifecycle_state WHERE vehicle_id = ?1",
            params![vehicle_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    let Some(state) = state else {
        return Ok(None);
    };
    let state = crate::lifecycle::OpenSessionState::decode(&state)
        .map_err(|_| StoreError::InvalidLifecycleSession)?;
    let Some(open) = state.open_drive else {
        return Ok(None);
    };
    let mut positions = std::collections::BTreeMap::new();
    {
        let mut statement = connection
            .prepare(
                "SELECT source_row_id, CAST(json_extract(row_json, '$.date_ms') AS INTEGER)
                   FROM lifecycle_open_rows
                  WHERE vehicle_id = ?1 AND domain = 'position' AND parent_source_row_id = ?2
                    AND source_row_id BETWEEN ?3 AND ?4
                  ORDER BY source_row_id ASC",
            )
            .map_err(StoreError::Query)?;
        let rows = statement
            .query_map(
                params![vehicle_key, open.id, HUB_ID_BASE, HUB_ID_MAX],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .map_err(StoreError::Query)?;
        for row in rows {
            let (id, at_ms) = row.map_err(StoreError::Query)?;
            // An open position without an instant cannot be placed; it stays.
            if let Some(at_ms) = at_ms {
                positions.insert(id, at_ms);
            }
        }
    }
    if !is_hub_id(open.id) && positions.is_empty() {
        return Ok(None);
    }
    let end_ms = positions
        .values()
        .copied()
        .chain([open.effective_last_folded_ms()])
        .max()
        .unwrap_or(open.start_date_ms)
        .max(open.start_date_ms);
    Ok(Some(SeamOpenDrive {
        session: crate::import::source_db::seam::SessionRow {
            id: open.id,
            start_ms: open.start_date_ms,
            end_ms: Some(end_ms),
        },
        positions,
    }))
}

/// The id of the vehicle's live open drive: the lifecycle's open drive the
/// Hub owns, as the seam weighs it (see [`SeamOpenDrive`]), so a drive
/// inherited from an import counts once the live collector extends it,
/// whatever its id range. A window's drive grace qualifies only on one.
pub(crate) fn live_open_drive_in_connection(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<Option<i64>, StoreError> {
    Ok(import_seam_open_drive_in_connection(connection, vehicle_key)?.map(|open| open.session.id))
}

/// The lifecycle's open charge when the Hub owns it (M03-T18, second part):
/// a charge still open in the lifecycle state whose id is in the Hub's range
/// or that holds a Hub-range sample in `lifecycle_open_rows`. It weighs as a
/// Hub session from its start to its latest sample, with its Hub-range open
/// samples as its children.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SeamOpenCharge {
    session: crate::import::source_db::seam::SessionRow,
    /// Hub-range open samples: id to instant.
    samples: std::collections::BTreeMap<i64, i64>,
}

/// The lifecycle's open charge as the seam weighs it (M03-T18, second part):
/// when the vehicle has coverage, the Hub owns the charge (see
/// [`SeamOpenCharge`]) and the store holds no charge under its id. The charge
/// reader of both planners and publication's settling read it here, so they
/// see the same charge. Reads only.
fn import_seam_open_charge_in_connection(
    connection: &Connection,
    vehicle_key: &str,
    coverage: &crate::import::source_db::seam::HubCoverage,
) -> Result<Option<SeamOpenCharge>, StoreError> {
    if coverage.intervals().is_empty() {
        return Ok(None);
    }
    let Some(open) = import_seam_owned_open_charge_in_connection(connection, vehicle_key)? else {
        return Ok(None);
    };
    let stored: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM materialised_charges
                            WHERE vehicle_id = ?1 AND charge_id = ?2)",
            params![vehicle_key, open.session.id],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)?;
    Ok((!stored).then_some(open))
}

/// The lifecycle's open charge of one vehicle when the Hub owns it (see
/// [`SeamOpenCharge`]), whatever the coverage. Reads only.
fn import_seam_owned_open_charge_in_connection(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<Option<SeamOpenCharge>, StoreError> {
    use crate::db::hub_ids::{HUB_ID_BASE, HUB_ID_MAX, is_hub_id};

    let state: Option<Vec<u8>> = connection
        .query_row(
            "SELECT open_session_json FROM vehicle_lifecycle_state WHERE vehicle_id = ?1",
            params![vehicle_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Query)?;
    let Some(state) = state else {
        return Ok(None);
    };
    let state = crate::lifecycle::OpenSessionState::decode(&state)
        .map_err(|_| StoreError::InvalidLifecycleSession)?;
    let Some(open) = state.open_charge else {
        return Ok(None);
    };
    let mut samples = std::collections::BTreeMap::new();
    {
        let mut statement = connection
            .prepare(
                "SELECT source_row_id, CAST(json_extract(row_json, '$.timestamp_ms') AS INTEGER)
                   FROM lifecycle_open_rows
                  WHERE vehicle_id = ?1 AND domain = 'charge_sample' AND parent_source_row_id = ?2
                    AND source_row_id BETWEEN ?3 AND ?4
                  ORDER BY source_row_id ASC",
            )
            .map_err(StoreError::Query)?;
        let rows = statement
            .query_map(
                params![vehicle_key, open.id, HUB_ID_BASE, HUB_ID_MAX],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .map_err(StoreError::Query)?;
        for row in rows {
            let (id, at_ms) = row.map_err(StoreError::Query)?;
            // An open sample without an instant cannot be placed; it stays.
            if let Some(at_ms) = at_ms {
                samples.insert(id, at_ms);
            }
        }
    }
    if !is_hub_id(open.id) && samples.is_empty() {
        return Ok(None);
    }
    let end_ms = samples
        .values()
        .copied()
        .chain(open.last_sample_timestamp_ms)
        .max()
        .unwrap_or(open.start_date_ms)
        .max(open.start_date_ms);
    Ok(Some(SeamOpenCharge {
        session: crate::import::source_db::seam::SessionRow {
            id: open.id,
            start_ms: open.start_date_ms,
            end_ms: Some(end_ms),
        },
        samples,
    }))
}

/// The recorded seam decisions of one vehicle and entity, read outside a
/// publication transaction.
fn seam_links_in_connection(
    connection: &Connection,
    vehicle_key: &str,
    entity: &str,
) -> Result<std::collections::BTreeMap<i64, i64>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT loser_id, winner_id FROM import_seam_links
              WHERE vehicle_id = ?1 AND entity = ?2 ORDER BY loser_id ASC",
        )
        .map_err(StoreError::Query)?;
    let rows = statement
        .query_map(params![vehicle_key, entity], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(StoreError::Query)?;
    rows.collect::<Result<_, _>>().map_err(StoreError::Query)
}

/// Applies everything of a drive and position plan except writing the
/// winning source drives (see [`HubStore::apply_import_seam_ahead_of_packs`]).
fn apply_import_seam_hub_side_in_transaction(
    transaction: &Transaction<'_>,
    vehicle_id: Uuid,
    plan: &crate::import::source_db::seam::SeamPlan,
) -> Result<(), StoreError> {
    use crate::db::hub_ids::is_hub_id;
    let vehicle_key = vehicle_id.to_string();
    let stored_car = |table: &str, column: &str, id: i64| -> Result<Option<i64>, StoreError> {
        transaction
            .query_row(
                &format!("SELECT car_id FROM {table} WHERE vehicle_id = ?1 AND {column} = ?2"),
                params![vehicle_key.as_str(), id],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Query)
    };
    let drive_plan = &plan.drives;
    // Hub drives that lost, then an earlier import's copy of a skipped or
    // stale source drive: removed with a tombstone. Their positions follow
    // the instant rule below, never their drive.
    for link in &drive_plan.superseded {
        if let Some(car_id) = stored_car("materialised_drives", "drive_id", link.loser_id)? {
            remove_seam_drive_in_transaction(transaction, vehicle_id, link.loser_id, car_id)?;
        }
    }
    let source_gone = drive_plan
        .skipped
        .iter()
        .map(|link| link.loser_id)
        .chain(drive_plan.stale.iter().copied());
    for id in source_gone {
        if is_hub_id(id) {
            return Err(StoreError::InvalidImportGeneration);
        }
        if let Some(car_id) = stored_car("materialised_drives", "drive_id", id)? {
            remove_seam_drive_in_transaction(transaction, vehicle_id, id, car_id)?;
        }
    }
    // Kept Hub sessions widen to hold the children they gained.
    for bounds in &drive_plan.widened {
        if drive_plan.hub_kept.binary_search(&bounds.id).is_ok() {
            widen_hub_drive_in_transaction(
                transaction,
                vehicle_id,
                bounds.id,
                bounds.start_ms,
                bounds.end_ms,
            )?;
        }
    }
    // Hub positions outside coverage go with a tombstone; kept ones whose
    // drive lost move to the winner.
    for id in &plan.positions.hub_removed {
        let Some(car_id) = stored_car("materialised_positions", "position_id", *id)? else {
            continue;
        };
        transaction
            .execute(
                "DELETE FROM materialised_positions WHERE vehicle_id = ?1 AND position_id = ?2",
                params![vehicle_key.as_str(), id],
            )
            .map_err(StoreError::LifecycleWrite)?;
        record_sync_mutation_in_transaction(
            transaction,
            vehicle_id,
            "position",
            *id,
            car_id,
            "tombstone",
            "{}",
        )?;
    }
    for moved in &plan.positions.moved_hub {
        move_hub_position_in_transaction(transaction, vehicle_id, moved.id, moved.to)?;
    }
    let decided: Vec<_> = drive_plan
        .skipped
        .iter()
        .chain(&drive_plan.superseded)
        .copied()
        .collect();
    record_seam_links_in_transaction(
        transaction,
        &vehicle_key,
        "drive",
        &decided,
        &drive_plan.links_dropped,
    )
}

/// The packed source drives whose stored row, once the plan's `changes` are
/// applied, differs from the row the plan writes (the source copy with any
/// widened bounds), ascending.
fn restated_import_drives_in_transaction(
    connection: &Connection,
    vehicle_key: &str,
    drives: &[ProjectionDrive],
    packs: &crate::import::source_db::seam::SeamPackPlan,
    changes: &SeamRowChanges,
) -> Result<Vec<i64>, StoreError> {
    use crate::import::source_db::seam::PackedSession;
    let mut restated = Vec::new();
    let mut statement = connection
        .prepare("SELECT drive_json FROM materialised_drives WHERE vehicle_id = ?1 AND drive_id = ?2")
        .map_err(StoreError::Query)?;
    for drive in drives {
        let Some(PackedSession::Keep { bounds }) = packs.drive(drive.id) else {
            continue;
        };
        let planned = planned_import_drive(drive, bounds);
        let planned = serde_json::to_string(&planned).map_err(StoreError::SerializeLifecycleRow)?;
        let stored: Option<String> = match changes.get(&drive.id) {
            Some(changed) => changed.as_ref().map(|(_, json)| json.clone()),
            None => statement
                .query_row(params![vehicle_key, drive.id], |row| row.get(0))
                .optional()
                .map_err(StoreError::Query)?,
        };
        if stored.as_deref() != Some(planned.as_str()) {
            restated.push(drive.id);
        }
    }
    Ok(restated)
}

/// The packed source charges the packs restate, ascending: those the plan
/// widened, and those readmitted because the link that skipped them went.
fn restated_import_charges(
    plan: &crate::import::source_db::seam::SeamPlan,
    packs: &crate::import::source_db::seam::SeamPackPlan,
) -> Vec<i64> {
    use crate::import::source_db::seam::PackedSession;
    packs
        .imported_charges()
        .filter(|id| {
            matches!(packs.charge(*id), Some(PackedSession::Keep { bounds: Some(_) }))
                || plan.charges.links_dropped.binary_search(id).is_ok()
        })
        .collect()
}

/// The row a packed source drive is written as: its source copy with the
/// bounds the plan widened it to.
pub(crate) fn planned_import_drive(
    drive: &ProjectionDrive,
    bounds: Option<(i64, Option<i64>)>,
) -> ProjectionDrive {
    let mut planned = drive.clone();
    if let Some((start_ms, end_ms)) = bounds {
        planned.start_date_ms = start_ms;
        planned.end_date_ms = end_ms.unwrap_or(planned.end_date_ms);
    }
    planned
}

/// Refuses a publication path that does not plan the seam while the vehicle
/// has any of it (design M03-SEAM-R3, section 4; review finding 9): coverage
/// in the store, a window still open, a Hub-range row, or a recorded seam
/// decision. Before the Hub has collected the vehicle, such a path is exact.
fn refuse_unplanned_import_seam_in_transaction(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<(), StoreError> {
    if import_seam_present_in_connection(connection, vehicle_key)? {
        return Err(StoreError::ImportGenerationConflict);
    }
    Ok(())
}

/// Whether the vehicle has seam history (DEC-103): any of the seam, or a
/// live delta span. Such a vehicle's import base is never replaced.
fn import_seam_history_in_connection(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<bool, StoreError> {
    if import_seam_present_in_connection(connection, vehicle_key)? {
        return Ok(true);
    }
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sync_live_delta_spans WHERE vehicle_id = ?1)",
            params![vehicle_key],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)
}

/// Whether the vehicle has any of the seam: coverage in the store, a window
/// still open, a Hub-range row, or a recorded seam decision.
fn import_seam_present_in_connection(
    connection: &Connection,
    vehicle_key: &str,
) -> Result<bool, StoreError> {
    use crate::db::hub_ids::{HUB_ID_BASE, HUB_ID_MAX};
    let covered = match crate::db::coverage_intervals_for_vehicle(connection, vehicle_key) {
        Ok(intervals) => !intervals.is_empty(),
        Err(_) => true,
    };
    let hub_rows: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM materialised_drives
                            WHERE vehicle_id = ?1 AND drive_id BETWEEN ?2 AND ?3)
                 OR EXISTS(SELECT 1 FROM materialised_positions
                            WHERE vehicle_id = ?1 AND position_id BETWEEN ?2 AND ?3)
                 OR EXISTS(SELECT 1 FROM materialised_charges
                            WHERE vehicle_id = ?1 AND charge_id BETWEEN ?2 AND ?3)
                 OR EXISTS(SELECT 1 FROM materialised_charge_samples
                            WHERE vehicle_id = ?1 AND sample_id BETWEEN ?2 AND ?3)
                 OR EXISTS(SELECT 1 FROM import_seam_links WHERE vehicle_id = ?1)",
            params![vehicle_key, HUB_ID_BASE, HUB_ID_MAX],
            |row| row.get(0),
        )
        .map_err(StoreError::Query)?;
    Ok(covered || hub_rows)
}

// The seam staged ahead of the packs and published with them (M03-T19).
include!("import_seam_publication.rs");
