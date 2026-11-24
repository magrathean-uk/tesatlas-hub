// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// Run after a complete PhysicalV3 admission while the importer still owns the
/// sealed stage and publication gate. Returning an error leaves history public;
/// the caller logs it and a later identical import may retry preparation.
pub(crate) fn publish_selected_month(
    store: &HubStore,
    cursor_key: &CursorKey,
    stage: &SourceDbStage,
    admission: &PendingPhysicalV3Admission,
    now_ms: i64,
) -> PreparedResult<()> {
    publish_selected_month_with_policy(
        store,
        cursor_key,
        stage,
        admission,
        now_ms,
        RDP_EVALUATION_LIMIT,
        &|path| File::open(path)?.sync_all(),
    )
}

pub(super) fn publish_selected_month_with_policy(
    store: &HubStore,
    cursor_key: &CursorKey,
    stage: &SourceDbStage,
    admission: &PendingPhysicalV3Admission,
    now_ms: i64,
    rdp_evaluation_limit: u64,
    sync_directory: &dyn Fn(&Path) -> io::Result<()>,
) -> PreparedResult<()> {
    let catalogue = store.open()?;
    prune_obsolete_prepared_receipts(&catalogue, cursor_key)?;
    // A replaced head remains available only while its PhysicalV3 rebase
    // receipt is retained. Prune catalogues at admission; pack repair can
    // reclaim the resulting orphaned content-addressed objects later.
    catalogue.execute(
        "DELETE FROM prepared_map_months AS p
         WHERE NOT EXISTS (
             SELECT 1 FROM pending_physical_v3_admissions AS h
             WHERE h.vehicle_id = p.vehicle_id
               AND h.receipt_id = p.input_receipt_id
               AND h.serve_state = 'public_first'
         ) AND NOT EXISTS (
             SELECT 1 FROM retained_physical_v3_admissions AS r
             WHERE r.vehicle_id = p.vehicle_id
               AND r.receipt_id = p.input_receipt_id
               AND r.expires_at_ms > ?1
         )",
        [now_ms],
    )?;
    drop(catalogue);
    let staging = scratch_dir(store)?;
    if let Some(month) = build_selected_month_with_limit(
        store,
        cursor_key,
        stage,
        admission,
        now_ms,
        &staging,
        rdp_evaluation_limit,
    )? {
        month.publish_with_sync(store, admission, sync_directory)?;
    }
    Ok(())
}

pub(crate) struct PreparedMonth {
    contents: PreparedMonthContents,
}

enum PreparedMonthContents {
    Cached(File),
    New {
        id: String,
        month: Month,
        source_digest: String,
        pack: NamedTempFile,
        digest: String,
        compressed_bytes: u64,
        receipt: Vec<u8>,
    },
}

#[cfg(test)]
thread_local! {
    pub(crate) static ON_MONTH_BUILD: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    pub(crate) static ON_MONTH_FINAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn build_selected_month(
    store: &HubStore,
    cursor_key: &CursorKey,
    stage: &SourceDbStage,
    admission: &PendingPhysicalV3Admission,
    now_ms: i64,
    staging: &Path,
) -> PreparedResult<Option<PreparedMonth>> {
    build_selected_month_with_limit(
        store,
        cursor_key,
        stage,
        admission,
        now_ms,
        staging,
        RDP_EVALUATION_LIMIT,
    )
}

fn build_selected_month_with_limit(
    store: &HubStore,
    cursor_key: &CursorKey,
    stage: &SourceDbStage,
    admission: &PendingPhysicalV3Admission,
    now_ms: i64,
    staging: &Path,
    rdp_evaluation_limit: u64,
) -> PreparedResult<Option<PreparedMonth>> {
    #[cfg(test)]
    if crate::physical_publication::FAIL_NEXT_PREPARATION.with(|fail| fail.replace(false)) {
        return Err(invalid("a test failed this preparation"));
    }
    let Some(month) = previous_complete_month(now_ms)? else {
        return Ok(None);
    };
    let id = artifact_id(admission, &month.name, month.from_ms, month.to_ms);
    let catalogue = store.open()?;
    let previous: Option<CachedMonth> = catalogue
        .query_row(
            "SELECT vehicle_id, input_receipt_id, month, input_manifest_id,
                    input_sequence, source_digest, pack_sha256, compressed_bytes,
                    CASE WHEN length(receipt_json) <= 2097152 THEN receipt_json END
         FROM prepared_map_months WHERE artifact_id = ?1",
            [&id],
            |row| {
                Ok(CachedMonth {
                    vehicle: row.get(0)?,
                    source_receipt: row.get(1)?,
                    month: row.get(2)?,
                    manifest: row.get(3)?,
                    sequence: row.get(4)?,
                    source_digest: row.get(5)?,
                    digest: row.get(6)?,
                    compressed_bytes: row.get(7)?,
                    receipt: row.get(8)?,
                })
            },
        )
        .optional()?;
    if let Some(previous) = &previous
        && (previous.vehicle != admission.vehicle_id.to_string()
            || previous.source_receipt != admission.receipt_id)
    {
        return Err(invalid(
            "prepared artifact identity conflicts with its source head",
        ));
    }
    if let Some(previous) = &previous
        && let Some(pack) =
            verified_cached_month(store, cursor_key, admission, &id, &month, previous)?
    {
        return Ok(Some(PreparedMonth {
            contents: PreparedMonthContents::Cached(pack),
        }));
    }

    let mut drives = Vec::new();
    let mut after = 0;
    loop {
        let page =
            stage.page::<SourceDbDrivePhysicalV2_2>(SourceDbStageTable::Drives, after, 10_000)?;
        for row in page.rows {
            let drive = row.value;
            if i64::from(drive.car_id) != admission.selected_car_id {
                return Err(invalid("prepared map drive escaped selected car"));
            }
            // The App's PhysicalV3 projection does not publish an unfinished
            // drive or its positions. Use that same drive set for prepared
            // geometry, including when an open drive starts in this month.
            if drive.end_date_pg_us.is_none() {
                continue;
            }
            let start_ms = pg_ms(drive.start_date_pg_us)?;
            if start_ms >= month.from_ms && start_ms < month.to_ms {
                drives.push((start_ms, drive.id));
                if drives.len() > 10_000 {
                    return Err(invalid("prepared month has too many drives"));
                }
            }
        }
        match page.next_after_id {
            Some(next) => after = next,
            None => break,
        }
    }
    drives.sort_unstable();
    let ids = drives.iter().map(|(_, id)| *id).collect::<HashSet<_>>();
    let scratch_file = NamedTempFile::new_in(staging)?;
    #[cfg(test)]
    if let Some(probe) = ON_MONTH_BUILD.with(|hook| hook.borrow_mut().take()) {
        probe();
    }
    let mut scratch = Connection::open(scratch_file.path())?;
    indexed_month_positions(stage, &mut scratch, &ids, admission.selected_car_id)?;
    let output = compute_month(&scratch, &drives, rdp_evaluation_limit)?;
    drop(scratch);
    let (source_digest, tiles) = match output {
        TileOutput::ReadyEmpty { drive_count, .. } => {
            if usize::try_from(drive_count)? != drives.len() {
                return Err(invalid("prepared drive count is inconsistent"));
            }
            let digest =
                Sha256Digest::of_bytes(format!("empty-month-v1:{}", drives.len()).as_bytes())
                    .to_string();
            (digest, Vec::new())
        }
        TileOutput::ReadyData {
            drive_count,
            payload,
            tile_count,
            ..
        } => {
            if usize::try_from(drive_count)? != drives.len() {
                return Err(invalid("prepared drive count is inconsistent"));
            }
            let tiles = decode_tiles(&payload)?;
            if tiles.len() != tile_count {
                return Err(invalid("prepared tile count is inconsistent"));
            }
            (Sha256Digest::of_bytes(&payload).to_string(), tiles)
        }
    };
    let sqlite_file = NamedTempFile::new_in(staging)?;
    let uncompressed_bytes =
        write_sqlite(&sqlite_file, &id, admission, &month, drives.len(), &tiles)?;
    let (pack, digest, compressed_bytes) =
        compress_month(staging, &sqlite_file, uncompressed_bytes)?;
    let receipt = signed_receipt(
        cursor_key,
        admission,
        &id,
        &month,
        drives.len(),
        tiles.len(),
        &digest,
        compressed_bytes,
        uncompressed_bytes,
        now_ms,
    )?;

    Ok(Some(PreparedMonth {
        contents: PreparedMonthContents::New {
            id,
            month,
            source_digest,
            pack,
            digest,
            compressed_bytes,
            receipt,
        },
    }))
}

impl PreparedMonth {
    pub(crate) fn publish(
        self,
        store: &HubStore,
        _gate: &crate::db::PublicationGate,
        admission: &PendingPhysicalV3Admission,
    ) -> PreparedResult<()> {
        self.publish_with_sync(store, admission, &|path| File::open(path)?.sync_all())
    }

    fn publish_with_sync(
        self,
        store: &HubStore,
        admission: &PendingPhysicalV3Admission,
        sync_directory: &dyn Fn(&Path) -> io::Result<()>,
    ) -> PreparedResult<()> {
        let mut catalogue = store.open()?;
        require_current_source_head(store, &catalogue, admission)?;
        #[cfg(test)]
        if let Some(probe) = ON_MONTH_FINAL.with(|hook| hook.borrow_mut().take()) {
            probe();
        }
        let (id, month, source_digest, digest, compressed_bytes, receipt) = match self.contents {
            PreparedMonthContents::Cached(pack) => {
                pack.sync_all()?;
                sync_directory(&store.packs_dir().join("sha256"))?;
                sync_directory(store.packs_dir())?;
                return Ok(());
            }
            PreparedMonthContents::New {
                id,
                month,
                source_digest,
                pack,
                digest,
                compressed_bytes,
                receipt,
            } => {
                install_month_pack(store, pack, &digest, compressed_bytes, sync_directory)?;
                (id, month, source_digest, digest, compressed_bytes, receipt)
            }
        };
        let transaction = catalogue.transaction()?;
        transaction.execute(
            "INSERT INTO prepared_map_months
             (artifact_id, vehicle_id, month, input_manifest_id, input_receipt_id,
              input_sequence, source_digest, pack_sha256, compressed_bytes, receipt_json)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(artifact_id) DO UPDATE SET
                 month = excluded.month,
                 input_manifest_id = excluded.input_manifest_id,
                 input_sequence = excluded.input_sequence,
                 source_digest = excluded.source_digest,
                 pack_sha256 = excluded.pack_sha256,
                 compressed_bytes = excluded.compressed_bytes,
                 receipt_json = excluded.receipt_json
             WHERE prepared_map_months.vehicle_id = excluded.vehicle_id
               AND prepared_map_months.input_receipt_id = excluded.input_receipt_id",
            params![
                id,
                admission.vehicle_id.to_string(),
                month.name,
                admission.snapshot_id.to_string(),
                admission.receipt_id,
                i64::try_from(admission.head_sequence)?,
                source_digest,
                digest,
                i64::try_from(compressed_bytes)?,
                receipt,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }
}
