// SPDX-License-Identifier: AGPL-3.0-only

// A window's data reaches 1.4 clients without another import (M03-T22,
// DEC-137). At import publication the sealed raw physical stage and the
// import's source sessions are kept per vehicle under the packs directory,
// their digests in `hub_metadata`. After a handback the seam is planned from
// the kept source sessions and the new coverage: when the plan restates no
// source row it is applied in one store transaction and the head is
// published from a copy of the kept stage with the new pack plan; when it
// restates one nothing is applied and the head stays refused until the next
// import.

/// The directory under the packs directory that holds the kept stages.
const PHYSICAL_KEPT_DIRECTORY: &str = "physical-kept";

/// The stable run-lock inode also carries the single coalesced request bit.
/// Positional I/O changes one byte without seeking or replacing the inode.
pub(crate) struct HandbackRunFile(std::fs::File);

pub(crate) struct HandbackRunGuard<'a> {
    file: &'a std::fs::File,
}

impl HandbackRunFile {
    pub(crate) fn open(data_dir: &std::path::Path) -> std::io::Result<Self> {
        use rustix::fs::{Mode, OFlags, open, openat};
        use std::os::unix::fs::MetadataExt;
        let directory = open(
            data_dir,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let fd = openat(
            &directory,
            "handback-run.lock",
            OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::RUSR | Mode::WUSR,
        )?;
        let file = std::fs::File::from(fd);
        let stat = file.metadata()?;
        if !stat.is_file()
            || stat.uid() != rustix::process::geteuid().as_raw()
            || stat.mode() & 0o077 != 0
            || stat.nlink() != 1
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsafe handback run lock",
            ));
        }
        Ok(Self(file))
    }

    pub(crate) fn try_acquire(&self) -> std::io::Result<Option<HandbackRunGuard<'_>>> {
        if crate::platform::private_directory::try_lock_exclusive(&self.0)? {
            Ok(Some(HandbackRunGuard { file: &self.0 }))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn request_rerun(&self) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.0.write_all_at(&[1], 0)
    }

    pub(crate) fn rerun_requested(&self) -> std::io::Result<bool> {
        use std::os::unix::fs::FileExt;
        let mut flag = [0];
        Ok(self.0.read_at(&mut flag, 0)? == 1 && flag[0] != 0)
    }
}

impl HandbackRunGuard<'_> {
    pub(crate) fn accept_requests(&self) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(&[0], 0)
    }
}

impl Drop for HandbackRunGuard<'_> {
    fn drop(&mut self) {
        if let Err(error) = rustix::fs::flock(self.file, rustix::fs::FlockOperation::Unlock) {
            tracing::warn!(%error, "handback run lock could not be released");
        }
    }
}

/// The import's source sessions kept beside its raw physical stage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct KeptPhysicalSource {
    binding: ProjectionBinding,
    drives: Vec<ProjectionDrive>,
    charges: Vec<ProjectionCharge>,
}

/// What a handback did with one vehicle's physical 1.4 head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HandbackPhysicalOutcome {
    /// The current head is served: nothing to plan.
    Current,
    /// The head was planned with the new coverage and published.
    Published(crate::physical_publication::PhysicalV3PublicationKind),
    /// The plan restates a source row: nothing was applied, and the head
    /// stays refused until the next import.
    Restates,
    /// The head cannot be planned from what is kept; it stays refused until
    /// the next import. The reason names what failed.
    Withheld(String),
    /// The vehicle has no physical head any more: its kept stage went.
    Pruned,
}

fn physical_kept_dir(store: &HubStore) -> std::path::PathBuf {
    store.packs_dir().join(PHYSICAL_KEPT_DIRECTORY)
}

/// The legacy kept raw stage an earlier build wrote: a private sealed stage
/// under `.staging`, as [`SourceDbStage::open_sealed`] requires. It stays
/// readable until a verified conversion to a kept archive commits.
fn kept_stage_path(store: &HubStore, vehicle_id: Uuid) -> std::path::PathBuf {
    physical_kept_dir(store)
        .join(".staging")
        .join(format!("{vehicle_id}.sqlite"))
}

/// The legacy source sessions beside a legacy raw stage.
fn legacy_kept_source_path(store: &HubStore, vehicle_id: Uuid) -> std::path::PathBuf {
    physical_kept_dir(store).join(format!("{vehicle_id}.source.json"))
}

/// The kept archive of one generation of the vehicle's replay base
/// (DEC-250).
fn kept_archive_name(vehicle_id: Uuid, generation: Uuid) -> String {
    format!("{vehicle_id}.{generation}.kept")
}

fn kept_generation_source_name(vehicle_id: Uuid, generation: Uuid) -> String {
    format!("{vehicle_id}.{generation}.source.json")
}

/// The source sessions file the record names.
fn kept_source_path(
    store: &HubStore,
    vehicle_id: Uuid,
    kept: &crate::db::PhysicalV3Kept,
) -> std::path::PathBuf {
    match &kept.archive {
        Some(archive) => physical_kept_dir(store)
            .join(kept_generation_source_name(vehicle_id, archive.generation)),
        None => legacy_kept_source_path(store, vehicle_id),
    }
}

/// A digest of the import binding a kept archive belongs to.
fn kept_binding_digest(binding: &ProjectionBinding) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"hub kept archive binding v1\n");
    hash.update(binding.installation_id.as_bytes());
    hash.update(binding.account_id.as_bytes());
    hash.update(binding.vehicle_id.as_bytes());
    hash.update(binding.generation.to_be_bytes());
    hash.update(binding.selected_car_id.to_be_bytes());
    hash.finalize().into()
}

fn kept_archive_identity(
    binding: &ProjectionBinding,
) -> crate::source_db_stage::KeptArchiveIdentity {
    crate::source_db_stage::KeptArchiveIdentity {
        vehicle_id: binding.vehicle_id,
        binding: kept_binding_digest(binding),
    }
}

/// A simulated crash at a named point of a keep or conversion (tests catch
/// the panic): `files` after the new pair is durable, `swap` after the
/// record names it, before the superseded files go.
fn kept_crash_point(point: &'static str) {
    #[cfg(test)]
    if crate::source_db_stage::KEPT_ARCHIVE_CRASH_AT.with(|at| at.get() == Some(point)) {
        crate::source_db_stage::KEPT_ARCHIVE_CRASH_AT.with(|at| at.set(None));
        panic!("simulated crash at kept point {point}");
    }
    #[cfg(not(test))]
    let _ = point;
}

fn sync_kept_dir(store: &HubStore) -> Result<(), String> {
    std::fs::File::open(physical_kept_dir(store))
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

/// Writes one generation's kept pair durably: the archive of `stage` and
/// the source sessions `source`, then the directory. Returns the archive
/// record; nothing names the files yet.
fn write_kept_pair(
    store: &HubStore,
    binding: &ProjectionBinding,
    stage: &SourceDbStage,
    source: &[u8],
) -> Result<(crate::db::PhysicalV3KeptArchive, String), String> {
    write_kept_pair_in(&physical_kept_dir(store), binding, stage, source)
}

fn write_kept_pair_in(
    directory: &Path,
    binding: &ProjectionBinding,
    stage: &SourceDbStage,
    source: &[u8],
) -> Result<(crate::db::PhysicalV3KeptArchive, String), String> {
    let vehicle_id = binding.vehicle_id;
    let generation = Uuid::new_v4();
    let receipt = stage
        .write_kept_archive(
            directory,
            &kept_archive_name(vehicle_id, generation),
            kept_archive_identity(binding),
        )
        .map_err(|error| error.to_string())?;
    write_private_file(
        &directory.join(kept_generation_source_name(vehicle_id, generation)),
        source,
    )?;
    sync_directory(directory)?;
    kept_crash_point("files");
    Ok((
        crate::db::PhysicalV3KeptArchive {
            generation,
            format: receipt.format,
            digest: receipt.digest.to_string(),
            bytes: receipt.bytes,
            decoded_bytes: receipt.decoded_bytes,
        },
        receipt.logical.to_string(),
    ))
}

/// Keeps the sealed raw `stage`, as one compact kept archive, and the
/// import's source sessions for the vehicle's next handback, replacing what
/// an earlier import kept. The record goes first, so a crash part-way
/// leaves nothing to plan and never an older import's base; the new pair is
/// durable before one transaction names it; the superseded files go after.
/// A keep that fails forgets the vehicle's kept state; the next import
/// keeps again. Returns the kept stage's recorded digest.
fn keep_physical_source(
    store: &HubStore,
    publication_gate: &PublicationGate,
    binding: &ProjectionBinding,
    stage: &SourceDbStage,
    drives: &[ProjectionDrive],
    charges: &[ProjectionCharge],
) -> Option<String> {
    let vehicle_id = binding.vehicle_id;
    let recorded = (|| -> Result<String, String> {
        store
            .record_physical_v3_kept(publication_gate, vehicle_id, None)
            .map_err(|error| error.to_string())?;
        store
            .record_physical_v3_handback_plan(publication_gate, vehicle_id, None)
            .map_err(|error| error.to_string())?;
        let source = serde_json::to_vec(&KeptPhysicalSource {
            binding: binding.clone(),
            drives: drives.to_vec(),
            charges: charges.to_vec(),
        })
        .map_err(|error| error.to_string())?;
        // Its rows are authenticated by this digest at every handback, and
        // each handback checks the pages of the stage it plans (M08-T19).
        let (archive, logical) = write_kept_pair(store, binding, stage, &source)?;
        let kept = crate::db::PhysicalV3Kept {
            stage: logical,
            source: Sha256Digest::of_bytes(&source).to_string(),
            archive: Some(archive),
        };
        if !store
            .swap_physical_v3_kept(publication_gate, vehicle_id, None, Some(&kept), true)
            .map_err(|error| error.to_string())?
        {
            return Err("the kept record changed while it was replaced".into());
        }
        kept_crash_point("swap");
        remove_superseded_kept_files(store, vehicle_id, Some(&kept));
        Ok(kept.stage)
    })();
    match recorded {
        Ok(stage) => Some(stage),
        Err(reason) => {
            tracing::warn!(
                %vehicle_id,
                %reason,
                "the import's physical stage was not kept; a handback leaves the 1.4 head refused \
                 until the next import"
            );
            if let Err(reason) = forget_physical_kept(store, publication_gate, vehicle_id) {
                tracing::warn!(%vehicle_id, %reason, "the failed keep's record was not forgotten");
            }
            None
        }
    }
}

/// What one file of the kept directory is, by name, for one vehicle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeptFile {
    LegacySource,
    Archive(Uuid),
    Source(Uuid),
    /// An archive or source file of a generation not yet renamed into place.
    Partial(Uuid),
}

fn kept_file(vehicle_id: Uuid, name: &str) -> Option<KeptFile> {
    let rest = name.strip_prefix(&format!("{vehicle_id}."))?;
    if rest == "source.json" {
        return Some(KeptFile::LegacySource);
    }
    let generation = rest.get(..36).and_then(|id| Uuid::parse_str(id).ok())?;
    let tail = rest.get(36..)?;
    match tail {
        ".kept" => Some(KeptFile::Archive(generation)),
        ".source.json" => Some(KeptFile::Source(generation)),
        ".kept.partial" => Some(KeptFile::Partial(generation)),
        _ => tail
            .strip_prefix(".source.")
            .and_then(|temporary| temporary.strip_suffix(".tmp"))
            .filter(|temporary| Uuid::parse_str(temporary).is_ok())
            .map(|_| KeptFile::Partial(generation)),
    }
}

#[cfg(test)]
thread_local! {
    static ON_CONVERSION_FINAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

fn build_legacy_conversion(
    scratch: &Path,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    legacy: &SourceDbStage,
    source: &[u8],
) -> Result<crate::db::PhysicalV3KeptArchive, String> {
    let (archive, logical) = write_kept_pair_in(scratch, binding, legacy, source)?;
    kept_crash_point("conversion-pair-written");
    handback_crash_point("conversion-pair-written");
    if logical != kept.stage {
        return Err("the archive's rows differ from the kept record".into());
    }
    Ok(archive)
}

fn finish_legacy_conversion(
    store: &HubStore,
    gate: &PublicationGate,
    vehicle_id: Uuid,
    kept: &crate::db::PhysicalV3Kept,
    scratch: &Path,
    archive: crate::db::PhysicalV3KeptArchive,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(probe) = ON_CONVERSION_FINAL.with(|hook| hook.borrow_mut().take()) {
        probe();
    }
    if store
        .physical_v3_kept(vehicle_id)
        .map_err(|error| error.to_string())?
        .as_ref()
        != Some(kept)
    {
        return Err("the kept record changed while the archive was built".into());
    }
    let destination = physical_kept_dir(store);
    let replacement = crate::db::PhysicalV3Kept {
        stage: kept.stage.clone(),
        source: kept.source.clone(),
        archive: Some(archive.clone()),
    };
    let publish = || -> Result<(), String> {
        for name in [
            kept_archive_name(vehicle_id, archive.generation),
            kept_generation_source_name(vehicle_id, archive.generation),
        ] {
            std::fs::rename(scratch.join(&name), destination.join(&name))
                .map_err(|error| error.to_string())?;
        }
        sync_kept_dir(store)?;
        sync_directory(scratch)?;
        if !store
            .swap_physical_v3_kept(gate, vehicle_id, Some(kept), Some(&replacement), false)
            .map_err(|error| error.to_string())?
        {
            return Err("the kept record changed while it was replaced".into());
        }
        kept_crash_point("swap");
        remove_superseded_kept_files(store, vehicle_id, Some(&replacement));
        Ok(())
    };
    let result = publish();
    if result.is_err() {
        remove_unnamed_kept_files(store, vehicle_id);
    }
    result
}

/// Removes the vehicle's kept files the record `kept` does not name (an
/// interrupted keep or conversion, or the superseded pair), under the
/// publication gate the caller holds. Only regular files of the kept shapes
/// go; anything else is left. A capture a pending import publication names
/// is never removed, wherever its adoption left it (issue #60); when the
/// pending publications cannot be read, nothing is removed.
fn remove_superseded_kept_files(
    store: &HubStore,
    vehicle_id: Uuid,
    kept: Option<&crate::db::PhysicalV3Kept>,
) {
    let (Ok(holds), Ok(Some(candidates))) = (
        store.import_publication_holds(),
        store.handback_candidate_files(),
    ) else {
        return;
    };
    if holds.holds_vehicle(vehicle_id) {
        return;
    }
    let current = kept.map(|kept| kept.archive.as_ref().map(|archive| archive.generation));
    let mut removed = false;
    let directory = physical_kept_dir(store);
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(file) = kept_file(vehicle_id, &name) else {
                continue;
            };
            let named = match (file, current) {
                (KeptFile::LegacySource, Some(None)) => true,
                (KeptFile::Archive(generation) | KeptFile::Source(generation), Some(Some(now))) => {
                    generation == now
                }
                _ => false,
            };
            let held = matches!(
                file,
                KeptFile::Archive(generation) | KeptFile::Source(generation)
                    if holds.holds(vehicle_id, generation)
            );
            if !named
                && !held
                && !candidates.contains(&std::path::Path::new(PHYSICAL_KEPT_DIRECTORY).join(&name))
                && entry.file_type().is_ok_and(|kind| kind.is_file())
                && std::fs::remove_file(entry.path()).is_ok()
            {
                removed = true;
            }
        }
    }
    let legacy = kept_stage_path(store, vehicle_id);
    if current != Some(None)
        && !candidates.contains(legacy.strip_prefix(store.packs_dir()).expect("kept path"))
        && std::fs::symlink_metadata(&legacy).is_ok_and(|metadata| metadata.is_file())
        && std::fs::remove_file(&legacy).is_ok()
    {
        removed = true;
        let _ = std::fs::File::open(directory.join(".staging")).and_then(|dir| dir.sync_all());
    }
    if removed {
        let _ = sync_kept_dir(store);
    }
}

/// The working stage a changed handback plans: the kept archive decoded,
/// bounded, into one owned sealed stage and authenticated before any edit,
/// or for a legacy record the raw stage authenticated and copied. A legacy
/// raw stage is returned too, still open, for its conversion.
fn open_kept_working(
    store: &HubStore,
    vehicle_id: Uuid,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
) -> Result<(crate::source_db_stage::OwnedSourceDbStage, Option<SourceDbStage>), String> {
    if let Some(archive) = &kept.archive {
        let working = SourceDbStage::decode_kept_archive(
            physical_kept_dir(store),
            &kept_archive_name(vehicle_id, archive.generation),
            crate::source_db_stage::KeptArchiveExpected {
                identity: kept_archive_identity(binding),
                logical: &kept.stage,
                digest: &archive.digest,
                bytes: archive.bytes,
                decoded_bytes: archive.decoded_bytes,
            },
        )
        .map_err(|error| format!("the kept archive: {error}"))?;
        return Ok((crate::source_db_stage::OwnedSourceDbStage::new(working), None));
    }
    // The kept stage is authenticated by its recorded logical digest and
    // only copied. The run's one full integrity check is of the planned
    // copy, after its last write, as the publication reads it (M08-T19).
    let kept_stage = SourceDbStage::open_sealed_to_copy(kept_stage_path(store, vehicle_id))
        .map_err(|error| format!("the kept stage: {error}"))?;
    match kept_stage.sealed_logical_digest() {
        Ok(digest) if digest.to_string() == kept.stage => {}
        Ok(_) => return Err("the kept stage differs from its record".into()),
        Err(error) => return Err(format!("the kept stage: {error}")),
    }
    let working = kept_stage
        .copy_sealed_into(physical_kept_dir(store))
        .map_err(|error| format!("a copy of the kept stage: {error}"))?;
    Ok((
        crate::source_db_stage::OwnedSourceDbStage::new(working),
        Some(kept_stage),
    ))
}

/// Converts a legacy kept raw stage, already authenticated against `kept`,
/// to a kept archive (DEC-250): the new pair is written durably, one
/// transaction replaces the record only while it is still `kept`, then the
/// legacy files go. The recorded plan inputs stay: the stage's logical
/// digest is unchanged. A failure leaves the legacy base as it was.
fn try_convert_legacy_kept(
    store: &HubStore,
    publication_gate: &PublicationGate,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    legacy: &SourceDbStage,
    source: &[u8],
) -> Result<(), String> {
    let vehicle_id = binding.vehicle_id;
    // Files an earlier refused attempt left go before this one writes more.
    remove_unnamed_kept_files(store, vehicle_id);
    let converted = (|| -> Result<(), String> {
        let (archive, logical) = write_kept_pair(store, binding, legacy, source)?;
        let replaced = logical == kept.stage
            && store
                .swap_physical_v3_kept(
                    publication_gate,
                    vehicle_id,
                    Some(kept),
                    Some(&crate::db::PhysicalV3Kept {
                        stage: kept.stage.clone(),
                        source: kept.source.clone(),
                        archive: Some(archive),
                    }),
                    false,
                )
                .map_err(|error| error.to_string())?;
        if !replaced {
            return Err("the kept record changed or the archive's rows differ".into());
        }
        kept_crash_point("swap");
        let now = store
            .physical_v3_kept(vehicle_id)
            .map_err(|error| error.to_string())?;
        remove_superseded_kept_files(store, vehicle_id, now.as_ref());
        Ok(())
    })();
    if converted.is_err() {
        remove_unnamed_kept_files(store, vehicle_id);
    }
    converted
}

/// Removes the vehicle's kept files the surviving catalogue record does not
/// name, under the held gate. A record that cannot be reread, or is gone,
/// keeps every file.
fn remove_unnamed_kept_files(store: &HubStore, vehicle_id: Uuid) {
    if let Ok(Some(record)) = store.physical_v3_kept(vehicle_id) {
        remove_superseded_kept_files(store, vehicle_id, Some(&record));
    }
}

fn convert_legacy_kept(
    store: &HubStore,
    publication_gate: &PublicationGate,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    legacy: &SourceDbStage,
    source: &[u8],
) {
    let vehicle_id = binding.vehicle_id;
    match try_convert_legacy_kept(store, publication_gate, kept, binding, legacy, source) {
        Ok(()) => tracing::info!(%vehicle_id, "the kept raw stage was converted to a kept archive"),
        Err(reason) => tracing::warn!(
            %vehicle_id,
            %reason,
            "the kept raw stage stays as it is; its conversion is retried at the next handback"
        ),
    }
}

/// Converts every live legacy kept raw base to a kept archive before a
/// backup snapshot is taken. The caller holds the publication gate. Each base
/// is authenticated (source sessions and stage against their record) and goes
/// through the same verified conversion as a handback's; a failure (space,
/// corruption, a changed record) is returned with the vehicle and the reason,
/// and leaves that base, its record and the files as they were.
pub(crate) fn convert_legacy_kept_bases(
    store: &HubStore,
    publication_gate: &PublicationGate,
) -> Result<(), (Uuid, String)> {
    let vehicles = store
        .physical_v3_kept_vehicles()
        .map_err(|error| (Uuid::nil(), error.to_string()))?;
    for vehicle_id in vehicles {
        let convert = || -> Result<(), String> {
            let Some(kept) = store.physical_v3_kept(vehicle_id).map_err(|e| e.to_string())? else {
                return Ok(());
            };
            if kept.archive.is_some() {
                return Ok(());
            }
            let source_bytes = std::fs::read(legacy_kept_source_path(store, vehicle_id))
                .map_err(|error| format!("the kept source sessions: {error}"))?;
            if Sha256Digest::of_bytes(&source_bytes).to_string() != kept.source {
                return Err("the kept source sessions differ from their record".into());
            }
            let source: KeptPhysicalSource = serde_json::from_slice(&source_bytes)
                .map_err(|error| format!("the kept source sessions: {error}"))?;
            if source.binding.vehicle_id != vehicle_id {
                return Err("the kept source sessions name another vehicle".into());
            }
            let legacy = SourceDbStage::open_sealed_to_copy(kept_stage_path(store, vehicle_id))
                .map_err(|error| format!("the kept stage: {error}"))?;
            match legacy.sealed_logical_digest() {
                Ok(digest) if digest.to_string() == kept.stage => {}
                Ok(_) => return Err("the kept stage differs from its record".into()),
                Err(error) => return Err(format!("the kept stage: {error}")),
            }
            try_convert_legacy_kept(
                store,
                publication_gate,
                &kept,
                &source.binding,
                &legacy,
                &source_bytes,
            )
        };
        convert().map_err(|reason| (vehicle_id, reason))?;
        tracing::info!(%vehicle_id, "a legacy kept raw stage was converted before a backup");
    }
    Ok(())
}

/// Writes `bytes` to `path` through a private temporary file and a rename.
fn write_private_file(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let written = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    written.map_err(|error| {
        let _ = std::fs::remove_file(&temporary);
        error.to_string()
    })
}

/// Forgets what is kept for the vehicle: the record, then the files. The files
/// go only against the record the catalogue actually holds afterwards (read
/// again), so a failed removal never unlinks what a surviving record names; a
/// failure to remove or to read again is returned and leaves every file.
fn forget_physical_kept(
    store: &HubStore,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
) -> Result<(), String> {
    #[cfg(test)]
    let injected = FAIL_KEPT_REMOVAL.with(|fail| fail.replace(false));
    #[cfg(not(test))]
    let injected = false;
    let removed = if injected {
        Err("the kept record removal failed (injected)".to_string())
    } else {
        store
            .record_physical_v3_kept(publication_gate, vehicle_id, None)
            .map_err(|error| error.to_string())
    };
    let plan = store
        .record_physical_v3_handback_plan(publication_gate, vehicle_id, None)
        .map_err(|error| error.to_string());
    let surviving = store.physical_v3_kept(vehicle_id).map_err(|error| error.to_string())?;
    remove_superseded_kept_files(store, vehicle_id, surviving.as_ref());
    removed?;
    plan
}

#[cfg(test)]
thread_local! {
    /// Fails the next kept-record removal of `forget_physical_kept` (tests).
    pub(super) static FAIL_KEPT_REMOVAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Fails a handback right after its store transaction commits (tests).
    static FAIL_AFTER_HANDBACK_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The vehicle whose handback meets a seam conflict (tests, PG-2).
    pub(super) static HANDBACK_CONFLICT_FOR: std::cell::Cell<Option<Uuid>> =
        const { std::cell::Cell::new(None) };
    /// Runs once right after a handback's seam commits, before its head is
    /// planned: a collector tick landing there (tests, PG-2).
    pub(super) static AFTER_HANDBACK_SEAM: std::cell::RefCell<Option<HandbackTick>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
type HandbackTick = Box<dyn FnOnce()>;

use crate::runtime::operator::authority::HandbackGateClock;
use std::path::{Path, PathBuf};

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_HANDBACK_BUILD: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

fn handback_crash_point(point: &'static str) {
    #[cfg(test)]
    if std::env::var("TESATLAS_TEST_HANDBACK_KILL").as_deref() == Ok(point) {
        rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::KILL)
            .expect("terminate synthetic handback child");
        unreachable!("SIGKILL returned");
    }
    #[cfg(not(test))]
    let _ = point;
}

/// Vehicle hand-back on the service thread.
pub(crate) fn publish_physical_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    run: &HandbackRunGuard<'_>,
    clock: &HandbackGateClock<'_>,
) -> Result<Vec<(Uuid, HandbackPhysicalOutcome)>, SourceDbImportError> {
    lower_handback_thread_priority();
    let _timing = crate::physical_publication::HandbackRun::start();
    {
        let _phase = crate::physical_publication::HandbackPhase::run_level("resume");
        for (vehicle_id, outcome) in
            resume_import_publications_in_handback(store, cursor_key, run, clock)?
        {
            tracing::info!(%vehicle_id, ?outcome, "import publication resumed before the handback");
        }
    }

    let mut outcomes = Vec::new();
    for vehicle_id in store.physical_v3_kept_vehicles()? {
        let before = clock.held_ms();
        let outcome = publish_vehicle_handback(store, cursor_key, run, clock, vehicle_id);
        let gate_held_ms = clock.held_ms().saturating_sub(before);
        tracing::info!(%vehicle_id, gate_held_ms, "handback vehicle gate held");
        outcomes.push((vehicle_id, outcome?));
        if clock.stopped() {
            return Ok(outcomes);
        }
    }

    // Recover an interrupted repair activation using the head just
    // published. Compensation can change the layer again, so replan after
    // releasing the settlement gate.
    for vehicle_id in store.drive_repair_publishing_vehicles()? {
        let compensate = {
            let Some(gate) = clock.acquire(store)? else {
                break;
            };
            if store.import_publication_holds()?.pending(vehicle_id) {
                continue;
            }
            let Some(batch) = store.drive_repair_publishing_batch(vehicle_id)? else {
                continue;
            };
            let served = store.physical_v3_current_head_served(vehicle_id)? == Some(true);
            let settlement = store.settle_drive_repair_batch(
                &gate,
                vehicle_id,
                batch,
                served,
                handback_clock_ms()?,
            )?;
            tracing::info!(%vehicle_id, batch, ?settlement, "an interrupted repair batch was settled");
            matches!(settlement, crate::db::DriveRepairSettlement::Failed(_))
        };
        if compensate {
            publish_vehicle_handback(store, cursor_key, run, clock, vehicle_id)?;
        }
    }
    Ok(outcomes)
}

fn handback_clock_ms() -> Result<i64, crate::physical_publication::SourceDbPhysicalPublicationError>
{
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|time| i64::try_from(time.as_millis()).ok())
        .ok_or(crate::physical_publication::SourceDbPhysicalPublicationError::Clock)
}

fn withheld_handback(why: impl Into<String>) -> HandbackPhysicalOutcome {
    HandbackPhysicalOutcome::Withheld(why.into())
}

struct HandbackScratch {
    path: PathBuf,
    created: bool,
}

impl HandbackScratch {
    fn create(&mut self) -> Result<(), String> {
        use std::os::unix::fs::DirBuilderExt;
        let parent = self.path.parent().expect("handback staging parent");
        crate::hub_pack::ensure_private_staging_directory(parent)
            .map_err(|error| error.to_string())?;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&self.path)
            .map_err(|error| error.to_string())?;
        self.created = true;
        sync_directory(parent).map_err(|error| error.to_string())
    }

    fn discard(&mut self) -> std::io::Result<()> {
        if self.created {
            std::fs::remove_dir_all(&self.path)?;
            self.created = false;
            std::fs::File::open(self.path.parent().expect("handback staging parent"))?
                .sync_all()?;
        }
        Ok(())
    }
}

struct ChangedHandbackPlan {
    working: crate::source_db_stage::OwnedSourceDbStage,
    conversion: Option<crate::db::PhysicalV3KeptArchive>,
    head: Option<crate::physical_publication::BuiltHandbackHead>,
    layer: Option<crate::db::PhysicalV3RepairLayer>,
    pre_rows: Vec<RepairPreRows>,
    span: Option<(i64, i64)>,
    inputs: Option<String>,
}

enum HandbackPlan {
    Same(crate::db::PhysicalV3HandbackPlan),
    Changed(Box<ChangedHandbackPlan>),
    Supersede(Vec<[u8; 32]>),
}

struct HandbackBuild {
    source: KeptPhysicalSource,
    packs: crate::import::source_db::seam::SeamPackPlan,
    stored: crate::db::ImportSeamStoredRows,
    plan: HandbackPlan,
    month_for: Option<crate::db::PendingPhysicalV3Admission>,
}

enum HandbackBuildResult {
    Stale,
    Answer(HandbackPhysicalOutcome),
    Built(Box<HandbackBuild>),
}

fn publish_vehicle_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    run: &HandbackRunGuard<'_>,
    clock: &HandbackGateClock<'_>,
    vehicle_id: Uuid,
) -> Result<HandbackPhysicalOutcome, SourceDbImportError> {
    let wait_expired =
        || withheld_handback("handback stopped or the publication gate wait expired");
    for _ in 0..3 {
        let now = handback_clock_ms()?;
        let run_id = Uuid::new_v4();
        let relative = PathBuf::from(format!(".staging/handback-{run_id}"));
        let mut scratch = HandbackScratch {
            path: store.packs_dir().join(&relative),
            created: false,
        };
        let basis = {
            let Some(gate) = clock.acquire(store)? else {
                return Ok(wait_expired());
            };
            let _phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "checks");
            let basis = store.plan_basis(vehicle_id)?;
            let Some(kept) = basis.kept.as_ref() else {
                return Ok(HandbackPhysicalOutcome::Pruned);
            };
            if basis.import_publication_pending {
                return Ok(withheld_handback("an import's publication is pending"));
            }
            if basis.coverage_unreconciled {
                return Ok(withheld_handback(
                    "a window's handback has not reached the store's coverage yet",
                ));
            }
            let Some(head) = &basis.head else {
                return Ok(match forget_physical_kept(store, &gate, vehicle_id) {
                    Ok(()) => HandbackPhysicalOutcome::Pruned,
                    Err(reason) => {
                        withheld_handback(format!("the kept record was not forgotten: {reason}"))
                    }
                });
            };
            if !head.blocked && store.physical_v3_current_head_served(vehicle_id)? == Some(true) {
                return Ok(HandbackPhysicalOutcome::Current);
            }
            if !basis.vehicle_active || basis.binding.is_none() {
                return Ok(withheld_handback(
                    "the vehicle has no active projection binding",
                ));
            }
            if basis.import_revision.as_ref().is_some_and(|revision| {
                revision.kept.is_none()
                    || revision.kept != kept.archive.as_ref().map(|archive| archive.generation)
                    || basis.import_intent.is_some()
            }) {
                return Ok(withheld_handback(
                    "the kept base is not the committed import's",
                ));
            }
            remove_superseded_kept_files(store, vehicle_id, Some(kept));
            store.record_handback_candidate(
                run,
                &gate,
                vehicle_id,
                &crate::db::HandbackCandidate {
                    run_id,
                    started_at_ms: now,
                    files: vec![relative],
                },
            )?;
            basis
        };

        let kept = basis.kept.as_ref().expect("PREFLIGHT admitted a kept base");
        let mut built = Some(build_vehicle_handback(
            store,
            cursor_key,
            kept,
            &basis,
            &mut scratch,
            now,
        ));
        #[cfg(test)]
        AFTER_HANDBACK_BUILD.with(|hook| {
            if let Some(hook) = hook.borrow_mut().as_mut() {
                hook();
            }
        });
        handback_crash_point("handback-built");

        // Read the basis even when BUILD failed: a concurrent import may
        // legitimately have removed the superseded kept file it was opening.
        let result = (|| -> Result<_, SourceDbImportError> {
            let Some(gate) = clock.acquire(store)? else {
                return Ok(Some(wait_expired()));
            };
            if store.plan_basis(vehicle_id)? != basis {
                return Ok(None);
            }
            let build = match built.as_mut().expect("BUILD ran") {
                Ok(HandbackBuildResult::Stale) => return Ok(None),
                Ok(HandbackBuildResult::Answer(answer)) => return Ok(Some(answer.clone())),
                Ok(HandbackBuildResult::Built(build)) => build,
                Err(_) => {
                    return Err(built
                        .take()
                        .expect("BUILD ran")
                        .err()
                        .expect("BUILD failed"));
                }
            };
            finish_vehicle_handback(
                store,
                run,
                &gate,
                run_id,
                &basis,
                kept,
                build,
                &scratch.path,
            )
        })();

        if matches!(result, Ok(Some(HandbackPhysicalOutcome::Published(_))))
            && let Some(Ok(HandbackBuildResult::Built(build))) = built.as_mut()
            && let Some(admission) = build.month_for.take()
        {
            prepare_month_of_served_head(
                store,
                cursor_key,
                clock,
                kept,
                build,
                &mut scratch,
                &admission,
                now,
            );
        }

        // Working stage and private pack links are disposed of outside the
        // gate. If a crash happens first the durable marker owns the tree.
        drop(built);
        let cleaned = cleanup_handback_attempt(
            store,
            run,
            clock,
            vehicle_id,
            run_id,
            &mut scratch,
            result.is_err(),
        );
        if !cleaned && matches!(result, Ok(None)) {
            return Ok(withheld_handback(
                "private handback cleanup must be retried",
            ));
        }
        match result {
            Ok(None) if clock.stopped() => return Ok(wait_expired()),
            Ok(None) => continue,
            Ok(Some(answer)) => return Ok(answer),
            Err(SourceDbImportError::Store(StoreError::ImportGenerationConflict)) => {
                return Ok(withheld_handback(
                    "the seam met a conflicting write; the next handback plans it again",
                ));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(withheld_handback(
        "the plan went stale 3 times; the next handback plans it",
    ))
}

fn cleanup_handback_attempt(
    store: &HubStore,
    run: &HandbackRunGuard<'_>,
    clock: &HandbackGateClock<'_>,
    vehicle_id: Uuid,
    run_id: Uuid,
    scratch: &mut HandbackScratch,
    sweep_names: bool,
) -> bool {
    let cleanup = (|| -> Result<bool, String> {
        scratch.discard().map_err(|error| error.to_string())?;
        let Some(gate) = clock.acquire(store).map_err(|error| error.to_string())? else {
            return Ok(false);
        };
        if sweep_names {
            store
                .sweep_dead_handback_candidates(
                    run,
                    &gate,
                    handback_clock_ms().map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?;
        } else {
            store
                .clear_handback_candidate(run, &gate, vehicle_id, run_id)
                .map_err(|error| error.to_string())?;
        }
        Ok(true)
    })();
    match cleanup {
        Ok(done) => done,
        Err(error) => {
            tracing::warn!(%vehicle_id, %error, "handback candidate cleanup deferred");
            false
        }
    }
}

fn build_vehicle_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    kept: &crate::db::PhysicalV3Kept,
    basis: &crate::db::PlanBasis,
    scratch: &mut HandbackScratch,
    now: i64,
) -> Result<HandbackBuildResult, SourceDbImportError> {
    let binding = basis
        .binding
        .as_ref()
        .expect("PREFLIGHT admitted a binding");
    let vehicle_id = binding.vehicle_id;
    let refuse = |reason| Ok(HandbackBuildResult::Answer(withheld_handback(reason)));
    let source_bytes = match std::fs::read(kept_source_path(store, vehicle_id, kept)) {
        Ok(bytes) if Sha256Digest::of_bytes(&bytes).to_string() == kept.source => bytes,
        Ok(_) => return refuse("the kept source sessions differ from their record".to_owned()),
        Err(error) => return refuse(format!("the kept source sessions: {error}")),
    };
    let source: KeptPhysicalSource = match serde_json::from_slice(&source_bytes) {
        Ok(source) => source,
        Err(error) => return refuse(format!("the kept source sessions: {error}")),
    };
    if source.binding != *binding {
        return refuse("the kept source sessions name another projection binding".to_owned());
    }
    let seam = crate::physical_publication::HandbackPhase::start(vehicle_id, "seam");
    let (dry, stored) = store.dry_run_handback_with_rows(
        vehicle_id,
        binding.selected_car_id,
        &source.drives,
        &source.charges,
    )?;
    drop(seam);
    if !dry.ahead.restated_drives.is_empty() || !dry.ahead.restated_charges.is_empty() {
        return Ok(HandbackBuildResult::Answer(
            HandbackPhysicalOutcome::Restates,
        ));
    }
    let packs = dry.ahead.packs;
    let early_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "early_answer");
    let early = if basis.seam_present && basis.head.as_ref().is_some_and(|head| !head.blocked) {
        store
            .physical_v3_handback_plan(vehicle_id)?
            .filter(|recorded| {
                basis
                    .head
                    .as_ref()
                    .is_some_and(|head| head.snapshot_id.to_string() == recorded.snapshot_id)
                    && stored_rows_digest(&stored).is_ok_and(|rows| {
                        let inputs = handback_plan_inputs(
                            &kept.stage,
                            binding,
                            &packs,
                            recorded.span,
                            &rows,
                            now,
                        );
                        handback_plan_inputs_layered(inputs, basis.repair_layer.as_deref())
                            == recorded.inputs
                    })
            })
    } else {
        None
    };
    drop(early_phase);
    let plan = if let Some(recorded) = early {
        HandbackPlan::Same(recorded)
    } else {
        let active = store.drive_repair_active_layer(vehicle_id)?;
        let layer = active.as_ref().map(|active| active.layer.layer.as_str());
        let pre_rows = active
            .iter()
            .flat_map(|active| &active.components)
            .map(|component| (&component.component_key, &component.before));
        if layer != basis.repair_layer.as_deref()
            || !pre_rows.eq(basis
                .repair_pre_rows
                .iter()
                .map(|(key, before)| (key, before)))
        {
            return Ok(HandbackBuildResult::Stale);
        }
        let open_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "open");
        let (mut working, legacy) = match scratch
            .create()
            .and_then(|()| open_kept_working_in_scratch(store, kept, binding, &scratch.path))
        {
            Ok(opened) => opened,
            Err(error) => return refuse(error),
        };
        drop(open_phase);
        let planned = match plan_handback_stage(
            store,
            binding,
            working.stage_mut(),
            basis,
            &packs,
            &stored,
            active,
        ) {
            Ok(planned) => planned,
            Err(PhysicalPlanError::Unplannable(reason)) => return refuse(reason),
            Err(PhysicalPlanError::Stage(error)) => {
                return refuse(format!("the planned copy of the kept stage: {error}"));
            }
            Err(PhysicalPlanError::Store(error)) => return Err(error.into()),
        };
        if planned.stale_repairs.is_empty() {
            let span = planned.facts.as_ref().and_then(|facts| facts.span);
            let inputs = planned.facts.as_ref().map(|facts| {
                handback_plan_inputs_layered(
                    handback_plan_inputs(&kept.stage, binding, &packs, span, &facts.stored, now),
                    layer_identity(planned.layer.as_ref()),
                )
            });
            let head = match crate::physical_publication::build_handback_head(
                store,
                cursor_key,
                binding.clone(),
                working.stage_mut(),
                &scratch.path,
                now,
            ) {
                Ok(head) => head,
                Err(crate::physical_publication::SourceDbPhysicalPublicationError::Stage(
                    error,
                )) => {
                    return refuse(format!("the planned copy of the kept stage: {error}"));
                }
                Err(error) => return Err(error.into()),
            };
            let conversion = legacy.and_then(|legacy| {
                let _phase =
                    crate::physical_publication::HandbackPhase::start(vehicle_id, "conversion");
                match build_legacy_conversion(&scratch.path, kept, binding, &legacy, &source_bytes)
                {
                    Ok(archive) => Some(archive),
                    Err(reason) => {
                        tracing::warn!(%vehicle_id, %reason, "legacy kept conversion deferred");
                        None
                    }
                }
            });
            HandbackPlan::Changed(Box::new(ChangedHandbackPlan {
                working,
                conversion,
                head: Some(head),
                layer: planned.layer,
                pre_rows: planned.first,
                span,
                inputs,
            }))
        } else {
            HandbackPlan::Supersede(planned.stale_repairs)
        }
    };
    Ok(HandbackBuildResult::Built(Box::new(HandbackBuild {
        source,
        packs,
        stored,
        plan,
        month_for: None,
    })))
}

fn layer_identity(layer: Option<&crate::db::PhysicalV3RepairLayer>) -> Option<&str> {
    layer.map(|layer| layer.layer.as_str())
}

fn open_kept_working_in_scratch(
    store: &HubStore,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    scratch: &Path,
) -> Result<
    (
        crate::source_db_stage::OwnedSourceDbStage,
        Option<SourceDbStage>,
    ),
    String,
> {
    let vehicle_id = binding.vehicle_id;
    if let Some(archive) = &kept.archive {
        let working = open_archived_working_in_scratch(
            &physical_kept_dir(store),
            kept,
            archive,
            binding,
            scratch,
        )?;
        return Ok((working, None));
    }
    let legacy = SourceDbStage::open_sealed_to_copy(kept_stage_path(store, vehicle_id))
        .map_err(|error| format!("the kept stage: {error}"))?;
    let digest = legacy
        .sealed_logical_digest()
        .map_err(|error| format!("the kept stage: {error}"))?;
    if digest.to_string() != kept.stage {
        return Err("the kept stage differs from its record".into());
    }
    let working = legacy
        .copy_sealed_into(scratch)
        .map_err(|error| format!("a copy of the kept stage: {error}"))?;
    Ok((
        crate::source_db_stage::OwnedSourceDbStage::new(working),
        Some(legacy),
    ))
}

fn open_archived_working_in_scratch(
    archive_directory: &Path,
    kept: &crate::db::PhysicalV3Kept,
    archive: &crate::db::PhysicalV3KeptArchive,
    binding: &ProjectionBinding,
    scratch: &Path,
) -> Result<crate::source_db_stage::OwnedSourceDbStage, String> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let vehicle_id = binding.vehicle_id;
    let name = kept_archive_name(vehicle_id, archive.generation);
    let copy = || -> std::io::Result<()> {
        let flags = rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK;
        let kept_dir = rustix::fs::open(
            archive_directory,
            rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let dir = rustix::fs::fstat(&kept_dir)?;
        if dir.st_uid != rustix::process::geteuid().as_raw() || dir.st_mode & 0o077 != 0 {
            return Err(std::io::Error::other(
                "kept archive directory is not private",
            ));
        }
        let file = std::fs::File::from(rustix::fs::openat(
            &kept_dir,
            name.as_str(),
            flags | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?);
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.mode() & 0o077 != 0
            || meta.nlink() != 1
            || meta.len() != archive.bytes
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid kept archive",
            ));
        }
        let reserve = archive
            .bytes
            .checked_add(archive.decoded_bytes)
            .ok_or_else(|| std::io::Error::other("kept archive size overflow"))?;
        if crate::hub_pack::available_bytes(scratch).map_err(std::io::Error::other)? < reserve {
            return Err(std::io::Error::other(
                "insufficient working space for kept archive",
            ));
        }
        let mut target = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(scratch.join(&name))?;
        let count = std::io::copy(&mut file.take(archive.bytes.saturating_add(1)), &mut target)?;
        if count != archive.bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "kept archive changed during copy",
            ));
        }

        Ok(())
    };
    copy().map_err(|error| format!("a private copy of the kept archive: {error}"))?;
    let working = SourceDbStage::decode_kept_archive(
        scratch,
        &name,
        crate::source_db_stage::KeptArchiveExpected {
            identity: kept_archive_identity(binding),
            logical: &kept.stage,
            digest: &archive.digest,
            bytes: archive.bytes,
            decoded_bytes: archive.decoded_bytes,
        },
    )
    .map_err(|error| format!("the kept archive: {error}"))?;
    Ok(crate::source_db_stage::OwnedSourceDbStage::new(working))
}

#[allow(clippy::too_many_arguments)]
fn finish_vehicle_handback(
    store: &HubStore,
    run: &HandbackRunGuard<'_>,
    gate: &PublicationGate,
    run_id: Uuid,
    basis: &crate::db::PlanBasis,
    kept: &crate::db::PhysicalV3Kept,
    build: &mut HandbackBuild,
    scratch: &Path,
) -> Result<Option<HandbackPhysicalOutcome>, SourceDbImportError> {
    use crate::physical_publication::{HandbackPhase, PhysicalV3PublicationKind};
    let binding = &build.source.binding;
    let vehicle_id = binding.vehicle_id;
    let final_now = handback_clock_ms()?;
    let span = match &build.plan {
        HandbackPlan::Same(recorded) => recorded.span,
        HandbackPlan::Changed(changed) => changed.span,
        HandbackPlan::Supersede(_) => None,
    };
    if let HandbackPlan::Changed(changed) = &build.plan
        && changed
            .head
            .as_ref()
            .expect("built head")
            .recover_unrelated_rotation(store, gate, vehicle_id, final_now)?
    {
        return Ok(None);
    }
    let seam = HandbackPhase::start(vehicle_id, "seam");
    let committed = store.commit_handback_seam(
        gate,
        vehicle_id,
        binding.selected_car_id,
        &build.source.drives,
        &build.source.charges,
        &build.packs,
        span,
    )?;
    drop(seam);
    if !committed {
        return Ok(None);
    }
    handback_crash_point("handback-seam-committed");
    #[cfg(test)]
    if let Some(tick) = AFTER_HANDBACK_SEAM.with(|hook| hook.borrow_mut().take()) {
        tick();
    }
    #[cfg(test)]
    if FAIL_AFTER_HANDBACK_COMMIT.with(|fail| fail.replace(false)) {
        return Err(StoreError::InvalidImportGeneration.into());
    }
    let coverage = crate::db::coverage_digest(build.packs.coverage().intervals());
    let outcome = match &mut build.plan {
        HandbackPlan::Supersede(keys) => {
            let superseded = store.supersede_stale_drive_repairs(gate, vehicle_id, keys, final_now)?;
            if superseded.len() != keys.len() {
                return Err(StoreError::ImportGenerationConflict.into());
            }
            return Ok(None);
        }
        HandbackPlan::Same(recorded) => {
            let snapshot = basis.head.as_ref().expect("matched head").snapshot_id;
            store.record_physical_v3_seam_planned_with_coverage(
                gate, vehicle_id, snapshot, &coverage,
            )?;
            handback_crash_point("handback-recorded");
            handback_crash_point("handback-rotated");
            if !recorded.prepared {
                build.month_for =
                    store.pending_physical_v3_control_admission_for_vehicle(vehicle_id)?;
            }
            HandbackPhysicalOutcome::Published(PhysicalV3PublicationKind::Unchanged)
        }
        HandbackPlan::Changed(changed) => {
            let head = changed.head.take().expect("built head");
            let snapshot = head.snapshot_id;
            if basis.seam_present {
                store.record_physical_v3_seam_planned_with_coverage(
                    gate, vehicle_id, snapshot, &coverage,
                )?;
            }
            if let Some(revision) = &basis.import_revision {
                store.stamp_physical_v3_import_head(gate, vehicle_id, snapshot, revision.run_id)?;
            }
            store.record_physical_v3_repair_layer(
                gate,
                vehicle_id,
                snapshot,
                changed.layer.as_ref(),
            )?;
            for (key, pre_rows) in &changed.pre_rows {
                if !store.record_drive_repair_pre_rows(gate, vehicle_id, key, pre_rows)? {
                    return Err(StoreError::ImportGenerationConflict.into());
                }
            }
            handback_crash_point("handback-recorded");
            let names = head.shared_names();
            if !names.is_empty() {
                store.extend_handback_candidate(run, gate, vehicle_id, run_id, names)?;
            }
            let published = head.publish(store, gate, vehicle_id, final_now)?;
            handback_crash_point("handback-rotated");
            build.month_for = Some(published.admission);
            if let Some(inputs) = changed.inputs.take() {
                let _phase = HandbackPhase::start(vehicle_id, "inputs");
                let recorded = crate::db::PhysicalV3HandbackPlan {
                    snapshot_id: snapshot.to_string(),
                    span: changed.span,
                    inputs,
                    prepared: false,
                };
                if let Err(error) =
                    store.record_physical_v3_handback_plan(gate, vehicle_id, Some(&recorded))
                {
                    tracing::warn!(%error, %vehicle_id, "the head's plan inputs were not recorded");
                }
            }
            if let Some(archive) = changed.conversion.take() {
                let _phase = HandbackPhase::start(vehicle_id, "conversion");
                if let Err(reason) =
                    finish_legacy_conversion(store, gate, vehicle_id, kept, scratch, archive)
                {
                    tracing::warn!(%vehicle_id, %reason, "legacy kept conversion deferred");
                }
            }
            HandbackPhysicalOutcome::Published(published.kind)
        }
    };
    Ok(Some(outcome))
}

/// Plans and publishes, after a handback and once at service start, the
/// physical 1.4 head of every vehicle with a kept stage whose current head
/// is refused (M03-T22), then settles each repair batch an interrupted apply
/// or undo left `publishing` (M03-T27c): served, it is complete; withheld,
/// its rows are restored and the head served before it is served again. The
/// caller holds the publication gate, so no apply is under way. A vehicle
/// that cannot be planned keeps its refusal and the next vehicle goes on.
pub(crate) fn publish_physical_after_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
) -> Result<Vec<(Uuid, HandbackPhysicalOutcome)>, SourceDbImportError> {
    // PG-1: the repair recovery below walks the heads again; the run's phase
    // lines add up both walks and log once when it ends.
    let _run = crate::physical_publication::HandbackRun::start();
    let outcomes = publish_physical_heads_after_handback(store, cursor_key, publication_gate)?;
    let vehicles = store.drive_repair_publishing_vehicles()?;
    if vehicles.is_empty() {
        return Ok(outcomes);
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_millis()).ok())
        .ok_or(crate::physical_publication::SourceDbPhysicalPublicationError::Clock)?;
    let holds = store.import_publication_holds()?;
    for vehicle_id in vehicles {
        // An import's pending publication goes first; its next call settles.
        if holds.pending(vehicle_id) {
            continue;
        }
        let Some(batch_id) = store.drive_repair_publishing_batch(vehicle_id)? else {
            continue;
        };
        let outcome = settle_drive_repair_batch(
            store,
            cursor_key,
            publication_gate,
            vehicle_id,
            batch_id,
            crate::db::DriveRepairActivation::default(),
            now_ms,
        )?;
        tracing::info!(%vehicle_id, batch_id, ?outcome, "an interrupted repair batch was settled");
    }
    Ok(outcomes)
}

/// The heads half of [`publish_physical_after_handback`]: plans and
/// publishes every refused head of a vehicle with a kept stage.
fn publish_physical_heads_after_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
) -> Result<Vec<(Uuid, HandbackPhysicalOutcome)>, SourceDbImportError> {
    lower_handback_thread_priority();
    // Issue #60: an interrupted import's publication is finished first, from
    // its capture, so no handback plans an older replay base in its place.
    // The sweep runs whether or not an intent is left, and may fail.
    let resume_phase = crate::physical_publication::HandbackPhase::run_level("resume");
    let resumed = resume_import_publications(store, cursor_key, publication_gate);
    drop(resume_phase);
    let resumed = resumed?;
    for (vehicle_id, outcome) in resumed {
        tracing::info!(%vehicle_id, ?outcome, "import publication resumed before the handback");
    }
    let mut outcomes = Vec::new();
    for vehicle_id in store.physical_v3_kept_vehicles()? {
        // A conflict is this vehicle's alone: it keeps its refusal and the
        // vehicles after it are still published (PG-2).
        let outcome =
            match publish_vehicle_after_handback(store, cursor_key, publication_gate, vehicle_id) {
                Err(SourceDbImportError::Store(StoreError::ImportGenerationConflict)) => {
                    HandbackPhysicalOutcome::Withheld(
                        "the seam met a conflicting write; the next handback, start or import \
                         plans it again"
                            .into(),
                    )
                }
                other => other?,
            };
        match &outcome {
            HandbackPhysicalOutcome::Restates => tracing::info!(
                %vehicle_id,
                "the window's plan restates an imported row; the 1.4 head stays refused until \
                 the next import"
            ),
            HandbackPhysicalOutcome::Withheld(reason) => tracing::warn!(
                %vehicle_id,
                %reason,
                "the 1.4 head was not planned after the handback; it stays refused until the \
                 next import"
            ),
            _ => {}
        }
        outcomes.push((vehicle_id, outcome));
    }
    Ok(outcomes)
}

fn publish_vehicle_after_handback(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
) -> Result<HandbackPhysicalOutcome, SourceDbImportError> {
    #[cfg(test)]
    if HANDBACK_CONFLICT_FOR.with(|conflict| conflict.get()) == Some(vehicle_id) {
        return Err(StoreError::ImportGenerationConflict.into());
    }
    let checks_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "checks");
    let Some(kept) = store.physical_v3_kept(vehicle_id)? else {
        return Ok(HandbackPhysicalOutcome::Pruned);
    };
    // Issue #60: while an import's publication is pending its capture may
    // already be the kept base, and the restart that finishes it needs every
    // file, so nothing is pruned or planned until it is resolved.
    if store.import_publication_holds()?.pending(vehicle_id) {
        return Ok(HandbackPhysicalOutcome::Withheld(
            "an import's publication is pending; the next start or window close finishes it"
                .into(),
        ));
    }
    if store.collection_coverage_unreconciled()? {
        return Ok(HandbackPhysicalOutcome::Withheld(
            "a window's handback has not reached the store's coverage yet".into(),
        ));
    }
    match store.physical_v3_current_head_served(vehicle_id)? {
        None => {
            return match forget_physical_kept(store, publication_gate, vehicle_id) {
                Ok(()) => Ok(HandbackPhysicalOutcome::Pruned),
                Err(reason) => Ok(HandbackPhysicalOutcome::Withheld(format!(
                    "the kept record was not forgotten: {reason}"
                ))),
            };
        }
        Some(true) => return Ok(HandbackPhysicalOutcome::Current),
        Some(false) => {}
    }
    // Issue #60: after a committed import only its own adopted base may be
    // planned, and the head is stamped with that import's revision.
    let revision = match store.physical_v3_import_revision(vehicle_id)? {
        None => None,
        Some(revision)
            if revision.kept.is_some()
                && revision.kept == kept.archive.as_ref().map(|archive| archive.generation)
                && store.import_publication_intent(vehicle_id)?.is_none() =>
        {
            Some(revision.run_id)
        }
        Some(_) => {
            return Ok(HandbackPhysicalOutcome::Withheld(
                "the kept base is not the committed import's; the next import publishes the head"
                    .into(),
            ));
        }
    };
    // An interrupted keep or conversion left files the record does not name.
    remove_superseded_kept_files(store, vehicle_id, Some(&kept));
    let withheld = |reason: String| Ok(HandbackPhysicalOutcome::Withheld(reason));
    let source_bytes = match std::fs::read(kept_source_path(store, vehicle_id, &kept)) {
        Ok(bytes) if Sha256Digest::of_bytes(&bytes).to_string() == kept.source => bytes,
        Ok(_) => return withheld("the kept source sessions differ from their record".into()),
        Err(error) => return withheld(format!("the kept source sessions: {error}")),
    };
    let source: KeptPhysicalSource = match serde_json::from_slice(&source_bytes) {
        Ok(source) => source,
        Err(error) => return withheld(format!("the kept source sessions: {error}")),
    };
    if source.binding.vehicle_id != vehicle_id {
        return withheld("the kept source sessions name another vehicle".into());
    }
    drop(checks_phase);
    // The head is planned with the stored rows the seam was planned from
    // (PG-2): rows a tick commits after the seam reach the next head.
    let seam_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "seam");
    let (run, committed, stored) = store.import_seam_for_handback_with_stored_rows(
        publication_gate,
        vehicle_id,
        source.binding.selected_car_id,
        &source.drives,
        &source.charges,
    )?;
    drop(seam_phase);
    if !committed {
        return Ok(HandbackPhysicalOutcome::Restates);
    }
    #[cfg(test)]
    if let Some(tick) = AFTER_HANDBACK_SEAM.with(|hook| hook.borrow_mut().take()) {
        tick();
    }
    #[cfg(test)]
    if FAIL_AFTER_HANDBACK_COMMIT.with(|fail| fail.replace(false)) {
        return Err(StoreError::InvalidImportGeneration.into());
    }
    let early_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "early_answer");
    let early = serve_unchanged_head(
        store,
        publication_gate,
        vehicle_id,
        &kept,
        &source.binding,
        &run.ahead.packs,
        &stored,
    );
    drop(early_phase);
    if let Some(recorded) = early? {
        if !recorded.prepared {
            let _phase =
                crate::physical_publication::HandbackPhase::start(vehicle_id, "prepared_month");
            prepare_month_of_served_head_with_gate(
                store,
                cursor_key,
                publication_gate,
                &kept,
                &source.binding,
                &run.ahead.packs,
                &stored,
                recorded,
            );
        }
        return Ok(HandbackPhysicalOutcome::Published(
            crate::physical_publication::PhysicalV3PublicationKind::Unchanged,
        ));
    }
    let open_phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "open");
    let opened = open_kept_working(store, vehicle_id, &kept, &source.binding);
    drop(open_phase);
    let (working, legacy) = match opened {
        Ok(opened) => opened,
        Err(reason) => return withheld(reason),
    };
    let binding = source.binding.clone();
    let outcome = match publish_planned_physical_route_with_stored_rows(
        store,
        cursor_key,
        source.binding,
        Some(working),
        publication_gate,
        Some(&run.ahead.packs),
        None,
        Some(&kept.stage),
        revision,
        Some(&stored),
    ) {
        Ok(Some(publication)) => Ok(HandbackPhysicalOutcome::Published(publication.kind)),
        Ok(None) => withheld("the window's plan cannot be held by the physical schema".into()),
        // The planned copy failed its check or could not be read: this
        // vehicle keeps its refusal, as for a damaged kept stage.
        Err(SourceDbImportError::PhysicalPublication(
            crate::physical_publication::SourceDbPhysicalPublicationError::Stage(error),
        )) => withheld(format!("the planned copy of the kept stage: {error}")),
        Err(error) => Err(error),
    };
    if let Some(legacy) = legacy {
        let _phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "conversion");
        convert_legacy_kept(store, publication_gate, &kept, &binding, &legacy, &source_bytes);
    }
    outcome
}

/// The pre-rows each of `components` would record at its first
/// application (M03-T27c): the vehicle's kept stage is opened as a handback
/// opens it, planned with the seam a rolled-back transaction works out, and
/// layered with `components` alone, then discarded. Nothing in the store or
/// in the kept files changes. The inner `Err` names why the head cannot be
/// planned now; activation then waits.
pub(crate) fn drive_repair_dry_run_pre_rows(
    store: &HubStore,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    components: &[crate::db::ActiveDriveRepair],
) -> Result<
    Result<Vec<([u8; 32], crate::import::broken_drive_repair::layer::ComponentPreRows)>, String>,
    SourceDbImportError,
> {
    let Some(kept) = store.physical_v3_kept(vehicle_id)? else {
        return Ok(Err("the vehicle has no kept stage to plan its head from".into()));
    };
    let source_bytes = match std::fs::read(kept_source_path(store, vehicle_id, &kept)) {
        Ok(bytes) if Sha256Digest::of_bytes(&bytes).to_string() == kept.source => bytes,
        Ok(_) => return Ok(Err("the kept source sessions differ from their record".into())),
        Err(error) => return Ok(Err(format!("the kept source sessions: {error}"))),
    };
    let source: KeptPhysicalSource = match serde_json::from_slice(&source_bytes) {
        Ok(source) => source,
        Err(error) => return Ok(Err(format!("the kept source sessions: {error}"))),
    };
    if source.binding.vehicle_id != vehicle_id {
        return Ok(Err("the kept source sessions name another vehicle".into()));
    }
    let (mut working, _legacy) = match open_kept_working(store, vehicle_id, &kept, &source.binding) {
        Ok(opened) => opened,
        Err(reason) => return Ok(Err(reason)),
    };
    let planned = if store.import_seam_present(vehicle_id)? {
        let run = store.dry_run_import_seam_ahead_of_packs(
            publication_gate,
            vehicle_id,
            source.binding.selected_car_id,
            &source.drives,
            &source.charges,
        )?;
        if !run.ahead.restated_drives.is_empty() || !run.ahead.restated_charges.is_empty() {
            return Ok(Err("the window's plan restates an imported row".into()));
        }
        plan_physical_stage(store, &source.binding, working.stage_mut(), &run.ahead.packs, None).map(|_| ())
    } else {
        Ok(())
    };
    let layered = planned.and_then(|()| {
        layer_drive_repair_components(store, vehicle_id, components, working.stage_mut())
    });
    match layered {
        Ok(pre_rows) => Ok(Ok(pre_rows)),
        Err(PhysicalPlanError::Unplannable(reason)) => Ok(Err(reason)),
        Err(PhysicalPlanError::Stage(error)) => {
            Ok(Err(format!("the planned copy of the kept stage: {error}")))
        }
        Err(PhysicalPlanError::Store(error)) => Err(error.into()),
    }
}

/// The early answer of a handback (M08-T19): when the vehicle's current head
/// was planned from inputs whose digest the inputs now have, its plan would
/// make the same stage again, so the same snapshot, and the full path would
/// end as `Unchanged` after recording the head with the new coverage. This
/// records it so, without opening, copying or planning the kept stage or
/// reading a pack, and returns the record. A head without recorded inputs
/// (a store an earlier build last planned) is planned in full, which
/// records them.
fn serve_unchanged_head(
    store: &HubStore,
    publication_gate: &PublicationGate,
    vehicle_id: Uuid,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    packs: &crate::import::source_db::seam::SeamPackPlan,
    stored: &crate::db::ImportSeamStoredRows,
) -> Result<Option<crate::db::PhysicalV3HandbackPlan>, SourceDbImportError> {
    let Some(recorded) = store.physical_v3_handback_plan(vehicle_id)? else {
        return Ok(None);
    };
    // Only a planned head is recorded, and only a planned head may be
    // served again: a vehicle without the seam publishes its raw stage.
    if !store.import_seam_present(vehicle_id)? {
        return Ok(None);
    }
    let Some(current) = store.physical_v3_current_snapshot_id(vehicle_id)? else {
        return Ok(None);
    };
    if current.to_string() != recorded.snapshot_id {
        return Ok(None);
    }
    let stored = match stored_rows_digest(stored) {
        Ok(stored) => stored,
        Err(_) => return Ok(None),
    };
    let Some(now_ms) = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_millis()).ok())
    else {
        return Ok(None);
    };
    let inputs = handback_plan_inputs(&kept.stage, binding, packs, recorded.span, &stored, now_ms);
    if inputs != recorded.inputs {
        return Ok(None);
    }
    store.record_physical_v3_seam_planned_with_coverage(
        publication_gate,
        vehicle_id,
        current,
        &crate::db::coverage_digest(packs.coverage().intervals()),
    )?;
    tracing::info!(
        %vehicle_id,
        snapshot_id = %current,
        "the handback's plan inputs are those of the current 1.4 head; it is served under the \
         new coverage without planning it again"
    );
    Ok(Some(recorded))
}

/// Builds the optional month in the attempt's private directory, then checks
/// the served receipt again before installing the pack and recording it.
#[allow(clippy::too_many_arguments)]
fn prepare_month_of_served_head(
    store: &HubStore,
    cursor_key: &CursorKey,
    clock: &HandbackGateClock<'_>,
    kept: &crate::db::PhysicalV3Kept,
    build: &mut HandbackBuild,
    scratch: &mut HandbackScratch,
    admission: &crate::db::PendingPhysicalV3Admission,
    now: i64,
) {
    use crate::sync::prepared_map::build_selected_month;
    let binding = &build.source.binding;
    let vehicle_id = binding.vehicle_id;
    let _phase = crate::physical_publication::HandbackPhase::start(vehicle_id, "prepared_month");
    let result = (|| -> Result<(), String> {
        if !scratch.created {
            scratch.create()?;
        }
        let month = match &mut build.plan {
            HandbackPlan::Changed(changed) => build_selected_month(
                store,
                cursor_key,
                changed.working.stage_mut(),
                admission,
                now,
                &scratch.path,
            ),
            HandbackPlan::Same(_) => {
                let (mut working, _) =
                    open_kept_working_in_scratch(store, kept, binding, &scratch.path)?;
                let stage = working.stage_mut();
                plan_physical_stage(store, binding, stage, &build.packs, Some(&build.stored))
                    .map_err(|error| format!("the plan: {error:?}"))?;
                let digest = stage
                    .sealed_content_digest()
                    .map_err(|error| error.to_string())?;
                if crate::physical_publication::physical_v3_snapshot_id(digest, binding)
                    != admission.snapshot_id
                {
                    return Err("the plan is not the served head's".into());
                }
                build_selected_month(store, cursor_key, stage, admission, now, &scratch.path)
            }
            HandbackPlan::Supersede(_) => return Ok(()),
        };
        finish_prepared_month(
            store,
            cursor_key,
            clock,
            admission,
            now,
            month.map_err(|error| error.to_string()),
        )
    })();
    if let Err(reason) = result {
        tracing::warn!(%vehicle_id, %reason, "optional prepared month is unavailable; local map generation remains authoritative");
    }
}

fn finish_prepared_month(
    store: &HubStore,
    cursor_key: &CursorKey,
    clock: &HandbackGateClock<'_>,
    admission: &crate::db::PendingPhysicalV3Admission,
    now: i64,
    month: Result<Option<crate::sync::prepared_map::PreparedMonth>, String>,
) -> Result<(), String> {
    let vehicle_id = admission.vehicle_id;
    let Some(gate) = clock.acquire(store).map_err(|error| error.to_string())? else {
        return Err("the publication gate wait expired".into());
    };
    if store
        .pending_physical_v3_control_admission_for_vehicle(vehicle_id)
        .map_err(|error| error.to_string())?
        .as_ref()
        != Some(admission)
    {
        return Err("the served head changed during preparation".into());
    }
    crate::sync::prepared_map::PreparedMonth::prune_stale_records(store, cursor_key, &gate, now)
        .map_err(|error| error.to_string())?;
    if let Some(month) = month? {
        month
            .publish(store, &gate, admission)
            .map_err(|error| error.to_string())?;
    }
    if let Some(mut recorded) = store
        .physical_v3_handback_plan(vehicle_id)
        .map_err(|error| error.to_string())?
        && recorded.snapshot_id == admission.snapshot_id.to_string()
    {
        recorded.prepared = true;
        store
            .record_physical_v3_handback_plan(&gate, vehicle_id, Some(&recorded))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// The optional prepared month of a head the early answer served, when the
/// preparation recorded with its inputs had failed (M08-T19): the kept
/// stage is authenticated, copied and planned as the full path does, its
/// planned copy checked and confirmed to be the served head's stage, and
/// only the month is prepared from it; nothing is published and no pack of
/// the head is read. A failure keeps the record unprepared for the next
/// handback; the head stays served either way.
#[allow(clippy::too_many_arguments)]
fn prepare_month_of_served_head_with_gate(
    store: &HubStore,
    cursor_key: &CursorKey,
    publication_gate: &PublicationGate,
    kept: &crate::db::PhysicalV3Kept,
    binding: &ProjectionBinding,
    packs: &crate::import::source_db::seam::SeamPackPlan,
    stored: &crate::db::ImportSeamStoredRows,
    mut recorded: crate::db::PhysicalV3HandbackPlan,
) {
    let vehicle_id = binding.vehicle_id;
    let prepared = (|| -> Result<(), String> {
        let (mut working, legacy) = open_kept_working(store, vehicle_id, kept, binding)?;
        drop(legacy);
        let result = (|| -> Result<(), String> {
            plan_physical_stage(store, binding, working.stage_mut(), packs, Some(stored))
                .map_err(|error| format!("the plan: {error:?}"))?;
            let digest = working
                .stage_mut()
                .sealed_content_digest()
                .map_err(|error| format!("the planned copy of the kept stage: {error}"))?;
            let snapshot_id =
                crate::physical_publication::physical_v3_snapshot_id(digest, binding);
            if snapshot_id.to_string() != recorded.snapshot_id {
                return Err("the plan is not the served head's".into());
            }
            let admission = store
                .pending_physical_v3_control_admission_for_vehicle(vehicle_id)
                .map_err(|error| error.to_string())?
                .filter(|admission| admission.snapshot_id == snapshot_id)
                .ok_or_else(|| "the head is not served".to_owned())?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|value| i64::try_from(value.as_millis()).ok())
                .ok_or_else(|| "the clock".to_owned())?;
            crate::sync::prepared_map::publish_selected_month(
                store,
                cursor_key,
                working.stage_mut(),
                &admission,
                now_ms,
            )
            .map_err(|error| error.to_string())
        })();
        let cleanup = working.take().discard();
        result?;
        cleanup.map_err(|error| error.to_string())
    })();
    match prepared {
        Ok(()) => {
            recorded.prepared = true;
            if let Err(error) =
                store.record_physical_v3_handback_plan(publication_gate, vehicle_id, Some(&recorded))
            {
                tracing::warn!(%vehicle_id, %error, "the prepared month was not recorded");
            }
        }
        Err(reason) => tracing::warn!(
            %vehicle_id,
            %reason,
            "optional prepared month is unavailable; local map generation remains authoritative"
        ),
    }
}

/// The thread the service runs a handback's publication on.
const HANDBACK_PUBLICATION_THREAD: &str = "hub-handback-publication";

/// Lowers the priority of the service's handback thread for the rest of its
/// life (M08-T19): it is that thread's only work. On Linux a thread's nice
/// value is set through its thread id; macOS sets a thread's QoS class only
/// through a C call this crate's forbid(unsafe_code) excludes, so there the
/// thread keeps its priority. Any other thread, such as a bounded
/// observation calling the handback inline, is left as it is.
fn lower_handback_thread_priority() {
    let handback_thread = std::thread::current().name() == Some(HANDBACK_PUBLICATION_THREAD);
    #[cfg(not(target_os = "linux"))]
    let _ = handback_thread;
    #[cfg(target_os = "linux")]
    if handback_thread {
        let thread = std::fs::read_link("/proc/thread-self")
            .ok()
            .and_then(|path| path.file_name()?.to_str()?.parse::<i32>().ok())
            .and_then(rustix::process::Pid::from_raw);
        if let Some(thread) = thread
            && let Err(error) = rustix::process::setpriority_process(Some(thread), 19)
        {
            tracing::debug!(%error, "the handback thread keeps its priority");
        }
    }
}

// The import publication intent and its restart (issue #60).
include!("import_publication.rs");

#[cfg(test)]
mod pg3_kept_tests {
    use super::*;
    use std::fs;

    #[test]
    fn pg3_live_candidate_survives_census_reclaim_and_both_import_sweeps() {
        let home = crate::private_tempdir().expect("private test store");
        let store = HubStore::initialize(home.path()).expect("initialise");
        let lock = HandbackRunFile::open(home.path()).expect("run lock");
        let _run = lock
            .try_acquire()
            .expect("try lock")
            .expect("run lock held");
        let vehicle = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let reserved = [
            format!("sha256/{}.sqlite.zst", "a".repeat(64)),
            format!("physical-kept/{vehicle}.{generation}.kept.partial"),
            format!("physical-kept/.staging/{vehicle}.sqlite"),
            format!("import-publication/{vehicle}.{generation}.updates.json"),
            format!("import-publication/.staging/{generation}.sqlite-wal"),
        ];
        let stray = [
            format!("sha256/{}.sqlite.zst", "b".repeat(64)),
            format!("physical-kept/{vehicle}.{}.kept.partial", Uuid::new_v4()),
            format!(
                "import-publication/{vehicle}.{}.updates.json",
                Uuid::new_v4()
            ),
            format!("import-publication/.staging/{}.sqlite-wal", Uuid::new_v4()),
        ];
        let gate = store.try_acquire_publication_gate().expect("gate");
        store
            .record_physical_v3_kept(
                &gate,
                vehicle,
                Some(&crate::db::PhysicalV3Kept {
                    stage: "c".repeat(64),
                    source: "d".repeat(64),
                    archive: Some(crate::db::PhysicalV3KeptArchive {
                        generation: Uuid::new_v4(),
                        format: 1,
                        digest: "e".repeat(64),
                        bytes: 3,
                        decoded_bytes: 3,
                    }),
                }),
            )
            .expect("record a current generation");
        store
            .record_handback_candidate(
                &_run,
                &gate,
                vehicle,
                &crate::db::HandbackCandidate {
                    run_id: Uuid::new_v4(),
                    started_at_ms: 1,
                    files: reserved.iter().map(Into::into).collect(),
                },
            )
            .expect("reserve names before creation");
        drop(gate);
        for name in reserved.iter().chain(&stray) {
            let path = store.packs_dir().join(name);
            fs::create_dir_all(path.parent().expect("parent")).expect("stage directory");
            fs::write(path, b"pg3").expect("synthetic file");
        }

        // A second start while this lock is held must not sweep the live row.
        crate::runtime::operator::authority::publish_window_after_handback(home.path());
        let census = store.storage_census().expect("census");
        let candidates = census.handback_candidates.expect("candidate class");
        assert_eq!((candidates.files, candidates.reclaimable_files), (5, 0));
        assert_eq!(census.orphans.reclaimable_files, 2);
        let gate = store.try_acquire_publication_gate().expect("cleaner gate");
        remove_unnamed_kept_files(&store, vehicle);
        sweep_import_publication_files(&store);
        drop(gate);
        assert_eq!(store.reclaim_storage().expect("reclaim").reclaimed_files, 1);
        let exists = |name: &String| store.packs_dir().join(name).exists();
        assert!(reserved.iter().all(exists));
        assert!(stray.iter().all(|name| !exists(name)));
    }
}
