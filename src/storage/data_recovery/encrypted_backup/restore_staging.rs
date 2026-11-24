// SPDX-License-Identifier: AGPL-3.0-only

//! Private staging shared by both restore formats and directory backups.
//! Each run creates and holds its own container. Names reserved for staging
//! are not evidence of who made an entry, so later runs leave them alone.

use std::{
    ffi::OsStr,
    fs::File,
    io,
    os::fd::{AsFd, OwnedFd},
    path::{Path, PathBuf},
};

use rustix::{
    fs::{
        AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, Stat, fchmod, flock,
        fstat, fsync, mkdirat, openat, renameat_with, statat, unlinkat,
    },
    io::Errno,
    process::getuid,
};
use uuid::Uuid;

use crate::data_recovery::{
    BackupMember, DataRecoveryError, PRIVATE_DIRECTORY_MODE, PRIVATE_FILE_MODE,
    backup_admission::{AdmittedDirectory, admit_private_directory, held_directory_path},
    encrypted_backup::hook, invalid, io_error,
};

/// The container used by both restore formats.
const RESTORE_STAGING_PREFIX: &str = ".teslatlas-encrypted-restore-";
const STAGING_SUFFIX: &str = ".staging";
/// Also used by older restores.
const BACKUP_STAGING_PREFIX: &str = ".teslatlas-data-recovery-";
const STAGED_DATA: &str = "data";
const MAX_TREE_DEPTH: usize = 8;

type Identity = (u64, u64);

fn uuid_between(name: &str, prefix: &str, suffix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix(suffix))
        .is_some_and(|id| Uuid::parse_str(id).is_ok_and(|uuid| uuid.hyphenated().to_string() == id))
}

/// Keep historical staging names reserved, including interrupted sweep names.
pub(in crate::storage::data_recovery) fn is_reserved_name(name: &str) -> bool {
    uuid_between(name, RESTORE_STAGING_PREFIX, STAGING_SUFFIX)
        || uuid_between(name, BACKUP_STAGING_PREFIX, STAGING_SUFFIX)
        || uuid_between(name, ".teslatlas-restore-sweep-", "")
}

fn open_directory_at(directory: &impl AsFd, name: &str) -> Result<OwnedFd, Errno> {
    openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

fn identity(stat: &Stat) -> Identity {
    #[allow(clippy::unnecessary_cast)]
    (stat.st_dev as u64, stat.st_ino as u64)
}

/// The identity of the entry `name` in `directory`, without following a link.
fn named_identity(directory: &impl AsFd, name: &OsStr) -> Option<Identity> {
    statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .ok()
        .as_ref()
        .map(identity)
}

/// Remove everything inside the held directory `directory`, through
/// descriptors only: no path is resolved and no link is followed (a link is
/// removed as an entry). Passes repeat until the directory reads empty.
fn remove_tree_at(directory: &OwnedFd, depth: usize) -> io::Result<()> {
    if depth > MAX_TREE_DEPTH {
        return Err(io::Error::other("the staging tree is deeper than a restore makes"));
    }
    loop {
        let mut removed = false;
        for entry in Dir::read_from(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            let stat = match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => stat,
                Err(Errno::NOENT) => continue,
                Err(error) => return Err(error.into()),
            };
            let outcome = if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                let child = openat(
                    directory,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?;
                remove_tree_at(&child, depth + 1)?;
                unlinkat(directory, name, AtFlags::REMOVEDIR)
            } else {
                unlinkat(directory, name, AtFlags::empty())
            };
            match outcome {
                Ok(()) | Err(Errno::NOENT) => removed = true,
                Err(error) => return Err(error.into()),
            }
        }
        if !removed {
            return Ok(());
        }
    }
}

/// A container created by this run. Its lock lives as long as its descriptor.
pub(in crate::storage::data_recovery) struct StagingDirectory {
    parent: AdmittedDirectory,
    name: String,
    directory: OwnedFd,
    identity: Identity,
    parent_identity: Identity,
    pub(in crate::storage::data_recovery) path: PathBuf,
    published: bool,
}

impl StagingDirectory {
    pub(in crate::storage::data_recovery) fn create(
        parent: AdmittedDirectory,
    ) -> Result<Self, DataRecoveryError> {
        Self::create_with_prefix(parent, BACKUP_STAGING_PREFIX)
    }

    fn create_with_prefix(
        parent: AdmittedDirectory,
        prefix: &str,
    ) -> Result<Self, DataRecoveryError> {
        let parent_stat = fstat(&parent.fd).map_err(|error| {
            io_error("inspecting the destination folder", &parent.path, error.into())
        })?;
        let parent_identity = identity(&parent_stat);
        for _ in 0..16 {
            let name = format!("{prefix}{}{STAGING_SUFFIX}", Uuid::new_v4());
            let shown = parent.path.join(&name);
            let failed = |operation, error: Errno| io_error(operation, &shown, error.into());
            match mkdirat(&parent.fd, name.as_str(), Mode::RWXU) {
                Ok(()) => {}
                Err(Errno::EXIST) => continue,
                Err(error) => return Err(failed("creating staging directory", error)),
            }
            let directory = open_directory_at(&parent.fd, &name)
                .map_err(|error| failed("opening staging directory", error))?;
            let stat = fstat(&directory)
                .map_err(|error| failed("inspecting staging directory", error))?;
            let identity = identity(&stat);
            if stat.st_uid != getuid().as_raw()
                || named_identity(&parent.fd, name.as_ref()) != Some(identity)
            {
                return Err(invalid("the staging directory changed as it was made"));
            }
            flock(&directory, FlockOperation::NonBlockingLockExclusive)
                .map_err(|error| failed("locking staging directory", error))?;
            let mut staging = Self {
                parent,
                name,
                directory,
                identity,
                parent_identity,
                path: shown.clone(),
                published: false,
            };
            fchmod(&staging.directory, Mode::from_raw_mode(PRIVATE_DIRECTORY_MODE as _))
                .map_err(|error| failed("protecting staging directory", error))?;
            admit_private_directory(&staging.directory, &shown)?;
            staging.check_bound()?;
            staging.path = held_directory_path(&staging.directory, &shown)?;
            return Ok(staging);
        }
        Err(invalid("could not allocate a unique sibling staging directory"))
    }

    fn shown(&self) -> PathBuf {
        self.parent.path.join(&self.name)
    }

    fn check_bound(&self) -> Result<(), DataRecoveryError> {
        if named_identity(&rustix::fs::CWD, self.parent.path.as_os_str()) != Some(self.parent_identity)
            || named_identity(&self.parent.fd, self.name.as_ref()) != Some(self.identity)
        {
            return Err(moved());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::storage::data_recovery) fn leave_as_killed(mut self) {
        self.published = true;
    }

    pub(in crate::storage::data_recovery) fn publish(
        mut self,
        destination: &Path,
    ) -> Result<(), DataRecoveryError> {
        fsync(&self.directory)
            .map_err(|error| io_error("syncing staged backup", &self.path, error.into()))?;
        self.check_bound()?;
        let target = self.target_name(destination)?;
        publish_at(&self.parent.fd, self.name.as_ref(), &self.parent.fd, target, destination)?;
        self.published = true;
        self.confirm_publication(target, self.identity)
    }

    fn target_name<'a>(&self, destination: &'a Path) -> Result<&'a OsStr, DataRecoveryError> {
        if destination.parent() != Some(self.parent.path.as_path()) {
            return Err(invalid("the target is not in the admitted folder"));
        }
        destination
            .file_name()
            .ok_or_else(|| invalid("destination must name a new directory"))
    }

    fn confirm_publication(&self, target: &OsStr, identity: Identity) -> Result<(), DataRecoveryError> {
        if named_identity(&self.parent.fd, target) != Some(identity) {
            return Err(invalid(
                "the published output is not the staged directory; check the target folder",
            ));
        }
        fsync(&self.parent.fd)
            .map_err(|error| io_error("syncing the destination parent", &self.parent.path, error.into()))
    }
}

fn publish_at(
    source: &OwnedFd,
    name: &OsStr,
    parent: &OwnedFd,
    target: &OsStr,
    destination: &Path,
) -> Result<(), DataRecoveryError> {
    renameat_with(source, name, parent, target, RenameFlags::NOREPLACE).map_err(|error| {
        io_error(
            "atomically publishing data-recovery directory without replacement",
            destination,
            error.into(),
        )
    })
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if !self.published
            && remove_tree_at(&self.directory, 0).is_ok()
            && named_identity(&self.parent.fd, self.name.as_ref()) == Some(self.identity)
        {
            let _ = unlinkat(&self.parent.fd, self.name.as_str(), AtFlags::REMOVEDIR);
        }
    }
}

fn moved() -> DataRecoveryError {
    invalid("the staging directory or its folder moved during recovery; nothing was published")
}

/// One restore tree. Path-based copy and catalogue operations use the held
/// data directory's working path. Publication uses the same directory's ID.
pub(in crate::storage::data_recovery) struct HeldStaging {
    container: StagingDirectory,
    data: OwnedFd,
    data_identity: Identity,
    sha256: OwnedFd,
    packs: OwnedFd,
    noop: Option<OwnedFd>,
    kept: Option<OwnedFd>,
    root: PathBuf,
}

impl HeldStaging {
    pub(in crate::storage::data_recovery) fn create(
        parent: AdmittedDirectory,
    ) -> Result<Self, DataRecoveryError> {
        let container = StagingDirectory::create_with_prefix(parent, RESTORE_STAGING_PREFIX)?;
        let shown = container.shown().join(STAGED_DATA);
        let data = Self::make_directory(&container.directory, STAGED_DATA, &shown)?;
        admit_private_directory(&data, &shown)?;
        let data_stat = fstat(&data)
            .map_err(|error| io_error("inspecting restored data directory", &shown, error.into()))?;
        let data_identity = identity(&data_stat);
        let root = held_directory_path(&data, &shown)?;
        let packs_path = shown.join("packs");
        let packs = Self::make_directory(&data, "packs", &packs_path)?;
        let sha256 = Self::make_directory(&packs, "sha256", &packs_path.join("sha256"))?;
        let staging = Self {
            container,
            data,
            data_identity,
            sha256,
            packs,
            noop: None,
            kept: None,
            root,
        };
        staging.check_bound()?;
        Ok(staging)
    }

    fn make_directory(
        directory: &OwnedFd,
        name: &str,
        shown: &Path,
    ) -> Result<OwnedFd, DataRecoveryError> {
        let failed = |error: Errno| io_error("creating restored data directory", shown, error.into());
        mkdirat(directory, name, Mode::RWXU).map_err(failed)?;
        let made = open_directory_at(directory, name).map_err(failed)?;
        fchmod(&made, Mode::from_raw_mode(PRIVATE_DIRECTORY_MODE as _)).map_err(failed)?;
        Ok(made)
    }

    pub(in crate::storage::data_recovery) fn root(&self) -> &Path {
        &self.root
    }

    /// Create one member's file, private and new, through the held
    /// directory its path names (the path was checked as canonical).
    pub(in crate::storage::data_recovery) fn create_member(&mut self, member: &BackupMember) -> Result<File, DataRecoveryError> {
        let relative = member
            .path
            .strip_prefix("data/")
            .ok_or_else(|| invalid("backup member is outside the data payload"))?;
        if relative.starts_with("packs/noop/") && self.noop.is_none() {
            self.noop = Some(Self::make_directory(&self.packs, "noop", &self.root.join("packs/noop"))?);
        }
        if relative.starts_with("packs/physical-kept/") && self.kept.is_none() {
            self.kept = Some(Self::make_directory(
                &self.packs, "physical-kept", &self.root.join("packs/physical-kept"),
            )?);
        }
        let (directory, name) = if relative == "hub.sqlite" {
            (Some(&self.data), relative)
        } else if let Some(name) = relative.strip_prefix("packs/sha256/") {
            (Some(&self.sha256), name)
        } else if let Some(name) = relative.strip_prefix("packs/noop/") {
            (self.noop.as_ref(), name)
        } else if let Some(name) = relative.strip_prefix("packs/physical-kept/") {
            (self.kept.as_ref(), name)
        } else {
            return Err(invalid(format!("unsafe data-backup member path: {}", member.path)));
        };
        let directory = directory.ok_or_else(|| invalid("staging is incomplete"))?;
        let shown = self.container.shown().join(STAGED_DATA).join(relative);
        let file = openat(
            directory,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(PRIVATE_FILE_MODE as _),
        )
        .map_err(|error| io_error("creating restored data member", &shown, error.into()))?;
        fchmod(&file, Mode::from_raw_mode(PRIVATE_FILE_MODE as _))
            .map_err(|error| io_error("protecting restored data member", &shown, error.into()))?;
        Ok(File::from(file))
    }

    pub(in crate::storage::data_recovery) fn check_bound(&self) -> Result<(), DataRecoveryError> {
        self.container.check_bound()?;
        if named_identity(&self.container.directory, STAGED_DATA.as_ref()) != Some(self.data_identity) {
            return Err(moved());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::storage::data_recovery) fn leave_as_killed(self) {
        self.container.leave_as_killed();
    }

    pub(in crate::storage::data_recovery) fn publish(
        self,
        destination: &Path,
    ) -> Result<(), DataRecoveryError> {
        fsync(&self.data)
            .map_err(|error| io_error("syncing the restored data directory", &self.root, error.into()))?;
        hook("restore_before_publish", &self.container.shown())?;
        self.check_bound()?;
        let target = self.container.target_name(destination)?;
        admit_private_directory(&self.data, &self.container.shown().join(STAGED_DATA))?;
        publish_at(
            &self.container.directory,
            STAGED_DATA.as_ref(),
            &self.container.parent.fd,
            target,
            destination,
        )?;
        self.container.confirm_publication(target, self.data_identity)
    }
}
