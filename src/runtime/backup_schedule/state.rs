// SPDX-License-Identifier: AGPL-3.0-only

//! What the schedule remembers across a restart: when the last attempt and the
//! last success happened, so a restart neither repeats a backup that is not
//! due nor skips one that is overdue. It holds times, a failure code and a
//! generation identifier; never a password, a path or any content.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use serde::{Deserialize, Serialize};

/// The state file's name inside the Hub data directory.
pub const STATE_FILE: &str = "backup-schedule.json";
const STATE_MAX_BYTES: u64 = 4096;
const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleState {
    #[serde(default)]
    pub version: u32,
    /// Start of the latest attempt, scheduled or manual (Unix milliseconds).
    #[serde(default)]
    pub last_attempt_ms: Option<i64>,
    /// Start of the latest successful run.
    #[serde(default)]
    pub last_success_ms: Option<i64>,
    /// The refusal code of the latest attempt; absent when it succeeded.
    #[serde(default)]
    pub last_failure_code: Option<String>,
    /// The generation the latest success wrote.
    #[serde(default)]
    pub last_generation: Option<String>,
}

/// Loads the state; a missing, oversized or damaged file is no state, so a
/// backup runs when it is next due rather than never.
pub fn load(path: &Path) -> ScheduleState {
    let Ok(file) = File::open(path) else {
        return ScheduleState::default();
    };
    let mut text = String::new();
    if file
        .take(STATE_MAX_BYTES + 1)
        .read_to_string(&mut text)
        .is_err()
        || text.len() as u64 > STATE_MAX_BYTES
    {
        tracing::warn!("the backup schedule state is unreadable; scheduling as if no backup had run");
        return ScheduleState::default();
    }
    match serde_json::from_str::<ScheduleState>(&text) {
        Ok(state) if state.version == STATE_VERSION => state,
        _ => {
            tracing::warn!("the backup schedule state is damaged; scheduling as if no backup had run");
            ScheduleState::default()
        }
    }
}

/// Writes the state atomically (a private temporary file, then a rename).
pub fn store(path: &Path, state: &ScheduleState) -> std::io::Result<()> {
    let mut state = state.clone();
    state.version = STATE_VERSION;
    let bytes = serde_json::to_vec(&state).map_err(std::io::Error::other)?;
    let temporary = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_schedule_state_reports_directory_sync_failure() {
        if rustix::process::geteuid().as_raw() == 0 {
            return;
        }

        let directory = tempfile::tempdir().expect("temporary schedule directory");
        let path = directory.path().join(STATE_FILE);
        let state = ScheduleState {
            last_generation: Some("synthetic-generation".to_owned()),
            ..ScheduleState::default()
        };

        // Writes and renames remain possible, but opening the directory to sync it fails.
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o300))
            .expect("restrict schedule directory permissions");
        let result = store(&path, &state);
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("restore schedule directory permissions");

        let error = result.expect_err("the directory sync error must be reported");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(load(&path).last_generation, state.last_generation);
    }
}
