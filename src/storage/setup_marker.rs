// SPDX-License-Identifier: AGPL-3.0-only

//! "Set up before" (M02-T21, DEC-116): a durable marker outside the catalogue.
//!
//! `hub-setup.json` in the data folder records that this Hub was set up (the
//! Hub's own login was stored, or the full serve admission passed) and the
//! installation id of its catalogue. It lives outside `hub.sqlite`, so losing
//! or emptying the catalogue cannot also lose the record that there was one.
//! Every command that may create or initialise `hub.sqlite` checks it first:
//! with a marker, the catalogue must exist, open read-only and hold the same
//! installation id, or the command refuses and creates nothing. A Hub with a
//! marker is never treated as never configured, so it never starts idle.
//! Without a marker (a Hub from before it, or one that lost it), what
//! survives outside the catalogue tells an earlier set-up (DEC-120), except a
//! credential keyring that a sign-in wrote before it stopped, beside a
//! catalogue that was never configured: that keyring is moved aside (NOW-2).

use std::{
    fs,
    io::{self, Write as _},
    os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use uuid::Uuid;

/// The marker's file name in the data folder.
pub const SETUP_MARKER_FILE: &str = "hub-setup.json";

/// The start of the name of the dated, owner-only folder in `secrets/` that
/// an orphan credential keyring is moved into (NOW-2); the keyring keeps its
/// name inside it.
pub const KEYRING_QUARANTINE_PREFIX: &str = "credential-keys-quarantine-";

const MARKER_VERSION: u32 = 1;
const MARKER_MAX_BYTES: u64 = 4096;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Marker {
    version: u32,
    installation_id: Uuid,
}

/// Why a Hub that was set up before may not start.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SetupMarkerError {
    #[error(
        "this Hub was set up before (hub-setup.json), but its catalogue hub.sqlite is missing; restore the data folder from a backup (restore-data); nothing was created"
    )]
    CatalogueMissing,
    #[error(
        "this Hub was set up before (hub-setup.json), but its catalogue hub.sqlite is empty or unreadable; restore the data folder from a backup (restore-data); nothing was changed"
    )]
    CatalogueUnreadable,
    #[error(
        "this Hub was set up before (hub-setup.json), but hub.sqlite belongs to another installation; restore this Hub's data folder from a backup (restore-data); nothing was changed"
    )]
    CatalogueReplaced,
    #[error("the Hub setup marker (hub-setup.json) cannot be read or written: {0}")]
    MarkerUnavailable(String),
    #[error(
        "this Hub was set up before (its credential keyring secrets/credential-keys remains), but its catalogue hub.sqlite is missing, empty or new; restore the data folder from a backup (restore-data); nothing was created or changed"
    )]
    CatalogueLostWithoutMarker,
    #[error("whether this Hub was set up before cannot be checked (secrets/credential-keys): {0}; nothing was created or changed")]
    EarlierSetupUnknown(String),
    #[error(
        "this Hub was set up before (its credential keyring secrets/credential-keys remains), but SQLite cannot open or read its catalogue hub.sqlite read-only: {0}; the start would recover it or set it up as a new catalogue, so nothing was created or changed. Keep hub.sqlite and any hub.sqlite-journal beside it; give the Hub's user access to the file, or let SQLite finish the interrupted write (sqlite3 hub.sqlite 'PRAGMA quick_check' as the Hub's user), then start again; or restore the data folder from a backup (restore-data)"
    )]
    CatalogueUnresolvedBesideKeyring(String),
    #[error(
        "the credential keyring secrets/credential-keys is used by no stored credential pair and this Hub's catalogue was never configured (a sign-in stopped before its pair was stored), but it cannot be moved aside: {0}; keep every file, move secrets/credential-keys out of secrets/ by hand and start again"
    )]
    OrphanKeyringNotMoved(String),
}

pub fn marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SETUP_MARKER_FILE)
}

/// The installation id the marker records, `None` when there is no marker.
/// A marker that is present but unreadable or invalid is an error, never
/// "no marker".
pub fn read(data_dir: &Path) -> Result<Option<Uuid>, SetupMarkerError> {
    let path = marker_path(data_dir);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(SetupMarkerError::MarkerUnavailable(error.kind().to_string())),
    };
    if !metadata.file_type().is_file() || metadata.len() > MARKER_MAX_BYTES {
        return Err(SetupMarkerError::MarkerUnavailable("not a small regular file".to_owned()));
    }
    let bytes = fs::read(&path).map_err(|error| SetupMarkerError::MarkerUnavailable(error.kind().to_string()))?;
    let marker: Marker = serde_json::from_slice(&bytes)
        .map_err(|_| SetupMarkerError::MarkerUnavailable("not a setup marker".to_owned()))?;
    if marker.version != MARKER_VERSION {
        return Err(SetupMarkerError::MarkerUnavailable("unknown marker version".to_owned()));
    }
    Ok(Some(marker.installation_id))
}

/// Records that this Hub, with catalogue `installation_id`, was set up. An
/// existing marker for the same installation is kept; one for another
/// installation is refused. Written 0600 through a synced temporary file.
pub fn record(data_dir: &Path, installation_id: Uuid) -> Result<(), SetupMarkerError> {
    match read(data_dir)? {
        Some(recorded) if recorded == installation_id => return Ok(()),
        Some(_) => return Err(SetupMarkerError::CatalogueReplaced),
        None => {}
    }
    let unavailable = |error: io::Error| SetupMarkerError::MarkerUnavailable(error.kind().to_string());
    let bytes = serde_json::to_vec(&Marker {
        version: MARKER_VERSION,
        installation_id,
    })
    .map_err(|_| SetupMarkerError::MarkerUnavailable("not encodable".to_owned()))?;
    let staged = data_dir.join(format!(".{SETUP_MARKER_FILE}.{}.tmp", Uuid::new_v4()));
    let written = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&staged)
        .and_then(|mut file| {
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()
        })
        .and_then(|()| fs::rename(&staged, marker_path(data_dir)))
        .and_then(|()| fs::File::open(data_dir)?.sync_all());
    if let Err(error) = written {
        let _ = fs::remove_file(&staged);
        return Err(unavailable(error));
    }
    Ok(())
}

/// The check before `hub.sqlite` is created or initialised: with a marker,
/// the catalogue must exist, be readable and hold the marker's installation
/// id. Without a marker, see [`check_unmarked_catalogue`]. It writes nothing
/// but a marker taken from an intact catalogue that was set up.
pub fn check_catalogue(data_dir: &Path) -> Result<(), SetupMarkerError> {
    let Some(recorded) = read(data_dir)? else {
        return check_unmarked_catalogue(data_dir);
    };
    let catalogue = data_dir.join("hub.sqlite");
    match fs::symlink_metadata(&catalogue) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() > 0 => {}
        Ok(_) => return Err(SetupMarkerError::CatalogueUnreadable),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(SetupMarkerError::CatalogueMissing);
        }
        Err(_) => return Err(SetupMarkerError::CatalogueUnreadable),
    }
    let found = crate::db::HubStore::open_migration_candidate_read_only(data_dir)
        .and_then(|store| store.installation_id())
        .map_err(|_| SetupMarkerError::CatalogueUnreadable)?;
    if found != recorded {
        return Err(SetupMarkerError::CatalogueReplaced);
    }
    Ok(())
}

/// Without a marker (DEC-120): a Hub from before the marker, or one that lost
/// it. An intact catalogue that was set up (a configured car, a published
/// vehicle or a credential pair) gets its marker written from it. A missing
/// or empty catalogue, or an intact one holding no set-up, beside the
/// credential keyring (written outside the catalogue only when a credential
/// pair is stored) is refused and left as it is, and so is a SQLite file with
/// nothing in it beside the keyring (DEC-121). Beside the keyring, a catalogue
/// that SQLite itself cannot open or query read-only (a hot rollback journal,
/// no read permission, a damaged header) is refused too, since the start would
/// recover or initialise it. One exception (NOW-2): a keyring beside a
/// readable catalogue that was never configured, written by a sign-in that
/// stopped before its pair was stored, is moved aside and the Hub starts as a
/// new one (see [`quarantine_orphan_keyring`]). Anything else is a Hub never
/// set up. Any other
/// catalogue that SQLite reads but that does not open as a current one (an
/// older schema the start migrates, or one the start refuses itself) is left to
/// the start.
fn check_unmarked_catalogue(data_dir: &Path) -> Result<(), SetupMarkerError> {
    let keyring = data_dir
        .join("secrets")
        .join(crate::auth::credential_keys::KEYRING_NAME);
    let lost = match fs::symlink_metadata(keyring) {
        Ok(_) => Err(SetupMarkerError::CatalogueLostWithoutMarker),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => return Err(SetupMarkerError::EarlierSetupUnknown(error.kind().to_string())),
    };
    let catalogue = data_dir.join("hub.sqlite");
    match fs::symlink_metadata(&catalogue) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() > 0 => {}
        _ => return lost,
    }
    let set_up = crate::db::HubStore::open_migration_candidate_read_only(data_dir).and_then(|store| {
        let held = !store.configured_tesla_vehicles()?.is_empty()
            || !store.published_vehicles()?.is_empty()
            || store
                .credential_pair_presence(crate::auth::credential_envelope::CredentialSlot::OwnerApi)?
                .is_some()
            || store
                .credential_pair_presence(crate::auth::credential_envelope::CredentialSlot::FleetApi)?
                .is_some();
        held.then(|| store.installation_id()).transpose()
    });
    match set_up {
        Ok(Some(installation)) => record(data_dir, installation),
        Ok(None) if lost.is_err() => match quarantine_orphan_keyring(data_dir, &catalogue)? {
            Some(folder) => {
                tracing::warn!(
                    "moved the credential keyring secrets/credential-keys, which no stored credential pair uses, to secrets/{folder}/credential-keys: a sign-in stopped before its pair was stored and the catalogue was never configured; this Hub starts as a new one"
                );
                Ok(())
            }
            None => lost,
        },
        Ok(None) => lost,
        Err(_) if lost.is_err() => match probe(&catalogue) {
            // It does not open as a catalogue, but initialisation would take
            // it as a new one and give it a new installation identity (DEC-121).
            Probe::SchemaEmpty => lost,
            // SQLite cannot read it in its current state; the start would
            // recover it read-write and may then initialise it (NOW-1).
            Probe::Unresolved(found) => Err(SetupMarkerError::CatalogueUnresolvedBesideKeyring(found)),
            Probe::Readable => Ok(()),
        },
        Err(_) => Ok(()),
    }
}

/// A credential keyring beside a readable catalogue that holds no set-up is
/// the trace of a sign-in that wrote its key and stopped before the pair was
/// stored (NOW-2, DEC-121's recorded limit). Holding the Hub's lifetime lock,
/// so no Hub runs on this data folder meanwhile, it checks again that the
/// catalogue holds no credential pair, no legacy token row and no vehicle,
/// and that the keyring is a regular file in a real `secrets/` folder, then
/// moves the keyring, never deleting it, into a new dated owner-only folder
/// beside it and makes the move durable. It returns the folder's name, or
/// `None` when any of that does not hold or the lock is held (a running Hub,
/// or a `serve` that already holds it): the caller then refuses as before.
/// A stop after the move leaves no keyring, which the next start reads as a
/// Hub never set up; a stop before it leaves at most an empty dated folder.
fn quarantine_orphan_keyring(data_dir: &Path, catalogue: &Path) -> Result<Option<String>, SetupMarkerError> {
    let Ok(_lock) = crate::user_lifetime_lock::UserLifetimeLock::acquire(data_dir) else {
        return Ok(None);
    };
    if !never_configured(catalogue) {
        return Ok(None);
    }
    let secrets = data_dir.join("secrets");
    let keyring = secrets.join(crate::auth::credential_keys::KEYRING_NAME);
    match (fs::symlink_metadata(&secrets), fs::symlink_metadata(&keyring)) {
        (Ok(folder), Ok(file)) if folder.file_type().is_dir() && file.file_type().is_file() => {}
        _ => return Ok(None),
    }
    let now = time::OffsetDateTime::now_utc();
    let name = format!(
        "{KEYRING_QUARANTINE_PREFIX}{:04}{:02}{:02}T{:02}{:02}{:02}Z-{}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        &Uuid::new_v4().simple().to_string()[..8],
    );
    let folder = secrets.join(&name);
    let failed = |error: io::Error| SetupMarkerError::OrphanKeyringNotMoved(error.kind().to_string());
    fs::DirBuilder::new().mode(0o700).create(&folder).map_err(failed)?;
    let moved = fs::set_permissions(&folder, fs::Permissions::from_mode(0o700))
        .and_then(|()| fs::rename(&keyring, folder.join(crate::auth::credential_keys::KEYRING_NAME)));
    if let Err(error) = moved {
        let _ = fs::remove_dir(&folder);
        return Err(failed(error));
    }
    fs::File::open(&folder)
        .and_then(|file| file.sync_all())
        .and_then(|()| fs::File::open(&secrets)?.sync_all())
        .map_err(failed)?;
    Ok(Some(name))
}

/// The catalogue, read read-only by SQLite, has a `credential_pairs` table
/// and a `vehicles` table, both empty, and no row in a legacy token table.
/// Anything it cannot read counts as configured.
fn never_configured(catalogue: &Path) -> bool {
    let read = || -> rusqlite::Result<bool> {
        let connection = rusqlite::Connection::open_with_flags(
            catalogue,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let rows = |table: &str| -> rusqlite::Result<Option<i64>> {
            let present: i64 = connection.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )?;
            if present == 0 {
                return Ok(None);
            }
            connection
                .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| row.get(0))
                .map(Some)
        };
        Ok(rows("credential_pairs")? == Some(0)
            && rows("vehicles")? == Some(0)
            && rows("legacy_owner_tokens")?.unwrap_or(0) == 0
            && rows("fleet_tokens")?.unwrap_or(0) == 0)
    };
    read().unwrap_or(false)
}

/// What a read-only SQLite open of the catalogue finds.
#[derive(Debug, PartialEq, Eq)]
enum Probe {
    /// application_id 0, user_version 0 and an empty schema, the state that
    /// `HubStore::initialize` admits as a new catalogue.
    SchemaEmpty,
    /// SQLite reads it; whether it is a catalogue the start can use is the
    /// start's own check.
    Readable,
    /// SQLite cannot open or query it read-only; what was found.
    Unresolved(String),
}

fn probe(catalogue: &Path) -> Probe {
    let read = || -> rusqlite::Result<(i64, i64, i64)> {
        let connection = rusqlite::Connection::open_with_flags(
            catalogue,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let read = |sql: &str| connection.query_row(sql, [], |row| row.get::<_, i64>(0));
        Ok((
            read("PRAGMA application_id")?,
            read("PRAGMA user_version")?,
            read("SELECT count(*) FROM sqlite_schema")?,
        ))
    };
    match read() {
        Ok((0, 0, 0)) => Probe::SchemaEmpty,
        Ok(_) => Probe::Readable,
        Err(error) => Probe::Unresolved(unresolved_finding(&error)),
    }
}

fn unresolved_finding(error: &rusqlite::Error) -> String {
    use rusqlite::ErrorCode;
    match error.sqlite_error_code() {
        Some(ErrorCode::ReadOnly) => {
            "it has a hot rollback journal (hub.sqlite-journal) left by an interrupted write".to_owned()
        }
        Some(ErrorCode::CannotOpen | ErrorCode::PermissionDenied) => {
            "the file cannot be opened (check its owner and permissions)".to_owned()
        }
        Some(ErrorCode::NotADatabase) => "its header is not a SQLite database header (damaged)".to_owned(),
        Some(ErrorCode::DatabaseCorrupt) => "it is damaged".to_owned(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => "it is locked by another process".to_owned(),
        _ => format!("SQLite reports {error}"),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[test]
    fn fresh_install_journey_marker_absent_passes_and_creates_nothing() {
        let folder = tempfile::tempdir().expect("folder");
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert_eq!(read(folder.path()), Ok(None));
        assert!(!folder.path().join("hub.sqlite").exists());
    }

    #[test]
    fn fresh_install_journey_marker_refuses_a_missing_empty_or_replaced_catalogue() {
        // The store admits only a 0700 data folder; a plain tempdir follows the umask.
        let folder = crate::private_tempdir().expect("folder");
        let store = crate::db::HubStore::initialize(folder.path()).expect("store");
        let id = store.installation_id().expect("id");
        drop(store);
        record(folder.path(), id).expect("marker");
        record(folder.path(), id).expect("the same marker again");
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert_eq!(record(folder.path(), Uuid::new_v4()), Err(SetupMarkerError::CatalogueReplaced));

        let catalogue = folder.path().join("hub.sqlite");
        fs::OpenOptions::new().write(true).open(&catalogue).expect("open").set_len(0).expect("truncate");
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueUnreadable));
        assert_eq!(fs::metadata(&catalogue).expect("catalogue").len(), 0, "nothing initialised");

        for name in ["hub.sqlite", "hub.sqlite-wal", "hub.sqlite-shm"] {
            let _ = fs::remove_file(folder.path().join(name));
        }
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueMissing));
        assert!(!catalogue.exists(), "nothing created");

        drop(crate::db::HubStore::initialize(folder.path()).expect("another catalogue"));
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueReplaced));
    }

    /// A catalogue that was set up, with no marker, and the credential keyring
    /// beside it (written only when a credential pair is stored).
    fn catalogue_beside(keyring: bool) -> tempfile::TempDir {
        let folder = crate::private_tempdir().expect("folder");
        drop(crate::db::HubStore::initialize(folder.path()).expect("store"));
        if keyring {
            let secrets = folder.path().join("secrets");
            fs::create_dir(&secrets).expect("secrets");
            fs::write(secrets.join(crate::auth::credential_keys::KEYRING_NAME), b"keyring").expect("keyring");
        }
        folder
    }

    /// The three catalogue states SQLite cannot open or query read-only.
    fn make_unresolved(folder: &Path, state: &str) {
        let catalogue = folder.join("hub.sqlite");
        match state {
            "hot journal" => {
                let connection = rusqlite::Connection::open(&catalogue).expect("catalogue");
                let mode: String = connection
                    .query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))
                    .expect("journal mode");
                assert_eq!(mode, "delete");
                drop(connection);
                // A non-zero first byte and a non-empty catalogue make it hot.
                let mut journal = vec![0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
                journal.resize(512, 0);
                fs::write(folder.join("hub.sqlite-journal"), journal).expect("journal");
            }
            "unreadable" => fs::set_permissions(&catalogue, fs::Permissions::from_mode(0o000)).expect("chmod"),
            "damaged header" => {
                let mut file = fs::OpenOptions::new().write(true).open(&catalogue).expect("open");
                file.write_all(b"not a catalogue!").expect("damage");
                file.sync_all().expect("sync");
            }
            other => panic!("unknown state {other}"),
        }
    }

    fn snapshot(folder: &Path) -> Vec<(String, Vec<u8>)> {
        let mut entries: Vec<_> = fs::read_dir(folder)
            .expect("listing")
            .map(|entry| {
                let entry = entry.expect("entry");
                let path = entry.path();
                let bytes = if entry.file_type().expect("type").is_file() {
                    fs::read(&path).unwrap_or_default()
                } else {
                    Vec::new()
                };
                (entry.file_name().to_string_lossy().into_owned(), bytes)
            })
            .collect();
        entries.sort();
        entries
    }

    const UNRESOLVED: [&str; 3] = ["hot journal", "unreadable", "damaged header"];

    #[test]
    fn setup_marker_unresolved_catalogue_beside_a_keyring_refuses_and_changes_nothing() {
        for state in UNRESOLVED {
            let folder = catalogue_beside(true);
            make_unresolved(folder.path(), state);
            let before = snapshot(folder.path());
            let refused = check_catalogue(folder.path());
            assert!(
                matches!(refused, Err(SetupMarkerError::CatalogueUnresolvedBesideKeyring(_))),
                "{state}: {refused:?}"
            );
            let message = refused.unwrap_err().to_string();
            assert!(message.contains("restore-data"), "{state}: {message}");
            assert_eq!(snapshot(folder.path()), before, "{state}: nothing changed");
            assert_eq!(read(folder.path()), Ok(None), "{state}: no marker");
            fs::set_permissions(folder.path().join("hub.sqlite"), fs::Permissions::from_mode(0o600)).expect("chmod");
        }
    }

    #[test]
    fn setup_marker_unresolved_catalogue_says_what_was_found() {
        let found = |state| {
            let folder = catalogue_beside(true);
            make_unresolved(folder.path(), state);
            let refused = check_catalogue(folder.path());
            fs::set_permissions(folder.path().join("hub.sqlite"), fs::Permissions::from_mode(0o600)).expect("chmod");
            match refused {
                Err(SetupMarkerError::CatalogueUnresolvedBesideKeyring(found)) => found,
                other => panic!("{state}: {other:?}"),
            }
        };
        assert!(found("hot journal").contains("hot rollback journal"));
        assert!(found("unreadable").contains("cannot be opened"));
        assert!(found("damaged header").contains("header"));
    }

    #[test]
    fn setup_marker_unresolved_catalogue_without_a_keyring_is_left_to_the_start() {
        for state in UNRESOLVED {
            let folder = catalogue_beside(false);
            make_unresolved(folder.path(), state);
            let before = snapshot(folder.path());
            assert_eq!(check_catalogue(folder.path()), Ok(()), "{state}");
            assert_eq!(snapshot(folder.path()), before, "{state}: nothing changed");
            assert_eq!(read(folder.path()), Ok(None), "{state}: no marker");
            fs::set_permissions(folder.path().join("hub.sqlite"), fs::Permissions::from_mode(0o600)).expect("chmod");
        }
    }

    #[test]
    fn setup_marker_readable_catalogue_beside_a_keyring_passes_as_before() {
        // Marked and set up: it passes.
        let folder = catalogue_beside(true);
        let id = crate::db::HubStore::open_migration_candidate_read_only(folder.path())
            .and_then(|store| store.installation_id())
            .expect("id");
        record(folder.path(), id).expect("marker");
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        // Unmarked and holding no set-up: refused as lost while a Hub holds the
        // lifetime lock; otherwise the orphan keyring is moved aside (NOW-2).
        let folder = catalogue_beside(true);
        let running = crate::user_lifetime_lock::UserLifetimeLock::acquire(folder.path()).expect("lock");
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        drop(running);
        // Unmarked and holding no set-up, no keyring: a Hub never set up.
        let folder = catalogue_beside(false);
        assert_eq!(check_catalogue(folder.path()), Ok(()));
    }

    fn keyring_path(folder: &Path) -> PathBuf {
        folder.join("secrets").join(crate::auth::credential_keys::KEYRING_NAME)
    }

    fn quarantine_folders(folder: &Path) -> Vec<PathBuf> {
        let mut found: Vec<_> = fs::read_dir(folder.join("secrets"))
            .expect("secrets")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(KEYRING_QUARANTINE_PREFIX))
            })
            .collect();
        found.sort();
        found
    }

    /// The keyring is where it was, byte for byte, and nothing was moved aside.
    fn assert_keyring_kept(folder: &Path, case: &str) {
        assert_eq!(fs::read(keyring_path(folder)).expect(case), b"keyring", "{case}: keyring kept");
        assert!(quarantine_folders(folder).is_empty(), "{case}: nothing quarantined");
    }

    fn catalogue_connection(folder: &Path) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open(folder.join("hub.sqlite")).expect("catalogue");
        connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("foreign keys");
        connection
    }

    fn insert_pair(folder: &Path) {
        catalogue_connection(folder)
            .execute(
                "INSERT INTO credential_pairs(account_id, slot, origin, refresh_owner, generation,
                     access_envelope, refresh_envelope, refresh_present, refresh_fingerprint,
                     expires_at, next_refresh_at, client_id, region, migrated, written_at_ms)
                 VALUES (1, 'owner_api', 'hub_login', 'hub', ?1, ?2, ?2, 1, ?3, 2000, 1000, NULL, NULL, 0, 1)",
                rusqlite::params![Uuid::new_v4().to_string(), vec![1_u8; 100], vec![7_u8; 32]],
            )
            .expect("synthetic credential pair");
    }

    fn insert_vehicle(folder: &Path, configured: bool) -> Uuid {
        let vehicle = Uuid::new_v4();
        let connection = catalogue_connection(folder);
        connection
            .execute(
                "INSERT INTO vehicles(vehicle_id, source_id, source_vehicle_key, created_at_ms, last_seen_at_ms)
                 VALUES (?1, ?2, 'car-1', 1, 1)",
                rusqlite::params![vehicle.to_string(), Uuid::new_v4().to_string()],
            )
            .expect("vehicle");
        if configured {
            connection
                .execute(
                    "INSERT INTO v2_base_bindings(vehicle_id, snapshot_id, installation_id, account_id,
                         generation, selected_car_id) VALUES (?1, ?2, ?3, ?4, 1, 1)",
                    rusqlite::params![
                        vehicle.to_string(),
                        Uuid::new_v4().to_string(),
                        Uuid::new_v4().to_string(),
                        Uuid::new_v4().to_string()
                    ],
                )
                .expect("binding");
            connection
                .execute(
                    "INSERT INTO vehicle_identity_aliases(alias_kind, alias_value, vehicle_id, source_id,
                         source_vehicle_key) VALUES ('tesla_eid', '42', ?1, ?2, 'car-1')",
                    rusqlite::params![vehicle.to_string(), Uuid::new_v4().to_string()],
                )
                .expect("eid");
        }
        vehicle
    }

    #[test]
    fn setup_marker_orphan_keyring_is_quarantined_and_the_start_proceeds_as_fresh() {
        let folder = catalogue_beside(true);
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert!(!keyring_path(folder.path()).exists(), "the keyring is moved");
        let moved = quarantine_folders(folder.path());
        assert_eq!(moved.len(), 1, "{moved:?}");
        let name = moved[0].file_name().and_then(|name| name.to_str()).expect("name").to_owned();
        let stamp = &name[KEYRING_QUARANTINE_PREFIX.len()..];
        assert_eq!(stamp.len(), "20261121T120000Z-0123abcd".len(), "{name}");
        assert_eq!(&stamp[8..9], "T", "{name}");
        assert_eq!(&stamp[15..17], "Z-", "{name}");
        let mode = fs::metadata(&moved[0]).expect("folder").permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "owner-only folder");
        assert_eq!(
            fs::read(moved[0].join(crate::auth::credential_keys::KEYRING_NAME)).expect("quarantined keyring"),
            b"keyring",
            "its bytes are kept"
        );
        assert_eq!(read(folder.path()), Ok(None), "no marker: a Hub never set up");
        // The next start is a fresh Hub and moves nothing more.
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert_eq!(quarantine_folders(folder.path()), moved);
        drop(crate::db::HubStore::initialize(folder.path()).expect("the start opens the catalogue"));
    }

    #[test]
    fn setup_marker_keyring_in_use_or_configured_keeps_todays_behaviour() {
        // A stored credential pair: the catalogue was set up, it gets its marker.
        let folder = catalogue_beside(true);
        insert_pair(folder.path());
        assert_eq!(check_catalogue(folder.path()), Ok(()), "stored pair");
        assert_keyring_kept(folder.path(), "stored pair");

        // A configured vehicle: set up as well.
        let folder = catalogue_beside(true);
        insert_vehicle(folder.path(), true);
        assert_eq!(check_catalogue(folder.path()), Ok(()), "configured vehicle");
        assert_keyring_kept(folder.path(), "configured vehicle");

        // A vehicle that is not a configured car: refused as lost, as before.
        let folder = catalogue_beside(true);
        insert_vehicle(folder.path(), false);
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        assert_keyring_kept(folder.path(), "unconfigured vehicle");

        // A marker: the marked check, the keyring untouched.
        let folder = catalogue_beside(true);
        let id = crate::db::HubStore::open_migration_candidate_read_only(folder.path())
            .and_then(|store| store.installation_id())
            .expect("id");
        record(folder.path(), id).expect("marker");
        assert_eq!(check_catalogue(folder.path()), Ok(()), "marker");
        assert_keyring_kept(folder.path(), "marker");

        // An unresolved catalogue: NOW-1's refusal.
        let folder = catalogue_beside(true);
        make_unresolved(folder.path(), "damaged header");
        assert!(matches!(
            check_catalogue(folder.path()),
            Err(SetupMarkerError::CatalogueUnresolvedBesideKeyring(_))
        ));
        assert_keyring_kept(folder.path(), "unresolved");

        // A missing or schema-empty catalogue: lost, as before.
        let folder = catalogue_beside(true);
        for name in ["hub.sqlite", "hub.sqlite-wal", "hub.sqlite-shm"] {
            let _ = fs::remove_file(folder.path().join(name));
        }
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        assert_keyring_kept(folder.path(), "missing catalogue");
        rusqlite::Connection::open(folder.path().join("hub.sqlite"))
            .and_then(|connection| connection.execute_batch("CREATE TABLE t(x); DROP TABLE t;"))
            .expect("empty");
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        assert_keyring_kept(folder.path(), "schema-empty catalogue");
    }

    #[test]
    fn setup_marker_orphan_keyring_is_left_while_a_hub_holds_the_lifetime_lock() {
        let folder = catalogue_beside(true);
        let running = crate::user_lifetime_lock::UserLifetimeLock::acquire(folder.path()).expect("running Hub");
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        assert_keyring_kept(folder.path(), "lock held");
        drop(running);
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert_eq!(quarantine_folders(folder.path()).len(), 1);
    }

    #[test]
    fn setup_marker_orphan_keyring_after_a_stop_mid_quarantine_is_handled_by_the_next_start() {
        // Stopped after the move, before the log line: the keyring is in its
        // folder and the next start is a fresh Hub that moves nothing more.
        let folder = catalogue_beside(true);
        let moved = folder.path().join("secrets").join(format!("{KEYRING_QUARANTINE_PREFIX}20261121T120000Z-0123abcd"));
        fs::create_dir(&moved).expect("folder");
        fs::rename(keyring_path(folder.path()), moved.join(crate::auth::credential_keys::KEYRING_NAME)).expect("move");
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert_eq!(quarantine_folders(folder.path()), vec![moved.clone()]);
        assert_eq!(fs::read(moved.join(crate::auth::credential_keys::KEYRING_NAME)).expect("kept"), b"keyring");
        assert_eq!(read(folder.path()), Ok(None));

        // Stopped after the folder was made, before the move: the next start
        // moves the keyring into a new folder and leaves the empty one.
        let folder = catalogue_beside(true);
        let empty = folder.path().join("secrets").join(format!("{KEYRING_QUARANTINE_PREFIX}20261121T120000Z-0123abcd"));
        fs::create_dir(&empty).expect("folder");
        assert_eq!(check_catalogue(folder.path()), Ok(()));
        assert!(!keyring_path(folder.path()).exists());
        let folders = quarantine_folders(folder.path());
        assert_eq!(folders.len(), 2, "{folders:?}");
        let holding: Vec<_> = folders
            .iter()
            .filter(|path| path.join(crate::auth::credential_keys::KEYRING_NAME).exists())
            .collect();
        assert_eq!(holding.len(), 1);
        assert_ne!(holding[0], &empty);
    }

    #[test]
    fn setup_marker_probe_tells_empty_readable_and_unresolved() {
        let folder = catalogue_beside(false);
        let catalogue = folder.path().join("hub.sqlite");
        assert_eq!(probe(&catalogue), Probe::Readable);
        let empty = folder.path().join("empty.sqlite");
        rusqlite::Connection::open(&empty)
            .and_then(|connection| connection.execute_batch("CREATE TABLE t(x); DROP TABLE t;"))
            .expect("empty");
        assert_eq!(probe(&empty), Probe::SchemaEmpty);
        make_unresolved(folder.path(), "damaged header");
        assert!(matches!(probe(&catalogue), Probe::Unresolved(_)));
    }
}
