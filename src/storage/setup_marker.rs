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
//! survives outside the catalogue tells an earlier set-up (DEC-120).

use std::{
    fs,
    io::{self, Write as _},
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
};

use uuid::Uuid;

/// The marker's file name in the data folder.
pub const SETUP_MARKER_FILE: &str = "hub-setup.json";

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
/// recover or initialise it. Anything else is a Hub never set up. Any other
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
        // Unmarked and holding no set-up: refused as lost, as before.
        let folder = catalogue_beside(true);
        assert_eq!(check_catalogue(folder.path()), Err(SetupMarkerError::CatalogueLostWithoutMarker));
        // Unmarked and holding no set-up, no keyring: a Hub never set up.
        let folder = catalogue_beside(false);
        assert_eq!(check_catalogue(folder.path()), Ok(()));
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
