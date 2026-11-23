// SPDX-License-Identifier: AGPL-3.0-only

//! The service's window authority (design 2.8, rulings R5 and R9).
//!
//! The running service builds one window controller on the process-wide
//! egress gate, so opening a window is what lets this process send Tesla
//! traffic and closing it is what stops every send. A supervisor thread drives
//! the controller and the collector slot; the local control channel forwards
//! the operator's window, setup, recovery and purge requests to
//! [`OperatorService`].
//! The API is served whatever the collector does: the collector runs only
//! inside an Open window, and its exit never stops the service.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use super::{
    admission::OperatorRefusal,
    channel::{FleetSignInInput, OperatorHandler, OperatorRequest, SetupMaterial},
    trust::{DialogConfirmation, TrustAuthority},
};
use crate::collection::collector::IntakeRecorder;
use crate::collection::window::{
    AnchorStore, CloseReason, CollectorSlot, DriveActivity, EgressGate, GRANT_MAX_BYTES, GraceDrive,
    Handback, HandbackSource,
    HighWater, HostClaim, KeychainAnchor, NewestHubRow, OffsetProbe, Phase, ResumeOutcome, SntpProbe,
    Supervisor, SystemClock, WindowConfig, WindowController, WindowDeps,
    WindowStatus,
};

/// Login-keychain service holding the Hub's consumption anchor (ruling R3).
pub const HUB_ANCHOR_SERVICE: &str = "Tesatlas Hub Window";
/// Login-keychain service holding the issuer's ledger pin (ruling R2).
pub const ISSUER_ANCHOR_SERVICE: &str = "Tesatlas Window Issuer";
/// The network time server of ruling R4 (not a Tesla endpoint).
pub const NTP_SERVER: &str = "time.apple.com:123";
const NTP_TIMEOUT: Duration = Duration::from_secs(5);

pub const WINDOW_CLOSED: &str = "window_closed";
/// Refusal once the service has accepted a stop (issue #38): no new
/// operator work is admitted while it drains.
pub const SERVICE_STOPPING: &str = "service_stopping";
pub const TRUST_DOCUMENT_MAX_BYTES: usize = 4096;

/// Window dependencies for the running service: the system clock and the
/// process-wide egress gate that every Tesla send in this process asks.
pub fn service_window_deps(
    anchor: Arc<dyn AnchorStore>,
    probe: Arc<dyn OffsetProbe>,
    handback: Arc<dyn HandbackSource>,
) -> WindowDeps {
    // The gate is created first so its system clock and this one share an
    // origin wherever the monotonic reading is process-scoped.
    let gate = EgressGate::process_wide().clone();
    WindowDeps {
        clock: Arc::new(SystemClock::new()),
        anchor,
        probe,
        gate,
        handback,
    }
}

pub fn service_window_config(data_dir: &Path, home: &Path, host_id: String) -> WindowConfig {
    WindowConfig {
        dir: super::window_dir(data_dir),
        claim_path: HostClaim::default_path(home),
        host_id,
    }
}

/// Starts the service's window authority for `data_dir` (it resumes or closes
/// whatever window the directory holds; design 2.9).
pub fn start_service_authority(
    data_dir: &Path,
    home: &Path,
) -> Result<(Arc<WindowController>, ResumeOutcome), OperatorRefusal> {
    let host_id = super::host::this_host_id()?;
    let (controller, outcome) = WindowController::start(
        service_window_config(data_dir, home, host_id),
        service_window_deps(
            Arc::new(KeychainAnchor::new(HUB_ANCHOR_SERVICE)),
            Arc::new(SntpProbe::new(NTP_SERVER, NTP_TIMEOUT)),
            Arc::new(StoreHighWater::new(data_dir)),
        ),
    );
    Ok((Arc::new(controller), outcome))
}

/// Per-table high-water marks read from the Hub database (read-only).
#[derive(Debug, Clone)]
pub struct StoreHighWater {
    database: PathBuf,
}

impl StoreHighWater {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            database: data_dir.join("hub.sqlite"),
        }
    }

    fn read(&self) -> Option<HighWater> {
        let connection = rusqlite::Connection::open_with_flags(
            &self.database,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .ok()?;
        // The high water is the largest row id the Hub allocated itself: an
        // id in its reserved range (M03-T12). SQLite's rowid counts insertions
        // and imported rows keep their source ids, so neither is it. Each
        // vehicle's maximum is a seek on the table's (vehicle_id, id) key.
        let greatest = |table: &str, column: &str| -> Option<i64> {
            connection
                .query_row(
                    &format!(
                        "SELECT MAX((SELECT MAX(t.{column}) FROM {table} AS t
                                      WHERE t.vehicle_id = v.vehicle_id
                                        AND t.{column} BETWEEN ?1 AND ?2))
                           FROM vehicles AS v"
                    ),
                    [
                        crate::db::hub_ids::HUB_ID_BASE,
                        crate::db::hub_ids::HUB_ID_MAX,
                    ],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .ok()
                .flatten()
                .filter(|id| crate::db::hub_ids::is_hub_id(*id))
        };
        Some(HighWater {
            drives: greatest("materialised_drives", "drive_id"),
            charges: greatest("materialised_charges", "charge_id"),
            positions: greatest("materialised_positions", "position_id"),
        })
    }
}

impl StoreHighWater {
    /// The newest instant among the Hub's own positions and charge samples,
    /// stored or still in an open session, in `[from_ms, until_ms)`; these are
    /// the rows an import seam keeps or tombstones by instant. `None` when the
    /// store cannot be read.
    fn newest_row_ms(&self, from_ms: i64, until_ms: i64) -> Option<Option<i64>> {
        let connection = rusqlite::Connection::open_with_flags(
            &self.database,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .ok()?;
        // Hub-range ids per vehicle are a seek on each table's key.
        connection
            .query_row(
                "SELECT MAX(at_ms) FROM (
                     SELECT CAST(json_extract(position_json, '$.date_ms') AS INTEGER) AS at_ms
                       FROM materialised_positions
                      WHERE vehicle_id IN (SELECT vehicle_id FROM vehicles)
                        AND position_id BETWEEN ?1 AND ?2
                     UNION ALL
                     SELECT CAST(json_extract(sample_json, '$.timestamp_ms') AS INTEGER)
                       FROM materialised_charge_samples
                      WHERE vehicle_id IN (SELECT vehicle_id FROM vehicles)
                        AND sample_id BETWEEN ?1 AND ?2
                     UNION ALL
                     SELECT CAST(json_extract(row_json,
                                CASE domain WHEN 'position' THEN '$.date_ms'
                                            ELSE '$.timestamp_ms' END) AS INTEGER)
                       FROM lifecycle_open_rows
                      WHERE domain IN ('position', 'charge_sample')
                        AND source_row_id BETWEEN ?1 AND ?2)
                  WHERE at_ms >= ?3 AND at_ms < ?4",
                rusqlite::params![
                    crate::db::hub_ids::HUB_ID_BASE,
                    crate::db::hub_ids::HUB_ID_MAX,
                    from_ms,
                    until_ms,
                ],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
    }
}

impl HandbackSource for StoreHighWater {
    fn high_water(&self) -> HighWater {
        self.read().unwrap_or_default()
    }

    fn newest_row(&self, from_ms: i64, until_ms: i64) -> NewestHubRow {
        match self.newest_row_ms(from_ms, until_ms) {
            None => NewestHubRow::Unknown,
            Some(None) => NewestHubRow::Absent,
            Some(Some(at_ms)) => NewestHubRow::At(at_ms),
        }
    }

    /// The first live open drive the Hub's lifecycle holds, by vehicle: its
    /// own notion of a live open drive (a Hub-range drive, or one inherited
    /// from an import that the live collector extends). A quarantined
    /// lifecycle never qualifies.
    fn drive_activity(&self) -> DriveActivity {
        let read = || -> Option<Option<GraceDrive>> {
            let connection = self.connect().ok()?;
            for vehicle_id in Self::lifecycle_vehicles(&connection)? {
                if let Some(drive_id) =
                    crate::db::live_open_drive_in_connection(&connection, &vehicle_id).ok()?
                {
                    return Some(Some(GraceDrive {
                        vehicle_id,
                        drive_id,
                    }));
                }
            }
            Some(None)
        };
        match read() {
            Some(Some(drive)) => DriveActivity::Driving(drive),
            Some(None) => DriveActivity::NotDriving,
            None => DriveActivity::Unknown,
        }
    }

    /// Whether that drive is still the vehicle's live open drive.
    fn drive_open(&self, drive: &GraceDrive) -> Option<bool> {
        let connection = self.connect().ok()?;
        if !Self::lifecycle_vehicles(&connection)?.contains(&drive.vehicle_id) {
            return Some(false);
        }
        let open = crate::db::live_open_drive_in_connection(&connection, &drive.vehicle_id).ok()?;
        Some(open == Some(drive.drive_id))
    }
}

impl StoreHighWater {
    /// The registered vehicles with a lifecycle that is not quarantined.
    fn lifecycle_vehicles(connection: &rusqlite::Connection) -> Option<Vec<String>> {
        let mut statement = connection
            .prepare(
                "SELECT state.vehicle_id FROM vehicle_lifecycle_state AS state
                   JOIN vehicles ON vehicles.vehicle_id = state.vehicle_id
                  WHERE state.quarantined = 0
                  ORDER BY state.vehicle_id",
            )
            .ok()?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .ok()?
            .collect::<rusqlite::Result<Vec<_>>>()
            .ok()
    }

    fn connect(&self) -> rusqlite::Result<rusqlite::Connection> {
        rusqlite::Connection::open_with_flags(
            &self.database,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    }
}

/// Refusal while the service waits for its credential recovery.
pub const CREDENTIAL_RECOVERY_PENDING: &str = "credential_recovery_pending";

/// Whether the data directory holds data a cursor key signed (sync
/// manifests or heads) but no cursor key: a data-only restore. Starting the
/// API now would create a new key that can never match the signed data and
/// would make the original key unrestorable (review finding 2).
pub fn credential_recovery_pending(data_dir: &Path) -> bool {
    let key = crate::legacy_owner_credentials::cursor_key_path(data_dir);
    if !matches!(std::fs::symlink_metadata(&key), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return false;
    }
    let Ok(connection) = rusqlite::Connection::open_with_flags(
        data_dir.join("hub.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return false;
    };
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sync_manifests) OR EXISTS(SELECT 1 FROM sync_heads)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
}

/// The service's recovery state (review finding 2). While pending, the
/// service answers only window status, window close and credential restore:
/// the API has not started, so no key has been generated and nothing signed,
/// and the collector does not start. A successful restore ends it, and the
/// service goes on to start the API with the recovered key.
#[derive(Debug)]
pub struct RecoveryGate {
    pending: tokio::sync::watch::Sender<bool>,
}

impl RecoveryGate {
    pub fn new(pending: bool) -> Arc<Self> {
        Arc::new(Self {
            pending: tokio::sync::watch::Sender::new(pending),
        })
    }

    pub fn pending(&self) -> bool {
        *self.pending.borrow()
    }

    fn complete(&self) {
        self.pending.send_replace(false);
    }

    /// Resolves once the recovery has completed (at once if none was due).
    pub async fn restored(&self) {
        let mut receiver = self.pending.subscribe();
        let _ = receiver.wait_for(|pending| !pending).await;
    }
}

/// The redacted window status the operator commands print.
pub fn status_json(status: &WindowStatus) -> Value {
    json!({
        "phase": status.phase,
        "trusted": status.trusted,
        "faulted": status.faulted,
        "closeReason": status.close_reason,
        "seq": status.seq,
        "grantDigest": status.grant_digest,
        "startsAt": status.starts_at,
        "deadline": status.deadline,
        "openedAt": status.opened_at,
        "closingAt": status.closing_at,
        "driveGraceSeconds": status.drive_grace_seconds,
        "inDriveGrace": status.in_drive_grace,
        "driveGraceUntil": status.drive_grace_until,
        "lastHandback": status.last_handback,
    })
}

/// What only the binary can do inside the service: the code exchange with
/// discovery, and credential recovery on the service's own store.
pub trait ServiceBackend: Send + Sync {
    /// Called only while the window is Open.
    fn setup(
        &self,
        material: SetupMaterial,
        vehicle_id: Option<i64>,
        all_vehicles: bool,
    ) -> Result<Value, OperatorRefusal>;
    /// `setup-fleet begin` (M09-T09). Called only while the window is Open.
    fn fleet_sign_in_begin(&self, _input: FleetSignInInput) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_sign_in_unavailable",
            "this service has no Fleet sign-in",
        ))
    }
    /// `setup-fleet` (M09-T09). Called only while the window is Open.
    fn fleet_sign_in_redeem(
        &self,
        _input: FleetSignInInput,
        _vehicle_id: Option<i64>,
        _all_vehicles: bool,
    ) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_sign_in_unavailable",
            "this service has no Fleet sign-in",
        ))
    }
    /// `setup-fleet discover` (M09-T19). Called only while the window is Open.
    fn fleet_discover(&self, _vehicle_id: Option<i64>, _all_vehicles: bool) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_sign_in_unavailable",
            "this service has no Fleet sign-in",
        ))
    }
    /// `setup-fleet activate` (M09-T19): no send, so no window needed.
    fn fleet_activate(&self) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_sign_in_unavailable",
            "this service has no Fleet sign-in",
        ))
    }
    /// `setup-fleet public-key` (M09-T18): no send, so no window needed.
    fn fleet_public_key(&self) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_partner_unavailable",
            "this service has no Fleet partner registration",
        ))
    }
    /// `setup-fleet register` (M09-T18). Called only while the window is Open.
    fn fleet_partner_register(&self, _input: FleetSignInInput) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "fleet_partner_unavailable",
            "this service has no Fleet partner registration",
        ))
    }
    /// Called only while the window is Closed, holding the import lock.
    fn recovery_export(
        &self,
        destination: &Path,
        recovery_key_file: &Path,
    ) -> Result<Value, OperatorRefusal>;
    /// Called only while the window is Closed, holding the import lock.
    fn recovery_restore(
        &self,
        source: &Path,
        recovery_key_file: &Path,
    ) -> Result<Value, OperatorRefusal>;
    /// Lists the refused credentials; deletes and writes nothing (M02-T11).
    fn refused_credentials(&self) -> Result<Value, OperatorRefusal>;
    /// Removes the refused credentials (M02-T11). Called only while the
    /// window is Closed, holding the import lock.
    fn purge_refused_credentials(&self) -> Result<Value, OperatorRefusal>;
    /// Runs one encrypted backup now (M06-T03): the service's backup
    /// schedule runs it, so a scheduled run and this one never overlap.
    fn backup_now(
        &self,
        _destination: Option<&Path>,
        _password_file: Option<&Path>,
    ) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "backup_unavailable",
            "this service has no backup schedule",
        ))
    }
    /// The backup schedule's state (M06-T04), read-only.
    fn backup_status(&self) -> Result<Value, OperatorRefusal> {
        Err(OperatorRefusal::new(
            "backup_unavailable",
            "this service has no backup schedule",
        ))
    }
    /// The broken-drive repair controls (M03-T28b); a service without the
    /// repair schedule has none.
    fn repair_control(&self) -> Option<&RepairControl> {
        None
    }
}

/// The service's trust authority: the issuer pin in the login keychain and
/// the owner's confirmation dialog (review finding 3).
pub fn service_trust_authority(host_id: &str) -> TrustAuthority {
    TrustAuthority::new(
        Arc::new(KeychainAnchor::new(HUB_ANCHOR_SERVICE)),
        host_id,
        Arc::new(DialogConfirmation),
    )
}

/// Answers the control channel inside the running service. The channel
/// hands it only requests from an authenticated requester (this user's Hub
/// executable); any other process is refused there, before its request is
/// read, so nothing here runs for it (M02-T11 review).
pub struct OperatorService {
    controller: Arc<WindowController>,
    backend: Arc<dyn ServiceBackend>,
    kick: Box<dyn Fn() + Send + Sync>,
    trust: TrustAuthority,
    window_dir: PathBuf,
    recovery: Arc<RecoveryGate>,
    /// The collector's intake latency recorder (M04-T11), read only.
    intake: &'static IntakeRecorder,
    /// The service's shutdown budget: once a stop is accepted only the
    /// window status is answered (issue #38, review F2).
    shutdown: Arc<crate::runtime::shutdown::ShutdownBudget>,
}

impl OperatorService {
    /// `kick` wakes the supervisor after a window change; `trust` decides
    /// who may change the trusted issuer of the window in `data_dir`.
    pub fn new(
        controller: Arc<WindowController>,
        backend: Arc<dyn ServiceBackend>,
        kick: Box<dyn Fn() + Send + Sync>,
        trust: TrustAuthority,
        data_dir: &Path,
    ) -> Self {
        Self {
            controller,
            backend,
            kick,
            trust,
            window_dir: super::window_dir(data_dir),
            recovery: RecoveryGate::new(false),
            intake: IntakeRecorder::process_wide(),
            shutdown: Arc::new(crate::runtime::shutdown::ShutdownBudget::system()),
        }
    }

    /// Refuses every request but the window status once `budget` has
    /// accepted a stop.
    pub fn with_shutdown(mut self, budget: Arc<crate::runtime::shutdown::ShutdownBudget>) -> Self {
        self.shutdown = budget;
        self
    }

    /// Answers with the intake latency figures of `recorder` (the process's own by
    /// default).
    pub fn with_intake_latency(mut self, recorder: &'static IntakeRecorder) -> Self {
        self.intake = recorder;
        self
    }

    /// Answers in the recovery state of `recovery` while it is pending.
    pub fn with_recovery(mut self, recovery: Arc<RecoveryGate>) -> Self {
        self.recovery = recovery;
        self
    }

    fn decode(text: &str, maximum: usize, what: &str) -> Result<Vec<u8>, OperatorRefusal> {
        let bytes = STANDARD
            .decode(text.as_bytes())
            .map_err(|_| OperatorRefusal::new("request_invalid", format!("the {what} is not valid base64")))?;
        if bytes.len() > maximum {
            return Err(OperatorRefusal::new(
                "request_invalid",
                format!("the {what} is larger than {maximum} bytes"),
            ));
        }
        Ok(bytes)
    }

    fn status(&self) -> Value {
        // One read of the window: the intake latency figures (M04-T11) are shown
        // only beside the identity of the window they belong to.
        let window = self.controller.status();
        let mut status = status_json(&window);
        status["credentialRecoveryPending"] = json!(self.recovery.pending());
        status["intakeLatency"] = self.intake.status(&window);
        status
    }

    fn require_open_window(&self) -> Result<(), OperatorRefusal> {
        if self.controller.status().phase != Phase::Open {
            return Err(OperatorRefusal::new(
                WINDOW_CLOSED,
                "sign-in and vehicle discovery need an open collection window",
            ));
        }
        Ok(())
    }

    /// The service reads recovery files only at absolute paths it is named:
    /// never its own standard input (`-`) and never a path relative to its
    /// working directory (review finding 6).
    fn recovery_paths(paths: &[&Path]) -> Result<(), OperatorRefusal> {
        for path in paths {
            let text = path.as_os_str();
            if text == "-" || text.is_empty() || !path.is_absolute() {
                return Err(OperatorRefusal::new(
                    "request_invalid",
                    "recovery files are named by absolute paths; standard input is not read",
                ));
            }
        }
        Ok(())
    }
}

impl OperatorHandler for OperatorService {
    fn handle(&self, request: OperatorRequest) -> Result<Value, OperatorRefusal> {
        if self.shutdown.is_stopping() && !matches!(request, OperatorRequest::WindowStatus) {
            return Err(OperatorRefusal::new(
                SERVICE_STOPPING,
                "the Hub service is stopping; try again after it restarts",
            ));
        }
        if self.recovery.pending() {
            match request {
                OperatorRequest::WindowStatus
                | OperatorRequest::WindowClose
                | OperatorRequest::RecoveryRestore { .. } => {}
                _ => {
                    return Err(OperatorRefusal::new(
                        CREDENTIAL_RECOVERY_PENDING,
                        "this Hub's data was restored without its keys; run restore-recovery-credentials first",
                    ));
                }
            }
        }
        match request {
            OperatorRequest::WindowStatus => Ok(self.status()),
            OperatorRequest::WindowTrust { document } => {
                let bytes = Self::decode(&document, TRUST_DOCUMENT_MAX_BYTES, "trust document")?;
                self.trust
                    .trust(&self.controller, &self.window_dir, &bytes)?;
                (self.kick)();
                Ok(self.status())
            }
            OperatorRequest::WindowOpen { grant } => {
                let bytes = Self::decode(&grant, GRANT_MAX_BYTES, "grant")?;
                self.controller
                    .open(&bytes)
                    .map_err(OperatorRefusal::window)?;
                (self.kick)();
                Ok(self.status())
            }
            OperatorRequest::WindowClose => {
                self.controller.close(CloseReason::OperatorClose);
                (self.kick)();
                Ok(self.status())
            }
            OperatorRequest::Setup {
                material,
                vehicle_id,
                all_vehicles,
            } => {
                // Tesla code exchange and discovery run only inside an Open
                // window; the egress gate refuses every send otherwise too.
                if self.controller.status().phase != Phase::Open {
                    return Err(OperatorRefusal::new(
                        WINDOW_CLOSED,
                        "sign-in and vehicle discovery need an open collection window",
                    ));
                }
                self.backend.setup(material, vehicle_id, all_vehicles)
            }
            // The Fleet sign-in (M09-T09) is gated as the owner sign-in is:
            // its binding, exchange and discovery need an Open window, and
            // every send still asks the egress gate.
            OperatorRequest::FleetSignInBegin { input } => {
                self.require_open_window()?;
                self.backend.fleet_sign_in_begin(input)
            }
            OperatorRequest::FleetSignInRedeem {
                input,
                vehicle_id,
                all_vehicles,
            } => {
                self.require_open_window()?;
                self.backend
                    .fleet_sign_in_redeem(input, vehicle_id, all_vehicles)
            }
            // The Fleet partner verbs (M09-T18): the key export sends
            // nothing; the registration needs an Open window and each of its
            // sends asks the egress gate again.
            // M09-T19: the discovery again with the stored login sends, so
            // it needs an Open window; the activation writes configuration
            // only.
            OperatorRequest::FleetDiscover {
                vehicle_id,
                all_vehicles,
            } => {
                self.require_open_window()?;
                self.backend.fleet_discover(vehicle_id, all_vehicles)
            }
            OperatorRequest::FleetActivate => self.backend.fleet_activate(),
            OperatorRequest::FleetPublicKey => self.backend.fleet_public_key(),
            OperatorRequest::FleetPartnerRegister { input } => {
                self.require_open_window()?;
                self.backend.fleet_partner_register(input)
            }
            OperatorRequest::RecoveryExport {
                destination,
                recovery_key_file,
            } => {
                Self::recovery_paths(&[&destination, &recovery_key_file])?;
                let _guard = self
                    .controller
                    .import_guard()
                    .map_err(OperatorRefusal::window)?;
                self.backend
                    .recovery_export(&destination, &recovery_key_file)
            }
            OperatorRequest::RecoveryRestore {
                source,
                recovery_key_file,
            } => {
                Self::recovery_paths(&[&source, &recovery_key_file])?;
                let _guard = self
                    .controller
                    .import_guard()
                    .map_err(OperatorRefusal::window)?;
                let report = self.backend.recovery_restore(&source, &recovery_key_file)?;
                // The vault the restore opened is closed again; the service
                // may now start its API with the restored key.
                self.recovery.complete();
                Ok(report)
            }
            // Without the confirmation only the read-only listing runs.
            OperatorRequest::PurgeRefusedCredentials { confirm: false } => {
                self.backend.refused_credentials()
            }
            // The purge waits for the window to close and holds the import
            // lock throughout, so no window opens and no collector starts
            // while rows are removed (M02-T11).
            OperatorRequest::PurgeRefusedCredentials { confirm: true } => {
                let _guard = self
                    .controller
                    .import_guard()
                    .map_err(OperatorRefusal::window)?;
                self.backend.purge_refused_credentials()
            }
            // A backup runs beside a collection window (the operation copies
            // the catalogue under its own publication gate), so it takes no
            // import lock; the schedule's one-run slot refuses an overlap.
            OperatorRequest::BackupNow {
                destination,
                password_file,
            } => {
                for path in [&destination, &password_file].into_iter().flatten() {
                    Self::recovery_paths(&[path])?;
                }
                self.backend
                    .backup_now(destination.as_deref(), password_file.as_deref())
            }
            OperatorRequest::BackupStatus => self.backend.backup_status(),
            // The repair verbs touch only the Hub's own history and send
            // nothing to Tesla, so they need no window; the stopping and
            // recovery gates above already apply (they are not in the
            // recovery allow-list). The rest of each authority check is in
            // `RepairControl`.
            OperatorRequest::RepairCheckNow { vehicle } => self.repairs()?.check_now(&vehicle),
            OperatorRequest::RepairAutomatic {
                vehicle,
                on,
                phone_witness,
            } => self.repairs()?.automatic(&vehicle, on, phone_witness),
            OperatorRequest::RepairProtect {
                vehicle,
                entity,
                entity_id,
                reason,
            } => self.repairs()?.protect(&vehicle, &entity, entity_id, &reason),
            OperatorRequest::RepairAttest { vehicle, drive_ids } => {
                self.repairs()?.attest(&vehicle, &drive_ids)
            }
            OperatorRequest::RepairUndo {
                vehicle,
                batch_id,
                component_key,
            } => self
                .repairs()?
                .undo(&vehicle, batch_id, component_key.as_deref()),
            OperatorRequest::RepairRetry {
                vehicle,
                component_key,
            } => self.repairs()?.retry(&vehicle, &component_key),
            OperatorRequest::RepairSummary { vehicle } => self.repairs()?.summary(&vehicle),
            OperatorRequest::RepairJob { job_id } => self.repairs()?.job(job_id),
        }
    }
}

impl OperatorService {
    fn repairs(&self) -> Result<&RepairControl, OperatorRefusal> {
        self.backend.repair_control().ok_or_else(|| {
            OperatorRefusal::new(REPAIR_UNAVAILABLE, "this service has no history repairs")
        })
    }
}

enum Signal {
    Kick,
    /// The collection configuration was reloaded: the supervisor forgets its
    /// start backoff and tries at once (M02-T24).
    Reloaded,
    /// Stop, waiting for the collector until this deadline, fixed when the stop
    /// was asked for: time the supervisor spends before it reads the request
    /// comes out of the allowance, not on top of it (review REV-CX-SSA, finding 2).
    Stop(std::time::Instant),
}

fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

/// How often a handback's physical publication asks for a busy publication
/// gate before it leaves the refusal to the next handback, start or import.
const HANDBACK_GATE_ATTEMPTS: u32 = 20;
const HANDBACK_GATE_RETRY: Duration = Duration::from_millis(250);

/// After a handback, and once at service start (M03-T22): the physical 1.4
/// heads a window left refused are planned again, under the publication
/// gate, from what each vehicle's last import kept, so the window's data
/// reaches 1.4 clients without another import. It needs the store and the
/// cursor key the data directory already holds and never creates a key (a
/// data-only restore waits for its credential recovery). A gate that stays
/// busy, or a vehicle that cannot be planned, leaves the refusal. Under the
/// same gate, each due request for a fresh compatibility base (issue #106)
/// is then attempted.
pub fn publish_window_after_handback(data_dir: &Path) {
    publish_window_after_handback_until(data_dir, &AtomicBool::new(false));
}

/// [`publish_window_after_handback`], ending a fresh-base build early once
/// `stop` is set (the supervisor sets it when the service stops).
fn publish_window_after_handback_until(data_dir: &Path, stop: &AtomicBool) {
    let present = |path: PathBuf| std::fs::symlink_metadata(path).is_ok();
    if !present(data_dir.join("hub.sqlite")) {
        return;
    }
    let key_present = present(crate::legacy_owner_credentials::cursor_key_path(data_dir));
    if !key_present && !crate::db::v2_lineage_rebase_due(data_dir, wall_clock_ms()) {
        return;
    }
    let store = match crate::db::HubStore::open_existing(data_dir) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!(%error, "handback publication: the store did not open");
            return;
        }
    };
    if !key_present {
        defer_lineage_rebases(&store);
        return;
    }
    let cursor_key = match crate::legacy_owner_credentials::load_or_create_cursor_key(data_dir) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(%error, "handback publication: the cursor key did not load");
            defer_lineage_rebases(&store);
            return;
        }
    };
    let mut attempts = 0;
    let gate = loop {
        match store.try_acquire_publication_gate() {
            Ok(gate) => break gate,
            Err(crate::db::StoreError::PublicationGateBusy) if attempts < HANDBACK_GATE_ATTEMPTS => {
                attempts += 1;
                std::thread::sleep(HANDBACK_GATE_RETRY);
            }
            Err(error) => {
                tracing::info!(
                    %error,
                    "handback publication: the publication gate is busy; the 1.4 head stays \
                     refused until the next handback, start or import"
                );
                defer_lineage_rebases(&store);
                return;
            }
        }
    };
    // PG-1: the gate is held from here to the end of this function.
    let gate_held = std::time::Instant::now();
    match crate::import::source_db::importer::publish_physical_after_handback(
        &store,
        &cursor_key,
        &gate,
    ) {
        Ok(outcomes) => {
            for (vehicle_id, outcome) in outcomes {
                tracing::info!(%vehicle_id, ?outcome, "handback publication of the 1.4 head");
            }
        }
        Err(error) => tracing::warn!(%error, "handback publication failed"),
    }
    match store.rebase_due_v2_lineages(
        &gate,
        &cursor_key,
        wall_clock_ms(),
        crate::db::V2LineageRebaseLimits::default(),
        &|| stop.load(Ordering::Relaxed),
    ) {
        Ok(outcomes) => {
            for (vehicle_id, outcome) in outcomes {
                tracing::info!(%vehicle_id, ?outcome, "fresh compatibility base attempt");
            }
        }
        Err(error) => {
            tracing::warn!(%error, "fresh compatibility base attempts failed");
            defer_lineage_rebases(&store);
        }
    }
    let gate_held_ms = u64::try_from(gate_held.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(gate_held_ms, "handback gate held");
}

/// Asks for every due fresh-base request again at its growing interval, so
/// one that this run could not attempt does not start the publication thread
/// at every supervisor step.
fn defer_lineage_rebases(store: &crate::db::HubStore) {
    if let Err(error) = store.defer_due_v2_lineage_rebases(wall_clock_ms()) {
        tracing::warn!(%error, "handback publication: a lineage rebase request was not deferred");
    }
}

/// Runs [`publish_window_after_handback`] off the calling thread, so a
/// window's supervision never waits for a publication. The thread is owned
/// by `running` (issue #38): the supervisor joins it when the service stops,
/// within the shutdown budget, instead of leaving it detached. `stop` ends a
/// fresh-base build early.
fn spawn_handback_publication(
    data_dir: Option<PathBuf>,
    running: &mut Vec<JoinHandle<()>>,
    stop: &Arc<AtomicBool>,
) {
    let Some(data_dir) = data_dir else {
        return;
    };
    running.retain(|handle| !handle.is_finished());
    let stop = Arc::clone(stop);
    let spawned = std::thread::Builder::new()
        .name("hub-handback-publication".to_owned())
        .spawn(move || publish_window_after_handback_until(&data_dir, &stop));
    match spawned {
        Ok(handle) => running.push(handle),
        Err(error) => tracing::warn!(%error, "handback publication did not start"),
    }
}

/// Whether the supervisor starts the hand-back publication for a due rebase
/// request: only when one is due and no publication thread is running.
fn lineage_rebase_spawn_due(due: bool, running: &mut Vec<JoinHandle<()>>) -> bool {
    running.retain(|handle| !handle.is_finished());
    due && running.is_empty()
}

/// The supervisor thread: it steps the window supervisor at least every
/// `next_wakeup_seconds` and at once after a kick, and stops the collector
/// when the service stops (the window itself survives a restart; design 2.9).
pub struct SupervisorThread {
    sender: mpsc::Sender<Signal>,
    thread: Option<JoinHandle<SupervisorExit>>,
    /// Gives the collector's slot the service's stop deadline from the requesting
    /// thread, at once (review REV-CX-RECHECK-SSA, finding 1).
    stop_slot_within: StopSlotWithin,
}

/// A stop of the collector's slot by a deadline, callable from any thread.
type StopSlotWithin = Arc<dyn Fn(std::time::Instant) + Send + Sync>;

/// What the supervisor thread hands back when it stops: whether its
/// collector exited within its allowance, and the publications still running.
struct SupervisorExit {
    collector_stopped: bool,
    publications: Vec<JoinHandle<()>>,
}

/// How long a stopping service waits for its collector to exit.
pub const COLLECTOR_STOP_WAIT: Duration = Duration::from_secs(30);

impl SupervisorThread {
    pub fn spawn<S>(controller: Arc<WindowController>, slot: S) -> std::io::Result<Self>
    where
        S: CollectorSlot + Clone + Send + 'static,
    {
        let mut shutdown_slot = slot.clone();
        // The service's stop reaches the slot from the requesting thread, before the
        // supervisor's loop takes the request (review REV-CX-RECHECK-SSA, finding 1):
        // a step that is running then, whose window close may signal a plain stop, or a
        // collector that sees the window end by itself, cannot start the stop's writes
        // under the default allowance while the request waits in the queue. The loop
        // still gives the slot the same deadline when it takes the request.
        let request_slot = Mutex::new(slot.clone());
        let stop_slot_within: StopSlotWithin = Arc::new(move |deadline| {
            request_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .stop_within(deadline);
        });
        let (sender, receiver) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("hub-window-supervisor".to_owned())
            .spawn(move || {
                // Once at service start: a handback whose publication did not
                // finish is planned again (M03-T22).
                let mut publications = Vec::new();
                let publication_stop = Arc::new(AtomicBool::new(false));
                spawn_handback_publication(
                    controller.data_dir(),
                    &mut publications,
                    &publication_stop,
                );
                let mut supervisor = Supervisor::new(controller.clone(), slot);
                let collector_deadline = loop {
                    let report = supervisor.step();
                    if let Some(reason) = report.closed {
                        tracing::info!(reason = reason.code(), "collection window closed");
                        spawn_handback_publication(
                            controller.data_dir(),
                            &mut publications,
                            &publication_stop,
                        );
                    }
                    // A handback that did not reach the store's coverage is
                    // written again; the heads stay refused until it is, and
                    // then the window's data is published (M03-T22).
                    if controller.coverage_unreconciled() && controller.reconcile_coverage() {
                        tracing::info!("the window's handback reached the store's coverage");
                        spawn_handback_publication(
                            controller.data_dir(),
                            &mut publications,
                            &publication_stop,
                        );
                    }
                    // A request for a fresh compatibility base (issue #106) that
                    // is due starts the publication thread when none runs.
                    if lineage_rebase_spawn_due(controller.lineage_rebase_due(), &mut publications)
                    {
                        spawn_handback_publication(
                            controller.data_dir(),
                            &mut publications,
                            &publication_stop,
                        );
                    }
                    if let Some(fault) = report.fault {
                        tracing::warn!(code = fault.code(), "collection window fault");
                    }
                    let wait = controller.next_wakeup_seconds().max(1) as u64;
                    match receiver.recv_timeout(Duration::from_secs(wait)) {
                        Ok(Signal::Kick) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                        // A start refused before the reload (an idle Hub's
                        // `collector_not_ready`) left a backoff of up to a
                        // minute; the reload makes the start possible now.
                        Ok(Signal::Reloaded) => {
                            supervisor.reset_start_backoff();
                        }
                        Ok(Signal::Stop(deadline)) => break deadline,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            break std::time::Instant::now() + COLLECTOR_STOP_WAIT;
                        }
                    }
                };
                drop(supervisor);
                // A fresh-base build in a publication thread ends at its next
                // pack, so the join below stays inside the shutdown budget.
                publication_stop.store(true, Ordering::Relaxed);
                let collector_stopped = stop_for_shutdown(&mut shutdown_slot, collector_deadline);
                if !collector_stopped {
                    tracing::warn!("the collector was still running when its stop allowance ended");
                }
                publications.retain(|handle| !handle.is_finished());
                SupervisorExit {
                    collector_stopped,
                    publications,
                }
            })?;
        Ok(Self {
            sender,
            thread: Some(thread),
            stop_slot_within,
        })
    }

    /// A cloneable wake-up for the channel handler.
    pub fn kicker(&self) -> Box<dyn Fn() + Send + Sync> {
        let sender = Mutex::new(self.sender.clone());
        Box::new(move || {
            if let Ok(sender) = sender.lock() {
                let _ = sender.send(Signal::Kick);
            }
        })
    }

    /// A wake-up for after the collector slot's configuration was reloaded:
    /// like [`Self::kicker`], and the supervisor's start backoff is cleared.
    pub fn reload_kicker(&self) -> Box<dyn Fn() + Send + Sync> {
        let sender = Mutex::new(self.sender.clone());
        Box::new(move || {
            if let Ok(sender) = sender.lock() {
                let _ = sender.send(Signal::Reloaded);
            }
        })
    }

    /// Asks the supervisor to stop now, without waiting (issue #38, review
    /// F2): called when the service accepts a stop, so the collector stops
    /// while the listener drains. Its collector gets at most what is left of
    /// `budget`'s work stages, and no more than [`COLLECTOR_STOP_WAIT`]. The slot is
    /// given that deadline here, before the request is queued (review
    /// REV-CX-RECHECK-SSA, finding 1).
    pub fn stopper(&self) -> Box<dyn Fn(&crate::runtime::shutdown::ShutdownBudget) + Send + Sync> {
        let sender = Mutex::new(self.sender.clone());
        let stop_slot_within = Arc::clone(&self.stop_slot_within);
        Box::new(move |budget| {
            let collector_wait = budget.remaining_for_work().min(COLLECTOR_STOP_WAIT);
            let deadline = std::time::Instant::now() + collector_wait;
            stop_slot_within(deadline);
            if let Ok(sender) = sender.lock() {
                let _ = sender.send(Signal::Stop(deadline));
            }
        })
    }

    /// Stops the collector and the thread.
    pub fn stop(mut self) {
        self.shutdown();
    }

    /// Stops the collector, the thread and the handback publications it
    /// started, within what is left of the service's shutdown budget (issue
    /// #38). The collector gets at most the remaining budget. A thread still
    /// running when the budget ends is reported, not waited for: a
    /// publication runs under the publication gate in SQLite transactions,
    /// and a head it leaves refused is planned again at the next start. A
    /// collector still running after its allowance is reported as the
    /// `collector` stage timing out, never as stopped (review F3): its
    /// unfinished writes are left to the store's recovery at the next open.
    pub fn stop_within(
        mut self,
        budget: &crate::runtime::shutdown::ShutdownBudget,
    ) -> Result<(), crate::runtime::shutdown::ShutdownTimedOut> {
        use crate::runtime::shutdown::join_within;
        let collector_wait = budget.remaining_for_work().min(COLLECTOR_STOP_WAIT);
        let deadline = std::time::Instant::now() + collector_wait;
        (self.stop_slot_within)(deadline);
        let _ = self.sender.send(Signal::Stop(deadline));
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        let (collector_stopped, publications) =
            match join_within(thread, budget, "window supervisor") {
                Ok(Ok(exit)) => (exit.collector_stopped, exit.publications),
                Ok(Err(_)) => (false, Vec::new()),
                Err((_thread, timeout)) => return Err(timeout),
            };
        let mut outcome = if collector_stopped {
            Ok(())
        } else {
            Err(budget.timed_out("collector"))
        };
        for publication in publications {
            if let Err((_publication, timeout)) =
                join_within(publication, budget, "handback publication")
            {
                outcome = Err(timeout);
            }
        }
        outcome
    }

    fn shutdown(&mut self) {
        let _ = self
            .sender
            .send(Signal::Stop(std::time::Instant::now() + COLLECTOR_STOP_WAIT));
        if let Some(thread) = self.thread.take()
            && let Ok(exit) = thread.join()
        {
            for publication in exit.publications {
                let _ = publication.join();
            }
        }
    }
}

impl Drop for SupervisorThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Runs a bounded observation under the window for its whole lifetime
/// (review finding 5). The window is ticked at least every `max_poll` and at
/// its deadline, so the periodic clock and time-offset checks run while the
/// observation does; once the window leaves Open the observation is told to
/// stop through the receiver it was given. Only after the observation has
/// returned, and with it released its collector lease, is a Closing window
/// handed back and Closed. Returns the observation's result and the
/// handback, if this run closed the window.
pub async fn supervise_observation<T, F, Fut>(
    controller: Arc<WindowController>,
    max_poll: Duration,
    observation: F,
) -> (T, Result<Option<Handback>, OperatorRefusal>)
where
    F: FnOnce(tokio::sync::oneshot::Receiver<()>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut stop = Some(stop);
    let observation = observation(stopped);
    tokio::pin!(observation);
    let result = loop {
        let wait = Duration::from_secs(controller.next_wakeup_seconds().max(1) as u64).min(max_poll);
        tokio::select! {
            result = &mut observation => break result,
            () = tokio::time::sleep(wait) => {
                let ticking = Arc::clone(&controller);
                let phase = tokio::task::spawn_blocking(move || ticking.tick())
                    .await
                    .ok()
                    .and_then(Result::ok);
                if phase != Some(Phase::Open)
                    && let Some(stop) = stop.take()
                {
                    tracing::info!("the collection window left Open; stopping the observation");
                    let _ = stop.send(());
                }
            }
        }
    };
    let closing = Arc::clone(&controller);
    let closed = tokio::task::spawn_blocking(move || {
        let _ = closing.tick();
        if closing.status().phase == Phase::Closing {
            let closed = closing.finish_close().map_err(OperatorRefusal::window);
            if matches!(closed, Ok(Some(_)))
                && let Some(data_dir) = closing.data_dir()
            {
                publish_window_after_handback(&data_dir);
            }
            closed
        } else {
            Ok(None)
        }
    })
    .await
    .unwrap_or_else(|_| Err(OperatorRefusal::window(crate::collection::window::Refusal::Io)));
    (result, closed)
}

/// Signals the collector to stop and waits for it to exit, up to `deadline`.
/// Says whether it exited. The collector is told the same deadline, so its
/// stop sizes its writes to the time it really has (STREAM-STOP-ALLOWANCE).
fn stop_for_shutdown<S: CollectorSlot>(slot: &mut S, deadline: std::time::Instant) -> bool {
    slot.stop_within(deadline);
    loop {
        if !slot.is_running() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Broken-drive repair controls (M03-T28b)
// ---------------------------------------------------------------------------

pub const REPAIR_UNAVAILABLE: &str = "repair_unavailable";
pub const REPAIR_INVALID: &str = "repair_invalid";
pub const REPAIR_UNKNOWN_VEHICLE: &str = "repair_unknown_vehicle";
pub const REPAIR_WITNESS_REQUIRED: &str = "repair_witness_required";
pub const REPAIR_BUSY: &str = "repair_busy";
pub const REPAIR_IMPORT_PENDING: &str = "repair_import_pending";
pub const REPAIR_NOT_READY: &str = "repair_not_ready";
pub const REPAIR_JOB_UNKNOWN: &str = "repair_job_unknown";
pub const REPAIR_STORE: &str = "repair_store";
/// A job the scheduler deferred fails `repair_deferred_<reason>`, for example
/// `repair_deferred_window`.
pub const REPAIR_DEFERRED_PREFIX: &str = "repair_deferred_";

/// What an Undo names.
enum UndoTarget {
    Batch(i64),
    Component([u8; 32]),
}

/// The most drives one attestation names.
const REPAIR_ATTEST_MAX: usize = 512;
/// Finished receipts kept for polling; the oldest finished go first.
const REPAIR_JOBS_KEPT: usize = 32;

enum RepairJobState {
    Running,
    Done(Value),
    Failed(String),
}

struct RepairJobEntry {
    id: u64,
    vehicle: uuid::Uuid,
    kind: &'static str,
    state: RepairJobState,
}

#[derive(Default)]
struct RepairJobs {
    next: u64,
    entries: std::collections::VecDeque<RepairJobEntry>,
}

fn repair_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn repair_refusal(code: &str, message: &str) -> OperatorRefusal {
    OperatorRefusal::new(code, message)
}

fn repair_store_refusal(error: &crate::db::StoreError) -> OperatorRefusal {
    tracing::warn!(%error, "repair control store error");
    repair_refusal(REPAIR_STORE, "the Hub's history store refused the request")
}

/// The owner-channel controls of the broken-drive repairs: Check now, on/off,
/// Protect, attest, Undo, Retry, the summary and a job's receipt.
///
/// Authority, in order, for every verb: the channel's peer check (this user's
/// Hub executable), then the service's stopping and credential-recovery gates
/// (`OperatorService::handle`), then here: a well-formed vehicle that an import
/// has indexed (the summary alone may name an unindexed one). Turning automatic
/// runs on needs the owner's recorded real-phone witness. Undo and Retry
/// refuse while an import's publication is pending. Check now is the
/// scheduler's own pass; Undo, and the republication after a protection that
/// left the vehicle's head refused, run under the same one-pass slot,
/// deferrals and holds ([`RepairScheduler::run_exclusive`]), taking the
/// publication gate without waiting. Protect and attest are single store
/// transactions that activation revalidates. Every job runs on the
/// scheduler, which starts none once the service stops and joins them all.
/// Nothing is sent to Tesla, and no value or coordinate leaves the store:
/// only counts, ids, codes and times.
///
/// [`RepairScheduler::run_exclusive`]: crate::runtime::broken_drive_repair::RepairScheduler::run_exclusive
pub struct RepairControl {
    store: crate::db::HubStore,
    data_dir: PathBuf,
    scheduler: Arc<crate::runtime::broken_drive_repair::RepairScheduler>,
    jobs: Arc<Mutex<RepairJobs>>,
}

impl RepairControl {
    pub fn new(
        store: crate::db::HubStore,
        data_dir: PathBuf,
        scheduler: Arc<crate::runtime::broken_drive_repair::RepairScheduler>,
    ) -> Self {
        Self {
            store,
            data_dir,
            scheduler,
            jobs: Arc::default(),
        }
    }

    fn vehicle(text: &str) -> Result<uuid::Uuid, OperatorRefusal> {
        uuid::Uuid::parse_str(text)
            .map_err(|_| repair_refusal(REPAIR_INVALID, "the vehicle is not a UUID"))
    }

    fn indexed_vehicle(&self, text: &str) -> Result<uuid::Uuid, OperatorRefusal> {
        let vehicle = Self::vehicle(text)?;
        let indexed = self
            .store
            .drive_repair_indexed_vehicles()
            .map_err(|error| repair_store_refusal(&error))?;
        if indexed.contains(&vehicle) {
            Ok(vehicle)
        } else {
            Err(repair_refusal(
                REPAIR_UNKNOWN_VEHICLE,
                "no import has recorded repair candidates for this vehicle",
            ))
        }
    }

    fn component_key(text: &str) -> Result<[u8; 32], OperatorRefusal> {
        let invalid = || repair_refusal(REPAIR_INVALID, "the component key is not 64 hex digits");
        let bytes = hex::decode(text).map_err(|_| invalid())?;
        <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| invalid())
    }

    fn import_pending(&self, vehicle: uuid::Uuid) -> Result<(), OperatorRefusal> {
        let holds = self
            .store
            .import_publication_holds()
            .map_err(|error| repair_store_refusal(&error))?;
        if holds.pending(vehicle) {
            return Err(repair_refusal(
                REPAIR_IMPORT_PENDING,
                "an import's publication is pending; try again after it is published",
            ));
        }
        Ok(())
    }

    fn receipt(id: u64, kind: &str, vehicle: uuid::Uuid, state: &str) -> Value {
        json!({ "jobId": id, "kind": kind, "vehicle": vehicle.to_string(), "state": state })
    }

    /// A short verb whose result is ready: a finished receipt, recorded so
    /// `RepairJob` reads it too.
    fn finished(
        &self,
        kind: &'static str,
        vehicle: uuid::Uuid,
        result: Value,
    ) -> Value {
        let mut jobs = repair_lock(&self.jobs);
        jobs.next += 1;
        let id = jobs.next;
        jobs.entries.push_back(RepairJobEntry {
            id,
            vehicle,
            kind,
            state: RepairJobState::Done(result.clone()),
        });
        Self::trim(&mut jobs);
        let mut receipt = Self::receipt(id, kind, vehicle, "done");
        receipt["result"] = result;
        receipt
    }

    fn trim(jobs: &mut RepairJobs) {
        while jobs.entries.len() > REPAIR_JOBS_KEPT {
            match jobs
                .entries
                .iter()
                .position(|entry| !matches!(entry.state, RepairJobState::Running))
            {
                Some(index) => {
                    jobs.entries.remove(index);
                }
                None => break,
            }
        }
    }

    /// Starts `work` on a worker thread and returns the accepted receipt at
    /// once. One running job per vehicle.
    fn spawn(
        &self,
        kind: &'static str,
        vehicle: uuid::Uuid,
        work: impl FnOnce() -> Result<Value, String> + Send + 'static,
    ) -> Result<Value, OperatorRefusal> {
        let id = {
            let mut jobs = repair_lock(&self.jobs);
            if jobs
                .entries
                .iter()
                .any(|entry| entry.vehicle == vehicle && matches!(entry.state, RepairJobState::Running))
            {
                return Err(repair_refusal(
                    REPAIR_BUSY,
                    "a repair job for this vehicle is already running",
                ));
            }
            jobs.next += 1;
            let id = jobs.next;
            jobs.entries.push_back(RepairJobEntry {
                id,
                vehicle,
                kind,
                state: RepairJobState::Running,
            });
            Self::trim(&mut jobs);
            id
        };
        let registry = Arc::clone(&self.jobs);
        let settle = move |state: RepairJobState| {
            let mut jobs = repair_lock(&registry);
            if let Some(entry) = jobs.entries.iter_mut().find(|entry| entry.id == id) {
                entry.state = state;
            }
        };
        let settle = Arc::new(Mutex::new(Some(settle)));
        let worker_settle = Arc::clone(&settle);
        let spawned = self.scheduler.spawn_job(move || {
            let state = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
                Ok(Ok(value)) => RepairJobState::Done(value),
                Ok(Err(code)) => RepairJobState::Failed(code),
                Err(_) => RepairJobState::Failed("panicked".to_owned()),
            };
            if let Some(settle) = repair_lock(&worker_settle).take() {
                settle(state);
            }
        });
        if spawned.is_err() {
            if let Some(settle) = repair_lock(&settle).take() {
                settle(RepairJobState::Failed("no_thread".to_owned()));
            }
            return Err(repair_refusal(REPAIR_UNAVAILABLE, "the repair job could not start"));
        }
        Ok(Self::receipt(id, kind, vehicle, "accepted"))
    }

    /// Check now: the scheduler's pass for the vehicle, as a job.
    pub fn check_now(&self, vehicle: &str) -> Result<Value, OperatorRefusal> {
        use crate::runtime::broken_drive_repair::{PassOutcome, VehicleStep};
        let vehicle = self.indexed_vehicle(vehicle)?;
        let scheduler = Arc::clone(&self.scheduler);
        self.spawn("check_now", vehicle, move || match scheduler.run_now(vehicle) {
            Ok(VehicleStep::Ran(PassOutcome::NoChange)) => Ok(json!({ "outcome": "no_change" })),
            Ok(VehicleStep::Ran(PassOutcome::Repaired { batch_id })) => {
                Ok(json!({ "outcome": "repaired", "batchId": batch_id }))
            }
            Ok(VehicleStep::Ran(PassOutcome::Deferred(why)))
            | Ok(VehicleStep::Deferred(why)) => {
                Ok(json!({ "outcome": "deferred", "reason": why.code() }))
            }
            Ok(VehicleStep::Ran(PassOutcome::Failed(code))) => Err(code),
            Ok(VehicleStep::Off | VehicleStep::NotDue { .. }) => {
                Ok(json!({ "outcome": "not_run" }))
            }
            Err(_) => Err(REPAIR_STORE.to_owned()),
        })
    }

    /// On or off for automatic runs. On needs the recorded phone witness.
    pub fn automatic(
        &self,
        vehicle: &str,
        on: bool,
        phone_witness: bool,
    ) -> Result<Value, OperatorRefusal> {
        let vehicle = self.indexed_vehicle(vehicle)?;
        if on && !phone_witness {
            return Err(repair_refusal(
                REPAIR_WITNESS_REQUIRED,
                "automatic repairs need the owner's recorded real-phone witness first",
            ));
        }
        let record = self
            .scheduler
            .set_automatic(vehicle, on)
            .map_err(|error| repair_store_refusal(&error))?;
        Ok(self.finished("automatic", vehicle, Self::schedule_json(&record)))
    }

    pub fn protect(
        &self,
        vehicle: &str,
        entity: &str,
        entity_id: i64,
        reason: &str,
    ) -> Result<Value, OperatorRefusal> {
        use crate::db::{DriveProtectionReason as Reason, DriveRepairEntity as Entity};
        let vehicle = self.indexed_vehicle(vehicle)?;
        let entity = match entity {
            "drive" => Entity::Drive,
            "position" => Entity::Position,
            _ => return Err(repair_refusal(REPAIR_INVALID, "the entity is `drive` or `position`")),
        };
        let reason = match reason {
            "edit" => Reason::Edit,
            "delete" => Reason::Delete,
            "merge" => Reason::Merge,
            "split" => Reason::Split,
            "annotation" => Reason::Annotation,
            "protect" => Reason::Protect,
            _ => return Err(repair_refusal(REPAIR_INVALID, "the protection reason is not known")),
        };
        let revision = self
            .store
            .record_drive_edit_protection(vehicle, entity, entity_id, reason, crate::runtime::backup_schedule::wall_ms())
            .map_err(|error| repair_store_refusal(&error))?;
        // A protection that superseded an active component changed the
        // vehicle's layer, and its served head with it: the head is
        // published again as a job (REV-CX-T28).
        if self.store.physical_v3_current_head_served(vehicle).ok().flatten() != Some(false) {
            return Ok(self.finished("protect", vehicle, json!({ "revision": revision })));
        }
        let scheduler = Arc::clone(&self.scheduler);
        let store = self.store.clone();
        let data_dir = self.data_dir.clone();
        let republish = move || {
            let republished = scheduler
                .run_exclusive(|| crate::runtime::broken_drive_repair::republish_head(&store, &data_dir, vehicle))
                .unwrap_or_else(|reason| reason.code().to_owned());
            Ok(json!({ "revision": revision, "republished": republished }))
        };
        match self.spawn("protect", vehicle, republish) {
            Ok(receipt) => Ok(receipt),
            // The protection is recorded; the running job or the next Check
            // now publishes the head.
            Err(_) => Ok(self.finished(
                "protect",
                vehicle,
                json!({ "revision": revision, "republished": "busy" }),
            )),
        }
    }

    pub fn attest(&self, vehicle: &str, drive_ids: &[i64]) -> Result<Value, OperatorRefusal> {
        let vehicle = self.indexed_vehicle(vehicle)?;
        if drive_ids.is_empty() || drive_ids.len() > REPAIR_ATTEST_MAX {
            return Err(repair_refusal(
                REPAIR_INVALID,
                "an attestation names between 1 and 512 drives",
            ));
        }
        let attested = self
            .store
            .attest_unedited_drive_repair_candidates(
                vehicle,
                drive_ids,
                crate::runtime::backup_schedule::wall_ms(),
            )
            .map_err(|error| repair_store_refusal(&error))?;
        Ok(self.finished(
            "attest",
            vehicle,
            json!({ "requested": drive_ids.len(), "attested": attested }),
        ))
    }

    /// Undo a batch or a component, as a job: compensated and suppressed in
    /// one transaction, then the head is published once.
    pub fn undo(
        &self,
        vehicle: &str,
        batch_id: Option<i64>,
        component_key: Option<&str>,
    ) -> Result<Value, OperatorRefusal> {
        let vehicle = self.indexed_vehicle(vehicle)?;
        let target = match (batch_id, component_key) {
            (Some(batch), None) => UndoTarget::Batch(batch),
            (None, Some(key)) => UndoTarget::Component(Self::component_key(key)?),
            _ => {
                return Err(repair_refusal(
                    REPAIR_INVALID,
                    "an undo names a batch or a component, not both and not neither",
                ));
            }
        };
        self.import_pending(vehicle)?;
        let store = self.store.clone();
        let data_dir = self.data_dir.clone();
        let scheduler = Arc::clone(&self.scheduler);
        self.spawn("undo", vehicle, move || {
            scheduler
                .run_exclusive(|| Self::undo_now(&store, &data_dir, vehicle, &target))
                .unwrap_or_else(|reason| Err(format!("{REPAIR_DEFERRED_PREFIX}{}", reason.code())))
        })
    }

    /// Undo's work, under the scheduler's slot and holds.
    fn undo_now(
        store: &crate::db::HubStore,
        data_dir: &Path,
        vehicle: uuid::Uuid,
        target: &UndoTarget,
    ) -> Result<Value, String> {
        use crate::db::DriveRepairTarget;
        use crate::import::source_db::importer::{DriveRepairApplyOutcome, undo_drive_repairs};
        use crate::runtime::broken_drive_repair::{Deferral, PublicationInputs, publication_inputs};
        let (cursor_key, gate) = match publication_inputs(store, data_dir) {
            PublicationInputs::Ready(key, gate) => (key, gate),
            PublicationInputs::Deferred(Deferral::NotReady) => return Err(REPAIR_NOT_READY.to_owned()),
            PublicationInputs::Deferred(_) => return Err(REPAIR_BUSY.to_owned()),
            PublicationInputs::Failed("store") => return Err(REPAIR_STORE.to_owned()),
            PublicationInputs::Failed(code) => return Err(code.to_owned()),
        };
        let target = match target {
            UndoTarget::Batch(batch) => DriveRepairTarget::Batch(*batch),
            UndoTarget::Component(key) => DriveRepairTarget::Component(key),
        };
        match undo_drive_repairs(
            store,
            &cursor_key,
            &gate,
            vehicle,
            target,
            crate::runtime::backup_schedule::wall_ms(),
        ) {
            Ok(DriveRepairApplyOutcome::Compensated {
                compensation, served, ..
            }) => Ok(json!({
                "outcome": "undone",
                "components": compensation.components,
                "restoredRows": compensation.restored_rows,
                "served": served,
            })),
            Ok(DriveRepairApplyOutcome::Refused(_)) => Err(REPAIR_IMPORT_PENDING.to_owned()),
            Ok(_) => Err("undo".to_owned()),
            Err(error) => {
                tracing::warn!(%vehicle, %error, "repair undo failed");
                Err("undo".to_owned())
            }
        }
    }

    /// Retry: the component is looked at again by the next pass.
    pub fn retry(&self, vehicle: &str, component_key: &str) -> Result<Value, OperatorRefusal> {
        let vehicle = self.indexed_vehicle(vehicle)?;
        let key = Self::component_key(component_key)?;
        self.import_pending(vehicle)?;
        let removed = self
            .store
            .retry_drive_repair_component(vehicle, &key)
            .map_err(|error| repair_store_refusal(&error))?;
        Ok(self.finished("retry", vehicle, json!({ "lifted": removed })))
    }

    fn schedule_json(record: &crate::db::DriveRepairSchedule) -> Value {
        json!({
            "automatic": record.automatic.unwrap_or(false),
            "lastRunMs": record.last_run_ms,
            "lastAttemptMs": record.last_attempt_ms,
            "nextRunMs": record.next_run_ms,
            "lastOutcome": record.last_outcome,
            "lastFailure": record.last_failure,
            "failures": record.failures,
        })
    }

    /// Counts and the schedule for one vehicle; read-only.
    pub fn summary(&self, vehicle: &str) -> Result<Value, OperatorRefusal> {
        let vehicle = Self::vehicle(vehicle)?;
        let indexed = self
            .store
            .drive_repair_indexed_vehicles()
            .map_err(|error| repair_store_refusal(&error))?
            .contains(&vehicle);
        let summary = self
            .store
            .drive_repair_summary(vehicle)
            .map_err(|error| repair_store_refusal(&error))?;
        let schedule = self
            .scheduler
            .status(vehicle)
            .map_err(|error| repair_store_refusal(&error))?;
        let running = repair_lock(&self.jobs)
            .entries
            .iter()
            .find(|entry| entry.vehicle == vehicle && matches!(entry.state, RepairJobState::Running))
            .map(|entry| json!({ "jobId": entry.id, "kind": entry.kind }));
        Ok(json!({
            "vehicle": vehicle.to_string(),
            "indexed": indexed,
            "indexRows": summary.index_rows,
            "planned": summary.planned,
            "active": summary.active,
            "undone": summary.undone,
            "superseded": summary.superseded,
            "suppressed": summary.suppressed,
            "protected": summary.protected,
            "attested": summary.attested,
            "schedule": Self::schedule_json(&schedule),
            "runningJob": running,
        }))
    }

    /// A job's receipt: `running`, `done` with its result, or `failed` with
    /// its code. Receipts last until the service restarts.
    pub fn job(&self, job_id: u64) -> Result<Value, OperatorRefusal> {
        let jobs = repair_lock(&self.jobs);
        let entry = jobs
            .entries
            .iter()
            .find(|entry| entry.id == job_id)
            .ok_or_else(|| repair_refusal(REPAIR_JOB_UNKNOWN, "no such repair job"))?;
        let mut receipt = match &entry.state {
            RepairJobState::Running => Self::receipt(entry.id, entry.kind, entry.vehicle, "running"),
            RepairJobState::Done(result) => {
                let mut receipt = Self::receipt(entry.id, entry.kind, entry.vehicle, "done");
                receipt["result"] = result.clone();
                receipt
            }
            RepairJobState::Failed(code) => {
                let mut receipt = Self::receipt(entry.id, entry.kind, entry.vehicle, "failed");
                receipt["code"] = json!(code);
                receipt
            }
        };
        if receipt["state"] == "running" {
            receipt["result"] = Value::Null;
        }
        Ok(receipt)
    }
}

#[cfg(test)]
mod lineage_rebase_tests {
    use super::*;

    fn running_thread() -> (JoinHandle<()>, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _ = receiver.recv();
        });
        (handle, sender)
    }

    #[test]
    fn design_later_106_a_the_supervisor_predicate_spawns_only_when_due_and_idle() {
        let mut none = Vec::new();
        assert!(!lineage_rebase_spawn_due(false, &mut none), "nothing due, idle");
        assert!(lineage_rebase_spawn_due(true, &mut none), "due and idle");

        let (handle, release) = running_thread();
        let mut busy = vec![handle];
        assert!(
            !lineage_rebase_spawn_due(true, &mut busy),
            "due, but a publication thread runs"
        );
        assert!(!lineage_rebase_spawn_due(false, &mut busy));
        release.send(()).expect("release the thread");
        let mut finished = busy;
        while !finished[0].is_finished() {
            std::thread::yield_now();
        }
        assert!(
            lineage_rebase_spawn_due(true, &mut finished),
            "a finished thread no longer counts as running"
        );
        assert!(finished.is_empty(), "and it is dropped from the list");
    }

    #[test]
    fn design_later_106_c_a_due_request_the_publication_cannot_attempt_is_deferred() {
        let temporary = crate::private_tempdir().expect("temporary store");
        let store = crate::db::HubStore::initialize(temporary.path()).expect("store");
        let vehicle_id = uuid::Uuid::new_v4();
        store
            .request_v2_lineage_rebase(
                vehicle_id,
                crate::protocol::Sha256Digest::of_bytes(b"head"),
                "no_compaction_plan",
                1_000,
            )
            .expect("request");
        drop(store);
        assert!(crate::db::v2_lineage_rebase_due(temporary.path(), wall_clock_ms()));
        // No cursor key: the thread cannot build a base, so it asks again
        // later instead of at every supervisor step.
        let before = wall_clock_ms();
        publish_window_after_handback_until(temporary.path(), &AtomicBool::new(false));
        let store = crate::db::HubStore::open_existing(temporary.path()).expect("store");
        let request = store
            .v2_lineage_rebase_request(vehicle_id)
            .expect("lookup")
            .expect("request kept");
        assert_eq!(request.attempts, 1);
        assert!(request.next_attempt_ms >= before + 1_000, "asked again a second later");
    }
}
