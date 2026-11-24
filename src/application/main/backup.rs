// SPDX-License-Identifier: AGPL-3.0-only

// The service's backup schedule (M06-T03). The schedule, the one-run slot and
// the operation live in `tesatlas_hub::runtime::backup_schedule`; this only
// builds it for `serve` from the `[backup]` configuration. The recovery
// password stays in the source `backup.password_source` names (the Mac
// Keychain, or an owned file) and is read at each run; `backup-password`
// below is how the app saves it in the Keychain (M06-T17).

/// Starts the schedule for `serve`. Scheduled runs wait while the Hub is in
/// its restored-without-keys state; `backup-now` is refused there by the
/// control channel. Every run, scheduled or manual, first waits for the
/// maintenance turn it shares with the repair passes, and holds it until its
/// archive is written (REV-CX-T28).
#[cfg(unix)]
fn start_backup_schedule(
    config: &HubConfig,
    store: &HubStore,
    recovery: &std::sync::Arc<operator::authority::RecoveryGate>,
    maintenance: &tesatlas_hub::runtime::broken_drive_repair::MaintenanceExclusion,
) -> Result<
    std::sync::Arc<tesatlas_hub::runtime::backup_schedule::BackupScheduler>,
    Box<dyn std::error::Error>,
> {
    use tesatlas_hub::runtime::backup_schedule::{
        BackupScheduler, BackupSettings, StoreBackupRunner, state::STATE_FILE,
    };
    let paused = std::sync::Arc::clone(recovery);
    let scheduler = BackupScheduler::start(
        BackupSettings::from_config(&config.backup),
        std::sync::Arc::new(TurnBackupRunner {
            runner: StoreBackupRunner::new(store.clone()),
            maintenance: maintenance.clone(),
        }),
        config.data_dir.join(STATE_FILE),
        Box::new(move || paused.pending()),
    )?;
    Ok(std::sync::Arc::new(scheduler))
}

/// A backup run that holds the maintenance turn for its whole run.
#[cfg(unix)]
struct TurnBackupRunner {
    runner: tesatlas_hub::runtime::backup_schedule::StoreBackupRunner,
    maintenance: tesatlas_hub::runtime::broken_drive_repair::MaintenanceExclusion,
}

#[cfg(unix)]
impl tesatlas_hub::runtime::backup_schedule::BackupRunner for TurnBackupRunner {
    fn run(
        &self,
        job: &tesatlas_hub::runtime::backup_schedule::BackupJob,
    ) -> Result<serde_json::Value, OperatorRefusal> {
        let _turn = self.maintenance.take();
        tesatlas_hub::runtime::backup_schedule::BackupRunner::run(&self.runner, job)
    }
}

/// The app submits the original and proposed TOML as a JSON pair. Return a
/// generic refusal so malformed input never includes configuration in output.
fn run_backup_config_check(input: impl std::io::Read) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Read as _;
    const MAX_INPUT: u64 = 8 * 1024 * 1024;
    let refusal = "The backup settings could not be edited safely. The Hub configuration was left in place.";
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    input.take(MAX_INPUT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_INPUT {
        return Err(refusal.into());
    }
    let [before, after]: [String; 2] = serde_json::from_slice(&bytes).map_err(|_| refusal)?;
    let (before, after) = (zeroize::Zeroizing::new(before), zeroize::Zeroizing::new(after));
    if !tesatlas_hub::config::backup_config_edit_is_scoped(&before, &after) {
        return Err(refusal.into());
    }
    Ok(())
}

/// The monthly broken-drive repair schedule (M03-T28a): started beside the
/// backup schedule, with automatic runs off until a stored on. A due run waits
/// for an import, a hand-back, a backup (through the shared maintenance
/// turn), an open window and a restored Hub, and nothing runs before the
/// service marks it ready once its API listener is bound.
#[cfg(unix)]
fn start_repair_schedule(
    config: &HubConfig,
    store: &HubStore,
    window: &std::sync::Arc<tesatlas_hub::collection::window::WindowController>,
    maintenance: &tesatlas_hub::runtime::broken_drive_repair::MaintenanceExclusion,
    recovery: &std::sync::Arc<operator::authority::RecoveryGate>,
) -> Result<
    std::sync::Arc<tesatlas_hub::runtime::broken_drive_repair::RepairScheduler>,
    Box<dyn std::error::Error>,
> {
    use tesatlas_hub::runtime::broken_drive_repair::{
        RepairScheduler, StoreRepairRunner, service_deferral,
    };
    let recovery = std::sync::Arc::clone(recovery);
    let scheduler = RepairScheduler::start(
        store.clone(),
        std::sync::Arc::new(StoreRepairRunner::new(store.clone(), config.data_dir.clone())),
        service_deferral(
            std::sync::Arc::clone(window),
            config.data_dir.clone(),
            maintenance.clone(),
            Box::new(move || recovery.pending()),
        ),
    )?;
    Ok(std::sync::Arc::new(scheduler))
}

/// `backup-password store|compare|status --account <account>` (M06-T17,
/// DEC-123 option A): the app hands the password to this binary on standard
/// input, so the Keychain item is created and owned by the service binary the
/// LaunchAgent runs. Prints one JSON line naming the outcome, never the
/// password.
fn run_backup_password(
    action: BackupPasswordAction,
    account: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "macos")]
    {
        use tesatlas_hub::runtime::backup_schedule::keychain::{self, Comparison};
        let store = keychain::system_keychain();
        let outcome = match action {
            BackupPasswordAction::Store => {
                let password = keychain::password_from_input(std::io::stdin().lock())?;
                keychain::store_password(store.as_ref(), account, &password)?;
                "stored"
            }
            BackupPasswordAction::Create => {
                let password = keychain::password_from_input(std::io::stdin().lock())?;
                keychain::create_password(store.as_ref(), account, &password)?;
                "stored"
            }
            BackupPasswordAction::Compare => {
                let password = keychain::password_from_input(std::io::stdin().lock())?;
                match keychain::compare_password(store.as_ref(), account, &password)? {
                    Comparison::Missing => "missing",
                    Comparison::Equal => "equal",
                    Comparison::Different => "different",
                }
            }
            BackupPasswordAction::Status => {
                if keychain::password_status(store.as_ref(), account)? { "present" } else { "missing" }
            }
        };
        println!("{}", serde_json::json!({ "backupPassword": outcome }));
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (action, account);
        Err(OperatorRefusal::new(
            tesatlas_hub::runtime::backup_schedule::keychain::BACKUP_PASSWORD_INVALID,
            "the Keychain is on the Mac only; elsewhere set backup.password_source = \"file\" with backup.password_file",
        )
        .into())
    }
}
