// SPDX-License-Identifier: AGPL-3.0-only

//! Operator wiring (M02-T10). Names carry the `collection_window` prefix so
//! the milestone's `collection_window` filter runs them. The service-side
//! controller here has its own gate on a manual clock; the process-wide gate
//! wiring is proven in `tests/cli_admission.rs`, in a process of its own, so
//! that no test here ever opens the gate other library tests read.

use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};
use tempfile::TempDir;

use super::{
    admission::{self, OperatorRefusal, Route},
    authority::{OperatorService, ServiceBackend},
    channel::{self, ChannelServer, FleetSignInInput, OperatorRequest, SetupMaterial},
    cli::{Cli, Command, WindowCommand},
    trust::{OwnerConfirmation, TrustAuthority},
};
use crate::collection::window::{
    CloseReason, EgressGate, IssueRequest, Issuer, ManualClock, ManualProbe, MemoryAnchor,
    NoHighWater, PermitKind, Provider, WindowClock, WindowConfig, WindowController,
    WindowDeps,
};

const T0: i64 = 1_800_000_000;
const HOST: &str = "host-operator-test";
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A temporary directory short enough for a Unix socket path.
fn short_tempdir() -> TempDir {
    let base = std::env::temp_dir();
    if base.as_os_str().len() <= 50 {
        TempDir::new_in(base).expect("temp dir")
    } else {
        TempDir::new_in("/tmp").expect("temp dir")
    }
}

#[derive(Default)]
struct Calls(Mutex<Vec<String>>);

impl Calls {
    fn push(&self, call: &str) {
        self.0.lock().expect("calls").push(call.to_owned());
    }

    fn all(&self) -> Vec<String> {
        self.0.lock().expect("calls").clone()
    }
}

struct FakeBackend(Arc<Calls>);

impl ServiceBackend for FakeBackend {
    fn setup(
        &self,
        material: SetupMaterial,
        vehicle_id: Option<i64>,
        all_vehicles: bool,
    ) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!(
            "setup:{}:{}:{vehicle_id:?}:{all_vehicles}",
            material.code.len(),
            material.redirect_uri
        ));
        Ok(json!({ "status": "configured" }))
    }

    fn recovery_export(&self, destination: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("export:{}", destination.display()));
        Ok(json!({ "status": "exported" }))
    }

    fn recovery_restore(&self, source: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("restore:{}", source.display()));
        Ok(json!({ "status": "restored" }))
    }

    fn refused_credentials(&self) -> Result<Value, OperatorRefusal> {
        self.0.push("refused_list");
        Ok(json!({ "mode": "list" }))
    }

    fn purge_refused_credentials(&self) -> Result<Value, OperatorRefusal> {
        self.0.push("refused_purge");
        Ok(json!({ "mode": "purge" }))
    }

    fn fleet_sign_in_begin(&self, input: FleetSignInInput) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("fleet_begin:{}", input.0.len()));
        Ok(json!({ "status": "sign_in_begun" }))
    }

    fn fleet_sign_in_redeem(
        &self,
        input: FleetSignInInput,
        vehicle_id: Option<i64>,
        all_vehicles: bool,
    ) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("fleet_redeem:{}:{vehicle_id:?}:{all_vehicles}", input.0.len()));
        Ok(json!({ "status": "signed_in" }))
    }

    fn fleet_public_key(&self) -> Result<Value, OperatorRefusal> {
        self.0.push("fleet_public_key");
        Ok(json!({ "publicKeyPem": "-----BEGIN PUBLIC KEY-----" }))
    }

    fn fleet_partner_register(&self, input: FleetSignInInput) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("fleet_partner_register:{}", input.0.len()));
        Ok(json!({ "status": "partner_registered" }))
    }

    fn fleet_discover(&self, vehicle_id: Option<i64>, all_vehicles: bool) -> Result<Value, OperatorRefusal> {
        self.0.push(&format!("fleet_discover:{vehicle_id:?}:{all_vehicles}"));
        Ok(json!({ "status": "vehicles_configured" }))
    }

    fn fleet_activate(&self) -> Result<Value, OperatorRefusal> {
        self.0.push("fleet_activate");
        Ok(json!({ "status": "collection_activated" }))
    }
}

/// The owner at the Hub Mac: answers the trust confirmation as told and
/// counts the questions.
#[derive(Default)]
struct Owner {
    declines: std::sync::atomic::AtomicBool,
    asked: std::sync::atomic::AtomicU32,
}

impl OwnerConfirmation for Owner {
    fn confirm_issuer(&self, _: &str, _: u64, _: Option<&str>) -> bool {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        !self.declines.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Owner {
    fn decline(&self, declines: bool) {
        self.declines.store(declines, std::sync::atomic::Ordering::SeqCst);
    }

    fn asked(&self) -> u32 {
        self.asked.load(std::sync::atomic::Ordering::SeqCst)
    }
}

struct Rig {
    probe: Arc<ManualProbe>,
    owner: Arc<Owner>,
    pin: Arc<MemoryAnchor>,
    tmp: TempDir,
    clock: Arc<ManualClock>,
    issuer: Issuer,
    key: std::path::PathBuf,
    trust: Vec<u8>,
    controller: Arc<WindowController>,
    calls: Arc<Calls>,
    kicks: Arc<Mutex<u32>>,
}

impl Rig {
    fn new() -> Self {
        Self::with_handback(Arc::new(NoHighWater))
    }

    fn with_handback(handback: Arc<dyn crate::collection::window::HandbackSource>) -> Self {
        let tmp = short_tempdir();
        let clock = Arc::new(ManualClock::new(T0));
        let anchor = Arc::new(MemoryAnchor::new());
        let issuer = Issuer::new(
            &tmp.path().join("issuer"),
            "host-operator-issuer",
            anchor.clone(),
            clock.clone(),
        );
        let key = tmp.path().join("issuer.key");
        let trust = issuer.init(&key).expect("issuer init").to_bytes().expect("trust bytes");
        let probe = Arc::new(ManualProbe::new(0.2));
        // A private data folder, as every admitted command requires, whatever the umask.
        let data = tmp.path().join("data");
        std::fs::create_dir(&data).expect("data dir");
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700))
            .expect("private data dir");
        let (controller, _) = WindowController::start(
            WindowConfig {
                dir: super::window_dir(&data),
                claim_path: crate::collection::window::HostClaim::default_path(&tmp.path().join("home")),
                host_id: HOST.to_owned(),
            },
            WindowDeps {
                clock: clock.clone(),
                anchor,
                probe: probe.clone(),
                gate: EgressGate::new(clock.clone()),
                handback,
            },
        );
        Self {
            probe,
            owner: Arc::new(Owner::default()),
            pin: Arc::new(MemoryAnchor::new()),
            tmp,
            clock,
            issuer,
            key,
            trust,
            controller: Arc::new(controller),
            calls: Arc::new(Calls::default()),
            kicks: Arc::new(Mutex::new(0)),
        }
    }

    fn data(&self) -> std::path::PathBuf {
        self.tmp.path().join("data")
    }

    fn home(&self) -> std::path::PathBuf {
        self.tmp.path().join("home")
    }

    fn admit(&self) -> Result<admission::ImportAdmission, OperatorRefusal> {
        admission::admit_import(Some(&self.data()), &self.home())
    }

    fn service(&self) -> OperatorService {
        let kicks = self.kicks.clone();
        OperatorService::new(
            self.controller.clone(),
            Arc::new(FakeBackend(self.calls.clone())),
            Box::new(move || *kicks.lock().expect("kicks") += 1),
            TrustAuthority::new(self.pin.clone(), HOST, self.owner.clone()),
            &self.data(),
        )
    }

    fn serve(&self) -> ChannelServer {
        ChannelServer::bind(&self.data(), Arc::new(self.service())).expect("channel bound")
    }

    fn grant(&self, length: i64) -> Vec<u8> {
        let now = self.clock.wall();
        let out = self.tmp.path().join(format!("grant-{now}.json"));
        self.issuer
            .issue(
                &self.key,
                &IssueRequest {
                    host_id: HOST.to_owned(),
                    starts_at: now,
                    expires_at: now + length,
                    approver: "G".to_owned(),
                    reason: "operator test window".to_owned(),
                    approval_ref: "approval-operator-test".to_owned(),
                },
                &out,
            )
            .expect("grant issued");
        std::fs::read(out).expect("grant bytes")
    }

    fn forward(&self, request: &OperatorRequest) -> Result<Value, OperatorRefusal> {
        channel::forward(&self.data(), request, TIMEOUT)
    }
}

fn encoded(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn import_admission_does_not_reconcile_a_store_owned_by_another_process() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data directory");
    std::fs::set_permissions(rig.data(), std::fs::Permissions::from_mode(0o700))
        .expect("private data directory");
    let _owner = crate::hub_user_process::AdmittedUserHub::admit(&rig.data())
        .expect("another operation owns the store");
    let store = crate::db::HubStore::initialize(rig.data()).expect("store");
    let connection = store.open().expect("catalogue");
    connection.execute(
        "INSERT INTO collection_coverage
         (seq, grant_digest, opened_at_ms, closed_at_ms, gaps_json, sealed)
         VALUES (17, 'synthetic-grant', 1000, NULL, '[]', 'open')",
        [],
    ).expect("unfinished coverage owned by the first operation");

    let outcome = rig.admit();
    let closed: Option<i64> = connection.query_row(
        "SELECT closed_at_ms FROM collection_coverage WHERE seq = 17", [], |row| row.get(0),
    ).expect("coverage");
    assert_eq!(closed, None, "a competing command must not seal the owner's coverage");
    assert!(outcome.is_err(), "a competing command must refuse before reconciliation");
}

#[test]
fn import_admission_does_not_reconcile_a_newer_store() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data directory");
    std::fs::set_permissions(rig.data(), std::fs::Permissions::from_mode(0o700))
        .expect("private data directory");
    let store = crate::db::HubStore::initialize(rig.data()).expect("store");
    let connection = store.open().expect("catalogue");
    connection.execute(
        "INSERT INTO collection_coverage
         (seq, grant_digest, opened_at_ms, closed_at_ms, gaps_json, sealed)
         VALUES (17, 'synthetic-grant', 1000, NULL, '[]', 'open')",
        [],
    ).expect("unfinished coverage from a newer store");
    connection.pragma_update(None, "user_version", crate::db::SCHEMA_VERSION + 1)
        .expect("simulate a newer Hub schema");

    let outcome = rig.admit();
    let closed: Option<i64> = connection.query_row(
        "SELECT closed_at_ms FROM collection_coverage WHERE seq = 17", [], |row| row.get(0),
    ).expect("coverage");
    assert_eq!(closed, None, "an older command must not seal newer-schema coverage");
    assert!(outcome.is_err(), "a newer schema must refuse before reconciliation");
}

fn material() -> SetupMaterial {
    SetupMaterial {
        code: "code-0001".to_owned(),
        code_verifier: "v".repeat(43),
        redirect_uri: "https://auth.tesla.com/void/callback".to_owned(),
    }
}

#[test]
fn collection_window_operator_channel_forwards_window_verbs_to_the_service() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["phase"], "closed");
    assert_eq!(status["trusted"], false);

    let status = rig
        .forward(&OperatorRequest::WindowTrust {
            document: encoded(&rig.trust),
        })
        .expect("trust");
    assert_eq!(status["trusted"], true);

    // Sign-in is refused outside a window, before the backend is asked.
    let refused = rig
        .forward(&OperatorRequest::Setup {
            material: material(),
            vehicle_id: None,
            all_vehicles: true,
        })
        .unwrap_err();
    assert_eq!(refused.code, "window_closed");
    assert!(rig.calls.all().is_empty());

    let gate = rig.controller.gate().clone();
    assert_eq!(
        gate.permit(Provider::Legacy, PermitKind::Request).unwrap_err().code(),
        "window_closed"
    );
    let status = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&rig.grant(3_600)),
        })
        .expect("open");
    assert_eq!(status["phase"], "open");
    assert_eq!(status["deadline"], T0 + 3_600);
    assert!(gate.permit(Provider::Legacy, PermitKind::Request).is_ok());
    assert!(gate.permit(Provider::Fleet, PermitKind::Request).is_ok());

    // Inside the window the bounded code material reaches the backend.
    rig.forward(&OperatorRequest::Setup {
        material: material(),
        vehicle_id: Some(7),
        all_vehicles: false,
    })
    .expect("setup forwarded");
    assert_eq!(
        rig.calls.all(),
        vec!["setup:9:https://auth.tesla.com/void/callback:Some(7):false".to_owned()]
    );

    // Recovery waits for the window to close.
    let refused = rig
        .forward(&OperatorRequest::RecoveryExport {
            destination: rig.tmp.path().join("export"),
            recovery_key_file: rig.tmp.path().join("recovery.key"),
        })
        .unwrap_err();
    assert_eq!(refused.code, "window_active");

    let status = rig.forward(&OperatorRequest::WindowClose).expect("close");
    assert_eq!(status["phase"], "closing");
    assert!(gate.permit(Provider::Legacy, PermitKind::Request).is_err());
    assert_eq!(*rig.kicks.lock().expect("kicks"), 3, "trust, open and close kick");

    rig.controller.finish_close().expect("handback");
    rig.forward(&OperatorRequest::RecoveryRestore {
        source: rig.tmp.path().join("export"),
        recovery_key_file: rig.tmp.path().join("recovery.key"),
    })
    .expect("restore forwarded once Closed");
    assert_eq!(rig.calls.all().len(), 2);
}

/// The Fleet sign-in (M09-T09) is forwarded like the owner sign-in: refused
/// `window_closed` outside an Open window before the backend is asked, and
/// its input, which may hold the client secret, never printed.
#[test]
fn fleet_sign_in_operator_channel_needs_an_open_window() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let input = || FleetSignInInput(r#"{"clientSecret":"fleet-secret-0001"}"#.to_owned());
    assert_eq!(format!("{:?}", input()), "FleetSignInInput { .. }");
    let requests = [
        OperatorRequest::FleetSignInBegin { input: input() },
        OperatorRequest::FleetSignInRedeem { input: input(), vehicle_id: Some(7), all_vehicles: false },
    ];
    for request in &requests {
        assert!(!format!("{request:?}").contains("fleet-secret-0001"));
        assert_eq!(rig.forward(request).unwrap_err().code, "window_closed");
    }
    assert!(rig.calls.all().is_empty(), "nothing reached the backend");
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&rig.grant(3_600)),
    })
    .expect("open");
    for request in &requests {
        rig.forward(request).expect("forwarded inside the window");
    }
    assert_eq!(rig.calls.all(), vec!["fleet_begin:36".to_owned(), "fleet_redeem:36:Some(7):false".to_owned()]);
}

/// M09-T19 rulings 3 and 4: the discovery again with the stored login sends,
/// so it is refused `window_closed` outside an Open window before the
/// backend is asked; the activation sends nothing and needs no window.
#[test]
fn fleet_discover_needs_an_open_window_and_activate_does_not() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let discover = OperatorRequest::FleetDiscover { vehicle_id: Some(7), all_vehicles: false };
    assert_eq!(rig.forward(&discover).unwrap_err().code, "window_closed");
    rig.forward(&OperatorRequest::FleetActivate).expect("activation outside a window");
    assert_eq!(rig.calls.all(), vec!["fleet_activate".to_owned()]);
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&rig.grant(3_600)),
    })
    .expect("open");
    rig.forward(&discover).expect("forwarded inside the window");
    assert_eq!(rig.calls.all(), vec!["fleet_activate".to_owned(), "fleet_discover:Some(7):false".to_owned()]);
}

/// M09-T19 ruling 8: a Fleet input travels base64-encoded, so the request's
/// JSON carries no escape for its decoder to copy into a scratch buffer, and
/// it arrives exactly as sent, quotes and backslashes included.
#[test]
fn fleet_sign_in_input_travels_without_json_escapes() {
    let text = r#"{"clientSecret":"a"b\c","code":"x"}"#;
    let request = OperatorRequest::FleetSignInBegin { input: FleetSignInInput(text.to_owned()) };
    let wire = serde_json::to_string(&request).expect("encoded");
    assert!(!wire.contains('\\'), "no JSON escape on the wire: {wire}");
    assert!(!wire.contains("clientSecret"), "the input is not readable as text on the wire");
    let OperatorRequest::FleetSignInBegin { input } = serde_json::from_str(&wire).expect("decoded") else { panic!("the same request") };
    assert_eq!(input.0, text);
}

/// The Fleet partner verbs (M09-T18): the public key export sends nothing
/// and needs no window; the domain registration is refused `window_closed`
/// outside an Open window before the backend is asked, and its input, which
/// holds the client secret, is never printed.
#[test]
fn fleet_partner_operator_channel_gates_the_registration_only() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let register = OperatorRequest::FleetPartnerRegister {
        input: FleetSignInInput(r#"{"clientSecret":"fleet-secret-0002"}"#.to_owned()),
    };
    assert!(!format!("{register:?}").contains("fleet-secret-0002"));
    rig.forward(&OperatorRequest::FleetPublicKey).expect("key export outside a window");
    assert_eq!(rig.forward(&register).unwrap_err().code, "window_closed");
    assert_eq!(rig.calls.all(), vec!["fleet_public_key".to_owned()]);
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&rig.grant(3_600)),
    })
    .expect("open");
    rig.forward(&register).expect("forwarded inside the window");
    assert_eq!(rig.calls.all(), vec!["fleet_public_key".to_owned(), "fleet_partner_register:36".to_owned()]);
}

#[test]
fn collection_window_operator_channel_refuses_a_replayed_grant_and_bad_material() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let grant = rig.grant(600);
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&grant),
    })
    .expect("open");
    rig.controller.close(CloseReason::OperatorClose);
    rig.controller.finish_close().expect("closed");
    let refused = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&grant),
        })
        .unwrap_err();
    assert_eq!(refused.code, "grant_consumed");
    let refused = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: "not base64!".to_owned(),
        })
        .unwrap_err();
    assert_eq!(refused.code, "request_invalid");
    let refused = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&vec![b' '; crate::collection::window::GRANT_MAX_BYTES + 1]),
        })
        .unwrap_err();
    assert_eq!(refused.code, "request_invalid");

    // A token-shaped request is not understood by the service at all.
    let stream = channel::connect(&rig.data()).expect("connected");
    let mut stream = stream;
    use std::io::{Read, Write};
    stream
        .write_all(br#"{"op":"setup","material":{"code":"c","code_verifier":"v","redirect_uri":"r","accessToken":"a"},"vehicle_id":null,"all_vehicles":false}"#)
        .expect("written");
    stream.shutdown(std::net::Shutdown::Write).expect("shutdown");
    let mut answer = String::new();
    stream.read_to_string(&mut answer).expect("answer");
    assert!(answer.contains("request_invalid"), "{answer}");
    assert!(rig.calls.all().is_empty());
}

#[test]
fn collection_window_operator_refuses_service_verbs_when_no_service_runs() {
    let tmp = short_tempdir();
    let data = tmp.path().join("data");
    // No data directory, then no channel directory, then no listener.
    for prepare in [0, 1, 2] {
        if prepare >= 1 {
            std::fs::create_dir_all(&data).expect("data dir");
        }
        if prepare == 2 {
            let dir = channel::channel_dir(&data);
            std::fs::create_dir_all(&dir).expect("channel dir");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("mode");
            let listener =
                std::os::unix::net::UnixListener::bind(channel::socket_path(&data)).expect("bind");
            drop(listener);
        }
        let refused = channel::forward(&data, &OperatorRequest::WindowStatus, TIMEOUT).unwrap_err();
        assert_eq!(refused.code, "service_not_running", "case {prepare}");
    }
    // A channel directory open to others is refused, not trusted.
    std::fs::set_permissions(
        channel::channel_dir(&data),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("mode");
    let refused = channel::forward(&data, &OperatorRequest::WindowStatus, TIMEOUT).unwrap_err();
    assert_eq!(refused.code, "control_channel_unsafe");
}

#[test]
fn collection_window_operator_import_admission_follows_the_window() {
    let rig = Rig::new();
    // A Hub that never trusted an issuer has no window to wait for.
    rig.admit().expect("fresh Hub admits import");
    rig.controller
        .trust(&crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust"))
        .expect("trusted");
    let held = rig.admit().expect("Closed admits import");
    drop(held);
    rig.controller.open(&rig.grant(600)).expect("open");
    assert_eq!(
        rig.admit().unwrap_err().code,
        "window_active"
    );
    rig.controller.close(CloseReason::OperatorClose);
    assert_eq!(
        rig.admit().unwrap_err().code,
        "window_active",
        "Closing still refuses"
    );
    rig.controller.finish_close().expect("handback");
    let held = rig.admit().expect("admitted after handback");
    // The issuer keeps its gap between grants.
    rig.clock.advance(2_000);
    // While the import holds the lock no window opens.
    assert_eq!(
        rig.controller.open(&rig.grant(600)).unwrap_err().code(),
        "import_running"
    );
    drop(held);
    // A corrupt state file fails closed.
    std::fs::write(super::window_dir(&rig.data()).join("state.json"), b"{").expect("corrupt");
    assert_eq!(
        rig.admit().unwrap_err().code,
        "state_unverifiable"
    );
}

fn parsed(arguments: &[&str]) -> Result<Command, String> {
    let mut all = vec!["tesatlas-hub"];
    all.extend_from_slice(arguments);
    Cli::try_parse_from(all)
        .map(|cli| cli.command)
        .map_err(|error| error.to_string())
}

#[test]
fn collection_window_operator_routes_every_operator_command() {
    let route = |arguments: &[&str]| admission::route(&parsed(arguments).expect("parsed"));
    assert_eq!(route(&["setup", "--all-vehicles"]), Route::Service);
    assert_eq!(route(&["setup-fleet"]), Route::Service);
    assert_eq!(route(&["setup-fleet", "begin"]), Route::Service);
    assert_eq!(route(&["setup-fleet", "register"]), Route::Service);
    assert_eq!(route(&["setup-fleet", "public-key", "--output", "key.pem"]), Route::Service);
    assert_eq!(route(&["configure-fleet-telemetry"]), Route::Refused("provider_parked"));
    for verb in [
        vec!["window", "status"],
        vec!["window", "close"],
        vec!["window", "trust", "trust.json"],
        vec!["window", "open", "--grant", "grant.json"],
        vec!["export-recovery-credentials", "--destination", "d", "--recovery-key-file", "k"],
        vec!["restore-recovery-credentials", "--source", "s", "--recovery-key-file", "k"],
    ] {
        assert_eq!(route(&verb), Route::Service, "{verb:?}");
    }
    for verb in [
        vec!["window", "host-id"],
        vec!["window", "issuer-init", "--key-out", "k", "--trust-out", "t"],
        vec!["window", "issuer-trust", "--key-file", "k", "--trust-out", "t"],
        vec!["window", "void", "--key-file", "k", "--seq", "1", "--reason", "r"],
    ] {
        assert_eq!(route(&verb), Route::Issuer, "{verb:?}");
    }
    assert_eq!(route(&["restore-data", "--source", "s", "--destination", "d"]), Route::Import);
    assert_eq!(route(&["doctor"]), Route::Local);
    // Forwarded commands never take the lifetime lock (ruling R5).
    for verb in [
        vec!["setup"],
        vec!["window", "open", "--grant", "g"],
        vec!["export-recovery-credentials", "--destination", "d", "--recovery-key-file", "k"],
    ] {
        assert!(!admission::requires_user_hub_admission(&parsed(&verb).expect("parsed")));
    }
    assert!(admission::requires_user_hub_admission(&parsed(&["serve"]).expect("parsed")));
    assert!(matches!(
        parsed(&["window", "open", "--grant", "g"]).expect("parsed"),
        Command::Window { command: WindowCommand::Open { .. } }
    ));
}

#[test]
fn collection_window_operator_removed_inputs_are_rejected_by_the_parser() {
    for removed in [
        vec!["setup", "--tokens-stdin"],
        vec!["setup", "--access-token-file", "a", "--refresh-token-file", "r"],
        vec!["setup", "--sign-in"],
        vec!["setup-fleet", "--sign-in"],
        vec!["source-check", "--source", "postgres://h/db", "--car-id", "1", "--postgres-password-file", "p", "--history-only"],
    ] {
        let error = parsed(&removed).unwrap_err();
        assert!(error.contains("unexpected argument"), "{removed:?}: {error}");
    }
    for removed in [
        "--source-token-key-file",
        "--encryption-key-file",
        "--access-token-file",
        "--refresh-token-file",
        "--replace-credentials",
        "--preserve-existing-credentials",
    ] {
        let error = parsed(&[
            "migrate", "--source", "postgres://h/db", "--car-id", "1",
            "--postgres-password-file", "p", "--accept-schema-evidence", removed, "x",
        ])
        .unwrap_err();
        assert!(error.contains("unexpected argument"), "{removed}: {error}");
    }
}

#[test]
fn collection_window_operator_host_id_and_times() {
    let ioreg = "  \"IOPlatformSerialNumber\" = \"XYZ\"\n  \"IOPlatformUUID\" = \"00000000-1111-2222-3333-444444444444\"\n";
    let uuid = super::host::parse_platform_uuid(ioreg).expect("uuid");
    assert_eq!(uuid, "00000000-1111-2222-3333-444444444444");
    let id = super::host::host_id_for(&uuid);
    assert!(id.starts_with("host-") && id.len() == 37, "{id}");
    assert_ne!(id, super::host::host_id_for("00000000-1111-2222-3333-444444444445"));
    assert!(!id.contains("1111"), "the raw identifier is not carried");
    assert_eq!(super::issuer::parse_time("1800000000").expect("seconds"), 1_800_000_000);
    assert_eq!(
        super::issuer::parse_time("2027-01-15T08:00:00Z").expect("rfc3339"),
        1_800_000_000
    );
    for bad in ["2027-01-15 08:00:00", "2027-13-01T00:00:00Z", "2027-01-15T08:00:00+01:00", "soon"] {
        assert_eq!(super::issuer::parse_time(bad).unwrap_err().code, "time_invalid", "{bad}");
    }
}

/// Finding 3: a process of the Hub's own user with an issuer key of its own
/// cannot install that key through the socket, before or after the owner's
/// bootstrap, so it cannot open a window; the pinned issuer's endorsed new
/// epoch is accepted without asking the owner.
#[test]
fn collection_window_operator_trust_needs_the_owner_or_the_pinned_issuer() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    let attacker = Issuer::new(
        &rig.tmp.path().join("attacker"),
        "host-attacker",
        Arc::new(MemoryAnchor::new()),
        rig.clock.clone(),
    );
    let attacker_key = rig.tmp.path().join("attacker.key");
    attacker.init(&attacker_key).expect("attacker issuer");
    let attacker_grant = || {
        let out = rig.tmp.path().join(format!("attacker-grant-{}.json", rig.clock.wall()));
        attacker
            .issue(
                &attacker_key,
                &IssueRequest {
                    host_id: HOST.to_owned(),
                    starts_at: rig.clock.wall(),
                    expires_at: rig.clock.wall() + 600,
                    approver: "G".to_owned(),
                    reason: "not approved".to_owned(),
                    approval_ref: "none".to_owned(),
                },
                &out,
            )
            .expect("attacker grant");
        encoded(&std::fs::read(out).expect("grant bytes"))
    };
    let trust = |bytes: &[u8]| {
        rig.forward(&OperatorRequest::WindowTrust {
            document: encoded(bytes),
        })
    };

    // A fresh Hub: the attacker's bootstrap is not the owner's.
    rig.owner.decline(true);
    let own = attacker.bump_trust_epoch(&attacker_key).expect("epoch 2");
    let refused = trust(&own.to_bytes().expect("bytes")).unwrap_err();
    assert_eq!(refused.code, "owner_confirmation_refused");
    assert!(!rig.controller.status().trusted);
    let refused = rig
        .forward(&OperatorRequest::WindowOpen { grant: attacker_grant() })
        .unwrap_err();
    assert_eq!(refused.code, "trust_missing");

    // The owner bootstraps G's issuer.
    rig.owner.decline(false);
    trust(&rig.trust).expect("owner bootstrap");
    assert_eq!(rig.owner.asked(), 2);

    // After it, neither a plain nor a self-endorsed newer document of the
    // attacker's key is accepted.
    rig.owner.decline(true);
    let newer = attacker.bump_trust_epoch(&attacker_key).expect("epoch 3");
    let plain = trust(&newer.to_bytes().expect("bytes")).unwrap_err();
    assert_eq!(plain.code, "owner_confirmation_refused");
    let endorsed = super::trust::endorse(&newer, &attacker_key).expect("self-endorsed");
    let refused = trust(&endorsed).unwrap_err();
    assert_eq!(refused.code, "owner_confirmation_refused");
    // A plain newer document of G's own key needs the owner as well.
    let mut bumped = crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust");
    bumped.trust_epoch = 9;
    assert_eq!(
        trust(&bumped.to_bytes().expect("bytes")).unwrap_err().code,
        "owner_confirmation_refused"
    );
    assert_eq!(rig.owner.asked(), 5);
    // The attacker's issuer keeps its own gap between grants.
    rig.clock.advance(2_000);
    let refused = rig
        .forward(&OperatorRequest::WindowOpen { grant: attacker_grant() })
        .unwrap_err();
    assert_eq!(refused.code, "key_id_mismatch");
    assert!(!rig.controller.gate().is_open());

    // G's rotation, endorsed by the pinned key, needs no confirmation.
    let rotated = rig.issuer.bump_trust_epoch(&rig.key).expect("G's epoch 2");
    let status = trust(&super::trust::endorse(&rotated, &rig.key).expect("endorsed"))
        .expect("pinned issuer's rotation");
    assert_eq!(status["trusted"], true);
    assert_eq!(rig.owner.asked(), 5, "no confirmation asked");
    let status = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&rig.grant(600)),
        })
        .expect("G's grant of the new epoch opens");
    assert_eq!(status["phase"], "open");
}

/// Finding 4: the import pauses after it holds its locks and before it reads
/// the window state; a `window open` in that pause is refused, so the import
/// is never admitted while a window is open. The first use of a Hub with no
/// window directory is locked the same way.
#[test]
fn collection_window_operator_import_admission_locks_before_it_reads_the_state() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    // First use: no window directory yet.
    let mut opened = None;
    let held = admission::admit_import_observed(Some(&rig.data()), &rig.home(), &mut || {
        rig.controller
            .trust(&crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust"))
            .expect_err("no trust while the import holds the window lock");
        opened = Some(());
    })
    .expect("a fresh Hub admits the import");
    assert!(opened.is_some());
    drop(held);
    rig.controller
        .trust(&crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust"))
        .expect("trusted");
    let grant = rig.grant(600);
    let mut refusal = None;
    let held = admission::admit_import_observed(Some(&rig.data()), &rig.home(), &mut || {
        refusal = Some(rig.controller.open(&grant).unwrap_err().code());
    })
    .expect("admitted: the window could not open in the pause");
    assert_eq!(refusal, Some("import_running"));
    assert_eq!(rig.controller.status().phase, crate::collection::window::Phase::Closed);
    assert!(!rig.controller.gate().is_open());
    drop(held);
    rig.controller.open(&grant).expect("opens once the import ends");
    assert_eq!(rig.admit().unwrap_err().code, "window_active");
}

/// Finding 8: a restore with no configuration, or with the configuration of
/// another, Closed Hub, is bound to this host's collection authority: it is
/// refused while any window is open on the host and holds the host claim
/// while it runs.
#[test]
fn collection_window_operator_restore_admission_follows_the_host_claim() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let other = rig.tmp.path().join("other-data");
    std::fs::create_dir_all(&other).expect("other data dir");
    std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o700))
        .expect("private other data dir");
    rig.controller
        .trust(&crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust"))
        .expect("trusted");
    rig.controller.open(&rig.grant(600)).expect("open");
    for data in [None, Some(other.as_path())] {
        assert_eq!(
            admission::admit_import(data, &rig.home()).unwrap_err().code,
            "window_active",
            "{data:?}"
        );
    }
    rig.controller.close(CloseReason::OperatorClose);
    rig.controller.finish_close().expect("handback");
    let held = admission::admit_import(None, &rig.home()).expect("no window on the host");
    rig.clock.advance(2_000);
    assert_eq!(
        rig.controller.open(&rig.grant(600)).unwrap_err().code(),
        "collector_busy",
        "no window opens while the configuration-free restore runs"
    );
    drop(held);
    let held = admission::admit_import(Some(&other), &rig.home()).expect("other Hub");
    rig.clock.advance(2_000);
    assert_eq!(rig.controller.open(&rig.grant(600)).unwrap_err().code(), "collector_busy");
    drop(held);
}

/// Records when the handback reads the high-water marks.
struct RecordingHandback(Arc<Calls>);

impl crate::collection::window::HandbackSource for RecordingHandback {
    fn high_water(&self) -> crate::collection::window::HighWater {
        self.0.push("handback");
        crate::collection::window::HighWater::default()
    }
}

/// Waits until the probe has been asked more times than `seen`; the bound is
/// a hang guard only, far beyond any loaded host.
async fn probe_asked_again(probe: &ManualProbe, seen: u32) -> u32 {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            let calls = probe.calls();
            if calls > seen {
                return calls;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the supervisor asked the time probe again")
}

/// Finding 5: a long observation is supervised for its whole life. When
/// the network time check fails mid-observation the window closes, the
/// observation is told to stop, and the handback is written only after the
/// observation has released its collector lease.
#[tokio::test]
async fn collection_window_operator_observation_is_supervised_for_its_whole_life() {
    let calls = Arc::new(Calls::default());
    let rig = Rig::with_handback(Arc::new(RecordingHandback(calls.clone())));
    rig.controller
        .trust(&crate::collection::window::TrustDocument::parse(&rig.trust).expect("trust"))
        .expect("trusted");
    rig.controller.open(&rig.grant(3_600)).expect("open");
    let controller = rig.controller.clone();
    let observed = calls.clone();
    let supervised = tokio::spawn(super::authority::supervise_observation(
        controller,
        std::time::Duration::from_millis(10),
        move |window_left| async move {
            // An observation far longer than the test: it ends only when told.
            let _ = window_left.await;
            observed.push("lease_released");
            "observed"
        },
    ));
    assert_eq!(rig.controller.status().phase, crate::collection::window::Phase::Open);
    // The time server stops answering; the next check is due.
    rig.probe.set(Err(crate::collection::window::Refusal::ClockOffset));
    let mut seen_calls = rig.probe.calls();
    // One miss is tolerated (DEC-131): the window stays Open until the third
    // consecutive miss, each due after the retry interval.
    // Each step waits for the supervisor's own tick to ask the probe, not for
    // a fixed time, so a busy host only makes the test slower.
    rig.clock.advance(crate::collection::window::NTP_RECHECK_SECONDS + 1);
    seen_calls = probe_asked_again(&rig.probe, seen_calls).await;
    assert_eq!(rig.controller.status().phase, crate::collection::window::Phase::Open);
    for _ in 1..crate::collection::window::clock::NTP_MAX_FAILURES {
        rig.clock.advance(crate::collection::window::clock::NTP_RETRY_SECONDS + 1);
        seen_calls = probe_asked_again(&rig.probe, seen_calls).await;
    }
    let (result, handback) = tokio::time::timeout(std::time::Duration::from_secs(120), supervised)
        .await
        .expect("the observation was stopped by the supervision")
        .expect("joined");
    assert_eq!(result, "observed");
    let handback = handback.expect("handback").expect("this run closed the window");
    assert_eq!(handback.reason, CloseReason::ClockInvalid);
    assert_eq!(calls.all(), vec!["lease_released".to_owned(), "handback".to_owned()]);
    assert_eq!(rig.controller.status().phase, crate::collection::window::Phase::Closed);
    assert!(!rig.controller.gate().is_open());
}

/// Finding 6: a client that trickles its request is cut off at the
/// absolute deadline and never stands between the operator and `window
/// close`; stopping the service interrupts it; the service never reads its
/// standard input or a relative path for recovery.
#[test]
fn collection_window_operator_channel_has_a_deadline_and_stops_promptly() {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let server = ChannelServer::bind_with_deadline(
        &rig.data(),
        Arc::new(rig.service()),
        Duration::from_millis(600),
    )
    .expect("channel bound");
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&rig.grant(600)),
    })
    .expect("open");

    // A slow client sends one byte every 100 ms and never finishes.
    let mut slow = channel::connect(&rig.data()).expect("slow client");
    let trickle = {
        let mut writer = slow.try_clone().expect("clone");
        std::thread::spawn(move || {
            for _ in 0..100 {
                if writer.write_all(b" ").is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    // The operator's close is answered while the slow request is pending.
    let started = Instant::now();
    let status = rig.forward(&OperatorRequest::WindowClose).expect("close");
    assert_eq!(status["phase"], "closing");
    assert!(started.elapsed() < Duration::from_millis(500), "{:?}", started.elapsed());
    // The slow request is cut off at its deadline, not when it stops.
    slow.set_read_timeout(Some(Duration::from_secs(5))).expect("timeout");
    let mut answer = String::new();
    let _ = slow.read_to_string(&mut answer);
    assert!(answer.contains("in time"), "{answer}");
    assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
    trickle.join().expect("trickle");

    // A stop interrupts a request in progress instead of waiting for it.
    let mut pending = channel::connect(&rig.data()).expect("pending client");
    pending.write_all(b"{").expect("partial");
    std::thread::sleep(Duration::from_millis(100));
    let stopping = Instant::now();
    drop(server);
    assert!(stopping.elapsed() < Duration::from_millis(500), "{:?}", stopping.elapsed());
    // The service shut the connection down: the client reads its end at once.
    let _ = pending.set_read_timeout(Some(Duration::from_secs(5)));
    let mut rest = Vec::new();
    let _ = pending.read_to_end(&mut rest);

    // Recovery never reads the service's standard input or a relative path.
    rig.controller.finish_close().expect("handback");
    let service = rig.service();
    use super::channel::OperatorHandler;
    for (source, key) in [
        ("-", "/abs/recovery.key"),
        ("/abs/export", "-"),
        ("relative/export", "/abs/recovery.key"),
        ("/abs/export", "recovery.key"),
    ] {
        let refused = service
            .handle(OperatorRequest::RecoveryRestore {
                source: source.into(),
                recovery_key_file: key.into(),
            })
            .unwrap_err();
        assert_eq!(refused.code, "request_invalid", "{source} {key}");
        let refused = service
            .handle(OperatorRequest::RecoveryExport {
                destination: source.into(),
                recovery_key_file: key.into(),
            })
            .unwrap_err();
        assert_eq!(refused.code, "request_invalid", "{source} {key}");
    }
    assert!(rig.calls.all().is_empty(), "the backend was never asked");
}

/// Restores the original cursor key, as the vault's credential restore does.
struct RestoringBackend {
    data: std::path::PathBuf,
    key: [u8; 32],
    calls: Arc<Calls>,
}

impl ServiceBackend for RestoringBackend {
    fn setup(&self, _: SetupMaterial, _: Option<i64>, _: bool) -> Result<Value, OperatorRefusal> {
        self.calls.push("setup");
        Ok(json!({}))
    }

    fn recovery_export(&self, _: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
        self.calls.push("export");
        Ok(json!({}))
    }

    fn recovery_restore(&self, _: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
        use std::os::unix::fs::OpenOptionsExt;
        self.calls.push("restore");
        let secrets = self.data.join("secrets");
        std::fs::create_dir_all(&secrets).expect("secrets");
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).expect("mode");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(crate::legacy_owner_credentials::cursor_key_path(&self.data))
            .map_err(|_| OperatorRefusal::new("recovery_failed", "a key already exists"))?;
        std::io::Write::write_all(&mut file, &self.key).expect("key");
        Ok(json!({ "status": "restored", "cursorKeyIncluded": true }))
    }

    fn refused_credentials(&self) -> Result<Value, OperatorRefusal> {
        self.calls.push("refused_list");
        Ok(json!({}))
    }

    fn purge_refused_credentials(&self) -> Result<Value, OperatorRefusal> {
        self.calls.push("refused_purge");
        Ok(json!({}))
    }
}

/// Finding 2: after a data-only restore the service waits in its recovery
/// state. Nothing that could create a key or sign data runs; the credential
/// restore then writes the original key, and the API's key loader returns
/// that key instead of a new one.
#[tokio::test]
async fn collection_window_operator_recovery_state_restores_keys_before_any_is_created() {
    let rig = Rig::new();
    let data = rig.data();
    crate::db::HubStore::initialize(&data).expect("store");
    assert!(!super::authority::credential_recovery_pending(&data), "nothing signed yet");
    // A data-only restore: signed history, no cursor key.
    // The head alone is enough for the check; its parents are not needed.
    let connection = rusqlite::Connection::open(data.join("hub.sqlite")).expect("database");
    connection.execute_batch("PRAGMA foreign_keys = OFF;").expect("no parents");
    connection
        .execute(
            "INSERT INTO sync_heads (vehicle_id, base_snapshot_id, head_sequence, head_digest, terminal_cursor) VALUES ('v', 's', 1, ?1, 'c')",
            [&"a".repeat(64)],
        )
        .expect("signed head");
    drop(connection);
    assert!(super::authority::credential_recovery_pending(&data));

    let gate = super::authority::RecoveryGate::new(true);
    let original = [7_u8; 32];
    let kicks = rig.kicks.clone();
    let service = OperatorService::new(
        rig.controller.clone(),
        Arc::new(RestoringBackend {
            data: data.clone(),
            key: original,
            calls: rig.calls.clone(),
        }),
        Box::new(move || *kicks.lock().expect("kicks") += 1),
        TrustAuthority::new(rig.pin.clone(), HOST, rig.owner.clone()),
        &data,
    )
    .with_recovery(gate.clone());
    let _server = ChannelServer::bind(&data, Arc::new(service)).expect("channel bound");

    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["credentialRecoveryPending"], true);
    for request in [
        OperatorRequest::WindowTrust { document: encoded(&rig.trust) },
        OperatorRequest::WindowOpen { grant: encoded(b"{}") },
        OperatorRequest::Setup { material: material(), vehicle_id: None, all_vehicles: true },
        OperatorRequest::RecoveryExport {
            destination: "/abs/export".into(),
            recovery_key_file: "/abs/recovery.key".into(),
        },
    ] {
        assert_eq!(rig.forward(&request).unwrap_err().code, "credential_recovery_pending");
    }
    assert!(rig.calls.all().is_empty());
    assert!(
        std::fs::symlink_metadata(crate::legacy_owner_credentials::cursor_key_path(&data)).is_err(),
        "no key was created while waiting"
    );

    rig.forward(&OperatorRequest::RecoveryRestore {
        source: "/abs/export".into(),
        recovery_key_file: "/abs/recovery.key".into(),
    })
    .expect("restore");
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.restored())
        .await
        .expect("the recovery state ended");
    assert!(!super::authority::credential_recovery_pending(&data));
    // The API now starts with the recovered key, not a new one.
    crate::legacy_owner_credentials::load_or_create_cursor_key(&data).expect("key loads");
    assert_eq!(
        std::fs::read(crate::legacy_owner_credentials::cursor_key_path(&data)).expect("key file"),
        original,
        "the API's key is the restored one"
    );
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["credentialRecoveryPending"], false);
    rig.forward(&OperatorRequest::WindowTrust { document: encoded(&rig.trust) })
        .expect("window verbs answer again");
}

/// M02-T11 acceptance 1 and 3: the purge command parses with a listing mode
/// and a confirmation flag, is forwarded to the running service, lists
/// without the confirmation (deleting nothing) and purges only once the
/// window is Closed, holding the import lock.
#[test]
fn refused_credential_purge_is_forwarded_lists_without_confirmation_and_waits_for_the_window() {
    for (arguments, confirm) in [
        (&["tesatlas-hub", "purge-refused-credentials"][..], false),
        (&["tesatlas-hub", "purge-refused-credentials", "--confirm"][..], true),
    ] {
        let cli = Cli::try_parse_from(arguments.iter().copied()).expect("parses");
        assert!(
            matches!(cli.command, Command::PurgeRefusedCredentials { confirm: parsed } if parsed == confirm),
            "{arguments:?}"
        );
        assert_eq!(admission::route(&cli.command), Route::Service, "forwarded to the service");
        assert!(!admission::requires_user_hub_admission(&cli.command), "never takes the lifetime lock");
    }
    assert!(Cli::try_parse_from(["tesatlas-hub", "purge-refused-credentials", "--yes"]).is_err());

    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    let list = OperatorRequest::PurgeRefusedCredentials { confirm: false };
    let purge = OperatorRequest::PurgeRefusedCredentials { confirm: true };
    assert_eq!(rig.forward(&list).expect("listing")["mode"], "list");
    assert_eq!(rig.calls.all(), vec!["refused_list".to_owned()], "without --confirm only the listing runs");

    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    rig.forward(&OperatorRequest::WindowOpen {
        grant: encoded(&rig.grant(600)),
    })
    .expect("open");
    let refused = rig.forward(&purge).unwrap_err();
    assert_eq!(refused.code, "window_active", "no purge inside a window");
    rig.forward(&list).expect("the listing is read-only, so it runs in a window");
    rig.forward(&OperatorRequest::WindowClose).expect("close");
    assert_eq!(rig.forward(&purge).unwrap_err().code, "window_active", "not while Closing");
    rig.controller.finish_close().expect("handback");
    assert_eq!(rig.forward(&purge).expect("purge")["mode"], "purge");
    assert_eq!(
        rig.calls.all(),
        vec!["refused_list".to_owned(), "refused_list".to_owned(), "refused_purge".to_owned()],
        "the purge reached the backend once, after the window closed"
    );
}

// Deprecated aliases (M05-T05). Names carry `deprecated_alias` so the
// milestone's filter runs them. An alias only maps an earlier name to a
// command that still exists; inputs removed on purpose stay refused.

const EARLIER_CHECK_VERB: &str = crate::runtime::legacy_names::DEPRECATED_VERB_SOURCE_CHECK;

fn check_arguments(command: &str, flag: &str) -> Vec<String> {
    let mut arguments = [
        "tesatlas-hub",
        command,
        "--source",
        "postgresql://reader@source.example/sourcedb",
        "--car-id",
        "42",
        "--postgres-password-file",
        "fictional-password",
        flag,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    if command == "migrate" {
        arguments.extend(["--history-only".to_owned(), "--online-snapshot".to_owned()]);
    }
    arguments
}

#[test]
fn source_commands_refuse_car_ids_outside_postgres_smallint() {
    for command in ["source-check", "migrate"] {
        let mut arguments = check_arguments(command, "--accept-schema-evidence");
        let car_id = arguments.iter().position(|arg| arg == "--car-id").unwrap() + 1;
        for valid in ["1", "32767"] {
            arguments[car_id] = valid.to_owned();
            assert!(Cli::try_parse_from(&arguments).is_ok(), "{command}: {valid}");
        }
        for invalid in ["32768", "0", "-1", "not-a-number"] {
            arguments[car_id] = invalid.to_owned();
            assert!(Cli::try_parse_from(&arguments).is_err(), "{command}: {invalid}");
        }
    }
}

#[test]
fn deprecated_alias_schema_flag_works_warns_and_is_hidden() {
    for command in ["source-check", "migrate"] {
        let mut warnings = Vec::new();
        let cli = Cli::parse_with_schema_warning(
            check_arguments(command, "--acknowledge-v4-2-compatible-schema"),
            &mut warnings,
        )
        .expect("the earlier flag parses");
        match cli.command {
            Command::SourceCheck { accept_schema_evidence, .. }
            | Command::Migrate { accept_schema_evidence, .. } => assert!(accept_schema_evidence),
            _ => panic!("wrong command"),
        }
        let warnings = String::from_utf8(warnings).expect("utf8");
        assert_eq!(warnings.lines().count(), 1, "{warnings}");
        assert!(warnings.contains("--accept-schema-evidence"));
        assert!(warnings.contains("a later version"));
        let help = Cli::try_parse_from(["tesatlas-hub", command, "--help"])
            .unwrap_err()
            .to_string();
        assert!(help.contains("--accept-schema-evidence"));
        assert!(!help.contains("--acknowledge-v4-2-compatible-schema"));
    }
}

#[test]
fn deprecated_alias_check_verb_works_warns_once_and_is_hidden() {
    let mut earlier = check_arguments("source-check", "--accept-schema-evidence");
    earlier[1] = EARLIER_CHECK_VERB.to_owned();
    for with_config in [false, true] {
        let mut arguments = earlier.clone();
        if with_config {
            arguments.splice(1..1, ["--config".to_owned(), "fictional.toml".to_owned()]);
        }
        let mut warnings = Vec::new();
        let cli = Cli::parse_with_schema_warning(arguments, &mut warnings)
            .expect("the earlier verb parses");
        assert!(matches!(
            cli.command,
            Command::SourceCheck { car_id: 42, accept_schema_evidence: true, .. }
        ));
        assert_eq!(cli.config.is_some(), with_config);
        let warnings = String::from_utf8(warnings).expect("utf8");
        assert_eq!(warnings.lines().count(), 1, "{warnings}");
        assert!(warnings.contains(EARLIER_CHECK_VERB));
        assert!(warnings.contains("use source-check"));
        assert!(warnings.contains("a later version"));
    }
    // The verb and the earlier flag each warn once, together.
    let mut warnings = Vec::new();
    Cli::parse_with_schema_warning(
        check_arguments(EARLIER_CHECK_VERB, "--acknowledge-v4-2-compatible-schema"),
        &mut warnings,
    )
    .expect("both earlier names parse");
    assert_eq!(String::from_utf8(warnings).expect("utf8").lines().count(), 2);
    // The current name stays silent, and the earlier verb is not in help.
    let mut warnings = Vec::new();
    Cli::parse_with_schema_warning(
        check_arguments("source-check", "--accept-schema-evidence"),
        &mut warnings,
    )
    .expect("current name parses");
    assert!(warnings.is_empty());
    let help = Cli::try_parse_from(["tesatlas-hub", "--help"]).unwrap_err().to_string();
    assert!(help.contains("source-check"));
    assert!(!help.contains(EARLIER_CHECK_VERB), "{help}");
    let command_help = Cli::try_parse_from(["tesatlas-hub", "source-check", "--help"])
        .unwrap_err()
        .to_string();
    assert!(!command_help.contains(EARLIER_CHECK_VERB), "{command_help}");
}

#[test]
fn deprecated_alias_check_verb_keeps_removed_inputs_refused() {
    for removed in ["--access-token-file", "--refresh-token-file", "--encryption-key-file"] {
        let mut arguments = check_arguments(EARLIER_CHECK_VERB, "--accept-schema-evidence");
        arguments.extend([removed.to_owned(), "fictional-token".to_owned()]);
        let error = Cli::try_parse_from(arguments).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument, "{removed}");
    }
}

/// Sends `request` on the control channel of `data` from `/usr/bin/nc`: a
/// process of the owner that is not the Hub. Returns what it was answered.
fn request_from_unrelated_process(data: &Path, request: &OperatorRequest) -> String {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut bytes = serde_json::to_vec(request).expect("request encodes");
    bytes.push(b'\n');
    let mut child = Command::new("/usr/bin/nc")
        .arg("-U")
        .arg(channel::socket_path(data))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("nc starts");
    {
        let mut input = child.stdin.take().expect("stdin");
        let _ = input.write_all(&bytes);
    }
    let output = child.wait_with_output().expect("nc exits");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn assert_peer_refused(answer: &str, what: &str) {
    let response: channel::OperatorResponse =
        serde_json::from_str(answer.trim()).unwrap_or_else(|_| panic!("{what}: {answer}"));
    assert!(!response.ok, "{what}: {answer}");
    assert_eq!(response.code.as_deref(), Some(channel::PEER_UNAUTHENTICATED), "{what}: {answer}");
    assert!(response.result.is_none(), "{what}: nothing answered but the refusal");
}

/// M02-T11 review: a process of the owner that is not the Hub sends the
/// purge with the confirmation. The service authenticates the requester
/// before it reads the request, refuses it and removes nothing; the Hub's
/// own operator command on the same channel still lists and purges.
#[test]
fn refused_credential_purge_an_unrelated_owner_process_is_refused_and_nothing_is_removed() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    for confirm in [true, false] {
        let answer = request_from_unrelated_process(
            &rig.data(),
            &OperatorRequest::PurgeRefusedCredentials { confirm },
        );
        assert_peer_refused(&answer, &format!("purge confirm={confirm}"));
    }
    assert!(rig.calls.all().is_empty(), "nothing listed or removed for the unrelated process");

    let purged = rig
        .forward(&OperatorRequest::PurgeRefusedCredentials { confirm: true })
        .expect("the Hub's own command still purges");
    assert_eq!(purged["mode"], "purge");
    assert_eq!(rig.calls.all(), vec!["refused_purge".to_owned()]);
}

/// M02-T11 review: the same for the window verbs. An unrelated process of
/// the owner cannot close, trust, open or read the window; the window stays
/// Open and the supervisor is not woken. The Hub's own command still closes
/// it.
#[test]
fn collection_window_operator_channel_refuses_an_unrelated_owner_process() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let _server = rig.serve();
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let grant = encoded(&rig.grant(600));
    rig.forward(&OperatorRequest::WindowOpen {
        grant: grant.clone(),
    })
    .expect("open");
    let kicks = *rig.kicks.lock().expect("kicks");

    for request in [
        OperatorRequest::WindowClose,
        OperatorRequest::WindowStatus,
        OperatorRequest::WindowTrust {
            document: encoded(&rig.trust),
        },
        OperatorRequest::WindowOpen { grant },
        OperatorRequest::Setup {
            material: material(),
            vehicle_id: Some(7),
            all_vehicles: false,
        },
    ] {
        let answer = request_from_unrelated_process(&rig.data(), &request);
        assert_peer_refused(&answer, &format!("{request:?}"));
    }
    assert_eq!(rig.controller.status().phase, crate::collection::window::Phase::Open, "still Open");
    assert_eq!(*rig.kicks.lock().expect("kicks"), kicks, "the supervisor was not woken");
    assert!(rig.calls.all().is_empty(), "the backend was never asked");

    let status = rig
        .forward(&OperatorRequest::WindowClose)
        .expect("the Hub's own command still closes the window");
    assert_eq!(status["phase"], "closing");
}

/// M04-T11: the window status answer carries the intake latency figures of the
/// window it names. The service and a collector thread share one recorder, as the
/// process-wide one is shared in production: samples a collector run records as its
/// window's cross the operator channel; refused opens change nothing; and the next
/// window shows empty figures until its own first sample.
#[test]
fn intake_latency_crosses_the_operator_channel_beside_its_own_window() {
    let rig = Rig::new();
    std::fs::create_dir_all(rig.data()).expect("data dir");
    let recorder: &'static crate::collection::collector::IntakeRecorder =
        Box::leak(Box::new(crate::collection::collector::IntakeRecorder::new()));
    let _server = ChannelServer::bind(
        &rig.data(),
        Arc::new(rig.service().with_intake_latency(recorder)),
    )
    .expect("channel bound");
    rig.forward(&OperatorRequest::WindowTrust {
        document: encoded(&rig.trust),
    })
    .expect("trust");
    let closed = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(closed["intakeLatency"]["count"], 0);
    assert!(closed["intakeLatency"]["seq"].is_null());

    let first_grant = rig.grant(3_600);
    let opened = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&first_grant),
        })
        .expect("open");
    assert_eq!(opened["intakeLatency"]["count"], 0);
    assert_eq!(opened["intakeLatency"]["seq"], opened["seq"]);
    let seq = opened["seq"].as_u64().expect("a grant sequence");

    // A collector run of the window records on its own thread.
    std::thread::spawn(move || {
        for millis in [12, 30, 400] {
            recorder.record(seq, std::time::Duration::from_millis(millis));
        }
    })
    .join()
    .expect("collector thread");
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    let latency = status["intakeLatency"].clone();
    assert_eq!(latency["count"], 3, "{status}");
    assert!(latency.get("dropped").is_none(), "no dropped count: {latency}");
    assert_eq!(latency["maxMs"].as_f64(), Some(400.0));
    assert_eq!(latency["overTargetCount"], 1);
    assert_eq!(latency["p95WithinTarget"], false);
    assert_eq!(latency["seq"], status["seq"]);
    assert_eq!(latency["grantDigest"], status["grantDigest"]);

    // Two refused opens in a row (the grant replayed) change nothing.
    for _ in 0..2 {
        let refused = rig
            .forward(&OperatorRequest::WindowOpen {
                grant: encoded(&first_grant),
            })
            .unwrap_err();
        assert_eq!(refused.code, "grant_consumed");
    }
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["intakeLatency"], latency, "{status}");

    // The next window shows empty figures beside its own identity until its first
    // sample; a late sample of the window before reaches nothing.
    rig.controller.close(CloseReason::OperatorClose);
    rig.controller.finish_close().expect("closed");
    rig.clock.advance(3_600 + 16 * 60);
    let next = rig
        .forward(&OperatorRequest::WindowOpen {
            grant: encoded(&rig.grant(3_600)),
        })
        .expect("next open");
    assert_ne!(next["seq"], opened["seq"]);
    assert_eq!(next["intakeLatency"]["count"], 0, "{next}");
    assert_eq!(next["intakeLatency"]["seq"], next["seq"]);
    assert!(next["intakeLatency"]["maxMs"].is_null());
    let next_seq = next["seq"].as_u64().expect("a grant sequence");
    recorder.record(next_seq, std::time::Duration::from_millis(8));
    recorder.record(seq, std::time::Duration::from_millis(900));
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["intakeLatency"]["count"], 1, "{status}");
    assert_eq!(status["intakeLatency"]["maxMs"].as_f64(), Some(8.0));
}

/// A collector that never reports stopping.
#[derive(Clone, Default)]
struct StuckSlot {
    stops: Arc<std::sync::atomic::AtomicUsize>,
}

impl crate::collection::window::supervisor::CollectorSlot for StuckSlot {
    fn start(&mut self, _deadline_wall: i64) -> Result<(), String> {
        Ok(())
    }
    fn is_running(&mut self) -> bool {
        true
    }
    fn stop(&mut self) {
        self.stops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn release_lease(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Issue #38: the window supervisor's stop waits for a collector that does
/// not stop only within the shared shutdown budget, not its own 30 seconds.
#[test]
fn design_later_38_supervisor_stop_gives_a_stuck_collector_only_the_budget() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let slot = StuckSlot::default();
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    let budget = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_millis(400),
        std::time::Duration::from_millis(100),
    );
    let began = std::time::Instant::now();
    let _ = thread.stop_within(&budget);
    let took = began.elapsed();
    assert!(took < std::time::Duration::from_secs(3), "the stop took {took:?}");
    assert!(slot.stops.load(std::sync::atomic::Ordering::SeqCst) >= 1, "the collector was told to stop");
}

/// Issue #38, review F2: once the service accepts a stop, sign-in and every
/// other operator verb are refused before the backend is asked, even inside
/// an Open window and while the listener is still draining; the window
/// status is still answered.
#[test]
fn design_later_38_an_accepted_stop_closes_operator_admission_at_once() {
    use crate::runtime::shutdown::ShutdownBudget;
    let rig = Rig::new();
    let budget = Arc::new(ShutdownBudget::system());
    let _server = ChannelServer::bind(
        &rig.data(),
        Arc::new(rig.service().with_shutdown(Arc::clone(&budget))),
    )
    .expect("channel bound");
    rig.forward(&OperatorRequest::WindowTrust { document: encoded(&rig.trust) })
        .expect("trust");
    rig.forward(&OperatorRequest::WindowOpen { grant: encoded(&rig.grant(3_600)) })
        .expect("open");

    budget.begin();
    let refused = rig
        .forward(&OperatorRequest::Setup { material: material(), vehicle_id: None, all_vehicles: true })
        .unwrap_err();
    assert_eq!(refused.code, super::authority::SERVICE_STOPPING);
    let refused = rig.forward(&OperatorRequest::WindowClose).unwrap_err();
    assert_eq!(refused.code, super::authority::SERVICE_STOPPING);
    assert!(rig.calls.all().is_empty(), "the backend was not asked");
    let status = rig.forward(&OperatorRequest::WindowStatus).expect("status");
    assert_eq!(status["phase"], "open");
}

/// Issue #38, review F2: accepting the stop stops the window supervisor's
/// collector at once, before the service waits for anything else.
#[test]
fn design_later_38_an_accepted_stop_stops_the_collector_at_once() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let slot = StuckSlot::default();
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    let budget = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_millis(400),
        std::time::Duration::from_millis(100),
    );
    budget.begin();
    (thread.stopper())(&budget);
    let asked = std::time::Instant::now();
    while slot.stops.load(std::sync::atomic::Ordering::SeqCst) == 0
        && asked.elapsed() < std::time::Duration::from_secs(3)
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        slot.stops.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the collector was told to stop when the stop was accepted"
    );
    let _ = thread.stop_within(&budget);
}

/// Issue #38, review F3: a collector still running when its stop allowance
/// ends is reported as the `collector` stage timing out, not as stopped,
/// even when the supervisor thread itself is joined in time.
#[test]
fn design_later_38_a_collector_running_past_its_allowance_is_not_reported_stopped() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let slot = StuckSlot::default();
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    // The stop accepted with 300 ms left gives the collector that allowance.
    let accepted = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_millis(300),
        std::time::Duration::ZERO,
    );
    (thread.stopper())(&accepted);
    // The service's join then has ample budget to see the supervisor end.
    let joining = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_secs(10),
        std::time::Duration::ZERO,
    );
    let timeout = thread
        .stop_within(&joining)
        .expect_err("a collector still running is not a clean stop");
    assert_eq!(timeout.stage, "collector");
}

// ---------------------------------------------------------------------------
// M03-T28b: the broken-drive repair controls over the owner channel
// ---------------------------------------------------------------------------

mod repair_controls {
    use std::{
        collections::VecDeque,
        sync::{Condvar, atomic::{AtomicUsize, Ordering}},
    };

    use uuid::Uuid;

    use super::*;
    use crate::{
        db::HubStore,
        import::broken_drive_repair::{
            BrokenDriveCandidateIndex, GapEvidence, RepairInput, RepairSample, StateCoverage,
        },
        runtime::{
            broken_drive_repair::{Deferral, PassHold, PassOutcome, RepairPassRunner, RepairScheduler},
            operator::authority::RepairControl,
        },
    };

    const NOW: i64 = 1_800_000_000_000;
    const MINUTE: i64 = 60_000;

    /// A scripted pass (a completed pass with no change when it is empty)
    /// that can be held until the test lets it go.
    #[derive(Default)]
    struct Runner {
        outcomes: Mutex<VecDeque<PassOutcome>>,
        passes: AtomicUsize,
        held: Mutex<bool>,
        released: Condvar,
    }

    impl Runner {
        fn with(outcomes: Vec<PassOutcome>) -> Arc<Self> {
            Arc::new(Self { outcomes: Mutex::new(outcomes.into()), ..Self::default() })
        }

        fn hold(&self) {
            *self.held.lock().unwrap() = true;
        }

        fn release(&self) {
            *self.held.lock().unwrap() = false;
            self.released.notify_all();
        }

        fn passes(&self) -> usize {
            self.passes.load(Ordering::SeqCst)
        }
    }

    impl RepairPassRunner for Runner {
        fn run_pass(&self, _: Uuid, _: i64) -> PassOutcome {
            self.passes.fetch_add(1, Ordering::SeqCst);
            let mut held = self.held.lock().unwrap();
            while *held {
                held = self.released.wait(held).unwrap();
            }
            drop(held);
            self.outcomes.lock().unwrap().pop_front().unwrap_or(PassOutcome::NoChange)
        }
    }

    /// The backend of a service whose only job is the repair controls.
    struct Backend(Option<RepairControl>);

    impl ServiceBackend for Backend {
        fn setup(&self, _: SetupMaterial, _: Option<i64>, _: bool) -> Result<Value, OperatorRefusal> {
            unreachable!("not used")
        }
        fn recovery_export(&self, _: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
            unreachable!("not used")
        }
        fn recovery_restore(&self, _: &Path, _: &Path) -> Result<Value, OperatorRefusal> {
            unreachable!("not used")
        }
        fn refused_credentials(&self) -> Result<Value, OperatorRefusal> {
            unreachable!("not used")
        }
        fn purge_refused_credentials(&self) -> Result<Value, OperatorRefusal> {
            unreachable!("not used")
        }
        fn repair_control(&self) -> Option<&RepairControl> {
            self.0.as_ref()
        }
    }

    struct Repairs {
        rig: Rig,
        store: HubStore,
        vehicle: Uuid,
        runner: Arc<Runner>,
        service: Arc<OperatorService>,
        /// What the scheduler's deferral check answers.
        hold: Arc<Mutex<Option<Deferral>>>,
        scheduler: Arc<RepairScheduler>,
    }

    fn sample(position_id: i64, date_ms: i64, odometer: f64) -> RepairSample {
        RepairSample { position_id, date_ms, odometer_km: Some(odometer), speed_kmh: Some(40), row_digest: [0; 32] }
    }

    /// One broken drive: two single-sample fragments and their destination.
    fn index_rows() -> Vec<RepairInput> {
        let fragment = |id: i64, at: i64, odometer: f64, unclosed_before: bool| {
            let s = sample(id * 100, at, odometer);
            RepairInput {
                drive_id: id,
                car_id: 1,
                start_ms: at,
                end_ms: None,
                distance_km: None,
                duration_min: None,
                sample_count: 1,
                first_sample: Some(s),
                last_sample: Some(s),
                previous_drive_id: Some(id - 1),
                previous_unclosed: unclosed_before,
                next_drive_id: Some(id + 1),
                gap_to_next: Some(GapEvidence {
                    charge_between: false,
                    state: StateCoverage::Online,
                }),
            }
        };
        vec![
            fragment(20, NOW, 100.0, false),
            fragment(21, NOW + MINUTE, 100.5, true),
            RepairInput {
                drive_id: 22,
                car_id: 1,
                start_ms: NOW + 2 * MINUTE,
                end_ms: Some(NOW + 32 * MINUTE),
                distance_km: Some(9.0),
                duration_min: Some(30),
                sample_count: 2,
                first_sample: Some(sample(2200, NOW + 2 * MINUTE, 101.5)),
                last_sample: Some(sample(2201, NOW + 32 * MINUTE, 110.5)),
                previous_drive_id: Some(21),
                previous_unclosed: true,
                next_drive_id: None,
                gap_to_next: None,
            },
        ]
    }

    fn repairs_with(outcomes: Vec<PassOutcome>, indexed: bool, with_control: bool) -> Repairs {
        let rig = Rig::new();
        let store = HubStore::initialize(rig.data()).expect("store");
        let vehicle = Uuid::new_v4();
        if indexed {
            store
                .record_drive_repair_index(
                    Uuid::new_v4(),
                    vehicle,
                    &BrokenDriveCandidateIndex { car_id: 1, rows: index_rows() },
                    NOW,
                )
                .expect("index");
        }
        let runner = Runner::with(outcomes);
        let hold: Arc<Mutex<Option<Deferral>>> = Arc::default();
        let held = Arc::clone(&hold);
        let scheduler = Arc::new(
            RepairScheduler::start(
                store.clone(),
                runner.clone(),
                Box::new(move || match *held.lock().unwrap() {
                    Some(reason) => Err(reason),
                    None => Ok(PassHold::none()),
                }),
            )
            .expect("scheduler"),
        );
        scheduler.mark_ready();
        let control = with_control
            .then(|| RepairControl::new(store.clone(), rig.data(), Arc::clone(&scheduler)));
        let kicks = rig.kicks.clone();
        let service = Arc::new(OperatorService::new(
            rig.controller.clone(),
            Arc::new(Backend(control)),
            Box::new(move || *kicks.lock().expect("kicks") += 1),
            TrustAuthority::new(rig.pin.clone(), HOST, rig.owner.clone()),
            &rig.data(),
        ));
        Repairs { rig, store, vehicle, runner, service, hold, scheduler }
    }

    fn repairs() -> Repairs {
        repairs_with(Vec::new(), true, true)
    }

    impl Repairs {
        fn call(&self, request: OperatorRequest) -> Result<Value, OperatorRefusal> {
            use super::channel::OperatorHandler as _;
            self.service.handle(request)
        }

        fn id(&self) -> String {
            self.vehicle.to_string()
        }

        /// Polls a job until it is no longer running.
        fn settled(&self, receipt: &Value) -> Value {
            let job_id = receipt["jobId"].as_u64().expect("job id");
            for _ in 0..400 {
                let read = self.call(OperatorRequest::RepairJob { job_id }).expect("job");
                if read["state"] != "running" && read["state"] != "accepted" {
                    return read;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            panic!("job {job_id} did not settle");
        }

        fn summary(&self) -> Value {
            self.call(OperatorRequest::RepairSummary { vehicle: self.id() }).expect("summary")
        }

    }

    fn code(result: Result<Value, OperatorRefusal>) -> String {
        result.expect_err("refused").code
    }

    #[test]
    fn broken_drive_repair_control_check_now_returns_a_receipt_at_once_and_the_job_settles() {
        let repairs = repairs_with(vec![PassOutcome::Repaired { batch_id: 7 }], true, true);
        repairs.runner.hold();
        let receipt = repairs
            .call(OperatorRequest::RepairCheckNow { vehicle: repairs.id() })
            .expect("receipt");
        assert_eq!(receipt["state"], "accepted");
        assert_eq!(receipt["kind"], "check_now");
        let running = repairs
            .call(OperatorRequest::RepairJob { job_id: receipt["jobId"].as_u64().unwrap() })
            .expect("poll");
        assert_eq!(running["state"], "running", "the pass is held, so the receipt came first");
        repairs.runner.release();
        let done = repairs.settled(&receipt);
        assert_eq!(done["state"], "done");
        assert_eq!(done["result"], json!({ "outcome": "repaired", "batchId": 7 }));
        assert_eq!(repairs.runner.passes(), 1);
    }

    #[test]
    fn broken_drive_repair_control_check_now_runs_with_automatic_off_and_a_failed_pass_is_a_failed_job() {
        let repairs = repairs_with(vec![PassOutcome::Failed("apply".into())], true, true);
        assert_eq!(repairs.summary()["schedule"]["automatic"], false);
        let receipt = repairs
            .call(OperatorRequest::RepairCheckNow { vehicle: repairs.id() })
            .expect("receipt");
        let done = repairs.settled(&receipt);
        assert_eq!(done["state"], "failed");
        assert_eq!(done["code"], "apply");
        assert_eq!(repairs.summary()["schedule"]["failures"], 1);
    }

    #[test]
    fn broken_drive_repair_control_one_running_job_per_vehicle() {
        let repairs = repairs();
        repairs.runner.hold();
        let first = repairs
            .call(OperatorRequest::RepairCheckNow { vehicle: repairs.id() })
            .expect("first");
        assert_eq!(code(repairs.call(OperatorRequest::RepairCheckNow { vehicle: repairs.id() })), "repair_busy");
        assert_eq!(
            code(repairs.call(OperatorRequest::RepairUndo {
                vehicle: repairs.id(),
                batch_id: Some(1),
                component_key: None,
            })),
            "repair_busy"
        );
        assert_eq!(repairs.summary()["runningJob"]["jobId"], first["jobId"]);
        repairs.runner.release();
        assert_eq!(repairs.settled(&first)["state"], "done");
        assert_eq!(repairs.summary()["runningJob"], Value::Null);
    }

    #[test]
    fn broken_drive_repair_control_automatic_on_needs_the_phone_witness_and_off_does_not() {
        let repairs = repairs();
        let on = |phone_witness| OperatorRequest::RepairAutomatic {
            vehicle: repairs.id(),
            on: true,
            phone_witness,
        };
        assert_eq!(code(repairs.call(on(false))), "repair_witness_required");
        assert_eq!(repairs.summary()["schedule"]["automatic"], false);
        assert_eq!(repairs.store.drive_repair_schedule(repairs.vehicle).unwrap().automatic, None);

        let receipt = repairs.call(on(true)).expect("on");
        assert_eq!(receipt["state"], "done");
        assert_eq!(receipt["result"]["automatic"], true);
        assert_eq!(repairs.store.drive_repair_schedule(repairs.vehicle).unwrap().automatic, Some(true));

        let off = repairs
            .call(OperatorRequest::RepairAutomatic {
                vehicle: repairs.id(),
                on: false,
                phone_witness: false,
            })
            .expect("off needs no witness");
        assert_eq!(off["result"]["automatic"], false);
        assert_eq!(repairs.store.drive_repair_schedule(repairs.vehicle).unwrap().automatic, Some(false));
    }

    #[test]
    fn broken_drive_repair_control_protect_and_attest_change_the_summary() {
        let repairs = repairs();
        assert_eq!(repairs.summary()["indexRows"], 3);
        let attested = repairs
            .call(OperatorRequest::RepairAttest { vehicle: repairs.id(), drive_ids: vec![20, 21, 22, 99] })
            .expect("attest");
        assert_eq!(attested["state"], "done");
        assert_eq!(attested["result"], json!({ "requested": 4, "attested": 3 }));
        assert_eq!(repairs.summary()["attested"], 3);
        let protect = |entity: &str, id: i64| OperatorRequest::RepairProtect {
            vehicle: repairs.id(),
            entity: entity.to_owned(),
            entity_id: id,
            reason: "edit".to_owned(),
        };
        repairs.call(protect("drive", 20)).expect("protect");
        let summary = repairs.summary();
        assert_eq!(summary["protected"], 1);
        assert_eq!(summary["attested"], 2, "a protection replaces the attestation");
        assert_eq!(summary["indexed"], true);
    }

    #[test]
    fn broken_drive_repair_control_retry_lifts_a_suppression_once() {
        let repairs = repairs();
        // An undone component is suppressed; seed that record directly, since
        // planning one needs a published import this rig does not hold.
        let key = [7_u8; 32];
        rusqlite::Connection::open(repairs.store.database_path())
            .expect("catalogue")
            .execute(
                "INSERT INTO drive_repair_suppressions(vehicle_id, component_key, reason, recorded_at_ms)
                 VALUES (?1, ?2, 'undo', ?3)",
                rusqlite::params![repairs.id(), key.as_slice(), NOW],
            )
            .expect("suppression");
        assert_eq!(repairs.summary()["suppressed"], 1);
        let retry = repairs
            .call(OperatorRequest::RepairRetry { vehicle: repairs.id(), component_key: hex::encode(key) })
            .expect("retry");
        assert_eq!(retry["state"], "done");
        assert_eq!(retry["result"], json!({ "lifted": true }));
        assert_eq!(repairs.summary()["suppressed"], 0);
        let again = repairs
            .call(OperatorRequest::RepairRetry { vehicle: repairs.id(), component_key: hex::encode(key) })
            .expect("retry again");
        assert_eq!(again["result"], json!({ "lifted": false }));
    }

    #[test]
    fn broken_drive_repair_control_undo_is_a_job_that_needs_the_cursor_key() {
        let repairs = repairs();
        let receipt = repairs
            .call(OperatorRequest::RepairUndo {
                vehicle: repairs.id(),
                batch_id: Some(1),
                component_key: None,
            })
            .expect("receipt");
        assert_eq!(receipt["state"], "accepted");
        assert_eq!(receipt["kind"], "undo");
        let done = repairs.settled(&receipt);
        assert_eq!(done["state"], "failed");
        assert_eq!(done["code"], "repair_not_ready", "no cursor key in this data directory");
    }

    /// REV-CX-T28 P2 "Undo bypasses the live window, hand-back and backup
    /// deferrals": Undo runs under the scheduler's slot and deferrals, and a
    /// stopped scheduler starts no Undo.
    #[test]
    fn t28_fix_undo_honours_the_window_hand_back_and_backup_deferrals() {
        let repairs = repairs();
        let undo = || OperatorRequest::RepairUndo { vehicle: repairs.id(), batch_id: Some(1), component_key: None };
        for (reason, expected) in [
            (Deferral::Window, "repair_deferred_window"),
            (Deferral::HandBack, "repair_deferred_hand_back"),
            (Deferral::Backup, "repair_deferred_backup"),
            (Deferral::Import, "repair_deferred_import"),
        ] {
            *repairs.hold.lock().unwrap() = Some(reason);
            let done = repairs.settled(&repairs.call(undo()).expect("receipt"));
            assert_eq!(done["state"], "failed", "{reason:?}");
            assert_eq!(done["code"], expected, "{reason:?}");
        }
        *repairs.hold.lock().unwrap() = None;
        let done = repairs.settled(&repairs.call(undo()).expect("receipt"));
        assert_eq!(done["code"], "repair_not_ready", "admitted, it reaches the cursor key check");
        repairs.scheduler.request_stop();
        assert_eq!(code(repairs.call(undo())), "repair_unavailable");
        assert_eq!(repairs.runner.passes(), 0);
    }

    #[test]
    fn broken_drive_repair_control_refuses_bad_input_before_any_work() {
        let repairs = repairs();
        let id = repairs.id();
        let other = Uuid::new_v4().to_string();
        let bad = [
            (OperatorRequest::RepairCheckNow { vehicle: "not-a-uuid".into() }, "repair_invalid"),
            (OperatorRequest::RepairCheckNow { vehicle: other.clone() }, "repair_unknown_vehicle"),
            (
                OperatorRequest::RepairProtect {
                    vehicle: id.clone(),
                    entity: "charge".into(),
                    entity_id: 1,
                    reason: "edit".into(),
                },
                "repair_invalid",
            ),
            (
                OperatorRequest::RepairProtect {
                    vehicle: id.clone(),
                    entity: "drive".into(),
                    entity_id: 1,
                    reason: "because".into(),
                },
                "repair_invalid",
            ),
            (OperatorRequest::RepairAttest { vehicle: id.clone(), drive_ids: Vec::new() }, "repair_invalid"),
            (
                OperatorRequest::RepairAttest { vehicle: id.clone(), drive_ids: (0..513).collect() },
                "repair_invalid",
            ),
            (
                OperatorRequest::RepairUndo { vehicle: id.clone(), batch_id: None, component_key: None },
                "repair_invalid",
            ),
            (
                OperatorRequest::RepairUndo {
                    vehicle: id.clone(),
                    batch_id: Some(1),
                    component_key: Some("00".repeat(32)),
                },
                "repair_invalid",
            ),
            (
                OperatorRequest::RepairUndo {
                    vehicle: id.clone(),
                    batch_id: None,
                    component_key: Some("zz".into()),
                },
                "repair_invalid",
            ),
            (
                OperatorRequest::RepairRetry { vehicle: id.clone(), component_key: "00".repeat(31) },
                "repair_invalid",
            ),
            (OperatorRequest::RepairJob { job_id: 4242 }, "repair_job_unknown"),
        ];
        for (request, expected) in bad {
            let described = format!("{request:?}");
            assert_eq!(code(repairs.call(request)), expected, "{described}");
        }
        assert_eq!(repairs.runner.passes(), 0);
        // The summary of an unindexed vehicle is zeros, not an error.
        let empty = repairs.call(OperatorRequest::RepairSummary { vehicle: other }).expect("summary");
        assert_eq!(empty["indexed"], false);
        assert_eq!(empty["indexRows"], 0);
    }

    #[test]
    fn broken_drive_repair_control_follows_the_service_gates() {
        use crate::runtime::operator::authority::{CREDENTIAL_RECOVERY_PENDING, RecoveryGate, SERVICE_STOPPING};
        let repairs = repairs();
        let request = || OperatorRequest::RepairSummary { vehicle: repairs.id() };
        // A recovery-pending service answers no repair verb.
        let gate = RecoveryGate::new(true);
        let pending = OperatorService::new(
            repairs.rig.controller.clone(),
            Arc::new(Backend(None)),
            Box::new(|| {}),
            TrustAuthority::new(repairs.rig.pin.clone(), HOST, repairs.rig.owner.clone()),
            &repairs.rig.data(),
        )
        .with_recovery(gate);
        use super::channel::OperatorHandler as _;
        assert_eq!(code(pending.handle(request())), CREDENTIAL_RECOVERY_PENDING);
        // A stopping service answers none either.
        let budget = Arc::new(crate::runtime::shutdown::ShutdownBudget::system());
        budget.begin();
        let stopping = OperatorService::new(
            repairs.rig.controller.clone(),
            Arc::new(Backend(None)),
            Box::new(|| {}),
            TrustAuthority::new(repairs.rig.pin.clone(), HOST, repairs.rig.owner.clone()),
            &repairs.rig.data(),
        )
        .with_shutdown(budget);
        assert_eq!(code(stopping.handle(request())), SERVICE_STOPPING);
        // A service without the repair schedule says so.
        let bare = repairs_with(Vec::new(), true, false);
        assert_eq!(code(bare.call(request())), "repair_unavailable");
        // No window is needed: the window is closed here and the verbs work.
        assert_eq!(repairs.summary()["indexed"], true);
    }

    #[test]
    fn broken_drive_repair_control_verbs_cross_the_owner_channel() {
        let repairs = repairs();
        let server = ChannelServer::bind(&repairs.rig.data(), repairs.service.clone()).expect("channel bound");
        let forward = |request: &OperatorRequest| channel::forward(&repairs.rig.data(), request, TIMEOUT);
        let summary = forward(&OperatorRequest::RepairSummary { vehicle: repairs.id() }).expect("summary");
        assert_eq!(summary["indexRows"], 3);
        let receipt = forward(&OperatorRequest::RepairCheckNow { vehicle: repairs.id() }).expect("receipt");
        assert_eq!(receipt["state"], "accepted");
        let done = repairs.settled(&receipt);
        assert_eq!(done["result"]["outcome"], "no_change");
        let refused = forward(&OperatorRequest::RepairAutomatic {
            vehicle: repairs.id(),
            on: true,
            phone_witness: false,
        })
        .expect_err("witness");
        assert_eq!(refused.code, "repair_witness_required");
        drop(server);
    }

    #[test]
    fn broken_drive_repair_control_wire_and_cli_shapes() {
        // Unknown fields are refused and the witness defaults to absent.
        let parsed: OperatorRequest =
            serde_json::from_str(r#"{"op":"repair_automatic","vehicle":"v","on":true}"#).expect("parse");
        assert_eq!(
            parsed,
            OperatorRequest::RepairAutomatic { vehicle: "v".into(), on: true, phone_witness: false }
        );
        assert!(serde_json::from_str::<OperatorRequest>(r#"{"op":"repair_job","job_id":1,"x":1}"#).is_err());
        // The CLI verbs route to the service.
        let cli = Cli::try_parse_from([
            "tesatlas-hub", "repairs", "undo", "--vehicle", "v", "--batch-id", "3",
        ])
        .expect("undo parses");
        assert_eq!(admission::route(&cli.command), Route::Service);
        assert!(Cli::try_parse_from(["tesatlas-hub", "repairs", "undo", "--vehicle", "v"]).is_err());
        assert!(
            Cli::try_parse_from([
                "tesatlas-hub", "repairs", "undo", "--vehicle", "v", "--batch-id", "3",
                "--component-key", "00",
            ])
            .is_err()
        );
        let on = Cli::try_parse_from([
            "tesatlas-hub", "repairs", "automatic", "--vehicle", "v", "on", "--phone-witness",
        ])
        .expect("automatic parses");
        assert_eq!(admission::route(&on.command), Route::Service);
    }
}

/// A collector that records the deadline its stop was given and stops at once.
#[derive(Clone, Default)]
struct DeadlineSlot {
    deadline: Arc<Mutex<Option<std::time::Instant>>>,
}

impl crate::collection::window::supervisor::CollectorSlot for DeadlineSlot {
    fn start(&mut self, _deadline_wall: i64) -> Result<(), String> {
        Ok(())
    }
    fn is_running(&mut self) -> bool {
        self.deadline.lock().expect("deadline").is_none()
    }
    fn stop(&mut self) {}
    fn stop_within(&mut self, deadline: std::time::Instant) {
        *self.deadline.lock().expect("deadline") = Some(deadline);
    }
    fn release_lease(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// STREAM-STOP-ALLOWANCE: a stopping service's allowance shorter than the
/// collector's default reaches the collector's stop as the deadline the
/// supervisor waits to, not the default 30 seconds.
#[test]
fn stream_stop_allowance_the_supervisor_gives_the_collector_its_deadline() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let slot = DeadlineSlot::default();
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    let budget = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_secs(12),
        std::time::Duration::from_secs(2),
    );
    let began = std::time::Instant::now();
    thread.stop_within(&budget).expect("the collector stopped");
    let deadline = slot.deadline.lock().expect("deadline").expect("a deadline reached the stop");
    let granted = deadline.saturating_duration_since(began);
    assert!(granted <= budget.remaining_for_work() + std::time::Duration::from_secs(1), "{granted:?}");
    assert!(granted < super::authority::COLLECTOR_STOP_WAIT, "{granted:?}");
    assert!(granted > std::time::Duration::from_secs(5), "{granted:?}");
}

/// A collector slot whose next `is_running` after `arm` holds the supervisor's
/// step for `hold`, as a slow step would; it records the stop's deadline.
#[derive(Clone)]
struct SlowStepSlot {
    deadline: Arc<Mutex<Option<std::time::Instant>>>,
    arm: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<Mutex<std::sync::mpsc::Sender<()>>>,
    hold: std::time::Duration,
}

impl crate::collection::window::supervisor::CollectorSlot for SlowStepSlot {
    fn start(&mut self, _deadline_wall: i64) -> Result<(), String> {
        Ok(())
    }
    fn is_running(&mut self) -> bool {
        if self.arm.swap(false, std::sync::atomic::Ordering::SeqCst) {
            let _ = self.entered.lock().expect("entered").send(());
            std::thread::sleep(self.hold);
        }
        self.deadline.lock().expect("deadline").is_none()
    }
    fn stop(&mut self) {}
    fn stop_within(&mut self, deadline: std::time::Instant) {
        *self.deadline.lock().expect("deadline") = Some(deadline);
    }
    fn release_lease(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// REV-CX-SSA finding 2: a stop asked for while the supervisor is in a slow step
/// gives the collector what was left of the service's budget when it was asked
/// for, not that much again from the moment the supervisor reads the request.
#[test]
fn stream_stop_allowance_rev_cx_ssa_2_a_slow_step_does_not_restart_the_collector_allowance() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let slot = SlowStepSlot {
        deadline: Arc::new(Mutex::new(None)),
        arm: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        entered: Arc::new(Mutex::new(entered_tx)),
        hold: std::time::Duration::from_millis(1_500),
    };
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    slot.arm.store(true, std::sync::atomic::Ordering::SeqCst);
    (thread.kicker())();
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the supervisor's step reached the slot");
    let budget = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_secs(12),
        std::time::Duration::from_secs(2),
    );
    let asked = std::time::Instant::now();
    let allowance = budget.remaining_for_work().min(super::authority::COLLECTOR_STOP_WAIT);
    thread.stop_within(&budget).expect("the collector stopped");
    let deadline = slot.deadline.lock().expect("deadline").expect("a deadline reached the stop");
    let granted = deadline.saturating_duration_since(asked);
    assert!(
        granted <= allowance + std::time::Duration::from_millis(500),
        "the slow step was added to the allowance: {granted:?} of {allowance:?}"
    );
}

/// REV-CX-RECHECK-SSA finding 1: a stop the service asks for while the supervisor is
/// in a step reaches the collector's slot with its deadline at once. The step may close
/// the window and signal a plain stop, or the collector may see the window end by
/// itself, so a deadline that waited for the step could reach the collector after its
/// stop had begun writing under the default allowance.
#[test]
fn ssa_r2_1_a_stop_asked_for_during_a_step_reaches_the_slot_before_the_step_ends() {
    use crate::runtime::shutdown::{ShutdownBudget, SystemMonotonic};
    let rig = Rig::new();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let slot = SlowStepSlot {
        deadline: Arc::new(Mutex::new(None)),
        arm: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        entered: Arc::new(Mutex::new(entered_tx)),
        hold: std::time::Duration::from_millis(1_500),
    };
    let thread = super::authority::SupervisorThread::spawn(Arc::clone(&rig.controller), slot.clone())
        .expect("supervisor thread");
    slot.arm.store(true, std::sync::atomic::Ordering::SeqCst);
    (thread.kicker())();
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the supervisor's step reached the slot");
    let budget = ShutdownBudget::new(
        Arc::new(SystemMonotonic::default()),
        std::time::Duration::from_secs(12),
        std::time::Duration::from_secs(2),
    );
    let asked = std::time::Instant::now();
    let allowance = budget.remaining_for_work().min(super::authority::COLLECTOR_STOP_WAIT);
    (thread.stopper())(&budget);
    // The step is still held: only a deadline given at the request is there now.
    let given = *slot.deadline.lock().expect("deadline");
    let deadline = given.expect("the deadline waited for the supervisor's step");
    let granted = deadline.saturating_duration_since(asked);
    assert!(
        granted <= allowance + std::time::Duration::from_millis(500),
        "{granted:?} of {allowance:?}"
    );
    thread.stop_within(&budget).expect("the collector stopped");
}
