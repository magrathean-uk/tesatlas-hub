// SPDX-License-Identifier: AGPL-3.0-only

//! M03-T28b: the broken-drive repair controls on the real service binary.
//!
//! A data folder holds an import of two broken drives as the importer leaves
//! it: the vehicle's materialised rows, its raw stage kept, a served 1.4
//! head and the candidate index (the library's `journey-doubles` fixture,
//! the same builders the importer's apply tests use). The binary serves it
//! idle; every control then crosses the authenticated owner channel exactly
//! as the App will send it: summary, attest, protect, automatic on and off
//! (with and without the phone witness), a refused bad input, Check now that
//! repairs the unprotected drive and serves a new head, and Undo that
//! restores it and serves the head again. A deferred or refused Check now or
//! Undo fails the test. The service is stopped and started again between
//! them and the choices and the repair are still there. Nothing reaches the
//! network.
#![cfg(target_os = "macos")]

use std::{
    fs,
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::TempDir;
use tesatlas_hub::import::source_db::importer::repair_journey_fixture as journey;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_tesatlas-hub");
const SERVE_START_LIMIT: Duration = Duration::from_secs(90);
const JOB_LIMIT: Duration = Duration::from_secs(60);

/// A temporary directory short enough for the operator socket path.
fn short_tempdir() -> TempDir {
    let base = std::env::temp_dir();
    if base.as_os_str().len() <= 50 {
        TempDir::new_in(base).expect("temp dir")
    } else {
        TempDir::new_in("/tmp").expect("temp dir")
    }
}

struct Hub {
    tmp: TempDir,
    accepted: Arc<AtomicUsize>,
    proxy: String,
    vehicle: Uuid,
}

impl Hub {
    fn new() -> Self {
        let tmp = short_tempdir();
        for dir in ["home", "data", "anchor", "stage"] {
            fs::create_dir(tmp.path().join(dir)).expect("dir");
            fs::set_permissions(tmp.path().join(dir), fs::Permissions::from_mode(0o700))
                .expect("private dir");
        }
        // Every proxy variable points at a counting endpoint that never
        // answers, so any connection the binary tries is counted.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let proxy = format!("http://{}", listener.local_addr().expect("address"));
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming().flatten() {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(stream);
            }
        });
        let port = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("free port");
            listener.local_addr().expect("address").port()
        };
        let vehicle = journey::seed_repair_journey(&tmp.path().join("data"), &tmp.path().join("stage"));
        let hub = Self { tmp, accepted, proxy, vehicle };
        let doubles = serde_json::json!({ "anchorDir": hub.path("anchor") });
        fs::write(hub.path("doubles.json"), doubles.to_string()).expect("doubles");
        let config = format!(
            "data_dir = {:?}\nbind = \"127.0.0.1:{port}\"\n\n[geocoder]\nenabled = false\n\n[terrain]\nenabled = false\n",
            hub.path("data"),
        );
        fs::write(hub.path("config.toml"), config).expect("config");
        fs::set_permissions(hub.path("config.toml"), fs::Permissions::from_mode(0o600))
            .expect("config mode");
        hub
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    /// The vehicle's current head and whether it is served, read from the
    /// data folder while the service is stopped.
    fn head(&self) -> (Uuid, bool) {
        journey::current_head(&self.path("data"), self.vehicle).expect("a physical head")
    }

    fn command(&self, arguments: &[&str]) -> Command {
        let mut command = Command::new(BIN);
        command
            .arg("--config")
            .arg(self.path("config.toml"))
            .args(arguments)
            .env_clear()
            .env("HOME", self.path("home"))
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("TMPDIR", self.tmp.path())
            .env("RUST_LOG", "info")
            .env("TESATLAS_HUB_JOURNEY_DOUBLES", self.path("doubles.json"));
        for variable in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"] {
            command.env(variable, &self.proxy);
        }
        command
    }

    fn run(&self, arguments: &[&str]) -> Output {
        self.command(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("binary runs")
    }

    fn spawn_serve(&self, label: &str) -> Child {
        let log = |name: &str| fs::File::create(self.path(&format!("{label}.{name}"))).expect("log");
        let mut serve = self
            .command(&["serve"])
            .stdin(Stdio::null())
            .stdout(log("out"))
            .stderr(log("err"))
            .spawn()
            .expect("serve starts");
        let started = Instant::now();
        loop {
            if let Some(status) = serve.try_wait().expect("serve state") {
                let log = fs::read_to_string(self.path(&format!("{label}.err"))).unwrap_or_default();
                panic!("{label}: serve exited {status} before answering: {log}");
            }
            if self.run(&["window", "status"]).status.success() {
                return serve;
            }
            assert!(started.elapsed() < SERVE_START_LIMIT, "{label}: no answer");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Stops `serve` with SIGTERM, as launchd does, and waits for it.
    fn stop(&self, mut serve: Child) {
        let _ = Command::new("/bin/kill").args(["-TERM", &serve.id().to_string()]).status();
        let started = Instant::now();
        while serve.try_wait().expect("serve state").is_none() {
            if started.elapsed() > SERVE_START_LIMIT {
                let _ = serve.kill();
                let _ = serve.wait();
                panic!("serve did not stop on SIGTERM");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// One `repairs` verb for the vehicle: its JSON reply, or the refusal
    /// code the service answered.
    fn repairs(&self, verb: &str, extra: &[&str]) -> Result<Value, String> {
        let vehicle = self.vehicle.to_string();
        let mut arguments = vec!["repairs", verb, "--vehicle", vehicle.as_str()];
        arguments.extend_from_slice(extra);
        let output = self.run(&arguments);
        if output.status.success() {
            serde_json::from_slice(&output.stdout).map_err(|error| format!("bad reply: {error}"))
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }

    /// Polls a receipt's job until it is no longer running.
    fn settled(&self, receipt: &Value) -> Value {
        let id = receipt["jobId"].as_u64().expect("job id").to_string();
        let started = Instant::now();
        loop {
            let output = self.run(&["repairs", "job", "--job-id", &id]);
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            let read: Value = serde_json::from_slice(&output.stdout).expect("job JSON");
            if read["state"] != "running" && read["state"] != "accepted" {
                return read;
            }
            assert!(started.elapsed() < JOB_LIMIT, "job {id} did not settle");
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// The settled job's result, which must be done: a deferred or refused
/// repair fails here with its code.
fn done(settled: &Value) -> &Value {
    assert_eq!(settled["state"], "done", "{settled}");
    &settled["result"]
}

#[test]
fn broken_drive_repair_journey_repairs_and_undoes_a_served_head_across_restarts() {
    let hub = Hub::new();
    let (seeded, served) = hub.head();
    assert!(served, "the import's head is served");
    let serve = hub.spawn_serve("first");

    // Summary: the imported index, nothing attested, automatic runs off.
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["indexed"], true, "{summary}");
    assert_eq!(summary["indexRows"], 6);
    assert_eq!(summary["attested"], 0);
    assert_eq!(summary["active"], 0);
    assert_eq!(summary["schedule"]["automatic"], false);

    // Attest and Protect each answer with a done receipt and change the
    // counts; the protected fragment keeps its broken drive out of repair.
    let attested = hub.repairs("attest", &["--drive-ids", "20,21,22,30,31,32"]).expect("attest");
    assert_eq!(attested["state"], "done", "{attested}");
    assert_eq!(hub.repairs("summary", &[]).expect("summary")["attested"], 6);
    let protect = hub
        .repairs("protect", &["--entity", "drive", "--entity-id", "20", "--reason", "edit"])
        .expect("protect");
    assert_eq!(protect["state"], "done", "{protect}");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["protected"], 1);
    assert_eq!(summary["attested"], 5);

    // Bad input is refused before any work.
    let refused = hub.repairs("undo", &["--batch-id", "1", "--component-key", "00"]).expect_err("both");
    assert!(!refused.is_empty());
    let refused = hub.repairs("retry", &["--component-key", "zz"]).expect_err("bad key");
    assert!(refused.contains("repair_invalid"), "{refused}");

    // Check now: a receipt at once, then the unprotected drive repaired as
    // one batch and its head served.
    let check = hub.repairs("check-now", &[]).expect("receipt");
    assert_eq!(check["state"], "accepted", "{check}");
    assert_eq!(check["kind"], "check_now");
    let settled = hub.settled(&check);
    let repaired = done(&settled);
    assert_eq!(repaired["outcome"], "repaired", "{settled}");
    let batch = repaired["batchId"].as_i64().expect("batch id");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["active"], 1, "{summary}");
    assert!(summary["schedule"]["lastAttemptMs"].is_number(), "{summary}");
    let attested = summary["attested"].clone();

    // Automatic runs need the phone witness to switch on, and never to
    // switch off. Switching on wakes the timer for a scheduled pass, so it
    // comes after Check now, whose pass would otherwise find the slot busy.
    let refused = hub.repairs("automatic", &["on"]).expect_err("witness needed");
    assert!(refused.contains("repair_witness_required"), "{refused}");
    let on = hub.repairs("automatic", &["on", "--phone-witness"]).expect("on");
    assert_eq!(on["result"]["automatic"], true, "{on}");
    let off = hub.repairs("automatic", &["off"]).expect("off");
    assert_eq!(off["result"]["automatic"], false, "{off}");
    hub.stop(serve);
    let (repaired_head, served) = hub.head();
    assert!(served, "the repaired head is served");
    assert_ne!(repaired_head, seeded, "the repair published a new head");

    // After a restart the choices and the repair are still there and the
    // receipt is gone.
    let serve = hub.spawn_serve("second");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["protected"], 1, "{summary}");
    assert_eq!(summary["attested"], attested);
    assert_eq!(summary["active"], 1);
    assert_eq!(summary["schedule"]["automatic"], false);
    let gone = hub.run(&["repairs", "job", "--job-id", &check["jobId"].to_string()]);
    assert!(String::from_utf8_lossy(&gone.stderr).contains("repair_job_unknown"));

    // Undo: a receipt at once, then the batch restored and the head served.
    let batch_id = batch.to_string();
    let receipt = hub.repairs("undo", &["--batch-id", &batch_id]).expect("receipt");
    assert_eq!(receipt["kind"], "undo", "{receipt}");
    let settled = hub.settled(&receipt);
    let undone = done(&settled);
    assert_eq!(undone["outcome"], "undone", "{settled}");
    assert_eq!(undone["components"], 1, "{settled}");
    assert_eq!(undone["served"], true, "{settled}");
    assert!(undone["restoredRows"].as_u64() > Some(0), "{settled}");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["active"], 0, "{summary}");
    assert_eq!(summary["undone"], 1, "{summary}");
    hub.stop(serve);
    let (restored_head, served) = hub.head();
    assert!(served, "the restored head is served");
    assert_ne!(restored_head, repaired_head, "the undo published a new head");

    assert_eq!(hub.accepted.load(Ordering::SeqCst), 0, "nothing reached the network");
}
