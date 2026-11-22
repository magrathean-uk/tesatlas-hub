// SPDX-License-Identifier: AGPL-3.0-only

//! M03-T28b: the broken-drive repair controls on the real service binary.
//!
//! A data folder holds an imported vehicle's candidate index (recorded through
//! the store's public API, as an import leaves it). The binary serves it idle;
//! every control then crosses the authenticated owner channel exactly as the
//! App will send it: summary, attest, protect, automatic on and off (with and
//! without the phone witness), Check now with its job receipt, Undo with its
//! job receipt and a refused bad input. The service is stopped and started
//! again and the choices are still there. Nothing reaches the network.
//!
//! Served packs for repaired and undone drives are proven in the importer's
//! in-crate apply tests (`broken_drive_repair_`), whose fixtures are private.
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
use tesatlas_hub::{
    db::HubStore,
    import::broken_drive_repair::{
        BrokenDriveCandidateIndex, GapEvidence, RepairInput, RepairSample, StateCoverage,
    },
};
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_tesatlas-hub");
const SERVE_START_LIMIT: Duration = Duration::from_secs(90);
const JOB_LIMIT: Duration = Duration::from_secs(60);
const NOW: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;

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
        for dir in ["home", "data", "anchor"] {
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
        let hub = Self { tmp, accepted, proxy, vehicle: Uuid::new_v4() };
        let doubles = serde_json::json!({ "anchorDir": hub.path("anchor") });
        fs::write(hub.path("doubles.json"), doubles.to_string()).expect("doubles");
        let config = format!(
            "data_dir = {:?}\nbind = \"127.0.0.1:{port}\"\n\n[geocoder]\nenabled = false\n\n[terrain]\nenabled = false\n",
            hub.path("data"),
        );
        fs::write(hub.path("config.toml"), config).expect("config");
        fs::set_permissions(hub.path("config.toml"), fs::Permissions::from_mode(0o600))
            .expect("config mode");
        hub.seed_index();
        hub
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    /// The candidate index an import of one broken drive leaves: two
    /// single-sample fragments and their closed destination.
    fn seed_index(&self) {
        let sample = |id: i64, at: i64, odometer: f64| RepairSample {
            position_id: id,
            date_ms: at,
            odometer_km: Some(odometer),
            speed_kmh: Some(40),
            row_digest: [0; 32],
        };
        let fragment = |id: i64, at: i64, odometer: f64, unclosed_before: bool| {
            let first = sample(id * 100, at, odometer);
            RepairInput {
                drive_id: id,
                car_id: 1,
                start_ms: at,
                end_ms: None,
                distance_km: None,
                duration_min: None,
                sample_count: 1,
                first_sample: Some(first),
                last_sample: Some(first),
                previous_drive_id: Some(id - 1),
                previous_unclosed: unclosed_before,
                next_drive_id: Some(id + 1),
                gap_to_next: Some(GapEvidence {
                    charge_between: false,
                    state: StateCoverage::Online,
                }),
            }
        };
        let rows = vec![
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
        ];
        HubStore::initialize(self.path("data"))
            .expect("store")
            .record_drive_repair_index(
                Uuid::new_v4(),
                self.vehicle,
                &BrokenDriveCandidateIndex { car_id: 1, rows },
                NOW,
            )
            .expect("index");
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

#[test]
fn broken_drive_repair_journey_controls_cross_the_owner_channel_and_survive_a_restart() {
    let hub = Hub::new();
    let serve = hub.spawn_serve("first");

    // Summary: the imported index, nothing attested, automatic runs off.
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["indexed"], true, "{summary}");
    assert_eq!(summary["indexRows"], 3);
    assert_eq!(summary["attested"], 0);
    assert_eq!(summary["schedule"]["automatic"], false);

    // Attest and Protect each answer with a done receipt and change the counts.
    let attested = hub.repairs("attest", &["--drive-ids", "20,21,22"]).expect("attest");
    assert_eq!(attested["state"], "done", "{attested}");
    assert_eq!(hub.repairs("summary", &[]).expect("summary")["attested"], 3);
    let protect = hub
        .repairs("protect", &["--entity", "drive", "--entity-id", "20", "--reason", "edit"])
        .expect("protect");
    assert_eq!(protect["state"], "done", "{protect}");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["protected"], 1);
    assert_eq!(summary["attested"], 2);

    // Automatic runs need the phone witness to switch on, and never to switch off.
    let refused = hub.repairs("automatic", &["on"]).expect_err("witness needed");
    assert!(refused.contains("repair_witness_required"), "{refused}");
    let on = hub.repairs("automatic", &["on", "--phone-witness"]).expect("on");
    assert_eq!(on["result"]["automatic"], true, "{on}");
    let off = hub.repairs("automatic", &["off"]).expect("off");
    assert_eq!(off["result"]["automatic"], false, "{off}");

    // Bad input is refused before any work.
    let refused = hub.repairs("undo", &["--batch-id", "1", "--component-key", "00"]).expect_err("both");
    assert!(!refused.is_empty());
    let refused = hub.repairs("retry", &["--component-key", "zz"]).expect_err("bad key");
    assert!(refused.contains("repair_invalid"), "{refused}");

    // Check now: a receipt at once, then a settled job that is on record.
    let receipt = hub.repairs("check-now", &[]).expect("receipt");
    assert_eq!(receipt["state"], "accepted", "{receipt}");
    assert_eq!(receipt["kind"], "check_now");
    let settled = hub.settled(&receipt);
    assert!(settled["state"] == "done" || settled["state"] == "failed", "{settled}");
    // A pass the service defers (its start-up is not finished) records nothing.
    if settled["state"] == "done" && settled["result"]["outcome"] != "deferred" {
        let summary = hub.repairs("summary", &[]).expect("summary");
        assert!(summary["schedule"]["lastAttemptMs"].is_number(), "{summary}");
    }

    // Undo: a receipt at once; with no repair and no cursor key it fails, named.
    let receipt = hub.repairs("undo", &["--batch-id", "1"]).expect("receipt");
    assert_eq!(receipt["kind"], "undo", "{receipt}");
    let settled = hub.settled(&receipt);
    assert_eq!(settled["state"], "failed", "{settled}");
    assert!(settled["code"].is_string(), "{settled}");

    hub.stop(serve);

    // After a restart the choices are still there and a receipt is gone.
    let serve = hub.spawn_serve("second");
    let summary = hub.repairs("summary", &[]).expect("summary");
    assert_eq!(summary["protected"], 1, "{summary}");
    assert_eq!(summary["attested"], 2);
    assert_eq!(summary["schedule"]["automatic"], false);
    let gone = hub.run(&["repairs", "job", "--job-id", &receipt["jobId"].to_string()]);
    assert!(String::from_utf8_lossy(&gone.stderr).contains("repair_job_unknown"));
    hub.stop(serve);

    assert_eq!(hub.accepted.load(Ordering::SeqCst), 0, "nothing reached the network");
}
