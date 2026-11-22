// SPDX-License-Identifier: AGPL-3.0-only

//! WINDOW-DRIVE-GRACE: a window whose signed end arrives during a drive stays
//! open until the drive ends, bounded by the grant's signed drive grace.

use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering},
    },
};

use ed25519_dalek::SigningKey;

use super::support::*;
use crate::collection::window::{
    AnchorStore, CloseReason, DriveActivity, EgressGate, GRANT_GAP_SECONDS, Gap, GraceDrive,
    HandbackSource, HighWater, ManualClock, NewestHubRow, OffsetProbe, Permit, PermitKind, Phase,
    Provider, Refusal, ResumeOutcome, SignedGrant, StepReport, Supervisor, WindowClock,
    WindowController, WindowDeps,
    grant::{GRANT_FORMAT, GrantBody},
};

/// The collector's open drives as the test sets them (`None` is a store
/// that cannot be read), and its newest row (`None`: unknown).
struct Drive(Mutex<Option<Vec<GraceDrive>>>, Mutex<Option<i64>>);

fn car(vehicle: &str, drive_id: i64) -> GraceDrive {
    GraceDrive {
        vehicle_id: vehicle.to_owned(),
        drive_id,
    }
}

/// The one car of most tests, driving.
fn driving() -> DriveActivity {
    DriveActivity::Driving(car("vehicle-a", 7))
}

impl Drive {
    fn new(activity: DriveActivity) -> Arc<Self> {
        let drive = Arc::new(Self(Mutex::new(None), Mutex::new(None)));
        drive.set(activity);
        drive
    }

    fn set(&self, activity: DriveActivity) {
        *self.0.lock().unwrap() = match activity {
            DriveActivity::Unknown => None,
            DriveActivity::NotDriving => Some(Vec::new()),
            DriveActivity::Driving(drive) => Some(vec![drive]),
        };
    }

    fn set_open(&self, drives: &[GraceDrive]) {
        *self.0.lock().unwrap() = Some(drives.to_vec());
    }

    fn newest_at(&self, wall: i64) {
        *self.1.lock().unwrap() = Some(wall);
    }
}

impl HandbackSource for Drive {
    fn high_water(&self) -> HighWater {
        HighWater::default()
    }

    fn drive_activity(&self) -> DriveActivity {
        match &*self.0.lock().unwrap() {
            None => DriveActivity::Unknown,
            Some(open) => open
                .first()
                .cloned()
                .map_or(DriveActivity::NotDriving, DriveActivity::Driving),
        }
    }

    fn drive_open(&self, drive: &GraceDrive) -> Option<bool> {
        self.0.lock().unwrap().as_ref().map(|open| open.contains(drive))
    }

    fn newest_row(&self, _from_ms: i64, _until_ms: i64) -> NewestHubRow {
        (*self.1.lock().unwrap()).map_or(NewestHubRow::Unknown, |wall| NewestHubRow::At(wall * 1000))
    }
}

fn hub_with(rig: &Rig, drive: &Arc<Drive>) -> WindowController {
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Fresh);
    hub.trust(&rig.trust).expect("trust");
    hub
}

fn try_grace_grant(rig: &Rig, start_in: i64, length: i64, grace: Option<i64>) -> Result<Vec<u8>, Refusal> {
    let out = rig.tmp.path().join(format!("grace-grant-{start_in}-{length}-{grace:?}.json"));
    rig.issuer
        .issue_with_drive_grace(&rig.key_path, &rig.request(HOST, start_in, length), grace, &out)?;
    Ok(fs::read(out).expect("grant file"))
}

fn grace_grant(rig: &Rig, length: i64, grace: i64) -> Vec<u8> {
    try_grace_grant(rig, 0, length, Some(grace)).expect("grant issued")
}

fn body(grace: Option<i64>) -> GrantBody {
    GrantBody {
        format: GRANT_FORMAT.to_owned(),
        issuer_key_id: "ed25519-sha256-aa".to_owned(),
        seq: 4,
        host_id: HOST.to_owned(),
        starts_at: T0,
        expires_at: T0 + 3600,
        approver: "G".to_owned(),
        reason: "why".to_owned(),
        trust_epoch: 1,
        drive_grace_seconds: grace,
    }
}

fn permitted(hub: &WindowController) -> bool {
    hub.gate().permit(Provider::Legacy, PermitKind::Request).is_ok()
}

fn ledger_has_grace(rig: &Rig) -> bool {
    String::from_utf8(rig.read("ledger.jsonl"))
        .unwrap()
        .contains("\"drive_grace\"")
}

type Running = (Arc<WindowController>, Supervisor<FakeSlot>, FakeSlot);

/// Opens a one-hour window with `grace`, starts its run and takes the last
/// supervisor step before the signed end, 10 s before it.
fn run_to_last_step(rig: &Rig, drive: &Arc<Drive>, grace: i64) -> Running {
    let hub = Arc::new(hub_with(rig, drive));
    hub.open(&grace_grant(rig, 3600, grace)).unwrap();
    let slot = FakeSlot::default();
    let mut supervisor = Supervisor::new(hub.clone(), slot.clone());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(3590);
    assert_eq!(supervisor.step().phase, Phase::Open);
    (hub, supervisor, slot)
}

/// [`run_to_last_step`], then the clock at the signed end with no step yet.
fn run_to_expiry(rig: &Rig, drive: &Arc<Drive>, grace: i64) -> Running {
    let running = run_to_last_step(rig, drive, grace);
    rig.clock.advance(10);
    running
}

#[test]
fn drive_grace_grant_round_trips_with_and_without_the_field() {
    let key = SigningKey::from_bytes(&[7u8; 32]);
    // Without the field: nothing is written, so an older grant's canonical
    // body, digest and signature are unchanged.
    let plain = SignedGrant::sign(body(None), &key).unwrap();
    let bytes = plain.to_bytes().unwrap();
    assert!(!String::from_utf8(bytes.clone()).unwrap().contains("drive_grace"));
    let parsed = SignedGrant::parse(&bytes).unwrap();
    assert_eq!(parsed, plain);
    parsed.verify(&key.verifying_key()).unwrap();
    let older: GrantBody = serde_json::from_value(serde_json::json!({
        "format": GRANT_FORMAT, "issuer_key_id": "ed25519-sha256-aa", "seq": 4,
        "host_id": HOST, "starts_at": T0, "expires_at": T0 + 3600, "approver": "G",
        "reason": "why", "trust_epoch": 1
    }))
    .unwrap();
    assert_eq!(older.canonical().unwrap(), body(None).canonical().unwrap());
    assert_eq!(older.drive_grace(), 0);

    // With it (zero included): signed like every other field.
    for grace in [0, 1800] {
        let grant = SignedGrant::sign(body(Some(grace)), &key).unwrap();
        let parsed = SignedGrant::parse(&grant.to_bytes().unwrap()).unwrap();
        assert_eq!(parsed.body.drive_grace_seconds, Some(grace));
        parsed.verify(&key.verifying_key()).unwrap();
        let mut json: serde_json::Value = serde_json::from_slice(&grant.to_bytes().unwrap()).unwrap();
        json["body"]["drive_grace_seconds"] = (grace + 60).into();
        let tampered = SignedGrant::parse(&serde_json::to_vec(&json).unwrap()).unwrap();
        assert_eq!(tampered.verify(&key.verifying_key()), Err(Refusal::SignatureInvalid));
        json["body"].as_object_mut().unwrap().remove("drive_grace_seconds");
        let stripped = SignedGrant::parse(&serde_json::to_vec(&json).unwrap()).unwrap();
        assert_eq!(stripped.verify(&key.verifying_key()), Err(Refusal::SignatureInvalid));
    }
    // A grace added to a signed grant without one does not verify.
    let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    json["body"]["drive_grace_seconds"] = 600.into();
    let added = SignedGrant::parse(&serde_json::to_vec(&json).unwrap()).unwrap();
    assert_eq!(added.verify(&key.verifying_key()), Err(Refusal::SignatureInvalid));
}

#[test]
fn drive_grace_old_grant_without_the_field_opens_and_resumes() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let hub = hub_with(&rig, &drive);
    let bytes = rig.grant(0, 3600);
    assert!(!String::from_utf8(bytes.clone()).unwrap().contains("drive_grace"));
    let status = hub.open(&bytes).unwrap();
    assert_eq!(status.phase, Phase::Open);
    assert_eq!(status.drive_grace_seconds, None);
    assert!(!String::from_utf8(rig.read("state.json")).unwrap().contains("drive_grace"));
    rig.clock.advance(100);
    drop(hub);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Resumed(Phase::Open));
    // No grace in the grant: today's close at expiry, even while driving.
    rig.clock.advance(3490);
    assert_eq!(hub.tick().unwrap(), Phase::Open);
    rig.clock.advance(10);
    assert!(!permitted(&hub));
    assert_eq!(hub.tick().unwrap(), Phase::Closing);
    assert_eq!(hub.finish_close().unwrap().unwrap().closed_at, T0 + 3600);
}

#[test]
fn drive_grace_out_of_range_is_refused() {
    let rig = Rig::new();
    for grace in [-1, 3601, i64::MAX] {
        assert_eq!(
            try_grace_grant(&rig, 0, 3600, Some(grace)).unwrap_err(),
            Refusal::DriveGraceInvalid,
            "grace {grace}"
        );
    }
    assert_eq!(body(Some(3601)).check_shape(), Err(Refusal::DriveGraceInvalid));
    assert_eq!(body(Some(3600)).check_shape(), Ok(()));
    assert_eq!(Refusal::DriveGraceInvalid.code(), "drive_grace_invalid");
    // The Hub refuses a validly signed grant that carries one.
    let drive = Drive::new(driving());
    let hub = hub_with(&rig, &drive);
    let mut bad = body(Some(3601));
    bad.issuer_key_id = rig.trust.issuer_key_id.clone();
    bad.seq = 50;
    let bytes = SignedGrant::sign(bad, &rig.signing_key()).unwrap().to_bytes().unwrap();
    assert_eq!(hub.open(&bytes).unwrap_err(), Refusal::DriveGraceInvalid);
    assert_eq!(hub.status().phase, Phase::Closed);
    // The maximum is issued.
    try_grace_grant(&rig, 0, 3600, Some(3600)).expect("3600 s grace issued");
}

#[test]
fn drive_grace_issuer_overlap_counts_the_grace() {
    let rig = Rig::new();
    // [0, 3600) plus 1800 s of grace: the next grant may start only a gap
    // after the grace end.
    try_grace_grant(&rig, 0, 3600, Some(1800)).unwrap();
    let after_grace = 3600 + 1800 + GRANT_GAP_SECONDS;
    assert_eq!(
        try_grace_grant(&rig, after_grace - 1, 600, None).unwrap_err(),
        Refusal::Overlap
    );
    try_grace_grant(&rig, after_grace, 600, None).expect("a gap after the grace end");
    // A new grant's own grace reaches a later reservation.
    let later = after_grace + 600 + GRANT_GAP_SECONDS + 10_000;
    try_grace_grant(&rig, later, 600, None).unwrap();
    let start = later - 1200 - GRANT_GAP_SECONDS;
    assert_eq!(
        try_grace_grant(&rig, start, 600, Some(601)).unwrap_err(),
        Refusal::Overlap
    );
    try_grace_grant(&rig, start, 600, Some(600)).expect("ends a gap before the later grant");
    // Only the reservations with a grace record one.
    let ledger = fs::read_to_string(rig.issuer.ledger_path()).unwrap();
    assert_eq!(ledger.matches("drive_grace_seconds").count(), 2);
}

#[test]
fn drive_grace_held_permit_stops_at_the_signed_end_and_one_new_run_starts() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, slot) = run_to_last_step(&rig, &drive, 1800);
    // The run's held stream and request permits, granted before the end.
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    let request = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    assert_eq!(stream.time_left().as_secs(), 10);
    rig.clock.advance(10);
    // At the signed end, before the controller decides: the gate refuses and
    // every held permit ends there.
    assert!(!permitted(&hub));
    assert!(stream.is_cancelled() && request.is_cancelled());
    assert!(hub.gate().expiry_observed().is_none(), "not latched: the controller decides");
    // One fresh reading: the drive is open, so the window enters its grace,
    // the old run is stopped and one new run starts at once.
    let report = supervisor.step();
    assert_eq!(report.phase, Phase::Open);
    assert!(report.collector_running);
    let status = hub.status();
    assert!(status.in_drive_grace);
    assert_eq!(status.drive_grace_until, Some(T0 + 5400));
    assert_eq!(status.deadline, Some(T0 + 3600), "the signed expiry is unchanged");
    assert!(ledger_has_grace(&rig));
    {
        let log = slot.log.lock().unwrap();
        assert_eq!((log.stops, log.starts), (1, 2));
        assert_eq!(log.deadline, Some(T0 + 5400));
    }
    // The held permits never moved; a new one runs to the grace end.
    assert!(stream.is_cancelled());
    assert_eq!(stream.time_left().as_secs(), 10);
    let grace = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    assert_eq!(grace.time_left().as_secs(), 1800);
    // Only one run without backoff: a later exit in the grace backs off.
    slot.log.lock().unwrap().running = false;
    rig.clock.advance(5);
    assert!(!supervisor.step().collector_running);
    assert_eq!(slot.log.lock().unwrap().starts, 2);
}

#[test]
fn drive_grace_closes_within_one_tick_of_the_drive_closing() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    for _ in 0..120 {
        rig.clock.advance(5);
        assert_eq!(supervisor.step().phase, Phase::Open);
    }
    assert!(permitted(&hub));
    assert!(!stream.is_cancelled());
    // The drive ends: the next tick closes through the normal close.
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().phase, Phase::Closed);
    assert!(stream.is_cancelled());
    assert!(!permitted(&hub));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3600 + 605);
    assert_eq!(handback.coverage_seconds, 3600 + 605);
    let log = slot.log.lock().unwrap();
    assert!(!log.running);
    assert_eq!(log.released, 1);
}

#[test]
fn drive_grace_failed_reading_closes_at_once() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    rig.clock.advance(30);
    drive.set(DriveActivity::Unknown);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert!(stream.is_cancelled());
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3630);
}

#[test]
fn drive_grace_closes_at_the_grace_end_while_still_driving() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    assert_eq!(stream.time_left().as_secs(), 600);
    for _ in 0..19 {
        rig.clock.advance(30);
        assert_eq!(supervisor.step().phase, Phase::Open);
    }
    let last = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    assert_eq!(last.time_left().as_secs(), 30, "cut to the grace end");
    drop(last);
    rig.clock.advance(30);
    assert!(!permitted(&hub));
    assert!(stream.is_cancelled());
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 4200));
    rig.clock.advance(5);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 4200);
    assert_eq!(handback.coverage_seconds, 4200);
    // The gate journalled the end it saw before it refused.
    let journal: serde_json::Value = serde_json::from_slice(&rig.read("drive-grace.json")).unwrap();
    assert_eq!(journal["ended_at"], T0 + 4200);
}

#[test]
fn drive_grace_not_driving_at_expiry_closes_at_expiry() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::NotDriving);
    let (hub, mut supervisor, slot) = run_to_expiry(&rig, &drive, 1800);
    assert!(!permitted(&hub));
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
    assert!(!ledger_has_grace(&rig));
    assert_eq!(slot.log.lock().unwrap().starts, 1);

    // An unreadable store at the signed end closes there too.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Unknown);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
    assert!(!ledger_has_grace(&rig));
}

#[test]
fn drive_grace_operator_close_in_grace_hands_back_to_the_close() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // Closed by the operator before any tick in the grace.
    rig.clock.advance(3);
    hub.close(CloseReason::OperatorClose);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::OperatorClose));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3603);
    assert_eq!(handback.coverage_seconds, 3603);
    // The gate's close journalled the end.
    let journal: serde_json::Value = serde_json::from_slice(&rig.read("drive-grace.json")).unwrap();
    assert_eq!(journal["ended_at"], T0 + 3603);

    // And after some ticks in the grace.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    for _ in 0..10 {
        assert_eq!(supervisor.step().phase, Phase::Open);
        rig.clock.advance(5);
    }
    hub.close(CloseReason::OperatorClose);
    supervisor.step();
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3650);
    assert_eq!(handback.coverage_seconds, 3650);
}

#[test]
fn drive_grace_restart_in_grace_resumes_and_after_the_grace_end_closes() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    rig.clock.advance(30);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Resumed(Phase::Open));
    let status = hub.status();
    assert!(status.in_drive_grace);
    assert_eq!(status.drive_grace_until, Some(T0 + 5400));
    let permit = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    assert_eq!(permit.time_left().as_secs(), 1770, "to the grace end");
    drop(permit);

    // A restart past the grace end closes at start, covering to it.
    drop(hub);
    rig.clock.advance(1800);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 5400);
    assert!(!permitted(&hub));

    // A restart in the grace after the drive ended closes at start.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    rig.clock.advance(30);
    drive.set(DriveActivity::NotDriving);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3630);

    // A restart in the grace without its journal (its end could not be
    // recorded) closes at start, even while driving.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    fs::remove_file(rig.window_dir().join("drive-grace.json")).unwrap();
    rig.clock.advance(30);
    let (_hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));

    // A window that was not in its grace does not resume past the expiry,
    // even while driving.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let hub = hub_with(&rig, &drive);
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    drop(hub);
    rig.clock.advance(3600);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
}

#[test]
fn drive_grace_unrecorded_grace_in_the_state_is_refused_at_restart() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let hub = hub_with(&rig, &drive);
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    drop(hub);
    // A state file that claims a grace its ledger never recorded.
    let path = rig.window_dir().join("state.json");
    let mut state: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    state["drive_grace_until"] = (T0 + 5400).into();
    fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
    rig.clock.advance(3700);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Faulted);
    assert!(!permitted(&hub));
}

#[test]
fn drive_grace_status_shows_the_grace() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = {
        let hub = Arc::new(hub_with(&rig, &drive));
        hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
        let json = crate::runtime::operator::authority::status_json(&hub.status());
        assert_eq!(json["driveGraceSeconds"], 1800);
        assert_eq!(json["inDriveGrace"], false);
        assert!(json["driveGraceUntil"].is_null());
        let slot = FakeSlot::default();
        let supervisor = Supervisor::new(hub.clone(), slot.clone());
        (hub, supervisor, slot)
    };
    supervisor.step();
    rig.clock.advance(3590);
    supervisor.step();
    rig.clock.advance(10);
    supervisor.step();
    let json = crate::runtime::operator::authority::status_json(&hub.status());
    assert_eq!(json["phase"], "open");
    assert_eq!(json["inDriveGrace"], true);
    assert_eq!(json["driveGraceUntil"], T0 + 5400);
    assert_eq!(json["deadline"], T0 + 3600);
    drive.set(DriveActivity::NotDriving);
    supervisor.step();
    let json = crate::runtime::operator::authority::status_json(&hub.status());
    assert_eq!(json["phase"], "closed");
    assert_eq!(json["inDriveGrace"], false);
    assert_eq!(json["closeReason"], "expired");
}

#[test]
fn drive_grace_follows_the_qualifying_drive_only() {
    let rig = Rig::new();
    let (a, b) = (car("vehicle-a", 7), car("vehicle-b", 9));
    let drive = Drive::new(DriveActivity::Driving(a.clone()));
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // The qualifying vehicle and drive are recorded in the state and the line.
    let state: serde_json::Value = serde_json::from_slice(&rig.read("state.json")).unwrap();
    assert_eq!(state["drive_grace_drive"]["vehicle_id"], "vehicle-a");
    assert_eq!(state["drive_grace_drive"]["drive_id"], 7);
    let ledger = String::from_utf8(rig.read("ledger.jsonl")).unwrap();
    let line = ledger.lines().find(|line| line.contains("drive_grace")).unwrap();
    assert!(line.contains("vehicle-a") && line.contains("\"drive_id\":7"), "{line}");
    // Car B starts a drive in the grace while A drives on.
    drive.set_open(&[a.clone(), b.clone()]);
    rig.clock.advance(5);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // A's drive ends: the grace ends, whatever B does.
    drive.set_open(std::slice::from_ref(&b));
    rig.clock.advance(5);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3610);

    // A restart resumes only on that same drive.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving(a.clone()));
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    drive.set_open(std::slice::from_ref(&b));
    rig.clock.advance(10);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3610);
    // And with it open the same restart resumes.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving(a.clone()));
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    drive.set_open(&[b, a]);
    rig.clock.advance(10);
    let (_hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Resumed(Phase::Open));
}

#[test]
fn drive_grace_restart_before_the_first_tick_adopts_the_journal_before_the_gap() {
    // The entry's journal is written and the process stops before the state
    // records the grace: the restart adopts the journal, then measures the
    // gap the collector was down for, past the signed end.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    let path = rig.window_dir().join("state.json");
    let mut state: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let object = state.as_object_mut().unwrap();
    object.remove("drive_grace_until");
    object.remove("drive_grace_drive");
    fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
    // The collector's newest row is ten seconds into the grace.
    drive.newest_at(T0 + 3610);
    rig.clock.advance(100);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::Resumed(Phase::Open));
    assert!(hub.status().in_drive_grace);
    assert!(permitted(&hub));
    // The window closes with the drive; the downtime is a gap.
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    assert_eq!(hub.tick().unwrap(), Phase::Closing);
    let handback = hub.finish_close().unwrap().unwrap();
    assert_eq!(handback.closed_at, T0 + 3705);
    assert_eq!(handback.gaps, vec![crate::collection::window::Gap { start: T0 + 3611, end: T0 + 3700 }]);
}

#[test]
fn drive_grace_end_seen_through_a_held_permit_is_final_across_a_restart() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let held = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    // A forward wall step past the grace end: the held permit finds its own
    // deadline passed and reports it; the gate journals the end before it
    // refuses, and the process stops before a tick writes the close.
    rig.clock.shift_wall(2000);
    held.deadline_seen(rig.clock.wall(), rig.clock.mono());
    assert!(held.is_cancelled());
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 5600));
    drop(held);
    drop(supervisor);
    drop(hub);
    // The wall clock returns to its earlier reading; the car still drives.
    rig.clock.shift_wall(-2000);
    rig.clock.advance(5);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert!(!permitted(&hub));
    // Coverage never runs past the restart.
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3605);

    // The same through a fresh ask.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    rig.clock.shift_wall(2000);
    assert!(!permitted(&hub));
    drop(supervisor);
    drop(hub);
    rig.clock.shift_wall(-2000);
    rig.clock.advance(5);
    let (_hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));

    // A permit granted before the grace never reports to the gate.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, _supervisor, _slot) = run_to_last_step(&rig, &drive, 1800);
    let before = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    rig.clock.advance(10);
    before.deadline_seen(rig.clock.wall(), rig.clock.mono());
    assert!(hub.gate().expiry_observed().is_none());
}

#[test]
fn drive_grace_overflowing_end_is_refused_as_invalid_expiry() {
    let edge = i64::MAX - 1000;
    // Room for the gap alone, none for the grace and the gap together.
    let mut overflowing = body(Some(600));
    overflowing.expires_at = edge;
    overflowing.starts_at = edge - 3600;
    assert_eq!(overflowing.check_shape(), Err(Refusal::ExpiryInvalid));
    overflowing.drive_grace_seconds = None;
    assert_eq!(overflowing.check_shape(), Ok(()));

    let rig = Rig::new();
    let at = |starts_at: i64, expires_at: i64| {
        let mut request = rig.request(HOST, 0, 60);
        request.starts_at = starts_at;
        request.expires_at = expires_at;
        request
    };
    let issue = |request: &crate::collection::window::IssueRequest, grace: Option<i64>| {
        let out = rig.tmp.path().join("edge-grant.json");
        rig.issuer
            .issue_with_drive_grace(&rig.key_path, request, grace, &out)
            .map(|_| ())
    };
    // A reservation starting 25 s after that expiry is held.
    issue(&at(edge + 25, edge + 85), None).expect("representable with its gap");
    // The grace request reaches the overflow: refused, no panic, no wrap.
    assert_eq!(
        issue(&at(edge - 3600, edge), Some(600)),
        Err(Refusal::ExpiryInvalid)
    );
    // Without grace it overlaps the held reservation as before.
    assert_eq!(issue(&at(edge - 3600, edge), None), Err(Refusal::Overlap));
}

// ---- round 4: the edges of the changed approach ------------------------------

#[test]
fn drive_grace_hard_end_seen_while_awaiting_the_controller_is_final() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, slot) = run_to_expiry(&rig, &drive, 1800);
    // The gate sees the signed end and waits for the controller.
    assert!(!permitted(&hub));
    assert!(hub.gate().expiry_observed().is_none());
    // Before the controller's tick the wall clock passes the grace end: the
    // hard end is latched at that reading.
    rig.clock.shift_wall(2000);
    assert!(!permitted(&hub));
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 5600));
    // It rolls back inside the grace while the car still drives: the
    // controller closes, it never enters the grace.
    rig.clock.shift_wall(-2000);
    assert!(!permitted(&hub));
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!permitted(&hub));
    assert!(!hub.status().in_drive_grace);
    assert!(!ledger_has_grace(&rig));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
    assert_eq!(slot.log.lock().unwrap().starts, 1, "no grace run");
}

/// The gate's clock as a held permit reads it, with the wall stepped by
/// `shift` (the controller's clock is not stepped).
#[derive(Debug)]
struct StepWall {
    base: Arc<ManualClock>,
    shift: AtomicI64,
}

impl WindowClock for StepWall {
    fn wall(&self) -> i64 {
        self.base.wall() + self.shift.load(Ordering::SeqCst)
    }
    fn mono(&self) -> u64 {
        self.base.mono()
    }
    fn mono_boot_scoped(&self) -> bool {
        self.base.mono_boot_scoped()
    }
    fn boot_time(&self) -> Result<i64, Refusal> {
        self.base.boot_time()
    }
}

#[test]
fn drive_grace_end_a_held_permit_saw_is_journalled_after_a_clock_correction() {
    use crate::api::fleet_api::egress::{ask, test_support::route_on};

    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let clock = Arc::new(StepWall {
        base: rig.clock.clone(),
        shift: AtomicI64::new(0),
    });
    let endpoint = "https://drive-grace-held-permit.test:9443/";
    let _route = route_on(endpoint, hub.gate().clone(), clock.clone());
    let held = ask(endpoint, Provider::Legacy, PermitKind::Stream).expect("permitted in the grace");
    // The held permit reads a wall step past the grace end; the clock is
    // corrected before anything else reads it.
    clock.shift.store(2000, Ordering::SeqCst);
    assert!(held.check().is_err());
    clock.shift.store(0, Ordering::SeqCst);
    // The journal keeps the end the permit saw, and the gate latched there.
    let journal: serde_json::Value = serde_json::from_slice(&rig.read("drive-grace.json")).unwrap();
    assert_eq!(journal["ended_at"], T0 + 5600);
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 5600));
    assert!(!permitted(&hub));
    drop(held);
    drop(supervisor);
    drop(hub);
    // A restart on the corrected clock, still driving: the window stays shut.
    rig.clock.advance(5);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert!(!permitted(&hub));
}

/// A time probe that takes `SLOW` seconds when armed; armed with `END`
/// the recorded drive ends as it starts.
struct SlowProbe {
    clock: Arc<ManualClock>,
    drive: Arc<Drive>,
    armed: AtomicU8,
    measured: AtomicBool,
}

impl SlowProbe {
    const SLOW: u8 = 1;
    const END: u8 = 2;
}

impl OffsetProbe for SlowProbe {
    fn offset_seconds(&self) -> Result<f64, Refusal> {
        match self.armed.swap(0, Ordering::SeqCst) {
            0 => {}
            armed => {
                if armed == Self::END {
                    self.drive.set(DriveActivity::NotDriving);
                }
                self.clock.advance(5);
                self.measured.store(true, Ordering::SeqCst);
            }
        }
        Ok(0.2)
    }
}

#[test]
fn drive_grace_slow_probe_closes_within_one_tick_of_the_drive_ending() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let probe = Arc::new(SlowProbe {
        clock: rig.clock.clone(),
        drive: drive.clone(),
        armed: AtomicU8::new(0),
        measured: AtomicBool::new(false),
    });
    let (hub, outcome) = WindowController::start(
        rig.config(),
        WindowDeps {
            clock: rig.clock.clone(),
            anchor: rig.anchor.clone(),
            probe: probe.clone(),
            gate: EgressGate::new(rig.clock.clone()),
            handback: drive.clone(),
        },
    );
    assert_eq!(outcome, ResumeOutcome::Fresh);
    hub.trust(&rig.trust).unwrap();
    let hub = Arc::new(hub);
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    supervisor.step();
    rig.clock.advance(3600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    let mut step_until_probed = |armed: u8| {
        probe.measured.store(false, Ordering::SeqCst);
        probe.armed.store(armed, Ordering::SeqCst);
        for _ in 0..200 {
            rig.clock.advance(5);
            let report = supervisor.step();
            if probe.measured.load(Ordering::SeqCst) {
                return report;
            }
            assert_eq!(report.phase, Phase::Open);
        }
        panic!("the time probe never ran");
    };
    // A slow probe in the grace: the next tick is due a poll after this one
    // began, not after it ended.
    assert_eq!(step_until_probed(SlowProbe::SLOW).phase, Phase::Open);
    assert_eq!(hub.next_wakeup_seconds(), 1);
    // The drive ends just after the tick read it, as the slow probe starts:
    // the window closes in that same tick, at the probe's end.
    let report = step_until_probed(SlowProbe::END);
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!permitted(&hub));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, rig.clock.wall());
}

#[test]
fn drive_grace_start_refusal_journals_the_end_before_the_close() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    // What a crash right after the journal write leaves on disk.
    let dir = rig.window_dir();
    let state = fs::read(dir.join("state.json")).unwrap();
    let ledger = fs::read(dir.join("ledger.jsonl")).unwrap();
    let anchor_name = format!("window-consumption-{HOST}");
    let anchor = rig.anchor.read(&anchor_name).unwrap().unwrap();
    // The start refuses: the drive cannot be read.
    rig.clock.advance(30);
    drive.set(DriveActivity::Unknown);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    drop(hub);
    let journal: serde_json::Value = serde_json::from_slice(&rig.read("drive-grace.json")).unwrap();
    assert_eq!(journal["ended_at"], T0 + 3630);
    // The process stopped before the closing state was durable.
    fs::write(dir.join("state.json"), &state).unwrap();
    fs::write(dir.join("ledger.jsonl"), &ledger).unwrap();
    rig.anchor.write(&anchor_name, &anchor).unwrap();
    // A second start with the drive readable and open: still closed.
    drive.set(driving());
    rig.clock.advance(5);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert!(!permitted(&hub));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3630);
}

#[test]
fn drive_grace_handback_shows_the_pause_before_the_grace_run_as_a_gap() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, slot) = run_to_last_step(&rig, &drive, 1800);
    // The signed end passes 4 s before the next step.
    rig.clock.advance(14);
    let report = supervisor.step();
    assert_eq!(report.phase, Phase::Open);
    assert!(report.collector_running);
    assert_eq!(slot.log.lock().unwrap().starts, 2);
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3609);
    assert_eq!(handback.gaps, vec![Gap { start: T0 + 3600, end: T0 + 3604 }]);
    assert_eq!(handback.coverage_seconds, 3609 - 4);
}

#[test]
fn drive_grace_explicit_null_is_refused() {
    let key = SigningKey::from_bytes(&[7u8; 32]);
    let plain = SignedGrant::sign(body(None), &key).unwrap();
    let bytes = plain.to_bytes().unwrap();
    // Absent stays valid and byte-identical.
    let parsed = SignedGrant::parse(&bytes).unwrap();
    parsed.verify(&key.verifying_key()).unwrap();
    assert_eq!(parsed.to_bytes().unwrap(), bytes);
    // An explicit null added to the signed grant is malformed.
    let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    json["body"]["drive_grace_seconds"] = serde_json::Value::Null;
    assert_eq!(
        SignedGrant::parse(&serde_json::to_vec(&json).unwrap()).unwrap_err(),
        Refusal::GrantMalformed
    );
    let mut fields = serde_json::to_value(body(None)).unwrap();
    fields["drive_grace_seconds"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<GrantBody>(fields).is_err());
}

// ---- WGE-a: the drive-grace edges in the gate and the supervisor ------------

/// [`Drive`] whose next drive reading takes `SLOW` seconds when armed.
struct SlowDrive {
    drive: Arc<Drive>,
    clock: Arc<ManualClock>,
    armed: AtomicBool,
}

impl SlowDrive {
    const SLOW: u64 = 5;
}

impl HandbackSource for SlowDrive {
    fn high_water(&self) -> HighWater {
        self.drive.high_water()
    }

    fn drive_activity(&self) -> DriveActivity {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.advance(Self::SLOW);
        }
        self.drive.drive_activity()
    }

    fn drive_open(&self, drive: &GraceDrive) -> Option<bool> {
        self.drive.drive_open(drive)
    }

    fn newest_row(&self, from_ms: i64, until_ms: i64) -> NewestHubRow {
        self.drive.newest_row(from_ms, until_ms)
    }
}

#[test]
fn window_grace_edge_slow_tick_at_the_signed_end_keeps_the_handback_gap() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let slow = Arc::new(SlowDrive {
        drive: drive.clone(),
        clock: rig.clock.clone(),
        armed: AtomicBool::new(false),
    });
    let (hub, outcome) = rig.start_hub_with(slow.clone());
    assert_eq!(outcome, ResumeOutcome::Fresh);
    hub.trust(&rig.trust).unwrap();
    let hub = Arc::new(hub);
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let slot = FakeSlot::default();
    let mut supervisor = Supervisor::new(hub.clone(), slot.clone());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(3590);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // The step starts exactly at the signed end; its tick spends 5 s on the
    // drive reading before the grace run can start.
    rig.clock.advance(10);
    slow.armed.store(true, Ordering::SeqCst);
    let report = supervisor.step();
    assert_eq!(report.phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    assert_eq!(slot.log.lock().unwrap().starts, 2);
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3610);
    // Nothing was collected from the signed end until the grace run began.
    assert_eq!(handback.gaps, vec![Gap { start: T0 + 3600, end: T0 + 3605 }]);
}

/// Reads the grace journal's end at the moment a held permit is woken by its
/// cancellation.
struct EndAtCancel {
    journal: std::path::PathBuf,
    seen: Mutex<Option<serde_json::Value>>,
}

impl std::task::Wake for EndAtCancel {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let journal: serde_json::Value =
            serde_json::from_slice(&fs::read(&self.journal).unwrap_or_default()).unwrap_or_default();
        *self.seen.lock().unwrap() = Some(journal["ended_at"].clone());
    }
}

#[test]
fn window_grace_edge_held_permit_is_cut_only_after_the_end_is_journalled() {
    use std::future::Future;

    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    let other = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    let witness = Arc::new(EndAtCancel {
        journal: rig.window_dir().join("drive-grace.json"),
        seen: Mutex::new(None),
    });
    let waker = std::task::Waker::from(witness.clone());
    let mut cancelled = std::pin::pin!(other.cancel_token().cancelled());
    assert!(
        cancelled
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    // A transient forward wall reading past the grace end latches the gate.
    rig.clock.shift_wall(2000);
    assert!(!permitted(&hub));
    rig.clock.shift_wall(-2000);
    assert!(other.is_cancelled());
    // When the held permit was woken, the end was already durable.
    assert_eq!(*witness.seen.lock().unwrap(), Some(serde_json::json!(T0 + 5600)));
}

#[test]
fn window_grace_edge_held_permit_reading_never_acts_on_a_successor_window() {
    use crate::collection::window::{gate::GraceArm, state::GraceJournal};

    let rig = Rig::new();
    let gate = EgressGate::new(rig.clock.clone());
    let (wall, mono) = (rig.clock.wall(), rig.clock.mono());
    // Window A, armed with a drive grace, enters it at its signed end.
    gate.open(1, wall + 60, mono + 60);
    gate.arm_drive_grace(GraceArm {
        end_wall: wall + 600,
        end_mono: mono + 600,
        journal: rig.tmp.path().join("edge-grace.json"),
    });
    rig.clock.advance(60);
    assert!(gate.permit(Provider::Legacy, PermitKind::Request).is_err());
    assert!(gate.enter_drive_grace(GraceJournal {
        grant_digest: "a".to_owned(),
        entered_at: wall + 60,
        drive: car("vehicle-a", 7),
        until: wall + 600,
        ended_at: None,
    }));
    let held = gate.permit(Provider::Legacy, PermitKind::Stream).unwrap();
    // The held permit takes a reading during a forward wall step and pauses
    // before it reports it; meanwhile the clock is corrected, A closes and B
    // (no grace) opens.
    let captured = (rig.clock.wall() + 1000, rig.clock.mono());
    gate.close();
    let (wall, mono) = (rig.clock.wall(), rig.clock.mono());
    gate.open(2, wall + 300, mono + 300);
    held.deadline_seen(captured.0, captured.1);
    // B stays open: both current clocks are inside it.
    assert!(gate.is_open());
    assert!(gate.expiry_observed().is_none());
    assert!(gate.permit(Provider::Legacy, PermitKind::Request).is_ok());
}

#[test]
fn window_grace_edge_held_permit_under_a_corrected_clock_neither_extends_nor_ends() {
    // A stream granted before the signed end sees the wall pass the grace
    // end; the clock is corrected before a tick or a fresh permit reads it.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, slot) = run_to_last_step(&rig, &drive, 1800);
    let held = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    rig.clock.shift_wall(2000);
    held.deadline_seen(rig.clock.wall(), rig.clock.mono());
    rig.clock.shift_wall(-2000);
    drop(held);
    // The hard end it saw is final: at the signed end the window closes and
    // never enters its grace.
    rig.clock.advance(10);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!hub.status().in_drive_grace);
    assert!(!ledger_has_grace(&rig));
    assert_eq!(slot.log.lock().unwrap().starts, 1, "no grace run");

    // A reading inside the grace from a permit granted before it, reported
    // once the window is in its grace, ends nothing.
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_last_step(&rig, &drive, 1800);
    let before = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    rig.clock.advance(10);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    rig.clock.advance(5);
    before.deadline_seen(rig.clock.wall(), rig.clock.mono());
    assert!(permitted(&hub));
    assert!(hub.gate().expiry_observed().is_none());
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
}

/// A time probe in the grace that takes `seconds` of the clock and asks the
/// gate for a fresh permit while it blocks; `granted` records the answer and
/// `held_live` whether the permit in `held` was still running.
struct GatedProbe {
    clock: Arc<ManualClock>,
    gate: EgressGate,
    seconds: AtomicU8,
    measured: AtomicBool,
    granted: AtomicBool,
    held: Mutex<Option<Permit>>,
    held_live: AtomicBool,
}

impl OffsetProbe for GatedProbe {
    fn offset_seconds(&self) -> Result<f64, Refusal> {
        let seconds = self.seconds.swap(0, Ordering::SeqCst);
        if seconds > 0 {
            self.clock.advance(u64::from(seconds));
            let granted = self.gate.permit(Provider::Legacy, PermitKind::Request).is_ok();
            self.granted.store(granted, Ordering::SeqCst);
            let live = self
                .held
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|permit| !permit.is_cancelled());
            self.held_live.store(live, Ordering::SeqCst);
            self.measured.store(true, Ordering::SeqCst);
        }
        Ok(0.2)
    }
}

/// Opens a one-hour window with a 30-minute grace on a hub whose time probe
/// is a [`GatedProbe`], and runs it into the grace while driving.
fn run_into_grace_with_gated_probe(
    rig: &Rig,
) -> (Arc<WindowController>, Arc<GatedProbe>, Supervisor<FakeSlot>) {
    let drive = Drive::new(driving());
    let gate = EgressGate::new(rig.clock.clone());
    let probe = Arc::new(GatedProbe {
        clock: rig.clock.clone(),
        gate: gate.clone(),
        seconds: AtomicU8::new(0),
        measured: AtomicBool::new(false),
        granted: AtomicBool::new(false),
        held: Mutex::new(None),
        held_live: AtomicBool::new(false),
    });
    let (hub, outcome) = WindowController::start(
        rig.config(),
        WindowDeps {
            clock: rig.clock.clone(),
            anchor: rig.anchor.clone(),
            probe: probe.clone(),
            gate,
            handback: drive,
        },
    );
    assert_eq!(outcome, ResumeOutcome::Fresh);
    hub.trust(&rig.trust).unwrap();
    let hub = Arc::new(hub);
    hub.open(&grace_grant(rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    supervisor.step();
    rig.clock.advance(3600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    (hub, probe, supervisor)
}

/// Steps the supervisor 5 s at a time until the next due time probe has run,
/// taking `seconds` of the clock.
fn step_until_probed(
    rig: &Rig,
    probe: &GatedProbe,
    supervisor: &mut Supervisor<FakeSlot>,
    seconds: u8,
) -> StepReport {
    probe.measured.store(false, Ordering::SeqCst);
    probe.granted.store(true, Ordering::SeqCst);
    probe.held_live.store(false, Ordering::SeqCst);
    probe.seconds.store(seconds, Ordering::SeqCst);
    for _ in 0..200 {
        rig.clock.advance(5);
        let report = supervisor.step();
        if probe.measured.load(Ordering::SeqCst) {
            return report;
        }
        assert_eq!(report.phase, Phase::Open);
    }
    panic!("the time probe never ran");
}

#[test]
fn window_grace_edge_probe_in_the_grace_holds_the_gate_and_a_late_answer_ends_it() {
    let rig = Rig::new();
    let (hub, probe, mut supervisor) = run_into_grace_with_gated_probe(&rig);
    // A probe that answers within its bound: no fresh permit while it
    // blocks; the drive is still open after it, so the gate opens again.
    assert_eq!(step_until_probed(&rig, &probe, &mut supervisor, 5).phase, Phase::Open);
    assert!(!probe.granted.load(Ordering::SeqCst), "a permit was granted during the probe");
    assert!(permitted(&hub));
    assert!(hub.status().in_drive_grace);
    // Two seconds of name resolution and a five-second timeout: a probe that
    // has not answered within its bound counts as not driving.
    let report = step_until_probed(&rig, &probe, &mut supervisor, 7);
    assert!(!probe.granted.load(Ordering::SeqCst), "a permit was granted during the probe");
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!permitted(&hub));
}

#[test]
fn window_grace_edge_probe_in_the_grace_keeps_held_permits_and_pauses_new_ones() {
    let rig = Rig::new();
    let (hub, probe, mut supervisor) = run_into_grace_with_gated_probe(&rig);
    // A stream granted in the grace keeps running through a probe that
    // answers driving, and a fresh request waits for the answer.
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).expect("a stream in the grace");
    *probe.held.lock().unwrap() = Some(stream);
    assert_eq!(step_until_probed(&rig, &probe, &mut supervisor, 5).phase, Phase::Open);
    assert!(!probe.granted.load(Ordering::SeqCst), "a permit was granted during the probe");
    assert!(probe.held_live.load(Ordering::SeqCst), "the probe cut the held stream");
    assert!(permitted(&hub), "the answer did not resume the grants");
    assert!(!probe.held.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert!(hub.status().in_drive_grace);
    // A late answer ends the grace: the stream ran through the probe and the
    // close cuts it.
    let report = step_until_probed(&rig, &probe, &mut supervisor, 7);
    assert!(!probe.granted.load(Ordering::SeqCst), "a permit was granted during the probe");
    assert!(probe.held_live.load(Ordering::SeqCst), "the probe cut the held stream");
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(probe.held.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert!(!permitted(&hub));
}

#[test]
fn window_grace_edge_refusal_after_a_clock_correction_claims_no_future_coverage() {
    // A fresh permit sees the wall jump from 30 minutes before the signed end
    // to just past it, inside the grace cap; the clock is corrected before
    // the tick, which finds no open drive.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::NotDriving);
    let hub = Arc::new(hub_with(&rig, &drive));
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    rig.clock.shift_wall(1801);
    assert!(!permitted(&hub));
    rig.clock.shift_wall(-1801);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!ledger_has_grace(&rig));
    // The refusal's coverage ends at the last reading the window observed,
    // never at the future reading the correction undid.
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 1800);
}

// ---- REV-CX-WGE repair (WGE-FIX) ------------------------------------------

/// The rig's clock with two edges a test sets: the next wall reading alone
/// is stepped by `once`, and the monotonic milliseconds carry `millis` past
/// the whole second the rig reads.
#[derive(Debug)]
struct EdgeClock {
    base: Arc<ManualClock>,
    once: AtomicI64,
    millis: AtomicU64,
}

impl EdgeClock {
    fn on(rig: &Rig) -> Arc<Self> {
        Arc::new(Self {
            base: rig.clock.clone(),
            once: AtomicI64::new(0),
            millis: AtomicU64::new(100),
        })
    }
}

impl WindowClock for EdgeClock {
    fn wall(&self) -> i64 {
        self.base.wall() + self.once.swap(0, Ordering::SeqCst)
    }
    fn mono(&self) -> u64 {
        self.base.mono()
    }
    fn mono_millis(&self) -> u64 {
        self.base.mono() * 1000 + self.millis.load(Ordering::SeqCst)
    }
    fn mono_boot_scoped(&self) -> bool {
        self.base.mono_boot_scoped()
    }
    fn boot_time(&self) -> Result<i64, Refusal> {
        self.base.boot_time()
    }
}

/// A trusted hub on `clock`, with its own probe and drive source.
fn edge_hub(
    rig: &Rig,
    clock: Arc<dyn WindowClock>,
    probe: Arc<dyn OffsetProbe>,
    handback: Arc<dyn HandbackSource>,
) -> Arc<WindowController> {
    let (hub, outcome) = WindowController::start(
        rig.config(),
        WindowDeps {
            clock: clock.clone(),
            anchor: rig.anchor.clone(),
            probe,
            gate: EgressGate::new(clock),
            handback,
        },
    );
    assert_eq!(outcome, ResumeOutcome::Fresh);
    hub.trust(&rig.trust).unwrap();
    Arc::new(hub)
}

#[test]
fn wge_fix_1_held_stream_cannot_send_while_the_hard_end_is_journalled() {
    use std::{cell::Cell, rc::Rc, sync::mpsc, time::Duration};

    use crate::{
        api::fleet_api::egress::{ask, test_support::route_on},
        collection::window::gate::before_grace_end_write,
    };

    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    // The held stream reads the corrected clock: time is left on its own
    // deadlines throughout.
    let held_clock = Arc::new(ManualClock::new(rig.clock.wall()));
    let endpoint = "https://wge-fix-held-stream.test:9443/";
    let _route = route_on(endpoint, hub.gate().clone(), held_clock);
    let held = Arc::new(ask(endpoint, Provider::Legacy, PermitKind::Stream).expect("permitted in the grace"));
    assert!(held.check().is_ok());
    let sends = Rc::new(Cell::new(None));
    let (witness, seen) = (held.clone(), sends.clone());
    // The held stream checks on its own thread: a check that meets a shut in
    // progress waits for it (WGE-R2), so a send is what must not come back.
    before_grace_end_write(Some(Box::new(move || {
        let (witness, (tx, rx)) = (witness.clone(), mpsc::channel());
        std::thread::spawn(move || {
            let _ = tx.send(witness.check().is_ok());
        });
        seen.set(Some(rx.recv_timeout(Duration::from_millis(300)).unwrap_or(false)));
    })));
    // A fresh admission check reads a wall step past the grace end and
    // latches; the held stream checks before its next Ping while the end is
    // being written.
    rig.clock.shift_wall(2000);
    assert!(!permitted(&hub));
    rig.clock.shift_wall(-2000);
    before_grace_end_write(None);
    assert_eq!(sends.get(), Some(false), "the held stream could send while the hard end was journalled");
    assert!(held.check().is_err());
}

#[test]
fn wge_fix_2_hard_end_latched_by_a_held_permit_claims_no_coverage_past_the_corrected_reading() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let hub = Arc::new(hub_with(&rig, &drive));
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // A stream granted before expiry reads a brief wall step past the hard
    // end; the wall is corrected before the tick.
    let held = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    rig.clock.shift_wall(3601);
    held.deadline_seen(rig.clock.wall(), rig.clock.mono());
    rig.clock.shift_wall(-3601);
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 5401));
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!ledger_has_grace(&rig));
    // The latch closes the window, and coverage ends at the corrected reading.
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 1800);
}

#[test]
fn wge_fix_3_corrected_clock_ends_the_handback_gap_when_the_grace_run_starts() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let slow = Arc::new(SlowDrive {
        drive: drive.clone(),
        clock: rig.clock.clone(),
        armed: AtomicBool::new(false),
    });
    let clock = EdgeClock::on(&rig);
    let hub = edge_hub(&rig, clock.clone(), rig.probe.clone(), slow.clone());
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let slot = FakeSlot::default();
    let mut supervisor = Supervisor::new(hub.clone(), slot.clone());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(3590);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // The step reads T+120 in a brief wall step; the controller reads the
    // corrected T+1 and spends 5 s on the drive reading.
    rig.clock.advance(11);
    clock.once.store(119, Ordering::SeqCst);
    slow.armed.store(true, Ordering::SeqCst);
    let report = supervisor.step();
    assert_eq!(report.phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    assert_eq!(slot.log.lock().unwrap().starts, 2);
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    // The grace run started at T+6: the gap ends there, not at the stale T+120.
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.gaps, vec![Gap { start: T0 + 3600, end: T0 + 3606 }]);
}

#[test]
fn wge_fix_4_normal_window_gap_ends_at_the_step_reading_after_a_slow_probe() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::NotDriving);
    let probe = Arc::new(SlowProbe {
        clock: rig.clock.clone(),
        drive: drive.clone(),
        armed: AtomicU8::new(0),
        measured: AtomicBool::new(false),
    });
    let hub = edge_hub(&rig, rig.clock.clone(), probe.clone(), drive.clone());
    hub.open(&rig.grant(0, 3600)).unwrap();
    assert_eq!(hub.status().drive_grace_seconds, None, "a normal window");
    let slot = FakeSlot::default();
    slot.log.lock().unwrap().fail_starts = 1;
    let mut supervisor = Supervisor::new(hub.clone(), slot.clone());
    // The collector fails to start: a gap opens at T0.
    assert!(!supervisor.step().collector_running);
    // The next start succeeds in a step whose due time probe takes 5 s.
    rig.clock.advance(300);
    probe.armed.store(SlowProbe::SLOW, Ordering::SeqCst);
    assert!(supervisor.step().collector_running);
    assert!(probe.measured.load(Ordering::SeqCst), "the probe ran in that step");
    rig.clock.advance(3295);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    // As before the drive grace: the gap ends at the step's own reading.
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.gaps, vec![Gap { start: T0, end: T0 + 300 }]);
}

/// A time probe that, when armed, takes 5.8 s: from .100 to .900 five
/// whole seconds later, which whole-second readings count as 5.
struct SubsecondProbe {
    clock: Arc<EdgeClock>,
    armed: AtomicBool,
    measured: AtomicBool,
}

impl OffsetProbe for SubsecondProbe {
    fn offset_seconds(&self) -> Result<f64, Refusal> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.base.advance(5);
            self.clock.millis.store(900, Ordering::SeqCst);
            self.measured.store(true, Ordering::SeqCst);
        }
        Ok(0.2)
    }
}

#[test]
fn wge_fix_5_probe_past_its_bound_by_a_fraction_ends_the_grace() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let clock = EdgeClock::on(&rig);
    let probe = Arc::new(SubsecondProbe {
        clock: clock.clone(),
        armed: AtomicBool::new(false),
        measured: AtomicBool::new(false),
    });
    let hub = edge_hub(&rig, clock, probe.clone(), drive.clone());
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    supervisor.step();
    rig.clock.advance(3600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    probe.armed.store(true, Ordering::SeqCst);
    let mut report = None;
    for _ in 0..200 {
        rig.clock.advance(5);
        let step = supervisor.step();
        if probe.measured.load(Ordering::SeqCst) {
            report = Some(step);
            break;
        }
        assert_eq!(step.phase, Phase::Open);
    }
    let report = report.expect("the time probe never ran");
    // Still driving, time left in the grace: the late answer alone ends it.
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!permitted(&hub));
}

/// The rig's clock read to the microsecond, with millisecond readings
/// truncated as the production clocks truncate theirs.
#[derive(Debug)]
struct MicroClock {
    base: Arc<ManualClock>,
    micros: AtomicU64,
}

impl WindowClock for MicroClock {
    fn wall(&self) -> i64 {
        self.base.wall()
    }
    fn mono(&self) -> u64 {
        self.base.mono()
    }
    fn mono_millis(&self) -> u64 {
        self.base.mono() * 1000 + self.micros.load(Ordering::SeqCst) / 1000
    }
    fn mono_boot_scoped(&self) -> bool {
        self.base.mono_boot_scoped()
    }
    fn boot_time(&self) -> Result<i64, Refusal> {
        self.base.boot_time()
    }
}

/// A time probe that, when armed, runs from .0001 s to .0008 s five whole
/// seconds later: 5.0007 s, which truncated readings measure as 5,000 ms.
struct TruncatedProbe {
    clock: Arc<MicroClock>,
    armed: AtomicBool,
    measured: AtomicBool,
}

impl OffsetProbe for TruncatedProbe {
    fn offset_seconds(&self) -> Result<f64, Refusal> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.base.advance(5);
            self.clock.micros.store(800, Ordering::SeqCst);
            self.measured.store(true, Ordering::SeqCst);
        }
        Ok(0.2)
    }
}

#[test]
fn wge_r2_orig5_probe_past_its_bound_under_truncated_readings_ends_the_grace() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let clock = Arc::new(MicroClock {
        base: rig.clock.clone(),
        micros: AtomicU64::new(100),
    });
    let probe = Arc::new(TruncatedProbe {
        clock: clock.clone(),
        armed: AtomicBool::new(false),
        measured: AtomicBool::new(false),
    });
    let hub = edge_hub(&rig, clock.clone(), probe.clone(), drive.clone());
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    supervisor.step();
    rig.clock.advance(3600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    probe.armed.store(true, Ordering::SeqCst);
    let mut report = None;
    for _ in 0..200 {
        clock.micros.store(100, Ordering::SeqCst);
        rig.clock.advance(5);
        let step = supervisor.step();
        if probe.measured.load(Ordering::SeqCst) {
            report = Some(step);
            break;
        }
        assert_eq!(step.phase, Phase::Open);
    }
    let report = report.expect("the time probe never ran");
    // Still driving, time left in the grace: a probe 0.7 ms past its bound
    // ends it, although both truncated readings are exactly 5,000 ms apart.
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(!permitted(&hub));
}

#[test]
fn wge_r2_new1_held_stream_sees_the_cut_only_once_the_hard_end_is_durable() {
    use std::{cell::RefCell, rc::Rc, sync::mpsc, time::Duration};

    use crate::{
        api::fleet_api::egress::{ask, test_support::route_on},
        collection::window::gate::before_grace_end_write,
    };

    let rig = Rig::new();
    let drive = Drive::new(driving());
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    let held_clock = Arc::new(ManualClock::new(rig.clock.wall()));
    let endpoint = "https://wge-r2-held-stream.test:9443/";
    let _route = route_on(endpoint, hub.gate().clone(), held_clock);
    let held = Arc::new(ask(endpoint, Provider::Legacy, PermitKind::Stream).expect("permitted in the grace"));
    assert!(held.check().is_ok());
    let journal = rig.window_dir().join("drive-grace.json");
    let (tx, rx) = mpsc::channel();
    let rx = Rc::new(rx);
    let early = Rc::new(RefCell::new(None));
    let (witness, seen, answers) = (held.clone(), early.clone(), rx.clone());
    before_grace_end_write(Some(Box::new(move || {
        // The held stream checks before its next Ping, on its own thread,
        // while the end is being written.
        let (witness, journal, tx) = (witness.clone(), journal.clone(), tx.clone());
        std::thread::spawn(move || {
            let sends = witness.check().is_ok();
            let ended: serde_json::Value =
                serde_json::from_slice(&fs::read(&journal).unwrap_or_default()).unwrap_or_default();
            let _ = tx.send((sends, ended["ended_at"].clone()));
        });
        *seen.borrow_mut() = Some(answers.recv_timeout(Duration::from_millis(300)).ok());
    })));
    let latched_at = rig.clock.wall() + 2000;
    rig.clock.shift_wall(2000);
    assert!(!permitted(&hub));
    rig.clock.shift_wall(-2000);
    before_grace_end_write(None);
    assert_eq!(
        early.borrow_mut().take(),
        Some(None),
        "the held stream saw the cut before the hard end was durable"
    );
    let (sends, ended_at) = rx.recv_timeout(Duration::from_secs(10)).expect("the held check returned");
    assert!(!sends, "the held stream could send after the hard end");
    assert_eq!(ended_at, serde_json::json!(latched_at));
}

#[test]
fn wge_r2_new2_corrected_latch_leaves_an_in_flight_exchange_running() {
    let rig = Rig::new();
    let drive = Drive::new(driving());
    let clock = EdgeClock::on(&rig);
    let hub = edge_hub(&rig, clock.clone(), rig.probe.clone(), drive.clone());
    hub.open(&grace_grant(&rig, 3600, 1800)).unwrap();
    let mut supervisor = Supervisor::new(hub.clone(), FakeSlot::default());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let exchange = hub.gate().permit(Provider::Legacy, PermitKind::TokenExchange).unwrap();
    // Another permit reads a brief wall step past the hard end and latches.
    let held = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    held.deadline_seen(rig.clock.wall() + 3601, rig.clock.mono());
    assert_eq!(hub.gate().expiry_observed(), Some(T0 + 5401));
    // The step's own reading still sees the step; the wall is corrected
    // before the controller reads it.
    clock.once.store(3601, Ordering::SeqCst);
    let report = supervisor.step();
    assert_eq!(hub.status().closing_at, Some(T0 + 1800));
    // The exchange keeps its grace from the corrected close.
    assert_eq!(report.phase, Phase::Closing);
    assert_eq!(report.closed, None);
    assert!(!exchange.cancel_token().is_cancelled());
    assert!(hub.gate().exchange_in_flight());
    drop(exchange);
    drop(held);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 1800);
}
