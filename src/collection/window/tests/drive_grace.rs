// SPDX-License-Identifier: AGPL-3.0-only

//! WINDOW-DRIVE-GRACE: a window whose signed end arrives during a drive stays
//! open until the drive ends, bounded by the grant's signed drive grace.

use std::{
    fs,
    sync::{Arc, Mutex},
};

use ed25519_dalek::SigningKey;

use super::support::*;
use crate::collection::window::{
    CloseReason, DriveActivity, GRANT_GAP_SECONDS, GateRefusal, HandbackSource, HighWater,
    PermitKind, Phase, Provider, Refusal, ResumeOutcome, SignedGrant, Supervisor,
    WindowController,
    grant::{GRANT_FORMAT, GrantBody},
};

/// The collector's current state as the test sets it.
struct Drive(Mutex<DriveActivity>);

impl Drive {
    fn new(activity: DriveActivity) -> Arc<Self> {
        Arc::new(Self(Mutex::new(activity)))
    }

    fn set(&self, activity: DriveActivity) {
        *self.0.lock().unwrap() = activity;
    }
}

impl HandbackSource for Drive {
    fn high_water(&self) -> HighWater {
        HighWater::default()
    }

    fn drive_activity(&self) -> DriveActivity {
        *self.0.lock().unwrap()
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

/// Opens a one-hour window with `grace` and runs it to its signed expiry
/// with a supervisor step 10 s before it and one at it.
fn run_to_expiry(
    rig: &Rig,
    drive: &Arc<Drive>,
    grace: i64,
) -> (Arc<WindowController>, Supervisor<FakeSlot>, FakeSlot) {
    let hub = Arc::new(hub_with(rig, drive));
    hub.open(&grace_grant(rig, 3600, grace)).unwrap();
    let slot = FakeSlot::default();
    let mut supervisor = Supervisor::new(hub.clone(), slot.clone());
    assert!(supervisor.step().collector_running);
    rig.clock.advance(3590);
    assert_eq!(supervisor.step().phase, Phase::Open);
    rig.clock.advance(10);
    (hub, supervisor, slot)
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
    let drive = Drive::new(DriveActivity::Driving);
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
    let drive = Drive::new(DriveActivity::Driving);
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
fn drive_grace_expiry_while_driving_stays_open_and_closes_at_drive_end() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, slot) = run_to_expiry(&rig, &drive, 1800);
    // At the expiry instant, before any step, the gate confirms the drive
    // itself and enters the grace for one confirmation.
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    assert_eq!(stream.time_left().as_secs(), 60);
    let report = supervisor.step();
    assert_eq!(report.phase, Phase::Open);
    assert!(report.collector_running);
    let status = hub.status();
    assert!(status.in_drive_grace);
    assert_eq!(status.drive_grace_until, Some(T0 + 3600 + 1800));
    assert_eq!(status.deadline, Some(T0 + 3600), "the signed expiry is unchanged");
    assert!(ledger_has_grace(&rig));
    assert_eq!(hub.next_wakeup_seconds(), 5);

    // Ten minutes on, each step confirming the drive: the held stream
    // follows the window's deadline.
    for _ in 0..120 {
        rig.clock.advance(5);
        assert_eq!(supervisor.step().phase, Phase::Open);
    }
    assert!(permitted(&hub));
    assert!(!stream.is_cancelled());
    assert_eq!(stream.extension().as_secs(), 600);
    // The drive ends: the normal expiry close and handback follow.
    drive.set(DriveActivity::NotDriving);
    rig.clock.advance(5);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().phase, Phase::Closed);
    assert!(stream.is_cancelled());
    assert!(!permitted(&hub));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.reason, CloseReason::Expired);
    assert_eq!(handback.closed_at, T0 + 3600 + 605);
    assert_eq!(handback.coverage_seconds, 3600 + 605);
    let log = slot.log.lock().unwrap();
    assert!(!log.running);
    assert_eq!(log.released, 1);
}

#[test]
fn drive_grace_closes_at_the_grace_end_while_still_driving() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    assert_eq!(stream.time_left().as_secs(), 60);
    // A step every 30 s: each confirmation moves the deadline on, never past
    // the grace end.
    for _ in 0..19 {
        rig.clock.advance(30);
        assert_eq!(supervisor.step().phase, Phase::Open);
    }
    let last = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    assert_eq!(last.time_left().as_secs(), 30, "cut to the grace end");
    drop(last);
    rig.clock.advance(30);
    assert_eq!(
        hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap_err(),
        GateRefusal::WindowClosed
    );
    assert!(stream.is_cancelled());
    rig.clock.advance(5);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3600 + 600);
    assert_eq!(handback.coverage_seconds, 4200);
}

#[test]
fn drive_grace_not_driving_at_expiry_closes_at_expiry() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::NotDriving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert!(!permitted(&hub));
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
    assert!(!ledger_has_grace(&rig));

    // Driving at the last step before expiry, but the drive ended by it: a
    // permit asked at the expiry, before the next step, rests on nothing
    // older than the expiry and is refused.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    drive.set(DriveActivity::NotDriving);
    assert!(!permitted(&hub), "no grace on the reading before the expiry");
    assert!(hub.gate().drive_grace().is_none());
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);
    assert!(!ledger_has_grace(&rig));
}

#[test]
fn drive_grace_unknown_activity_closes_at_expiry() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Unknown);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert!(!permitted(&hub));
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3600);

    // Unknown during the grace ends it.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    rig.clock.advance(30);
    drive.set(DriveActivity::Unknown);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3630);
}

#[test]
fn drive_grace_missing_confirmation_ends_the_grace() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    let stream = hub.gate().permit(Provider::Legacy, PermitKind::Stream).unwrap();
    rig.clock.advance(30);
    assert_eq!(supervisor.step().phase, Phase::Open);
    // No step for a minute: the car still drives, but the gate stops one
    // confirmation after the last one.
    rig.clock.advance(60);
    assert!(!permitted(&hub));
    assert!(stream.is_cancelled());
    rig.clock.advance(20);
    assert_eq!(supervisor.step().closed, Some(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3630 + 60, "covers what the gate granted");
}

#[test]
fn drive_grace_handback_covers_a_grace_permit_used_before_a_failed_confirmation() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    // The gate enters the grace on a permit asked at the expiry; the drive
    // ends before the next step, whose confirmation fails.
    let permit = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    rig.clock.advance(3);
    drive.set(DriveActivity::NotDriving);
    let report = supervisor.step();
    assert_eq!(report.closed, Some(CloseReason::Expired));
    assert!(permit.is_cancelled());
    assert!(ledger_has_grace(&rig));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3603, "the permit used past the expiry is covered");
    assert_eq!(handback.coverage_seconds, 3603);
}

#[test]
fn drive_grace_restart_in_grace_resumes_and_after_the_grace_end_closes() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
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
    // One confirmation, the one taken at the resume.
    let permit = hub.gate().permit(Provider::Legacy, PermitKind::Request).unwrap();
    assert_eq!(permit.time_left().as_secs(), 60);
    drop(permit);

    // A restart past the last confirmation's deadline closes at start,
    // covering to that deadline.
    drop(hub);
    rig.clock.advance(1800);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    let handback = hub.status().last_handback.unwrap();
    assert_eq!(handback.closed_at, T0 + 3630 + 60);
    assert!(!permitted(&hub));

    // A restart in the grace after the drive ended closes at start.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, _slot) = run_to_expiry(&rig, &drive, 1800);
    assert_eq!(supervisor.step().phase, Phase::Open);
    drop(supervisor);
    drop(hub);
    rig.clock.advance(30);
    drive.set(DriveActivity::NotDriving);
    let (hub, outcome) = rig.start_hub_with(drive.clone());
    assert_eq!(outcome, ResumeOutcome::ClosedAtStart(CloseReason::Expired));
    assert_eq!(hub.status().last_handback.unwrap().closed_at, T0 + 3630);

    // A window that was not in its grace does not resume past the expiry,
    // even while driving.
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
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
    let drive = Drive::new(DriveActivity::Driving);
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
    let drive = Drive::new(DriveActivity::Driving);
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
fn drive_grace_no_failure_backoff_spends_the_grace() {
    let rig = Rig::new();
    let drive = Drive::new(DriveActivity::Driving);
    let (hub, mut supervisor, slot) = run_to_expiry(&rig, &drive, 600);
    assert_eq!(supervisor.step().phase, Phase::Open);
    assert!(hub.status().in_drive_grace);
    // The run exits during the grace, again and again: each step starts a
    // replacement at once instead of waiting out a growing backoff.
    for starts in 2..6 {
        slot.log.lock().unwrap().running = false;
        rig.clock.advance(1);
        let report = supervisor.step();
        assert_eq!(report.phase, Phase::Open);
        assert!(report.collector_running);
        assert_eq!(slot.log.lock().unwrap().starts, starts);
    }
}
