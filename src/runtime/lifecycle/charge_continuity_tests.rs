// SPDX-License-Identifier: AGPL-3.0-only

//! DESIGN-CHARGE-CONTINUITY section 3, cases C01 to C26, C28, C29 and C30:
//! every charge record takes its energy baseline and its session-fee slot
//! from the session ledger, inside a window and across a hand-back alike.
//! Synthetic data only. "A" is the window before the hand-back and "B" the
//! next one; counters are kWh, odometers miles as Tesla reports them. Each case
//! asserts `(charge_energy_added, continues_plug_in)` for every row, in close
//! order.

use serde_json::{Map, json};

use super::charge_tests::{HOUR, MIN, T0, cv, drive_obs};
use super::*;
use crate::tesla_stream::StreamUpdate;

/// The parked odometer of every case, in miles.
const ODO: f64 = 1000.0;

/// Kilometres as miles, for odometer offsets stated in km.
fn mi(km: f64) -> f64 {
    km / K_MILE_KM
}

/// A Fleet `vehicle_data` with the sparse marker (a coarse odometer source),
/// shaped as `cv`: a charge group, and a vehicle group when an odometer is
/// given.
fn sp(
    id: i64,
    t: i64,
    state: &str,
    energy: Option<f64>,
    soc: Option<i64>,
    odometer_mi: Option<f64>,
) -> LifecycleSample {
    let mut charge = Map::new();
    charge.insert("timestamp".to_owned(), json!(t));
    charge.insert("charging_state".to_owned(), json!(state));
    if let Some(energy) = energy {
        charge.insert("charge_energy_added".to_owned(), json!(energy));
    }
    if let Some(soc) = soc {
        charge.insert("battery_level".to_owned(), json!(soc));
    }
    let mut data = Map::new();
    data.insert("charge_state".to_owned(), Value::Object(charge));
    if let Some(odometer) = odometer_mi {
        data.insert(
            "vehicle_state".to_owned(),
            json!({"timestamp": t, "odometer": odometer}),
        );
    }
    LifecycleSample {
        observation_id: id,
        observed_at_ms: t,
        vehicle_state: "online".to_owned(),
        payload: json!({
            "record_type": "fleet_api_vehicle_data_v1",
            "source_vehicle_id": "9",
            "source_vehicle_state": "online",
            "provider_feed": "provider_stream",
            "provider_raw_json": {"response": Value::Object(data)},
        }),
    }
}

/// The hand-back event a window end stores (M03-T20).
fn handback(id: i64, t: i64) -> LifecycleSample {
    LifecycleSample {
        observation_id: id,
        observed_at_ms: t,
        vehicle_state: "unknown".to_owned(),
        payload: json!({"record_type": "hub_lifecycle_event_v1", "event": "handback"}),
    }
}

/// A legacy stream frame that carries only a battery level (no odometer, no
/// gear), built the way production builds it.
fn battery_frame(id: i64, t: i64, soc: i64) -> LifecycleSample {
    let update = StreamUpdate {
        tag: "9".to_owned(),
        timestamp_ms: t,
        speed: None,
        odometer: None,
        soc: Some(soc as f64),
        elevation: None,
        est_heading: None,
        est_lat: None,
        est_lng: None,
        power: None,
        shift_state: None,
        range: None,
        est_range: None,
        heading: None,
    };
    LifecycleSample {
        observation_id: id,
        observed_at_ms: t,
        vehicle_state: "online".to_owned(),
        payload: stream_observation_payload(&update),
    }
}

type Rows = Vec<(Option<f64>, Option<bool>)>;

fn rows_of(delta: &LifecycleDelta) -> Rows {
    delta
        .charges
        .iter()
        .map(|charge| (charge.charge_energy_added, charge.continues_plug_in))
        .collect()
}

fn run_window(state: OpenSessionState, samples: &[LifecycleSample]) -> LifecycleStep {
    apply_samples(state, 1, samples).expect("apply window")
}

/// A Hub restart between windows: the state goes through its blob.
fn reload(state: &OpenSessionState) -> OpenSessionState {
    OpenSessionState::decode(&state.encode().expect("encode")).expect("decode")
}

/// An upgrade: the blob of a build without the ledger (`charge_ledger` key
/// absent), with an optional change to its parked snapshot.
fn without_ledger(
    state: &OpenSessionState,
    change: impl FnOnce(&mut ParkedSnapshot),
) -> OpenSessionState {
    let mut state = state.clone();
    if let Some(snapshot) = state.parked_snapshot.as_mut() {
        change(snapshot);
    }
    let mut blob = serde_json::to_value(&state).expect("blob value");
    blob.as_object_mut()
        .expect("blob object")
        .remove("charge_ledger");
    let old = OpenSessionState::decode(&serde_json::to_vec(&blob).expect("blob bytes"))
        .expect("old blob decodes");
    assert_eq!(old.charge_ledger, None, "an old blob is unseeded");
    old
}

/// Window A, then window B, with a Hub restart between them when `restart`.
fn two_windows(a: &[LifecycleSample], b: &[LifecycleSample], restart: bool) -> Rows {
    let first = run_window(OpenSessionState::new(), a);
    let state = if restart {
        reload(&first.state)
    } else {
        first.state
    };
    let second = run_window(state, b);
    let mut rows = rows_of(&first.delta);
    rows.extend(rows_of(&second.delta));
    rows
}

/// Window A of C01: a charge open at the hand-back on a coarse odometer, peak
/// 0.64.
fn c01_a() -> Vec<LifecycleSample> {
    vec![
        sp(1, T0, "Charging", Some(0.30), Some(50), Some(ODO)),
        sp(2, T0 + 2 * MIN, "Charging", Some(0.64), Some(51), Some(ODO)),
        handback(3, T0 + 3 * MIN),
    ]
}

/// A window B that charges from `first` to 9.56 at `odometer_mi` on a full
/// source.
fn b_to_956(first_id: i64, first: f64, odometer_mi: f64) -> Vec<LifecycleSample> {
    let t = T0 + 5 * HOUR;
    vec![
        cv(
            first_id,
            t,
            "Charging",
            Some(first),
            Some(52),
            Some(odometer_mi),
        ),
        cv(
            first_id + 1,
            t + 5 * MIN,
            "Charging",
            Some(5.0),
            Some(70),
            Some(odometer_mi),
        ),
        cv(
            first_id + 2,
            t + 9 * MIN,
            "Complete",
            Some(9.56),
            Some(90),
            Some(odometer_mi),
        ),
    ]
}

/// Window B of C01: the first full reading 0.11 km above A's coarse one.
fn c01_b() -> Vec<LifecycleSample> {
    b_to_956(4, 0.66, ODO + mi(0.11))
}

/// A window A on a full source: a charge open at the hand-back, counters as
/// given.
fn full_a(counters: &[Option<f64>]) -> Vec<LifecycleSample> {
    let mut samples = counters
        .iter()
        .enumerate()
        .map(|(index, counter)| {
            let index = index as i64;
            cv(
                index + 1,
                T0 + index * MIN,
                "Charging",
                *counter,
                Some(50),
                Some(ODO),
            )
        })
        .collect::<Vec<_>>();
    let next = samples.len() as i64 + 1;
    samples.push(handback(next, T0 + next * MIN));
    samples
}

#[test]
fn charge_continuity_c01_window_22_continues_across_a_cross_class_odometer() {
    let rows = two_windows(&c01_a(), &c01_b(), false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

/// A full-source charge in A that closes at Complete 0.64, then a drive at the
/// same odometer, open at the hand-back.
fn c02_a() -> Vec<LifecycleSample> {
    vec![
        cv(1, T0, "Charging", Some(0.30), Some(50), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Complete", Some(0.64), Some(51), Some(ODO)),
        drive_obs(3, T0 + HOUR, json!("D"), json!(30), 10.0, ODO, 51, 0.64),
        handback(4, T0 + HOUR + MIN),
    ]
}

#[test]
fn charge_continuity_c02_a_drive_ends_the_plug_in() {
    let rows = two_windows(&c02_a(), &b_to_956(5, 0.66, ODO), false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c03_a_drive_ends_the_plug_in_across_a_hub_restart() {
    let first = run_window(OpenSessionState::new(), &c02_a());
    let reloaded = reload(&first.state);
    assert_eq!(
        reloaded.charge_ledger.map(|ledger| ledger.live),
        Some(false)
    );
    let rows = two_windows(&c02_a(), &b_to_956(5, 0.66, ODO), true);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c04_disconnected_in_b_ends_the_plug_in() {
    let mut b = vec![cv(
        4,
        T0 + 4 * HOUR,
        "Disconnected",
        Some(0.64),
        Some(51),
        Some(ODO),
    )];
    b.extend(b_to_956(5, 0.66, ODO));
    let rows = two_windows(&full_a(&[Some(0.30), Some(0.64)]), &b, false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c05_disconnected_in_a_ends_the_plug_in() {
    let a = vec![
        cv(1, T0, "Charging", Some(0.30), Some(50), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Complete", Some(0.64), Some(51), Some(ODO)),
        cv(
            3,
            T0 + HOUR,
            "Disconnected",
            Some(0.64),
            Some(51),
            Some(ODO),
        ),
        handback(4, T0 + 2 * HOUR),
    ];
    let rows = two_windows(&a, &b_to_956(5, 0.66, ODO), false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c06_a_counter_reset_between_windows_is_a_new_session() {
    let rows = two_windows(
        &full_a(&[Some(1.0), Some(5.0)]),
        &b_to_956(4, 0.66, ODO),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c07_a_known_zero_row_keeps_the_fee_and_b_continues() {
    let rows = two_windows(&full_a(&[Some(0.0)]), &b_to_956(3, 0.66, ODO), true);
    assert_eq!(
        rows,
        vec![(Some(0.0), Some(false)), (Some(9.56), Some(true))]
    );
}

#[test]
fn charge_continuity_c08_a_mixed_source_odometer_below_the_mixed_tolerance_continues() {
    let rows = two_windows(&c01_a(), &b_to_956(4, 0.66, ODO + mi(0.5)), false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

#[test]
fn charge_continuity_c09_the_peak_not_the_last_reading_is_accounted() {
    let first = run_window(
        OpenSessionState::new(),
        &full_a(&[Some(0.30), Some(0.64), Some(0.63)]),
    );
    let ledger = first.state.charge_ledger.expect("ledger");
    assert_eq!(ledger.accounted_kwh, Some(0.64));
    assert_eq!(ledger.last_kwh, Some(0.63));
    let rows = two_windows(
        &full_a(&[Some(0.30), Some(0.64), Some(0.63)]),
        &b_to_956(5, 0.66, ODO),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

#[test]
fn charge_continuity_c10_a_dip_at_resume_adds_nothing() {
    let rows = two_windows(
        &full_a(&[Some(0.30), Some(0.64)]),
        &b_to_956(4, 0.63, ODO),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

#[test]
fn charge_continuity_c11_a_dip_in_a_stopped_reading_is_no_break() {
    let mut b = vec![cv(
        4,
        T0 + 4 * HOUR,
        "Stopped",
        Some(0.63),
        Some(51),
        Some(ODO),
    )];
    b.extend(b_to_956(5, 0.66, ODO));
    let rows = two_windows(&full_a(&[Some(0.30), Some(0.64)]), &b, false);
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

/// Window B whose first Charging sample has no counter, then `first` and on to
/// 9.56.
fn b_without_first_counter(first: f64) -> Vec<LifecycleSample> {
    let mut b = vec![cv(
        4,
        T0 + 5 * HOUR - MIN,
        "Charging",
        None,
        Some(52),
        Some(ODO),
    )];
    b.extend(b_to_956(5, first, ODO));
    b
}

#[test]
fn charge_continuity_c12_an_unknown_first_counter_waits_for_the_first_usable_one() {
    let rows = two_windows(
        &full_a(&[Some(0.30), Some(0.64)]),
        &b_without_first_counter(0.66),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

#[test]
fn charge_continuity_c13_a_first_usable_counter_that_restarted_takes_the_fee() {
    let rows = two_windows(
        &full_a(&[Some(0.30), Some(0.64)]),
        &b_without_first_counter(0.30),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c14_a_coarse_drive_between_windows_ends_the_plug_in() {
    let moved = ODO + 10.0;
    let t = T0 + 5 * HOUR;
    let b = vec![
        cv(4, t, "Stopped", None, Some(52), None),
        sp(5, t + MIN, "Charging", Some(6.0), Some(53), Some(moved)),
        sp(6, t + 5 * MIN, "Charging", Some(8.0), Some(70), Some(moved)),
        sp(
            7,
            t + 9 * MIN,
            "Complete",
            Some(9.56),
            Some(90),
            Some(moved),
        ),
    ];
    let rows = two_windows(&full_a(&[Some(1.0), Some(5.0)]), &b, false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c15_a_fine_movement_ends_the_plug_in() {
    let rows = two_windows(
        &full_a(&[Some(1.0), Some(5.0)]),
        &b_to_956(4, 6.0, ODO + mi(0.2)),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_c16_a_fine_reading_below_the_tolerance_continues() {
    let rows = two_windows(
        &full_a(&[Some(1.0), Some(5.0)]),
        &b_to_956(4, 6.0, ODO + mi(0.05)),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(4.56), Some(true))]
    );
}

/// Window A of C17: a charge that closes at Complete 5.00 well before an idle
/// hand-back.
fn c17_a() -> Vec<LifecycleSample> {
    vec![
        cv(1, T0, "Charging", Some(1.0), Some(50), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Complete", Some(5.0), Some(60), Some(ODO)),
        handback(3, T0 + HOUR),
    ]
}

#[test]
fn charge_continuity_c17_a_top_up_after_complete_across_the_hand_back_continues() {
    let rows = two_windows(&c17_a(), &b_to_956(4, 6.0, ODO), false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(4.56), Some(true))]
    );
}

#[test]
fn charge_continuity_c18_a_recovered_gap_charge_and_its_top_up_continue() {
    let a = vec![
        cv(1, T0, "Charging", Some(1.0), Some(45), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Charging", Some(2.0), Some(50), Some(ODO)),
        handback(3, T0 + 3 * MIN),
    ];
    let t = T0 + 5 * HOUR;
    let b = vec![
        cv(4, t, "Complete", Some(3.0), Some(49), Some(ODO)),
        battery_frame(5, t + MIN, 51),
        cv(6, t + HOUR, "Charging", Some(3.10), Some(52), Some(ODO)),
        cv(7, t + 2 * HOUR, "Complete", Some(5.0), Some(60), Some(ODO)),
    ];
    let rows = two_windows(&a, &b, false);
    assert_eq!(
        rows,
        vec![
            (Some(2.0), Some(false)),
            (Some(1.0), Some(true)),
            (Some(2.0), Some(true)),
        ]
    );
}

#[test]
fn charge_continuity_c19_an_after_gap_complete_continues_the_plug_in() {
    let a = vec![
        cv(1, T0, "Charging", Some(1.0), Some(40), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Charging", Some(5.0), Some(50), Some(ODO)),
        handback(3, T0 + 3 * MIN),
    ];
    let b = vec![cv(
        4,
        T0 + 5 * HOUR,
        "Complete",
        Some(9.56),
        Some(80),
        Some(ODO),
    )];
    let rows = two_windows(&a, &b, false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(4.56), Some(true))]
    );
}

/// Window A of C20 and C21: 0.66 to 3.00, then a provisional fall to 0.10
/// still pending at the hand-back.
fn c20_a() -> Vec<LifecycleSample> {
    full_a(&[Some(0.66), Some(3.0), Some(0.10)])
}

#[test]
fn charge_continuity_c20_a_pending_reset_corroborated_in_b_is_a_new_session() {
    let first = run_window(OpenSessionState::new(), &c20_a());
    let ledger = first.state.charge_ledger.expect("ledger");
    assert_eq!(
        (
            ledger.accounted_kwh,
            ledger.last_kwh,
            ledger.pending_fall_kwh
        ),
        (Some(3.0), Some(3.0), Some(0.10))
    );
    let t = T0 + 5 * HOUR;
    let b = vec![
        cv(5, t, "Charging", Some(0.20), Some(52), Some(ODO)),
        cv(6, t + 9 * MIN, "Complete", Some(2.0), Some(60), Some(ODO)),
    ];
    let rows = two_windows(&c20_a(), &b, false);
    assert_eq!(rows, vec![(None, Some(false)), (Some(2.0), Some(false))]);
}

#[test]
fn charge_continuity_c21_a_pending_reset_contradicted_in_b_continues() {
    let t = T0 + 5 * HOUR;
    let b = vec![
        cv(5, t, "Charging", Some(3.05), Some(52), Some(ODO)),
        cv(6, t + 9 * MIN, "Complete", Some(5.0), Some(60), Some(ODO)),
    ];
    let rows = two_windows(&c20_a(), &b, false);
    assert_eq!(rows, vec![(None, Some(false)), (Some(2.0), Some(true))]);
}

#[test]
fn charge_continuity_c22_a_reset_inside_one_record_closes_and_reopens_with_the_fee() {
    // The first record starts on an empty ledger, so it counts its whole
    // counter (3.00); the reopened one starts its own Tesla session.
    let samples = [
        cv(1, T0, "Charging", Some(0.66), Some(50), Some(ODO)),
        cv(2, T0 + MIN, "Charging", Some(3.0), Some(55), Some(ODO)),
        cv(3, T0 + 2 * MIN, "Charging", Some(0.10), Some(55), Some(ODO)),
        cv(4, T0 + 3 * MIN, "Charging", Some(0.20), Some(56), Some(ODO)),
        cv(5, T0 + 9 * MIN, "Complete", Some(2.0), Some(60), Some(ODO)),
    ];
    let result = run_window(OpenSessionState::new(), &samples);
    assert_eq!(
        rows_of(&result.delta),
        vec![(Some(3.0), Some(false)), (Some(2.0), Some(false))]
    );
}

#[test]
fn charge_continuity_c23_an_earlier_row_without_a_counter_leaves_nothing_to_subtract() {
    let rows = two_windows(&full_a(&[None, None]), &b_to_956(4, 0.66, ODO), false);
    assert_eq!(rows, vec![(None, Some(false)), (Some(9.56), Some(true))]);
}

#[test]
fn charge_continuity_c24_a_hub_restart_between_windows_changes_nothing() {
    let cases: [(Vec<LifecycleSample>, Vec<LifecycleSample>); 3] = [
        (c01_a(), c01_b()),
        (full_a(&[Some(0.0)]), b_to_956(3, 0.66, ODO)),
        (c17_a(), b_to_956(4, 6.0, ODO)),
    ];
    for (index, (a, b)) in cases.iter().enumerate() {
        let straight = two_windows(a, b, false);
        let restarted = two_windows(a, b, true);
        assert_eq!(restarted, straight, "case {index}");
        assert_eq!(straight.len(), 2, "case {index}");
        assert_eq!(straight[1].1, Some(true), "case {index}");
    }
}

#[test]
fn charge_continuity_c25_an_upgrade_seeds_the_ledger_from_a_charge_close_snapshot() {
    let first = run_window(OpenSessionState::new(), &c01_a());
    let old = without_ledger(&first.state, |_| {});
    let second = run_window(old, &c01_b());
    let ledger = second.state.charge_ledger.expect("seeded");
    assert!(ledger.live);
    let mut rows = rows_of(&first.delta);
    rows.extend(rows_of(&second.delta));
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

#[test]
fn charge_continuity_c26_an_upgrade_without_a_charge_close_snapshot_starts_empty() {
    let first = run_window(OpenSessionState::new(), &c01_a());
    let changes: [fn(&mut ParkedSnapshot); 3] = [
        |snapshot| snapshot.after_drive = true,
        |snapshot| snapshot.unplugged = true,
        |snapshot| snapshot.energy_added_kwh = None,
    ];
    for (index, change) in changes.into_iter().enumerate() {
        let old = without_ledger(&first.state, change);
        let second = run_window(old, &c01_b());
        assert_eq!(
            rows_of(&second.delta),
            vec![(Some(9.56), Some(false))],
            "variant {index}"
        );
    }
}

#[test]
fn charge_continuity_c28_no_power_readings_alone_open_no_plug_in() {
    let samples = [
        cv(1, T0, "NoPower", Some(0.0), Some(50), Some(ODO)),
        cv(2, T0 + 10 * MIN, "NoPower", Some(0.0), Some(50), Some(ODO)),
        cv(3, T0 + HOUR, "Charging", Some(0.0), Some(50), Some(ODO)),
        cv(
            4,
            T0 + HOUR + 5 * MIN,
            "Charging",
            Some(5.0),
            Some(70),
            Some(ODO),
        ),
        cv(
            5,
            T0 + HOUR + 9 * MIN,
            "Complete",
            Some(9.56),
            Some(90),
            Some(ODO),
        ),
    ];
    let result = run_window(OpenSessionState::new(), &samples);
    assert_eq!(rows_of(&result.delta), vec![(Some(9.56), Some(false))]);
}

#[test]
fn charge_continuity_c29_a_parked_counter_reset_ends_the_plug_in() {
    let samples = [
        cv(1, T0, "Charging", Some(1.0), Some(50), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Complete", Some(5.0), Some(60), Some(ODO)),
        cv(3, T0 + HOUR, "NoPower", Some(0.0), Some(60), Some(ODO)),
        cv(4, T0 + 2 * HOUR, "Charging", Some(0.5), Some(60), Some(ODO)),
        cv(
            5,
            T0 + 2 * HOUR + 5 * MIN,
            "Charging",
            Some(2.0),
            Some(65),
            Some(ODO),
        ),
        cv(
            6,
            T0 + 2 * HOUR + 9 * MIN,
            "Complete",
            Some(3.0),
            Some(70),
            Some(ODO),
        ),
    ];
    let result = run_window(OpenSessionState::new(), &samples);
    assert_eq!(
        rows_of(&result.delta),
        vec![(Some(5.0), Some(false)), (Some(3.0), Some(false))]
    );
}

#[test]
fn charge_continuity_c30_a_same_site_replug_without_a_restart_reads_as_a_continuation() {
    // Undecidable (design 5): no evidence tells this replug from C01, so the
    // rule continues the plug-in; B undercounts by A's 0.64 and skips its fee.
    let rows = two_windows(
        &full_a(&[Some(0.30), Some(0.64)]),
        &b_to_956(4, 0.66, ODO),
        false,
    );
    assert_eq!(
        rows,
        vec![(Some(0.64), Some(false)), (Some(8.92), Some(true))]
    );
}

// REV-CX-CC repair round (CC-FIX): one test per P2 finding, named after it.

/// The same observation with its vehicle group (the odometer) stamped at
/// `vehicle_ms`: a cached group Tesla re-serves with an older time.
fn with_vehicle_at(mut sample: LifecycleSample, vehicle_ms: i64) -> LifecycleSample {
    sample.payload["provider_raw_json"]["response"]["vehicle_state"]["timestamp"] =
        json!(vehicle_ms);
    sample
}

#[test]
fn charge_continuity_rev_cx_cc_p2_2_a_delayed_recovery_after_disconnected_continues() {
    // C18 with B's first reading `Disconnected`: the recovery that the next
    // battery frame corroborates still continues A's plug-in (1.00, no fee),
    // and the removed cable then leaves the ledger empty.
    let a = vec![
        cv(1, T0, "Charging", Some(1.0), Some(45), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Charging", Some(2.0), Some(50), Some(ODO)),
        handback(3, T0 + 3 * MIN),
    ];
    let t = T0 + 5 * HOUR;
    let b = vec![
        cv(4, t, "Disconnected", Some(3.0), Some(49), Some(ODO)),
        battery_frame(5, t + MIN, 51),
    ];
    let first = run_window(OpenSessionState::new(), &a);
    let second = run_window(first.state, &b);
    let mut rows = rows_of(&first.delta);
    rows.extend(rows_of(&second.delta));
    assert_eq!(
        rows,
        vec![(Some(2.0), Some(false)), (Some(1.0), Some(true))]
    );
    assert_eq!(
        second.state.charge_ledger.map(|ledger| ledger.live),
        Some(false),
        "the cable was removed"
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_2_a_discarded_disconnected_gap_charge_still_unplugs() {
    // The retained `Disconnected` sample is not corroborated (the next battery
    // reading is lower): no row, and the plug-in is over all the same.
    let a = vec![
        cv(1, T0, "Charging", Some(1.0), Some(45), Some(ODO)),
        cv(2, T0 + 2 * MIN, "Charging", Some(2.0), Some(50), Some(ODO)),
        handback(3, T0 + 3 * MIN),
    ];
    let t = T0 + 4 * HOUR;
    let mut b = vec![
        cv(4, t, "Disconnected", Some(3.0), Some(49), Some(ODO)),
        battery_frame(5, t + MIN, 48),
    ];
    b.extend(b_to_956(6, 0.66, ODO));
    let rows = two_windows(&a, &b, false);
    assert_eq!(
        rows,
        vec![(Some(2.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_3_an_open_charge_keeps_the_latest_odometer_of_each_class() {
    // A: coarse 1,000.00 km, a full 1,000.05 km while charging, coarse again
    // before the hand-back. B's full reading 0.20 km above A's latest full one
    // is a same-class move: a new plug-in, whole counter and the fee.
    let a = vec![
        sp(1, T0, "Charging", Some(1.0), Some(50), Some(ODO)),
        cv(2, T0 + MIN, "Charging", Some(3.0), Some(55), Some(ODO + mi(0.05))),
        sp(3, T0 + 2 * MIN, "Charging", Some(5.0), Some(60), Some(ODO)),
        handback(4, T0 + 3 * MIN),
    ];
    let rows = two_windows(&a, &b_to_956(5, 6.0, ODO + mi(0.25)), false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_3_a_stale_odometer_at_the_open_is_not_kept() {
    // The charge opens on a fresh charge group whose vehicle group is an older
    // cached one (0.20 km below the parked reading) and closes without a fresh
    // reading. The next fresh reading at the parked odometer is no movement,
    // so the top-up continues the plug-in.
    let samples = [
        cv(1, T0, "Stopped", None, Some(50), Some(ODO)),
        with_vehicle_at(
            cv(2, T0 + HOUR, "Charging", Some(1.0), Some(50), Some(ODO - mi(0.2))),
            T0 - HOUR,
        ),
        cv(3, T0 + HOUR + 5 * MIN, "Complete", Some(5.0), Some(60), None),
        cv(4, T0 + 2 * HOUR, "Charging", Some(6.0), Some(61), Some(ODO)),
        cv(
            5,
            T0 + 2 * HOUR + 9 * MIN,
            "Complete",
            Some(9.56),
            Some(90),
            Some(ODO),
        ),
    ];
    let result = run_window(OpenSessionState::new(), &samples);
    assert_eq!(
        rows_of(&result.delta),
        vec![(Some(5.0), Some(false)), (Some(4.56), Some(true))]
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_4_a_late_first_odometer_breaks_a_tentative_continuation() {
    // C12 with C15: B's first Charging sample has neither counter nor
    // odometer; its next one brings 6.00 and a full reading 0.20 km above A's.
    let t = T0 + 5 * HOUR;
    let mut b = vec![cv(4, t - MIN, "Charging", None, Some(52), None)];
    b.extend(b_to_956(5, 6.0, ODO + mi(0.2)));
    let rows = two_windows(&full_a(&[Some(1.0), Some(5.0)]), &b, false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(9.56), Some(false))]
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_4_a_late_first_odometer_that_did_not_move_continues() {
    // The counterpart: the late first reading is within the tolerance.
    let t = T0 + 5 * HOUR;
    let mut b = vec![cv(4, t - MIN, "Charging", None, Some(52), None)];
    b.extend(b_to_956(5, 6.0, ODO + mi(0.05)));
    let rows = two_windows(&full_a(&[Some(1.0), Some(5.0)]), &b, false);
    assert_eq!(
        rows,
        vec![(Some(5.0), Some(false)), (Some(4.56), Some(true))]
    );
}

#[test]
fn charge_continuity_rev_cx_cc_p2_5_an_idle_pre_upgrade_snapshot_seeds_no_plug_in() {
    // On the old build a newly tracked parked car saw only `NoPower` 0.00: a
    // parked snapshot with a known zero counter, but no charge row ever
    // closed. After the upgrade the first real row takes the fee (C28).
    let idle = run_window(
        OpenSessionState::new(),
        &[cv(1, T0, "NoPower", Some(0.0), Some(50), Some(ODO))],
    );
    assert_eq!(
        idle.state.parked_snapshot.as_ref().and_then(|s| s.energy_added_kwh),
        Some(0.0)
    );
    let t = T0 + HOUR;
    let charge = [
        cv(2, t, "Charging", Some(0.0), Some(50), Some(ODO)),
        cv(3, t + 5 * MIN, "Charging", Some(5.0), Some(70), Some(ODO)),
        cv(4, t + 9 * MIN, "Complete", Some(9.56), Some(90), Some(ODO)),
    ];
    let fresh_cursor = without_ledger(&idle.state, |_| {});
    let mut raised = fresh_cursor.clone();
    raised.raise_id_cursors(&IdFloors::default());
    for (name, old) in [("fresh cursor", fresh_cursor), ("raised cursor", raised)] {
        let after = run_window(old, &charge);
        assert_eq!(
            rows_of(&after.delta),
            vec![(Some(9.56), Some(false))],
            "{name}"
        );
    }
}

#[test]
fn charge_continuity_rev_cx_cc_p2_6_c22_counts_from_zero_on_an_empty_ledger_as_c02_does() {
    // Finding 6 is not taken: the table's 2.34 would make an Empty ledger's
    // first counter a baseline, which design 2.3 (Empty: baseline 0, the whole
    // counter) and C02, C04, C05 and C28 (0.66 to 9.56 records 9.56) exclude.
    // C22's first record is 0.66 to 3.00 from baseline 0, as C02's B is 0.66
    // to 9.56 from baseline 0.
    let c22 = run_window(
        OpenSessionState::new(),
        &[
            cv(1, T0, "Charging", Some(0.66), Some(50), Some(ODO)),
            cv(2, T0 + MIN, "Charging", Some(3.0), Some(55), Some(ODO)),
            cv(3, T0 + 2 * MIN, "Charging", Some(0.10), Some(55), Some(ODO)),
            cv(4, T0 + 3 * MIN, "Charging", Some(0.20), Some(56), Some(ODO)),
            cv(5, T0 + 9 * MIN, "Complete", Some(2.0), Some(60), Some(ODO)),
        ],
    );
    let c02_shape = run_window(OpenSessionState::new(), &b_to_956(1, 0.66, ODO));
    assert_eq!(rows_of(&c02_shape.delta), vec![(Some(9.56), Some(false))]);
    assert_eq!(
        rows_of(&c22.delta),
        vec![(Some(3.0), Some(false)), (Some(2.0), Some(false))]
    );
}
