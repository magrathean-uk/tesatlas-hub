// SPDX-License-Identifier: AGPL-3.0-only

//! CC-3 (DESIGN-CHARGE-CONTINUITY, section 4): the charge rule holds through a
//! real hand-back on a real store. Window A is fed through the ordinary intake
//! (`accept_owner_observation_and_lifecycle`), `record_coverage_handback`
//! runs `close_open_sessions_at_handback` in its own transaction, the store is
//! closed and opened again (a Hub restart, C24), and window B is fed through
//! the same intake. Each case asserts the rows the store holds: energy added,
//! session fee and cost, and the stored `continues_plug_in` flag. Fence 3
//! prices every DC reading at 0.40 per kWh with a 2.00 session fee, so a
//! plug-in that continues pays the fee once. Synthetic data only; "A" is the
//! window before the hand-back and "B" the next one.

use rusqlite::params;
use serde_json::{Value, json};

use super::*;
use crate::lifecycle::charge_tests::{HOUR, MIN, T0, at as place, cv};

/// The parked odometer of every case, in miles.
const ODO: f64 = 1000.0;
const CAR: i64 = 1;

/// One observation: its time and its stored payload.
type Obs = (i64, Value);

/// A DC owner reading inside fence 3, shaped as the lifecycle tests' `cv`.
fn reading(id: i64, t: i64, state: &str, energy: Option<f64>, soc: i64, odometer_mi: f64) -> Obs {
    let mut sample = place(
        cv(id, t, state, energy, Some(soc), Some(odometer_mi)),
        10.0005,
        20.0,
    );
    sample.payload["provider_raw_json"]["response"]["charge_state"]["fast_charger_present"] =
        json!(true);
    (t, sample.payload)
}

/// The same reading from the sparse Fleet feed (a coarse odometer source).
fn sparse(id: i64, t: i64, state: &str, energy: f64, soc: i64) -> Obs {
    let (t, mut payload) = reading(id, t, state, Some(energy), soc, ODO);
    payload["record_type"] = json!("fleet_api_vehicle_data_v1");
    payload["provider_feed"] = json!("provider_stream");
    (t, payload)
}

/// A legacy stream frame that carries only a battery level, built the way
/// production builds it.
fn battery_frame(t: i64, soc: i64) -> Obs {
    let update = crate::tesla_stream::StreamUpdate {
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
    (t, crate::lifecycle::stream_observation_payload(&update))
}

/// A window B that charges from `first` to 9.56 at `odometer_mi`.
fn b_to_956(first_id: i64, first: f64, odometer_mi: f64) -> Vec<Obs> {
    let t = T0 + 5 * HOUR;
    vec![
        reading(first_id, t, "Charging", Some(first), 52, odometer_mi),
        reading(first_id + 1, t + 5 * MIN, "Charging", Some(5.0), 70, odometer_mi),
        reading(first_id + 2, t + 9 * MIN, "Complete", Some(9.56), 90, odometer_mi),
    ]
}

/// `(energy added, session fee, cost)` of a stored row.
type Billing = (Option<f64>, Option<f64>, Option<f64>);

struct Lab {
    directory: tempfile::TempDir,
    store: Option<HubStore>,
    source: SourceRecord,
    vehicle: VehicleRecord,
}

impl Lab {
    fn new() -> Self {
        let directory = crate::private_tempdir().expect("tempdir");
        let store = HubStore::initialize(directory.path()).expect("store");
        let source = store
            .register_source(
                &SourceDescriptor::new("tesla_owner_api", "account-test"),
                1_000,
            )
            .expect("source");
        let vehicle = store
            .register_vehicle(&VehicleDescriptor::new(source.source_id, "vehicle-test"), 1_001)
            .expect("vehicle");
        Self {
            directory,
            store: Some(store),
            source,
            vehicle,
        }
    }

    /// A lab whose fence 3 prices a DC charge at 0.40 per kWh plus a 2.00
    /// session fee.
    fn priced() -> Self {
        let lab = Self::new();
        lab.store()
            .upsert_geofences(
                lab.vehicle.vehicle_id,
                &[crate::source_db_projection::SourceDbGeofence {
                    id: 3,
                    name: "F3".to_owned(),
                    latitude: Some(10.0),
                    longitude: Some(20.0),
                    radius_m: Some(150.0),
                    billing_type: Some(crate::hub_pack::GeofenceBillingType::PerKwh),
                    cost_per_unit: Some(0.40),
                    session_fee: Some(2.00),
                }],
            )
            .expect("fence");
        lab
    }

    fn store(&self) -> &HubStore {
        self.store.as_ref().expect("an open store")
    }

    /// A Hub restart: the store is closed, then opened again on the same
    /// folder.
    fn reopen(&mut self) {
        drop(self.store.take());
        self.store = Some(HubStore::initialize(self.directory.path()).expect("reopen"));
    }

    fn open_window(&self, seq: i64, at_ms: i64) {
        assert!(
            record_coverage_open(
                &self.store().open().expect("connection"),
                seq,
                &format!("grant-{seq}"),
                at_ms,
            )
            .expect("window opens"),
            "the store has its coverage table"
        );
    }

    fn feed_vehicle(&self, vehicle: &VehicleRecord, (at_ms, payload): &Obs) {
        self.store()
            .accept_owner_observation_and_lifecycle(
                &ObservationInput {
                    source_id: self.source.source_id,
                    vehicle_id: vehicle.vehicle_id,
                    observed_at_ms: *at_ms,
                    payload: payload.clone(),
                },
                *at_ms,
                CAR,
            )
            .expect("intake");
    }

    fn feed(&self, observations: &[Obs]) {
        for observation in observations {
            self.feed_vehicle(&self.vehicle, observation);
        }
    }

    fn hand_back(&self, seq: i64, opened_at_ms: i64, closed_at_ms: i64) {
        assert!(
            record_coverage_handback(
                &self.store().open().expect("connection"),
                &WindowCoverage {
                    seq,
                    grant_digest: format!("grant-{seq}"),
                    opened_at_ms,
                    closed_at_ms,
                    gaps: Vec::new(),
                },
            )
            .expect("hand-back"),
            "the store has its coverage table"
        );
    }

    fn lifecycle_of(
        &self,
        vehicle: &VehicleRecord,
    ) -> Option<(LifecycleStateRecord, crate::lifecycle::OpenSessionState)> {
        let record = self
            .store()
            .load_lifecycle_state(vehicle.vehicle_id)
            .expect("lifecycle state")?;
        let state = crate::lifecycle::OpenSessionState::decode(&record.open_session_json)
            .expect("decode the blob");
        Some((record, state))
    }

    fn lifecycle(&self) -> (LifecycleStateRecord, crate::lifecycle::OpenSessionState) {
        self.lifecycle_of(&self.vehicle).expect("a lifecycle record")
    }

    /// The ledger the stored blob carries.
    fn ledger(&self) -> Option<crate::lifecycle::ChargeLedger> {
        self.lifecycle().1.charge_ledger
    }

    fn charges_of(&self, vehicle: &VehicleRecord) -> Vec<crate::hub_pack::ProjectionCharge> {
        self.store()
            .materialised_history(vehicle.vehicle_id)
            .expect("history")
            .charges
    }

    fn charges(&self) -> Vec<crate::hub_pack::ProjectionCharge> {
        self.charges_of(&self.vehicle)
    }

    fn billing(&self) -> Vec<Billing> {
        self.charges()
            .iter()
            .map(|charge| (charge.charge_energy_added, charge.session_fee, charge.cost))
            .collect()
    }

    /// The stored `continues_plug_in` of every charge row, in id order.
    fn flags(&self) -> Vec<Option<bool>> {
        let connection = self.store().open().expect("connection");
        let mut statement = connection
            .prepare(
                "SELECT continues_plug_in FROM materialised_charges
                 WHERE vehicle_id = ?1 ORDER BY charge_id",
            )
            .expect("prepare");
        statement
            .query_map([self.vehicle.vehicle_id.to_string()], |row| row.get(0))
            .expect("flags")
            .collect::<Result<_, _>>()
            .expect("flag rows")
    }
}

fn fees(rows: &[Billing]) -> f64 {
    rows.iter().filter_map(|(_, fee, _)| *fee).sum()
}

/// Window A, its hand-back, a store reopen, then window B. Returns the ledger
/// the blob carried at the hand-back, after checking the reopened store holds
/// the very same one (C24).
fn across_a_handback(lab: &mut Lab, a: &[Obs], b: &[Obs]) -> Option<crate::lifecycle::ChargeLedger> {
    let opened = T0 - MIN;
    lab.open_window(1, opened);
    lab.feed(a);
    let closed = a.last().map_or(opened, |(at, _)| *at) + MIN;
    lab.hand_back(1, opened, closed);
    let (_, state) = lab.lifecycle();
    assert!(
        state.open_charge.is_none() && state.open_drive.is_none(),
        "the hand-back leaves nothing open"
    );
    let at_handback = lab.ledger();
    lab.reopen();
    assert_eq!(
        lab.ledger(),
        at_handback,
        "the ledger survives the blob and the reopen unchanged"
    );
    lab.open_window(2, T0 + 5 * HOUR - MIN);
    lab.feed(b);
    at_handback
}

#[test]
fn charge_continuity_store_c01_window_22_continues_through_a_real_handback() {
    let mut lab = Lab::priced();
    let a = vec![
        sparse(1, T0, "Charging", 0.30, 50),
        sparse(2, T0 + 2 * MIN, "Charging", 0.64, 51),
    ];
    let b = b_to_956(3, 0.66, ODO + 0.11 / crate::lifecycle::K_MILE_KM);
    let ledger = across_a_handback(&mut lab, &a, &b).expect("a ledger at the hand-back");
    assert!(ledger.live);
    assert_eq!(ledger.accounted_kwh, Some(0.64));
    assert_eq!(ledger.last_kwh, Some(0.64));
    assert!(ledger.odo_coarse_km.is_some(), "a coarse odometer was last seen");
    // The hand-back closed A at its newest sample, not at the hand-back.
    assert_eq!(lab.charges()[0].end_date_ms, Some(T0 + 2 * MIN));
    let billing = lab.billing();
    assert_eq!(
        billing,
        vec![
            (Some(0.64), Some(2.00), Some(2.26)),
            (Some(8.92), Some(0.00), Some(3.57)),
        ]
    );
    assert_eq!(fees(&billing), 2.00);
    assert_eq!(lab.flags(), vec![Some(false), Some(true)]);
}

#[test]
fn charge_continuity_store_c07_a_known_zero_continues_through_a_real_handback() {
    let mut lab = Lab::priced();
    let a = vec![reading(1, T0, "Charging", Some(0.0), 50, ODO)];
    let ledger =
        across_a_handback(&mut lab, &a, &b_to_956(2, 0.66, ODO)).expect("a ledger at the hand-back");
    assert!(ledger.live);
    assert_eq!(ledger.accounted_kwh, Some(0.0), "a known zero is not unknown");
    assert_eq!(ledger.last_kwh, Some(0.0));
    let billing = lab.billing();
    assert_eq!(
        billing,
        vec![
            (Some(0.0), Some(2.00), Some(2.00)),
            // The row's own flag waives the fee; the inference alone would
            // charge it (its largest counter equals its energy added).
            (Some(9.56), Some(0.00), Some(3.82)),
        ]
    );
    assert_eq!(fees(&billing), 2.00);
    assert_eq!(lab.flags(), vec![Some(false), Some(true)]);
}

#[test]
fn charge_continuity_store_c17_a_top_up_after_complete_continues_through_a_real_handback() {
    let mut lab = Lab::priced();
    let a = vec![
        reading(1, T0, "Charging", Some(1.0), 50, ODO),
        reading(2, T0 + 2 * MIN, "Complete", Some(5.0), 60, ODO),
    ];
    // A closed its row at Complete: nothing is open at the hand-back, so the
    // hand-back stores no event and leaves the lifecycle record as it was.
    let opened = T0 - MIN;
    lab.open_window(1, opened);
    lab.feed(&a);
    let (before_record, _) = lab.lifecycle();
    lab.hand_back(1, opened, T0 + HOUR);
    let (after_record, state) = lab.lifecycle();
    assert_eq!(after_record, before_record, "an idle hand-back writes no state");
    let ledger = state.charge_ledger.expect("a ledger");
    assert!(ledger.live);
    assert_eq!(ledger.accounted_kwh, Some(5.0));
    lab.reopen();
    assert_eq!(lab.ledger(), Some(ledger), "the reopened store holds the same ledger");
    lab.open_window(2, T0 + 5 * HOUR - MIN);
    lab.feed(&b_to_956(3, 6.0, ODO));
    let billing = lab.billing();
    assert_eq!(
        billing,
        vec![
            (Some(5.0), Some(2.00), Some(4.00)),
            (Some(4.56), Some(0.00), Some(1.82)),
        ]
    );
    assert_eq!(fees(&billing), 2.00);
    assert_eq!(lab.flags(), vec![Some(false), Some(true)]);
}

#[test]
fn charge_continuity_store_c18_a_recovered_gap_charge_continues_through_a_real_handback() {
    let mut lab = Lab::priced();
    let a = vec![
        reading(1, T0, "Charging", Some(1.0), 45, ODO),
        reading(2, T0 + 2 * MIN, "Charging", Some(2.0), 50, ODO),
    ];
    let t = T0 + 5 * HOUR;
    let b = vec![
        reading(3, t, "Complete", Some(3.0), 49, ODO),
        battery_frame(t + MIN, 51),
        reading(5, t + HOUR, "Charging", Some(3.10), 52, ODO),
        reading(6, t + 2 * HOUR, "Complete", Some(5.0), 60, ODO),
    ];
    let ledger = across_a_handback(&mut lab, &a, &b).expect("a ledger at the hand-back");
    assert_eq!(ledger.accounted_kwh, Some(2.0));
    let billing = lab.billing();
    assert_eq!(
        billing,
        vec![
            (Some(2.0), Some(2.00), Some(2.80)),
            // The recovered gap charge and its top-up are one plug-in.
            (Some(1.0), Some(0.00), Some(0.40)),
            (Some(2.0), Some(0.00), Some(0.80)),
        ]
    );
    assert_eq!(fees(&billing), 2.00);
    assert_eq!(lab.flags(), vec![Some(false), Some(true), Some(true)]);
}

#[test]
fn charge_continuity_store_c27_an_idle_vehicle_gets_no_event_no_rows_and_no_catalogue_change() {
    let lab = Lab::new();
    let idle = lab.vehicle.clone();
    let charging = lab
        .store()
        .register_vehicle(&VehicleDescriptor::new(lab.source.source_id, "vehicle-two"), 1_002)
        .expect("second vehicle");
    lab.open_window(1, T0 - MIN);
    // The idle vehicle is parked and online (no charging state, no counter);
    // the other one is charging at the hand-back.
    lab.feed_vehicle(&idle, &(T0, reading(1, T0, "", None, 80, ODO).1));
    lab.feed_vehicle(&idle, &(T0 + 2 * MIN, reading(2, T0 + 2 * MIN, "", None, 80, ODO).1));
    lab.feed_vehicle(&charging, &reading(1, T0 + MIN, "Charging", Some(1.0), 50, ODO));
    let catalogue = |lab: &Lab| -> Vec<(String, i64)> {
        let connection = lab.store().open().expect("connection");
        let mut statement = connection
            .prepare(
                "SELECT vehicle_id, last_seen_at_ms FROM vehicles
                 ORDER BY last_seen_at_ms DESC, vehicle_id ASC",
            )
            .expect("prepare");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("catalogue")
            .collect::<Result<_, _>>()
            .expect("catalogue rows")
    };
    let mutations = |lab: &Lab, vehicle: &VehicleRecord| -> i64 {
        lab.store()
            .open()
            .expect("connection")
            .query_row(
                "SELECT COUNT(*) FROM sync_mutations WHERE vehicle_id = ?1",
                params![vehicle.vehicle_id.to_string()],
                |row| row.get(0),
            )
            .expect("mutation count")
    };
    let (record_before, state_before) = lab.lifecycle_of(&idle).expect("the idle vehicle has a state");
    assert!(state_before.open_charge.is_none() && state_before.open_drive.is_none());
    assert!(state_before.charge_ledger.is_none_or(|ledger| !ledger.live));
    let catalogue_before = catalogue(&lab);
    let mutations_before = mutations(&lab, &idle);
    let (charging_before, _) = lab.lifecycle_of(&charging).expect("the charging vehicle has a state");
    lab.hand_back(1, T0 - MIN, T0 + HOUR);
    // The idle vehicle: no event (the lifecycle cursor, which advances exactly
    // when an observation reaches the lifecycle, and the whole record are
    // unchanged), no rows, no catalogue move.
    let (record_after, _) = lab.lifecycle_of(&idle).expect("state");
    assert_eq!(record_after, record_before, "the idle vehicle's state is untouched");
    assert!(lab.charges_of(&idle).is_empty(), "no charge row");
    assert_eq!(mutations(&lab, &idle), mutations_before, "no sync mutation");
    assert_eq!(catalogue(&lab), catalogue_before, "catalogue order and last seen unchanged");
    // The same hand-back closed the charging vehicle's open charge.
    let (charging_after, charging_state) = lab.lifecycle_of(&charging).expect("state");
    assert_eq!(
        charging_after.last_observation_id,
        charging_before.last_observation_id + 1,
        "one hand-back event reached the charging vehicle's lifecycle"
    );
    assert!(charging_state.open_charge.is_none());
    assert_eq!(lab.charges_of(&charging).len(), 1);
    // A second hand-back of the sealed window changes nothing.
    lab.hand_back(1, T0 - MIN, T0 + HOUR);
    assert_eq!(catalogue(&lab), catalogue_before);
    let (again, _) = lab.lifecycle_of(&idle).expect("state");
    assert_eq!(again, record_before);
}

/// An imported (TeslaMate) open charge 7 with two samples, as the window
/// rebind seeds it.
fn imported_open_charge(start: i64) -> crate::source_db_projection::SourceDbOpenSession {
    let process = serde_json::from_value(json!({
        "id": 7, "car_id": 1, "position_id": null, "address_id": null,
        "geofence_id": null, "start_date_ms": start, "end_date_ms": null,
        "charge_energy_added": 99.0, "charge_energy_used_kwh": 99.0,
        "start_ideal_range_km": 151.0, "end_ideal_range_km": 152.0,
        "start_battery_level": 40, "end_battery_level": 50,
        "duration_min": null, "outside_temp_avg": 11.0,
        "start_rated_range_km": 141.0, "end_rated_range_km": 142.0,
        "cost": null
    }))
    .expect("imported charging process");
    let sample = |id: i64, at_ms: i64, energy: f64, level: i64| {
        serde_json::from_value(json!({
            "id": id, "charging_process_id": 7, "date_ms": at_ms,
            "battery_heater": false, "battery_heater_on": false,
            "battery_heater_no_power": false, "battery_level": level,
            "usable_battery_level": level, "charge_energy_added_kwh": energy,
            "charger_actual_current": 16.0, "charger_phases": 3,
            "charger_pilot_current": 16.0, "charger_power_kw": 11.0,
            "charger_voltage": 230.0, "charge_cable": "IEC",
            "fast_charger_present": false, "fast_charger_brand": null,
            "fast_charger_type": null, "ideal_range_km": 150.0 + id as f64,
            "rated_range_km": 140.0 + id as f64,
            "not_enough_power_to_heat": false, "outside_temp_c": 10.0
        }))
        .expect("imported charge sample")
    };
    let mut session = crate::source_db_projection::SourceDbOpenSession {
        car_id: 1,
        charge: Some(process),
        charge_samples: vec![sample(1, start, 1.0, 40), sample(2, start + 5 * MIN, 2.0, 50)],
        ..Default::default()
    };
    session.watermarks.charging_processes.max_id = Some(7);
    session.watermarks.charges.max_timestamp_ms = Some(start + 5 * MIN);
    session
}

#[test]
fn charge_continuity_store_c31_an_imported_open_charge_at_the_rebind_continues_as_on_main() {
    // Pinned (design C31, D5): A's hand-back leaves a live ledger at 0.64;
    // TeslaMate's open charge seeded at the next window's rebind owns its own
    // energy, so the ledger is reset by the seed, the imported counter is not a
    // baseline, and the live samples close the row at the whole counter, as the
    // lifecycle's own seed test (T63) pins on main.
    let mut lab = Lab::new();
    lab.open_window(1, T0 - MIN);
    lab.feed(&[
        reading(1, T0, "Charging", Some(0.30), 50, ODO),
        reading(2, T0 + 2 * MIN, "Charging", Some(0.64), 51, ODO),
    ]);
    lab.hand_back(1, T0 - MIN, T0 + HOUR);
    let ledger = lab.ledger().expect("a ledger at the hand-back");
    assert!(ledger.live);
    assert_eq!(ledger.accounted_kwh, Some(0.64));
    lab.reopen();
    lab.open_window(2, T0 + 5 * HOUR - MIN);
    // The imported samples end 2 min before the first live one, as in the
    // lifecycle's T63, so no gap closes the seeded charge.
    let start = T0 + 5 * HOUR - 7 * MIN;
    lab.store()
        .seed_imported_open_session(
            lab.source.source_id,
            lab.vehicle.vehicle_id,
            CAR,
            &imported_open_charge(start),
            T0 + 5 * HOUR - MIN,
        )
        .expect("the rebind seeds the imported open charge");
    let (_, seeded) = lab.lifecycle();
    assert_eq!(
        seeded.open_charge.as_ref().map(|open| open.id),
        Some(7),
        "the imported charge is the open one"
    );
    assert_eq!(
        seeded.charge_ledger,
        Some(crate::lifecycle::ChargeLedger::default()),
        "D5: the seed empties the ledger"
    );
    let t = T0 + 5 * HOUR;
    lab.feed(&[
        reading(3, t, "Charging", Some(3.00), 60, ODO),
        reading(4, t + 2 * MIN, "Complete", Some(4.00), 70, ODO),
    ]);
    let rows = lab.charges();
    let seeded_row = rows.iter().find(|charge| charge.id == 7).expect("the seeded row closes");
    assert_eq!(
        seeded_row.charge_energy_added,
        Some(4.00),
        "neither the imported counter nor A's 0.64 is a baseline"
    );
    // A's own row is untouched by the seeded one.
    assert_eq!(
        rows.iter()
            .filter(|charge| charge.id != 7)
            .map(|charge| charge.charge_energy_added)
            .collect::<Vec<_>>(),
        vec![Some(0.64)]
    );
}
