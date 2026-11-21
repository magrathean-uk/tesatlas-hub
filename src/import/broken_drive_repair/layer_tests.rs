// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::import::broken_drive_repair::{
    ComponentPlanner, GapEvidence, MemberRole, PlanOutcome, PlannedComponent, RepairEligibility,
    RepairInput, RepairRefusal, RepairSample, StateCoverage,
};
use std::convert::Infallible;

const PG_EPOCH_US: i64 = 946_684_800_000_000;
const T0_MS: i64 = 1_700_000_000_000;
const DEST_ID: i32 = 40;
const FRAGMENTS: [i32; 3] = [10, 11, 12];

fn pg(ms: i64) -> i64 {
    ms * 1_000 - PG_EPOCH_US
}

fn km(value: f64) -> Option<ProjectionFloat64BitsV2_2> {
    Some(ProjectionFloat64BitsV2_2::from_f64(value))
}

fn e1(value: i64) -> Option<ProjectionFixedNumericV2_2> {
    Some(ProjectionFixedNumericV2_2::Finite(value))
}

fn fragment_row(id: i32, at_ms: i64, odometer: f64) -> SourceDbDrivePhysicalV2_2 {
    SourceDbDrivePhysicalV2_2 {
        id,
        car_id: 1,
        start_date_pg_us: pg(at_ms),
        end_date_pg_us: None,
        start_position_id: Some(id * 100),
        end_position_id: None,
        start_address_id: Some(1_000 + id),
        end_address_id: None,
        start_geofence_id: Some(2_000 + id),
        end_geofence_id: None,
        outside_temp_avg_e1: None,
        inside_temp_avg_e1: None,
        speed_max: None,
        power_max: None,
        power_min: None,
        start_ideal_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(30_000 + i64::from(id))),
        end_ideal_range_km_e2: None,
        start_rated_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(29_000 + i64::from(id))),
        end_rated_range_km_e2: None,
        start_km: km(odometer),
        end_km: None,
        distance: None,
        duration_min: None,
        ascent: None,
        descent: None,
    }
}

fn destination_row() -> SourceDbDrivePhysicalV2_2 {
    SourceDbDrivePhysicalV2_2 {
        id: DEST_ID,
        car_id: 1,
        start_date_pg_us: pg(T0_MS + 180_000),
        end_date_pg_us: Some(pg(T0_MS + 3_600_000)),
        start_position_id: Some(4_000),
        end_position_id: Some(4_999),
        start_address_id: Some(77),
        end_address_id: Some(78),
        start_geofence_id: Some(79),
        end_geofence_id: Some(80),
        outside_temp_avg_e1: e1(100),
        inside_temp_avg_e1: e1(210),
        speed_max: Some(88),
        power_max: Some(60),
        power_min: Some(-30),
        start_ideal_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(11_111)),
        end_ideal_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(22_222)),
        start_rated_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(33_333)),
        end_rated_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(44_444)),
        start_km: km(1_000.3),
        end_km: km(1_020.0),
        distance: km(19.7),
        duration_min: Some(59),
        ascent: Some(120),
        descent: Some(95),
    }
}

fn sample_row(id: i32, parent: i32, at_ms: i64, odometer: f64) -> SourceDbPositionPhysicalV2_2 {
    SourceDbPositionPhysicalV2_2 {
        id,
        car_id: 1,
        drive_id: Some(parent),
        date_pg_us: pg(at_ms),
        latitude_e6: ProjectionFixedNumericV2_2::Finite(47_500_000 + i64::from(id)),
        longitude_e6: ProjectionFixedNumericV2_2::Finite(19_000_000),
        elevation: Some(100),
        speed: Some(40),
        power: Some(20),
        odometer: km(odometer),
        ideal_battery_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(30_000)),
        est_battery_range_km_e2: None,
        rated_battery_range_km_e2: Some(ProjectionFixedNumericV2_2::Finite(29_000)),
        battery_level: Some(80),
        usable_battery_level: Some(79),
        battery_heater: None,
        battery_heater_on: None,
        battery_heater_no_power: None,
        outside_temp_e1: e1(50),
        inside_temp_e1: e1(200),
        fan_status: None,
        driver_temp_setting_e1: None,
        passenger_temp_setting_e1: None,
        is_climate_on: None,
        is_rear_defroster_on: None,
        is_front_defroster_on: None,
        tpms_pressure_fl_e1: None,
        tpms_pressure_fr_e1: None,
        tpms_pressure_rl_e1: None,
        tpms_pressure_rr_e1: None,
    }
}

fn materialised_destination() -> ProjectionDrive {
    ProjectionDrive {
        id: i64::from(DEST_ID),
        car_id: 1,
        optimized_at_ms: Some(T0_MS + 4_000_000),
        start_date_ms: T0_MS + 180_000,
        end_date_ms: T0_MS + 3_600_000,
        distance_km: Some(19.7),
        duration_min: Some(59),
        efficiency: Some(150.0),
        outside_temp_avg: Some(10.0),
        inside_temp_avg: Some(21.0),
        speed_max: Some(88),
        power_max: Some(60.0),
        power_min: Some(-30.0),
        start_ideal_range_km: Some(111.11),
        end_ideal_range_km: Some(222.22),
        start_address: Some("Old start".to_owned()),
        end_address: Some("End".to_owned()),
        start_geofence: Some("Old fence".to_owned()),
        end_geofence: Some("End fence".to_owned()),
        start_latitude: Some(1.0),
        start_longitude: Some(2.0),
        end_latitude: Some(3.0),
        end_longitude: Some(4.0),
        start_soc: Some(50),
        end_soc: Some(40),
        start_rated_range_km: Some(333.33),
        end_rated_range_km: Some(444.44),
        ascent: Some(120),
        descent: Some(95),
    }
}

fn materialised_sample(id: i32, at_ms: i64, odometer: f64) -> ProjectionPosition {
    ProjectionPosition {
        id: i64::from(id),
        drive_id: None,
        car_id: 1,
        date_ms: at_ms,
        latitude: 47.5 + f64::from(id) / 1e6,
        longitude: 19.0,
        speed: Some(40),
        power: Some(20.0),
        battery_level: Some(80),
        usable_battery_level: Some(79),
        elevation: Some(100),
        odometer: Some(odometer),
        ideal_battery_range_km: Some(300.0),
        est_battery_range_km: None,
        rated_battery_range_km: Some(290.0),
        fan_status: None,
        driver_temp_setting: None,
        passenger_temp_setting: None,
        is_climate_on: None,
        is_rear_defroster_on: None,
        is_front_defroster_on: None,
        inside_temp: Some(20.0),
        outside_temp: Some(5.0),
        battery_heater: None,
        battery_heater_on: None,
        battery_heater_no_power: None,
        tpms_pressure_fl: None,
        tpms_pressure_fr: None,
        tpms_pressure_rl: None,
        tpms_pressure_rr: None,
    }
}

/// Three fragments one minute apart, then the destination 30 s after the last.
fn pre_rows() -> ComponentPreRows {
    let starts = [T0_MS, T0_MS + 60_000, T0_MS + 120_000];
    let odometers = [1_000.0, 1_000.1, 1_000.2];
    let mut fragments = Vec::new();
    let mut samples = Vec::new();
    let mut materialised_samples = Vec::new();
    for (index, &id) in FRAGMENTS.iter().enumerate() {
        fragments.push(fragment_row(id, starts[index], odometers[index]));
        let sample_id = id * 100;
        samples.push(sample_row(sample_id, id, starts[index], odometers[index]));
        materialised_samples.push(materialised_sample(sample_id, starts[index], odometers[index]));
    }
    // The second fragment's sample is faster and has no temperatures.
    samples[1].speed = Some(130);
    samples[1].power = Some(-75);
    samples[1].outside_temp_e1 = None;
    samples[1].inside_temp_e1 = None;
    materialised_samples[1].speed = Some(130);
    materialised_samples[1].power = Some(-75.0);
    materialised_samples[1].outside_temp = None;
    materialised_samples[1].inside_temp = None;
    ComponentPreRows {
        destination: destination_row(),
        fragments,
        samples,
        destination_samples: DestinationSampleCounts {
            outside_temp: 8,
            inside_temp: 8,
        },
        materialised_destination: materialised_destination(),
        materialised_samples,
    }
}

fn input(row: &SourceDbDrivePhysicalV2_2, sample: Option<&SourceDbPositionPhysicalV2_2>) -> RepairInput {
    let start_ms = (row.start_date_pg_us + PG_EPOCH_US) / 1_000;
    let sample = sample.map(|s| RepairSample {
        position_id: i64::from(s.id),
        date_ms: (s.date_pg_us + PG_EPOCH_US) / 1_000,
        odometer_km: Some(1_000.0),
        speed_kmh: s.speed.map(i64::from),
    });
    RepairInput {
        drive_id: i64::from(row.id),
        car_id: 1,
        start_ms,
        end_ms: row.end_date_pg_us.map(|end| (end + PG_EPOCH_US) / 1_000),
        distance_km: None,
        duration_min: None,
        sample_count: u32::from(sample.is_some()),
        first_sample: sample,
        last_sample: sample,
        previous_drive_id: None,
        previous_unclosed: false,
        next_drive_id: None,
        gap_to_next: None,
    }
}

/// The planner's own component type for `pre`, as the next task would hold it.
fn component(pre: &ComponentPreRows) -> PlannedComponent {
    let mut members: Vec<RepairInput> = pre
        .fragments
        .iter()
        .zip(&pre.samples)
        .map(|(row, sample)| input(row, Some(sample)))
        .collect();
    members.push(input(&pre.destination, None));
    PlannedComponent {
        component_key: [7; 32],
        input_digest: [8; 32],
        destination_drive_id: i64::from(pre.destination.id),
        fragment_drive_ids: pre.fragments.iter().map(|row| i64::from(row.id)).collect(),
        sample_position_ids: pre.samples.iter().map(|s| i64::from(s.id)).collect(),
        effective_start_ms: T0_MS,
        changed_rows: pre.fragments.len() * 2 + 1,
        members,
    }
}

#[test]
fn broken_drive_repair_layer_joins_three_fragments_to_the_destination() {
    let pre = pre_rows();
    let component = component(&pre);
    let layered = layer_component(&component, &pre, None).expect("layers");
    let effective = &layered.effective;

    // Counts: one drive row remains, three drives are removed, three samples move.
    assert_eq!(effective.removed_drive_ids, FRAGMENTS.to_vec());
    assert_eq!(effective.samples.len(), 3);
    assert_eq!(layered.after_image.samples.len(), 3);

    // Start fields come from the first fragment; the end stays.
    let drive = &effective.destination;
    let first = &pre.fragments[0];
    assert_eq!(drive.id, DEST_ID);
    assert_eq!(drive.start_date_pg_us, first.start_date_pg_us);
    assert_eq!(drive.start_position_id, first.start_position_id);
    assert_eq!(drive.start_address_id, first.start_address_id);
    assert_eq!(drive.start_geofence_id, first.start_geofence_id);
    assert_eq!(drive.start_ideal_range_km_e2, first.start_ideal_range_km_e2);
    assert_eq!(drive.start_rated_range_km_e2, first.start_rated_range_km_e2);
    assert_eq!(drive.start_km, first.start_km);
    assert_eq!(drive.end_date_pg_us, pre.destination.end_date_pg_us);
    assert_eq!(drive.end_position_id, pre.destination.end_position_id);
    assert_eq!(drive.end_km, pre.destination.end_km);
    assert_eq!(drive.end_ideal_range_km_e2, pre.destination.end_ideal_range_km_e2);

    // Reparenting: every fragment sample now belongs to the destination and
    // nothing else about it changes.
    for (after, before) in effective.samples.iter().zip(&pre.samples) {
        assert_eq!(after.drive_id, Some(DEST_ID));
        assert_eq!(SourceDbPositionPhysicalV2_2 { drive_id: before.drive_id, ..after.clone() }, *before);
    }
    for (after, before) in layered.after_image.samples.iter().zip(&pre.materialised_samples) {
        assert_eq!(after.drive_id, Some(i64::from(DEST_ID)));
        assert_eq!(ProjectionPosition { drive_id: None, ..after.clone() }, *before);
    }

    // Rounding. From the first fragment start to the end is 3,600 s: 60 min.
    // Distance is the endpoint odometers, not the old stored figure.
    assert_eq!(drive.duration_min, Some(60));
    let ProjectionFloat64BitsV2_2(bits) = drive.distance.expect("distance");
    assert!((f64::from_bits(bits) - 20.0).abs() < 1e-9);
    // Summaries: maxima over the destination and the fragment samples; the
    // temperature means are weighted by sample count and rounded to one place.
    assert_eq!(drive.speed_max, Some(130));
    assert_eq!(drive.power_max, Some(60));
    assert_eq!(drive.power_min, Some(-75));
    // (100 * 8 + 50 + 50) / 10 = 90 ; inside (210 * 8 + 200 + 200) / 10 = 208.
    assert_eq!(drive.outside_temp_avg_e1, e1(90));
    assert_eq!(drive.inside_temp_avg_e1, e1(208));
    // Terrain cannot be justified from the parts: cleared, and flagged.
    assert_eq!((drive.ascent, drive.descent), (None, None));
    assert!(layered.terrain_recompute);

    // The materialised after-image agrees with the physical row.
    let materialised = &layered.after_image.destination;
    assert_eq!(materialised.id, i64::from(DEST_ID));
    assert_eq!(materialised.start_date_ms, T0_MS);
    assert_eq!(materialised.end_date_ms, pre.materialised_destination.end_date_ms);
    assert_eq!(materialised.duration_min, Some(60));
    assert!((materialised.distance_km.expect("distance") - 20.0).abs() < 1e-9);
    assert_eq!(materialised.speed_max, Some(130));
    assert_eq!(materialised.power_max, Some(60.0));
    assert_eq!(materialised.power_min, Some(-75.0));
    assert_eq!(materialised.outside_temp_avg, Some(9.0));
    assert_eq!(materialised.inside_temp_avg, Some(20.8));
    // Start references are rebuilt from the first sample; no name is invented.
    assert_eq!(materialised.start_address, None);
    assert_eq!(materialised.start_geofence, None);
    assert_eq!(materialised.start_latitude, Some(pre.materialised_samples[0].latitude));
    assert_eq!(materialised.start_soc, Some(80));
    assert_eq!(materialised.start_ideal_range_km, Some(300.0));
    assert_eq!(materialised.start_rated_range_km, Some(290.0));
    assert_eq!(materialised.end_address.as_deref(), Some("End"));
    assert_eq!((materialised.ascent, materialised.descent, materialised.efficiency), (None, None, None));
    assert_eq!(materialised.optimized_at_ms, None);
}

#[test]
fn broken_drive_repair_layer_rounds_the_duration_to_the_nearest_minute() {
    // 90 s rounds half away from zero to 2; 89.999 s stays 1.
    for (extra_us, expected) in [(0, 2), (-1, 1)] {
        let mut pre = pre_rows();
        let end = pg(T0_MS) + 90_000_000 + extra_us;
        pre.destination.end_date_pg_us = Some(end);
        pre.destination.end_km = km(1_000.5);
        pre.materialised_destination.end_date_ms = (end + PG_EPOCH_US) / 1_000;
        let component = component(&pre);
        let layered = layer_component(&component, &pre, None).expect("layers");
        assert_eq!(layered.effective.destination.duration_min, Some(expected));
        assert_eq!(layered.after_image.destination.duration_min, Some(expected));
    }
}

#[test]
fn broken_drive_repair_layer_round_trips_to_the_pre_rows() {
    let pre = pre_rows();
    let component = component(&pre);
    let layered = layer_component(&component, &pre, None).expect("layers");
    assert_ne!(layered.effective.destination, pre.destination);
    let back = unlayer_component(&component, &layered, &pre).expect("unlayers");
    assert_eq!(back, pre);
    // The digest the first application records is the one later ones compare.
    let recorded = pre.digest();
    assert_eq!(layered.pre_rows_digest, recorded);
    let again = layer_component(&component, &pre, Some(&recorded)).expect("layers again");
    assert_eq!(again, layered);
    assert_eq!(unlayer_component(&component, &again, &pre), Ok(pre));
}

#[test]
fn broken_drive_repair_layer_keeps_every_id_and_the_totals() {
    let pre = pre_rows();
    let component = component(&pre);
    let layered = layer_component(&component, &pre, None).expect("layers");
    let kept: Vec<i32> = std::iter::once(layered.effective.destination.id)
        .chain(layered.effective.removed_drive_ids.iter().copied())
        .collect();
    assert_eq!(kept, [DEST_ID, 10, 11, 12]);
    let before: Vec<i32> = pre.samples.iter().map(|s| s.id).collect();
    let after: Vec<i32> = layered.effective.samples.iter().map(|s| s.id).collect();
    assert_eq!(before, after);
    let car: Vec<i16> = layered.effective.samples.iter().map(|s| s.car_id).collect();
    assert_eq!(car, [1, 1, 1]);
    // One completed journey before and after (the destination); samples constant.
    assert_eq!(pre.samples.len(), layered.effective.samples.len());
    assert_eq!(layered.after_image.destination.id, i64::from(DEST_ID));
    let ids: Vec<i64> = layered.after_image.samples.iter().map(|s| s.id).collect();
    assert_eq!(ids, [1_000, 1_100, 1_200]);
}

#[test]
fn broken_drive_repair_layer_refuses_a_pre_row_digest_mismatch() {
    let pre = pre_rows();
    let component = component(&pre);
    let recorded = pre.digest();
    let mut changed = pre.clone();
    changed.destination.speed_max = Some(89);
    assert_eq!(
        layer_component(&component, &changed, Some(&recorded)),
        Err(LayerError::PreRowsDigestMismatch)
    );
    let mut moved = pre.clone();
    moved.materialised_samples[2].latitude += 0.000_001;
    assert_eq!(
        layer_component(&component, &moved, Some(&recorded)),
        Err(LayerError::PreRowsDigestMismatch)
    );
}

#[test]
fn broken_drive_repair_layer_refuses_a_closed_fragment() {
    type Closer = fn(&mut SourceDbDrivePhysicalV2_2);
    let closers: [Closer; 3] = [
        |row| row.end_date_pg_us = Some(row.start_date_pg_us + 1),
        |row| row.distance = km(0.0),
        |row| row.duration_min = Some(0),
    ];
    for closer in closers {
        let mut pre = pre_rows();
        closer(&mut pre.fragments[1]);
        let component = component(&pre);
        assert_eq!(
            layer_component(&component, &pre, None),
            Err(LayerError::FragmentClosed { drive_id: 11 })
        );
    }
}

#[test]
fn broken_drive_repair_layer_refuses_a_total_that_cannot_be_represented() {
    // Duration beyond the smallint the public schema keeps.
    let mut pre = pre_rows();
    let end = pg(T0_MS) + (32_768_i64 * 60_000_000);
    pre.destination.end_date_pg_us = Some(end);
    let component_long = component(&pre);
    assert_eq!(
        layer_component(&component_long, &pre, None),
        Err(LayerError::TotalUnrepresentable("duration"))
    );
    // An end before the start.
    let mut pre = pre_rows();
    pre.destination.end_date_pg_us = Some(pre.fragments[0].start_date_pg_us - 1);
    assert_eq!(
        layer_component(&component(&pre), &pre, None),
        Err(LayerError::TotalUnrepresentable("duration"))
    );
    // Odometers that run backwards, or are not finite, or are missing.
    for end_km in [km(999.0), km(f64::NAN), km(f64::INFINITY), None] {
        let mut pre = pre_rows();
        pre.destination.end_km = end_km;
        assert_eq!(
            layer_component(&component(&pre), &pre, None),
            Err(LayerError::TotalUnrepresentable("distance"))
        );
    }
    // A temperature that is not a number.
    let mut pre = pre_rows();
    pre.samples[0].outside_temp_e1 = Some(ProjectionFixedNumericV2_2::NaN);
    assert_eq!(
        layer_component(&component(&pre), &pre, None),
        Err(LayerError::TotalUnrepresentable("temperature"))
    );
}

#[test]
fn broken_drive_repair_layer_refuses_pre_rows_that_are_not_the_component() {
    let base = pre_rows();
    let component = component(&base);
    let mismatch = |pre: &ComponentPreRows| layer_component(&component, pre, None);

    let mut pre = base.clone();
    pre.destination.id = 41;
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base.clone();
    pre.fragments.swap(0, 1);
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base.clone();
    pre.samples[2].drive_id = Some(10);
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base.clone();
    pre.materialised_samples[0].drive_id = Some(i64::from(DEST_ID));
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base.clone();
    pre.materialised_destination.id = 99;
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base.clone();
    pre.samples.pop();
    assert!(matches!(mismatch(&pre), Err(LayerError::ComponentMismatch(_))));
    let mut pre = base;
    pre.destination.end_date_pg_us = None;
    assert_eq!(mismatch(&pre), Err(LayerError::DestinationOpen));
}

#[test]
fn broken_drive_repair_layer_refuses_to_unlayer_rows_that_have_drifted() {
    let pre = pre_rows();
    let component = component(&pre);
    let layered = layer_component(&component, &pre, None).expect("layers");
    let mut drifted = layered.clone();
    drifted.effective.destination.speed_max = Some(1);
    assert_eq!(
        unlayer_component(&component, &drifted, &pre),
        Err(LayerError::LayerMismatch)
    );
    let mut other = pre.clone();
    other.destination.speed_max = Some(90);
    assert_eq!(
        unlayer_component(&component, &layered, &other),
        Err(LayerError::PreRowsDigestMismatch)
    );
}

/// The planner's own output feeds the layer: a component planned from
/// captured rows layers without further adaptation.
#[test]
fn broken_drive_repair_layer_accepts_a_component_the_planner_made() {
    struct Admit;
    impl RepairEligibility for Admit {
        type Error = Infallible;
        fn member_refusal(&mut self, _: &RepairInput, _: MemberRole) -> Result<Option<RepairRefusal>, Infallible> {
            Ok(None)
        }
        fn suppressed(&mut self, _: &[u8; 32]) -> Result<bool, Infallible> {
            Ok(false)
        }
    }
    let pre = pre_rows();
    let rows: Vec<RepairInput> = (0..3)
        .map(|i| {
            let ms = T0_MS + 60_000 * i as i64;
            let s = RepairSample {
                position_id: i64::from(FRAGMENTS[i]) * 100,
                date_ms: ms,
                odometer_km: Some(1_000.0 + 0.1 * i as f64),
                speed_kmh: Some(30),
            };
            RepairInput {
                drive_id: i64::from(FRAGMENTS[i]),
                car_id: 1,
                start_ms: ms,
                end_ms: None,
                distance_km: None,
                duration_min: None,
                sample_count: 1,
                first_sample: Some(s),
                last_sample: Some(s),
                previous_drive_id: None,
                previous_unclosed: false,
                next_drive_id: Some(if i == 2 { i64::from(DEST_ID) } else { i64::from(FRAGMENTS[i + 1]) }),
                gap_to_next: Some(GapEvidence {
                    charge_between: false,
                    state: StateCoverage::Online,
                }),
            }
        })
        .collect();
    let dest_sample = RepairSample {
        position_id: 4_000,
        date_ms: T0_MS + 180_000,
        odometer_km: Some(1_000.3),
        speed_kmh: Some(30),
    };
    let destination = RepairInput {
        drive_id: i64::from(DEST_ID),
        car_id: 1,
        start_ms: T0_MS + 180_000,
        end_ms: Some(T0_MS + 3_600_000),
        distance_km: Some(19.7),
        duration_min: Some(59),
        sample_count: 9,
        first_sample: Some(dest_sample),
        last_sample: Some(dest_sample),
        previous_drive_id: Some(i64::from(FRAGMENTS[2])),
        previous_unclosed: true,
        next_drive_id: None,
        gap_to_next: None,
    };
    let mut planner = ComponentPlanner::new("vehicle", T0_MS + 100 * 3_600_000);
    let mut planned = Vec::new();
    for row in rows.into_iter().chain(std::iter::once(destination)) {
        planned.extend(planner.push(row, &mut Admit).expect("pushes"));
    }
    let [PlanOutcome::Planned(component)] = planned.as_slice() else {
        panic!("one planned component expected, got {planned:?}");
    };
    let layered = layer_component(component, &pre, None).expect("layers");
    assert_eq!(layered.effective.removed_drive_ids, FRAGMENTS.to_vec());
}
