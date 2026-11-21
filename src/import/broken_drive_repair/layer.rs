// SPDX-License-Identifier: AGPL-3.0-only

//! The pure repair layer over one component (M03-T27a).
//!
//! `layer_component` turns a component's pre-layer rows into the rows a
//! planned head and the materialised store show once the repair applies, and
//! `unlayer_component` verifies and returns the pre-layer rows again. Both are
//! functions of their inputs alone: no store, source or clock is read, and
//! nothing here is reachable from production until a later task calls it.
//!
//! The destination drive keeps its id and end and takes the first fragment's
//! start fields; its duration, distance and sample-derived summaries are
//! recomputed with the Hub's usual rounding. The fragment drives are removed
//! and every fragment sample is reparented to the destination. Journey and
//! sample totals stay constant and no id changes. Nothing is guessed: a
//! mismatched pre-row digest, a fragment that is closed, or a total the public
//! schema cannot hold is a refusal with its own variant.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::PlannedComponent;
use crate::hub_pack::{
    ProjectionDrive, ProjectionFixedNumericV2_2, ProjectionFloat64BitsV2_2, ProjectionPosition,
};
use crate::source_db_projection::{SourceDbDrivePhysicalV2_2, SourceDbPositionPhysicalV2_2};

/// PostgreSQL's epoch in Unix microseconds.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
/// The largest `duration_min`: the public schema's smallint.
const MAX_DURATION_MIN: i64 = 32_767;

/// How many of the destination's own samples carry each averaged value; the
/// weights that fold the fragment samples into the stored means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationSampleCounts {
    pub outside_temp: u32,
    pub inside_temp: u32,
}

/// One component's rows as they are before the layer applies: what the first
/// application records in `drive_repair_changes.before_json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComponentPreRows {
    /// The physical destination drive row.
    pub destination: SourceDbDrivePhysicalV2_2,
    /// The physical fragment drive rows, in run order.
    pub fragments: Vec<SourceDbDrivePhysicalV2_2>,
    /// The physical fragment samples, one per fragment in run order, each
    /// parented to its fragment.
    pub samples: Vec<SourceDbPositionPhysicalV2_2>,
    pub destination_samples: DestinationSampleCounts,
    /// The materialised destination drive.
    pub materialised_destination: ProjectionDrive,
    /// The materialised fragment samples (detached), in the same order.
    pub materialised_samples: Vec<ProjectionPosition>,
}

impl ComponentPreRows {
    /// The digest a later application compares against the recorded one.
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"tesatlas-hub/drive-repair-pre-rows/v1\0");
        hasher.update(serde_json::to_vec(self).expect("pre-rows serialise"));
        hasher.finalize().into()
    }
}

/// The effective physical rows of one repaired component.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveRows {
    pub destination: SourceDbDrivePhysicalV2_2,
    /// The fragment drive rows that no longer exist as journeys.
    pub removed_drive_ids: Vec<i32>,
    /// The fragment samples, parented to the destination.
    pub samples: Vec<SourceDbPositionPhysicalV2_2>,
}

/// The materialised after-image: what the store holds once the repair applies.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterialisedAfterImage {
    pub destination: ProjectionDrive,
    pub samples: Vec<ProjectionPosition>,
}

/// A layered component.
#[derive(Debug, Clone, PartialEq)]
pub struct LayeredComponent {
    pub effective: EffectiveRows,
    pub after_image: MaterialisedAfterImage,
    /// Ascent and descent were cleared; the local terrain pass should run.
    pub terrain_recompute: bool,
    /// The digest of the pre-rows this was made from.
    pub pre_rows_digest: [u8; 32],
}

/// Why a component cannot be layered or unlayered.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LayerError {
    /// The pre-rows are not the rows of this component.
    #[error("the pre-rows do not match the component: {0}")]
    ComponentMismatch(&'static str),
    /// The pre-rows differ from the digest recorded at the first application.
    #[error("the pre-rows differ from the recorded digest")]
    PreRowsDigestMismatch,
    /// A fragment has gained an end, a distance or a duration since planning.
    #[error("fragment drive {drive_id} is closed")]
    FragmentClosed { drive_id: i32 },
    /// The destination has no end.
    #[error("the destination drive is open")]
    DestinationOpen,
    /// A total cannot be represented in the public schema.
    #[error("the combined {0} cannot be represented")]
    TotalUnrepresentable(&'static str),
    /// The rows offered for unlayering are not what layering the pre-rows gives.
    #[error("the layered rows differ from the layer of the pre-rows")]
    LayerMismatch,
}

fn mismatch<T>(what: &'static str) -> Result<T, LayerError> {
    Err(LayerError::ComponentMismatch(what))
}

fn check_identity(component: &PlannedComponent, pre: &ComponentPreRows) -> Result<(), LayerError> {
    let fragments = &pre.fragments;
    if fragments.is_empty() {
        return mismatch("no fragments");
    }
    if component.destination_drive_id != i64::from(pre.destination.id)
        || pre.materialised_destination.id != i64::from(pre.destination.id)
    {
        return mismatch("destination id");
    }
    let fragment_ids = fragments.iter().map(|row| i64::from(row.id));
    if !fragment_ids.eq(component.fragment_drive_ids.iter().copied()) {
        return mismatch("fragment ids");
    }
    let sample_ids = pre.samples.iter().map(|row| i64::from(row.id));
    if !sample_ids.eq(component.sample_position_ids.iter().copied())
        || pre.samples.len() != fragments.len()
    {
        return mismatch("sample ids");
    }
    if pre
        .samples
        .iter()
        .zip(fragments)
        .any(|(sample, fragment)| sample.drive_id != Some(fragment.id))
    {
        return mismatch("sample parents");
    }
    let materialised_ids = pre.materialised_samples.iter().map(|row| row.id);
    if !materialised_ids.eq(pre.samples.iter().map(|row| i64::from(row.id)))
        || pre.materialised_samples.iter().any(|row| row.drive_id.is_some())
    {
        return mismatch("materialised samples");
    }
    let car = pre.destination.car_id;
    if fragments.iter().any(|row| row.car_id != car)
        || pre.samples.iter().any(|row| row.car_id != car)
        || pre.materialised_destination.car_id != i64::from(car)
        || pre.materialised_samples.iter().any(|row| row.car_id != i64::from(car))
    {
        return mismatch("car id");
    }
    Ok(())
}

fn bits(value: ProjectionFloat64BitsV2_2) -> f64 {
    f64::from_bits(value.0)
}

/// The nearest minute, half away from zero, of a span in microseconds.
fn duration_minutes(start_us: i64, end_us: i64) -> Result<i16, LayerError> {
    let span = end_us
        .checked_sub(start_us)
        .filter(|span| *span >= 0)
        .ok_or(LayerError::TotalUnrepresentable("duration"))?;
    let minutes = span
        .checked_add(30_000_000)
        .ok_or(LayerError::TotalUnrepresentable("duration"))?
        / 60_000_000;
    if minutes > MAX_DURATION_MIN {
        return Err(LayerError::TotalUnrepresentable("duration"));
    }
    i16::try_from(minutes).map_err(|_| LayerError::TotalUnrepresentable("duration"))
}

/// The odometer span: the first fragment's start reading (or, when the row
/// carries none, its sample's) to the destination's end reading.
fn distance_km(pre: &ComponentPreRows) -> Result<(ProjectionFloat64BitsV2_2, f64), LayerError> {
    let unrepresentable = LayerError::TotalUnrepresentable("distance");
    let Some(start_km) = pre.fragments[0].start_km.or(pre.samples[0].odometer) else {
        return Err(unrepresentable);
    };
    let (start, Some(end)) = (bits(start_km), pre.destination.end_km.map(bits)) else {
        return Err(unrepresentable);
    };
    let distance = end - start;
    if start.is_finite() && end.is_finite() && distance.is_finite() && distance >= 0.0 {
        Ok((start_km, distance))
    } else {
        Err(unrepresentable)
    }
}

fn max_of<T: Ord + Copy>(base: Option<T>, values: impl Iterator<Item = Option<T>>) -> Option<T> {
    values.flatten().chain(base).max()
}

fn min_of<T: Ord + Copy>(base: Option<T>, values: impl Iterator<Item = Option<T>>) -> Option<T> {
    values.flatten().chain(base).min()
}

/// The mean of the destination's stored mean (over `count` samples) and the
/// fragment samples' values, rounded to the tenth half away from zero.
fn mean_e1(
    stored: Option<ProjectionFixedNumericV2_2>,
    count: u32,
    values: impl Iterator<Item = Option<ProjectionFixedNumericV2_2>>,
) -> Result<Option<ProjectionFixedNumericV2_2>, LayerError> {
    use ProjectionFixedNumericV2_2::{Finite, NaN};
    let bad = LayerError::TotalUnrepresentable("temperature");
    let stored = match stored {
        Some(NaN) => return Err(bad),
        Some(Finite(value)) => Some(value),
        None => None,
    };
    if stored.is_some() != (count > 0) {
        return mismatch("destination temperature count");
    }
    let mut sum = i128::from(stored.unwrap_or(0)) * i128::from(count);
    let mut total = i128::from(count);
    for value in values.flatten() {
        match value {
            NaN => return Err(bad),
            Finite(value) => {
                sum += i128::from(value);
                total += 1;
            }
        }
    }
    if total == 0 {
        return Ok(None);
    }
    let mean = (2 * sum + sum.signum() * total) / (2 * total);
    i64::try_from(mean).map(|mean| Some(Finite(mean))).map_err(|_| bad)
}

fn mean_f64(
    stored: Option<f64>,
    count: u32,
    values: impl Iterator<Item = Option<f64>>,
) -> Result<Option<f64>, LayerError> {
    let bad = LayerError::TotalUnrepresentable("temperature");
    if stored.is_some_and(|value| !value.is_finite()) {
        return Err(bad);
    }
    if stored.is_some() != (count > 0) {
        return mismatch("destination temperature count");
    }
    let mut sum = stored.unwrap_or(0.0) * f64::from(count);
    let mut total = f64::from(count);
    for value in values.flatten() {
        if !value.is_finite() {
            return Err(bad);
        }
        sum += value;
        total += 1.0;
    }
    if total == 0.0 {
        return Ok(None);
    }
    let mean = (sum / total * 10.0).round() / 10.0;
    mean.is_finite().then_some(Some(mean)).ok_or(bad)
}

fn power_extreme(
    base: Option<f64>,
    values: impl Iterator<Item = Option<f64>>,
    pick: fn(f64, f64) -> f64,
) -> Result<Option<f64>, LayerError> {
    let mut result = base;
    for value in values.flatten() {
        if !value.is_finite() {
            return Err(LayerError::TotalUnrepresentable("power"));
        }
        result = Some(result.map_or(value, |current| pick(current, value)));
    }
    Ok(result)
}

/// Layers one component. `recorded` is the pre-row digest stored at the first
/// application; without it this is the first application.
pub fn layer_component(
    component: &PlannedComponent,
    pre: &ComponentPreRows,
    recorded: Option<&[u8; 32]>,
) -> Result<LayeredComponent, LayerError> {
    check_identity(component, pre)?;
    let digest = pre.digest();
    if recorded.is_some_and(|recorded| *recorded != digest) {
        return Err(LayerError::PreRowsDigestMismatch);
    }
    if let Some(closed) = pre.fragments.iter().find(|row| {
        row.end_date_pg_us.is_some() || row.distance.is_some() || row.duration_min.is_some()
    }) {
        return Err(LayerError::FragmentClosed { drive_id: closed.id });
    }
    let Some(end_us) = pre.destination.end_date_pg_us else {
        return Err(LayerError::DestinationOpen);
    };
    let first = &pre.fragments[0];
    let duration_min = duration_minutes(first.start_date_pg_us, end_us)?;
    let (start_km, distance) = distance_km(pre)?;
    let counts = pre.destination_samples;
    let destination = &pre.destination;
    let samples = &pre.samples;
    let effective_destination = SourceDbDrivePhysicalV2_2 {
        start_date_pg_us: first.start_date_pg_us,
        start_position_id: first.start_position_id,
        start_address_id: first.start_address_id,
        start_geofence_id: first.start_geofence_id,
        start_ideal_range_km_e2: first.start_ideal_range_km_e2,
        start_rated_range_km_e2: first.start_rated_range_km_e2,
        start_km: Some(start_km),
        distance: Some(ProjectionFloat64BitsV2_2::from_f64(distance)),
        duration_min: Some(duration_min),
        outside_temp_avg_e1: mean_e1(
            destination.outside_temp_avg_e1,
            counts.outside_temp,
            samples.iter().map(|s| s.outside_temp_e1),
        )?,
        inside_temp_avg_e1: mean_e1(
            destination.inside_temp_avg_e1,
            counts.inside_temp,
            samples.iter().map(|s| s.inside_temp_e1),
        )?,
        speed_max: max_of(destination.speed_max, samples.iter().map(|s| s.speed)),
        power_max: max_of(destination.power_max, samples.iter().map(|s| s.power)),
        power_min: min_of(destination.power_min, samples.iter().map(|s| s.power)),
        ascent: None,
        descent: None,
        ..destination.clone()
    };
    let materialised = &pre.materialised_destination;
    let detached = &pre.materialised_samples;
    let start_us = first.start_date_pg_us;
    let start_date_ms = start_us
        .checked_add(PG_EPOCH_OFFSET_US)
        .ok_or(LayerError::TotalUnrepresentable("duration"))?
        .div_euclid(1_000);
    let head = &detached[0];
    let after_destination = ProjectionDrive {
        optimized_at_ms: None,
        start_date_ms,
        distance_km: Some(distance),
        duration_min: Some(i64::from(duration_min)),
        efficiency: None,
        outside_temp_avg: mean_f64(
            materialised.outside_temp_avg,
            counts.outside_temp,
            detached.iter().map(|s| s.outside_temp),
        )?,
        inside_temp_avg: mean_f64(
            materialised.inside_temp_avg,
            counts.inside_temp,
            detached.iter().map(|s| s.inside_temp),
        )?,
        speed_max: max_of(materialised.speed_max, detached.iter().map(|s| s.speed)),
        power_max: power_extreme(materialised.power_max, detached.iter().map(|s| s.power), f64::max)?,
        power_min: power_extreme(materialised.power_min, detached.iter().map(|s| s.power), f64::min)?,
        start_ideal_range_km: head.ideal_battery_range_km,
        start_rated_range_km: head.rated_battery_range_km,
        start_address: None,
        start_geofence: None,
        start_latitude: Some(head.latitude),
        start_longitude: Some(head.longitude),
        start_soc: head.battery_level,
        ascent: None,
        descent: None,
        ..materialised.clone()
    };
    Ok(LayeredComponent {
        effective: EffectiveRows {
            destination: effective_destination,
            removed_drive_ids: pre.fragments.iter().map(|row| row.id).collect(),
            samples: samples
                .iter()
                .map(|sample| SourceDbPositionPhysicalV2_2 {
                    drive_id: Some(destination.id),
                    ..sample.clone()
                })
                .collect(),
        },
        after_image: MaterialisedAfterImage {
            destination: after_destination,
            samples: detached
                .iter()
                .map(|sample| ProjectionPosition {
                    drive_id: Some(i64::from(destination.id)),
                    ..sample.clone()
                })
                .collect(),
        },
        terrain_recompute: true,
        pre_rows_digest: digest,
    })
}

/// Returns the pre-layer rows of a layered component, after checking that
/// `pre` is the very rows `layered` was made from and that `layered` is still
/// exactly their layer: rows that drifted are never silently reverted.
pub fn unlayer_component(
    component: &PlannedComponent,
    layered: &LayeredComponent,
    pre: &ComponentPreRows,
) -> Result<ComponentPreRows, LayerError> {
    check_identity(component, pre)?;
    if pre.digest() != layered.pre_rows_digest {
        return Err(LayerError::PreRowsDigestMismatch);
    }
    let expected = layer_component(component, pre, Some(&layered.pre_rows_digest))?;
    if expected != *layered {
        return Err(LayerError::LayerMismatch);
    }
    Ok(pre.clone())
}

#[cfg(test)]
#[path = "layer_tests.rs"]
mod tests;
