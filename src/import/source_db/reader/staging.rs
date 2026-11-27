// SPDX-License-Identifier: AGPL-3.0-only

/// Materialize a sealed capture for isolated compatibility tests only. Normal
/// import publication uses the staged fragment producer and never calls this
/// all-memory helper.
#[cfg(test)]
pub fn materialize_small_staged_history(
    stage: &SourceDbStage,
    maximum_rows: usize,
) -> Result<SourceDbHistory, SourceDbReaderError> {
    let stats = stage.stats()?;
    if stats.row_count > u64::try_from(maximum_rows).expect("usize fits u64") {
        return Err(SourceDbReaderError::MaximumRowsExceeded {
            maximum: maximum_rows,
        });
    }
    Ok(SourceDbHistory {
        cars: collect_staged_rows(stage, SourceDbStageTable::Cars)?,
        drives: collect_staged_rows(stage, SourceDbStageTable::Drives)?,
        positions: collect_staged_rows(stage, SourceDbStageTable::Positions)?,
        charging_processes: collect_staged_rows(stage, SourceDbStageTable::ChargingProcesses)?,
        charges: collect_staged_rows(stage, SourceDbStageTable::Charges)?,
        addresses: collect_staged_rows(stage, SourceDbStageTable::Addresses)?,
        geofences: collect_staged_rows(stage, SourceDbStageTable::Geofences)?,
        states: collect_staged_rows(stage, SourceDbStageTable::States)?,
        updates: collect_staged_rows(stage, SourceDbStageTable::Updates)?,
    })
}

#[cfg(test)]
fn collect_staged_rows<T: DeserializeOwned>(
    stage: &SourceDbStage,
    table: SourceDbStageTable,
) -> Result<Vec<T>, SourceDbReaderError> {
    let mut after_id = 0_i64;
    let mut output = Vec::new();
    loop {
        let page = stage.page(table, after_id, 10_000)?;
        output.extend(page.rows.into_iter().map(|row| row.value));
        match page.next_after_id {
            Some(next_after_id) => after_id = next_after_id,
            None => return Ok(output),
        }
    }
}

const GEOFENCE_GEOMETRY_SQL: &str = r#"
SELECT
  source.id,
  source.name,
  source.latitude::double precision AS latitude,
  source.longitude::double precision AS longitude,
  source.radius::double precision AS radius_m,
  source.billing_type::text AS billing_type,
  source.cost_per_unit::double precision AS cost_per_unit,
  source.session_fee::double precision AS session_fee
FROM public.geofences AS source
JOIN (
  SELECT drive.start_geofence_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.start_geofence_id IS NOT NULL
  UNION
  SELECT drive.end_geofence_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.end_geofence_id IS NOT NULL
  UNION
  SELECT process.geofence_id AS id
  FROM public.charging_processes AS process
  WHERE process.car_id = $3 AND process.geofence_id IS NOT NULL
) AS related ON related.id = source.id
WHERE source.id > $1
ORDER BY source.id ASC
LIMIT $2
"#;

// This is deliberately a sibling of the legacy geometry query.  It is the
// bounded THP2.2 local-candidate source shape only; `read_geofences` and the
// existing source database import compatibility path must keep their legacy f64
// representation and query unchanged.
// This is deliberately a separate physical query from `ADDRESSES_SQL` and
// `read_addresses`. It is bounded local-candidate work only; the existing
// compatibility reader keeps its three-column binary-copy shape unchanged.
// This is deliberately separate from the legacy binary-copy drive reader.
// It retains every selected-car physical source field without completed-row
// filtering, joins, casts, defaults, or time/numeric/float normalization.
// This is deliberately separate from the legacy binary-copy positions reader.
// It retains all selected-car physical source columns without joins, casts,
// defaults, coordinate policy, or timestamp/numeric/FLOAT8 normalization.
// Dedicated source-shaped local-candidate readers for charging history. These
// do not reuse compatibility charge/session projection: every selected-car
// process is direct, while charge rows are scoped only through an extant
// process INNER JOIN. That scope asserts selected ownership only; source
// constraint state is not re-attested by the local physical slice.
// Dedicated signed-id physical source queries for the THP2.2 local candidate.
// They deliberately do not reuse legacy COPY readers or compatibility epoch-ms
// decoders: PostgreSQL timestamp binary i64 microseconds remain raw, including
// the source infinity sentinels.
const DRIVES_V2_2_SQL: &str = r#"
SELECT
  source.id, source.car_id, source.start_date, source.end_date,
  source.start_position_id, source.end_position_id,
  source.start_address_id, source.end_address_id,
  source.start_geofence_id, source.end_geofence_id,
  source.outside_temp_avg::text AS outside_temp_avg,
  source.inside_temp_avg::text AS inside_temp_avg,
  source.speed_max, source.power_max, source.power_min,
  source.start_ideal_range_km::text AS start_ideal_range_km,
  source.end_ideal_range_km::text AS end_ideal_range_km,
  source.start_rated_range_km::text AS start_rated_range_km,
  source.end_rated_range_km::text AS end_rated_range_km,
  source.start_km, source.end_km, source.distance,
  source.duration_min, source.ascent, source.descent
FROM public.drives AS source
WHERE ($1::integer IS NULL OR source.id > $1)
  AND source.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

const POSITIONS_V2_2_SQL: &str = r#"
SELECT
  source.id, source.car_id, source.drive_id, source.date,
  source.latitude::text AS latitude,
  source.longitude::text AS longitude,
  source.elevation, source.speed, source.power, source.odometer,
  source.ideal_battery_range_km::text AS ideal_battery_range_km,
  source.est_battery_range_km::text AS est_battery_range_km,
  source.rated_battery_range_km::text AS rated_battery_range_km,
  source.battery_level, source.usable_battery_level,
  source.battery_heater, source.battery_heater_on, source.battery_heater_no_power,
  source.outside_temp::text AS outside_temp,
  source.inside_temp::text AS inside_temp,
  source.fan_status,
  source.driver_temp_setting::text AS driver_temp_setting,
  source.passenger_temp_setting::text AS passenger_temp_setting,
  source.is_climate_on, source.is_rear_defroster_on, source.is_front_defroster_on,
  source.tpms_pressure_fl::text AS tpms_pressure_fl,
  source.tpms_pressure_fr::text AS tpms_pressure_fr,
  source.tpms_pressure_rl::text AS tpms_pressure_rl,
  source.tpms_pressure_rr::text AS tpms_pressure_rr
FROM public.positions AS source
WHERE ($1::integer IS NULL OR source.id > $1)
  AND source.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

const CHARGING_PROCESSES_V2_2_SQL: &str = r#"
SELECT
  source.id, source.car_id, source.position_id,
  source.address_id, source.geofence_id,
  source.start_date, source.end_date,
  source.charge_energy_added::text AS charge_energy_added,
  source.charge_energy_used::text AS charge_energy_used,
  source.start_ideal_range_km::text AS start_ideal_range_km,
  source.end_ideal_range_km::text AS end_ideal_range_km,
  source.start_rated_range_km::text AS start_rated_range_km,
  source.end_rated_range_km::text AS end_rated_range_km,
  source.start_battery_level, source.end_battery_level,
  source.duration_min,
  source.outside_temp_avg::text AS outside_temp_avg,
  source.cost::text AS cost
FROM public.charging_processes AS source
WHERE ($1::integer IS NULL OR source.id > $1)
  AND source.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

const CHARGES_V2_2_SQL: &str = r#"
SELECT
  source.id, source.charging_process_id, source.date,
  source.battery_heater, source.battery_heater_on,
  source.battery_heater_no_power,
  source.battery_level, source.usable_battery_level,
  source.charge_energy_added::text AS charge_energy_added,
  source.charger_actual_current, source.charger_phases,
  source.charger_pilot_current, source.charger_power,
  source.charger_voltage, source.conn_charge_cable,
  source.fast_charger_present, source.fast_charger_brand,
  source.fast_charger_type,
  source.ideal_battery_range_km::text AS ideal_battery_range_km,
  source.rated_battery_range_km::text AS rated_battery_range_km,
  source.not_enough_power_to_heat,
  source.outside_temp::text AS outside_temp
FROM public.charges AS source
INNER JOIN public.charging_processes AS process
  ON process.id = source.charging_process_id
WHERE ($1::integer IS NULL OR source.id > $1)
  AND process.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

const ADDRESSES_V2_2_SQL: &str = r#"
SELECT
  source.id, source.display_name,
  source.latitude::text AS latitude,
  source.longitude::text AS longitude,
  source.name, source.house_number, source.road, source.neighbourhood,
  source.city, source.county, source.postcode, source.state,
  source.state_district, source.country,
  source.inserted_at, source.updated_at, source.osm_id, source.osm_type
FROM public.addresses AS source
INNER JOIN (
  SELECT drive.start_address_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.start_address_id IS NOT NULL
  UNION
  SELECT drive.end_address_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.end_address_id IS NOT NULL
  UNION
  SELECT process.address_id AS id
  FROM public.charging_processes AS process
  WHERE process.car_id = $3 AND process.address_id IS NOT NULL
) AS related ON related.id = source.id
WHERE ($1::integer IS NULL OR source.id > $1)
ORDER BY source.id ASC
LIMIT $2
"#;

const GEOFENCES_V2_2_SQL: &str = r#"
SELECT
  source.id, source.name,
  source.latitude::text AS latitude,
  source.longitude::text AS longitude,
  source.radius, source.billing_type::text AS billing_type,
  source.cost_per_unit::text AS cost_per_unit,
  source.session_fee::text AS session_fee,
  source.inserted_at, source.updated_at
FROM public.geofences AS source
INNER JOIN (
  SELECT drive.start_geofence_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.start_geofence_id IS NOT NULL
  UNION
  SELECT drive.end_geofence_id AS id
  FROM public.drives AS drive
  WHERE drive.car_id = $3 AND drive.end_geofence_id IS NOT NULL
  UNION
  SELECT process.geofence_id AS id
  FROM public.charging_processes AS process
  WHERE process.car_id = $3 AND process.geofence_id IS NOT NULL
) AS related ON related.id = source.id
WHERE ($1::integer IS NULL OR source.id > $1)
ORDER BY source.id ASC
LIMIT $2
"#;

const STATES_V2_2_SQL: &str = r#"
SELECT
  source.id,
  source.car_id,
  source.state::text AS state,
  source.start_date,
  source.end_date
FROM public.states AS source
WHERE ($1::integer IS NULL OR source.id > $1)
  AND source.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

#[allow(dead_code)] // local candidate only; import/publication wiring is deliberately absent.
const UPDATES_V2_2_SQL: &str = r#"
SELECT
  source.id,
  source.car_id,
  source.start_date,
  source.end_date,
  source.version
FROM public.updates AS source
WHERE ($1::integer IS NULL OR source.id > $1)
  AND source.car_id = $3
ORDER BY source.id ASC
LIMIT $2
"#;

// Source-wide singleton settings are intentionally separate from selected-car
// history. Cast only the four PostgreSQL enum values to text so the physical
// reader can decode their reviewed labels without a global enum codec.
#[allow(dead_code)] // local candidate only; import/publication wiring is deliberately absent.
const SETTINGS_V2_2_SQL: &str = r#"
SELECT
  source.id,
  source.unit_of_length::text AS unit_of_length,
  source.unit_of_temperature::text AS unit_of_temperature,
  source.unit_of_pressure::text AS unit_of_pressure,
  source.preferred_range::text AS preferred_range,
  source.base_url,
  source.grafana_url,
  source.language,
  source.theme_mode,
  source.inserted_at,
  source.updated_at
FROM public.settings AS source
ORDER BY source.id ASC
LIMIT 2
"#;

const CAR_V2_2_SQL: &str = r#"
SELECT
  source.id,
  source.eid,
  source.vid,
  source.vin,
  source.name,
  source.model,
  source.efficiency,
  source.trim_badging,
  source.marketing_name,
  source.exterior_color,
  source.wheel_type,
  source.spoiler_type,
  source.display_priority,
  source.inserted_at,
  source.updated_at,
  source.settings_id
FROM public.cars AS source
WHERE source.id = $1
ORDER BY source.id ASC
LIMIT 1
"#;

const CAR_SETTINGS_V2_2_SQL: &str = r#"
SELECT
  source.id AS car_settings_row_id,
  source.suspend_min,
  source.suspend_after_idle_min,
  source.req_not_unlocked,
  source.free_supercharging,
  source.use_streaming_api,
  source.enabled,
  source.lfp_battery
FROM public.car_settings AS source
WHERE source.id = $1
ORDER BY source.id ASC
LIMIT 1
"#;

// This is a dedicated, selected-car physical source relation for the THP2.2
// local candidate. It deliberately does not reuse the legacy `CARS` query or
// its lossy/default-resolving decoder, and it never joins global `settings`.
#[allow(dead_code)] // local candidate only; import/publication wiring is deliberately absent.
const CARS_AND_CAR_SETTINGS_V2_2_SQL: &str = r#"
SELECT
  source.id,
  source.eid,
  source.vid,
  source.vin,
  source.name,
  source.model,
  source.efficiency,
  source.trim_badging,
  source.marketing_name,
  source.exterior_color,
  source.wheel_type,
  source.spoiler_type,
  source.display_priority,
  source.inserted_at,
  source.updated_at,
  source.settings_id,
  car_settings.id AS car_settings_row_id,
  car_settings.suspend_min,
  car_settings.suspend_after_idle_min,
  car_settings.req_not_unlocked,
  car_settings.free_supercharging,
  car_settings.use_streaming_api,
  car_settings.enabled,
  car_settings.lfp_battery
FROM public.cars AS source
INNER JOIN public.car_settings AS car_settings ON car_settings.id = source.settings_id
WHERE source.id = $1
ORDER BY source.id ASC
LIMIT 1
"#;
