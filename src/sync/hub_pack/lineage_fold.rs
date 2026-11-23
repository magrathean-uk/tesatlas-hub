// SPDX-License-Identifier: AGPL-3.0-only

// The fold of a served 1.x compatibility lineage (issue #106). A client
// applies a lineage as: every base pack, then every delta in sequence order;
// per table and key the last mention wins and a tombstone removes the row. A
// row repeated across base packs (each pack is self-contained, so it repeats
// its car and the parents of its children) must be identical. This file
// applies exactly that rule to a private scratch SQLite under
// `packs/.staging`, cuts the result into self-contained schema-2.1 fragments
// and reads freshly written packs back against the scratch rows. It never
// reads the raw stage, a kept capture or the materialised read model, and it
// writes nothing to the catalogue. This file is `include!`d into
// `hub_pack.rs` through `validation_helpers.rs`.

/// The staging suffix of the scratch fold; swept at store opening.
const LINEAGE_FOLD_SCRATCH: &str = "lineage-fold.sqlite";
/// The staging suffix of one decoded pack while it is folded or read back.
const LINEAGE_FOLD_PACK: &str = "lineage-fold-pack.sqlite";

#[cfg(test)]
thread_local! {
    /// Test only (REV-CX-ISS106 finding 3): the decoded copy of a pack is
    /// created, but every write to it fails, as on a full scratch volume.
    pub(crate) static LINEAGE_FOLD_DECODED_WRITE_FAILS: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FoldColumn {
    Integer,
    Real,
    Text,
}

/// One mirror table of the compatibility schema: its key, its columns as
/// schema 2.1 lays them out, and the columns a schema-2.0 base pack has.
struct FoldTable {
    name: &'static str,
    key: &'static str,
    columns: &'static [(&'static str, FoldColumn)],
    /// `None`: the table is absent from a schema-2.0 pack.
    schema_2_0: Option<&'static [&'static str]>,
    /// The tombstone entity that removes a row of this table.
    tombstone: Option<&'static str>,
}

use lineage_fold_schema::FOLD_TABLES;

mod lineage_fold_schema {
    use super::FoldColumn::{Integer as I, Real as R, Text as T};
    use super::FoldTable;

pub(super) const FOLD_TABLES: [FoldTable; 8] = [
    FoldTable {
        name: "cars",
        key: "id",
        columns: &[
            ("id", I),
            ("name", T),
            ("model", T),
            ("vin", T),
            ("source_eid", I),
            ("source_vid", I),
            ("trim_badging", T),
            ("marketing_name", T),
            ("exterior_color", T),
            ("wheel_type", T),
            ("spoiler_type", T),
            ("firmware_version", T),
            ("efficiency_wh_per_km", R),
        ],
        schema_2_0: Some(&["id", "name", "model", "vin", "firmware_version", "efficiency_wh_per_km"]),
        tombstone: None,
    },
    FoldTable {
        name: "car_settings",
        key: "car_id",
        columns: &[
            ("car_id", I),
            ("enabled", I),
            ("use_streaming_api", I),
            ("suspend_after_idle_min", I),
            ("suspend_min", I),
            ("suspend_min_resolved", I),
            ("req_not_unlocked", I),
            ("free_supercharging", I),
            ("lfp_battery", I),
        ],
        schema_2_0: None,
        tombstone: None,
    },
    FoldTable {
        name: "drives",
        key: "id",
        columns: &[
            ("id", I),
            ("car_id", I),
            ("optimized_at_ms", I),
            ("start_date_ms", I),
            ("end_date_ms", I),
            ("distance_km", R),
            ("duration_min", I),
            ("efficiency", R),
            ("outside_temp_avg", R),
            ("inside_temp_avg", R),
            ("speed_max", I),
            ("power_max", R),
            ("power_min", R),
            ("start_ideal_range_km", R),
            ("end_ideal_range_km", R),
            ("start_address", T),
            ("end_address", T),
            ("start_geofence", T),
            ("end_geofence", T),
            ("start_latitude", R),
            ("start_longitude", R),
            ("end_latitude", R),
            ("end_longitude", R),
            ("start_soc", I),
            ("end_soc", I),
            ("start_rated_range_km", R),
            ("end_rated_range_km", R),
            ("ascent", I),
            ("descent", I),
        ],
        schema_2_0: Some(&[
            "id",
            "car_id",
            "optimized_at_ms",
            "start_date_ms",
            "end_date_ms",
            "distance_km",
            "duration_min",
            "efficiency",
            "outside_temp_avg",
            "speed_max",
            "start_address",
            "end_address",
            "start_geofence",
            "end_geofence",
            "start_latitude",
            "start_longitude",
            "end_latitude",
            "end_longitude",
            "start_soc",
            "end_soc",
            "start_rated_range_km",
            "end_rated_range_km",
        ]),
        tombstone: Some("drive"),
    },
    FoldTable {
        name: "charges",
        key: "id",
        columns: &[
            ("id", I),
            ("car_id", I),
            ("start_date_ms", I),
            ("end_date_ms", I),
            ("charge_energy_added", R),
            ("charge_energy_used_kwh", R),
            ("start_ideal_range_km", R),
            ("end_ideal_range_km", R),
            ("cost", R),
            ("fast_charger_type", T),
            ("billing_type", T),
            ("cost_per_unit", R),
            ("session_fee", R),
            ("start_latitude", R),
            ("start_longitude", R),
            ("start_battery_level", I),
            ("end_battery_level", I),
            ("duration_min", I),
            ("address", T),
            ("location_name", T),
            ("geofence", T),
            ("is_dc", I),
            ("charge_rate_km_per_hour", R),
            ("max_charger_power_kw", R),
            ("outside_temp_avg", R),
            ("start_rated_range_km", R),
            ("end_rated_range_km", R),
        ],
        schema_2_0: Some(&[
            "id",
            "car_id",
            "start_date_ms",
            "end_date_ms",
            "charge_energy_added",
            "start_battery_level",
            "end_battery_level",
            "duration_min",
            "address",
            "location_name",
            "geofence",
            "is_dc",
            "charge_rate_km_per_hour",
            "max_charger_power_kw",
            "outside_temp_avg",
            "start_rated_range_km",
            "end_rated_range_km",
        ]),
        tombstone: Some("charge"),
    },
    FoldTable {
        name: "positions",
        key: "id",
        columns: &[
            ("id", I),
            ("drive_id", I),
            ("car_id", I),
            ("date_ms", I),
            ("latitude", R),
            ("longitude", R),
            ("speed", I),
            ("power", R),
            ("battery_level", I),
            ("usable_battery_level", I),
            ("elevation", I),
            ("odometer", R),
            ("ideal_battery_range_km", R),
            ("est_battery_range_km", R),
            ("rated_battery_range_km", R),
            ("fan_status", I),
            ("driver_temp_setting", R),
            ("passenger_temp_setting", R),
            ("is_climate_on", I),
            ("is_rear_defroster_on", I),
            ("is_front_defroster_on", I),
            ("inside_temp", R),
            ("outside_temp", R),
            ("battery_heater", I),
            ("battery_heater_on", I),
            ("battery_heater_no_power", I),
            ("tpms_pressure_fl", R),
            ("tpms_pressure_fr", R),
            ("tpms_pressure_rl", R),
            ("tpms_pressure_rr", R),
        ],
        schema_2_0: Some(&[
            "id",
            "drive_id",
            "car_id",
            "date_ms",
            "latitude",
            "longitude",
            "speed",
            "power",
            "battery_level",
            "usable_battery_level",
            "elevation",
            "odometer",
            "ideal_battery_range_km",
            "rated_battery_range_km",
            "is_climate_on",
            "inside_temp",
            "outside_temp",
        ]),
        tombstone: Some("position"),
    },
    FoldTable {
        name: "charge_samples",
        key: "id",
        columns: &[
            ("id", I),
            ("charge_process_id", I),
            ("timestamp_ms", I),
            ("battery_level", I),
            ("usable_battery_level", I),
            ("charge_energy_added_kwh", R),
            ("charger_power_kw", R),
            ("charger_voltage", R),
            ("charger_actual_current", R),
            ("charger_pilot_current", R),
            ("charger_phases", I),
            ("ideal_range_km", R),
            ("rated_range_km", R),
            ("outside_temp_c", R),
            ("battery_heater_on", I),
            ("battery_heater", I),
            ("battery_heater_no_power", I),
            ("not_enough_power_to_heat", I),
            ("fast_charger_present", I),
            ("fast_charger_brand", T),
            ("fast_charger_type", T),
            ("charge_cable", T),
        ],
        schema_2_0: Some(&[
            "id",
            "charge_process_id",
            "timestamp_ms",
            "battery_level",
            "usable_battery_level",
            "charge_energy_added_kwh",
            "charger_power_kw",
            "charger_voltage",
            "charger_actual_current",
            "charger_pilot_current",
            "charger_phases",
            "ideal_range_km",
            "rated_range_km",
            "outside_temp_c",
            "battery_heater_on",
            "battery_heater",
            "battery_heater_no_power",
            "not_enough_power_to_heat",
            "fast_charger_present",
            "fast_charger_brand",
            "fast_charger_type",
            "charge_cable",
        ]),
        tombstone: Some("charge_sample"),
    },
    FoldTable {
        name: "states",
        key: "id",
        columns: &[
            ("id", I),
            ("car_id", I),
            ("state", T),
            ("start_date_ms", I),
            ("end_date_ms", I),
        ],
        schema_2_0: None,
        tombstone: Some("state"),
    },
    FoldTable {
        name: "updates",
        key: "id",
        columns: &[
            ("id", I),
            ("car_id", I),
            ("start_date_ms", I),
            ("end_date_ms", I),
            ("version", T),
        ],
        schema_2_0: None,
        tombstone: Some("update"),
    },
];
}

impl FoldTable {
    fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The columns this pack's layout holds, in pack order.
    fn pack_columns(&self, schema: SchemaVersion) -> Option<Vec<&'static str>> {
        if schema == HUB_PROJECTION_SCHEMA_V1 {
            self.schema_2_0.map(<[&str]>::to_vec)
        } else {
            Some(self.columns.iter().map(|(name, _)| *name).collect())
        }
    }

    /// A select list that reads a pack of `schema` as schema-2.1 rows: a
    /// column a schema-2.0 pack lacks reads as NULL.
    fn select_from_pack(&self, schema: SchemaVersion) -> String {
        let present = self.pack_columns(schema).unwrap_or_default();
        self.columns
            .iter()
            .map(|(name, _)| {
                if present.contains(name) {
                    format!("{name} AS {name}")
                } else {
                    format!("NULL AS {name}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn scratch_ddl(&self) -> String {
        let columns = self
            .columns
            .iter()
            .map(|(name, kind)| {
                let kind = match kind {
                    FoldColumn::Integer => "INTEGER",
                    FoldColumn::Real => "REAL",
                    FoldColumn::Text => "TEXT",
                };
                if *name == self.key {
                    format!("{name} {kind} NOT NULL")
                } else {
                    format!("{name} {kind}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE {} ({columns}, PRIMARY KEY({})) STRICT, WITHOUT ROWID;",
            self.name, self.key
        )
    }
}

/// Why a fold or its readback stopped. `Damaged` and `Inexact` are
/// refusals of this head; `Scratch` is a local failure worth retrying.
#[derive(Debug, Error)]
pub(crate) enum LineageFoldError {
    #[error("{0}")]
    Damaged(String),
    #[error("{0}")]
    Inexact(String),
    #[error("{0}")]
    Scratch(ProjectionPackError),
    /// The caller asked to stop; nothing about the head is decided.
    #[error("the fold was stopped")]
    Stopped,
}

fn fold_scratch(error: rusqlite::Error) -> LineageFoldError {
    LineageFoldError::Scratch(ProjectionPackError::Insert(error))
}

fn fold_inexact(message: impl Into<String>) -> LineageFoldError {
    LineageFoldError::Inexact(message.into())
}

fn fold_damaged(pack: &TransportPack, message: impl std::fmt::Display) -> LineageFoldError {
    LineageFoldError::Damaged(format!("pack {}: {message}", pack.sha256))
}

/// Rows per table in the fold, in [`FOLD_TABLES`] order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct LineageFoldCounts(pub(crate) Vec<(&'static str, u64)>);

/// One self-contained schema-2.1 fragment of the fold: the car, the parents
/// of its children, and its rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LineageFoldFragment {
    pub(crate) snapshot: ProjectionSnapshot,
    pub(crate) states: Vec<ProjectionState>,
    pub(crate) updates: Vec<ProjectionUpdate>,
}

/// The scratch fold of one lineage. Dropping it removes the scratch file.
pub(crate) struct LineageFold {
    staging_dir: PathBuf,
    binding: ProjectionBinding,
    connection: Connection,
    scratch: StagedFile,
    finished: bool,
}

impl std::fmt::Debug for LineageFold {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LineageFold")
            .field("scratch", &self.scratch.path())
            .field("vehicle_id", &self.binding.vehicle_id)
            .finish()
    }
}

impl LineageFold {
    /// A new, empty scratch fold under `packs_dir/.staging`.
    pub(crate) fn create(
        packs_dir: &Path,
        binding: ProjectionBinding,
    ) -> Result<Self, LineageFoldError> {
        let staging_dir = packs_dir.join(".staging");
        ensure_private_staging_directory(&staging_dir).map_err(LineageFoldError::Scratch)?;
        let scratch = StagedFile::create(&staging_dir, LINEAGE_FOLD_SCRATCH)
            .map_err(LineageFoldError::Scratch)?;
        let connection = Connection::open_with_flags(
            scratch.path(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| LineageFoldError::Scratch(ProjectionPackError::OpenSqlite(error)))?;
        let mut schema = String::from(
            "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA trusted_schema = OFF;
             PRAGMA temp_store = FILE;",
        );
        for table in &FOLD_TABLES {
            schema.push_str(&table.scratch_ddl());
        }
        schema.push_str(
            "CREATE TABLE readback_seen (tbl TEXT NOT NULL, id INTEGER NOT NULL,
                 PRIMARY KEY(tbl, id)) STRICT, WITHOUT ROWID;",
        );
        connection
            .execute_batch(&schema)
            .map_err(|error| LineageFoldError::Scratch(ProjectionPackError::CreateSchema(error)))?;
        Ok(Self {
            staging_dir,
            binding,
            connection,
            scratch,
            finished: false,
        })
    }

    /// Folds one base pack of `path` in. A row that an earlier base pack
    /// already holds must be identical, else the fold is refused.
    pub(crate) fn apply_base_pack(
        &self,
        pack: &TransportPack,
        path: &Path,
    ) -> Result<(), LineageFoldError> {
        if pack.schema != HUB_PROJECTION_SCHEMA_V1 && pack.schema != HUB_PROJECTION_SCHEMA_V2 {
            return Err(fold_damaged(pack, "base pack schema has no typed fold"));
        }
        let decoded = self.attach_pack(pack, path, "full_snapshot")?;
        let result = self.fold_attached_base(pack);
        self.detach_pack(decoded, result)
    }

    fn fold_attached_base(&self, pack: &TransportPack) -> Result<(), LineageFoldError> {
        self.connection.execute_batch("BEGIN").map_err(fold_scratch)?;
        for table in &FOLD_TABLES {
            if table.pack_columns(pack.schema).is_none() {
                continue;
            }
            let select = table.select_from_pack(pack.schema);
            let comparison = table
                .columns
                .iter()
                .map(|(name, _)| format!("held.{name} IS incoming.{name}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let conflict: Option<i64> = self
                .connection
                .query_row(
                    &format!(
                        "SELECT incoming.{key} FROM (SELECT {select} FROM pack.{name}) AS incoming
                           JOIN main.{name} AS held ON held.{key} = incoming.{key}
                          WHERE NOT ({comparison}) LIMIT 1",
                        key = table.key,
                        name = table.name,
                    ),
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(fold_scratch)?;
            if let Some(id) = conflict {
                return Err(fold_inexact(format!(
                    "base packs disagree on {} {id}",
                    table.name
                )));
            }
            self.connection
                .execute(
                    &format!(
                        "INSERT OR IGNORE INTO main.{name} ({columns}) SELECT {select} FROM pack.{name}",
                        name = table.name,
                        columns = table.column_list(),
                    ),
                    [],
                )
                .map_err(fold_scratch)?;
        }
        self.connection.execute_batch("COMMIT").map_err(fold_scratch)
    }

    /// Folds one typed delta of `path` in: its rows replace the held rows of
    /// the same key and its tombstones remove theirs.
    pub(crate) fn apply_delta_pack(
        &self,
        pack: &TransportPack,
        path: &Path,
    ) -> Result<(), LineageFoldError> {
        if pack.schema != HUB_PROJECTION_SCHEMA_V2 {
            return Err(fold_damaged(pack, "delta pack is not schema 2.1"));
        }
        let decoded = self.attach_pack(pack, path, "typed_delta")?;
        let result = self.fold_attached_delta(pack);
        self.detach_pack(decoded, result)
    }

    fn fold_attached_delta(&self, pack: &TransportPack) -> Result<(), LineageFoldError> {
        let unknown: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pack.tombstones WHERE entity NOT IN
                    ('drive', 'position', 'charge', 'charge_sample', 'state', 'update'))",
                [],
                |row| row.get(0),
            )
            .map_err(fold_scratch)?;
        if unknown {
            return Err(fold_damaged(pack, "tombstone names an entity without rows"));
        }
        self.connection.execute_batch("BEGIN").map_err(fold_scratch)?;
        for table in &FOLD_TABLES {
            let columns = table.column_list();
            self.connection
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO main.{name} ({columns})
                         SELECT {columns} FROM pack.{name}",
                        name = table.name,
                    ),
                    [],
                )
                .map_err(fold_scratch)?;
            if let Some(entity) = table.tombstone {
                self.connection
                    .execute(
                        &format!(
                            "DELETE FROM main.{name} WHERE {key} IN
                               (SELECT entity_id FROM pack.tombstones WHERE entity = ?1)",
                            name = table.name,
                            key = table.key,
                        ),
                        params![entity],
                    )
                    .map_err(fold_scratch)?;
            }
        }
        self.connection.execute_batch("COMMIT").map_err(fold_scratch)
    }

    /// Closes the fold: it must be one vehicle's representable history.
    /// A car without settings (a schema-2.0 base, which has no settings
    /// table, and no later change of them) is not: a client holds no settings
    /// row and a schema-2.1 base always serves one, so such a fold is
    /// refused rather than given a row the lineage never served
    /// (REV-CX-ISS106 finding 5). Returns the rows per table.
    pub(crate) fn finish(&mut self) -> Result<LineageFoldCounts, LineageFoldError> {
        let unsettled: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM cars WHERE id NOT IN (SELECT car_id FROM car_settings))",
                [],
                |row| row.get(0),
            )
            .map_err(fold_scratch)?;
        if unsettled {
            return Err(fold_inexact(
                "the history holds no settings for the car, which a schema-2.1 base cannot omit",
            ));
        }
        let selected = self.binding.selected_car_id;
        let cars: Vec<i64> = self
            .connection
            .prepare("SELECT id FROM cars ORDER BY id")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(fold_scratch)?;
        if cars != [selected] {
            return Err(fold_inexact("the fold does not hold exactly the selected car"));
        }
        for (table, column) in [
            ("car_settings", "car_id"),
            ("drives", "car_id"),
            ("charges", "car_id"),
            ("positions", "car_id"),
            ("states", "car_id"),
            ("updates", "car_id"),
        ] {
            let foreign: bool = self
                .connection
                .query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} IS NOT ?1)"),
                    params![selected],
                    |row| row.get(0),
                )
                .map_err(fold_scratch)?;
            if foreign {
                return Err(fold_inexact(format!("{table} holds a row of another car")));
            }
        }
        let orphan_position: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM positions WHERE drive_id IS NOT NULL
                                  AND drive_id NOT IN (SELECT id FROM drives))",
                [],
                |row| row.get(0),
            )
            .map_err(fold_scratch)?;
        if orphan_position {
            return Err(fold_inexact("a position names a drive the history removed"));
        }
        let orphan_sample: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM charge_samples
                                WHERE charge_process_id NOT IN (SELECT id FROM charges))",
                [],
                |row| row.get(0),
            )
            .map_err(fold_scratch)?;
        if orphan_sample {
            return Err(fold_inexact("a charge sample names a charge the history removed"));
        }
        self.connection
            .execute_batch(
                "CREATE INDEX positions_by_drive ON positions(drive_id, id);
                 CREATE INDEX samples_by_charge ON charge_samples(charge_process_id, id);",
            )
            .map_err(fold_scratch)?;
        self.finished = true;
        self.counts()
    }

    /// Rows per table, in [`FOLD_TABLES`] order.
    pub(crate) fn counts(&self) -> Result<LineageFoldCounts, LineageFoldError> {
        FOLD_TABLES
            .iter()
            .map(|table| {
                self.connection
                    .query_row(&format!("SELECT COUNT(*) FROM {}", table.name), [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(fold_scratch)
                    .map(|count| (table.name, u64::try_from(count).unwrap_or(0)))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(LineageFoldCounts)
    }

    /// Cuts the finished fold into self-contained fragments of at most
    /// `rows_per_pack` rows (at least three: the car, a parent and a child)
    /// and hands each to `emit` in order: drives with their positions,
    /// positions without a drive, charges with their samples, states, then
    /// updates. A parent is repeated in every fragment that holds one of its
    /// children. A fold of the car alone is one fragment.
    pub(crate) fn for_each_fragment<E: From<LineageFoldError>>(
        &self,
        rows_per_pack: u64,
        mut emit: impl FnMut(LineageFoldFragment) -> Result<(), E>,
    ) -> Result<(), E> {
        if !self.finished {
            return Err(fold_inexact("the fold is not finished").into());
        }
        let target = rows_per_pack.max(3);
        let car = self.car()?;
        let mut builder = FragmentBuilder::new(car);
        let mut emitted = 0_u64;
        let mut flush = |builder: &mut FragmentBuilder, emitted: &mut u64| -> Result<(), E> {
            *emitted += 1;
            emit(builder.take())
        };

        let mut drives = self.prepare("SELECT * FROM drives ORDER BY id")?;
        let mut drive_positions =
            self.prepare("SELECT * FROM positions WHERE drive_id = ?1 ORDER BY id")?;
        let mut rows = drives.query([]).map_err(fold_scratch)?;
        while let Some(row) = rows.next().map_err(fold_scratch)? {
            let drive = decode_row(row, drive_from_row)?;
            if builder.rows() + 1 > target && builder.has_rows() {
                flush(&mut builder, &mut emitted)?;
            }
            builder.push_drive(drive.clone());
            let mut children = drive_positions.query(params![drive.id]).map_err(fold_scratch)?;
            while let Some(child) = children.next().map_err(fold_scratch)? {
                let position = decode_row(child, position_from_row)?;
                if builder.rows() + 1 > target {
                    flush(&mut builder, &mut emitted)?;
                    builder.push_drive(drive.clone());
                }
                builder.positions.push(position);
            }
        }
        drop(rows);

        let mut loose = self.prepare("SELECT * FROM positions WHERE drive_id IS NULL ORDER BY id")?;
        let mut rows = loose.query([]).map_err(fold_scratch)?;
        while let Some(row) = rows.next().map_err(fold_scratch)? {
            let position = decode_row(row, position_from_row)?;
            if builder.rows() + 1 > target {
                flush(&mut builder, &mut emitted)?;
            }
            builder.positions.push(position);
        }
        drop(rows);

        let mut charges = self.prepare("SELECT * FROM charges ORDER BY id")?;
        let mut charge_samples =
            self.prepare("SELECT * FROM charge_samples WHERE charge_process_id = ?1 ORDER BY id")?;
        let mut rows = charges.query([]).map_err(fold_scratch)?;
        while let Some(row) = rows.next().map_err(fold_scratch)? {
            let charge = decode_row(row, charge_from_row)?;
            if builder.rows() + 1 > target && builder.has_rows() {
                flush(&mut builder, &mut emitted)?;
            }
            builder.push_charge(charge.clone());
            let mut children = charge_samples.query(params![charge.id]).map_err(fold_scratch)?;
            while let Some(child) = children.next().map_err(fold_scratch)? {
                let sample = decode_row(child, charge_sample_from_row)?;
                if builder.rows() + 1 > target {
                    flush(&mut builder, &mut emitted)?;
                    builder.push_charge(charge.clone());
                }
                builder.snapshot_samples.push(sample);
            }
        }
        drop(rows);

        let mut states = self.prepare("SELECT * FROM states ORDER BY id")?;
        let mut rows = states.query([]).map_err(fold_scratch)?;
        while let Some(row) = rows.next().map_err(fold_scratch)? {
            let state = decode_row(row, state_from_row)?;
            if builder.rows() + 1 > target {
                flush(&mut builder, &mut emitted)?;
            }
            builder.states.push(state);
        }
        drop(rows);

        let mut updates = self.prepare("SELECT * FROM updates ORDER BY id")?;
        let mut rows = updates.query([]).map_err(fold_scratch)?;
        while let Some(row) = rows.next().map_err(fold_scratch)? {
            let update = decode_row(row, update_from_row)?;
            if builder.rows() + 1 > target {
                flush(&mut builder, &mut emitted)?;
            }
            builder.updates.push(update);
        }
        drop(rows);

        if builder.has_rows() || emitted == 0 {
            flush(&mut builder, &mut emitted)?;
        }
        Ok(())
    }

    /// Reads each written pack back (digest, length and layout verified as a
    /// client would) and requires every row to equal its fold row column for
    /// column (REAL by bits, NULL kept) and every fold row to be in some pack.
    /// `stop` is asked before each pack; it ends the readback with `Stopped`.
    pub(crate) fn verify_readback(
        &mut self,
        packs: &[(TransportPack, PathBuf)],
        stop: &dyn Fn() -> bool,
    ) -> Result<(), LineageFoldError> {
        if !self.finished {
            return Err(fold_inexact("the fold is not finished"));
        }
        self.connection
            .execute("DELETE FROM readback_seen", [])
            .map_err(fold_scratch)?;
        for (pack, path) in packs {
            if stop() {
                return Err(LineageFoldError::Stopped);
            }
            if pack.schema != HUB_PROJECTION_SCHEMA_V2 {
                return Err(fold_inexact("a fresh base pack is not schema 2.1"));
            }
            let decoded = self.attach_pack(pack, path, "full_snapshot")?;
            let result = self
                .connection
                .execute_batch("BEGIN")
                .map_err(fold_scratch)
                .and_then(|()| self.compare_attached_pack(pack))
                .and_then(|()| self.connection.execute_batch("COMMIT").map_err(fold_scratch));
            self.detach_pack(decoded, result)?;
        }
        for table in &FOLD_TABLES {
            let (seen, held): (i64, i64) = self
                .connection
                .query_row(
                    &format!(
                        "SELECT (SELECT COUNT(*) FROM readback_seen WHERE tbl = ?1),
                                (SELECT COUNT(*) FROM {})",
                        table.name
                    ),
                    params![table.name],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(fold_scratch)?;
            if seen != held {
                return Err(fold_inexact(format!(
                    "the fresh base holds {seen} {} rows, the history {held}",
                    table.name
                )));
            }
        }
        Ok(())
    }

    fn compare_attached_pack(&self, pack: &TransportPack) -> Result<(), LineageFoldError> {
        let mut record = self
            .connection
            .prepare("INSERT OR IGNORE INTO readback_seen (tbl, id) VALUES (?1, ?2)")
            .map_err(fold_scratch)?;
        for table in &FOLD_TABLES {
            let columns = table.column_list();
            let width = table.columns.len();
            let mut written = self
                .connection
                .prepare(&format!("SELECT {columns} FROM pack.{}", table.name))
                .map_err(fold_scratch)?;
            let mut held = self
                .connection
                .prepare(&format!(
                    "SELECT {columns} FROM main.{} WHERE {} = ?1",
                    table.name, table.key
                ))
                .map_err(fold_scratch)?;
            let mut rows = written.query([]).map_err(fold_scratch)?;
            while let Some(row) = rows.next().map_err(fold_scratch)? {
                let values = (0..width)
                    .map(|index| row.get::<_, rusqlite::types::Value>(index))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(fold_scratch)?;
                let key = match values.first() {
                    Some(rusqlite::types::Value::Integer(key)) => *key,
                    _ => return Err(fold_inexact(format!("{} row without a key", table.name))),
                };
                let expected = held
                    .query_row(params![key], |row| {
                        (0..width)
                            .map(|index| row.get::<_, rusqlite::types::Value>(index))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .optional()
                    .map_err(fold_scratch)?;
                let Some(expected) = expected else {
                    return Err(fold_inexact(format!(
                        "pack {} holds {} {key}, which the history does not",
                        pack.sha256, table.name
                    )));
                };
                if !values
                    .iter()
                    .zip(&expected)
                    .all(|(left, right)| fold_value_identical(left, right))
                {
                    return Err(fold_inexact(format!(
                        "pack {} changes {} {key}",
                        pack.sha256, table.name
                    )));
                }
                record
                    .execute(params![table.name, key])
                    .map_err(fold_scratch)?;
            }
        }
        Ok(())
    }

    fn prepare(&self, sql: &str) -> Result<rusqlite::Statement<'_>, LineageFoldError> {
        self.connection.prepare(sql).map_err(fold_scratch)
    }

    fn car(&self) -> Result<ProjectionCar, LineageFoldError> {
        self.connection
            .query_row(
                "SELECT cars.*, car_settings.* FROM cars
                   JOIN car_settings ON car_settings.car_id = cars.id",
                [],
                |row| Ok(car_from_row(row)),
            )
            .map_err(fold_scratch)?
            .map_err(|error| fold_inexact(format!("car row: {error}")))
    }

    /// Decodes `pack` from `path` into a private staging file, verifying its
    /// digest, compressed and decoded lengths in the same read, and attaches
    /// it as `pack` after its identity and layout are checked.
    fn attach_pack(
        &self,
        pack: &TransportPack,
        path: &Path,
        mode: &str,
    ) -> Result<StagedFile, LineageFoldError> {
        let decoded =
            StagedFile::create(&self.staging_dir, LINEAGE_FOLD_PACK).map_err(LineageFoldError::Scratch)?;
        let output = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(decoded.path())
            .map_err(|source| {
                LineageFoldError::Scratch(ProjectionPackError::CreateTemporary {
                    path: decoded.path().to_path_buf(),
                    source,
                })
            })?;
        #[cfg(test)]
        let output = if LINEAGE_FOLD_DECODED_WRITE_FAILS.with(std::cell::Cell::get) {
            File::open(decoded.path()).map_err(|source| {
                LineageFoldError::Scratch(ProjectionPackError::CreateTemporary {
                    path: decoded.path().to_path_buf(),
                    source,
                })
            })?
        } else {
            output
        };
        let decoder = zstd::stream::write::Decoder::new(FoldDecodedCopy {
            file: output,
            written: 0,
            limit: pack.uncompressed_bytes,
            write_failure: None,
        })
        .map_err(|error| LineageFoldError::Scratch(ProjectionPackError::Decompress(error)))?;
        let source = File::open(path).map_err(|error| fold_damaged(pack, error))?;
        let mut tee = DecodingTee {
            inner: source,
            decoder,
        };
        // A failed write of the decoded copy is the scratch volume's, not the
        // pack's: it is retried, never recorded against the head
        // (REV-CX-ISS106 finding 3).
        let verified = pack
            .verify_reader(&mut tee, ProtocolLimits::default())
            .map(drop)
            .map_err(|error| error.to_string())
            .and_then(|()| tee.decoder.flush().map_err(|error| error.to_string()));
        if let Err(message) = verified {
            return Err(match tee.decoder.get_mut().write_failure.take() {
                Some(source) => LineageFoldError::Scratch(ProjectionPackError::CreateTemporary {
                    path: decoded.path().to_path_buf(),
                    source,
                }),
                None => fold_damaged(pack, message),
            });
        }
        let output = tee.decoder.into_inner();
        if output.written != pack.uncompressed_bytes {
            return Err(fold_damaged(pack, "decoded length differs from its record"));
        }
        drop(output);
        let location = decoded
            .path()
            .to_str()
            .ok_or_else(|| fold_damaged(pack, "staging path is not text"))?
            .to_owned();
        self.connection
            .execute("ATTACH DATABASE ?1 AS pack", params![location])
            .map_err(fold_scratch)?;
        if let Err(error) = self.check_attached_pack(pack, mode) {
            let _ = self.connection.execute_batch("DETACH DATABASE pack");
            return Err(error);
        }
        Ok(decoded)
    }

    fn check_attached_pack(&self, pack: &TransportPack, mode: &str) -> Result<(), LineageFoldError> {
        let damaged = |message: &str| fold_damaged(pack, message);
        let application_id: i64 = self
            .connection
            .query_row("PRAGMA pack.application_id", [], |row| row.get(0))
            .map_err(|error| fold_damaged(pack, error))?;
        let user_version: i64 = self
            .connection
            .query_row("PRAGMA pack.user_version", [], |row| row.get(0))
            .map_err(|error| fold_damaged(pack, error))?;
        if application_id != i64::from(SQLITE_HUB_PROJECTION_APPLICATION_ID)
            || user_version != i64::from(pack.schema.sqlite_user_version())
        {
            return Err(damaged("not a projection pack of its recorded schema"));
        }
        let quick_check: String = self
            .connection
            .query_row("PRAGMA pack.quick_check", [], |row| row.get(0))
            .map_err(|error| fold_damaged(pack, error))?;
        if quick_check != "ok" {
            return Err(damaged("SQLite quick check failed"));
        }
        let metadata = |key: &str| -> Result<Option<String>, LineageFoldError> {
            self.connection
                .query_row(
                    "SELECT value FROM pack.hub_pack_metadata WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| fold_damaged(pack, error))
        };
        let mut expected = vec![
            ("mode", mode.to_owned()),
            ("pack_id", pack.pack_id.to_string()),
            ("snapshot_id", pack.snapshot_id.to_string()),
            ("vehicle_id", self.binding.vehicle_id.to_string()),
            ("selected_car_id", self.binding.selected_car_id.to_string()),
        ];
        if mode == "typed_delta" {
            expected.push(("from_sequence", pack.sequence.from_exclusive.to_string()));
            expected.push(("to_sequence", pack.sequence.to_inclusive.to_string()));
        } else {
            expected.push(("base_sequence", pack.sequence.from_exclusive.to_string()));
            expected.push(("head_sequence", pack.sequence.to_inclusive.to_string()));
        }
        for (key, value) in expected {
            if metadata(key)?.as_deref() != Some(value.as_str()) {
                return Err(fold_damaged(pack, format!("metadata {key} does not match")));
            }
        }
        let mut tables = FOLD_TABLES
            .iter()
            .filter_map(|table| {
                table
                    .pack_columns(pack.schema)
                    .map(|columns| (table.name, columns))
            })
            .collect::<Vec<_>>();
        if mode == "typed_delta" {
            tables.push(("tombstones", vec!["entity", "entity_id", "car_id"]));
        }
        for (name, columns) in tables {
            let present: Vec<String> = self
                .connection
                .prepare(&format!("PRAGMA pack.table_info({name})"))
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| row.get::<_, String>(1))?
                        .collect::<Result<Vec<_>, _>>()
                })
                .map_err(|error| fold_damaged(pack, error))?;
            if present != columns {
                return Err(fold_damaged(pack, format!("table {name} has another layout")));
            }
        }
        Ok(())
    }

    /// Detaches the pack and removes its decoded copy, keeping the first
    /// error of the fold step.
    fn detach_pack(
        &self,
        decoded: StagedFile,
        result: Result<(), LineageFoldError>,
    ) -> Result<(), LineageFoldError> {
        if result.is_err() {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        let detached = self
            .connection
            .execute_batch("DETACH DATABASE pack")
            .map_err(fold_scratch);
        drop(decoded);
        result.and(detached)
    }
}

/// The decoded copy of a pack, bounded by the pack's declared decoded
/// length. A failed write of the copy itself (a full scratch volume) is kept,
/// so it is told apart from a pack that decodes beyond its record.
struct FoldDecodedCopy {
    file: File,
    written: u64,
    limit: u64,
    write_failure: Option<io::Error>,
}

impl Write for FoldDecodedCopy {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(buffer.len()).map_err(io::Error::other)?;
        if self.written.checked_add(length).is_none_or(|total| total > self.limit) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "lineage pack decodes beyond its declared length",
            ));
        }
        match self.file.write(buffer) {
            Ok(written) => {
                self.written += written as u64;
                Ok(written)
            }
            Err(error) => {
                let kind = error.kind();
                self.write_failure.get_or_insert(error);
                Err(io::Error::new(kind, "the decoded copy could not be written"))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush().inspect_err(|error| {
            self.write_failure
                .get_or_insert_with(|| io::Error::new(error.kind(), error.to_string()));
        })
    }
}

/// REAL by bits, INTEGER and TEXT by value, NULL only equal to NULL.
fn fold_value_identical(left: &rusqlite::types::Value, right: &rusqlite::types::Value) -> bool {
    use rusqlite::types::Value;
    match (left, right) {
        (Value::Real(left), Value::Real(right)) => left.to_bits() == right.to_bits(),
        (left, right) => left == right,
    }
}

struct FragmentBuilder {
    car: ProjectionCar,
    drives: Vec<ProjectionDrive>,
    positions: Vec<ProjectionPosition>,
    charges: Vec<ProjectionCharge>,
    snapshot_samples: Vec<ProjectionChargeSample>,
    states: Vec<ProjectionState>,
    updates: Vec<ProjectionUpdate>,
}

impl FragmentBuilder {
    fn new(car: ProjectionCar) -> Self {
        Self {
            car,
            drives: Vec::new(),
            positions: Vec::new(),
            charges: Vec::new(),
            snapshot_samples: Vec::new(),
            states: Vec::new(),
            updates: Vec::new(),
        }
    }

    fn rows(&self) -> u64 {
        1 + [
            self.drives.len(),
            self.positions.len(),
            self.charges.len(),
            self.snapshot_samples.len(),
            self.states.len(),
            self.updates.len(),
        ]
        .into_iter()
        .map(|count| count as u64)
        .sum::<u64>()
    }

    fn has_rows(&self) -> bool {
        self.rows() > 1
    }

    fn push_drive(&mut self, drive: ProjectionDrive) {
        if self.drives.last().is_none_or(|last| last.id != drive.id) {
            self.drives.push(drive);
        }
    }

    fn push_charge(&mut self, charge: ProjectionCharge) {
        if self.charges.last().is_none_or(|last| last.id != charge.id) {
            self.charges.push(charge);
        }
    }

    fn take(&mut self) -> LineageFoldFragment {
        LineageFoldFragment {
            snapshot: ProjectionSnapshot {
                cars: vec![self.car.clone()],
                drives: std::mem::take(&mut self.drives),
                positions: std::mem::take(&mut self.positions),
                charges: std::mem::take(&mut self.charges),
                charge_samples: std::mem::take(&mut self.snapshot_samples),
            },
            states: std::mem::take(&mut self.states),
            updates: std::mem::take(&mut self.updates),
        }
    }
}

fn decode_row<V>(
    row: &rusqlite::Row<'_>,
    decode: fn(&rusqlite::Row<'_>) -> Result<V, String>,
) -> Result<V, LineageFoldError> {
    decode(row).map_err(fold_inexact)
}

fn column<V: rusqlite::types::FromSql>(row: &rusqlite::Row<'_>, name: &str) -> Result<V, String> {
    row.get(name).map_err(|error| format!("column {name}: {error}"))
}

/// Typed decode of the scratch car joined with its settings.
fn car_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionCar, String> {
    Ok(ProjectionCar {
        id: column(row, "id")?,
        name: column(row, "name")?,
        model: column(row, "model")?,
        vin: column(row, "vin")?,
        source_eid: column(row, "source_eid")?,
        source_vid: column(row, "source_vid")?,
        trim_badging: column(row, "trim_badging")?,
        marketing_name: column(row, "marketing_name")?,
        exterior_color: column(row, "exterior_color")?,
        wheel_type: column(row, "wheel_type")?,
        spoiler_type: column(row, "spoiler_type")?,
        source_model_text: None,
        firmware_version: column(row, "firmware_version")?,
        efficiency_wh_per_km: column(row, "efficiency_wh_per_km")?,
        settings: ProjectionCarSettings {
            enabled: column(row, "enabled")?,
            use_streaming_api: column(row, "use_streaming_api")?,
            suspend_after_idle_min: column(row, "suspend_after_idle_min")?,
            suspend_min: column(row, "suspend_min")?,
            suspend_min_resolved: column(row, "suspend_min_resolved")?,
            req_not_unlocked: column(row, "req_not_unlocked")?,
            free_supercharging: column(row, "free_supercharging")?,
            lfp_battery: column(row, "lfp_battery")?,
        },
    })
}

fn drive_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionDrive, String> {
    Ok(ProjectionDrive {
        id: column(row, "id")?,
        car_id: column(row, "car_id")?,
        optimized_at_ms: column(row, "optimized_at_ms")?,
        start_date_ms: column(row, "start_date_ms")?,
        end_date_ms: column(row, "end_date_ms")?,
        distance_km: column(row, "distance_km")?,
        duration_min: column(row, "duration_min")?,
        efficiency: column(row, "efficiency")?,
        outside_temp_avg: column(row, "outside_temp_avg")?,
        inside_temp_avg: column(row, "inside_temp_avg")?,
        speed_max: column(row, "speed_max")?,
        power_max: column(row, "power_max")?,
        power_min: column(row, "power_min")?,
        start_ideal_range_km: column(row, "start_ideal_range_km")?,
        end_ideal_range_km: column(row, "end_ideal_range_km")?,
        start_address: column(row, "start_address")?,
        end_address: column(row, "end_address")?,
        start_geofence: column(row, "start_geofence")?,
        end_geofence: column(row, "end_geofence")?,
        start_latitude: column(row, "start_latitude")?,
        start_longitude: column(row, "start_longitude")?,
        end_latitude: column(row, "end_latitude")?,
        end_longitude: column(row, "end_longitude")?,
        start_soc: column(row, "start_soc")?,
        end_soc: column(row, "end_soc")?,
        start_rated_range_km: column(row, "start_rated_range_km")?,
        end_rated_range_km: column(row, "end_rated_range_km")?,
        ascent: column(row, "ascent")?,
        descent: column(row, "descent")?,
    })
}

fn position_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionPosition, String> {
    Ok(ProjectionPosition {
        id: column(row, "id")?,
        drive_id: column(row, "drive_id")?,
        car_id: column(row, "car_id")?,
        date_ms: column(row, "date_ms")?,
        latitude: column(row, "latitude")?,
        longitude: column(row, "longitude")?,
        speed: column(row, "speed")?,
        power: column(row, "power")?,
        battery_level: column(row, "battery_level")?,
        usable_battery_level: column(row, "usable_battery_level")?,
        elevation: column(row, "elevation")?,
        odometer: column(row, "odometer")?,
        ideal_battery_range_km: column(row, "ideal_battery_range_km")?,
        est_battery_range_km: column(row, "est_battery_range_km")?,
        rated_battery_range_km: column(row, "rated_battery_range_km")?,
        fan_status: column(row, "fan_status")?,
        driver_temp_setting: column(row, "driver_temp_setting")?,
        passenger_temp_setting: column(row, "passenger_temp_setting")?,
        is_climate_on: column(row, "is_climate_on")?,
        is_rear_defroster_on: column(row, "is_rear_defroster_on")?,
        is_front_defroster_on: column(row, "is_front_defroster_on")?,
        inside_temp: column(row, "inside_temp")?,
        outside_temp: column(row, "outside_temp")?,
        battery_heater: column(row, "battery_heater")?,
        battery_heater_on: column(row, "battery_heater_on")?,
        battery_heater_no_power: column(row, "battery_heater_no_power")?,
        tpms_pressure_fl: column(row, "tpms_pressure_fl")?,
        tpms_pressure_fr: column(row, "tpms_pressure_fr")?,
        tpms_pressure_rl: column(row, "tpms_pressure_rl")?,
        tpms_pressure_rr: column(row, "tpms_pressure_rr")?,
    })
}

fn charge_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionCharge, String> {
    let billing_type = column::<Option<String>>(row, "billing_type")?
        .map(|value| GeofenceBillingType::from_str(&value))
        .transpose()
        .map_err(|_| "column billing_type: unknown value".to_owned())?;
    Ok(ProjectionCharge {
        id: column(row, "id")?,
        car_id: column(row, "car_id")?,
        start_date_ms: column(row, "start_date_ms")?,
        end_date_ms: column(row, "end_date_ms")?,
        charge_energy_added: column(row, "charge_energy_added")?,
        charge_energy_used_kwh: column(row, "charge_energy_used_kwh")?,
        start_ideal_range_km: column(row, "start_ideal_range_km")?,
        end_ideal_range_km: column(row, "end_ideal_range_km")?,
        cost: column(row, "cost")?,
        fast_charger_type: column(row, "fast_charger_type")?,
        billing_type,
        cost_per_unit: column(row, "cost_per_unit")?,
        session_fee: column(row, "session_fee")?,
        start_latitude: column(row, "start_latitude")?,
        start_longitude: column(row, "start_longitude")?,
        start_battery_level: column(row, "start_battery_level")?,
        end_battery_level: column(row, "end_battery_level")?,
        duration_min: column(row, "duration_min")?,
        address: column(row, "address")?,
        location_name: column(row, "location_name")?,
        geofence: column(row, "geofence")?,
        is_dc: column(row, "is_dc")?,
        charge_rate_km_per_hour: column(row, "charge_rate_km_per_hour")?,
        max_charger_power_kw: column(row, "max_charger_power_kw")?,
        outside_temp_avg: column(row, "outside_temp_avg")?,
        start_rated_range_km: column(row, "start_rated_range_km")?,
        end_rated_range_km: column(row, "end_rated_range_km")?,
    })
}

fn charge_sample_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionChargeSample, String> {
    Ok(ProjectionChargeSample {
        id: column(row, "id")?,
        charge_process_id: column(row, "charge_process_id")?,
        timestamp_ms: column(row, "timestamp_ms")?,
        battery_level: column(row, "battery_level")?,
        usable_battery_level: column(row, "usable_battery_level")?,
        charge_energy_added_kwh: column(row, "charge_energy_added_kwh")?,
        charger_power_kw: column(row, "charger_power_kw")?,
        charger_voltage: column(row, "charger_voltage")?,
        charger_actual_current: column(row, "charger_actual_current")?,
        charger_pilot_current: column(row, "charger_pilot_current")?,
        charger_phases: column(row, "charger_phases")?,
        ideal_range_km: column(row, "ideal_range_km")?,
        rated_range_km: column(row, "rated_range_km")?,
        outside_temp_c: column(row, "outside_temp_c")?,
        battery_heater_on: column(row, "battery_heater_on")?,
        battery_heater: column(row, "battery_heater")?,
        battery_heater_no_power: column(row, "battery_heater_no_power")?,
        not_enough_power_to_heat: column(row, "not_enough_power_to_heat")?,
        fast_charger_present: column(row, "fast_charger_present")?,
        fast_charger_brand: column(row, "fast_charger_brand")?,
        fast_charger_type: column(row, "fast_charger_type")?,
        charge_cable: column(row, "charge_cable")?,
    })
}

fn state_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionState, String> {
    Ok(ProjectionState {
        id: column(row, "id")?,
        car_id: column(row, "car_id")?,
        state: column(row, "state")?,
        start_date_ms: column(row, "start_date_ms")?,
        end_date_ms: column(row, "end_date_ms")?,
    })
}

fn update_from_row(row: &rusqlite::Row<'_>) -> Result<ProjectionUpdate, String> {
    Ok(ProjectionUpdate {
        id: column(row, "id")?,
        car_id: column(row, "car_id")?,
        start_date_ms: column(row, "start_date_ms")?,
        end_date_ms: column(row, "end_date_ms")?,
        version: column(row, "version")?,
    })
}
