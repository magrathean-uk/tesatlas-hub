// SPDX-License-Identifier: AGPL-3.0-only

/// Persisted open-session member (see `OpenSessionState::pending_gained_range_charge`),
/// kept so existing state blobs still decode. A decoded seed is converted once
/// into a parked snapshot (C1 R12) and the unit then writes `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GainedRangeChargeSeed {
    pub start_date_ms: i64,
    pub start_battery_level: Option<i64>,
    pub start_ideal_range_km: Option<f64>,
    pub start_rated_range_km: Option<f64>,
    pub start_latitude: Option<f64>,
    pub start_longitude: Option<f64>,
    pub first_energy_added: f64,
}

/// Kilometres in one international mile, exact (C1 section 5, brief invariant
/// "international mile = 1.609344 km exactly"). Tesla reports distances in
/// miles and speeds in mph; every conversion goes through this constant.
pub(crate) const K_MILE_KM: f64 = 1.609_344;

/// Earliest plausible Tesla or Hub timestamp, 2012-01-01T00:00:00Z in epoch
/// milliseconds (C1 R5). No vehicle the Owner API, the Fleet API or the stream
/// serves was delivered before 2012 (first Model S deliveries June 2012), so
/// an earlier value is a fault such as an unset clock or seconds sent where
/// milliseconds are expected. Shared with the collector's intake check.
pub(crate) const K_EARLIEST_MS: i64 = 1_325_376_000_000;

/// Largest accepted lead of a group or frame time over the observation time
/// (C1 R5), 60 s. Vehicle and Hub clocks are both network-synchronised, so
/// honest offsets are seconds. A future-dated time would make every later fact
/// of its family stale until real time caught up, so the allowance caps that
/// damage at one minute. Shared with the collector's intake check.
pub(crate) const K_SKEW_MS: i64 = 60_000;

/// Longest `car_version` text the unit uses (C1 R30): the public
/// `updates.version` column is varchar(255).
const MAX_VERSION_TEXT_CHARS: usize = 255;

/// Longest free text a stored position or charge sample keeps (C1 R36): the
/// public columns are varchar(255).
const MAX_FIELD_TEXT_CHARS: usize = 255;

/// Reachability the unit accepts as evidence (C1 R7). These are the only three
/// values of Tesla's vehicle state and of the public `states.state` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    Online,
    Asleep,
    Offline,
}

impl Reach {
    /// Parse the exact lowercase Tesla text; anything else, including the
    /// storage substitute `"unknown"`, is no evidence (R7).
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "online" => Some(Self::Online),
            "asleep" => Some(Self::Asleep),
            "offline" => Some(Self::Offline),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Asleep => "asleep",
            Self::Offline => "offline",
        }
    }
}

/// Class of `vehicle_state.software_update.status` (C1 R29).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateStatusClass {
    /// `installing`: the car is unavailable while it installs.
    Installing,
    /// `available`, `scheduled`, `downloading`, `downloading_wifi_wait`,
    /// `wifi_wait`: an update is offered or being fetched, not installing.
    Offered,
    /// The empty string, the public Owner API's idle value.
    Idle,
    /// Absent, null or an unknown value: no information.
    NoInfo,
}

/// Classify a status text (C1 R29): trimmed and ASCII-lowercased. Only
/// `installing` changes behaviour, and an unknown value gives no information,
/// so a wrong list cannot create an update.
fn update_status_class(status: Option<&str>) -> UpdateStatusClass {
    let Some(status) = status else {
        return UpdateStatusClass::NoInfo;
    };
    match status.trim().to_ascii_lowercase().as_str() {
        "installing" => UpdateStatusClass::Installing,
        "available" | "scheduled" | "downloading" | "downloading_wifi_wait" | "wifi_wait" => {
            UpdateStatusClass::Offered
        }
        "" => UpdateStatusClass::Idle,
        _ => UpdateStatusClass::NoInfo,
    }
}

/// Comparable key of a Tesla version text (C1 R30), or `None` when the text is
/// unusable. The text is trimmed and at most 255 characters; the key is the
/// text before the first whitespace split on `.`, with at least two
/// components, each 1 to 9 ASCII digits (nine digits always fit `u32`).
fn version_key(text: &str) -> Option<Vec<u32>> {
    let text = text.trim();
    if text.chars().count() > MAX_VERSION_TEXT_CHARS {
        return None;
    }
    let token = text.split_whitespace().next()?;
    let components = token
        .split('.')
        .map(|part| {
            let valid = (1..=9).contains(&part.len()) && part.bytes().all(|b| b.is_ascii_digit());
            valid.then(|| part.parse::<u32>().ok()).flatten()
        })
        .collect::<Option<Vec<u32>>>()?;
    (components.len() >= 2).then_some(components)
}

/// The user-visible part of a Tesla version text for the current summary (C6
/// R36, R37): the text before the first whitespace, when the whole text is
/// usable under the C1 R30 grammar of [`version_key`] (the build hash is
/// dropped). One definition, so the summary and the lifecycle read the same
/// versions.
pub(crate) fn usable_car_version(text: &str) -> Option<String> {
    version_key(text)?;
    text.split_whitespace().next().map(str::to_owned)
}

/// Component-wise comparison of two version keys; a missing trailing component
/// counts as 0, so `2024.14` equals `2024.14.0` (C1 R30).
fn compare_version_keys(left: &[u32], right: &[u32]) -> std::cmp::Ordering {
    let length = left.len().max(right.len());
    for index in 0..length {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        match a.cmp(&b) {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}

/// Seed `last_car_version` from the newest imported update whose version
/// parses (C1 R14, section 7 item 9): greatest `end_date_ms`, then greatest id.
/// A state that already knows a version is left alone. Storage calls this
/// after `seed_imported_open_session_state`.
pub(crate) fn seed_version_from_updates(
    state: &mut OpenSessionState,
    updates: &[ProjectionUpdate],
) {
    if state.last_car_version.is_some() {
        return;
    }
    let newest = updates
        .iter()
        .filter(|update| version_key(&update.version).is_some())
        .max_by_key(|update| (update.end_date_ms, update.id));
    if let Some(update) = newest {
        state.last_car_version = Some(update.version.trim().to_owned());
        state.last_car_version_at_ms = Some(update.end_date_ms);
    }
}

/// The time an import records for the `updates` domain's watermark (C1 R94):
/// the latest time at which one of the vehicle's imported software updates is
/// known to have become installed. `updates` holds each update's
/// `(start_ms, end_ms)`. An update's time is its `end_ms` when that is known
/// and positive, else its `start_ms` when that is known and positive, else
/// none (R89: a value that is not positive counts as none); an end before its
/// start is taken as recorded. The result is the greatest such time, `None`
/// when no update has one, whatever the input order. R89 step 2 reads this
/// value back as the time of a version seeded from the car row.
///
/// Known limit 15: an imported update without an end time counts from its
/// start (R94). If it never completed, the car kept its earlier version, and
/// the source also missed an update that started before it, then a later
/// missed-update row starts after that update's true start, as in known
/// limit 12.
// The import writer (G2-4 bulk M5) is its only production caller.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn updates_watermark_time_ms(
    updates: impl IntoIterator<Item = (Option<i64>, Option<i64>)>,
) -> Option<i64> {
    updates
        .into_iter()
        .filter_map(|(start_ms, end_ms)| {
            end_ms
                .filter(|at| *at > 0)
                .or(start_ms.filter(|at| *at > 0))
        })
        .max()
}

/// Seed `last_car_version` from the installed-version text an import left on
/// the car row (C1 R14, section 7 item 9), when the imported updates
/// themselves are no longer at hand: the importer keeps the newest imported
/// update's version on the car row but not the update row, so its end date is
/// unknown. `known_since_ms` is then the time the import recorded for its
/// updates class (the `updates` watermark time, C1 R89 step 2 with R94), or
/// failing that the end of its newest imported drive: the lower bound R31
/// item 2 uses as the start of an update the Hub missed; without it the row of
/// a missed update starts at its own end. A state that already knows a
/// version, and a text that does not parse (R30), are left alone.
pub(crate) fn seed_version_from_car_row(
    state: &mut OpenSessionState,
    version: Option<&str>,
    known_since_ms: Option<i64>,
) {
    if state.last_car_version.is_some() {
        return;
    }
    let Some(version) = version.map(str::trim) else {
        return;
    };
    if version_key(version).is_none() {
        return;
    }
    state.last_car_version = Some(version.to_owned());
    state.last_car_version_at_ms = known_since_ms.filter(|at| *at > 0);
}

/// What two odometer readings say about the car having moved (C1 R65, R70). `Moved` and `Unmoved` need both readings; with either unknown nothing
/// is shown either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Movement {
    /// The later reading is at least [`K_DMIN_KM`] above the earlier one.
    Moved,
    /// The later reading rose by less than [`K_DMIN_KM`]. A fall, as mixed
    /// sources round differently, counts as unmoved: an odometer cannot
    /// decrease, so the car did not move.
    Unmoved,
    /// A reading is missing or not finite.
    Unknown,
}

/// Movement between an earlier and a later odometer reading, in kilometres.
/// [`K_DMIN_KM`] is the one tolerance of "moved" in the unit (R47, R53), so a
/// car that has not moved far enough to be a drive has not moved far enough to
/// be a different plug-in either.
fn movement_between(earlier_km: Option<f64>, later_km: Option<f64>) -> Movement {
    match (
        earlier_km.filter(|value| value.is_finite()),
        later_km.filter(|value| value.is_finite()),
    ) {
        (Some(earlier), Some(later)) if later - earlier >= K_DMIN_KM => Movement::Moved,
        (Some(_), Some(_)) => Movement::Unmoved,
        _ => Movement::Unknown,
    }
}

/// Smallest odometer rise, in km, that shows the car moved when only the other
/// class of reading is known (DESIGN-CHARGE-CONTINUITY 2.2 D3): a full source
/// against a stream or sparse one. The parsers keep each source's own
/// precision (`miles_to_km` of the reported value, no rounding by the Hub);
/// the stream's documented row carries a tenth of a mile (0.161 km), so two
/// readings of one parked car from different classes can differ by about that
/// much. 1.0 km is more than twice that step, and still far below any real
/// trip between two windows.
pub(crate) const K_ODO_MIXED_KM: f64 = 1.0;

impl ChargeLedger {
    /// A live ledger for a row that opens a new plug-in and takes its fee.
    fn renewed(odometer: Option<(f64, bool)>) -> Self {
        let mut ledger = Self {
            live: true,
            fee_taken: true,
            ..Self::default()
        };
        ledger.record_odometer(odometer);
        ledger
    }

    /// Keep an odometer reading in its class: `(km, coarse)`.
    fn record_odometer(&mut self, reading: Option<(f64, bool)>) {
        let Some((km, coarse)) = reading.filter(|(km, _)| km.is_finite()) else {
            return;
        };
        if coarse {
            self.odo_coarse_km = Some(km);
        } else {
            self.odo_fine_km = Some(km);
        }
    }

    /// Design 2.2 D3: the reading rose by at least [`K_DMIN_KM`] against the
    /// ledger's reading of the same class, or, with only the other class known,
    /// by at least [`K_ODO_MIXED_KM`]. A fall is unmoved: an odometer cannot
    /// decrease, and mixed sources round differently.
    fn moved_since(&self, km: f64, coarse: bool) -> bool {
        if !km.is_finite() {
            return false;
        }
        let (same, other) = if coarse {
            (self.odo_coarse_km, self.odo_fine_km)
        } else {
            (self.odo_fine_km, self.odo_coarse_km)
        };
        match (same, other) {
            (Some(earlier), _) => km - earlier >= K_DMIN_KM,
            (None, Some(earlier)) => km - earlier >= K_ODO_MIXED_KM,
            (None, None) => false,
        }
    }

    /// The restart reference: the last counter seen, else the accounted one.
    fn reference_kwh(&self) -> Option<f64> {
        self.last_kwh.or(self.accounted_kwh)
    }
}

/// A record's energy baseline and fee slot, decided at its open (design 2.3).
#[derive(Debug, Clone, Copy, PartialEq)]
struct LedgerOpen {
    /// `first_energy_added` of the new record.
    baseline_kwh: f64,
    /// The fold's first-value restart reference (the ledger's `last_kwh`).
    preset_last_kwh: Option<f64>,
    /// The record continues a plug-in whose fee an earlier row took.
    continues_plug_in: bool,
}

impl LedgerOpen {
    fn preset(self, open: &mut OpenCharge) {
        open.first_energy_added = Some(self.baseline_kwh);
        open.preset_last_kwh = self.preset_last_kwh;
        open.continues_plug_in = Some(self.continues_plug_in);
    }
}

/// Seed an `Unseeded` ledger once (design 2.3, last row): from a parked
/// snapshot a charge close left (not after a drive, not unplugged, counter
/// known) the plug-in is live with that counter accounted and its fee taken;
/// anything else is `Empty`. A fresh state has no snapshot, so it starts
/// `Empty`.
///
/// The old blob does not say which step left its snapshot, and a parked
/// reading alone (a `NoPower` 0.00 of a newly tracked car) also leaves one
/// with a known counter. Only a state whose charge cursor shows that a Hub
/// charge row was allocated can hold a charge-close snapshot, so any other
/// state starts `Empty` and its first row takes the fee (C28, REV-CX-CC P2-5).
fn seed_charge_ledger(state: &mut OpenSessionState) {
    if state.charge_ledger.is_some() {
        return;
    }
    let charge_row_allocated =
        state.next_charge_id > 1 && state.next_charge_id != crate::db::hub_ids::HUB_ID_BASE;
    let seeded = state
        .parked_snapshot
        .as_ref()
        .filter(|_| charge_row_allocated)
        .filter(|snapshot| !snapshot.after_drive && !snapshot.unplugged)
        .and_then(|snapshot| {
            let counter = snapshot
                .energy_added_kwh
                .filter(|value| value.is_finite() && *value >= 0.0)?;
            let mut ledger = ChargeLedger {
                live: true,
                accounted_kwh: Some(counter),
                last_kwh: Some(counter),
                fee_taken: true,
                ..ChargeLedger::default()
            };
            ledger.record_odometer(
                snapshot
                    .odometer_km
                    .map(|km| (km, snapshot.odometer_coarse)),
            );
            Some(ledger)
        });
    state.charge_ledger = Some(seeded.unwrap_or_default());
}

/// The ledger's breaks and readings for an observation while no record is open
/// (design 2.2), before the charge open step. An odometer that moved (D3) or a
/// counter that restarted against the last one (D4) ends the plug-in;
/// otherwise the readings are kept. A dip, a missing counter or odometer, and
/// any gap are no break. `Disconnected` (D2) is applied after the charge steps
/// by [`charge_ledger_unplug`], so an after-gap record that this very reading
/// proves (R70) still continues the plug-in its counter belongs to.
fn charge_ledger_step(state: &mut OpenSessionState, observation: &Observation<'_>) {
    if state.open_charge.is_some() {
        return;
    }
    let Some(ledger) = state.charge_ledger.as_mut().filter(|ledger| ledger.live) else {
        return;
    };
    let parsed = observation.parsed;
    let odometer = parsed
        .odometer
        .filter(|km| observation.fresh.odometer && km.is_finite())
        .map(|km| (km, parsed.kind != SourceKind::Full));
    let counter = usable_energy_counter_of(parsed).filter(|_| observation.fresh.charge_session);
    let moved = odometer.is_some_and(|(km, coarse)| ledger.moved_since(km, coarse));
    let restarted = counter.is_some_and(|now| {
        ledger
            .reference_kwh()
            .is_some_and(|last| counter_restarted(last, now))
    });
    if moved || restarted {
        *ledger = ChargeLedger::default();
        return;
    }
    ledger.record_odometer(odometer);
    if let Some(now) = counter {
        ledger.last_kwh = Some(now);
        ledger.pending_fall_kwh = None;
    }
}

/// Design 2.2 D2: a `Disconnected` reading with no record left open ends the
/// plug-in. A record it closed already emptied the ledger at its close. A gap
/// charge retained from a `Disconnected` reading holds the unplug back: its
/// recovery still continues the plug-in its counter belongs to and empties the
/// ledger at its own close, and its discard applies the unplug
/// ([`discard_pending_gap_charge`], REV-CX-CC P2-2).
fn charge_ledger_unplug(state: &mut OpenSessionState, observation: &Observation<'_>) {
    let unplug_held = state
        .pending_gap_charge
        .as_ref()
        .is_some_and(|pending| pending.unplugged);
    if observation.charge == ChargeEvidence::Disconnected
        && state.open_charge.is_none()
        && !unplug_held
    {
        state.charge_ledger = Some(ChargeLedger::default());
    }
}

/// Design 2.2 D3 while a record is open (REV-CX-CC P2-3): each fresh odometer
/// goes into the live ledger in its class, so a close whose last reading is of
/// the other class still leaves the latest reading of each.
fn charge_ledger_odometer_while_open(state: &mut OpenSessionState, observation: &Observation<'_>) {
    if state.open_charge.is_none() || !observation.fresh.odometer {
        return;
    }
    let parsed = observation.parsed;
    let reading = parsed
        .odometer
        .map(|km| (km, parsed.kind != SourceKind::Full));
    if let Some(ledger) = state.charge_ledger.as_mut().filter(|ledger| ledger.live) {
        ledger.record_odometer(reading);
    }
}

/// Design 2.2 D3 for a record that opened as a continuation before it saw an
/// odometer (REV-CX-CC P2-4): its first fresh reading, or its first after a
/// reading of the other class, is judged against the ledger before the sample
/// is folded. A move ends the plug-in the record tentatively continued: the
/// ledger renews with this reading, and the record counts from 0 and takes the
/// fee. A counter already folded keeps its floor (the larger of the old
/// baseline and that counter).
fn charge_ledger_late_odometer(state: &mut OpenSessionState, observation: &Observation<'_>) {
    let parsed = observation.parsed;
    let Some((km, coarse)) = parsed
        .odometer
        .filter(|km| observation.fresh.odometer && km.is_finite())
        .map(|km| (km, parsed.kind != SourceKind::Full))
    else {
        return;
    };
    let Some(open) = state
        .open_charge
        .as_mut()
        .filter(|open| open.continues_plug_in == Some(true))
    else {
        return;
    };
    let first_of_class =
        open.last_odometer_km.is_none() || open.last_odometer_coarse != Some(coarse);
    if !first_of_class {
        return;
    }
    let Some(ledger) = state.charge_ledger.as_mut().filter(|ledger| ledger.live) else {
        return;
    };
    if !ledger.moved_since(km, coarse) {
        return;
    }
    *ledger = ChargeLedger::renewed(Some((km, coarse)));
    open.first_energy_added = Some(0.0);
    open.preset_last_kwh = None;
    open.continues_plug_in = Some(false);
}

/// Baseline and fee slot of a record that opens now (design 2.3), from the
/// session ledger. `counter` is the record's first usable counter when known,
/// `odometer` its reading as `(km, coarse)`.
///
/// An `Empty` ledger, or a counter that restarted against the ledger's last
/// one, opens a new plug-in: the whole counter is this record's (baseline 0)
/// and it takes the fee. Otherwise the record continues the live plug-in: its
/// baseline is the counter already inside closed rows (0 when none was
/// known), the fold starts at the larger of that and its first counter (a dip
/// adds nothing), and when its first counter is not known yet the fold tests
/// the first one against the ledger's last counter. The fee is waived when an
/// earlier row took it.
fn open_from_ledger(
    state: &mut OpenSessionState,
    counter: Option<f64>,
    odometer: Option<(f64, bool)>,
) -> LedgerOpen {
    let ledger = state.charge_ledger.get_or_insert_with(ChargeLedger::default);
    let restarted = counter
        .zip(ledger.reference_kwh())
        .is_some_and(|(now, last)| counter_restarted(last, now));
    if !ledger.live || restarted {
        *ledger = ChargeLedger::renewed(odometer);
        return LedgerOpen {
            baseline_kwh: 0.0,
            preset_last_kwh: None,
            continues_plug_in: false,
        };
    }
    ledger.record_odometer(odometer);
    let continues_plug_in = ledger.fee_taken;
    ledger.fee_taken = true;
    LedgerOpen {
        baseline_kwh: ledger.accounted_kwh.unwrap_or(0.0),
        preset_last_kwh: ledger.reference_kwh(),
        continues_plug_in,
    }
}

/// Write a closing record back to the ledger (design 2.4): the segment peak is
/// now accounted, the last counter is the restart reference, and a pending
/// provisional reset is kept. A record the ledger did not open (an imported or
/// an older blob's charge) makes the plug-in live with its own counter. A
/// `Disconnected` close ends the plug-in (D2).
fn write_ledger_at_close(state: &mut OpenSessionState, open: &OpenCharge, unplugged: bool) {
    if unplugged {
        state.charge_ledger = Some(ChargeLedger::default());
        return;
    }
    let ledger = state.charge_ledger.get_or_insert_with(ChargeLedger::default);
    if !ledger.live {
        *ledger = ChargeLedger::renewed(None);
    }
    ledger.fee_taken = true;
    if let Some(peak) = open.max_energy_added {
        ledger.accounted_kwh = Some(peak);
    }
    if let Some(last) = open.last_energy_added {
        ledger.last_kwh = Some(last);
    }
    ledger.pending_fall_kwh = open.pending_counter_reset_kwh;
    ledger.record_odometer(
        open.last_odometer_km
            .map(|km| (km, open.last_odometer_coarse.unwrap_or(true))),
    );
}

/// What an after-gap charge needs from its conditions (C1 R70).
struct AfterGapCharge {
    /// The time the snapshot accounts energy up to: the start of the charge.
    start_ms: i64,
    /// The car is unmoved against the snapshot (R65): its start battery, start
    /// ranges and, without coordinates of its own, its start coordinates come
    /// from the snapshot.
    unmoved: bool,
}

/// Decide whether a sample-anchoring observation records a charge nobody
/// recorded (C1 R70, battery or range higher after a gap).
///
/// Decision: record a charge, and only when the car's own counter proves
/// energy flowed that no session recorded. With `S` the parked snapshot, whose
/// `at_ms` and counter are known, all must hold:
///
/// 1. the class is Hold, `Complete`, or `Disconnected` with the car unmoved
///    against `S`; Active is left to R61;
/// 2. the counter is known now, above 0, and either rose beyond the tolerance
///    of C2 R76 or restarted against `S`'s (`counter_restarted` in either
///    direction, C1 R91): neither a dip nor its recovery is proof that energy
///    flowed;
/// 3. the battery level is higher than `S`'s, or, when either level is
///    unknown, the rated range is higher.
///
/// Reasons: energy entered the battery and Tesla's per-session counter
/// measured it; omitting it would silently lose home charging done without
/// signal, while a battery or range rise alone (recalibration, temperature)
/// proves nothing and gives no row. `S.at_ms` is the latest moment up to which
/// the car's energy was accounted for, so every unit of the new energy flowed
/// after it, and `t_charge` is the first observation after the charge: start
/// and end are true bounds of the delivery and the duration is an upper bound.
/// No gap threshold is used, because conditions 1 to 3 prove that energy
/// flowed after `S.at_ms` whatever the interval, and a threshold would only
/// drop a real short charge between two polls. A charge that has ended or is
/// held happened where the car now stands, because a plugged car cannot move;
/// `Disconnected` additionally needs the car unmoved. A gap marker is not an
/// output of the unit; Hub downtime gaps are recorded elsewhere.
fn after_gap_charge(
    snapshot: &ParkedSnapshot,
    parsed: &ParsedSample,
    class: ChargeEvidence,
) -> Option<AfterGapCharge> {
    let start_ms = snapshot.at_ms?;
    let unmoved = movement_between(snapshot.odometer_km, parsed.odometer) == Movement::Unmoved;
    let class_fits = match class {
        ChargeEvidence::Hold | ChargeEvidence::Complete => true,
        ChargeEvidence::Disconnected => unmoved,
        ChargeEvidence::Active | ChargeEvidence::None => false,
    };
    let counter_new = after_gap_counter_changed(snapshot, parsed);
    let level_higher = match (parsed.battery_level, snapshot.battery_level) {
        (Some(now), Some(before)) => now > before,
        _ => matches!(
            (parsed.rated_range_km, snapshot.rated_range_km),
            (Some(now), Some(before)) if now > before
        ),
    };
    (class_fits && counter_new && level_higher).then_some(AfterGapCharge { start_ms, unmoved })
}

fn after_gap_counter_changed(snapshot: &ParkedSnapshot, parsed: &ParsedSample) -> bool {
    let Some(known) = snapshot.energy_added_kwh.filter(|value| value.is_finite()) else {
        return false;
    };
    parsed.charge_energy_added.is_some_and(|now| {
        now > 0.0 && (counter_restarted(now, known) || counter_restarted(known, now))
    })
}

/// The parked snapshot a charge leaves when it closes (C1 R69). `at_ms` and
/// `seen_at_ms` are the charge's end; the values are the session's last ones
/// and its start coordinates; `energy_added_kwh` is the session's last
/// counter. The session is over, so `after_drive` is false, and `unplugged`
/// records that a `Disconnected` sample ended it. Its counter serves R70's
/// test only; the energy baseline comes from the charge ledger.
///
/// The charge records the source of its last odometer reading
/// (`last_odometer_coarse`), and the snapshot copies it, so a fine reading
/// stays fine: R47 lets a fine odometer decide the keep rule of a following
/// drive alone, and a charge that closed between two polls must not make a
/// shuffle with a GNSS jump look like a trip. Only a reading whose source is
/// unknown (a blob from before the field, or an imported charge) is treated as
/// coarse, which makes the keep rule fall back to displacement as well as the
/// odometer, so a real trip is never lost to a resolution the unit cannot
/// vouch for; the next fresh parked odometer replaces the flag with the
/// reading's own.
fn snapshot_after_charge(open: &OpenCharge, end_ms: i64, unplugged: bool) -> ParkedSnapshot {
    ParkedSnapshot {
        at_ms: Some(end_ms),
        seen_at_ms: end_ms,
        battery_level: open.last_battery_level,
        ideal_range_km: open.last_ideal_range_km,
        rated_range_km: open.last_rated_range_km,
        odometer_km: open.last_odometer_km,
        odometer_coarse: open.last_odometer_km.is_some()
            && open.last_odometer_coarse.unwrap_or(true),
        latitude: open.start_latitude,
        longitude: open.start_longitude,
        energy_added_kwh: open.last_energy_added,
        after_drive: false,
        unplugged,
    }
}

/// Phase named by a stored or reported vehicle state text (C1 R23).
fn phase_from_vehicle_state(state: &str) -> VehiclePhase {
    match Reach::from_text(state) {
        Some(Reach::Online) => VehiclePhase::Online,
        Some(Reach::Asleep) => VehiclePhase::Asleep,
        Some(Reach::Offline) => VehiclePhase::Offline,
        None => VehiclePhase::Unknown,
    }
}

/// The stored `states.state` value of a phase (C1 R23). The public vocabulary
/// is online, asleep and offline, so a drive, a charge, an install and the
/// never-produced `Suspended` are all `online`; `Unknown` has no interval.
fn logical_state(phase: VehiclePhase) -> Option<&'static str> {
    match phase {
        VehiclePhase::Online
        | VehiclePhase::Driving
        | VehiclePhase::Charging
        | VehiclePhase::Updating
        | VehiclePhase::Suspended => Some("online"),
        VehiclePhase::Asleep => Some("asleep"),
        VehiclePhase::Offline => Some("offline"),
        VehiclePhase::Unknown => None,
    }
}

/// Change the open state interval when the logical state changes (C1 R24).
///
/// An unchanged state emits nothing, because silence carries no state and an
/// imported open interval with the same text simply continues (R14). A change
/// closes the open interval at `at_ms`, never earlier than its start, and
/// opens the next one. `Unknown` opens no interval.
fn update_state_interval(
    state: &mut OpenSessionState,
    car_id: i64,
    phase: VehiclePhase,
    at_ms: i64,
    delta: &mut LifecycleDelta,
) -> Result<(), LifecycleError> {
    let Some(target) = logical_state(phase) else {
        return Ok(());
    };
    if state
        .open_state
        .as_ref()
        .is_some_and(|open| open.state == target)
    {
        return Ok(());
    }

    if let Some(open) = state.open_state.take() {
        delta.states.push(ProjectionState {
            id: open.id,
            car_id,
            state: open.state,
            start_date_ms: open.start_date_ms,
            end_date_ms: Some(at_ms.max(open.start_date_ms)),
        });
    }

    let id = allocate_hub_id(&mut state.next_state_id)?;
    state.open_state = Some(OpenState {
        id,
        car_id,
        state: target.to_owned(),
        start_date_ms: at_ms,
    });
    delta.states.push(ProjectionState {
        id,
        car_id,
        state: target.to_owned(),
        start_date_ms: at_ms,
        end_date_ms: None,
    });
    Ok(())
}

fn object_field<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Map<String, Value>> {
    map.get(key).and_then(Value::as_object)
}

/// A numeric field (C1 R4a): a finite JSON number, or a JSON string whose
/// trimmed text parses as a finite decimal number. Anything else is the wrong
/// type and gives `None`. Fleet Telemetry's speed and state of charge are real
/// numbers and adapters may forward text, so neither may vanish.
fn float_field(map: &Map<String, Value>, key: &str) -> Option<f64> {
    let number = match map.get(key)? {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    number.is_finite().then_some(number)
}

/// An integer field (C1 R4a): the numeric forms of [`float_field`] rounded
/// half away from zero. Values outside the `i64` range are the wrong type.
fn int_field(map: &Map<String, Value>, key: &str) -> Option<i64> {
    let number = float_field(map, key)?.round();
    // `i64::MAX as f64` is 2^63, the first f64 above `i64::MAX`, so the cast
    // below is exact for every value that passes.
    (number.abs() < i64::MAX as f64).then_some(number as i64)
}

/// A boolean field (C1 R4a): JSON `true` or `false` only.
fn bool_field(map: &Map<String, Value>, key: &str) -> Option<bool> {
    map.get(key).and_then(Value::as_bool)
}

/// Miles to kilometres, unrounded (C1 R34).
pub(crate) fn miles_to_km(miles: f64) -> f64 {
    miles * K_MILE_KM
}

/// Miles to kilometres, rounded to `precision` decimal places (C1 R34).
pub(crate) fn miles_to_km_rounded(miles: f64, precision: u32) -> f64 {
    round_to_precision(miles_to_km(miles), precision)
}

/// Round half away from zero to `precision` decimal places (C1 R34). The
/// standard nearest rounding of `f64::round`, deterministic across replays.
pub(crate) fn round_to_precision(value: f64, precision: u32) -> f64 {
    let scale = 10f64.powi(precision as i32);
    (value * scale).round() / scale
}

/// Miles per hour to whole kilometres per hour (C1 R34).
pub(crate) fn mph_to_kmh(mph: f64) -> i64 {
    (mph * K_MILE_KM).round() as i64
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("lifecycle car id must be positive")]
    InvalidCarId,
    #[error("offline drive timeout must be positive")]
    InvalidOfflineDriveTimeout,
    #[error("lifecycle observation payload is not a valid owner_api_vehicle_data_v1 object")]
    InvalidPayload,
    #[error("lifecycle coordinates are outside the WGS84 domain")]
    InvalidCoordinates,
    #[error("lifecycle end time precedes start time")]
    InvalidTimeline,
    #[error("lifecycle local identifier space is exhausted")]
    IdentifierExhausted,
    #[error("open session exceeds {maximum} bytes ({actual})")]
    SessionTooLarge { actual: usize, maximum: usize },
    #[error("cannot encode open session state")]
    SessionEncode,
    #[error("cannot decode open session state")]
    SessionDecode,
    #[error("unsupported open session version {0}")]
    UnsupportedSessionVersion(u32),
    #[error("open session state is corrupt")]
    CorruptSession,
    #[error("imported open session is invalid")]
    InvalidImportedSession,
}

#[cfg(test)]
mod thermal_flag_tests {
    use super::*;
    use serde_json::json;

    fn sample(id: i64, at_ms: i64, vehicle_data: Value) -> LifecycleSample {
        LifecycleSample {
            observation_id: id,
            observed_at_ms: at_ms,
            vehicle_state: "online".to_owned(),
            payload: json!({
                "record_type": "owner_api_vehicle_data_v1",
                "source_vehicle_id": "thermal-test",
                "vehicle_data": vehicle_data,
            }),
        }
    }

    fn charging_data(at_ms: i64, charge_state: Value, climate_state: Value) -> Value {
        let mut charge = json!({
            "charging_state": "Charging",
            "timestamp": at_ms,
            "battery_level": 50,
            "charge_energy_added": 1.0
        });
        if let (Some(base), Some(extra)) = (charge.as_object_mut(), charge_state.as_object()) {
            for (key, value) in extra {
                base.insert(key.clone(), value.clone());
            }
        }
        json!({
            "charge_state": charge,
            "climate_state": climate_state,
            "drive_state": {"shift_state": "P", "speed": 0}
        })
    }

    #[test]
    fn conflicting_charge_thermal_flags_stay_distinct() {
        let first = apply_sample(
            OpenSessionState::new(),
            1,
            &sample(
                1,
                1_900_000_000_000,
                charging_data(1_900_000_000_000, json!({}), json!({})),
            ),
        )
        .expect("open charging session");
        let second = apply_sample(
            first.state,
            1,
            &sample(
                2,
                1_900_000_001_000,
                charging_data(
                    1_900_000_001_000,
                    json!({
                        "battery_heater_on": true,
                        "not_enough_power_to_heat": false
                    }),
                    json!({
                        "battery_heater": false,
                        "battery_heater_no_power": true
                    }),
                ),
            ),
        )
        .expect("append charging sample");
        let third = apply_sample(
            second.state,
            1,
            &sample(
                3,
                1_900_000_002_000,
                charging_data(
                    1_900_000_002_000,
                    json!({"charging_state": "Complete"}),
                    json!({}),
                ),
            ),
        )
        .expect("close charging session");

        assert_eq!(third.delta.charges.len(), 1);
        let charge = &second.delta.open_charge_samples[0];
        assert_eq!(charge.battery_heater_on, Some(true));
        assert_eq!(charge.battery_heater, Some(false));
        assert_eq!(charge.battery_heater_no_power, Some(true));
        assert_eq!(charge.not_enough_power_to_heat, Some(false));
    }

    #[test]
    fn missing_charge_thermal_flags_remain_none() {
        let first = apply_sample(
            OpenSessionState::new(),
            1,
            &sample(
                1,
                1_900_000_000_000,
                charging_data(1_900_000_000_000, json!({}), json!({})),
            ),
        )
        .expect("open charging session");
        let second = apply_sample(
            first.state,
            1,
            &sample(
                2,
                1_900_000_001_000,
                charging_data(1_900_000_001_000, json!({}), json!({})),
            ),
        )
        .expect("append charging sample");
        let third = apply_sample(
            second.state,
            1,
            &sample(
                3,
                1_900_000_002_000,
                charging_data(
                    1_900_000_002_000,
                    json!({"charging_state": "Complete"}),
                    json!({}),
                ),
            ),
        )
        .expect("close charging session");

        assert_eq!(third.delta.charges.len(), 1);
        let charge = &second.delta.open_charge_samples[0];
        assert_eq!(charge.battery_heater_on, None);
        assert_eq!(charge.battery_heater, None);
        assert_eq!(charge.battery_heater_no_power, None);
        assert_eq!(charge.not_enough_power_to_heat, None);
    }
}
