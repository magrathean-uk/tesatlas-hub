// SPDX-License-Identifier: AGPL-3.0-only

//! Guarded publication of one complete sealed PhysicalV3 source stage.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    db::{
        HubStore, PendingPhysicalV3Admission, PhysicalV3PublicationState, PublicationGate,
        StoreError,
    },
    hub_pack::{ProjectionBinding, ProjectionPackError, ProjectionPackWriter},
    physical_fragments::{
        SourceDbPhysicalFragmentError, SourceDbPhysicalFragmentLimits,
        write_staged_physical_updates_snapshot_v3_with_limits,
    },
    protocol::{CursorKey, ProtocolLimits, SequenceRange, Sha256Digest},
    source_db_stage::{SourceDbStage, SourceDbStageError, SourceDbStageFormat},
};

const MAX_IJSON_INTEGER: u64 = 9_007_199_254_740_991;

/// One phase's time within a [`HandbackRun`]: repeated work of the run (a
/// repair recovery walks the kept vehicles again) adds to the same entry.
struct PhaseTotal {
    vehicle_id: Option<Uuid>,
    phase: &'static str,
    elapsed: std::time::Duration,
    passes: u32,
}

thread_local! {
    static HANDBACK_RUN: std::cell::RefCell<Option<Vec<PhaseTotal>>> =
        const { std::cell::RefCell::new(None) };
}

/// PG-1: logs one line, "handback phase timing", with the `phase` name, its
/// `ms` duration, the `passes` it covers and, for a per-vehicle phase, its
/// `vehicle_id`. Only the clock is read; a phase that does not run logs
/// nothing.
fn emit_handback_phase(
    vehicle_id: Option<Uuid>,
    phase: &'static str,
    elapsed: std::time::Duration,
    passes: u32,
) {
    let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    match vehicle_id {
        Some(vehicle_id) => {
            tracing::info!(%vehicle_id, %phase, ms, passes, "handback phase timing");
        }
        None => tracing::info!(%phase, ms, passes, "handback phase timing"),
    }
}

fn record_handback_phase(
    vehicle_id: Option<Uuid>,
    phase: &'static str,
    started: std::time::Instant,
) {
    let elapsed = started.elapsed();
    let absorbed = HANDBACK_RUN.with(|run| {
        let mut run = run.borrow_mut();
        let Some(totals) = run.as_mut() else {
            return false;
        };
        match totals.iter_mut().find(|total| total.vehicle_id == vehicle_id && total.phase == phase) {
            Some(total) => {
                total.elapsed += elapsed;
                total.passes += 1;
            }
            None => totals.push(PhaseTotal { vehicle_id, phase, elapsed, passes: 1 }),
        }
        true
    });
    if !absorbed {
        emit_handback_phase(vehicle_id, phase, elapsed, 1);
    }
}

/// One hand-back run (PG-1): the phases that finish while it lives add up per
/// vehicle and phase and are logged once, in the order they first finished,
/// when it drops. A run inside a run (the repair recovery's walk) joins the
/// outer one; phases outside any run log as they finish.
pub(crate) struct HandbackRun {
    owner: bool,
}

impl HandbackRun {
    pub(crate) fn start() -> Self {
        let owner = HANDBACK_RUN.with(|run| {
            let mut run = run.borrow_mut();
            if run.is_some() {
                return false;
            }
            *run = Some(Vec::new());
            true
        });
        Self { owner }
    }
}

impl Drop for HandbackRun {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        let totals = HANDBACK_RUN.with(|run| run.borrow_mut().take()).unwrap_or_default();
        for total in totals {
            emit_handback_phase(total.vehicle_id, total.phase, total.elapsed, total.passes);
        }
    }
}

/// Runs `run` under a `tracing` subscriber and returns its result with the
/// PG-1 lines it logged: each is `(vehicle_id, phase, ms)` of a phase line
/// (`vehicle_id` empty for a run-level phase), or `("", "gate_held", ms)`
/// for the total gate-held line (tests).
#[cfg(test)]
pub(crate) fn capture_handback_timing<T>(
    run: impl FnOnce() -> T,
) -> (T, Vec<(String, String, u64)>) {
    let (result, text) = capture_handback_log(run);
    let field = |line: &str, name: &str| -> Option<String> {
        let start = line.find(&format!("{name}="))? + name.len() + 1;
        Some(line[start..].split_whitespace().next()?.to_owned())
    };
    let mut lines = Vec::new();
    for line in text.lines() {
        if line.contains("handback phase timing") {
            lines.push((
                field(line, "vehicle_id").unwrap_or_default(),
                field(line, "phase").expect("a phase"),
                field(line, "ms").expect("ms").parse().expect("ms is a number"),
            ));
        } else if line.contains("handback gate held") {
            lines.push((
                String::new(),
                "gate_held".to_owned(),
                field(line, "gate_held_ms").expect("ms").parse().expect("ms is a number"),
            ));
        }
    }
    (result, lines)
}

/// Runs `run` under a `tracing` subscriber and returns its result with the
/// text it logged at info level and above (tests).
#[cfg(test)]
pub(crate) fn capture_handback_log<T>(run: impl FnOnce() -> T) -> (T, String) {
    #[derive(Clone, Default)]
    struct Sink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("sink").extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sink = Sink::default();
    let writer = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::callsite::rebuild_interest_cache();
    let result = tracing::subscriber::with_default(subscriber, run);
    let text = String::from_utf8(sink.0.lock().expect("sink").clone()).expect("UTF-8 log");
    (result, text)
}

/// Times a phase from its creation to its drop (PG-1), so every way out of
/// the phase, a `?` and an early return included, logs it once.
pub(crate) struct HandbackPhase {
    vehicle_id: Option<Uuid>,
    phase: &'static str,
    started: std::time::Instant,
}

impl HandbackPhase {
    pub(crate) fn start(vehicle_id: Uuid, phase: &'static str) -> Self {
        Self {
            vehicle_id: Some(vehicle_id),
            phase,
            started: std::time::Instant::now(),
        }
    }

    /// A phase of the whole run, with no vehicle.
    pub(crate) fn run_level(phase: &'static str) -> Self {
        Self {
            vehicle_id: None,
            phase,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for HandbackPhase {
    fn drop(&mut self) {
        record_handback_phase(self.vehicle_id, self.phase, self.started);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalV3PublicationKind {
    FirstHead,
    Unchanged,
    Rotation,
    ResumedRotation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalV3Publication {
    pub kind: PhysicalV3PublicationKind,
    pub admission: PendingPhysicalV3Admission,
}

/// Consume one complete private stage and make its exact schema-2.2 snapshot
/// public. The stage is discarded after every outcome. A retry of the same
/// stage derives the same snapshot ID and either returns the public head or
/// finishes a rotation that committed before its activation became visible.
pub async fn publish_sealed_physical_v3_stage(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: SourceDbStage,
) -> Result<PhysicalV3Publication, SourceDbPhysicalPublicationError> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SourceDbPhysicalPublicationError::Clock)?
        .as_millis();
    let now_ms = i64::try_from(now_ms).map_err(|_| SourceDbPhysicalPublicationError::Clock)?;
    publish_sealed_physical_v3_stage_at(store, cursor_key, binding, stage, now_ms).await
}

async fn publish_sealed_physical_v3_stage_at(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: SourceDbStage,
    now_ms: i64,
) -> Result<PhysicalV3Publication, SourceDbPhysicalPublicationError> {
    let owned = crate::source_db_stage::OwnedSourceDbStage::new(stage);
    let gate = store.acquire_publication_gate().await?;
    publish_sealed_physical_v3_stage_with_gate_at(
        store,
        cursor_key,
        binding,
        owned.take(),
        &gate,
        now_ms,
    )
}

/// The normal importer already holds the publication gate from source capture
/// through both catalogues. Never release it or acquire it recursively here.
pub(crate) fn publish_sealed_physical_v3_stage_with_gate_at(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: SourceDbStage,
    gate: &PublicationGate,
    now_ms: i64,
) -> Result<PhysicalV3Publication, SourceDbPhysicalPublicationError> {
    publish_recorded_physical_v3_stage_with_gate_at(
        store,
        cursor_key,
        binding,
        stage,
        gate,
        now_ms,
        &|_| Ok(()),
    )
}

/// As [`publish_sealed_physical_v3_stage_with_gate_at`], telling `record`
/// the head's snapshot id before anything is published (M03-T21: the
/// importer records a head it planned with the import seam).
pub(crate) fn publish_recorded_physical_v3_stage_with_gate_at(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: SourceDbStage,
    gate: &PublicationGate,
    now_ms: i64,
    record: &dyn Fn(Uuid) -> Result<(), StoreError>,
) -> Result<PhysicalV3Publication, SourceDbPhysicalPublicationError> {
    publish_recorded_physical_v3_stage_reporting_month(
        store, cursor_key, binding, stage, gate, now_ms, record,
    )
    .map(|(publication, _)| publication)
}

#[cfg(test)]
thread_local! {
    /// Fails the next month preparation on this thread (M08-T19 tests).
    pub(crate) static FAIL_NEXT_PREPARATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// As [`publish_recorded_physical_v3_stage_with_gate_at`], also saying
/// whether the optional prepared month was made (M08-T19).
pub(crate) fn publish_recorded_physical_v3_stage_reporting_month(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: SourceDbStage,
    gate: &PublicationGate,
    now_ms: i64,
    record: &dyn Fn(Uuid) -> Result<(), StoreError>,
) -> Result<(PhysicalV3Publication, bool), SourceDbPhysicalPublicationError> {
    let mut prepared = false;
    let vehicle_id = binding.vehicle_id;
    let result = publish_sealed_physical_v3_stage_inner(
        store,
        cursor_key,
        binding,
        &stage,
        gate,
        now_ms,
        SourceDbPhysicalFragmentLimits::default(),
        record,
    );
    if let Ok(publication) = &result {
        let _phase = HandbackPhase::start(vehicle_id, "prepared_month");
        match crate::sync::prepared_map::publish_selected_month(
            store,
            cursor_key,
            &stage,
            &publication.admission,
            now_ms,
        ) {
            Ok(()) => prepared = true,
            Err(error) => tracing::warn!(%error, vehicle_id = %publication.admission.vehicle_id,
                "optional prepared month is unavailable; local map generation remains authoritative"),
        }
    }
    use crate::runtime::development_event_log as dev;
    let mut event = dev::Event::new(
        dev::Kind::Publication,
        if result.is_ok() {
            dev::Outcome::Complete
        } else {
            dev::Outcome::Failed
        },
    );
    if let Ok(publication) = &result {
        event.target_sequence = Some(publication.admission.head_sequence);
        event.chunks = publication.admission.manifest.chunk_count as u64;
        event.rows = publication.admission.manifest.total_rows;
    }
    dev::record(event);
    let cleanup = stage.discard();
    match (result, cleanup) {
        (Ok(publication), Ok(())) => Ok((publication, prepared)),
        (Err(error), Ok(())) => Err(error),
        (_, Err(error)) => Err(SourceDbPhysicalPublicationError::StageCleanup(error)),
    }
}

#[derive(Debug)]
enum IntendedPublication {
    First,
    Rotate(Box<PendingPhysicalV3Admission>),
}

#[allow(clippy::too_many_arguments)]
fn publish_sealed_physical_v3_stage_inner(
    store: &HubStore,
    cursor_key: &CursorKey,
    binding: ProjectionBinding,
    stage: &SourceDbStage,
    publication_gate: &PublicationGate,
    now_ms: i64,
    fragment_limits: SourceDbPhysicalFragmentLimits,
    record: &dyn Fn(Uuid) -> Result<(), StoreError>,
) -> Result<PhysicalV3Publication, SourceDbPhysicalPublicationError> {
    if stage.format()? != SourceDbStageFormat::PhysicalV3 {
        return Err(SourceDbPhysicalPublicationError::WrongStageFormat);
    }
    let digest_phase = HandbackPhase::start(binding.vehicle_id, "digest");
    let stage_digest = stage.sealed_content_digest()?;
    let snapshot_id = physical_v3_snapshot_id(stage_digest, &binding);
    record(snapshot_id)?;
    drop(digest_phase);
    let state_phase = HandbackPhase::start(binding.vehicle_id, "state");
    let state = store.physical_v3_publication_state_for_vehicle_at(binding.vehicle_id, now_ms)?;
    // Recover the already committed, fully verified successor before deciding
    // what to do with a fresh source snapshot. The source may have advanced
    // while the Hub was stopped; that must not strand the persisted head.
    let state = if let PhysicalV3PublicationState::Blocked { current, retained } = state {
        if binding.installation_id != current.installation_id
            || binding.account_id != current.account_id
            || binding.generation != current.manifest.generation
            || binding.selected_car_id != current.selected_car_id
        {
            return Err(crate::db::StoreError::PhysicalV3AdmissionInvalid.into());
        }
        let recovered = store.activate_pending_physical_v3_rotation_at(
            publication_gate,
            current.vehicle_id,
            &retained.admission.receipt_id,
            now_ms,
        )?;
        if recovered.snapshot_id == snapshot_id {
            return Ok(PhysicalV3Publication {
                kind: PhysicalV3PublicationKind::ResumedRotation,
                admission: recovered,
            });
        }
        PhysicalV3PublicationState::Public(recovered)
    } else {
        state
    };
    drop(state_phase);
    let (head_sequence, intended) = match state {
        PhysicalV3PublicationState::Empty => (1, IntendedPublication::First),
        PhysicalV3PublicationState::Public(current) if current.snapshot_id == snapshot_id => {
            if binding.installation_id != current.installation_id
                || binding.account_id != current.account_id
                || binding.generation != current.manifest.generation
                || binding.selected_car_id != current.selected_car_id
            {
                return Err(crate::db::StoreError::PhysicalV3AdmissionInvalid.into());
            }
            // The sealed full-table digest and binding select the same head,
            // and the state lookup has verified every persisted pack. Reuse
            // that exact receipt without another full-capacity reservation,
            // SQLite reconstruction or compression pass.
            return Ok(PhysicalV3Publication {
                kind: PhysicalV3PublicationKind::Unchanged,
                admission: current,
            });
        }
        PhysicalV3PublicationState::Public(current) => {
            let next = current
                .head_sequence
                .checked_add(1)
                .filter(|sequence| *sequence <= MAX_IJSON_INTEGER)
                .ok_or(SourceDbPhysicalPublicationError::SequenceExhausted)?;
            (next, IntendedPublication::Rotate(Box::new(current)))
        }
        PhysicalV3PublicationState::Blocked { .. } => {
            return Err(SourceDbPhysicalPublicationError::DifferentBlockedRotation);
        }
    };

    let packs_phase = HandbackPhase::start(binding.vehicle_id, "packs");
    let stage_limits = stage.stats()?.limits;
    let writer = ProjectionPackWriter::with_limits(
        store.packs_dir(),
        ProtocolLimits::hub_sync_v1_1_3_schema_2_2(),
    )
    .with_minimum_free_bytes(stage_limits.minimum_free_bytes);
    writer.ensure_full_snapshot_capacity_for_capture(
        stage_limits.max_stage_bytes,
        stage_limits.minimum_free_bytes,
    )?;
    #[cfg(test)]
    crate::hub_pack::record_verification("stage-emit", stage.path().display(), true);
    let candidate = write_staged_physical_updates_snapshot_v3_with_limits(
        stage,
        &writer,
        binding,
        snapshot_id,
        SequenceRange {
            from_exclusive: head_sequence,
            to_inclusive: head_sequence,
        },
        cursor_key,
        fragment_limits,
    )?;
    drop(packs_phase);

    match intended {
        IntendedPublication::First => {
            let _phase = HandbackPhase::start(candidate.binding.vehicle_id, "admission");
            let admission =
                store.stage_pending_physical_v3_admission(publication_gate, candidate)?;
            Ok(PhysicalV3Publication {
                kind: PhysicalV3PublicationKind::FirstHead,
                admission,
            })
        }
        IntendedPublication::Rotate(prior) => {
            let delta_phase = HandbackPhase::start(candidate.binding.vehicle_id, "delta");
            let delta = match crate::db::physical_v3_admission_from_manifest(
                &candidate.manifest,
                candidate.binding.selected_car_id,
            ) {
                Ok(target) => {
                    match crate::import::source_db::physical_delta_pack::prepare_changed_set_of_verified_heads(
                        &prior,
                        &target,
                        &candidate.binding,
                        cursor_key,
                        stage_digest,
                        store.packs_dir(),
                        stage_limits.minimum_free_bytes,
                    ) {
                        Ok(delta) => Some(delta),
                        Err(error) => {
                            tracing::warn!(%error, vehicle_id = %candidate.binding.vehicle_id,
                            "bounded physical changed set unavailable; signed full replacement remains available");
                            None
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, vehicle_id = %candidate.binding.vehicle_id,
                        "physical changed set candidate identity unavailable");
                    None
                }
            };
            drop(delta_phase);
            let mut event = dev_publication_choice(delta.is_some(), prior.head_sequence);
            event.target_sequence = Some(head_sequence);
            crate::runtime::development_event_log::record(event);
            let _phase = HandbackPhase::start(candidate.binding.vehicle_id, "admission");
            let admission = store.rotate_and_activate_physical_v3_admission_with_delta_at(
                publication_gate,
                candidate,
                delta,
                now_ms,
            )?;
            Ok(PhysicalV3Publication {
                kind: PhysicalV3PublicationKind::Rotation,
                admission,
            })
        }
    }
}

pub(crate) fn physical_v3_snapshot_id(stage_digest: Sha256Digest, binding: &ProjectionBinding) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"teslatlas-hub/hub-sync-v1/1.3.0/physical-v3-snapshot/v1\0"); // rename:keep
    digest.update(stage_digest.as_bytes());
    digest.update(binding.installation_id.as_bytes());
    digest.update(binding.account_id.as_bytes());
    digest.update(binding.vehicle_id.as_bytes());
    digest.update(binding.generation.to_be_bytes());
    digest.update(binding.selected_car_id.to_be_bytes());
    Uuid::new_v5(&Uuid::NAMESPACE_URL, &digest.finalize())
}

#[derive(Debug, Error)]
pub enum SourceDbPhysicalPublicationError {
    #[error("physical V3 publication requires a sealed physical-v3 stage")]
    WrongStageFormat,
    #[error("physical V3 publication sequence is exhausted")]
    SequenceExhausted,
    #[error("a different physical V3 rotation is already blocked awaiting activation")]
    DifferentBlockedRotation,
    #[error("the deterministic physical V3 candidate does not match its stored head")]
    DeterministicCandidateMismatch,
    #[error("the system clock cannot produce a valid publication timestamp")]
    Clock,
    #[error("cannot discard consumed physical V3 stage: {0}")]
    StageCleanup(SourceDbStageError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Stage(#[from] SourceDbStageError),
    #[error(transparent)]
    Fragment(#[from] SourceDbPhysicalFragmentError),
    #[error(transparent)]
    Pack(#[from] ProjectionPackError),
}

#[cfg(test)]
#[path = "physical_publication/tests.rs"]
mod tests;

fn dev_publication_choice(delta: bool, base: u64) -> crate::runtime::development_event_log::Event {
    use crate::runtime::development_event_log as dev;
    let mut event = dev::Event::new(
        dev::Kind::Publication,
        if delta {
            dev::Outcome::ChangedSet
        } else {
            dev::Outcome::FullReplacement
        },
    );
    event.base_sequence = Some(base);
    event
}

#[cfg(test)]
mod n1_src_sync_physical_publication_rs_display_tests {
    use super::*;
    use crate::runtime::name_test_support::{ErrorExample, assert_name_free, example};

    impl ErrorExample for SourceDbPhysicalPublicationError {
        fn error_example() -> Self {
            SourceDbPhysicalPublicationError::WrongStageFormat
        }
    }

    #[test]
    fn every_reachable_error_display_omits_the_source_project_name() {
        fn check_source_db_physical_publication_error(error: SourceDbPhysicalPublicationError) {
            match &error {
                SourceDbPhysicalPublicationError::WrongStageFormat => (),
                SourceDbPhysicalPublicationError::SequenceExhausted => (),
                SourceDbPhysicalPublicationError::DifferentBlockedRotation => (),
                SourceDbPhysicalPublicationError::DeterministicCandidateMismatch => (),
                SourceDbPhysicalPublicationError::Clock => (),
                SourceDbPhysicalPublicationError::StageCleanup(..) => (),
                SourceDbPhysicalPublicationError::Store(..) => (),
                SourceDbPhysicalPublicationError::Stage(..) => (),
                SourceDbPhysicalPublicationError::Fragment(..) => (),
                SourceDbPhysicalPublicationError::Pack(..) => (),
            }
            assert_name_free(&error);
        }
        check_source_db_physical_publication_error(
            SourceDbPhysicalPublicationError::WrongStageFormat,
        );
        check_source_db_physical_publication_error(
            SourceDbPhysicalPublicationError::SequenceExhausted,
        );
        check_source_db_physical_publication_error(
            SourceDbPhysicalPublicationError::DifferentBlockedRotation,
        );
        check_source_db_physical_publication_error(
            SourceDbPhysicalPublicationError::DeterministicCandidateMismatch,
        );
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::Clock);
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::StageCleanup(
            example(),
        ));
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::Store(
            example(),
        ));
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::Stage(
            example(),
        ));
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::Fragment(
            example(),
        ));
        check_source_db_physical_publication_error(SourceDbPhysicalPublicationError::Pack(
            example(),
        ));
    }
}
