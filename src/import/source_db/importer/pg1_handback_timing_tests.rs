// SPDX-License-Identifier: AGPL-3.0-only

//! PG-1: a handback logs one line per phase it ran, with its duration in
//! milliseconds, and one line with the milliseconds it held the publication
//! gate. Synthetic data only.

use super::*;
use crate::physical_publication::capture_handback_timing;

fn phases(lines: &[(String, String, u64)]) -> Vec<&str> {
    lines.iter().map(|(_, phase, _)| phase.as_str()).collect()
}

fn assert_once_each(lines: &[(String, String, u64)], expected: &[&str]) {
    let mut seen = phases(lines);
    seen.sort_unstable();
    let mut wanted = expected.to_vec();
    wanted.sort_unstable();
    assert_eq!(seen, wanted, "{lines:?}");
}

#[test]
fn pg1_full_handback_logs_each_phase_once() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let _env = paired(&fixture);
    collect(&fixture);
    let legacy = fixture
        .store
        .physical_v3_kept(fixture.binding.vehicle_id)
        .expect("kept")
        .expect("a kept stage")
        .archive
        .is_none();

    let (outcome, lines) = capture_handback_timing(|| handback(&fixture));
    assert_eq!(outcome, published(PhysicalV3PublicationKind::Rotation));
    let mut expected = vec![
        "checks",
        "seam",
        "early_answer",
        "open",
        "plan",
        "layer",
        "digest",
        "state",
        "packs",
        "delta",
        "admission",
        "inputs",
        "prepared_month",
    ];
    if legacy {
        expected.push("conversion");
    }
    assert_once_each(&lines, &expected);
    let vehicle = fixture.binding.vehicle_id.to_string();
    assert!(lines.iter().all(|(id, _, _)| *id == vehicle), "{lines:?}");

    // A second handback has nothing to publish: only the phases it ran log.
    let (outcome, lines) = capture_handback_timing(|| handback(&fixture));
    assert_eq!(outcome, HandbackPhysicalOutcome::Current);
    assert_once_each(&lines, &["checks"]);
}

#[test]
fn pg1_early_answer_logs_only_the_phases_it_ran() {
    let (fixture, _root, _env) = planned_then_refused(false);
    let (outcome, lines) = capture_handback_timing(|| handback(&fixture));
    assert_eq!(outcome, published(PhysicalV3PublicationKind::Unchanged));
    let seen = phases(&lines);
    for phase in ["checks", "seam", "early_answer"] {
        assert_eq!(seen.iter().filter(|seen| **seen == phase).count(), 1, "{lines:?}");
    }
    for phase in ["open", "plan", "layer", "digest", "state", "packs", "delta", "admission", "inputs"] {
        assert!(!seen.contains(&phase), "{phase} did not run: {lines:?}");
    }
}

/// The service's entry point holds the gate for the whole run and logs its
/// total once, after the vehicle's phases.
#[test]
fn pg1_window_handback_logs_the_gate_held_total_once() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let Fixture { _temporary, store, binding, car: _, gate, cursor_key: _ } = fixture;
    drop(gate);
    let data_dir = _temporary.path().to_path_buf();
    // The key the run loads; the vehicle's head is current, so nothing it
    // signs is read.
    crate::legacy_owner_credentials::load_or_create_cursor_key(&data_dir).expect("cursor key");
    let (_, lines) = capture_handback_timing(|| {
        crate::runtime::operator::authority::publish_window_after_handback(&data_dir)
    });
    drop(store);
    let totals: Vec<_> = lines.iter().filter(|(_, phase, _)| phase == "gate_held").collect();
    assert_eq!(totals.len(), 1, "{lines:?}");
    assert_eq!(lines.last().map(|(_, phase, _)| phase.as_str()), Some("gate_held"));
    assert!(lines.iter().any(|(id, phase, _)| *id == binding.vehicle_id.to_string() && phase == "checks"));
}

/// Review finding 1: the total is logged after the gate is released, so the
/// event's own time is not in it and does not keep another publisher out.
#[test]
fn pg1_finding1_gate_held_total_is_logged_after_the_gate_is_released() {
    #[derive(Clone)]
    struct Probe {
        data_dir: std::path::PathBuf,
        gate_free_at_total: std::sync::Arc<std::sync::Mutex<Vec<bool>>>,
    }
    impl std::io::Write for Probe {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if String::from_utf8_lossy(bytes).contains("handback gate held") {
                let free = crate::db::HubStore::open_existing(&self.data_dir)
                    .expect("another publisher's store")
                    .try_acquire_publication_gate()
                    .is_ok();
                self.gate_free_at_total.lock().expect("probe").push(free);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let Fixture { _temporary, store, binding: _, car: _, gate, cursor_key: _ } = fixture;
    drop(gate);
    let data_dir = _temporary.path().to_path_buf();
    crate::legacy_owner_credentials::load_or_create_cursor_key(&data_dir).expect("cursor key");
    let probe = Probe { data_dir: data_dir.clone(), gate_free_at_total: Default::default() };
    let writer = probe.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::callsite::rebuild_interest_cache();
    tracing::subscriber::with_default(subscriber, || {
        crate::runtime::operator::authority::publish_window_after_handback(&data_dir)
    });
    drop(store);
    assert_eq!(*probe.gate_free_at_total.lock().expect("probe"), vec![true]);
}

fn resume_lines(lines: &[(String, String, u64)]) -> usize {
    lines.iter().filter(|(vehicle, phase, _)| vehicle.is_empty() && phase == "resume").count()
}

/// Review finding 2: the sweep runs with no intent left, so it is timed.
#[test]
fn pg1_finding2_resume_is_timed_when_no_intent_is_left() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let (_, lines) = capture_handback_timing(|| {
        publish_physical_after_handback(&fixture.store, &fixture.cursor_key, &fixture.gate)
            .expect("handback publication")
    });
    assert_eq!(resume_lines(&lines), 1, "{lines:?}");
}

/// Review finding 2: a failed sweep still logs the time it took.
#[test]
fn pg1_finding2_resume_is_timed_when_the_sweep_fails() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    RESUME_FAILS.with(|fails| fails.set(true));
    let (result, lines) = capture_handback_timing(|| {
        publish_physical_after_handback(&fixture.store, &fixture.cursor_key, &fixture.gate)
    });
    assert!(result.is_err());
    assert_eq!(resume_lines(&lines), 1, "{lines:?}");
    assert!(!phases(&lines).contains(&"checks"), "the heads were not walked: {lines:?}");
}
