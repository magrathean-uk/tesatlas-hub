// SPDX-License-Identifier: AGPL-3.0-only

// The B2 fix round's tests of the stream-evidence unit (C3 R9, R28, R36, R40): the
// append-time `no_data` decision, the audit journal and what a stopped run keeps.
// They share the beds of `stream_evidence.rs`. Synthetic data only.

// ---- R40 (B2 clarification 3): a `no_data` event is decided inside its transaction ------------------

impl TickBed {
    /// The decision times the committed transactions stamped on their `no_data`
    /// events (observed and received time are the same instant), oldest first.
    fn no_data_rows(&self) -> Vec<i64> {
        SUPERVISED_COLLECTOR_TEST_SEAM
            .try_with(|seam| seam.no_data_stored())
            .expect("inside the test seam")
    }

    /// Plan the `no_data` candidates of one tick now, without committing them.
    fn plan_no_data_batch(&mut self) -> TickBatch {
        let now = self.now();
        self.scheduler.begin_tick();
        let mut batch = TickBatch::default();
        plan_no_data_events(&self.store, &mut self.scheduler, &mut batch).expect("plan");
        if !batch.hub_events.is_empty() {
            batch.first_result_at = Some(now);
        }
        batch
    }

    /// A parked read result of car 1, received at the bed's wall time.
    fn fresh_read_item(&self, timestamp_ms: i64) -> SnapshotItem {
        SnapshotItem {
            vehicle: self.scheduler.vehicles[&core_id(1)].vehicle.clone(),
            snapshot: VehicleData::for_test(
                1,
                json!({
                    "drive_state": {
                        "shift_state": "D",
                        "speed": 20,
                        "latitude": 47.52,
                        "longitude": 19.0,
                        "timestamp": timestamp_ms
                    },
                    "vehicle_state": {"odometer": 1000.2}
                }),
            ),
            received_at_ms: self.wall(),
        }
    }

    fn drive_open(&self, hub: Uuid) -> bool {
        self.store
            .collector_lifecycle_snapshot(hub)
            .expect("snapshot")
            .drive_open
    }
}

#[tokio::test]
async fn r9_the_manual_collection_failure_log_names_no_tesla_id() {
    // The failure of a manual collection is logged with its class, and never with
    // the Tesla identifier of the car or a field that would name one (C3 R9).
    let log = DecisionLog::start();
    let temporary = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(temporary.path()).expect("store");
    let cursor_key = CursorKey::from_bytes([0x31; 32]);
    let collection = ManualCollection {
        vehicles: Vec::new(),
        snapshots: Vec::new(),
        failures: vec![VehicleCollectionFailure {
            vehicle_id: VehicleId::from_test(9_876_543_210),
            error: OwnerApiError::HttpStatus(503),
        }],
    };
    // The collection itself may end as it likes; the log line is the subject.
    let _ = finish_collection(&store, &cursor_key, &collection).await;
    let text = log.contents();
    let warnings: Vec<&str> = text
        .lines()
        .filter(|line| line.contains(" WARN ") && line.contains("owner API vehicle collection failed"))
        .collect();
    assert_eq!(warnings.len(), 1, "{text}");
    assert!(!warnings[0].contains("9876543210"), "{}", warnings[0]);
    assert!(!warnings[0].contains("vehicle_id"), "{}", warnings[0]);
    assert!(warnings[0].contains("error="), "the class is still named: {}", warnings[0]);
}

#[tokio::test]
async fn ct31b_a_no_data_candidate_does_not_close_a_drive_resumed_by_a_stored_stream_frame() {
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam, async {
            let mut bed = TickBed::owner();
            let hub = bed.store_open_drive().await;
            let id = core_id(1);
            let w0 = CORE_W0;
            // The candidate is planned an instant after `T_gap` without evidence.
            bed.advance(Duration::from_millis(600_001));
            let batch = bed.plan_no_data_batch();
            assert_eq!(batch.hub_events.len(), 1, "a candidate is planned");
            // A fresh Driving frame is stored by the stream path before the candidate's
            // transaction runs: the car resumed, and the scheduler's cache knows nothing
            // of it yet.
            assert!(
                persist_stream_update(
                    &bed.store,
                    id,
                    &frame(w0 + 300_000, Some("D"), Some(20), Some(12)),
                )
                .expect("the frame"),
                "the frame is stored"
            );
            let receipt: i64 = bed.query(
                "SELECT MAX(received_at_ms) FROM current_observations
                  WHERE record_type = 'tesla_stream_update_v1'",
            );
            assert_eq!(bed.scheduler.vehicles[&id].last_stored_obs_ms, Some(w0));
            let observation = bed.last_observation_id(hub);
            // The decision is aligned to the frame's actual receipt, just after it.
            bed.advance(Duration::from_millis(
                u64::try_from(receipt + 100 - bed.wall()).expect("forward"),
            ));
            bed.commit(batch).await;
            assert!(bed.no_data_rows().is_empty(), "no event");
            assert_eq!(bed.last_observation_id(hub), observation, "nothing closed the drive");
            assert!(bed.drive_open(hub));
            assert!(!bed.scheduler.vehicles[&id].force_closed_at_gap, "no flag");
            // The evidence is the frame's receipt: exactly `T_gap` after it there is
            // still no event, and one millisecond later there is exactly one.
            bed.advance(Duration::from_millis(600_000 - 100));
            assert_eq!(bed.wall(), receipt + 600_000);
            let batch = bed.plan_no_data_batch();
            bed.commit(batch).await;
            assert!(bed.no_data_rows().is_empty());
            bed.advance(Duration::from_millis(1));
            let batch = bed.plan_no_data_batch();
            assert_eq!(batch.hub_events.len(), 1);
            bed.commit(batch).await;
            assert_eq!(bed.no_data_rows(), vec![receipt + 600_001]);
            assert!(bed.scheduler.vehicles[&id].force_closed_at_gap);
            assert!(!bed.drive_open(hub));
        })
        .await;
}

async fn ct31c_case(mode: crate::config::CollectorMode) {
    let mut bed = TickBed::new(mode);
    let hub = bed.store_open_drive().await;
    let id = core_id(1);
    bed.advance(Duration::from_millis(600_001));
    let mut batch = bed.plan_no_data_batch();
    assert_eq!(batch.hub_events.len(), 1, "a candidate is planned");
    // A read of the same tick is received before the transaction runs.
    let item = bed.fresh_read_item(bed.wall());
    let received = item.received_at_ms;
    batch.snapshots.push(item);
    bed.commit(batch).await;
    assert!(bed.no_data_rows().is_empty(), "the read is evidence: no event");
    assert!(!bed.scheduler.vehicles[&id].force_closed_at_gap, "no flag");
    // The read's own lifecycle effects remain: it is stored, and the car's evidence is
    // its receipt.
    assert_eq!(bed.scheduler.vehicles[&id].last_stored_obs_ms, Some(received));
    assert!(bed.drive_open(hub), "the read opened or continued a drive");
}

#[tokio::test]
async fn ct31c_a_same_transaction_read_invalidates_a_no_data_candidate() {
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam.clone(), async {
            ct31c_case(crate::config::CollectorMode::Legacy).await;
            ct31c_case(crate::config::CollectorMode::FleetPolling).await;
        })
        .await;
}

#[tokio::test]
async fn ct31d_a_same_transaction_delivered_discovery_invalidates_a_no_data_candidate() {
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam, async {
            // A state that gives the lifecycle no reachability is delivered when it
            // differs from the current discovery row: a fresh Deliver discovery.
            let mut bed = TickBed::owner();
            let hub = bed.store_open_drive().await;
            let id = core_id(1);
            bed.advance(Duration::from_millis(600_001));
            let mut batch = bed.plan_no_data_batch();
            assert_eq!(batch.hub_events.len(), 1);
            let mut vehicle = bed.scheduler.vehicles[&id].vehicle.clone();
            vehicle.state = "waking".to_owned();
            // Received exactly `T_gap` after the drive's last fold, one millisecond
            // before the decision: stale enough for nothing but its own receipt to
            // matter, and not late enough to end the drive by the gap rule.
            let received = bed.wall() - 1;
            batch.discoveries.push(DiscoveryItem {
                vehicle,
                received_at_ms: received,
            });
            bed.commit(batch).await;
            assert!(bed.no_data_rows().is_empty(), "the discovery is evidence");
            assert!(!bed.scheduler.vehicles[&id].force_closed_at_gap);
            assert_eq!(bed.scheduler.vehicles[&id].last_stored_obs_ms, Some(received));
            assert!(bed.drive_open(hub), "the drive was neither ended by the gap rule nor closed");

            // A discovery that only writes the car's current row is no evidence.
            let mut bed = TickBed::owner();
            let hub = bed.store_open_drive().await;
            bed.store
                .open()
                .expect("connection")
                .execute(
                    "DELETE FROM current_observations WHERE record_type = 'owner_api_discovery_v1'",
                    [],
                )
                .expect("the car has no current discovery row");
            let w0 = CORE_W0;
            bed.advance(Duration::from_millis(600_001));
            let mut batch = bed.plan_no_data_batch();
            assert_eq!(batch.hub_events.len(), 1);
            let mut vehicle = bed.scheduler.vehicles[&id].vehicle.clone();
            vehicle.state = "online".to_owned();
            // The same state as the lifecycle's last reachability, and no current row
            // yet: the action is `WriteCurrentOnly`.
            assert_eq!(
                crate::db::discovery_action("online", Some("online"), None),
                crate::db::DiscoveryAction::WriteCurrentOnly
            );
            batch.discoveries.push(DiscoveryItem {
                vehicle,
                received_at_ms: bed.wall(),
            });
            bed.commit(batch).await;
            assert_eq!(
                bed.no_data_rows(),
                vec![w0 + 600_001],
                "a current-only discovery proves nothing: the drive is closed"
            );
            assert!(bed.scheduler.vehicles[&id].force_closed_at_gap);
            assert!(!bed.drive_open(hub));
        })
        .await;
}

#[tokio::test]
async fn ct31e_an_accepted_no_data_candidate_uses_the_transaction_decision_time() {
    for mode in [
        crate::config::CollectorMode::Legacy,
        crate::config::CollectorMode::FleetPolling,
    ] {
        let seam = Arc::new(SupervisedCollectorTestSeam::default());
        SUPERVISED_COLLECTOR_TEST_SEAM
            .scope(seam, async {
                let mut bed = TickBed::new(mode);
                let hub = bed.store_open_drive().await;
                let id = core_id(1);
                bed.advance(Duration::from_millis(600_001));
                let planned_at = bed.wall();
                let batch = bed.plan_no_data_batch();
                assert_eq!(batch.hub_events.len(), 1);
                assert_eq!(batch.hub_events[0].observed_at_ms, planned_at);
                // The tick's requests take 37 ms before its transaction runs.
                bed.advance(Duration::from_millis(37));
                bed.commit(batch).await;
                assert_eq!(
                    bed.no_data_rows(),
                    vec![planned_at + 37],
                    "{mode:?}: observed and received at the decision time"
                );
                assert!(bed.scheduler.vehicles[&id].force_closed_at_gap);
                assert!(!bed.drive_open(hub), "one close");
            })
            .await;
    }
}

#[tokio::test]
async fn ct31f_a_no_data_candidate_rechecks_the_transaction_drive_state() {
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam, async {
            let mut bed = TickBed::owner();
            let hub = bed.store_open_drive().await;
            let id = core_id(1);
            let w0 = CORE_W0;
            // Long past `T_gap`: the only reason left to refuse the candidate is the
            // drive's state.
            bed.advance(Duration::from_millis(700_000));
            let mut batch = bed.plan_no_data_batch();
            assert_eq!(batch.hub_events.len(), 1);
            // The old Asleep discovery of the same tick closes the drive in storage
            // while the cache still says it is open, and its receipt is stale too.
            let mut vehicle = bed.scheduler.vehicles[&id].vehicle.clone();
            vehicle.state = "asleep".to_owned();
            batch.discoveries.push(DiscoveryItem {
                vehicle,
                received_at_ms: w0 + 1_000,
            });
            assert!(bed.drive_open(hub), "storage: the drive is open before the commit");
            assert!(
                bed.scheduler.vehicles[&id]
                    .lifecycle
                    .as_ref()
                    .is_some_and(|view| view.drive_open),
                "the cache: open"
            );
            bed.commit(batch).await;
            assert!(!bed.drive_open(hub), "storage: the Asleep intake closed it");
            assert!(bed.no_data_rows().is_empty(), "no event");
            assert!(!bed.scheduler.vehicles[&id].force_closed_at_gap, "no flag");
            let drives = bed.store.materialised_history(hub).expect("history").drives;
            assert_eq!(drives.len(), 1, "one historical closure");
            assert_eq!(drives[0].end_date_ms, w0);
        })
        .await;
}

#[tokio::test]
async fn stream_audit_failure_is_logged_once_without_a_durable_row() {
    // The audit transition is a journal entry of its own, one WARN at the first
    // failure of a car, whether or not any row can be written (C3 R28, R36, B2
    // clarification 3). A repeated failure adds none; a `Healthy` clears the state and
    // lets the next failure log again; the fresh lease stays service-ready, because
    // attention is not a service failure.
    let log = DecisionLog::start();
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam.clone(), async {
            let mut bed = StreamBed::new();
            let id = core_id(1);
            let source = bed
                .store
                .register_source(&provider_source(CollectorProvider::Legacy), CORE_W0)
                .expect("source");
            let registered = bed
                .store
                .register_vehicle(
                    &VehicleDescriptor::new(source.source_id, "1")
                        .with_tesla_identity(Some(1), None),
                    CORE_W0,
                )
                .expect("vehicle");
            bed.core.scheduler.vehicles.get_mut(&id).unwrap().hub_id = Some(registered.vehicle_id);
            let hub_label = registered.vehicle_id.to_string();
            let lease_ms = current_epoch_millis().expect("clock");
            let _lease = bed
                .store
                .acquire_supervised_collector_lease(lease_ms)
                .expect("a fresh lease");

            // One run ends on an audit failure.
            async fn audit_failed_run(bed: &mut StreamBed, id: VehicleId) {
                let (_events, receiver) = mpsc::channel(4);
                let task = tokio::spawn(async {
                    Err::<(), _>(crate::tesla_stream::StreamSupervisorError::Audit(
                        StoreError::NilVehicleId,
                    ))
                });
                let mut streams = vec![VehicleStreamRuntime {
                    vehicle_id: id,
                    power_gate: Arc::new(StreamPowerGate::default()),
                    sensitive_access_failure: Arc::new(AtomicBool::new(false)),
                    events: receiver,
                    _shutdown: None,
                    quiet: None,
                    task: Some(task),
                    held_frame: None,
                }];
                while !streams[0].task.as_ref().is_some_and(JoinHandle::is_finished) {
                    tokio::task::yield_now().await;
                }
                let mut projection = HashMap::new();
                drain_stream_events_with_cache(
                    &bed.store,
                    &mut bed.core.scheduler,
                    &mut streams,
                    &mut projection,
                )
                .await
                .expect("an audit failure is not a collector error");
            }
            let audit_lines =
                |log: &DecisionLog| log.lines("WARN", "stream_audit_failed");

            audit_failed_run(&mut bed, id).await;
            assert_eq!(audit_lines(&log), 1, "{}", log.contents());
            // The store refuses the tick that would write the row: no durable row, the
            // live attention is shown.
            seam.set_tick_commit_fault(true);
            let now = bed.core.clock.mono_now();
            bed.core.scheduler.begin_tick();
            let commit = TickCommit {
                store: &bed.store,
                cursor_key: &CursorKey::from_bytes([0x13; 32]),
                provider: CollectorProvider::Legacy,
                offline_drive_timeout: Duration::from_secs(600),
            };
            let mut identities = IdentityCache::default();
            let outcome = commit_tick(
                &commit,
                &mut identities,
                &mut bed.core.scheduler,
                TickBatch::default(),
                now,
            )
            .await
            .expect("commit");
            seam.set_tick_commit_fault(false);
            assert!(matches!(outcome, TickOutcome::Failed), "{outcome:?}");
            let stored: i64 = bed
                .store
                .open()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM collector_vehicle_state WHERE reason_code = 'stream_audit_failed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, 0, "no durable row");
            assert_eq!(
                bed.core.scheduler.status(now).vehicles[0].detail,
                Some(STATUS_STREAM_AUDIT_FAILED)
            );
            assert_eq!(
                bed.store.collector_readiness_at(lease_ms).unwrap().failure,
                Some(crate::db::CollectionAttentionCode::CollectorStreamAuditFailed),
                "the live attention"
            );
            assert!(
                bed.store.service_readiness_at(true, lease_ms).is_ok(),
                "attention does not fail a fresh lease"
            );
            // A repeated failure of the same car adds no line.
            audit_failed_run(&mut bed, id).await;
            assert_eq!(audit_lines(&log), 1, "{}", log.contents());
            // `Healthy` clears it, and the next failure is a new transition.
            bed.dispatch(StreamEvent::Healthy);
            assert!(!bed.core.scheduler.vehicles[&id].stream.audit_failed);
            audit_failed_run(&mut bed, id).await;
            assert_eq!(audit_lines(&log), 2, "{}", log.contents());
            // The entry is redacted: the car's Hub id, no Tesla id, no error text.
            let entry = log
                .contents()
                .lines()
                .find(|line| line.contains("stream_audit_failed"))
                .expect("an audit entry")
                .to_owned();
            assert!(entry.contains(&hub_label), "{entry}");
            assert!(entry.contains("collector.decision"), "{entry}");
            assert!(!entry.contains("VIN"), "{entry}");
            assert!(!entry.contains("error="), "{entry}");
            assert!(!entry.contains("vehicle_id"), "{entry}");
            assert!(bed.store.service_readiness_at(true, lease_ms).is_ok());
        })
        .await;
}

#[tokio::test]
async fn r36_a_stopped_run_keeps_its_queued_telemetry() {
    // Frames still queued when runs are stopped are each stored once, for the cars
    // whose frames are stored at all: a Disabled or Removed car's are not (R36, R62).
    let mut bed = StreamBed::new();
    let now = bed.core.t0;
    bed.core.scheduler.accept_discovery(
        vec![
            core_vehicle(1, "online"),
            core_vehicle(2, "online"),
            core_vehicle(3, "online"),
        ],
        now,
    );
    bed.core.scheduler.vehicles.get_mut(&core_id(2)).unwrap().hold = VehicleHold::Disabled;
    bed.core.scheduler.vehicles.get_mut(&core_id(3)).unwrap().hold = VehicleHold::Removed;
    let base = StreamBed::now_ms();
    let mut streams = Vec::new();
    let mut senders = Vec::new();
    for id in [1u64, 2, 3] {
        let (events, receiver) = mpsc::channel(16);
        let (shutdown, stop) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = stop.await;
            Ok::<(), crate::tesla_stream::StreamSupervisorError>(())
        });
        streams.push(VehicleStreamRuntime {
            vehicle_id: core_id(id),
            power_gate: Arc::new(StreamPowerGate::default()),
            sensitive_access_failure: Arc::new(AtomicBool::new(false)),
            events: receiver,
            _shutdown: Some(shutdown),
            quiet: None,
            task: Some(task),
            held_frame: None,
        });
        for step in 0..3 {
            events
                .send(bed.telemetry(frame(base + step * 100 + id as i64, Some("P"), Some(0), Some(0))))
                .await
                .expect("queued");
        }
        // A control event among them changes nothing.
        events.send(StreamEvent::Healthy).await.expect("queued");
        senders.push(events);
    }
    let mut cache = HashMap::new();
    let mut pending = StreamDrainResult::default();
    let mut budget = DrainBudget::new(Instant::now());
    let store = bed.store.clone();
    stop_vehicle_streams(
        &store,
        &mut bed.core.scheduler,
        &mut streams,
        &[core_id(1), core_id(2), core_id(3)],
        &mut cache,
        &mut pending,
        &mut budget,
    )
    .await
    .expect("the stop");
    assert!(streams.is_empty());
    // Each frame of the eligible car was folded once: its cursor stands at three. The
    // others were never stored (no stream context was even made for them).
    let hub = cache[&core_id(1)].registered_vehicle_id;
    assert_eq!(
        bed.store
            .load_lifecycle_state(hub)
            .expect("lifecycle")
            .expect("state")
            .last_observation_id,
        3,
        "each frame of the eligible car once"
    );
    assert!(cache.contains_key(&core_id(1)));
    assert!(!cache.contains_key(&core_id(2)) && !cache.contains_key(&core_id(3)));
    assert_eq!(cache[&core_id(1)].last_stream_timestamp_ms, Some(base + 201));
}

// ---- R40: a current-only discovery of an earlier tick is no evidence -----------------------

impl TickBed {
    /// A tick, committed alone, whose only content is the car's discovery with `state`
    /// received now, with no `no_data` candidate: the scheduler's cached view is made
    /// stale first so the transaction decides the discovery from the store's rows.
    async fn commit_a_standalone_discovery(&mut self, state: &str) {
        let id = core_id(1);
        let mut vehicle = self.scheduler.vehicles[&id].vehicle.clone();
        vehicle.state = state.to_owned();
        self.scheduler
            .vehicles
            .get_mut(&id)
            .expect("the car")
            .lifecycle_stale = true;
        let now = self.now();
        self.scheduler.begin_tick();
        let batch = TickBatch {
            discoveries: vec![DiscoveryItem {
                vehicle,
                received_at_ms: self.wall(),
            }],
            first_result_at: Some(now),
            ..TickBatch::default()
        };
        self.commit(batch).await;
    }

    /// The evidence the store holds for the car: the newest receipt of a delivered row.
    fn stored_evidence(&self, hub: Uuid) -> Option<i64> {
        self.store
            .collector_lifecycle_snapshot(hub)
            .expect("snapshot")
            .last_stored_obs_ms
    }
}

async fn ct31g_case(mode: crate::config::CollectorMode) {
    let discovery_type = if mode == crate::config::CollectorMode::FleetPolling {
        "fleet_api_discovery_v1"
    } else {
        "owner_api_discovery_v1"
    };
    // A current-only discovery committed in an EARLIER tick, with no candidate, is no
    // evidence: it must not postpone the closure of the drive its car has left open.
    let mut bed = TickBed::new(mode);
    let hub = bed.store_open_drive().await;
    let id = core_id(1);
    let w0 = CORE_W0;
    assert_eq!(bed.stored_evidence(hub), Some(w0), "the evidence is the car's last data");
    // The car's current discovery row is gone (the missing-row case of R14).
    bed.store
        .open()
        .expect("connection")
        .execute(
            "DELETE FROM current_observations WHERE record_type IN
             ('owner_api_discovery_v1', 'fleet_api_discovery_v1')",
            [],
        )
        .expect("the car has no current discovery row");
    bed.advance(Duration::from_millis(300_000));
    // The discovery states what the lifecycle already knows: the action is the write of
    // the current row alone.
    assert_eq!(
        crate::db::discovery_action("online", Some("online"), None),
        crate::db::DiscoveryAction::WriteCurrentOnly
    );
    bed.commit_a_standalone_discovery("online").await;
    let row_receipt: Option<i64> = bed
        .store
        .open()
        .expect("connection")
        .query_row(
            "SELECT received_at_ms FROM current_observations WHERE record_type = ?1",
            [discovery_type],
            |row| row.get(0),
        )
        .ok();
    assert_eq!(row_receipt, Some(w0 + 300_000), "presence changed: the current row is written");
    assert_eq!(
        bed.stored_evidence(hub),
        Some(w0),
        "the evidence did not: the row was delivered to no lifecycle"
    );
    assert!(bed.no_data_rows().is_empty());
    // `T_gap` after the evidence there is no event, and one millisecond later there is
    // exactly one, stamped with the decision time, closing the drive.
    bed.advance(Duration::from_millis(300_000));
    assert_eq!(bed.wall(), w0 + 600_000);
    assert_eq!(bed.scheduler.vehicles[&id].last_stored_obs_ms, Some(w0));
    // What a reload of the scheduler would read from the store agrees.
    assert_eq!(bed.stored_evidence(hub), Some(w0));
    let batch = bed.plan_no_data_batch();
    assert!(batch.hub_events.is_empty(), "not yet strictly over T_gap");
    bed.commit(batch).await;
    assert!(bed.no_data_rows().is_empty());
    bed.advance(Duration::from_millis(1));
    let batch = bed.plan_no_data_batch();
    assert_eq!(batch.hub_events.len(), 1, "the candidate is planned");
    bed.commit(batch).await;
    assert_eq!(
        bed.no_data_rows(),
        vec![w0 + 600_001],
        "one event, stamped with the decision time"
    );
    assert!(bed.scheduler.vehicles[&id].force_closed_at_gap, "the flag is set");
    assert!(!bed.drive_open(hub), "the drive is closed");
}

#[tokio::test]
async fn ct31g_a_prior_current_only_discovery_does_not_delay_no_data() {
    for mode in [
        crate::config::CollectorMode::Legacy,
        crate::config::CollectorMode::FleetPolling,
    ] {
        let seam = Arc::new(SupervisedCollectorTestSeam::default());
        SUPERVISED_COLLECTOR_TEST_SEAM.scope(seam, ct31g_case(mode)).await;
    }
}

#[tokio::test]
async fn ct31g_a_prior_delivered_discovery_advances_the_evidence() {
    // The control: a discovery that is DELIVERED to the lifecycle (its state differs
    // from the lifecycle's reachability) is evidence, whenever its tick was.
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam, async {
            let mut bed = TickBed::owner();
            let hub = bed.store_open_drive().await;
            let id = core_id(1);
            let w0 = CORE_W0;
            bed.advance(Duration::from_millis(300_000));
            bed.commit_a_standalone_discovery("waking").await;
            assert_eq!(
                bed.stored_evidence(hub),
                Some(w0 + 300_000),
                "a delivered discovery advances the evidence"
            );
            bed.advance(Duration::from_millis(300_001));
            let batch = bed.plan_no_data_batch();
            assert!(batch.hub_events.is_empty(), "the evidence is 300 s newer");
            assert_eq!(
                bed.scheduler.vehicles[&id].last_stored_obs_ms,
                Some(w0 + 300_000)
            );
            bed.commit(batch).await;
            assert!(bed.no_data_rows().is_empty());
            assert!(bed.drive_open(hub));
        })
        .await;
}

#[tokio::test]
async fn stream_pace_a_slow_commit_is_logged_with_its_duration() {
    // STREAM-PACE: the collector loop stalled for tens of seconds and the log could
    // not say whether the commits were the cause. A commit over 500 ms is logged with
    // its duration and whether it flushed the drive's cache, at most once per 5 s per
    // car (the next line carries the count suppressed), and the lag warning and its
    // recovery line carry the longest commit since the last report.
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    let log = DecisionLog::start();
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam.clone(), async {
            let mut bed = TickBed::owner();
            bed.list(vec![core_vehicle(1, "online")]).await;
            let commit_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let clock = bed.clock.clone();
            let hook_ms = Arc::clone(&commit_ms);
            seam.set_stream_commit_hook(Arc::new(move || {
                clock.advance_mono(Duration::from_millis(
                    hook_ms.load(std::sync::atomic::Ordering::SeqCst),
                ));
            }));
            let base_ms = bed.wall() - 60_000;
            let mut cache = HashMap::new();
            let mut commit = |bed: &mut TickBed, offset_ms: i64, took_ms: u64, queued_ms: u64| {
                commit_ms.store(took_ms, std::sync::atomic::Ordering::SeqCst);
                let queued_at = bed.now();
                bed.clock.advance_mono(Duration::from_millis(queued_ms));
                assert!(
                    process_stream_telemetry_with_cache(
                        &bed.store,
                        &mut bed.scheduler,
                        core_id(1),
                        &frame(base_ms + offset_ms, Some("P"), Some(0), Some(0)),
                        queued_at,
                        &mut cache,
                    )
                    .expect("frame"),
                    "the frame is committed"
                );
            };
            // The writer's first frame is its flushed commit; 600 ms is slow.
            commit(&mut bed, 0, 600, 0);
            // 700 ms and 800 ms are slow too, but inside 5 s of the line above.
            commit(&mut bed, 1_000, 700, 0);
            commit(&mut bed, 2_000, 800, 0);
            // 400 ms is under the threshold: not logged.
            commit(&mut bed, 3_000, 400, 0);
            // Five seconds on, the next slow commit is logged with the two it suppressed.
            bed.clock.advance_mono(Duration::from_secs(5));
            commit(&mut bed, 4_000, 900, 0);
            // A frame 6 s in the queue: the lag warning carries the longest commit.
            commit(&mut bed, 5_000, 0, 6_000);
            // Back under 1 s: the recovery line carries the longest since the warning.
            commit(&mut bed, 6_000, 300, 0);
        })
        .await;
    let contents = log.contents();
    let slow: Vec<&str> = contents
        .lines()
        .filter(|line| line.contains("Tesla stream frame commit was slow"))
        .collect();
    assert_eq!(slow.len(), 2, "{contents}");
    assert!(
        slow[0].contains("commit_ms=600")
            && slow[0].contains("flushed=true")
            && slow[0].contains("suppressed=0"),
        "{contents}"
    );
    assert!(
        slow[1].contains("commit_ms=900")
            && slow[1].contains("flushed=false")
            && slow[1].contains("suppressed=2"),
        "{contents}"
    );
    assert!(slow.iter().all(|line| line.contains(" WARN ")), "{contents}");
    assert!(!contents.contains("commit_ms=400"), "{contents}");
    let lag: Vec<&str> = contents
        .lines()
        .filter(|line| line.contains("local_stream_processing_lag"))
        .collect();
    assert_eq!(lag.len(), 1, "{contents}");
    assert!(lag[0].contains("longest_commit_ms=900"), "{contents}");
    let recovered: Vec<&str> = contents
        .lines()
        .filter(|line| line.contains("processing lag recovered"))
        .collect();
    assert_eq!(recovered.len(), 1, "{contents}");
    assert!(recovered[0].contains("longest_commit_ms=300"), "{contents}");
}

#[tokio::test]
async fn intake_latency_ends_before_the_slow_commit_warning_is_written() {
    // STREAM-PACE, review finding 6: the slow-commit line is written after the commit
    // returned, and a blocked log write (here one second per warning) is not intake.
    use tracing_subscriber::layer::SubscriberExt as _;

    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(seam.clone(), async {
            let recorder = intake_recorder();
            let window = intake_window(41);
            let mut bed = TickBed::owner();
            bed.list(vec![core_vehicle(1, "online")]).await;
            bed.scheduler.guard_writes_with(intake_guard(recorder, 41));
            let clock = bed.clock.clone();
            seam.set_stream_commit_hook(Arc::new(move || {
                clock.advance_mono(Duration::from_millis(600));
            }));
            let base_ms = bed.wall() - 60_000;
            let mut cache = HashMap::new();
            let queued_at = bed.now();
            bed.clock.advance_mono(Duration::from_millis(100));
            let update = frame(base_ms, Some("P"), Some(0), Some(0));
            let subscriber =
                tracing_subscriber::registry().with(AdvanceOnWarning(bed.clock.clone()));
            let before = bed.now();
            let stored = tracing::subscriber::with_default(subscriber, || {
                process_stream_telemetry_with_cache(
                    &bed.store,
                    &mut bed.scheduler,
                    core_id(1),
                    &update,
                    queued_at,
                    &mut cache,
                )
            })
            .expect("frame");
            assert!(stored);
            assert!(
                bed.now() >= before + Duration::from_millis(1_600),
                "the slow commit warned"
            );
            let figures = recorder.status(&window);
            assert_eq!(figures["count"], 1, "{figures}");
            assert_eq!(latency_ms(&figures, "maxMs"), 700.0, "100 ms queued, 600 ms committing");
        })
        .await;
}

// ---- STREAM-STOP-ALLOWANCE: the stop uses the time the supervisor really grants ------------------

/// What one stop of [`stream_stop_allowance_stop`] left.
struct AllowanceStop {
    _directory: tempfile::TempDir,
    store: HubStore,
    log: Vec<LoopEvent>,
    budget: DrainBudget,
    took: Duration,
    base_ms: i64,
    poll_ms: i64,
}

/// A stop of one car with `frames` queued and no frame of it stored in this run, only
/// a polled observation at `poll_ms`, each commit taking `commit` on the collector's
/// clock, under `grant`. When `record` is set the stop record is written as the run
/// writes it, from the floor of the queued frames; otherwise it did not commit.
/// `shorten`, when given, limits the slot's grant after that many commits, as a
/// service's stop after the window's close does (REV-CX-SSA finding 1).
async fn stream_stop_allowance_stop(
    frames: usize,
    commit: Duration,
    grant: Option<Duration>,
    record: bool,
    shorten: Option<(usize, Duration)>,
) -> AllowanceStop {
    stream_stop_allowance_stop_from(frames, commit, grant, record, shorten, 0).await
}

/// [`stream_stop_allowance_stop`] with the queued frames observed from `base_ms` plus
/// `frames_offset_ms`; a negative offset beyond the poll's five seconds puts the first
/// frames before the polled observation (REV-CX-RECHECK-SSA finding 3).
async fn stream_stop_allowance_stop_from(
    frames: usize,
    commit: Duration,
    grant: Option<Duration>,
    record: bool,
    shorten: Option<(usize, Duration)>,
    frames_offset_ms: i64,
) -> AllowanceStop {
    let seam = Arc::new(SupervisedCollectorTestSeam::default());
    let directory = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(directory.path()).expect("store");
    let mut core = Core::owner();
    let t0 = core.t0;
    core.scheduler.accept_discovery(vec![core_vehicle(1, "online")], t0);
    let base_ms = current_epoch_millis().expect("clock") - 1_000_000;
    let poll_ms = base_ms - 5_000;
    core.scheduler.note_observation_stored(core_id(1), poll_ms);
    let frame_ms = |index: usize| base_ms + frames_offset_ms + i64::try_from(index).expect("index") * 333;
    let mut streams = vec![pace_queued_run(1, frames, frame_ms)];
    let mut cache = HashMap::new();
    let mut budget = DrainBudget::new(Instant::now());
    if record {
        let floor = queued_stream_frames_floor_ms(&mut streams, &cache, &core.scheduler);
        if floor.is_some() {
            budget.settle_stop_gap();
        }
        let stop_ms = current_epoch_millis().expect("clock");
        store
            .end_collector_run_from(None, stop_ms, floor.unwrap_or(stop_ms))
            .expect("stop record");
        budget.note_stop_record(StopRecord::Recorded);
    } else {
        budget.note_stop_record(StopRecord::Unrecorded);
    }
    let stop_began = core.clock.mono_now();
    let stop_time = StreamStopTime::granted(stop_began, grant);
    let slot = StopGrant::default();
    stop_time.grant.follow(slot.clone());
    let began_at = stop_time.began_at;
    let clock = Arc::clone(&core.clock);
    let commits = Arc::new(AtomicUsize::new(0));
    seam.set_stream_commit_hook(Arc::new(move || {
        clock.advance(commit);
        let done = commits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if let Some((after, granted)) = shorten
            && done == after
        {
            slot.limit(began_at + granted);
        }
    }));
    budget.limit_stop_writes(stop_time);
    let mut pending = StreamDrainResult::default();
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(
            Arc::clone(&seam),
            stop_all_streams_and_store(
                &store,
                &mut core.scheduler,
                &mut streams,
                &mut cache,
                &mut pending,
                &mut budget,
            ),
        )
        .await
        .expect("the orderly stop");
    assert!(streams.is_empty(), "the stopped run is gone");
    let took = core.clock.mono_now() - stop_began;
    AllowanceStop {
        _directory: directory,
        store,
        log: seam.loop_log(),
        budget,
        took,
        base_ms,
        poll_ms,
    }
}

#[test]
fn stream_stop_allowance_the_default_grant_keeps_todays_limits() {
    // No grant, or the supervisor's full 30 s: frames for 20 s, the gap write started
    // by 22 s, as STREAM-PACE set them. A grant above the default changes nothing.
    let began = Instant::now();
    let today = StreamStopLimits {
        frames_until: began + STREAM_STOP_WRITE_ALLOWANCE,
        gap_write_by: Some(began + STREAM_STOP_WRITE_ALLOWANCE + STREAM_STOP_GAP_WRITE_GRACE),
    };
    assert_eq!(stream_stop_limits(began, None), today);
    let full = crate::runtime::operator::authority::COLLECTOR_STOP_WAIT;
    assert_eq!(stream_stop_limits(began, Some(full)), today);
    assert_eq!(stream_stop_limits(began, Some(full * 2)), today);
    // A shorter grant moves both limits with it.
    assert_eq!(
        stream_stop_limits(began, Some(Duration::from_secs(15))),
        StreamStopLimits {
            frames_until: began + Duration::from_secs(5),
            gap_write_by: Some(began + Duration::from_secs(7)),
        }
    );
    // Under the gap write's reserve the grant cannot hold the write at all.
    assert_eq!(
        stream_stop_limits(began, Some(Duration::from_secs(7))),
        StreamStopLimits { frames_until: began, gap_write_by: None }
    );
    // The signal's payload: `()` and `None` leave the default; a deadline is kept.
    assert_eq!(().into_grant().deadline(), None);
    assert_eq!(None::<std::time::Instant>.into_grant().deadline(), None);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    assert_eq!(Some(deadline).into_grant().deadline(), Some(deadline));
}

#[tokio::test]
async fn stream_stop_allowance_a_shorter_grant_bounds_the_frame_writes_and_the_gap_write() {
    // The supervisor grants 15 s, not 30 s: frames are written for 5 s, and the gap of
    // the rest is written from the first unwritten frame, inside the grant.
    const COMMIT: Duration = Duration::from_millis(75);
    let frames = STREAM_EVENT_CHANNEL_CAPACITY;
    let stop =
        stream_stop_allowance_stop(frames, COMMIT, Some(Duration::from_secs(15)), true, None).await;
    let frame_ms = |index: usize| stop.base_ms + i64::try_from(index).expect("index") * 333;
    assert!(stop.took <= Duration::from_secs(5) + COMMIT, "the stop took {:?}", stop.took);
    let written = pace_committed(&stop.log, 1).len();
    assert!(written > 0 && written < frames, "{written} written");
    assert!(written <= 5_000 / 75 + 1, "frames were written past the granted cut-off");
    assert_eq!(pace_recorded_gaps(&stop.log), vec![(frame_ms(written), frames - written)]);
    assert_eq!(pace_stop_gaps(&stop.store), vec![(frame_ms(written), None)]);
    assert_eq!(stop.budget.stop_record(StopRecord::Recorded), StopRecord::Recorded);
}

#[tokio::test]
async fn stream_stop_allowance_a_gap_write_the_grant_no_longer_holds_is_not_started() {
    // A 12 s grant: frames until 2 s, the gap write started by 4 s. The commit running at
    // the cut-off ends at 4.5 s, so the gap write is not started (with the default it
    // would be, by 22 s): the earlier stop record stands and the stop is unrecorded.
    let log_lines = DecisionLog::start();
    let stop = stream_stop_allowance_stop(
        10,
        Duration::from_millis(4_500),
        Some(Duration::from_secs(12)),
        true,
        None,
    )
    .await;
    assert_eq!(stop.took, Duration::from_millis(4_500));
    assert_eq!(pace_committed(&stop.log, 1).len(), 1);
    assert!(pace_recorded_gaps(&stop.log).is_empty(), "no gap was written");
    assert_eq!(
        pace_stop_gaps(&stop.store),
        vec![(stop.poll_ms, None)],
        "the earlier stop record stands"
    );
    assert_eq!(stop.budget.stop_record(StopRecord::Recorded), StopRecord::Unrecorded);
    let contents = log_lines.contents();
    assert!(contents.contains("stream_stop_allowance_spent"), "{contents}");
}

#[tokio::test]
async fn stream_stop_allowance_a_grant_too_short_for_the_gap_write_keeps_the_earlier_stop() {
    // A 6 s grant cannot hold the gap write (it may wait the store's 5 s busy timeout,
    // and the lease release follows): no frame and no gap is written, the stop record
    // written before the frames stands, the stop counts as unrecorded and the log says
    // why.
    let log_lines = DecisionLog::start();
    let stop = stream_stop_allowance_stop(
        10,
        Duration::from_millis(75),
        Some(Duration::from_secs(6)),
        true,
        None,
    )
    .await;
    assert_eq!(stop.took, Duration::ZERO, "nothing was written");
    assert!(pace_committed(&stop.log, 1).is_empty());
    assert!(pace_recorded_gaps(&stop.log).is_empty(), "no gap was written");
    assert_eq!(
        pace_stop_gaps(&stop.store),
        vec![(stop.poll_ms, None)],
        "the earlier stop record stands"
    );
    assert_eq!(stop.budget.stop_record(StopRecord::Recorded), StopRecord::Unrecorded);
    let contents = log_lines.contents();
    let too_short: Vec<&str> = contents
        .lines()
        .filter(|line| line.contains("stream_stop_allowance_too_short"))
        .collect();
    assert_eq!(too_short.len(), 1, "{contents}");
    assert!(too_short[0].contains("unwritten=10"), "{contents}");
    assert!(too_short[0].contains("earlier stop record stands"), "{contents}");
}

// ---- REV-CX-SSA: the review's findings on STREAM-STOP-ALLOWANCE ------------------------------

#[tokio::test]
async fn stream_stop_allowance_rev_cx_ssa_1_a_deadline_given_during_the_stop_bounds_the_frames() {
    // The window's close began the stop with no deadline (frames for 20 s); the
    // service's stop then grants 11 s, measured from the stop's start, after the tenth
    // commit. The frames end at 1 s, not 20 s, and the gap write follows inside the
    // grant.
    const COMMIT: Duration = Duration::from_millis(75);
    let frames = STREAM_EVENT_CHANNEL_CAPACITY;
    let stop = stream_stop_allowance_stop(
        frames,
        COMMIT,
        None,
        true,
        Some((10, Duration::from_secs(11))),
    )
    .await;
    let written = pace_committed(&stop.log, 1).len();
    assert!((10..=1_000 / 75 + 1).contains(&written), "{written} written");
    assert!(stop.took <= Duration::from_secs(1) + COMMIT, "the stop took {:?}", stop.took);
    let frame_ms = |index: usize| stop.base_ms + i64::try_from(index).expect("index") * 333;
    assert_eq!(pace_recorded_gaps(&stop.log), vec![(frame_ms(written), frames - written)]);
    assert_eq!(stop.budget.stop_record(StopRecord::Recorded), StopRecord::Recorded);
}

#[tokio::test]
async fn stream_stop_allowance_rev_cx_ssa_1_a_ready_signal_bounds_a_stop_another_branch_began() {
    // The window's end won the run's select while the stop signal was ready too: the
    // orderly stop still takes the signal's grant before its first check, and follows
    // a deadline the slot gives later.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let slot = StopGrant::within(deadline);
    let shutdown = std::future::ready(slot.clone());
    tokio::pin!(shutdown);
    let grant = StopGrant::default();
    let mut seen = false;
    let first_check = watch_stop_grant(async { grant.deadline() }, shutdown.as_mut(), &mut seen, &grant).await;
    assert!(seen);
    assert_eq!(first_check, Some(deadline));
    let earlier = deadline - Duration::from_secs(2);
    slot.limit(earlier);
    assert_eq!(grant.deadline(), Some(earlier));
    // A signal that arrives while the stop runs is read at its next step.
    let (signal, receiver) = oneshot::channel::<()>();
    let late_slot = StopGrant::within(deadline);
    let shutdown = async move {
        let _ = receiver.await;
        late_slot
    };
    tokio::pin!(shutdown);
    let grant = StopGrant::default();
    let checks = grant.clone();
    let mut seen = false;
    let observed = watch_stop_grant(
        async move {
            let before = checks.deadline();
            let _ = signal.send(());
            tokio::task::yield_now().await;
            (before, checks.deadline())
        },
        shutdown.as_mut(),
        &mut seen,
        &grant,
    )
    .await;
    assert_eq!(observed, (None, Some(deadline)));
}

#[test]
fn stream_stop_allowance_rev_cx_ssa_2_time_before_the_stop_begins_comes_out_of_the_grant() {
    // A 15 s deadline given 300 ms before the stop begins leaves 14.7 s: frames end
    // 4.7 s after the start, not 5 s, and the gap write starts by 6.7 s.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    std::thread::sleep(Duration::from_millis(300));
    let began = Instant::now();
    let stop = StreamStopTime::begin(began, StopGrant::within(deadline));
    let limits = stop.limits();
    assert!(
        limits.frames_until <= began + Duration::from_millis(4_700),
        "the delay was added to the grant: {:?}",
        limits.frames_until - began
    );
    let gap_write_by = limits.gap_write_by.expect("the grant holds the gap write");
    assert!(gap_write_by <= began + Duration::from_millis(6_700));
    // Read again later, the limits stay where the one start put them.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(stop.limits(), limits);
}

#[tokio::test]
async fn stream_stop_allowance_rev_cx_ssa_3_a_short_stop_record_starts_before_the_discarded_frames() {
    // A newly started stream's first frames are queued, none of them stored, after the
    // car's polled observation; a 6 s grant discards them all and writes no gap. The
    // stop record starts at the polled observation, before every discarded frame, so
    // coverage claims none of their time.
    let stop = stream_stop_allowance_stop(
        10,
        Duration::from_millis(75),
        Some(Duration::from_secs(6)),
        true,
        None,
    )
    .await;
    assert!(pace_committed(&stop.log, 1).is_empty(), "every frame was discarded");
    let gaps = pace_stop_gaps(&stop.store);
    assert_eq!(gaps.len(), 1);
    assert!(gaps[0].0 <= stop.base_ms, "the stop record starts after a discarded frame: {gaps:?}");
    assert_eq!(gaps, vec![(stop.poll_ms, None)]);
}

#[tokio::test]
async fn stream_stop_allowance_rev_cx_ssa_4_a_short_grant_after_an_unrecorded_stop_says_none_stands() {
    // The stop record did not commit (another writer held the store past its busy
    // timeout) and the 6 s grant cannot hold the gap write: the log says no stop is
    // recorded, never that an earlier stop record stands.
    let log_lines = DecisionLog::start();
    let stop = stream_stop_allowance_stop(
        10,
        Duration::from_millis(75),
        Some(Duration::from_secs(6)),
        false,
        None,
    )
    .await;
    assert!(pace_stop_gaps(&stop.store).is_empty(), "no stop row exists");
    assert_eq!(stop.budget.stop_record(StopRecord::Unrecorded), StopRecord::Unrecorded);
    let contents = log_lines.contents();
    let too_short: Vec<&str> = contents
        .lines()
        .filter(|line| line.contains("stream_stop_allowance_too_short"))
        .collect();
    assert_eq!(too_short.len(), 1, "{contents}");
    assert!(!too_short[0].contains("earlier stop record stands"), "{contents}");
    assert!(too_short[0].contains("did not commit"), "{contents}");
}

// ---- SSA-R2: the second repair round after REV-CX-RECHECK-SSA -----------------------------------

#[tokio::test]
async fn ssa_r2_3_a_short_stop_record_starts_at_a_discarded_frame_observed_before_the_poll() {
    // REV-CX-RECHECK-SSA finding 3: a newly started stream's queued frames carry the
    // car's own observation times, and the first of them precede the Hub's last poll
    // (3 s before it). A 6 s grant discards them all and writes no gap, so the stop
    // record must start at the earliest discarded frame, not at the poll.
    let stop = stream_stop_allowance_stop_from(
        10,
        Duration::from_millis(75),
        Some(Duration::from_secs(6)),
        true,
        None,
        -8_000,
    )
    .await;
    let first_frame_ms = stop.base_ms - 8_000;
    assert!(first_frame_ms < stop.poll_ms, "the first frame precedes the poll");
    assert!(pace_committed(&stop.log, 1).is_empty(), "every frame was discarded");
    assert_eq!(
        pace_stop_gaps(&stop.store),
        vec![(first_frame_ms, None)],
        "the stop record starts at the earliest discarded frame"
    );
}

#[tokio::test]
async fn ssa_r2_3_frames_read_ahead_for_the_floor_are_still_written_in_queue_order() {
    // REV-CX-RECHECK-SSA finding 3, the fix's other side: reading the queue ahead for
    // the frames' times loses and reorders none of them. With the default grant every
    // frame is written, in queue order, and the gap write moves the stop row to the
    // stop time.
    let stop = stream_stop_allowance_stop_from(10, Duration::from_millis(75), None, true, None, -8_000).await;
    let expected: Vec<i64> = (0..10).map(|index| stop.base_ms - 8_000 + index * 333).collect();
    assert_eq!(pace_committed(&stop.log, 1), expected);
    let gaps = pace_stop_gaps(&stop.store);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert!(gaps[0].0 > expected[9], "the gap write moved the row past the frames: {gaps:?}");
}

/// A stream run of car `vehicle` whose producer ends at the stop signal, with `events`
/// queued in order.
fn ssa_r2_queued_events(vehicle: u64, events: Vec<StreamEvent>) -> VehicleStreamRuntime {
    let (sender, receiver) = new_stream_event_channel();
    let (shutdown, stop) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = stop.await;
        Ok::<(), crate::tesla_stream::StreamSupervisorError>(())
    });
    for event in events {
        sender.try_send(event).expect("room in the queue");
    }
    pace_runtime(vehicle, receiver, Some(shutdown), task)
}

#[tokio::test]
async fn ssa_r2_new_a_control_only_queue_moves_no_stop_floor() {
    // REV-CX-RECHECK-SSA new finding: a car with a stored poll and no stored stream
    // frame has only a transport outage queued at the stop. No frame of it can be
    // dropped, so it gives the account-wide stop row no earlier start (which the
    // handback would subtract from every car's coverage). The same holds for a car
    // whose stream frames are durable. A frame behind a control event still counts,
    // and the events stay in queue order for the stop's drain.
    let directory = crate::private_tempdir().expect("temporary store");
    let store = HubStore::initialize(directory.path()).expect("store");
    let mut core = Core::owner();
    let t0 = core.t0;
    core.scheduler
        .accept_discovery(vec![core_vehicle(1, "online"), core_vehicle(2, "online")], t0);
    let base_ms = current_epoch_millis().expect("clock") - 1_000_000;
    core.scheduler.note_observation_stored(core_id(1), base_ms - 5_000);
    let mut cache = HashMap::new();

    let mut streams = vec![ssa_r2_queued_events(1, vec![StreamEvent::TransportUnavailable])];
    assert_eq!(queued_stream_frames_floor_ms(&mut streams, &cache, &core.scheduler), None);

    // Car 2 stores one frame in this run, then only an outage is queued.
    let mut durable = vec![ssa_r2_queued_events(2, vec![drain_stream_frame(base_ms, 0)])];
    SUPERVISED_COLLECTOR_TEST_SEAM
        .scope(
            Arc::new(SupervisedCollectorTestSeam::default()),
            drain_stream_events_with_cache(&store, &mut core.scheduler, &mut durable, &mut cache),
        )
        .await
        .expect("the frame is stored");
    assert!(cache.get(&core_id(2)).and_then(|context| context.last_stream_timestamp_ms).is_some());
    let mut durable = vec![ssa_r2_queued_events(2, vec![StreamEvent::TransportUnavailable])];
    assert_eq!(queued_stream_frames_floor_ms(&mut durable, &cache, &core.scheduler), None);

    // A frame behind the outage: the floor is the earlier of the poll and the frame,
    // and the outage still comes first.
    let mut mixed = vec![ssa_r2_queued_events(
        1,
        vec![StreamEvent::TransportUnavailable, drain_stream_frame(base_ms - 9_000, 0)],
    )];
    assert_eq!(
        queued_stream_frames_floor_ms(&mut mixed, &cache, &core.scheduler),
        Some(base_ms - 9_000)
    );
    assert!(matches!(mixed[0].next_event(), Some(StreamEvent::TransportUnavailable)));
    assert!(matches!(
        mixed[0].next_event(),
        Some(StreamEvent::Telemetry { update, .. }) if update.timestamp_ms == base_ms - 9_000
    ));
    assert!(mixed[0].next_event().is_none());
    assert!(mixed[0].held_frame.is_none());
}
