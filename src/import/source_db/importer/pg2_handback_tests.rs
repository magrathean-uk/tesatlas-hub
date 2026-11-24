// SPDX-License-Identifier: AGPL-3.0-only

//! PG-2: a handback plans its head with the stored rows its seam was planned
//! from. A collector tick that commits between the seam and the stage (a new
//! window opens and a drive in it closes) adds nothing to that head; the
//! tick's rows reach the next head. Synthetic data only.

use super::*;

/// A collector tick in a second window, still open: the window opens, then
/// one transaction stores and journals Hub drive `id` and a position of it,
/// without the publication gate and without publishing them.
pub(super) fn pg2_tick(connection: &mut rusqlite::Connection, vehicle: &str, id: i64) {
    let (start_ms, end_ms) = (T + 2 * HOUR, T + 2 * HOUR + 30 * MINUTE);
    connection
        .execute(
            "INSERT INTO collection_coverage(seq, grant_digest, opened_at_ms, closed_at_ms, gaps_json, sealed)
             VALUES (2, 'grant-2', ?1, NULL, '[]', 'open')",
            params![start_ms - 5 * MINUTE],
        )
        .expect("the second window opens");
    let mut drive = project_vehicle(&base_history(), CAR).expect("projection").snapshot.drives[0]
        .clone();
    drive.id = id;
    drive.start_date_ms = start_ms;
    drive.end_date_ms = end_ms;
    let drive = serde_json::to_string(&drive).expect("drive json");
    let mut position =
        crate::lifecycle::imported_position(&source_position(id, Some(id), start_ms + 15 * MINUTE));
    position.drive_id = Some(id);
    let position = serde_json::to_string(&position).expect("position json");
    let transaction = connection.transaction().expect("tick");
    for (entity, json) in [("drive", &drive), ("position", &position)] {
        let revision: i64 = transaction
            .query_row(
                "INSERT INTO sync_mutation_sequences(vehicle_id, next_revision) VALUES (?1, 2)
                 ON CONFLICT(vehicle_id) DO UPDATE SET next_revision = next_revision + 1
                 RETURNING next_revision - 1",
                params![vehicle],
                |row| row.get(0),
            )
            .expect("revision");
        transaction
            .execute(
                "INSERT INTO sync_mutations(vehicle_id, revision, entity, entity_id, car_id,
                    operation, payload_json, published, claimed_until_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'upsert', ?6, 0, 0)",
                params![vehicle, revision, entity, id, CAR, json],
            )
            .expect("mutation");
    }
    transaction
        .execute(
            "INSERT INTO materialised_drives(vehicle_id, drive_id, car_id, drive_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![vehicle, id, CAR, drive],
        )
        .expect("Hub drive");
    transaction
        .execute(
            "INSERT INTO materialised_positions(vehicle_id, position_id, drive_id, car_id, position_json)
             VALUES (?1, ?2, ?2, ?3, ?4)",
            params![vehicle, id, CAR, position],
        )
        .expect("Hub position");
    transaction.commit().expect("the tick commits");
}

#[test]
fn pg2_handback_head_holds_only_rows_its_seam_planned() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let env = paired(&fixture);
    let (h1, h2) = collect(&fixture);

    // The tick lands after the handback's seam commits, before its stage
    // reads the store.
    let h4 = HUB_ID_BASE + 40;
    let mut collector = fixture.store.open().expect("collector connection");
    let vehicle = fixture.binding.vehicle_id.to_string();
    AFTER_HANDBACK_SEAM.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || pg2_tick(&mut collector, &vehicle, h4)));
    });
    let outcome = handback(&fixture);
    let ticked = AFTER_HANDBACK_SEAM.with(|hook| hook.borrow_mut().take().is_none());
    assert!(ticked, "the tick ran inside the handback");
    assert_eq!(outcome, published(PhysicalV3PublicationKind::Rotation));
    let first = PhysicalClient::bootstrap(&env, &fixture)
        .expect("an open window leaves the planned head served");
    assert_eq!(
        first.mirror.drives.keys().copied().collect::<Vec<_>>(),
        vec![10, h1, h2],
        "the tick's drive is not in a head its seam did not plan"
    );
    assert_eq!(
        first.mirror.positions,
        BTreeMap::from([
            (HUB_ID_BASE + 1, (Some(h1), T + 15 * MINUTE)),
            (HUB_ID_BASE + 2, (Some(h2), T + 55 * MINUTE)),
        ]),
        "nor its position"
    );

    // The window closes and its live sync is published: the next head
    // holds the tick's rows.
    fixture
        .store
        .open()
        .expect("catalogue")
        .execute(
            "UPDATE collection_coverage SET closed_at_ms = ?1, sealed = 'handback' WHERE seq = 2",
            params![T + 3 * HOUR],
        )
        .expect("the second window closes");
    publish_sync_mutations_ahead_of_packs(
        &fixture.store,
        &fixture.gate,
        &fixture.cursor_key,
        fixture.binding.vehicle_id,
    )
    .expect("live sync of the tick");
    assert_eq!(
        handback(&fixture),
        published(PhysicalV3PublicationKind::Rotation)
    );
    let next = PhysicalClient::bootstrap(&env, &fixture).expect("served after the window");
    assert_eq!(
        next.mirror.drives.keys().copied().collect::<Vec<_>>(),
        vec![10, h1, h2, h4]
    );
    assert_eq!(
        next.mirror.positions.get(&h4),
        Some(&(Some(h4), T + 2 * HOUR + 15 * MINUTE)),
        "the tick's position reaches the next head under its drive"
    );
}

#[test]
fn pg2_handback_conflict_on_one_vehicle_still_publishes_the_next() {
    let fixture = fixture();
    let root = crate::private_tempdir().expect("stage root");
    publish_kept(&fixture, root.path(), "base", &base_history(), None);
    let env = paired(&fixture);
    let (h1, h2) = collect(&fixture);

    // A second vehicle with a kept stage, visited first, whose handback
    // meets a seam conflict.
    let first = Uuid::from_u128(1);
    assert!(first.to_string() < fixture.binding.vehicle_id.to_string());
    fixture
        .store
        .record_physical_v3_kept(
            &fixture.gate,
            first,
            Some(&crate::db::PhysicalV3Kept {
                stage: "a".repeat(64),
                source: "b".repeat(64),
                archive: None,
            }),
        )
        .expect("kept record");
    HANDBACK_CONFLICT_FOR.with(|conflict| conflict.set(Some(first)));
    let outcomes =
        publish_physical_after_handback(&fixture.store, &fixture.cursor_key, &fixture.gate);
    HANDBACK_CONFLICT_FOR.with(|conflict| conflict.set(None));

    let outcomes = outcomes.expect("one vehicle's conflict does not end the handback");
    assert_eq!(outcomes.len(), 2, "{outcomes:?}");
    assert_eq!(outcomes[0].0, first);
    assert!(
        matches!(outcomes[0].1, HandbackPhysicalOutcome::Withheld(_)),
        "the conflicting vehicle keeps its refusal: {outcomes:?}"
    );
    assert_eq!(
        outcomes[1],
        (
            fixture.binding.vehicle_id,
            published(PhysicalV3PublicationKind::Rotation)
        )
    );
    let served = PhysicalClient::bootstrap(&env, &fixture).expect("the next vehicle is served");
    assert_eq!(
        served.mirror.drives.keys().copied().collect::<Vec<_>>(),
        vec![10, h1, h2]
    );
}
