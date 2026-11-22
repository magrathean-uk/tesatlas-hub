// SPDX-License-Identifier: AGPL-3.0-only

import AppKit
import XCTest
@testable import Tesatlas_Hub

/// A repairs backend that records each verb and answers from a queue.
final class FakeRepairsBackend: HubRepairsBackend {
    var vehicleNames: [UUID: String] = [:]
    var summaryAnswer: Result<HubRepairSummary, Error> = .success(HubRepairSummary())
    var summaryCalls = 0
    var acts: [(HubRepairAction, UUID)] = []
    var actAnswers: [Result<HubRepairResult, Error>] = []

    func repairSummary(vehicle: UUID, completion: @escaping (Result<HubRepairSummary, Error>) -> Void) {
        summaryCalls += 1
        completion(summaryAnswer)
    }

    func repairAct(_ action: HubRepairAction, vehicle: UUID,
                   completion: @escaping (Result<HubRepairResult, Error>) -> Void) {
        acts.append((action, vehicle))
        completion(actAnswers.isEmpty ? .success(.noChange) : actAnswers.removeFirst())
    }
}

/// M03-T28c: Service details > History repairs. No window is shown, nothing is spoken and no
/// service is started.
final class BrokenDriveRepairTests: XCTestCase {
    private let car = UUID(uuidString: "11111111-2222-4333-8444-555555555555")!
    private let key = String(repeating: "ab", count: 32)

    private func summaryJSON(automatic: Bool = true) -> String {
        """
        {"vehicle":"\(car.uuidString.lowercased())","indexed":true,"indexRows":40,"planned":2,"active":5,"undone":1,\
        "superseded":3,"suppressed":4,"protected":6,"attested":7,"schedule":{"automatic":\(automatic),\
        "lastRunMs":1700000000000,"lastAttemptMs":1700000000000,"nextRunMs":1702592000000,\
        "lastOutcome":"repaired","lastFailure":null,"failures":0},"runningJob":null}
        """
    }

    private func make(_ backend: FakeRepairsBackend, allowed: @escaping () -> Bool = { true },
                      alert: @escaping (NSAlert) -> NSApplication.ModalResponse = { _ in .alertSecondButtonReturn },
                      input: @escaping (HubRepairInputRequest) -> HubRepairInputAnswer? = { _ in nil }) -> HistoryRepairsView {
        HistoryRepairsView(backend: backend, mutationAllowed: allowed,
                           confirmationPresenter: { shown, _ in alert(shown) },
                           inputPresenter: input,
                           date: { "D\(Int($0.timeIntervalSince1970))" })
    }

    private func loaded(automatic: Bool = true) throws -> (FakeRepairsBackend, HubRepairSummary) {
        let backend = FakeRepairsBackend()
        backend.vehicleNames = [car: "Model Y"]
        let summary = try XCTUnwrap(HubRepairSummary(report: summaryJSON(automatic: automatic)))
        backend.summaryAnswer = .success(summary)
        return (backend, summary)
    }

    func testTheSummaryReportIsReadWithItsScheduleAndCounts() throws {
        let summary = try XCTUnwrap(HubRepairSummary(report: "noise\n" + summaryJSON()))
        XCTAssertTrue(summary.indexed)
        XCTAssertTrue(summary.automatic)
        XCTAssertEqual([summary.planned, summary.active, summary.undone, summary.superseded,
                        summary.suppressed, summary.protectedCount, summary.attested], [2, 5, 1, 3, 4, 6, 7])
        XCTAssertEqual(summary.lastRunMs, 1_700_000_000_000)
        XCTAssertEqual(summary.nextRunMs, 1_702_592_000_000)
        XCTAssertEqual(summary.lastOutcome, "repaired")
        XCTAssertNil(summary.lastFailure)
        XCTAssertFalse(summary.running)
        XCTAssertNil(HubRepairSummary(report: "not json"))
        let unindexed = try XCTUnwrap(HubRepairSummary(report: #"{"indexed":false,"runningJob":{"jobId":3,"kind":"check_now"}}"#))
        XCTAssertFalse(unindexed.indexed)
        XCTAssertTrue(unindexed.running)
        XCTAssertFalse(unindexed.automatic)
    }

    func testScheduleAndUndo() throws {
        let (backend, _) = try loaded()
        var requests: [HubRepairInputRequest] = []
        let view = make(backend, input: { request in
            requests.append(request)
            return HubRepairInputAnswer(text: request.prefill)
        })
        view.reload()
        let values = Dictionary(uniqueKeysWithValues: view.shownRows.map { ($0.label, $0.value) })
        XCTAssertEqual(values["Automatic repairs"], "On")
        XCTAssertEqual(values["Next check"], "D1702592000")
        XCTAssertEqual(values["Last check"], "D1700000000")
        XCTAssertEqual(values["Repaired"], "5")
        XCTAssertEqual(values["Undone"], "1")
        XCTAssertEqual(values["Set aside"], "4")
        XCTAssertEqual(values["Status"], "The last check repaired history")
        XCTAssertEqual(view.automaticButton.title, "Turn off automatic repairs")
        XCTAssertTrue(view.carRow.isHidden, "one car shows no picker")

        backend.actAnswers = [.success(.repaired(batchId: 7))]
        view.checkNowButton.performClick(nil)
        XCTAssertEqual(backend.acts.map(\.0), [.checkNow])
        XCTAssertEqual(backend.acts.first?.1, car)
        XCTAssertEqual(view.lastBatchId, 7)
        XCTAssertEqual(view.messageLabel.stringValue, "History repaired (batch 7). You can undo it.")
        XCTAssertGreaterThanOrEqual(backend.summaryCalls, 2, "the summary is read again after a verb")

        backend.actAnswers = [.success(.undone(restoredRows: 12))]
        view.undoButton.performClick(nil)
        XCTAssertEqual(requests.last?.prefill, "7", "Undo offers the batch just made")
        XCTAssertEqual(backend.acts.last?.0, .undo(.batch(7)))
        XCTAssertEqual(view.messageLabel.stringValue, "Undone. Rows restored: 12.")

        backend.actAnswers = [.failure(HubActionError.commandExited(1, "Error: repair_busy: one repair job runs per car"))]
        view.undoButton.performClick(nil)
        XCTAssertEqual(view.messageLabel.stringValue, "Hub is busy with this car. Try again shortly.")
        XCTAssertTrue(view.undoButton.isEnabled, "buttons return after a refusal")
    }

    func testAutomaticOffNeedsNoWitnessAndOnAsksFirst() throws {
        var (backend, _) = try loaded()
        let view = make(backend)
        view.reload()
        view.automaticButton.performClick(nil)
        XCTAssertEqual(backend.acts.map(\.0), [.automatic(on: false, phoneWitness: false)])

        (backend, _) = try loaded(automatic: false)
        var alerts: [NSAlert] = []
        var answer = NSApplication.ModalResponse.alertFirstButtonReturn
        let asking = make(backend, alert: { alerts.append($0); return answer })
        asking.reload()
        XCTAssertEqual(asking.automaticButton.title, "Turn on automatic repairs…")
        XCTAssertEqual(asking.shownRows.first?.value, "Off")
        XCTAssertEqual(asking.shownRows.dropFirst().first?.value, "Not scheduled")
        asking.automaticButton.performClick(nil)
        XCTAssertEqual(alerts.count, 1)
        XCTAssertEqual(alerts[0].buttons[0].keyEquivalent, "\r", "Cancel answers Return")
        XCTAssertTrue(backend.acts.isEmpty, "Cancel changes nothing")
        answer = .alertSecondButtonReturn
        asking.automaticButton.performClick(nil)
        XCTAssertEqual(backend.acts.map(\.0), [.automatic(on: true, phoneWitness: true)])
    }

    func testEntriesAreCheckedBeforeAnyVerbRuns() throws {
        let (backend, _) = try loaded()
        var typed = ""
        var entity = HubRepairEntity.drive
        let view = make(backend, input: { _ in HubRepairInputAnswer(text: typed, entity: entity) })
        view.reload()
        typed = "12, x"
        view.attestButton.performClick(nil)
        view.protectButton.performClick(nil)
        view.undoButton.performClick(nil)
        view.retryButton.performClick(nil)
        XCTAssertTrue(backend.acts.isEmpty)
        XCTAssertEqual(view.messageLabel.stringValue, "That entry is not valid. Check it and try again.")

        typed = "12, 15 21,12"
        view.attestButton.performClick(nil)
        XCTAssertEqual(backend.acts.last?.0, .attest([12, 15, 21]))
        typed = "9"
        entity = .position
        view.protectButton.performClick(nil)
        XCTAssertEqual(backend.acts.last?.0, .protect(entity: .position, id: 9))
        typed = key.uppercased()
        view.undoButton.performClick(nil)
        XCTAssertEqual(backend.acts.last?.0, .undo(.component(key)))
        view.retryButton.performClick(nil)
        XCTAssertEqual(backend.acts.last?.0, .retry(componentKey: key))
        XCTAssertEqual(backend.acts.count, 4)
    }

    func testAClosedOwnerGateAndAMissingCarRunNothing() throws {
        let (backend, _) = try loaded()
        var asked = 0
        let gated = make(backend, allowed: { false }, input: { _ in asked += 1; return nil })
        gated.reload()
        gated.checkNowButton.performClick(nil)
        gated.attestButton.performClick(nil)
        XCTAssertEqual(asked, 0)
        XCTAssertTrue(backend.acts.isEmpty)
        let open = make(backend)
        open.reload()
        open.setMutationsEnabled(false)
        XCTAssertFalse(open.checkNowButton.isEnabled)
        XCTAssertFalse(open.automaticButton.isEnabled)

        let empty = FakeRepairsBackend()
        let none = make(empty)
        none.reload()
        XCTAssertEqual(none.messageLabel.stringValue, "No cars to show.")
        XCTAssertFalse(none.checkNowButton.isEnabled)
        XCTAssertEqual(empty.summaryCalls, 0)
    }

    func testSeveralCarsOfferAPickerAndAFailedReadShowsAMessage() throws {
        let (backend, _) = try loaded()
        let other = UUID()
        backend.vehicleNames[other] = "Model 3"
        let view = make(backend)
        view.reload()
        XCTAssertFalse(view.carRow.isHidden)
        XCTAssertEqual(view.carPopUp.itemTitles, ["Model 3", "Model Y"])
        backend.summaryAnswer = .failure(HubActionError.commandExited(1, "Error: repair_unavailable: no schedule"))
        view.reload()
        XCTAssertEqual(view.messageLabel.stringValue, "Repairs are not available in this Hub.")
        XCTAssertTrue(view.shownRows.isEmpty)
    }

    func testTheVerbsAndTheirResultsAreMapped() throws {
        XCTAssertEqual(HubRepairAction.checkNow.arguments, ["check-now"])
        XCTAssertEqual(HubRepairAction.automatic(on: true, phoneWitness: true).arguments, ["automatic", "on", "--phone-witness"])
        XCTAssertEqual(HubRepairAction.automatic(on: false, phoneWitness: true).arguments, ["automatic", "off"])
        XCTAssertEqual(HubRepairAction.attest([1, 2]).arguments, ["attest", "--drive-ids", "1,2"])
        XCTAssertEqual(HubRepairAction.undo(.batch(4)).arguments, ["undo", "--batch-id", "4"])
        XCTAssertEqual(HubRepairAction.undo(.component(key)).arguments, ["undo", "--component-key", key])
        XCTAssertEqual(HubRepairAction.retry(componentKey: key).arguments, ["retry", "--component-key", key])
        XCTAssertEqual(HubRepairAction.protect(entity: .drive, id: 3).arguments,
                       ["protect", "--entity", "drive", "--entity-id", "3", "--reason", "protect"])
        XCTAssertEqual(HubRepairResult.parse(.checkNow, result: ["outcome": "deferred", "reason": "backup"]), .deferred)
        XCTAssertEqual(HubRepairResult.parse(.attest([1]), result: ["requested": 1, "attested": 1]),
                       .attested(requested: 1, attested: 1))
        XCTAssertNil(HubRepairResult.parse(.undo(.batch(1)), result: ["outcome": "other"]))
    }

    func testTheControllerFollowsAJobUntilItEnds() throws {
        let home = HubTestScratch.fixtureRoot("BrokenDriveRepair")
        defer { try? FileManager.default.removeItem(at: home) }
        let runner = RecordingMaintenanceRunner()
        let id = car.uuidString.lowercased()
        runner.answer = { arguments in
            if arguments.contains("check-now") {
                return .success(#"{"jobId":9,"kind":"check_now","vehicle":"\#(id)","state":"running"}"#)
            }
            return .success(#"{"jobId":9,"kind":"check_now","vehicle":"\#(id)","state":"done","result":{"outcome":"repaired","batchId":3}}"#)
        }
        let controller = HubController(environment: [:], commandRunner: runner, launchAgent: nil,
                                       dataRemover: nil, homeDirectory: home, serviceInstalledOverride: true)
        let backend = HubControllerMaintenance(controller: controller)
        let finished = expectation(description: "finished")
        backend.repairAct(.checkNow, vehicle: car) { result in
            XCTAssertEqual(try? result.get(), .repaired(batchId: 3))
            finished.fulfill()
        }
        wait(for: [finished], timeout: 5)
        XCTAssertEqual(runner.arguments.map { Array($0.dropFirst(2)) },
                       [["repairs", "check-now", "--vehicle", id], ["repairs", "job", "--job-id", "9"]])
    }
}
