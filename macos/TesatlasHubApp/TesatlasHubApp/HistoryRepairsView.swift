// SPDX-License-Identifier: AGPL-3.0-only

import AppKit

/// What an input dialog of History repairs asks for.
struct HubRepairInputRequest: Equatable {
    let title: String
    let info: String
    let confirm: String
    /// Protect also asks whether the number is a drive or a position.
    var choosesEntity = false
    var prefill = ""
}

struct HubRepairInputAnswer: Equatable {
    let text: String
    var entity: HubRepairEntity = .drive
}

/// Service details > History repairs (M03-T28c): whether automatic repairs are on, the next
/// and last check, the counts and the state, with Check now, Attest, Protect, Undo and Retry
/// through the owner-channel repair verbs. Only Hub's own history changes; nothing goes to Tesla.
final class HistoryRepairsView: NSView {
    typealias ConfirmationPresenter = (NSAlert, NSApplication.ModalResponse) -> NSApplication.ModalResponse
    typealias InputPresenter = (HubRepairInputRequest) -> HubRepairInputAnswer?

    private let backend: HubRepairsBackend
    private let mutationAllowed: () -> Bool
    private let confirmationPresenter: ConfirmationPresenter
    private let inputPresenter: InputPresenter
    private let date: (Date) -> String

    let rowsStack = NSStackView()
    let messageLabel = NSTextField(wrappingLabelWithString: "")
    let carPopUp = NSPopUpButton(frame: .zero, pullsDown: false)
    let carRow = NSStackView()
    let automaticButton = HubActionButton(title: "", target: nil, action: nil)
    let checkNowButton = HubActionButton(title: HubL10n.text("hub.repairs.checkNow", fallback: "Check now"), target: nil, action: nil)
    let attestButton = HubActionButton(title: HubL10n.text("hub.repairs.attest", fallback: "Attest…"), target: nil, action: nil)
    let protectButton = HubActionButton(title: HubL10n.text("hub.repairs.protect", fallback: "Protect…"), target: nil, action: nil)
    let undoButton = HubActionButton(title: HubL10n.text("hub.repairs.undo", fallback: "Undo…"), target: nil, action: nil)
    let retryButton = HubActionButton(title: HubL10n.text("hub.repairs.retry", fallback: "Retry…"), target: nil, action: nil)

    private(set) var vehicles: [(id: UUID, name: String)] = []
    private(set) var summary: HubRepairSummary?
    private(set) var shownRows: [HubServiceDetail] = []
    private(set) var busy = false
    /// The batch the last check of this session made, offered when Undo asks for one.
    private(set) var lastBatchId: Int64?
    private var selectedVehicle: UUID?
    private var mutationsEnabled = true

    init(backend: HubRepairsBackend,
         mutationAllowed: @escaping () -> Bool = { true },
         confirmationPresenter: @escaping ConfirmationPresenter = { alert, silentResponse in
             HubUIPresentation.response(to: alert, silentResponse: silentResponse)
         },
         inputPresenter: @escaping InputPresenter = HistoryRepairsView.presentInput,
         date: @escaping (Date) -> String = HubCollectionStatus.defaultDateText) {
        self.backend = backend
        self.mutationAllowed = mutationAllowed
        self.confirmationPresenter = confirmationPresenter
        self.inputPresenter = inputPresenter
        self.date = date
        super.init(frame: .zero)
        identifier = NSUserInterfaceItemIdentifier("hub.repairs")
        translatesAutoresizingMaskIntoConstraints = false
        build()
        updateButtons()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    // MARK: Loading

    func reload(completion: (() -> Void)? = nil) {
        vehicles = backend.vehicleNames.map { (id: $0.key, name: $0.value) }
            .sorted { ($0.name, $0.id.uuidString) < ($1.name, $1.id.uuidString) }
        if selectedVehicle == nil || !vehicles.contains(where: { $0.id == selectedVehicle }) {
            selectedVehicle = vehicles.first?.id
        }
        carPopUp.removeAllItems()
        carPopUp.addItems(withTitles: vehicles.map(\.name))
        if let index = vehicles.firstIndex(where: { $0.id == selectedVehicle }) { carPopUp.selectItem(at: index) }
        carRow.isHidden = vehicles.count < 2
        guard let vehicle = selectedVehicle else {
            summary = nil
            show(rows: [])
            showMessage(HubL10n.text("hub.repairs.noCars", fallback: "No cars to show."), isError: false)
            updateButtons()
            completion?()
            return
        }
        backend.repairSummary(vehicle: vehicle) { [weak self] result in
            guard let self, self.selectedVehicle == vehicle else { return }
            switch result {
            case let .success(summary):
                self.summary = summary
                self.show(rows: Self.rows(for: summary, date: self.date))
            case let .failure(error):
                self.summary = nil
                self.show(rows: [])
                self.showMessage(Self.message(for: error), isError: true)
            }
            self.updateButtons()
            completion?()
        }
    }

    func setMutationsEnabled(_ enabled: Bool) {
        mutationsEnabled = enabled
        updateButtons()
    }

    // MARK: Text

    static func rows(for summary: HubRepairSummary, date: (Date) -> String) -> [HubServiceDetail] {
        func when(_ ms: Int64?, missing: String) -> String {
            ms.map { date(Date(timeIntervalSince1970: TimeInterval($0) / 1000)) } ?? missing
        }
        func text(_ key: String, _ fallback: String) -> String { HubL10n.text("hub.repairs." + key, fallback: fallback) }
        return [
            .init(label: text("automatic", "Automatic repairs"),
                  value: summary.automatic ? text("on", "On") : text("off", "Off")),
            .init(label: text("next", "Next check"),
                  value: summary.automatic ? when(summary.nextRunMs, missing: text("notScheduled", "Not scheduled"))
                                           : text("notScheduled", "Not scheduled")),
            .init(label: text("last", "Last check"), value: when(summary.lastRunMs, missing: text("never", "Not yet"))),
            .init(label: text("planned", "Planned"), value: String(summary.planned)),
            .init(label: text("repaired", "Repaired"), value: String(summary.active)),
            .init(label: text("undone", "Undone"), value: String(summary.undone)),
            .init(label: text("superseded", "Superseded"), value: String(summary.superseded)),
            .init(label: text("paused", "Set aside"), value: String(summary.suppressed)),
            .init(label: text("protected", "Protected"), value: String(summary.protectedCount)),
            .init(label: text("attested", "Attested"), value: String(summary.attested)),
            .init(label: text("status", "Status"), value: statusText(summary))
        ]
    }

    static func statusText(_ summary: HubRepairSummary) -> String {
        func text(_ key: String, _ fallback: String) -> String { HubL10n.text("hub.repairs.status." + key, fallback: fallback) }
        if summary.running { return text("running", "A repair job is running") }
        if !summary.indexed { return text("notIndexed", "History not indexed yet") }
        if summary.lastFailure != nil { return text("failed", "The last check did not finish") }
        switch summary.lastOutcome {
        case "repaired"?: return text("repaired", "The last check repaired history")
        case "no_change"?: return text("noChange", "The last check found nothing to repair")
        default: return text("none", "No check has run yet")
        }
    }

    /// A refusal or failure in the app's own words; raw service text is not shown.
    static func message(for error: Error) -> String {
        func text(_ key: String, _ fallback: String) -> String { HubL10n.text("hub.repairs.err." + key, fallback: fallback) }
        if let known = error as? HubMaintenanceError, case .unreadableReport = known {
            return known.errorDescription ?? text("failed", "The repair did not finish. Try again later.")
        }
        switch HubServiceRefusal.parse(error.localizedDescription)?.code {
        case "repair_unavailable"?: return text("unavailable", "Repairs are not available in this Hub.")
        case "repair_unknown_vehicle"?: return text("notIndexed", "Hub has not indexed this car's history yet.")
        case "repair_invalid"?: return HubL10n.text("hub.repairs.input.invalid", fallback: "That entry is not valid. Check it and try again.")
        case "repair_busy"?, "repair_not_ready"?, "repair_import_pending"?, "repair_job_unknown"?:
            return text("busy", "Hub is busy with this car. Try again shortly.")
        default: return text("failed", "The repair did not finish. Try again later.")
        }
    }

    static func message(for result: HubRepairResult) -> String? {
        func text(_ key: String, _ fallback: String) -> String { HubL10n.text("hub.repairs.done." + key, fallback: fallback) }
        switch result {
        case .noChange: return text("noChange", "Nothing needed repairing.")
        case let .repaired(batch):
            return HubL10n.format("hub.repairs.done.repaired", fallback: "History repaired (batch %1$@). You can undo it.", arguments: [String(batch)])
        case .deferred: return text("deferred", "The check was postponed. Hub will look again later.")
        case .automatic: return nil
        case .protected: return text("protected", "Protected from repair.")
        case let .attested(requested, attested):
            return HubL10n.format("hub.repairs.done.attested", fallback: "%1$@ of %2$@ requested drives attested.", arguments: [String(attested), String(requested)])
        case let .undone(rows):
            return HubL10n.format("hub.repairs.done.undone", fallback: "Undone. Rows restored: %1$@.", arguments: [String(rows)])
        case .retried: return text("retried", "The next check will look at it again.")
        }
    }

    // MARK: Input

    static func parseDriveIds(_ text: String) -> [Int64]? {
        let parts = text.split(whereSeparator: { $0 == "," || $0 == " " || $0.isNewline }).map(String.init)
        let ids = parts.compactMap { Int64($0).flatMap { $0 > 0 ? $0 : nil } }
        guard !parts.isEmpty, ids.count == parts.count, parts.count <= 512 else { return nil }
        var seen = Set<Int64>()
        return ids.filter { seen.insert($0).inserted }
    }

    static func parseEntityId(_ text: String) -> Int64? {
        Int64(text.trimmingCharacters(in: .whitespacesAndNewlines)).flatMap { $0 > 0 ? $0 : nil }
    }

    static func parseComponentKey(_ text: String) -> String? {
        let key = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return key.count == 64 && key.allSatisfy({ $0.isHexDigit }) ? key : nil
    }

    static func parseTarget(_ text: String) -> HubRepairTarget? {
        if let batch = parseEntityId(text), text.trimmingCharacters(in: .whitespacesAndNewlines).allSatisfy(\.isNumber) {
            return .batch(batch)
        }
        return parseComponentKey(text).map { .component($0) }
    }

    /// A dialog with a text field; Cancel is the first button, so a silent host cancels.
    static func presentInput(_ request: HubRepairInputRequest) -> HubRepairInputAnswer? {
        let alert = NSAlert()
        alert.messageText = request.title
        alert.informativeText = request.info
        alert.addButton(withTitle: HubL10n.text("hub.ImportSheetController.112.1706", fallback: "Cancel"))
        alert.addButton(withTitle: request.confirm)
        alert.buttons[0].keyEquivalent = "\u{1b}"
        alert.buttons[1].keyEquivalent = "\r"
        let field = NSTextField(string: request.prefill)
        field.identifier = NSUserInterfaceItemIdentifier("hub.repairs.input")
        field.widthAnchor.constraint(equalToConstant: 280).isActive = true
        let popUp = NSPopUpButton(frame: .zero, pullsDown: false)
        popUp.addItems(withTitles: [HubL10n.text("hub.repairs.entity.drive", fallback: "Drive"),
                                    HubL10n.text("hub.repairs.entity.position", fallback: "Position")])
        let stack = NSStackView(views: request.choosesEntity ? [popUp, field] : [field])
        stack.spacing = 8
        stack.frame = NSRect(x: 0, y: 0, width: 360, height: 28)
        alert.accessoryView = stack
        guard HubUIPresentation.response(to: alert, silentResponse: .alertFirstButtonReturn) == .alertSecondButtonReturn else {
            return nil
        }
        return HubRepairInputAnswer(text: field.stringValue, entity: popUp.indexOfSelectedItem == 1 ? .position : .drive)
    }

    // MARK: Actions

    @objc func carChanged(_ sender: Any?) {
        let index = carPopUp.indexOfSelectedItem
        guard vehicles.indices.contains(index), !busy else { return }
        selectedVehicle = vehicles[index].id
        reload()
    }

    @objc func checkNowPressed(_ sender: Any?) { perform(.checkNow) }

    @objc func automaticPressed(_ sender: Any?) {
        guard let summary else { return }
        guard !summary.automatic else { perform(.automatic(on: false, phoneWitness: false)); return }
        guard mutationsEnabled, mutationAllowed(), !busy else { HubUIPresentation.playRefusalSound(); return }
        let alert = NSAlert()
        alert.messageText = HubL10n.text("hub.repairs.on.title", fallback: "Turn on automatic repairs?")
        alert.informativeText = HubL10n.text("hub.repairs.on.info", fallback: "Hub will check this car's history once a month and repair gaps. Turn this on after a phone has shown the repaired history correctly.")
        alert.addButton(withTitle: HubL10n.text("hub.ImportSheetController.112.1706", fallback: "Cancel"))
        alert.addButton(withTitle: HubL10n.text("hub.repairs.on.confirm", fallback: "Turn on"))
        alert.buttons[0].keyEquivalent = "\r"
        alert.buttons[1].keyEquivalent = ""
        guard confirmationPresenter(alert, .alertFirstButtonReturn) == .alertSecondButtonReturn else { return }
        perform(.automatic(on: true, phoneWitness: true))
    }

    @objc func attestPressed(_ sender: Any?) {
        guard let answer = ask("attest", "Attest drives",
                               "Enter the numbers of drives you have not edited, separated by commas (up to 512).",
                               "Attest") else { return }
        guard let ids = Self.parseDriveIds(answer.text) else { return invalidEntry() }
        perform(.attest(ids))
    }

    @objc func protectPressed(_ sender: Any?) {
        guard let answer = ask("protect", "Protect from repair",
                               "Choose a drive or position and enter its number. Hub leaves it as it is.",
                               "Protect", choosesEntity: true) else { return }
        guard let id = Self.parseEntityId(answer.text) else { return invalidEntry() }
        perform(.protect(entity: answer.entity, id: id))
    }

    @objc func undoPressed(_ sender: Any?) {
        guard let answer = ask("undo", "Undo a repair",
                               "Enter a batch number or a 64-character component key. Hub restores the earlier history.",
                               "Undo", prefill: lastBatchId.map(String.init) ?? "") else { return }
        guard let target = Self.parseTarget(answer.text) else { return invalidEntry() }
        perform(.undo(target))
    }

    @objc func retryPressed(_ sender: Any?) {
        guard let answer = ask("retry", "Retry a repair",
                               "Enter the 64-character component key of a set-aside repair. The next check looks at it again.",
                               "Retry") else { return }
        guard let key = Self.parseComponentKey(answer.text) else { return invalidEntry() }
        perform(.retry(componentKey: key))
    }

    private func ask(_ key: String, _ title: String, _ info: String, _ confirm: String,
                     choosesEntity: Bool = false, prefill: String = "") -> HubRepairInputAnswer? {
        guard mutationsEnabled, mutationAllowed(), !busy, selectedVehicle != nil else {
            HubUIPresentation.playRefusalSound()
            return nil
        }
        return inputPresenter(HubRepairInputRequest(
            title: HubL10n.text("hub.repairs.\(key).title", fallback: title),
            info: HubL10n.text("hub.repairs.\(key).info", fallback: info),
            confirm: HubL10n.text("hub.repairs.\(key).confirm", fallback: confirm),
            choosesEntity: choosesEntity, prefill: prefill))
    }

    private func invalidEntry() {
        showMessage(HubL10n.text("hub.repairs.input.invalid", fallback: "That entry is not valid. Check it and try again."), isError: true)
    }

    private func perform(_ action: HubRepairAction) {
        guard mutationsEnabled, mutationAllowed(), !busy, let vehicle = selectedVehicle else {
            HubUIPresentation.playRefusalSound()
            return
        }
        busy = true
        updateButtons()
        showMessage(HubL10n.text("hub.repairs.working", fallback: "Working…"), isError: false)
        backend.repairAct(action, vehicle: vehicle) { [weak self] result in
            guard let self else { return }
            self.busy = false
            let note: (String?, Bool)
            switch result {
            case let .success(outcome):
                if case let .repaired(batch) = outcome { self.lastBatchId = batch }
                note = (Self.message(for: outcome), false)
            case let .failure(error):
                note = (Self.message(for: error), true)
            }
            self.reload {
                if let text = note.0 { self.showMessage(text, isError: note.1) } else { self.messageLabel.isHidden = true }
            }
        }
    }

    // MARK: Showing

    private func show(rows: [HubServiceDetail]) {
        shownRows = rows
        messageLabel.isHidden = true
        rowsStack.arrangedSubviews.forEach {
            rowsStack.removeArrangedSubview($0)
            $0.removeFromSuperview()
        }
        for (index, detail) in rows.enumerated() {
            if index > 0 {
                let line = HubModalChrome.divider()
                rowsStack.addArrangedSubview(line)
                line.widthAnchor.constraint(equalTo: rowsStack.widthAnchor).isActive = true
            }
            let row = ServiceDetailRowView(detail: detail)
            rowsStack.addArrangedSubview(row)
            row.widthAnchor.constraint(equalTo: rowsStack.widthAnchor).isActive = true
        }
    }

    private func showMessage(_ text: String, isError: Bool) {
        messageLabel.stringValue = text
        messageLabel.textColor = isError ? HubPalette.danger : HubPalette.mutedForeground
        messageLabel.isHidden = false
    }

    private func updateButtons() {
        let ready = mutationsEnabled && !busy && selectedVehicle != nil
        for button in [checkNowButton, attestButton, protectButton, undoButton, retryButton] { button.isEnabled = ready }
        automaticButton.isEnabled = ready && summary != nil
        automaticButton.title = (summary?.automatic == true)
            ? HubL10n.text("hub.repairs.turnOff", fallback: "Turn off automatic repairs")
            : HubL10n.text("hub.repairs.turnOn", fallback: "Turn on automatic repairs…")
        carPopUp.isEnabled = !busy
    }

    private func build() {
        let title = NSTextField(labelWithString: HubL10n.text("hub.repairs.title", fallback: "History repairs"))
        title.font = HubTypography.emphasis
        title.textColor = HubPalette.foreground
        let detail = NSTextField(wrappingLabelWithString: HubL10n.text("hub.repairs.intro", fallback: "Hub can fill gaps in a car's history using drives it already holds. Repairs change only Hub's own history, send nothing to Tesla and can be undone."))
        detail.font = HubTypography.caption
        detail.textColor = HubPalette.mutedForeground
        rowsStack.orientation = .vertical
        rowsStack.alignment = .leading
        rowsStack.spacing = 0
        rowsStack.identifier = NSUserInterfaceItemIdentifier("hub.repairs.list")
        messageLabel.identifier = NSUserInterfaceItemIdentifier("hub.repairs.message")
        messageLabel.font = HubTypography.caption
        messageLabel.isHidden = true
        let buttons: [(HubActionButton, String, Selector, HubButtonStyle)] = [
            (automaticButton, "automatic", #selector(automaticPressed(_:)), .flat),
            (checkNowButton, "checkNow", #selector(checkNowPressed(_:)), .neutral),
            (attestButton, "attest", #selector(attestPressed(_:)), .flat),
            (protectButton, "protect", #selector(protectPressed(_:)), .flat),
            (undoButton, "undo", #selector(undoPressed(_:)), .flat),
            (retryButton, "retry", #selector(retryPressed(_:)), .flat)
        ]
        for (button, name, action, style) in buttons {
            button.target = self
            button.action = action
            button.identifier = NSUserInterfaceItemIdentifier("hub.repairs.\(name)")
            button.hubStyle = style
            button.hubFont = HubTypography.action
            button.heightAnchor.constraint(equalToConstant: HubMetrics.compactControlHeight).isActive = true
        }
        let carLabel = NSTextField(labelWithString: HubL10n.text("hub.repairs.car", fallback: "Car"))
        carLabel.font = HubTypography.action
        carLabel.textColor = HubPalette.mutedForeground
        carPopUp.target = self
        carPopUp.action = #selector(carChanged(_:))
        carPopUp.identifier = NSUserInterfaceItemIdentifier("hub.repairs.car")
        carRow.setViews([carLabel, carPopUp], in: .leading)
        carRow.alignment = .centerY
        carRow.spacing = 8
        carRow.isHidden = true
        let header = NSStackView(views: [title, NSView(), automaticButton])
        header.alignment = .centerY
        let actions = NSStackView(views: [checkNowButton, attestButton, protectButton, undoButton, retryButton, NSView()])
        actions.alignment = .centerY
        actions.spacing = 8
        let contents = NSStackView(views: [header, detail, carRow, rowsStack, messageLabel, actions])
        contents.orientation = .vertical
        contents.alignment = .leading
        contents.spacing = 8
        contents.setCustomSpacing(4, after: header)
        contents.translatesAutoresizingMaskIntoConstraints = false
        let card = HubCardView()
        card.translatesAutoresizingMaskIntoConstraints = false
        card.addSubview(contents)
        addSubview(card)
        NSLayoutConstraint.activate([
            card.leadingAnchor.constraint(equalTo: leadingAnchor),
            card.trailingAnchor.constraint(equalTo: trailingAnchor),
            card.topAnchor.constraint(equalTo: topAnchor),
            card.bottomAnchor.constraint(equalTo: bottomAnchor),
            contents.leadingAnchor.constraint(equalTo: card.leadingAnchor, constant: HubMetrics.rowHorizontalInset),
            contents.trailingAnchor.constraint(equalTo: card.trailingAnchor, constant: -HubMetrics.rowHorizontalInset),
            contents.topAnchor.constraint(equalTo: card.topAnchor, constant: 14),
            contents.bottomAnchor.constraint(equalTo: card.bottomAnchor, constant: -14),
            rowsStack.widthAnchor.constraint(equalTo: contents.widthAnchor),
            header.widthAnchor.constraint(equalTo: contents.widthAnchor),
            actions.widthAnchor.constraint(equalTo: contents.widthAnchor)
        ])
        for view in [detail, messageLabel] {
            view.widthAnchor.constraint(lessThanOrEqualTo: contents.widthAnchor).isActive = true
        }
    }
}
