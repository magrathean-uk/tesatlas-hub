// SPDX-License-Identifier: AGPL-3.0-only

import Foundation
import XCTest
@testable import Tesatlas_Hub

final class DiagnosticsReportReplacementTests: XCTestCase {
    func testFailedWritePreservesExistingReportAndRemovesStaging() throws {
        let destination = try existingReport()

        XCTAssertThrowsError(try DiagnosticsReportReplacement.write("New report", to: destination) { _, staged in
            try Data("Partial report".utf8).write(to: staged)
            XCTAssertEqual(try Data(contentsOf: destination), Data("Previous report\n".utf8))
            throw CocoaError(.fileWriteOutOfSpace)
        }) { error in
            XCTAssertEqual((error as? CocoaError)?.code, .fileWriteOutOfSpace)
        }

        XCTAssertEqual(try Data(contentsOf: destination), Data("Previous report\n".utf8))
        try assertNoStaging(beside: destination)
    }

    func testFailedRenamePreservesExistingReportAndRemovesStaging() throws {
        let destination = try existingReport()

        // Leave the staged file absent so the actual rename fails.
        XCTAssertThrowsError(try DiagnosticsReportReplacement.write("New report", to: destination) { _, _ in })

        XCTAssertEqual(try Data(contentsOf: destination), Data("Previous report\n".utf8))
        try assertNoStaging(beside: destination)
    }

    func testSuccessfulReplacementPublishesCompletePrivateReportAndRemovesStaging() throws {
        let destination = try existingReport()
        let replacement = "Résumé\n✓\n"

        try DiagnosticsReportReplacement.write(replacement, to: destination)

        XCTAssertEqual(try Data(contentsOf: destination), Data(replacement.utf8))
        let attributes = try FileManager.default.attributesOfItem(atPath: destination.path)
        XCTAssertEqual(attributes[.posixPermissions] as? Int, 0o600)
        try assertNoStaging(beside: destination)
    }

    private func existingReport() throws -> URL {
        let manager = FileManager.default
        let directory = manager.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try manager.createDirectory(at: directory, withIntermediateDirectories: false,
                                    attributes: [.posixPermissions: 0o700])
        addTeardownBlock { try manager.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("diagnostics.txt")
        try Data("Previous report\n".utf8).write(to: destination)
        try manager.setAttributes([.posixPermissions: 0o644], ofItemAtPath: destination.path)
        return destination
    }

    private func assertNoStaging(beside destination: URL, file: StaticString = #filePath, line: UInt = #line) throws {
        let entries = try FileManager.default.contentsOfDirectory(atPath: destination.deletingLastPathComponent().path)
        XCTAssertEqual(entries, [destination.lastPathComponent], file: file, line: line)
    }
}
