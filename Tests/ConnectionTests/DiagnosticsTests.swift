import XCTest
@testable import SQLServerKit

final class SQLServerEnvDiagnosticsTests: XCTestCase {
    func testPrintTdsEnvironment() throws {
        // Do not load .env here; we want to inspect what the host injected.
        let env = ProcessInfo.processInfo.environment
        let interestingKeys = [
            "TDS_ENABLE_EXPLORER_FLOW",
            "TDS_ENABLE_DEADLOCK_TESTS",
            "TDS_ENABLE_SCHEMA_TESTS",
            "TDS_ENABLE_AGENT_TESTS",
            "TDS_ENABLE_ADVENTUREWORKS",
            "TDS_HOSTNAME",
            "TDS_PORT",
            "TDS_DATABASE",
            "TDS_USERNAME"
        ]

        var report: [String] = []
        report.append("--- TDS environment diagnostics ---")
        for key in interestingKeys {
            let value = env[key] ?? "<nil>"
            report.append("\(key)=\(value)")
        }
        // Also dump all TDS_* keys present
        let allTds = env.filter { $0.key.hasPrefix("TDS_") }.sorted { $0.key < $1.key }
        report.append("--- All TDS_* ---")
        for (k, v) in allTds { report.append("\(k)=\(v)") }

        // Evaluate gating flags using same logic as tests
        func gate(_ k: String) -> String { envFlagEnabled(k) ? "ENABLED" : "disabled" }
        report.append("--- Gates ---")
        report.append("TDS_ENABLE_EXPLORER_FLOW: \(gate("TDS_ENABLE_EXPLORER_FLOW"))")
        report.append("TDS_ENABLE_DEADLOCK_TESTS: \(gate("TDS_ENABLE_DEADLOCK_TESTS"))")

        let text = report.joined(separator: "\n")
        print(text)

        let attachment = XCTAttachment(string: text)
        attachment.lifetime = .keepAlways
        add(attachment)
    }
}

