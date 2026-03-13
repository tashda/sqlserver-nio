import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class SQLServerEnvDiagnosticsTests: XCTestCase, @unchecked Sendable {
    func testPrintTdsEnvironment() throws {
        // Do not load .env here; we want to inspect what the host injected.
        let env = ProcessInfo.processInfo.environment
        let interestingKeys = [
            "TDS_ENV",
            "USE_DOCKER",
            "TDS_VERSION",
            "TDS_DOCKER_PORT",
            "TDS_LOAD_ADVENTUREWORKS",
            "TDS_AW_DATABASE",
            "TDS_HOSTNAME",
            "TDS_PORT",
            "TDS_DATABASE",
            "TDS_USERNAME",
            "LOG_LEVEL"
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

        report.append("--- Interpretation ---")
        let dockerMode = envFlagEnabled("USE_DOCKER") ? "ENABLED" : "disabled"
        let adventureWorksRestore = envFlagEnabled("TDS_LOAD_ADVENTUREWORKS") ? "ENABLED" : "disabled"
        report.append("docker_mode=\(dockerMode)")
        report.append("adventureworks_restore=\(adventureWorksRestore)")

        let text = report.joined(separator: "\n")
        print(text)

        #if canImport(Darwin)
        let attachment = XCTAttachment(string: text)
        attachment.lifetime = .keepAlways
        self.add(attachment)
        #endif
    }
}
