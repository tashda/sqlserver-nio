import Foundation
import SQLServerKitTesting

let requireAdventureWorks = CommandLine.arguments.contains("--require-adventureworks")

do {
    let report = try ensureSQLServerTestFixture(requireAdventureWorks: requireAdventureWorks)
    print("fixture=sqlserver")
    print("image=\(report.image)")
    print("port=\(report.port)")
    print("fixture_version=\(report.fixtureVersion)")
    print("reused_container=\(report.reusedContainer)")
    print("recreated_container=\(report.recreatedContainer)")
    print("validations=\(report.validations.joined(separator: ","))")
} catch {
    let message = "sqlserver-test-fixture failed: \(error)\n"
    FileHandle.standardError.write(Data(message.utf8))
    exit(1)
}
