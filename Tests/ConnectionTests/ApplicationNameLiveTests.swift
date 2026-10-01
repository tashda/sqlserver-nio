import XCTest
import Foundation
import SQLServerKit
import SQLServerKitTesting

/// What a session reports about its client: the application name as APP_NAME() and program_name
/// (what DBAs see in Activity Monitor and sp_who2), the driver as client_interface_name.
final class ApplicationNameLiveTests: XCTestCase, @unchecked Sendable {
    override func setUp() async throws {
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables()
    }

    func testApplicationNameIsTheProgramName() async throws {
        var configuration = makeSQLServerConnectionConfiguration()
        configuration.applicationName = "Echo (tests)"
        let connection = try await SQLServerConnection.connect(configuration: configuration)
        defer { Task { try? await connection.close() } }
        let rows = try await connection.query("""
        SELECT APP_NAME() AS app, program_name AS program, client_interface_name AS interface
        FROM sys.dm_exec_sessions WHERE session_id = @@SPID
        """)
        let row = try XCTUnwrap(rows.first)
        XCTAssertEqual(row.column("app")?.string, "Echo (tests)")
        XCTAssertEqual(row.column("program")?.string, "Echo (tests)")
        XCTAssertEqual(row.column("interface")?.string, "sqlserver-nio")
    }
}
