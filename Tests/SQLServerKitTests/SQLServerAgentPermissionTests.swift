import Foundation
import NIO
@testable import SQLServerKit
import XCTest

final class SQLServerAgentPermissionTests: XCTestCase {
    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    func testAgentProxyPermissionReport() throws {
        loadEnvFileIfPresent()
        // Only run when proxy tests are enabled since this is admin-leaning.
        try requireEnvFlag("TDS_ENABLE_AGENT_PROXY_TESTS", description: "SQL Agent proxy permission report")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let agent = SQLServerAgentClient(connection: conn)
        let report = try waitForResult(agent.fetchProxyAndCredentialPermissions(), timeout: TIMEOUT, description: "fetch perms")

        // Sanity: fields are present; we don't enforce a policy here.
        XCTAssertNotNil(report.isSysadmin as Bool?)
        XCTAssertNotNil(report.hasAlterAnyCredential as Bool?)
        // roles list is best-effort; presence is enough
        _ = report.msdbRoles
    }
}

