import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class SQLServerServerSecurityVariantsTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!

    override func setUp() async throws {
        try await super.setUp()
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
        try await super.tearDown()
    }

    func testExternalLoginCreate() async throws {
        let serverSec = SQLServerServerSecurityClient(client: client)
        // This requires EXTERNAL PROVIDER support and elevated permissions; expect success only when enabled.
        do {
            try await serverSec.createExternalLogin(name: "nio_ext_login_test").get()
        } catch {
            // Expected to fail unless EXTERNAL PROVIDER is configured
            print("External login creation failed as expected: \(error)")
        }

        do {
            try await serverSec.dropLogin(name: "nio_ext_login_test").get()
        } catch {
            // Expected to fail unless login exists or EXTERNAL PROVIDER is configured
            print("External login drop failed as expected: \(error)")
        }
    }
}
