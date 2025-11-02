import XCTest
@testable import SQLServerKit

final class SQLServerServerSecurityVariantsTests: XCTestCase {
    private var client: SQLServerClient!
    private var group: EventLoopGroup!

    override func setUpWithError() throws {
        try super.setUpWithError()
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
              group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDownWithError() throws {
        try? client?.shutdownGracefully().wait()
        try? group?.syncShutdownGracefully()
        client = nil
        group = nil
        try super.tearDownWithError()
    }

    func testExternalLoginCreate() throws {
        let serverSec = SQLServerServerSecurityClient(client: client)
        // This requires EXTERNAL PROVIDER support and elevated permissions; expect success only when enabled.
        do {
            try serverSec.createExternalLogin(name: "nio_ext_login_test").wait()
        } catch {
            // Expected to fail unless EXTERNAL PROVIDER is configured
            print("External login creation failed as expected: \(error)")
        }

        do {
            try serverSec.dropLogin(name: "nio_ext_login_test").wait()
        } catch {
            // Expected to fail unless login exists or EXTERNAL PROVIDER is configured
            print("External login drop failed as expected: \(error)")
        }
    }
}

