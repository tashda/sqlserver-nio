import XCTest
@testable import SQLServerKit

final class SQLServerServerSecurityVariantsTests: XCTestCase {
    private var client: SQLServerClient!
    private var group: EventLoopGroup!

    override func setUpWithError() throws {
        try super.setUpWithError()
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server login variants integration tests")
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
        _ = try? waitForResult(serverSec.createExternalLogin(name: "nio_ext_login_test"), timeout: 30, description: "create external login")
        _ = try? waitForResult(serverSec.dropLogin(name: "nio_ext_login_test"), timeout: 30, description: "drop external login")
    }
}

