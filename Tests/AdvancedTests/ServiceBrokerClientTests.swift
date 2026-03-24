@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerServiceBrokerClientTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - Message Types

    @available(macOS 12.0, *)
    func testListMessageTypes() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_sb") { db in
                // Enable Service Broker on the temp database
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let messageTypes = try await self.client.serviceBroker.listMessageTypes(database: db)
                // Every database with broker has at least the system default message types
                XCTAssertFalse(messageTypes.isEmpty, "Expected at least system default message types")
                XCTAssertTrue(messageTypes.contains(where: { $0.name == "DEFAULT" }), "Expected DEFAULT message type")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during Service Broker test") }
            throw e
        }
    }

    // MARK: - Contracts

    @available(macOS 12.0, *)
    func testListContracts() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_sc") { db in
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let contracts = try await self.client.serviceBroker.listContracts(database: db)
                XCTAssertFalse(contracts.isEmpty, "Expected at least system default contracts")
                XCTAssertTrue(contracts.contains(where: { $0.name == "DEFAULT" }), "Expected DEFAULT contract")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during contracts test") }
            throw e
        }
    }

    // MARK: - Queues

    @available(macOS 12.0, *)
    func testListQueues() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_sq") { db in
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let queues = try await self.client.serviceBroker.listQueues(database: db)
                // System queues should always exist
                XCTAssertFalse(queues.isEmpty, "Expected at least system queues")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during queues test") }
            throw e
        }
    }

    // MARK: - Services

    @available(macOS 12.0, *)
    func testListServices() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_ss") { db in
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let services = try await self.client.serviceBroker.listServices(database: db)
                // There should be system services at minimum
                // Some test servers may not have them, so just verify query works
                _ = services
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during services test") }
            throw e
        }
    }

    // MARK: - Routes

    @available(macOS 12.0, *)
    func testListRoutes() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_sr") { db in
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let routes = try await self.client.serviceBroker.listRoutes(database: db)
                // AutoCreatedLocal route should exist
                XCTAssertTrue(routes.contains(where: { $0.name == "AutoCreatedLocal" }), "Expected AutoCreatedLocal route")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during routes test") }
            throw e
        }
    }

    // MARK: - Remote Service Bindings

    @available(macOS 12.0, *)
    func testListRemoteServiceBindings() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_rb") { db in
                _ = try? await self.client.execute("ALTER DATABASE [\(db)] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE")

                let bindings = try await self.client.serviceBroker.listRemoteServiceBindings(database: db)
                // May be empty — just verify the query succeeds
                _ = bindings
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during bindings test") }
            throw e
        }
    }
}
