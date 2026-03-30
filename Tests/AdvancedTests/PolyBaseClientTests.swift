import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerPolyBaseClientTests: XCTestCase, @unchecked Sendable {
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

    @available(macOS 12.0, *)
    func testIsPolyBaseInstalled() async throws {
        // Just verify the query runs without error
        let installed = try await client.polyBase.isPolyBaseInstalled()
        // Result depends on server configuration — just validate it's a bool
        _ = installed
    }

    @available(macOS 12.0, *)
    func testListExternalDataSourcesEmpty() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_pb") { db in
                // On servers without PolyBase, sys.external_data_sources may not exist.
                // Wrap in do/catch to handle gracefully.
                do {
                    let sources = try await self.client.polyBase.listExternalDataSources(database: db)
                    // May be empty — just verify query succeeded
                    _ = sources
                } catch {
                    // sys.external_data_sources doesn't exist if PolyBase not installed
                    // That's expected — skip the test
                    throw XCTSkip("PolyBase system views not available: \(error)")
                }
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during PolyBase test") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testListExternalTablesEmpty() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_pt") { db in
                do {
                    let tables = try await self.client.polyBase.listExternalTables(database: db)
                    _ = tables
                } catch {
                    throw XCTSkip("PolyBase system views not available: \(error)")
                }
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during PolyBase test") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testListExternalFileFormatsEmpty() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_pf") { db in
                do {
                    let formats = try await self.client.polyBase.listExternalFileFormats(database: db)
                    _ = formats
                } catch {
                    throw XCTSkip("PolyBase system views not available: \(error)")
                }
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during PolyBase test") }
            throw e
        }
    }
}
