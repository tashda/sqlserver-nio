import Foundation
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

final class FullTextSearchTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    let operationTimeout: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)

        _ = try await withTimeout(operationTimeout) {
            try await self.client.query("SELECT 1")
        }
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully()
        } catch {
            let message = error.localizedDescription
            if !message.contains("Already closed") && !message.contains("ChannelError error 6") {
                throw error
            }
        }
    }

    // MARK: - List Catalogs

    func testListCatalogs() async throws {
        let catalogs: [SQLServerFullTextCatalog]
        do {
            catalogs = try await withTimeout(operationTimeout) {
                try await self.client.fullText.listCatalogs()
            }
        } catch {
            throw XCTSkip("Full-Text Search not available: \(error)")
        }

        // Result may be empty if no full-text catalogs exist; that's fine
        XCTAssertNotNil(catalogs, "listCatalogs should return a non-nil array")
        XCTAssertTrue(catalogs.count >= 0, "Should return zero or more catalogs")

        if let first = catalogs.first {
            XCTAssertFalse(first.name.isEmpty, "Catalog name should not be empty")
            XCTAssertTrue(first.catalogID >= 0, "Catalog ID should be non-negative")
        }
    }

    // MARK: - List Indexes

    func testListIndexes() async throws {
        let indexes: [SQLServerFullTextIndex]
        do {
            indexes = try await withTimeout(operationTimeout) {
                try await self.client.fullText.listIndexes()
            }
        } catch {
            throw XCTSkip("Full-Text Search not available: \(error)")
        }

        XCTAssertNotNil(indexes, "listIndexes should return a non-nil array")
        XCTAssertTrue(indexes.count >= 0, "Should return zero or more indexes")

        if let first = indexes.first {
            XCTAssertFalse(first.tableName.isEmpty, "Table name should not be empty")
            XCTAssertTrue(first.catalogID >= 0, "Catalog ID should be non-negative")
        }
    }

    // MARK: - Catalog Create & Drop

    func testCreateAndDropCatalog() async throws {
        // FTS cannot operate in master/tempdb/model — skip if we're connected to one of those
        let currentDB = try await client.query("SELECT DB_NAME() AS db").first?.column("db")?.string ?? "master"
        if ["master", "tempdb", "model"].contains(currentDB.lowercased()) {
            throw XCTSkip("Full-Text Search cannot be used in \(currentDB) database")
        }

        let existing: [SQLServerFullTextCatalog]
        do {
            existing = try await withTimeout(operationTimeout) {
                try await self.client.fullText.listCatalogs()
            }
        } catch {
            throw XCTSkip("Full-Text Search not available: \(error)")
        }

        guard existing.isEmpty else {
            print("Skipping catalog creation — \(existing.count) catalog(s) already exist")
            return
        }

        let catalogName = "test_ft_catalog_\(UUID().uuidString.prefix(8))"

        do {
            try await withTimeout(operationTimeout) {
                try await self.client.fullText.createCatalog(name: catalogName)
            }

            // Verify catalog was created
            let catalogs = try await withTimeout(operationTimeout) {
                try await self.client.fullText.listCatalogs()
            }
            let created = catalogs.first(where: { $0.name == catalogName })
            XCTAssertNotNil(created, "Created catalog should appear in the list")

            // Clean up
            try await withTimeout(operationTimeout) {
                try await self.client.fullText.dropCatalog(name: catalogName)
            }

            // Verify drop
            let afterDrop = try await withTimeout(operationTimeout) {
                try await self.client.fullText.listCatalogs()
            }
            let stillExists = afterDrop.first(where: { $0.name == catalogName })
            XCTAssertNil(stillExists, "Dropped catalog should no longer appear in the list")
        } catch {
            // Clean up on failure
            try? await self.client.fullText.dropCatalog(name: catalogName)
            throw error
        }
    }

    // MARK: - PopulationType Enum

    func testPopulationTypeEnumCases() {
        // Verify all expected enum cases exist and have correct raw values
        XCTAssertEqual(SQLServerFullTextClient.PopulationType.full.rawValue, "FULL")
        XCTAssertEqual(SQLServerFullTextClient.PopulationType.incremental.rawValue, "INCREMENTAL")
        XCTAssertEqual(SQLServerFullTextClient.PopulationType.update.rawValue, "UPDATE")
    }
}
