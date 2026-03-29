import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DatabasePropertiesTests: DatabaseTestBase, @unchecked Sendable {
    // MARK: - Fetch Properties

    func testFetchDatabaseProperties() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        XCTAssertEqual(props.name, testDatabase)
        XCTAssertFalse(props.owner.isEmpty, "Owner should not be empty")
        XCTAssertEqual(props.stateDescription, "ONLINE")
        XCTAssertFalse(props.createDate.isEmpty, "Create date should not be empty")
        XCTAssertGreaterThan(props.sizeMB, 0, "Size should be greater than 0")
        XCTAssertFalse(props.collationName.isEmpty, "Collation should not be empty")
        XCTAssertGreaterThan(props.compatibilityLevel, 0, "Compatibility level should be positive")
    }

    func testFetchDatabasePropertiesRecoveryDefaults() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        // New databases default to FULL recovery model
        XCTAssertEqual(props.recoveryModel, "FULL")
        XCTAssertFalse(props.isReadOnly)
        XCTAssertEqual(props.userAccessDescription, "MULTI_USER")
        XCTAssertFalse(props.isEncrypted)
    }

    func testFetchDatabasePropertiesAutoDefaults() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        XCTAssertTrue(props.isAutoCreateStatsOn, "Auto create statistics should be on by default")
        XCTAssertTrue(props.isAutoUpdateStatsOn, "Auto update statistics should be on by default")
        XCTAssertFalse(props.isAutoUpdateStatsAsyncOn, "Auto update stats async should be off by default")
        XCTAssertFalse(props.isAutoCloseOn, "Auto close should be off by default")
        XCTAssertFalse(props.isAutoShrinkOn, "Auto shrink should be off by default")
    }

    func testFetchDatabasePropertiesAnsiDefaults() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        // Default ANSI settings for a new database
        XCTAssertFalse(props.isAnsiNullDefaultOn)
        XCTAssertFalse(props.isAnsiNullsOn)
        XCTAssertFalse(props.isAnsiPaddingOn)
        XCTAssertFalse(props.isAnsiWarningsOn)
        XCTAssertFalse(props.isArithAbortOn)
        XCTAssertFalse(props.isConcatNullYieldsNullOn)
        XCTAssertFalse(props.isQuotedIdentifierOn)
        XCTAssertFalse(props.isRecursiveTriggersOn)
        XCTAssertFalse(props.isNumericRoundAbortOn)
        XCTAssertFalse(props.isDateCorrelationOn)
    }

    func testFetchDatabasePropertiesIsolationDefaults() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        XCTAssertFalse(props.isReadCommittedSnapshotOn)
        // Snapshot isolation state should contain OFF
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("OFF"),
                       "Snapshot isolation should be off by default, got: \(props.snapshotIsolationState)")
    }

    func testFetchDatabasePropertiesMiscDefaults() async throws {

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        XCTAssertTrue(props.isBrokerEnabled)
        XCTAssertFalse(props.isTrustworthy)
        XCTAssertFalse(props.isParameterizationForced)
    }

    func testFetchNonExistentDatabaseThrows() async throws {

        do {
            _ = try await adminClient.getDatabaseProperties(name: "nonexistent_db_\(UUID().uuidString.prefix(8))")
            XCTFail("Should have thrown for non-existent database")
        } catch {
            // Expected
        }
    }

    // MARK: - Fetch Files

    func testFetchDatabaseFiles() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)

        XCTAssertGreaterThanOrEqual(files.count, 2, "Should have at least a data file and a log file")

        // Check data file
        let dataFiles = files.filter { $0.typeDescription == "ROWS" }
        XCTAssertGreaterThanOrEqual(dataFiles.count, 1, "Should have at least one data file")

        if let dataFile = dataFiles.first {
            XCTAssertFalse(dataFile.name.isEmpty)
            XCTAssertFalse(dataFile.physicalName.isEmpty)
            XCTAssertGreaterThan(dataFile.sizeMB, 0)
            XCTAssertGreaterThan(dataFile.sizePages, 0)
            XCTAssertEqual(dataFile.type, 0, "Data file type should be 0")
        }

        // Check log file
        let logFiles = files.filter { $0.typeDescription == "LOG" }
        XCTAssertGreaterThanOrEqual(logFiles.count, 1, "Should have at least one log file")

        if let logFile = logFiles.first {
            XCTAssertFalse(logFile.name.isEmpty)
            XCTAssertFalse(logFile.physicalName.isEmpty)
            XCTAssertGreaterThan(logFile.sizeMB, 0)
            XCTAssertEqual(logFile.type, 1, "Log file type should be 1")
        }
    }

    func testFetchDatabaseFileRawMetadata() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let file = files.first else {
            XCTFail("No files returned")
            return
        }

        // Raw metadata should be populated
        XCTAssertGreaterThan(file.sizePages, 0, "Size pages should be positive")

        // Computed properties should work
        if file.isPercentGrowth {
            XCTAssertNotNil(file.growthPercent, "Percent growth file should have growthPercent")
            XCTAssertNil(file.growthMB, "Percent growth file should not have growthMB")
        } else {
            XCTAssertNotNil(file.growthMB, "MB growth file should have growthMB")
            XCTAssertNil(file.growthPercent, "MB growth file should not have growthPercent")
        }
    }
    
    // MARK: - Database Lifecycle

    func testTakeDatabaseOfflineAndOnline() async throws {

        try await adminClient.takeDatabaseOffline(name: testDatabase)

        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.stateDescription, "OFFLINE")

        try await adminClient.bringDatabaseOnline(name: testDatabase)

        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.stateDescription, "ONLINE")
    }

    func testShrinkDatabase() async throws {

        // Shrink should execute without error
        let messages = try await adminClient.shrinkDatabase(name: testDatabase)
        // Just verify it didn't throw -- messages may or may not be empty
        _ = messages
    }
}
