import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// Comprehensive tests for MSSQL database property operations.
/// Covers: fetch properties, fetch files, alter options (recovery, state, ANSI, automatic, isolation,
/// miscellaneous), and file modification (resize, max size, growth).
///
/// Requires a live SQL Server instance configured via environment variables:
///   TDS_HOSTNAME, TDS_PORT, TDS_USERNAME, TDS_PASSWORD, TDS_DATABASE
final class DatabasePropertiesTests: XCTestCase {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    private var testDatabase: String!
    private var adminClient: SQLServerAdministrationClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            skipDueToEnv = true
            return
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "dbprops")
        self.adminClient = SQLServerAdministrationClient(client: baseClient)
    }

    override func tearDown() async throws {
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully().get()
        try? await group?.shutdownGracefully()
        testDatabase = nil
        group = nil
    }

    // MARK: - Fetch Properties

    func testFetchDatabaseProperties() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        XCTAssertEqual(props.name, testDatabase)
        XCTAssertFalse(props.owner.isEmpty, "Owner should not be empty")
        XCTAssertEqual(props.stateDescription, "ONLINE")
        XCTAssertFalse(props.createDate.isEmpty, "Create date should not be empty")
        XCTAssertGreaterThan(props.sizeMB, 0, "Size should be greater than 0")
        XCTAssertFalse(props.collationName.isEmpty, "Collation should not be empty")
        XCTAssertGreaterThan(props.compatibilityLevel, 0, "Compatibility level should be positive")
    }

    func testFetchDatabasePropertiesRecoveryDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        // New databases default to FULL recovery model
        XCTAssertEqual(props.recoveryModel, "FULL")
        XCTAssertFalse(props.isReadOnly)
        XCTAssertEqual(props.userAccessDescription, "MULTI_USER")
        XCTAssertFalse(props.isEncrypted)
    }

    func testFetchDatabasePropertiesAutoDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        XCTAssertTrue(props.isAutoCreateStatsOn, "Auto create statistics should be on by default")
        XCTAssertTrue(props.isAutoUpdateStatsOn, "Auto update statistics should be on by default")
        XCTAssertFalse(props.isAutoUpdateStatsAsyncOn, "Auto update stats async should be off by default")
        XCTAssertFalse(props.isAutoCloseOn, "Auto close should be off by default")
        XCTAssertFalse(props.isAutoShrinkOn, "Auto shrink should be off by default")
    }

    func testFetchDatabasePropertiesAnsiDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        // Default ANSI settings for a new database
        XCTAssertFalse(props.isAnsiNullDefaultOn)
        XCTAssertFalse(props.isAnsiNullsOn)
        XCTAssertFalse(props.isAnsiPaddingOn)
        XCTAssertFalse(props.isAnsiWarningsOn)
        XCTAssertFalse(props.isArithAbortOn)
        XCTAssertTrue(props.isConcatNullYieldsNullOn)
        XCTAssertTrue(props.isQuotedIdentifierOn)
        XCTAssertFalse(props.isRecursiveTriggersOn)
        XCTAssertFalse(props.isNumericRoundAbortOn)
        XCTAssertFalse(props.isDateCorrelationOn)
    }

    func testFetchDatabasePropertiesIsolationDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        XCTAssertFalse(props.isReadCommittedSnapshotOn)
        // Snapshot isolation state should contain OFF
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("OFF"),
                       "Snapshot isolation should be off by default, got: \(props.snapshotIsolationState)")
    }

    func testFetchDatabasePropertiesMiscDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        XCTAssertFalse(props.isBrokerEnabled)
        XCTAssertFalse(props.isTrustworthy)
        XCTAssertFalse(props.isParameterizationForced)
    }

    func testFetchNonExistentDatabaseThrows() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        do {
            _ = try await adminClient.fetchDatabaseProperties(name: "nonexistent_db_\(UUID().uuidString.prefix(8))")
            XCTFail("Should have thrown for non-existent database")
        } catch {
            // Expected
        }
    }

    // MARK: - Fetch Files

    func testFetchDatabaseFiles() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)

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
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
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

    // MARK: - Recovery Model

    func testAlterRecoveryModelSimple() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "SIMPLE")
    }

    func testAlterRecoveryModelBulkLogged() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.bulkLogged))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "BULK_LOGGED")
    }

    func testAlterRecoveryModelFull() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Switch to simple first, then back to full
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.full))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "FULL")
    }

    // MARK: - Compatibility Level

    func testAlterCompatibilityLevel() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Get current level first
        let originalProps = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        let originalLevel = originalProps.compatibilityLevel

        // Set to 140 (SQL Server 2017)
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .compatibilityLevel(140))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.compatibilityLevel, 140)

        // Restore
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .compatibilityLevel(originalLevel))
    }

    // MARK: - State Options

    func testAlterPageVerifyChecksum() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.checksum))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "CHECKSUM")
    }

    func testAlterPageVerifyTornPage() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.tornPageDetection))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "TORN_PAGE_DETECTION")
    }

    func testAlterPageVerifyNone() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.none))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "NONE")
    }

    func testAlterTargetRecoveryTime() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .targetRecoveryTime(120))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.targetRecoveryTimeSeconds, 120)
    }

    func testAlterDelayedDurabilityAllowed() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.allowed))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "ALLOWED")
    }

    func testAlterDelayedDurabilityForced() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.forced))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "FORCED")
    }

    func testAlterDelayedDurabilityDisabled() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.forced))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.disabled))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "DISABLED")
    }

    // MARK: - Automatic Options

    func testAlterAutoClose() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoCloseOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoCloseOn)
    }

    func testAlterAutoShrink() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoShrink(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoShrinkOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoShrink(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoShrinkOn)
    }

    func testAlterAutoCreateStatistics() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoCreateStatistics(false))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoCreateStatsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoCreateStatistics(true))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoCreateStatsOn)
    }

    func testAlterAutoUpdateStatistics() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(false))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoUpdateStatsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(true))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoUpdateStatsOn)
    }

    func testAlterAutoUpdateStatisticsAsync() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Must enable auto update stats first for async to work
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(true))

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatisticsAsync(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoUpdateStatsAsyncOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatisticsAsync(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoUpdateStatsAsyncOn)
    }

    // MARK: - ANSI Options

    func testAlterAnsiNullDefault() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiNullDefaultOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiNullDefaultOn)
    }

    func testAlterAnsiNulls() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNulls(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiNullsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNulls(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiNullsOn)
    }

    func testAlterAnsiPadding() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiPadding(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiPaddingOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiPadding(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiPaddingOn)
    }

    func testAlterAnsiWarnings() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiWarnings(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiWarningsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiWarnings(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiWarningsOn)
    }

    func testAlterArithAbort() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .arithAbort(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isArithAbortOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .arithAbort(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isArithAbortOn)
    }

    func testAlterConcatNullYieldsNull() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .concatNullYieldsNull(false))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isConcatNullYieldsNullOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .concatNullYieldsNull(true))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isConcatNullYieldsNullOn)
    }

    func testAlterQuotedIdentifier() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .quotedIdentifier(false))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isQuotedIdentifierOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .quotedIdentifier(true))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isQuotedIdentifierOn)
    }

    func testAlterRecursiveTriggers() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recursiveTriggers(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isRecursiveTriggersOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recursiveTriggers(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isRecursiveTriggersOn)
    }

    func testAlterNumericRoundAbort() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .numericRoundAbort(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isNumericRoundAbortOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .numericRoundAbort(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isNumericRoundAbortOn)
    }

    func testAlterDateCorrelationOptimization() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .dateCorrelationOptimization(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isDateCorrelationOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .dateCorrelationOptimization(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isDateCorrelationOn)
    }

    // MARK: - Isolation Options

    func testAlterAllowSnapshotIsolation() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .allowSnapshotIsolation(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("ON"),
                       "Expected ON, got: \(props.snapshotIsolationState)")

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .allowSnapshotIsolation(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("OFF"),
                       "Expected OFF, got: \(props.snapshotIsolationState)")
    }

    func testAlterReadCommittedSnapshot() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .readCommittedSnapshot(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isReadCommittedSnapshotOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .readCommittedSnapshot(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isReadCommittedSnapshotOn)
    }

    // MARK: - Miscellaneous Options

    func testAlterParameterizationForced() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.forced))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isParameterizationForced)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.simple))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isParameterizationForced)
    }

    func testAlterTrustworthy() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .trustworthy(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isTrustworthy)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .trustworthy(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isTrustworthy)
    }

    func testAlterBrokerEnabled() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .brokerEnabled(true))
        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isBrokerEnabled)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .brokerEnabled(false))
        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isBrokerEnabled)
    }

    // MARK: - User Access

    func testAlterUserAccessRestrictedUser() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .userAccess(.restrictedUser))
        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.userAccessDescription, "RESTRICTED_USER")

        // Restore to multi user
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .userAccess(.multiUser))
    }

    // MARK: - File Modification

    func testModifyFileSize() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        // Grow the file to a larger size
        let newSizeMB = Int(dataFile.sizeMB) + 8
        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .sizeMB(newSizeMB)
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertGreaterThanOrEqual(Int(updatedFile!.sizeMB), newSizeMB,
                                     "File should have been resized to at least \(newSizeMB) MB")
    }

    func testModifyFileMaxSizeUnlimited() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .maxSizeUnlimited
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertTrue(updatedFile!.isMaxSizeUnlimited, "Max size should be unlimited")
        XCTAssertEqual(updatedFile!.maxSizeRaw, -1)
    }

    func testModifyFileMaxSizeMB() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .maxSizeMB(512)
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertFalse(updatedFile!.isMaxSizeUnlimited, "Max size should not be unlimited")
        // Max size in MB should be approximately 512
        if let maxMB = updatedFile!.maxSizeMB {
            XCTAssertEqual(maxMB, 512, "Max size should be 512 MB")
        } else {
            XCTFail("Max size MB should not be nil")
        }
    }

    func testModifyFileGrowthMB() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthMB(128)
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertFalse(updatedFile!.isPercentGrowth)
        XCTAssertEqual(updatedFile!.growthMB, 128, "Growth should be 128 MB")
    }

    func testModifyFileGrowthPercent() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthPercent(25)
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertTrue(updatedFile!.isPercentGrowth)
        XCTAssertEqual(updatedFile!.growthPercent, 25, "Growth should be 25%")
    }

    func testModifyFileGrowthNone() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthNone
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertEqual(updatedFile!.growthRaw, 0, "Growth should be 0 (disabled)")
    }

    func testModifyLogFileGrowth() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let logFile = files.first(where: { $0.typeDescription == "LOG" }) else {
            XCTFail("No log file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: logFile.name,
            option: .filegrowthMB(64)
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == logFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertEqual(updatedFile!.growthMB, 64, "Log file growth should be 64 MB")
    }

    // MARK: - Add / Remove Files

    func testAddAndRemoveDataFile() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Get the data directory from existing file
        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let existingFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        let dataDir = (existingFile.physicalName as NSString).deletingLastPathComponent
        let newFileName = "\(testDatabase!)_extra.ndf"
        let newFilePath = (dataDir as NSString).appendingPathComponent(newFileName)
        let logicalName = "\(testDatabase!)_extra"

        // Add a new data file
        try await adminClient.addDatabaseFile(
            databaseName: testDatabase,
            logicalName: logicalName,
            fileName: newFilePath,
            sizeMB: 8,
            filegrowthMB: 8
        )

        var updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let addedFile = updatedFiles.first(where: { $0.name == logicalName })
        XCTAssertNotNil(addedFile, "New data file should appear in file list")
        XCTAssertEqual(addedFile?.typeDescription, "ROWS")

        // Empty the file before removing (shrink to 0)
        try await adminClient.shrinkDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: logicalName,
            targetSizeMB: 0
        )

        // Remove the file
        try await adminClient.removeDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: logicalName
        )

        updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let removedFile = updatedFiles.first(where: { $0.name == logicalName })
        XCTAssertNil(removedFile, "Removed file should no longer appear")
    }

    func testAddLogFile() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        let files = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        guard let existingLog = files.first(where: { $0.typeDescription == "LOG" }) else {
            XCTFail("No log file found")
            return
        }

        let logDir = (existingLog.physicalName as NSString).deletingLastPathComponent
        let newFileName = "\(testDatabase!)_extra_log.ldf"
        let newFilePath = (logDir as NSString).appendingPathComponent(newFileName)
        let logicalName = "\(testDatabase!)_extra_log"

        try await adminClient.addDatabaseLogFile(
            databaseName: testDatabase,
            logicalName: logicalName,
            fileName: newFilePath,
            sizeMB: 8,
            filegrowthMB: 8
        )

        let updatedFiles = try await adminClient.fetchDatabaseFiles(name: testDatabase)
        let addedFile = updatedFiles.first(where: { $0.name == logicalName })
        XCTAssertNotNil(addedFile, "New log file should appear")
        XCTAssertEqual(addedFile?.typeDescription, "LOG")
    }

    // MARK: - Database Lifecycle

    func testTakeDatabaseOfflineAndOnline() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        try await adminClient.takeDatabaseOffline(name: testDatabase)

        var props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.stateDescription, "OFFLINE")

        try await adminClient.bringDatabaseOnline(name: testDatabase)

        props = try await adminClient.fetchDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.stateDescription, "ONLINE")
    }

    func testShrinkDatabase() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Shrink should execute without error
        let messages = try await adminClient.shrinkDatabase(name: testDatabase)
        // Just verify it didn't throw -- messages may or may not be empty
        _ = messages
    }

    // MARK: - Multiple Options in Sequence

    func testApplyMultipleOptionsSequentially() async throws {
        if skipDueToEnv { throw XCTSkip("Server unavailable") }

        // Apply a batch of options
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(true))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(true))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.forced))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .targetRecoveryTime(30))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.tornPageDetection))

        let props = try await adminClient.fetchDatabaseProperties(name: testDatabase)

        XCTAssertEqual(props.recoveryModel, "SIMPLE")
        XCTAssertTrue(props.isAutoCloseOn)
        XCTAssertTrue(props.isAnsiNullDefaultOn)
        XCTAssertTrue(props.isParameterizationForced)
        XCTAssertEqual(props.targetRecoveryTimeSeconds, 30)
        XCTAssertEqual(props.pageVerifyOption, "TORN_PAGE_DETECTION")
    }

}

// MARK: - SQLServerDatabaseFile Computed Property Tests (no server required)

final class DatabaseFileComputedPropertyTests: XCTestCase {

    func testFileComputedPropertiesUnlimited() {
        let file = SQLServerDatabaseFile(
            name: "test",
            typeDescription: "ROWS",
            physicalName: "/test.mdf",
            sizeMB: 100,
            maxSizeDescription: "Unlimited",
            growthDescription: "64 MB",
            fileGroupName: "PRIMARY",
            sizePages: 12800,
            maxSizeRaw: -1,
            growthRaw: 8192,
            isPercentGrowth: false,
            type: 0
        )

        XCTAssertTrue(file.isMaxSizeUnlimited)
        XCTAssertNil(file.maxSizeMB)
        XCTAssertEqual(file.growthMB, 64) // 8192 * 8 / 1024 = 64
        XCTAssertNil(file.growthPercent)
        XCTAssertFalse(file.isNoGrowth)
    }

    func testFileComputedPropertiesPercentGrowth() {
        let file = SQLServerDatabaseFile(
            name: "test",
            typeDescription: "ROWS",
            physicalName: "/test.mdf",
            sizeMB: 100,
            maxSizeDescription: "500 MB",
            growthDescription: "10%",
            fileGroupName: "PRIMARY",
            sizePages: 12800,
            maxSizeRaw: 64000,
            growthRaw: 10,
            isPercentGrowth: true,
            type: 0
        )

        XCTAssertFalse(file.isMaxSizeUnlimited)
        XCTAssertEqual(file.maxSizeMB, 500) // 64000 * 8 / 1024 = 500
        XCTAssertNil(file.growthMB)
        XCTAssertEqual(file.growthPercent, 10)
    }

    func testFileComputedPropertiesNoGrowth() {
        let file = SQLServerDatabaseFile(
            name: "test",
            typeDescription: "ROWS",
            physicalName: "/test.mdf",
            sizeMB: 100,
            maxSizeDescription: "No Growth",
            growthDescription: "None",
            fileGroupName: "PRIMARY",
            sizePages: 12800,
            maxSizeRaw: 0,
            growthRaw: 0,
            isPercentGrowth: false,
            type: 0
        )

        XCTAssertFalse(file.isMaxSizeUnlimited)
        XCTAssertTrue(file.isNoGrowth)
        XCTAssertEqual(file.growthMB, 0)
    }

    func testFileComputedPropertiesDefaultInit() {
        let file = SQLServerDatabaseFile(
            name: "test",
            typeDescription: "ROWS",
            physicalName: "/test.mdf",
            sizeMB: 0,
            maxSizeDescription: "",
            growthDescription: "",
            fileGroupName: nil
        )

        // Default values
        XCTAssertEqual(file.sizePages, 0)
        XCTAssertTrue(file.isMaxSizeUnlimited, "Default maxSizeRaw is -1 (unlimited)")
        XCTAssertEqual(file.growthRaw, 0)
        XCTAssertFalse(file.isPercentGrowth)
        XCTAssertEqual(file.type, 0)
    }

    func testFileTypeValues() {
        let dataFile = SQLServerDatabaseFile(
            name: "data", typeDescription: "ROWS", physicalName: "/d.mdf",
            sizeMB: 10, maxSizeDescription: "", growthDescription: "",
            fileGroupName: nil, type: 0
        )
        XCTAssertEqual(dataFile.type, 0)

        let logFile = SQLServerDatabaseFile(
            name: "log", typeDescription: "LOG", physicalName: "/l.ldf",
            sizeMB: 10, maxSizeDescription: "", growthDescription: "",
            fileGroupName: nil, type: 1
        )
        XCTAssertEqual(logFile.type, 1)
    }
}
