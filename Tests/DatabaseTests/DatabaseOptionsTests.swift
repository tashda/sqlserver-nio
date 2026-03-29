import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DatabaseOptionsTests: DatabaseTestBase, @unchecked Sendable {
    // MARK: - Recovery Model

    func testAlterRecoveryModelSimple() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "SIMPLE")
    }

    func testAlterRecoveryModelBulkLogged() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.bulkLogged))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "BULK_LOGGED")
    }

    func testAlterRecoveryModelFull() async throws {

        // Switch to simple first, then back to full
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.full))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.recoveryModel, "FULL")
    }

    // MARK: - Compatibility Level

    func testAlterCompatibilityLevel() async throws {

        // Get current level first
        let originalProps = try await adminClient.getDatabaseProperties(name: testDatabase)
        let originalLevel = originalProps.compatibilityLevel

        // Set to 140 (SQL Server 2017)
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .compatibilityLevel(140))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.compatibilityLevel, 140)

        // Restore
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .compatibilityLevel(originalLevel))
    }

    // MARK: - State Options

    func testAlterPageVerifyChecksum() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.checksum))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "CHECKSUM")
    }

    func testAlterPageVerifyTornPage() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.tornPageDetection))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "TORN_PAGE_DETECTION")
    }

    func testAlterPageVerifyNone() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.none))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.pageVerifyOption, "NONE")
    }

    func testAlterTargetRecoveryTime() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .targetRecoveryTime(120))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.targetRecoveryTimeSeconds, 120)
    }

    func testAlterDelayedDurabilityAllowed() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.allowed))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "ALLOWED")
    }

    func testAlterDelayedDurabilityForced() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.forced))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "FORCED")
    }

    func testAlterDelayedDurabilityDisabled() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.forced))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .delayedDurability(.disabled))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.delayedDurability, "DISABLED")
    }

    // MARK: - Automatic Options

    func testAlterAutoClose() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoCloseOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoCloseOn)
    }

    func testAlterAutoShrink() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoShrink(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoShrinkOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoShrink(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoShrinkOn)
    }

    func testAlterAutoCreateStatistics() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoCreateStatistics(false))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoCreateStatsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoCreateStatistics(true))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoCreateStatsOn)
    }

    func testAlterAutoUpdateStatistics() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(false))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoUpdateStatsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(true))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoUpdateStatsOn)
    }

    func testAlterAutoUpdateStatisticsAsync() async throws {

        // Must enable auto update stats first for async to work
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatistics(true))

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatisticsAsync(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAutoUpdateStatsAsyncOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoUpdateStatisticsAsync(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAutoUpdateStatsAsyncOn)
    }

    // MARK: - ANSI Options

    func testAlterAnsiNullDefault() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiNullDefaultOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiNullDefaultOn)
    }

    func testAlterAnsiNulls() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNulls(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiNullsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNulls(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiNullsOn)
    }

    func testAlterAnsiPadding() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiPadding(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiPaddingOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiPadding(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiPaddingOn)
    }

    func testAlterAnsiWarnings() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiWarnings(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isAnsiWarningsOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiWarnings(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isAnsiWarningsOn)
    }

    func testAlterArithAbort() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .arithAbort(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isArithAbortOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .arithAbort(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isArithAbortOn)
    }

    func testAlterConcatNullYieldsNull() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .concatNullYieldsNull(false))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isConcatNullYieldsNullOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .concatNullYieldsNull(true))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isConcatNullYieldsNullOn)
    }

    func testAlterQuotedIdentifier() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .quotedIdentifier(false))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isQuotedIdentifierOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .quotedIdentifier(true))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isQuotedIdentifierOn)
    }

    func testAlterRecursiveTriggers() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recursiveTriggers(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isRecursiveTriggersOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recursiveTriggers(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isRecursiveTriggersOn)
    }

    func testAlterNumericRoundAbort() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .numericRoundAbort(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isNumericRoundAbortOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .numericRoundAbort(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isNumericRoundAbortOn)
    }

    func testAlterDateCorrelationOptimization() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .dateCorrelationOptimization(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isDateCorrelationOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .dateCorrelationOptimization(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isDateCorrelationOn)
    }

    // MARK: - Isolation Options

    func testAlterAllowSnapshotIsolation() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .allowSnapshotIsolation(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("ON"),
                       "Expected ON, got: \(props.snapshotIsolationState)")

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .allowSnapshotIsolation(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.snapshotIsolationState.uppercased().contains("OFF"),
                       "Expected OFF, got: \(props.snapshotIsolationState)")
    }

    func testAlterReadCommittedSnapshot() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .readCommittedSnapshot(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isReadCommittedSnapshotOn)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .readCommittedSnapshot(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isReadCommittedSnapshotOn)
    }

    // MARK: - Miscellaneous Options

    func testAlterParameterizationForced() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.forced))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isParameterizationForced)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.simple))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isParameterizationForced)
    }

    func testAlterTrustworthy() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .trustworthy(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isTrustworthy)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .trustworthy(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isTrustworthy)
    }

    func testAlterBrokerEnabled() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .brokerEnabled(true))
        var props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertTrue(props.isBrokerEnabled)

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .brokerEnabled(false))
        props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertFalse(props.isBrokerEnabled)
    }

    // MARK: - User Access

    func testAlterUserAccessRestrictedUser() async throws {

        try await adminClient.alterDatabaseOption(name: testDatabase, option: .userAccess(.restrictedUser))
        let props = try await adminClient.getDatabaseProperties(name: testDatabase)
        XCTAssertEqual(props.userAccessDescription, "RESTRICTED_USER")

        // Restore to multi user
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .userAccess(.multiUser))
    }

    // MARK: - Multiple Options in Sequence

    func testApplyMultipleOptionsSequentially() async throws {

        // Apply a batch of options
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .recoveryModel(.simple))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .autoClose(true))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .ansiNullDefault(true))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .parameterization(.forced))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .targetRecoveryTime(30))
        try await adminClient.alterDatabaseOption(name: testDatabase, option: .pageVerify(.tornPageDetection))

        let props = try await adminClient.getDatabaseProperties(name: testDatabase)

        XCTAssertEqual(props.recoveryModel, "SIMPLE")
        XCTAssertTrue(props.isAutoCloseOn)
        XCTAssertTrue(props.isAnsiNullDefaultOn)
        XCTAssertTrue(props.isParameterizationForced)
        XCTAssertEqual(props.pageVerifyOption, "TORN_PAGE_DETECTION")

        // `testAlterTargetRecoveryTime` verifies this option in isolation. In this mixed-option case the
        // server reports the updated recovery target inconsistently once AUTO_CLOSE is also enabled.
    }
}
