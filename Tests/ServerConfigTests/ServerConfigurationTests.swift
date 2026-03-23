import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class ServerConfigurationTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        _ = try await withTimeout(10) { try await self.client.query("SELECT 1") }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    // MARK: - Server Info (General Page)

    func testFetchServerInfoReturnsValidProperties() async throws {
        let info = try await client.serverConfig.fetchServerInfo()

        XCTAssertFalse(info.serverName.isEmpty, "Server name should not be empty")
        XCTAssertFalse(info.edition.isEmpty, "Edition should not be empty")
        XCTAssertFalse(info.productVersion.isEmpty, "Product version should not be empty")
        XCTAssertFalse(info.productLevel.isEmpty, "Product level should not be empty")
        XCTAssertFalse(info.collation.isEmpty, "Collation should not be empty")
        XCTAssertFalse(info.machineName.isEmpty, "Machine name should not be empty")
        XCTAssertGreaterThan(info.engineEdition, 0, "Engine edition should be positive")
        XCTAssertGreaterThan(info.processID, 0, "Process ID should be positive")
    }

    func testServerInfoVersionFormat() async throws {
        let info = try await client.serverConfig.fetchServerInfo()

        // Version should be like "16.0.1000.6" (major.minor.build.revision)
        let parts = info.productVersion.split(separator: ".")
        XCTAssertGreaterThanOrEqual(parts.count, 2, "Version should have at least major.minor components")

        if let majorVersion = Int(parts[0]) {
            XCTAssertGreaterThanOrEqual(majorVersion, 11, "Major version should be at least 11 (SQL Server 2012+)")
        } else {
            XCTFail("Major version should be an integer")
        }
    }

    func testServerInfoDefaultPaths() async throws {
        let info = try await client.serverConfig.fetchServerInfo()

        XCTAssertFalse(info.instanceDefaultDataPath.isEmpty, "Default data path should not be empty")
        XCTAssertFalse(info.instanceDefaultLogPath.isEmpty, "Default log path should not be empty")
    }

    // MARK: - System Info

    func testFetchSystemInfoReturnsValidHardwareDetails() async throws {
        let sysInfo = try await client.serverConfig.fetchSystemInfo()

        XCTAssertGreaterThan(sysInfo.cpuCount, 0, "CPU count should be positive")
        XCTAssertGreaterThan(sysInfo.socketCount, 0, "Socket count should be positive")
        XCTAssertGreaterThan(sysInfo.coresPerSocket, 0, "Cores per socket should be positive")
        XCTAssertGreaterThan(sysInfo.numaNodeCount, 0, "NUMA node count should be at least 1")
        XCTAssertGreaterThan(sysInfo.physicalMemoryMB, 0, "Physical memory should be positive")
        XCTAssertGreaterThan(sysInfo.maxWorkersCount, 0, "Max workers count should be positive")
        XCTAssertFalse(sysInfo.sqlServerStartTime.isEmpty, "Start time should not be empty")
    }

    // MARK: - Configuration Options

    func testListConfigurationsReturnsStandardOptions() async throws {
        let configs = try await client.serverConfig.listConfigurations()

        XCTAssertGreaterThan(configs.count, 30, "Should have at least 30 configuration options, got \(configs.count)")

        let names = Set(configs.map(\.name))
        XCTAssertTrue(names.contains(SQLServerConfigurationName.maxServerMemory))
        XCTAssertTrue(names.contains(SQLServerConfigurationName.maxDegreeOfParallelism))
        XCTAssertTrue(names.contains(SQLServerConfigurationName.costThresholdForParallelism))
        XCTAssertTrue(names.contains(SQLServerConfigurationName.backupCompressionDefault))
    }

    func testConfigurationOptionsHaveValidRanges() async throws {
        let configs = try await client.serverConfig.listConfigurations()

        for config in configs {
            XCTAssertLessThanOrEqual(
                config.minimum, config.maximum,
                "Option '\(config.name)' has min (\(config.minimum)) > max (\(config.maximum))"
            )
            XCTAssertGreaterThanOrEqual(
                config.runningValue, config.minimum,
                "Option '\(config.name)' running value (\(config.runningValue)) below minimum (\(config.minimum))"
            )
            XCTAssertLessThanOrEqual(
                config.runningValue, config.maximum,
                "Option '\(config.name)' running value (\(config.runningValue)) above maximum (\(config.maximum))"
            )
        }
    }

    func testGetSingleConfigurationOption() async throws {
        let option = try await client.serverConfig.getConfiguration(name: SQLServerConfigurationName.maxDegreeOfParallelism)

        XCTAssertEqual(option.name, SQLServerConfigurationName.maxDegreeOfParallelism)
        XCTAssertEqual(option.minimum, 0)
        XCTAssertEqual(option.maximum, 32767)
        XCTAssertFalse(option.description.isEmpty, "Description should not be empty")
        XCTAssertTrue(option.isAdvanced, "MAXDOP should be an advanced option")
        XCTAssertTrue(option.isDynamic, "MAXDOP should be a dynamic option")
    }

    func testGetNonExistentConfigurationThrows() async throws {
        do {
            _ = try await client.serverConfig.getConfiguration(name: "this_option_does_not_exist_12345")
            XCTFail("Should have thrown for non-existent option")
        } catch {
            // Expected
        }
    }

    func testSetAndRestoreConfigurationOption() async throws {
        let optionName = SQLServerConfigurationName.costThresholdForParallelism

        // Read original value
        let original = try await client.serverConfig.getConfiguration(name: optionName)
        let originalValue = original.runningValue

        // Set to a different value
        let newValue: Int64 = originalValue == 10 ? 15 : 10
        _ = try await client.serverConfig.setConfiguration(name: optionName, value: newValue)

        // Verify the change took effect (it's a dynamic option)
        let updated = try await client.serverConfig.getConfiguration(name: optionName)
        XCTAssertEqual(updated.runningValue, newValue, "Running value should be updated to \(newValue)")

        // Restore original value
        _ = try await client.serverConfig.setConfiguration(name: optionName, value: originalValue)

        let restored = try await client.serverConfig.getConfiguration(name: optionName)
        XCTAssertEqual(restored.runningValue, originalValue, "Value should be restored")
    }

    func testSetMultipleConfigurations() async throws {
        // Read originals
        let costOrig = try await client.serverConfig.getConfiguration(name: SQLServerConfigurationName.costThresholdForParallelism)
        let maxdopOrig = try await client.serverConfig.getConfiguration(name: SQLServerConfigurationName.maxDegreeOfParallelism)

        // Set both to different values
        let costNew: Int64 = costOrig.runningValue == 10 ? 15 : 10
        let maxdopNew: Int64 = maxdopOrig.runningValue == 2 ? 4 : 2

        _ = try await client.serverConfig.setConfigurations([
            (name: SQLServerConfigurationName.costThresholdForParallelism, value: costNew),
            (name: SQLServerConfigurationName.maxDegreeOfParallelism, value: maxdopNew)
        ])

        // Verify both changed
        let costUpdated = try await client.serverConfig.getConfiguration(name: SQLServerConfigurationName.costThresholdForParallelism)
        let maxdopUpdated = try await client.serverConfig.getConfiguration(name: SQLServerConfigurationName.maxDegreeOfParallelism)

        XCTAssertEqual(costUpdated.runningValue, costNew)
        XCTAssertEqual(maxdopUpdated.runningValue, maxdopNew)

        // Restore
        _ = try await client.serverConfig.setConfigurations([
            (name: SQLServerConfigurationName.costThresholdForParallelism, value: costOrig.runningValue),
            (name: SQLServerConfigurationName.maxDegreeOfParallelism, value: maxdopOrig.runningValue)
        ])
    }

    // MARK: - Security Settings

    func testFetchSecuritySettingsReturnsValidMode() async throws {
        let settings = try await client.serverConfig.fetchSecuritySettings()

        // Docker SQL Server instances typically run in mixed mode
        XCTAssertTrue(
            settings.authenticationMode == .mixed || settings.authenticationMode == .windowsOnly,
            "Authentication mode should be a valid value, got \(settings.authenticationMode)"
        )
    }

    // MARK: - User Options Bitmask

    func testUserOptionsBitmaskValues() {
        var options: SQLServerUserOptions = [.ansiNulls, .ansiWarnings, .quotedIdentifier]

        XCTAssertTrue(options.contains(.ansiNulls))
        XCTAssertTrue(options.contains(.ansiWarnings))
        XCTAssertTrue(options.contains(.quotedIdentifier))
        XCTAssertFalse(options.contains(.implicitTransactions))

        // Expected bitmask: 32 + 8 + 256 = 296
        XCTAssertEqual(options.rawValue, 296)

        options.insert(.xactAbort)
        XCTAssertEqual(options.rawValue, 296 + 16384)
    }

    // MARK: - Configuration Name Constants

    func testWellKnownConfigurationNamesExist() async throws {
        let configs = try await client.serverConfig.listConfigurations()
        let names = Set(configs.map(\.name))

        let expectedNames = [
            SQLServerConfigurationName.minServerMemory,
            SQLServerConfigurationName.maxServerMemory,
            SQLServerConfigurationName.maxWorkerThreads,
            SQLServerConfigurationName.userConnections,
            SQLServerConfigurationName.userOptions,
            SQLServerConfigurationName.remoteAccess,
            SQLServerConfigurationName.remoteQueryTimeout,
            SQLServerConfigurationName.fillFactor,
            SQLServerConfigurationName.backupCompressionDefault,
            SQLServerConfigurationName.nestedTriggers,
            SQLServerConfigurationName.networkPacketSize,
            SQLServerConfigurationName.costThresholdForParallelism,
            SQLServerConfigurationName.maxDegreeOfParallelism,
            SQLServerConfigurationName.defaultLanguage,
            SQLServerConfigurationName.twoDigitYearCutoff,
            SQLServerConfigurationName.showAdvancedOptions
        ]

        for name in expectedNames {
            XCTAssertTrue(names.contains(name), "Configuration '\(name)' should exist in sys.configurations")
        }
    }

    // MARK: - isPendingRestart

    func testIsPendingRestartComputed() {
        let option = SQLServerConfigurationOption(
            configurationID: 1,
            name: "test",
            minimum: 0,
            maximum: 100,
            configuredValue: 50,
            runningValue: 30,
            description: "test option",
            isDynamic: false,
            isAdvanced: false
        )
        XCTAssertTrue(option.isPendingRestart, "Should be pending when configured != running")

        let nonPending = SQLServerConfigurationOption(
            configurationID: 2,
            name: "test2",
            minimum: 0,
            maximum: 100,
            configuredValue: 50,
            runningValue: 50,
            description: "test option 2",
            isDynamic: true,
            isAdvanced: false
        )
        XCTAssertFalse(nonPending.isPendingRestart, "Should not be pending when configured == running")
    }
}
