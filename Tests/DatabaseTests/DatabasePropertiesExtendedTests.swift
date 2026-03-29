import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// Tests for the extended Database Properties APIs:
/// filegroups, scoped configurations, mirroring, log shipping,
/// FILESTREAM, cursor, containment, and Service Broker.
final class DatabasePropertiesExtendedTests: DatabaseTestBase, @unchecked Sendable {

    override func setUp() async throws {
        try await super.setUp()
        
        // Ensure contained database support is enabled on the server
        do {
            _ = try await baseClient.serverConfig.setConfiguration(name: "contained database authentication", value: 1)
        } catch {
            print("Warning: Failed to enable contained database authentication: \(error)")
        }
    }

    // MARK: - Filegroups

    func testListFilegroupsReturnsAtLeastPrimary() async throws {
        let filegroups = try await adminClient.listFilegroups(database: testDatabase)

        XCTAssertFalse(filegroups.isEmpty, "Should have at least one filegroup")

        let primary = filegroups.first(where: { $0.isPrimary })
        XCTAssertNotNil(primary, "PRIMARY filegroup should exist")
        XCTAssertTrue(primary!.isDefault, "PRIMARY should be the default filegroup on a new database")
        // Note: is_system for PRIMARY is often 0 in sys.filegroups on modern SQL Server versions
        XCTAssertEqual(primary!.typeDescription, "ROWS_FILEGROUP")
        XCTAssertGreaterThan(primary!.fileCount, 0, "PRIMARY should have at least one file")
    }

    func testCreateAndDropFilegroup() async throws {
        let fgName = "test_fg_\(UUID().uuidString.prefix(8))"

        // Create
        _ = try await adminClient.createFilegroup(database: testDatabase, name: fgName)

        var filegroups = try await adminClient.listFilegroups(database: testDatabase)
        let created = filegroups.first(where: { $0.name == fgName })
        XCTAssertNotNil(created, "New filegroup should exist")
        XCTAssertFalse(created!.isDefault)
        XCTAssertFalse(created!.isReadOnly)
        XCTAssertEqual(created!.fileCount, 0)

        // Drop
        _ = try await adminClient.dropFilegroup(database: testDatabase, filegroup: fgName)

        filegroups = try await adminClient.listFilegroups(database: testDatabase)
        XCTAssertNil(filegroups.first(where: { $0.name == fgName }), "Filegroup should be removed")
    }

    func testSetDefaultFilegroup() async throws {
        let fgName = "test_default_fg_\(UUID().uuidString.prefix(8))"

        // Create filegroup and add a file to it (required for it to be usable)
        _ = try await adminClient.createFilegroup(database: testDatabase, name: fgName)

        // We need to add a file to the filegroup before making it default
        let dataPath = try await getDefaultDataPath()
        let fileName = "\(testDatabase!)_\(fgName).ndf"
        let filePath = "\(dataPath)\(fileName)"
        _ = try await adminClient.addDatabaseFile(
            databaseName: testDatabase,
            logicalName: fgName + "_data",
            fileName: filePath,
            sizeMB: 4,
            fileGroup: fgName
        )

        // Set as default
        _ = try await adminClient.setDefaultFilegroup(database: testDatabase, filegroup: fgName)

        var filegroups = try await adminClient.listFilegroups(database: testDatabase)
        let fg = filegroups.first(where: { $0.name == fgName })
        XCTAssertTrue(fg?.isDefault ?? false, "New filegroup should now be default")

        // Restore PRIMARY as default
        _ = try await adminClient.setDefaultFilegroup(database: testDatabase, filegroup: "PRIMARY")

        // Clean up
        _ = try await adminClient.removeDatabaseFile(databaseName: testDatabase, logicalFileName: fgName + "_data")
        _ = try await adminClient.dropFilegroup(database: testDatabase, filegroup: fgName)

        filegroups = try await adminClient.listFilegroups(database: testDatabase)
        let primary = filegroups.first(where: { $0.isPrimary })
        XCTAssertTrue(primary?.isDefault ?? false, "PRIMARY should be default again")
    }

    func testRenameFilegroup() async throws {
        let originalName = "test_rename_fg_\(UUID().uuidString.prefix(8))"
        let newName = "test_renamed_fg_\(UUID().uuidString.prefix(8))"

        _ = try await adminClient.createFilegroup(database: testDatabase, name: originalName)
        _ = try await adminClient.renameFilegroup(database: testDatabase, oldName: originalName, newName: newName)

        let filegroups = try await adminClient.listFilegroups(database: testDatabase)
        XCTAssertNil(filegroups.first(where: { $0.name == originalName }))
        XCTAssertNotNil(filegroups.first(where: { $0.name == newName }))

        _ = try await adminClient.dropFilegroup(database: testDatabase, filegroup: newName)
    }

    // MARK: - Database Scoped Configurations

    func testListScopedConfigurations() async throws {
        let configs = try await adminClient.listScopedConfigurations(database: testDatabase)

        // SQL Server 2016+ should have scoped configs
        XCTAssertGreaterThan(configs.count, 0, "Should have at least one scoped configuration")

        let names = configs.map(\.name)
        XCTAssertTrue(names.contains("MAXDOP"), "MAXDOP should be a scoped configuration")
    }

    func testAlterScopedConfigurationMaxDOP() async throws {
        // Read original
        let original = try await adminClient.listScopedConfigurations(database: testDatabase)
        let maxdop = original.first(where: { $0.name == "MAXDOP" })
        XCTAssertNotNil(maxdop)
        let originalValue = maxdop!.value

        // Change to a specific value
        let newValue = originalValue == "2" ? "4" : "2"
        _ = try await adminClient.alterScopedConfiguration(database: testDatabase, name: "MAXDOP", value: newValue)

        // Verify
        let updated = try await adminClient.listScopedConfigurations(database: testDatabase)
        let updatedMaxdop = updated.first(where: { $0.name == "MAXDOP" })
        XCTAssertEqual(updatedMaxdop?.value, newValue)

        // Restore
        _ = try await adminClient.alterScopedConfiguration(database: testDatabase, name: "MAXDOP", value: originalValue)
    }

    // MARK: - Mirroring

    func testFetchMirroringStatusForNonMirroredDatabase() async throws {
        let status = try await adminClient.getMirroringStatus(database: testDatabase)

        XCTAssertFalse(status.isConfigured, "Test database should not have mirroring configured")
        XCTAssertNil(status.stateDescription)
        XCTAssertNil(status.roleDescription)
        XCTAssertTrue(status.partnerName.isEmpty)
        XCTAssertTrue(status.witnessName.isEmpty)
    }

    // MARK: - Log Shipping

    func testFetchLogShippingConfigForNonConfiguredDatabase() async throws {
        let config = try await adminClient.getLogShippingConfig(database: testDatabase)
        XCTAssertNil(config, "Test database should not have log shipping configured")
    }

    func testFetchLogShippingSecondaryConfigForNonConfiguredDatabase() async throws {
        let config = try await adminClient.getLogShippingSecondaryConfig(database: testDatabase)
        XCTAssertNil(config, "Test database should not be a log shipping secondary")
    }

    // MARK: - FILESTREAM Options

    func testFetchFilestreamOptions() async throws {
        let options = try await adminClient.getFilestreamOptions(database: testDatabase)

        // New database should have FILESTREAM off by default
        XCTAssertEqual(options.nonTransactedAccess, 0)
        XCTAssertEqual(options.nonTransactedAccessDescription, "OFF")
    }

    // MARK: - Cursor Defaults

    func testFetchCursorDefaults() async throws {
        let defaults = try await adminClient.getCursorDefaults(database: testDatabase)

        // Default for new databases is GLOBAL cursor scope and cursor not close on commit
        XCTAssertFalse(defaults.isLocalCursorDefault, "Default cursor scope should be GLOBAL")
        XCTAssertFalse(defaults.isCursorCloseOnCommitOn, "Cursor close on commit should be OFF by default")
    }

    func testAlterCursorCloseOnCommit() async throws {
        // Enable cursor close on commit
        _ = try await adminClient.alterDatabaseOption(name: testDatabase, option: .cursorCloseOnCommit(true))

        var defaults = try await adminClient.getCursorDefaults(database: testDatabase)
        XCTAssertTrue(defaults.isCursorCloseOnCommitOn)

        // Restore
        _ = try await adminClient.alterDatabaseOption(name: testDatabase, option: .cursorCloseOnCommit(false))

        defaults = try await adminClient.getCursorDefaults(database: testDatabase)
        XCTAssertFalse(defaults.isCursorCloseOnCommitOn)
    }

    func testAlterCursorDefaultLocal() async throws {
        // Set cursor default to LOCAL
        _ = try await adminClient.alterDatabaseOption(name: testDatabase, option: .cursorDefaultLocal(true))

        var defaults = try await adminClient.getCursorDefaults(database: testDatabase)
        XCTAssertTrue(defaults.isLocalCursorDefault)

        // Restore to GLOBAL
        _ = try await adminClient.alterDatabaseOption(name: testDatabase, option: .cursorDefaultLocal(false))

        defaults = try await adminClient.getCursorDefaults(database: testDatabase)
        XCTAssertFalse(defaults.isLocalCursorDefault)
    }

    // MARK: - Service Broker Properties

    func testFetchServiceBrokerProperties() async throws {
        let props = try await adminClient.getServiceBrokerProperties(database: testDatabase)

        // New databases have broker enabled by default
        XCTAssertTrue(props.isBrokerEnabled, "Broker should be enabled by default on new databases")
        XCTAssertFalse(props.serviceBrokerGUID.isEmpty, "Service Broker GUID should not be empty")
    }

    // MARK: - Containment Properties

    func testFetchContainmentPropertiesForNonContainedDatabase() async throws {
        let props = try await adminClient.getContainmentProperties(database: testDatabase)
        XCTAssertNil(props, "Non-contained database should return nil")
    }

    // MARK: - New Database Option Cases

    func testAlterTwoDigitYearCutoff() async throws {
        try await withTemporaryDatabase(client: baseClient, prefix: "contained", contained: true) { dbName in
            let escapedName = dbName.replacingOccurrences(of: "'", with: "''")
            let origRows = try await baseClient.query(
                "SELECT two_digit_year_cutoff FROM sys.databases WHERE name = N'\(escapedName)'"
            )
            let originalValue = origRows.first?.column("two_digit_year_cutoff")?.int ?? 2049

            // Change
            let newValue = originalValue == 2049 ? 2030 : 2049
            _ = try await adminClient.alterDatabaseOption(name: dbName, option: .twoDigitYearCutoff(newValue))

            let updatedRows = try await baseClient.query(
                "SELECT two_digit_year_cutoff FROM sys.databases WHERE name = N'\(escapedName)'"
            )
            let updatedValue = updatedRows.first?.column("two_digit_year_cutoff")?.int ?? 0
            XCTAssertEqual(updatedValue, newValue)

            // Restore
            _ = try await adminClient.alterDatabaseOption(name: dbName, option: .twoDigitYearCutoff(originalValue))
        }
    }

    func testAlterNestedTriggers() async throws {
        try await withTemporaryDatabase(client: baseClient, prefix: "contained", contained: true) { dbName in
            let escapedName = dbName.replacingOccurrences(of: "'", with: "''")

            // Read original
            let origRows = try await baseClient.query(
                "SELECT is_nested_triggers_on FROM sys.databases WHERE name = N'\(escapedName)'"
            )
            let originallyOn = (origRows.first?.column("is_nested_triggers_on")?.int ?? 1) != 0

            // Toggle
            _ = try await adminClient.alterDatabaseOption(name: dbName, option: .nestedTriggers(!originallyOn))

            let updatedRows = try await baseClient.query(
                "SELECT is_nested_triggers_on FROM sys.databases WHERE name = N'\(escapedName)'"
            )
            let updatedValue = (updatedRows.first?.column("is_nested_triggers_on")?.int ?? 0) != 0
            XCTAssertEqual(updatedValue, !originallyOn)

            // Restore
            _ = try await adminClient.alterDatabaseOption(name: dbName, option: .nestedTriggers(originallyOn))
        }
    }

    // MARK: - Helpers

    private func getDefaultDataPath() async throws -> String {
        let rows = try await baseClient.query(
            "SELECT CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(512)) AS path"
        )
        return rows.first?.column("path")?.string ?? "/var/opt/mssql/data/"
    }
}
