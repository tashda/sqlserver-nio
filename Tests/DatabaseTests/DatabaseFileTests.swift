import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DatabaseFileTests: DatabaseTestBase, @unchecked Sendable {
    // MARK: - File Modification

    func testModifyFileSize() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
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

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertGreaterThanOrEqual(Int(updatedFile!.sizeMB), newSizeMB,
                                     "File should have been resized to at least \(newSizeMB) MB")
    }

    func testModifyFileMaxSizeUnlimited() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .maxSizeUnlimited
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertTrue(updatedFile!.isMaxSizeUnlimited, "Max size should be unlimited")
        XCTAssertEqual(updatedFile!.maxSizeRaw, -1)
    }

    func testModifyFileMaxSizeMB() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .maxSizeMB(512)
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
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

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthMB(128)
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertFalse(updatedFile!.isPercentGrowth)
        XCTAssertEqual(updatedFile!.growthMB, 128, "Growth should be 128 MB")
    }

    func testModifyFileGrowthPercent() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthPercent(25)
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertTrue(updatedFile!.isPercentGrowth)
        XCTAssertEqual(updatedFile!.growthPercent, 25, "Growth should be 25%")
    }

    func testModifyFileGrowthNone() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let dataFile = files.first(where: { $0.typeDescription == "ROWS" }) else {
            XCTFail("No data file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: dataFile.name,
            option: .filegrowthNone
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == dataFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertEqual(updatedFile!.growthRaw, 0, "Growth should be 0 (disabled)")
    }

    func testModifyLogFileGrowth() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
        guard let logFile = files.first(where: { $0.typeDescription == "LOG" }) else {
            XCTFail("No log file found")
            return
        }

        try await adminClient.modifyDatabaseFile(
            databaseName: testDatabase,
            logicalFileName: logFile.name,
            option: .filegrowthMB(64)
        )

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let updatedFile = updatedFiles.first(where: { $0.name == logFile.name })
        XCTAssertNotNil(updatedFile)
        XCTAssertEqual(updatedFile!.growthMB, 64, "Log file growth should be 64 MB")
    }

    // MARK: - Add / Remove Files

    func testAddAndRemoveDataFile() async throws {

        // Get the data directory from existing file
        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
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

        var updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
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

        updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let removedFile = updatedFiles.first(where: { $0.name == logicalName })
        XCTAssertNil(removedFile, "Removed file should no longer appear")
    }

    func testAddLogFile() async throws {

        let files = try await adminClient.getDatabaseFiles(name: testDatabase)
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

        let updatedFiles = try await adminClient.getDatabaseFiles(name: testDatabase)
        let addedFile = updatedFiles.first(where: { $0.name == logicalName })
        XCTAssertNotNil(addedFile, "New log file should appear")
        XCTAssertEqual(addedFile?.typeDescription, "LOG")
    }
}
