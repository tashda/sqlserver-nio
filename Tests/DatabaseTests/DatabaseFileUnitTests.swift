import XCTest
@testable import SQLServerKit

// MARK: - SQLServerDatabaseFile Computed Property Tests (no server required)

final class DatabaseFileUnitTests: XCTestCase, @unchecked Sendable {

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
