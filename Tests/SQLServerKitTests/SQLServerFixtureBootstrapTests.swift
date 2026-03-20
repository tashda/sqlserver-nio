import XCTest
import SQLServerKitTesting

final class SQLServerFixtureBootstrapTests: XCTestCase {
    func testEnsureAdventureWorksFixtureIsRepeatable() throws {
        guard ProcessInfo.processInfo.environment["USE_DOCKER"] == "1" else {
            throw XCTSkip("USE_DOCKER not set")
        }

        setenv("TDS_LOAD_ADVENTUREWORKS", "1", 1)

        let first = try ensureSQLServerTestFixture(requireAdventureWorks: true)
        let second = try ensureSQLServerTestFixture(requireAdventureWorks: true)

        XCTAssertFalse(first.fixtureVersion.isEmpty)
        XCTAssertFalse(second.fixtureVersion.isEmpty)
        XCTAssertTrue(first.validations.contains("db:AdventureWorks"))
        XCTAssertTrue(second.validations.contains("table:HumanResources.Employee"))
        XCTAssertEqual(first.port, second.port)
        XCTAssertEqual(first.image, second.image)
    }
}
