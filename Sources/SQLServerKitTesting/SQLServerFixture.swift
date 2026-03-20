import Foundation

public struct SQLServerFixtureReport: Sendable {
    public let image: String
    public let port: Int
    public let reusedContainer: Bool
    public let recreatedContainer: Bool
    public let fixtureVersion: String
    public let validations: [String]

    public init(
        image: String,
        port: Int,
        reusedContainer: Bool,
        recreatedContainer: Bool,
        fixtureVersion: String,
        validations: [String]
    ) {
        self.image = image
        self.port = port
        self.reusedContainer = reusedContainer
        self.recreatedContainer = recreatedContainer
        self.fixtureVersion = fixtureVersion
        self.validations = validations
    }
}

@discardableResult
public func ensureSQLServerTestFixture(
    requireAdventureWorks: Bool = false
) throws -> SQLServerFixtureReport {
    try SQLServerDockerManager.shared.ensureFixture(requireAdventureWorks: requireAdventureWorks)
}
