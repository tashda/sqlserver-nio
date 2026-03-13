import Foundation
import NIO
import SQLServerKit
import SQLServerTDS

// MARK: - SQL Server Version Utilities

public struct SQLServerVersion {
    public let major: Int
    public let minor: Int
    public let build: Int
    public let revision: Int

    public init(major: Int, minor: Int, build: Int = 0, revision: Int = 0) {
        self.major = major
        self.minor = minor
        self.build = build
        self.revision = revision
    }

    public static func from(string: String) -> SQLServerVersion? {
        let parts = string.split(separator: ".").compactMap { Int($0) }
        guard parts.count >= 2 else { return nil }

        return SQLServerVersion(
            major: parts[0],
            minor: parts[1],
            build: parts.count > 2 ? parts[2] : 0,
            revision: parts.count > 3 ? parts[3] : 0
        )
    }
}

public func getSQLServerVersion(client: SQLServerClient) async throws -> SQLServerVersion? {
    let result = try await client.query("SELECT SERVERPROPERTY('ProductVersion') as version").get()
    guard let versionString = result.first?.column("version")?.string else { return nil }
    return SQLServerVersion.from(string: versionString)
}

public func supportsVersion(_ version: SQLServerVersion, minimumMajor: Int, minimumMinor: Int = 0) -> Bool {
    if version.major > minimumMajor { return true }
    if version.major == minimumMajor && version.minor >= minimumMinor { return true }
    return false
}

// MARK: - Feature Detection Utilities

public func supportsFeature(_ feature: String, client: SQLServerClient) async throws -> Bool {
    let result = try await client.query("""
        SELECT CASE
            WHEN EXISTS (
                SELECT 1 FROM sys.all_objects
                WHERE name = '\(feature)'
            ) THEN 1
            ELSE 0
        END as supported
    """).get()

    return result.first?.column("supported")?.bool ?? false
}

public func requiresMinimumVersion(minimumMajor: Int, minimumMinor: Int = 0, client: SQLServerClient) async throws -> Bool {
    guard let version = try await getSQLServerVersion(client: client) else {
        return false
    }
    return supportsVersion(version, minimumMajor: minimumMajor, minimumMinor: minimumMinor)
}
