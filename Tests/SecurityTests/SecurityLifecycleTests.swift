import XCTest
import NIO
import SQLServerKit
import SQLServerKitTesting

/// Basic lifecycle tests for SQL Server security.
/// Detailed tests are located in:
/// - SecurityUserTests.swift
/// - SecurityRoleTests.swift
/// - SecurityPermissionTests.swift
/// - SecurityComplexTests.swift
/// - SecurityErrorTests.swift
final class SecurityLifecycleTests: SecurityTestBase, @unchecked Sendable {
    func testSecurityClientInitialization() async throws {
        XCTAssertNotNil(securityClient)
        XCTAssertNotNil(adminClient)
    }
}
