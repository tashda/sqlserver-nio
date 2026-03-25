import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class RowLevelSecurityTests: SecurityTestBase, @unchecked Sendable {

    // MARK: - RLS Types

    func testPredicateTypeRawValues() {
        XCTAssertEqual(PredicateType.filter.rawValue, "FILTER")
        XCTAssertEqual(PredicateType.block.rawValue, "BLOCK")
    }

    func testBlockOperationRawValues() {
        XCTAssertEqual(BlockOperation.afterInsert.rawValue, "AFTER INSERT")
        XCTAssertEqual(BlockOperation.afterUpdate.rawValue, "AFTER UPDATE")
        XCTAssertEqual(BlockOperation.beforeUpdate.rawValue, "BEFORE UPDATE")
        XCTAssertEqual(BlockOperation.beforeDelete.rawValue, "BEFORE DELETE")
    }

    func testSecurityPolicyInfoIdentifiable() {
        let info = SecurityPolicyInfo(name: "policy1", schema: "dbo", isEnabled: true, isSchemaBound: false)
        XCTAssertEqual(info.id, "dbo.policy1")
    }

    // MARK: - Integration Tests

    func testListSecurityPolicies() async throws {
        // Should not throw; may return empty if no policies exist
        let policies = try await securityClient.listSecurityPolicies()
        // Just verify it returns an array without error
        _ = policies
    }

    func testListSecurityPredicatesForNonexistentPolicy() async throws {
        let predicates = try await securityClient.listSecurityPredicates(policyName: "nonexistent_policy_xyz", schema: "dbo")
        XCTAssertTrue(predicates.isEmpty, "Should return empty for nonexistent policy")
    }
}
