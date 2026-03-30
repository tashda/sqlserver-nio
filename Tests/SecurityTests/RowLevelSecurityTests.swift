import XCTest
import SQLServerKit
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

    // MARK: - SecurityPredicateDefinition

    func testSecurityPredicateDefinitionDefaults() {
        let pred = SecurityPredicateDefinition(
            functionName: "fn_test",
            functionSchema: "dbo",
            targetTable: "Employees",
            targetSchema: "dbo"
        )
        XCTAssertEqual(pred.predicateType, .filter)
        XCTAssertNil(pred.blockOperation)
    }

    func testSecurityPredicateDefinitionBlock() {
        let pred = SecurityPredicateDefinition(
            predicateType: .block,
            functionName: "fn_block",
            functionSchema: "dbo",
            targetTable: "Orders",
            targetSchema: "Sales",
            blockOperation: .beforeDelete
        )
        XCTAssertEqual(pred.predicateType, .block)
        XCTAssertEqual(pred.blockOperation, .beforeDelete)
        XCTAssertEqual(pred.targetSchema, "Sales")
    }

    func testSecurityPredicateDefinitionHashable() {
        let pred1 = SecurityPredicateDefinition(
            functionName: "fn_a",
            functionSchema: "dbo",
            targetTable: "T1",
            targetSchema: "dbo"
        )
        let pred2 = SecurityPredicateDefinition(
            functionName: "fn_a",
            functionSchema: "dbo",
            targetTable: "T1",
            targetSchema: "dbo"
        )
        XCTAssertEqual(pred1, pred2)

        let pred3 = SecurityPredicateDefinition(
            predicateType: .block,
            functionName: "fn_a",
            functionSchema: "dbo",
            targetTable: "T1",
            targetSchema: "dbo",
            blockOperation: .afterInsert
        )
        XCTAssertNotEqual(pred1, pred3)
    }

    // MARK: - Multi-Predicate Create (Requires DB)

    func testCreateSecurityPolicyWithMultiplePredicates() async throws {
        let tableName1 = "rls_mp_t1_\(UUID().uuidString.prefix(8))"
        let tableName2 = "rls_mp_t2_\(UUID().uuidString.prefix(8))"
        let funcName = "rls_mp_fn_\(UUID().uuidString.prefix(8))"
        let policyName = "rls_mp_pol_\(UUID().uuidString.prefix(8))"

        // Create tables
        _ = try await client.execute("CREATE TABLE dbo.\(tableName1) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName1)
        _ = try await client.execute("CREATE TABLE dbo.\(tableName2) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName2)

        // Create predicate function
        _ = try await client.execute("""
        CREATE FUNCTION dbo.\(funcName)(@tenant_id INT)
        RETURNS TABLE
        WITH SCHEMABINDING
        AS RETURN SELECT 1 AS fn_result WHERE @tenant_id = 1
        """)

        addTeardownBlock {
            try? await self.securityClient.dropSecurityPolicy(name: policyName, schema: "dbo")
            _ = try? await self.client.execute("DROP FUNCTION IF EXISTS dbo.\(funcName)")
        }

        let predicates = [
            SecurityPredicateDefinition(
                predicateType: .filter,
                functionName: funcName,
                functionSchema: "dbo",
                targetTable: tableName1,
                targetSchema: "dbo"
            ),
            SecurityPredicateDefinition(
                predicateType: .filter,
                functionName: funcName,
                functionSchema: "dbo",
                targetTable: tableName2,
                targetSchema: "dbo"
            )
        ]

        try await securityClient.createSecurityPolicy(
            name: policyName,
            schema: "dbo",
            predicates: predicates,
            enabled: true,
            schemaBound: true
        )

        let policies = try await securityClient.listSecurityPolicies()
        let created = policies.first { $0.name == policyName }
        XCTAssertNotNil(created, "Policy should exist")
        XCTAssertTrue(created?.isEnabled ?? false, "Policy should be enabled")

        let preds = try await securityClient.listSecurityPredicates(policyName: policyName, schema: "dbo")
        XCTAssertEqual(preds.count, 2, "Should have 2 predicates")
    }

    func testCreateSecurityPolicyEmptyPredicatesThrows() async throws {
        do {
            try await securityClient.createSecurityPolicy(
                name: "empty_policy",
                schema: "dbo",
                predicates: [],
                enabled: true,
                schemaBound: true
            )
            XCTFail("Should throw for empty predicates")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddSecurityPredicate() async throws {
        let tableName1 = "rls_add_t1_\(UUID().uuidString.prefix(8))"
        let tableName2 = "rls_add_t2_\(UUID().uuidString.prefix(8))"
        let funcName = "rls_add_fn_\(UUID().uuidString.prefix(8))"
        let policyName = "rls_add_pol_\(UUID().uuidString.prefix(8))"

        _ = try await client.execute("CREATE TABLE dbo.\(tableName1) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName1)
        _ = try await client.execute("CREATE TABLE dbo.\(tableName2) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName2)

        _ = try await client.execute("""
        CREATE FUNCTION dbo.\(funcName)(@tenant_id INT)
        RETURNS TABLE
        WITH SCHEMABINDING
        AS RETURN SELECT 1 AS fn_result WHERE @tenant_id = 1
        """)

        addTeardownBlock {
            try? await self.securityClient.dropSecurityPolicy(name: policyName, schema: "dbo")
            _ = try? await self.client.execute("DROP FUNCTION IF EXISTS dbo.\(funcName)")
        }

        // Create with single predicate
        try await securityClient.createSecurityPolicy(
            name: policyName,
            schema: "dbo",
            filterFunction: funcName,
            filterFunctionSchema: "dbo",
            targetTable: tableName1,
            targetSchema: "dbo"
        )

        // Add a second predicate
        let newPred = SecurityPredicateDefinition(
            predicateType: .filter,
            functionName: funcName,
            functionSchema: "dbo",
            targetTable: tableName2,
            targetSchema: "dbo"
        )
        try await securityClient.addSecurityPredicate(
            policyName: policyName,
            policySchema: "dbo",
            predicate: newPred
        )

        let preds = try await securityClient.listSecurityPredicates(policyName: policyName, schema: "dbo")
        XCTAssertEqual(preds.count, 2, "Should have 2 predicates after add")
    }

    func testDropSecurityPredicate() async throws {
        let tableName1 = "rls_drp_t1_\(UUID().uuidString.prefix(8))"
        let tableName2 = "rls_drp_t2_\(UUID().uuidString.prefix(8))"
        let funcName = "rls_drp_fn_\(UUID().uuidString.prefix(8))"
        let policyName = "rls_drp_pol_\(UUID().uuidString.prefix(8))"

        _ = try await client.execute("CREATE TABLE dbo.\(tableName1) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName1)
        _ = try await client.execute("CREATE TABLE dbo.\(tableName2) (id INT, tenant_id INT)")
        tablesToDrop.append(tableName2)

        _ = try await client.execute("""
        CREATE FUNCTION dbo.\(funcName)(@tenant_id INT)
        RETURNS TABLE
        WITH SCHEMABINDING
        AS RETURN SELECT 1 AS fn_result WHERE @tenant_id = 1
        """)

        addTeardownBlock {
            try? await self.securityClient.dropSecurityPolicy(name: policyName, schema: "dbo")
            _ = try? await self.client.execute("DROP FUNCTION IF EXISTS dbo.\(funcName)")
        }

        // Create with two predicates
        let predicates = [
            SecurityPredicateDefinition(
                predicateType: .filter,
                functionName: funcName,
                functionSchema: "dbo",
                targetTable: tableName1,
                targetSchema: "dbo"
            ),
            SecurityPredicateDefinition(
                predicateType: .filter,
                functionName: funcName,
                functionSchema: "dbo",
                targetTable: tableName2,
                targetSchema: "dbo"
            )
        ]
        try await securityClient.createSecurityPolicy(
            name: policyName,
            schema: "dbo",
            predicates: predicates
        )

        // Drop one predicate
        try await securityClient.dropSecurityPredicate(
            policyName: policyName,
            policySchema: "dbo",
            predicateType: .filter,
            targetTable: tableName2,
            targetSchema: "dbo"
        )

        let preds = try await securityClient.listSecurityPredicates(policyName: policyName, schema: "dbo")
        XCTAssertEqual(preds.count, 1, "Should have 1 predicate after drop")
    }
}
