import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DynamicDataMaskingTests: SecurityTestBase, @unchecked Sendable {

    // MARK: - Mask Function Parsing

    func testParseDefaultMask() {
        let result = MaskFunction.parse("default()")
        XCTAssertEqual(result, .defaultMask)
    }

    func testParseEmailMask() {
        let result = MaskFunction.parse("email()")
        XCTAssertEqual(result, .email)
    }

    func testParseRandomMask() {
        let result = MaskFunction.parse("random(1, 100)")
        XCTAssertEqual(result, .random(start: 1, end: 100))
    }

    func testParsePartialMask() {
        let result = MaskFunction.parse("partial(2, \"XXX\", 1)")
        XCTAssertEqual(result, .partial(prefix: 2, padding: "XXX", suffix: 1))
    }

    func testParseDatetimeMask() {
        let result = MaskFunction.parse("datetime('Y')")
        XCTAssertEqual(result, .datetime(part: "Y"))
    }

    func testMaskFunctionSQLExpression() {
        XCTAssertEqual(MaskFunction.defaultMask.sqlExpression, "default()")
        XCTAssertEqual(MaskFunction.email.sqlExpression, "email()")
        XCTAssertEqual(MaskFunction.random(start: 1, end: 100).sqlExpression, "random(1, 100)")
        XCTAssertEqual(MaskFunction.partial(prefix: 2, padding: "XXX", suffix: 1).sqlExpression, "partial(2, 'XXX', 1)")
        XCTAssertEqual(MaskFunction.datetime(part: "Y").sqlExpression, "datetime('Y')")
    }

    // MARK: - Integration Tests

    func testAddAndListMaskedColumn() async throws {
        let tableName = "test_mask_\(Int.random(in: 1000...9999))"
        try await createTestTable(name: tableName)

        try await securityClient.addMask(schema: "dbo", table: tableName, column: "email", function: .email)

        let masked = try await securityClient.listMaskedColumns(schema: "dbo", table: tableName)
        XCTAssertEqual(masked.count, 1)
        XCTAssertEqual(masked.first?.column, "email")
        XCTAssertNotNil(MaskFunction.parse(masked.first?.maskingFunction ?? ""))
    }

    func testDropMask() async throws {
        let tableName = "test_mask_drop_\(Int.random(in: 1000...9999))"
        try await createTestTable(name: tableName)

        try await securityClient.addMask(schema: "dbo", table: tableName, column: "name", function: .defaultMask)
        try await securityClient.dropMask(schema: "dbo", table: tableName, column: "name")

        let masked = try await securityClient.listMaskedColumns(schema: "dbo", table: tableName)
        XCTAssertTrue(masked.isEmpty, "Mask should have been removed")
    }

    func testGrantAndRevokeUnmask() async throws {
        let userName = "test_unmask_\(Int.random(in: 1000...9999))"
        try await securityClient.createUser(name: userName, type: .withoutLogin)
        usersToDrop.append(userName)

        try await securityClient.grantUnmask(to: userName)
        try await securityClient.revokeUnmask(from: userName)
        // If we get here without throwing, grant/revoke succeeded
    }
}
