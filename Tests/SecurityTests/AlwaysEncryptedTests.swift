import XCTest
import SQLServerKit
import SQLServerKitTesting

final class AlwaysEncryptedTests: SecurityTestBase, @unchecked Sendable {

    var aeClient: SQLServerAlwaysEncryptedClient!

    override func setUp() async throws {
        try await super.setUp()
        aeClient = SQLServerAlwaysEncryptedClient(client: client)
    }

    // MARK: - Type Tests

    func testColumnMasterKeyInfoIdentifiable() {
        let info = ColumnMasterKeyInfo(name: "CMK1", keyStoreProviderName: "MSSQL_CERTIFICATE_STORE", keyPath: "path")
        XCTAssertEqual(info.id, "CMK1")
    }

    func testColumnEncryptionKeyInfoIdentifiable() {
        let info = ColumnEncryptionKeyInfo(name: "CEK1")
        XCTAssertEqual(info.id, "CEK1")
    }

    func testEncryptedColumnInfoIdentifiable() {
        let info = EncryptedColumnInfo(schema: "dbo", table: "t1", column: "c1", encryptionType: "DETERMINISTIC", cekName: "CEK1")
        XCTAssertEqual(info.id, "dbo.t1.c1")
    }

    // MARK: - Integration Tests

    func testListColumnMasterKeys() async throws {
        let keys = try await aeClient.listColumnMasterKeys()
        // Should not throw; may be empty on test instances
        _ = keys
    }

    func testListColumnEncryptionKeys() async throws {
        let keys = try await aeClient.listColumnEncryptionKeys()
        _ = keys
    }

    func testListEncryptedColumns() async throws {
        let cols = try await aeClient.listEncryptedColumns()
        _ = cols
    }

    func testListColumnEncryptionKeyValuesForNonexistent() async throws {
        let vals = try await aeClient.listColumnEncryptionKeyValues(cekName: "nonexistent_cek_xyz")
        XCTAssertTrue(vals.isEmpty, "Should return empty for nonexistent CEK")
    }
}
