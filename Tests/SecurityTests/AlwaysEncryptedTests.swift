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

    /// Keys and an encrypted table (no data: inserting needs client-side encryption). The CEK's
    /// value is not checked when it is created, so a placeholder stands in for a wrapped key.
    func testEncryptedColumnsAreCreatedAndListed() async throws {
        try await withTemporaryDatabase(client: client, prefix: "tmp_ae") { db in
            try await self.client.withDatabase(db) { connection in
                let ae = self.client.alwaysEncrypted
                try await ae.createColumnMasterKey(name: "LabCMK", keyStoreProviderName: "MSSQL_CERTIFICATE_STORE",
                                                   keyPath: "CurrentUser/My/0123456789ABCDEF0123456789ABCDEF01234567")
                try await ae.createColumnEncryptionKey(name: "LabCEK", cmkName: "LabCMK", algorithm: "RSA_OAEP",
                                                       encryptedValue: "0x" + String(repeating: "AB", count: 256))
                _ = connection
                try await self.client.admin.createTable(name: "Patients", columns: [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "SSN", definition: .standard(.init(
                        dataType: .nvarchar(length: .length(11)), collation: "Latin1_General_BIN2",
                        alwaysEncrypted: .init(columnEncryptionKey: "LabCEK", type: .deterministic)))),
                    SQLServerColumnDefinition(name: "Salary", definition: .standard(.init(
                        dataType: .int, alwaysEncrypted: .init(columnEncryptionKey: "LabCEK", type: .randomized)))),
                ])
                let columns = try await ae.listEncryptedColumns()
                XCTAssertEqual(Set(columns.map(\.column)), ["SSN", "Salary"])
                XCTAssertEqual(columns.first { $0.column == "SSN" }?.encryptionType.uppercased(), "DETERMINISTIC")
            }
        }
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
