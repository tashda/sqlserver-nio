import XCTest
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// Always Encrypted without the keys: with `columnEncryption`, SQL Server describes encrypted columns
/// (plaintext type, deterministic or randomized, key path) and their values stay ciphertext. The keys
/// here are made up, so the ciphertext is copied in as it is (`allowEncryptedValueModifications`).
final class AlwaysEncryptedMetadataTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!

    override func setUp() async throws {
        _ = isLoggingConfigured
        try requireSQLServerTestServer()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    /// Ciphertext as Always Encrypted stores it: version byte, 32-byte MAC, 16-byte IV, AES blocks.
    private static func ciphertext(plaintextBytes: Int, fill: UInt8) -> [UInt8] {
        let blocks = plaintextBytes / 16 + 1
        return [0x01] + [UInt8](repeating: fill, count: 32 + 16 + blocks * 16)
    }

    func testEncryptedColumnsAreDescribedAndKeepTheirCiphertext() async throws {
        let suffix = UUID().uuidString.prefix(8).lowercased()
        let login = "nio_ae_\(suffix)"
        let password = "Ae_\(UUID().uuidString)_a1"
        let keyPath = "CurrentUser/My/0123456789ABCDEF0123456789ABCDEF01234567"

        try await withTemporaryDatabase(client: client, prefix: "ae") { database in
            try await withDbClient(for: database) { dbClient in
                try await dbClient.alwaysEncrypted.createColumnMasterKey(name: "TestMasterKey", keyStoreProviderName: "MSSQL_CERTIFICATE_STORE", keyPath: keyPath)
                try await dbClient.alwaysEncrypted.createColumnEncryptionKey(name: "TestColumnKey", cmkName: "TestMasterKey", algorithm: "RSA_OAEP",
                                                                             encryptedValue: "0x" + String(repeating: "AB", count: 256))
                try await dbClient.admin.createTable(name: "Patients", columns: [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "SSN", definition: .standard(.init(
                        dataType: .nvarchar(length: .length(11)), collation: "Latin1_General_BIN2",
                        alwaysEncrypted: .init(columnEncryptionKey: "TestColumnKey", type: .deterministic)))),
                    SQLServerColumnDefinition(name: "Salary", definition: .standard(.init(
                        dataType: .int, alwaysEncrypted: .init(columnEncryptionKey: "TestColumnKey", type: .randomized)))),
                    SQLServerColumnDefinition(name: "Notes", definition: .standard(.init(
                        dataType: .nvarchar(length: .length(50)), isNullable: true,
                        alwaysEncrypted: .init(columnEncryptionKey: "TestColumnKey", type: .randomized)))),
                ])
                try await self.client.serverSecurity.createSqlLogin(name: login, password: password)
                try await dbClient.security.createUser(name: login, login: login, options: UserOptions(allowEncryptedValueModifications: true))
                try await dbClient.security.addUserToRole(user: login, role: "db_owner")
            }
            defer { Task { [client] in try? await client?.serverSecurity.dropLogin(name: login, dropMappedUsers: true) } }

            // Copy ciphertext in as that user, on a connection without column encryption (encrypted
            // columns are varbinary there).
            let ssn = Self.ciphertext(plaintextBytes: 22, fill: 0x5A)
            let salary = Self.ciphertext(plaintextBytes: 4, fill: 0x6B)
            var copierConfiguration = makeSQLServerConnectionConfiguration()
            copierConfiguration.login = .init(database: database, authentication: .sqlPassword(username: login, password: password))
            copierConfiguration.columnEncryption = false
            let copier = try await SQLServerConnection.connect(configuration: copierConfiguration)
            var options = SQLServerBulkCopyOptions(table: "Patients", columns: ["Id", "SSN", "Salary", "Notes"])
            options.allowEncryptedValueModifications = true
            let summary = try await copier.bulkCopy(rows: [
                SQLServerBulkCopyRow(values: [.int(1), .bytes(ssn), .bytes(salary), .null]),
            ], options: options)
            try? await copier.close()
            XCTAssertEqual(summary.method, .bulkLoad)
            XCTAssertEqual(summary.totalRows, 1)

            var readerConfiguration = makeSQLServerConnectionConfiguration()
            readerConfiguration.login.database = database
            readerConfiguration.columnEncryption = true
            let reader = try await SQLServerConnection.connect(configuration: readerConfiguration)
            defer { Task { try? await reader.close() } }
            XCTAssertTrue(reader.isColumnEncryptionEnabled)

            var columns: [SQLServerColumnDescription] = []
            var rows: [SQLServerRow] = []
            for try await event in reader.streamQuery("SELECT Id, SSN, Salary, Notes FROM dbo.Patients ORDER BY Id") {
                switch event {
                case .metadata(let described): columns = described
                case .row(let row): rows.append(row)
                default: break
                }
            }
            XCTAssertEqual(columns.map(\.name), ["Id", "SSN", "Salary", "Notes"])
            XCTAssertNil(columns[0].encryption)
            let ssnColumn = try XCTUnwrap(columns[1].encryption)
            XCTAssertEqual(ssnColumn.kind, .deterministic)
            XCTAssertEqual(ssnColumn.typeName, "nvarchar(11)")
            XCTAssertEqual(ssnColumn.algorithm, "AEAD_AES_256_CBC_HMAC_SHA_256")
            XCTAssertEqual(ssnColumn.keyStoreName, "MSSQL_CERTIFICATE_STORE")
            XCTAssertEqual(ssnColumn.keyPath, keyPath)
            XCTAssertEqual(columns[2].encryption?.kind, .randomized)
            XCTAssertEqual(columns[2].encryption?.typeName, "int")
            XCTAssertEqual(columns[3].encryption?.typeName, "nvarchar(50)")
            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(rows.first?.column("SSN")?.bytes, ssn, "The ciphertext comes back as stored")
            XCTAssertEqual(rows.first?.column("Salary")?.bytes, salary)
            XCTAssertTrue(rows.first?.column("Notes")?.isNull ?? false)

            // Plain work on the same connection: every COLMETADATA now has a CekTable, RPC return
            // values may carry crypto metadata, and bulk loads write a CekTable of their own.
            let plain = try await reader.query("SELECT 1 AS a, N'x' AS b; SELECT CAST(2.5 AS decimal(5, 2)) AS c")
            XCTAssertEqual(plain.first?.column("a")?.int, 1)
            let rpc = try await reader.call(procedure: "sp_executesql", parameters: [
                .init(name: "@stmt", value: SQLServerValue(string: "SET @out = 41 + 1"), direction: .in),
                .init(name: "@params", value: SQLServerValue(string: "@out int OUTPUT"), direction: .in),
                .init(name: "@out", value: SQLServerValue(int32: 0), direction: .out),
            ])
            XCTAssertEqual(rpc.returnValues.first { $0.name.caseInsensitiveCompare("@out") == .orderedSame }?.int, 42)
            try await reader.createTable(name: "Plain", columns: [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(20))))),
            ])
            let plainCopy = try await reader.bulkCopy(
                rows: (1...3).map { SQLServerBulkCopyRow(values: [.int($0), .nString("row \($0)")]) },
                options: SQLServerBulkCopyOptions(table: "Plain", columns: ["id", "name"])
            )
            XCTAssertEqual(plainCopy.method, .bulkLoad)
            XCTAssertEqual(plainCopy.totalRows, 3)
        }
    }

    func testWithoutColumnEncryptionTheColumnsAreJustBinary() async throws {
        var configuration = makeSQLServerConnectionConfiguration()
        configuration.columnEncryption = false
        let connection = try await SQLServerConnection.connect(configuration: configuration)
        defer { Task { try? await connection.close() } }
        XCTAssertFalse(connection.isColumnEncryptionEnabled)
    }
}
