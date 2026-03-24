import NIO
import SQLServerTDS

/// Client for SQL Server Always Encrypted operations.
///
/// Provides access to column master keys, column encryption keys, and
/// encrypted column metadata.
///
/// Usage:
/// ```swift
/// let cmks = try await client.alwaysEncrypted.listColumnMasterKeys()
/// let ceks = try await client.alwaysEncrypted.listColumnEncryptionKeys()
/// let encrypted = try await client.alwaysEncrypted.listEncryptedColumns()
/// ```
public final class SQLServerAlwaysEncryptedClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    // MARK: - Column Master Keys

    /// Lists all column master keys in the current database.
    @available(macOS 12.0, *)
    public func listColumnMasterKeys() async throws -> [ColumnMasterKeyInfo] {
        let sql = """
        SELECT name, key_store_provider_name, key_path,
               allow_enclave_computations,
               CONVERT(varchar(30), create_date, 126) AS create_date
        FROM sys.column_master_keys
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            ColumnMasterKeyInfo(
                name: row.column("name")?.string ?? "",
                keyStoreProviderName: row.column("key_store_provider_name")?.string ?? "",
                keyPath: row.column("key_path")?.string ?? "",
                allowEnclaveComputations: row.column("allow_enclave_computations")?.bool ?? false,
                createDate: row.column("create_date")?.string
            )
        }
    }

    /// Creates a column master key definition.
    @available(macOS 12.0, *)
    public func createColumnMasterKey(name: String, keyStoreProviderName: String, keyPath: String) async throws {
        let escapedName = Self.escapeIdentifier(name)
        let escapedProvider = keyStoreProviderName.replacingOccurrences(of: "'", with: "''")
        let escapedPath = keyPath.replacingOccurrences(of: "'", with: "''")
        let sql = """
        CREATE COLUMN MASTER KEY \(escapedName)
        WITH (KEY_STORE_PROVIDER_NAME = N'\(escapedProvider)',
              KEY_PATH = N'\(escapedPath)')
        """
        _ = try await client.execute(sql)
    }

    /// Drops a column master key.
    @available(macOS 12.0, *)
    public func dropColumnMasterKey(name: String) async throws {
        let escapedName = Self.escapeIdentifier(name)
        _ = try await client.execute("DROP COLUMN MASTER KEY \(escapedName)")
    }

    // MARK: - Column Encryption Keys

    /// Lists all column encryption keys in the current database.
    @available(macOS 12.0, *)
    public func listColumnEncryptionKeys() async throws -> [ColumnEncryptionKeyInfo] {
        let sql = """
        SELECT name,
               CONVERT(varchar(30), create_date, 126) AS create_date,
               CONVERT(varchar(30), modify_date, 126) AS modify_date
        FROM sys.column_encryption_keys
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            ColumnEncryptionKeyInfo(
                name: row.column("name")?.string ?? "",
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string
            )
        }
    }

    /// Lists the key values (CMK mappings) for a specific column encryption key.
    @available(macOS 12.0, *)
    public func listColumnEncryptionKeyValues(cekName: String) async throws -> [ColumnEncryptionKeyValueInfo] {
        let sql = """
        SELECT cek.name AS cek_name, cmk.name AS cmk_name,
               v.encryption_algorithm_name AS encryption_algorithm
        FROM sys.column_encryption_key_values AS v
        INNER JOIN sys.column_encryption_keys AS cek ON cek.column_encryption_key_id = v.column_encryption_key_id
        INNER JOIN sys.column_master_keys AS cmk ON cmk.column_master_key_id = v.column_master_key_id
        WHERE cek.name = N'\(cekName.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY cmk.name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            ColumnEncryptionKeyValueInfo(
                cekName: row.column("cek_name")?.string ?? "",
                cmkName: row.column("cmk_name")?.string ?? "",
                encryptionAlgorithm: row.column("encryption_algorithm")?.string ?? ""
            )
        }
    }

    /// Creates a column encryption key.
    @available(macOS 12.0, *)
    public func createColumnEncryptionKey(name: String, cmkName: String, algorithm: String, encryptedValue: String) async throws {
        let escapedName = Self.escapeIdentifier(name)
        let escapedCMK = Self.escapeIdentifier(cmkName)
        let escapedAlgo = algorithm.replacingOccurrences(of: "'", with: "''")
        let sql = """
        CREATE COLUMN ENCRYPTION KEY \(escapedName)
        WITH VALUES (
            COLUMN_MASTER_KEY = \(escapedCMK),
            ALGORITHM = '\(escapedAlgo)',
            ENCRYPTED_VALUE = \(encryptedValue)
        )
        """
        _ = try await client.execute(sql)
    }

    /// Drops a column encryption key.
    @available(macOS 12.0, *)
    public func dropColumnEncryptionKey(name: String) async throws {
        let escapedName = Self.escapeIdentifier(name)
        _ = try await client.execute("DROP COLUMN ENCRYPTION KEY \(escapedName)")
    }

    // MARK: - Encrypted Columns

    /// Lists all columns with Always Encrypted enabled in the current database.
    @available(macOS 12.0, *)
    public func listEncryptedColumns() async throws -> [EncryptedColumnInfo] {
        let sql = """
        SELECT s.name AS schema_name, t.name AS table_name,
               c.name AS column_name,
               CASE c.encryption_type
                   WHEN 1 THEN 'DETERMINISTIC'
                   WHEN 2 THEN 'RANDOMIZED'
                   ELSE 'UNKNOWN'
               END AS encryption_type,
               cek.name AS cek_name
        FROM sys.columns AS c
        INNER JOIN sys.tables AS t ON t.object_id = c.object_id
        INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        INNER JOIN sys.column_encryption_keys AS cek ON cek.column_encryption_key_id = c.column_encryption_key_id
        WHERE c.encryption_type IS NOT NULL
        ORDER BY s.name, t.name, c.column_id
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            EncryptedColumnInfo(
                schema: row.column("schema_name")?.string ?? "",
                table: row.column("table_name")?.string ?? "",
                column: row.column("column_name")?.string ?? "",
                encryptionType: row.column("encryption_type")?.string ?? "",
                cekName: row.column("cek_name")?.string ?? ""
            )
        }
    }
}
