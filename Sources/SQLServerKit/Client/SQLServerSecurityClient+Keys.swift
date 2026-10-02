import Foundation

/// A symmetric key from `sys.symmetric_keys`.
public struct SymmetricKeyInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    /// `AES_256`, `AES_128`, `TRIPLE_DES_3KEY`, …
    public let algorithm: String
    public let keyLength: Int
}

/// Transparent Data Encryption state of one database (`sys.dm_database_encryption_keys`).
public struct DatabaseEncryptionState: Sendable, Hashable {
    public let database: String
    /// `UNENCRYPTED`, `ENCRYPTION_IN_PROGRESS`, `ENCRYPTED`, `KEY_CHANGE_IN_PROGRESS`,
    /// `DECRYPTION_IN_PROGRESS`, … (SQL Server's `encryption_state_desc`).
    public let state: String
    public let algorithm: String
    public let percentComplete: Double
    /// The server certificate protecting the key (by thumbprint match), when there is one.
    public let certificate: String?
}

@available(macOS 12.0, *)
extension SQLServerSecurityClient {
    public enum AsymmetricKeyAlgorithm: String, Sendable { case rsa2048 = "RSA_2048", rsa3072 = "RSA_3072", rsa4096 = "RSA_4096" }
    public enum SymmetricKeyAlgorithm: String, Sendable { case aes256 = "AES_256", aes192 = "AES_192", aes128 = "AES_128" }

    /// Creates an asymmetric key in the connection's database; its private key protected by
    /// `password`, or by the database master key when nil.
    public func createAsymmetricKey(name: String, algorithm: AsymmetricKeyAlgorithm = .rsa2048, password: String? = nil) async throws {
        _ = try await exec(Self.asymmetricKeySQL(name: name, algorithm: algorithm, password: password))
    }

    /// Creates a symmetric key protected by a certificate in the connection's database.
    public func createSymmetricKey(name: String, algorithm: SymmetricKeyAlgorithm = .aes256, encryptedByCertificate certificate: String) async throws {
        _ = try await exec(Self.symmetricKeySQL(name: name, algorithm: algorithm, certificate: certificate))
    }

    public func listSymmetricKeys() async throws -> [SymmetricKeyInfo] {
        let rows = try await query("SELECT name, algorithm_desc, key_length FROM sys.symmetric_keys ORDER BY name")
        return rows.map { row in
            SymmetricKeyInfo(name: row.column("name")?.string ?? "", algorithm: row.column("algorithm_desc")?.string ?? "",
                             keyLength: row.column("key_length")?.int ?? 0)
        }
    }

    /// Creates the database encryption key for TDE in `database`, protected by a certificate in
    /// master; then turn encryption on with `alterDatabaseOption(name:option: .encryption(true))`.
    public func createDatabaseEncryptionKey(database: String, algorithm: SymmetricKeyAlgorithm = .aes256,
                                            serverCertificate: String) async throws {
        // Run inside the database without changing the session's database (a pooled connection
        // must come back to the pool where it was).
        let statement = Self.databaseEncryptionKeySQL(algorithm: algorithm, certificate: serverCertificate)
        _ = try await exec("EXEC \(SQLServerSQL.escapeIdentifier(database)).sys.sp_executesql N'\(SQLServerSQL.escapeLiteral(statement))'")
    }

    /// TDE state of every database that has a database encryption key.
    public func listDatabaseEncryption() async throws -> [DatabaseEncryptionState] {
        let rows = try await query("""
            SELECT DB_NAME(k.database_id) AS database_name,
                   -- encryption_state_desc arrived in SQL Server 2019; the number is on every version.
                   CASE k.encryption_state
                       WHEN 0 THEN 'NONE' WHEN 1 THEN 'UNENCRYPTED' WHEN 2 THEN 'ENCRYPTION_IN_PROGRESS'
                       WHEN 3 THEN 'ENCRYPTED' WHEN 4 THEN 'KEY_CHANGE_IN_PROGRESS' WHEN 5 THEN 'DECRYPTION_IN_PROGRESS'
                       WHEN 6 THEN 'PROTECTION_CHANGE_IN_PROGRESS' ELSE CAST(k.encryption_state AS VARCHAR(10))
                   END AS encryption_state_desc,
                   k.key_algorithm, k.key_length,
                   CAST(k.percent_complete AS FLOAT) AS percent_complete, c.name AS certificate_name
            FROM sys.dm_database_encryption_keys k
            LEFT JOIN master.sys.certificates c ON c.thumbprint = k.encryptor_thumbprint
            ORDER BY database_name
            """)
        return rows.map { row in
            DatabaseEncryptionState(
                database: row.column("database_name")?.string ?? "",
                state: row.column("encryption_state_desc")?.string ?? "",
                algorithm: "\(row.column("key_algorithm")?.string ?? "")_\(row.column("key_length")?.int ?? 0)",
                percentComplete: row.column("percent_complete")?.double ?? 0,
                certificate: row.column("certificate_name")?.string
            )
        }
    }

    internal static func asymmetricKeySQL(name: String, algorithm: AsymmetricKeyAlgorithm, password: String?) -> String {
        var sql = "CREATE ASYMMETRIC KEY \(SQLServerSQL.escapeIdentifier(name)) WITH ALGORITHM = \(algorithm.rawValue)"
        if let password { sql += " ENCRYPTION BY PASSWORD = N'\(SQLServerSQL.escapeLiteral(password))'" }
        return sql
    }

    internal static func symmetricKeySQL(name: String, algorithm: SymmetricKeyAlgorithm, certificate: String) -> String {
        "CREATE SYMMETRIC KEY \(SQLServerSQL.escapeIdentifier(name)) WITH ALGORITHM = \(algorithm.rawValue) "
            + "ENCRYPTION BY CERTIFICATE \(SQLServerSQL.escapeIdentifier(certificate))"
    }

    internal static func databaseEncryptionKeySQL(algorithm: SymmetricKeyAlgorithm, certificate: String) -> String {
        "CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = \(algorithm.rawValue) "
            + "ENCRYPTION BY SERVER CERTIFICATE \(SQLServerSQL.escapeIdentifier(certificate))"
    }
}
