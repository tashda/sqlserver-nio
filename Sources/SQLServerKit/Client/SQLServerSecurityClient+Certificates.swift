import Foundation

@available(macOS 12.0, *)
extension SQLServerSecurityClient {
    // MARK: - Master Key and Certificates

    /// Creates the database master key of the connection's database, which protects certificates' private keys.
    public func createMasterKey(password: String) async throws {
        _ = try await exec(Self.masterKeySQL(password: password))
    }

    /// Creates a self-signed certificate in the connection's database. Needs a database master key.
    public func createCertificate(name: String, subject: String, expiryDate: Date? = nil) async throws {
        _ = try await exec(Self.certificateSQL(name: name, subject: subject, expiryDate: expiryDate))
    }

    /// Writes a certificate and its private key to files on the server (`BACKUP CERTIFICATE`), e.g. to
    /// create the same certificate on another instance with `createCertificate(name:fromFile:…)`.
    public func backupCertificate(name: String, toFile path: String, privateKeyFile: String, privateKeyPassword: String) async throws {
        _ = try await exec(Self.backupCertificateSQL(name: name, path: path, privateKeyFile: privateKeyFile, password: privateKeyPassword))
    }

    /// Creates a certificate from files on the server written by `backupCertificate`. Needs a
    /// database master key.
    public func createCertificate(name: String, fromFile path: String, privateKeyFile: String, privateKeyPassword: String) async throws {
        _ = try await exec(Self.certificateFromFileSQL(name: name, path: path, privateKeyFile: privateKeyFile, password: privateKeyPassword))
    }

    internal static func backupCertificateSQL(name: String, path: String, privateKeyFile: String, password: String) -> String {
        "BACKUP CERTIFICATE \(SQLServerSQL.escapeIdentifier(name)) TO FILE = N'\(SQLServerSQL.escapeLiteral(path))' "
            + "WITH PRIVATE KEY (FILE = N'\(SQLServerSQL.escapeLiteral(privateKeyFile))', "
            + "ENCRYPTION BY PASSWORD = N'\(SQLServerSQL.escapeLiteral(password))')"
    }

    internal static func certificateFromFileSQL(name: String, path: String, privateKeyFile: String, password: String) -> String {
        "CREATE CERTIFICATE \(SQLServerSQL.escapeIdentifier(name)) FROM FILE = N'\(SQLServerSQL.escapeLiteral(path))' "
            + "WITH PRIVATE KEY (FILE = N'\(SQLServerSQL.escapeLiteral(privateKeyFile))', "
            + "DECRYPTION BY PASSWORD = N'\(SQLServerSQL.escapeLiteral(password))')"
    }

    public func dropCertificate(name: String) async throws {
        _ = try await exec("DROP CERTIFICATE \(SQLServerSQL.escapeIdentifier(name))")
    }

    internal static func masterKeySQL(password: String) -> String {
        "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'\(SQLServerSQL.escapeLiteral(password))'"
    }

    internal static func certificateSQL(name: String, subject: String, expiryDate: Date?) -> String {
        var sql = "CREATE CERTIFICATE \(SQLServerSQL.escapeIdentifier(name)) WITH SUBJECT = N'\(SQLServerSQL.escapeLiteral(subject))'"
        if let expiryDate {
            let formatter = DateFormatter()
            formatter.locale = Locale(identifier: "en_US_POSIX")
            formatter.timeZone = TimeZone(secondsFromGMT: 0)
            formatter.dateFormat = "yyyyMMdd"
            sql += ", EXPIRY_DATE = '\(formatter.string(from: expiryDate))'"
        }
        return sql
    }
}
