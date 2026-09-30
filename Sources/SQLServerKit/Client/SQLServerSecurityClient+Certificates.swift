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
