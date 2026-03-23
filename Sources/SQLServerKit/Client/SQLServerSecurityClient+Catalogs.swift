// MARK: - Catalog Listing (Certificates, Asymmetric Keys, Languages)

extension SQLServerSecurityClient {

    /// Lists certificates in the current database from `sys.certificates`.
    @available(macOS 12.0, *)
    public func listCertificates() async throws -> [CertificateInfo] {
        let sql = """
        SELECT name, subject, CONVERT(VARCHAR(30), expiry_date, 121) AS expiry_date
        FROM sys.certificates
        ORDER BY name
        """
        let rows = try await query(sql)
        return rows.map { row in
            CertificateInfo(
                name: row.column("name")?.string ?? "",
                subject: row.column("subject")?.string,
                expiryDate: row.column("expiry_date")?.string
            )
        }
    }

    /// Lists asymmetric keys in the current database from `sys.asymmetric_keys`.
    @available(macOS 12.0, *)
    public func listAsymmetricKeys() async throws -> [AsymmetricKeyInfo] {
        let sql = """
        SELECT name, algorithm_desc
        FROM sys.asymmetric_keys
        ORDER BY name
        """
        let rows = try await query(sql)
        return rows.map { row in
            AsymmetricKeyInfo(
                name: row.column("name")?.string ?? "",
                algorithm: row.column("algorithm_desc")?.string
            )
        }
    }

    /// Lists available languages from `sys.syslanguages`.
    @available(macOS 12.0, *)
    public func listLanguages() async throws -> [LanguageInfo] {
        let sql = """
        SELECT name, alias, lcid
        FROM sys.syslanguages
        ORDER BY name
        """
        let rows = try await query(sql)
        return rows.map { row in
            LanguageInfo(
                name: row.column("name")?.string ?? "",
                alias: row.column("alias")?.string,
                lcid: row.column("lcid")?.int ?? 0
            )
        }
    }
}
