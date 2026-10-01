import Foundation
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Certificates, symmetric and asymmetric keys, and Transparent Data Encryption.
final class EncryptionKeyTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    func testSQLForKeys() {
        XCTAssertEqual(SQLServerSecurityClient.asymmetricKeySQL(name: "k", algorithm: .rsa2048, password: "p'w"),
                       "CREATE ASYMMETRIC KEY [k] WITH ALGORITHM = RSA_2048 ENCRYPTION BY PASSWORD = N'p''w'")
        XCTAssertEqual(SQLServerSecurityClient.symmetricKeySQL(name: "s", algorithm: .aes256, certificate: "c"),
                       "CREATE SYMMETRIC KEY [s] WITH ALGORITHM = AES_256 ENCRYPTION BY CERTIFICATE [c]")
        XCTAssertEqual(SQLServerSecurityClient.certificateSQL(name: "old", subject: "s", startDate: Date(timeIntervalSince1970: 1_262_304_000),
                                                              expiryDate: Date(timeIntervalSince1970: 1_577_836_800)),
                       "CREATE CERTIFICATE [old] WITH SUBJECT = N's', START_DATE = '20100101', EXPIRY_DATE = '20200101'")
    }

    func testKeysInADatabase() async throws {
        try await withTemporaryDatabase(client: client, prefix: "tmp_keys") { db in
            try await self.client.withDatabase(db) { connection in
                let security = SQLServerSecurityClient(connection: connection)
                try await security.createMasterKey(password: "Master-\(UUID().uuidString)")
                try await security.createCertificate(name: "KeyCert", subject: "Protects the data key")
                try await security.createCertificate(name: "OldCert", subject: "Expired", startDate: Date(timeIntervalSince1970: 1_262_304_000),
                                                     expiryDate: Date(timeIntervalSince1970: 1_577_836_800))
                try await security.createSymmetricKey(name: "DataKey", algorithm: .aes256, encryptedByCertificate: "KeyCert")
                try await security.createAsymmetricKey(name: "SigningKey", algorithm: .rsa2048)
                let symmetric = try await security.listSymmetricKeys().first { $0.name == "DataKey" }
                XCTAssertEqual(symmetric?.algorithm, "AES_256")
                XCTAssertEqual(symmetric?.keyLength, 256)
                let asymmetric = try await security.listAsymmetricKeys().contains { $0.name == "SigningKey" }
                XCTAssertTrue(asymmetric)
            }
        }
    }

    func testTransparentDataEncryption() async throws {
        let certificate = "tde_cert_\(UUID().uuidString.prefix(8))"
        let security = client.security
        // master needs a master key to hold the TDE certificate; one may already exist.
        do { try await security.createMasterKey(password: "Master-\(UUID().uuidString)") } catch {
            guard "\(error)".contains("already a master key") else { throw error }
        }
        try await security.createCertificate(name: certificate, subject: "TDE test")
        defer { Task { [client] in try? await client?.security.dropCertificate(name: certificate) } }
        try await withTemporaryDatabase(client: client, prefix: "tmp_tde") { db in
            try await security.createDatabaseEncryptionKey(database: db, serverCertificate: certificate)
            _ = try await self.client.admin.alterDatabaseOption(name: db, option: .encryption(true))
            let state = try await security.listDatabaseEncryption().first { $0.database == db }
            XCTAssertNotNil(state)
            XCTAssertTrue(["ENCRYPTED", "ENCRYPTION_IN_PROGRESS"].contains(state?.state ?? ""), "state \(state?.state ?? "none")")
            XCTAssertEqual(state?.algorithm, "AES_256")
            XCTAssertEqual(state?.certificate, certificate)
        }
    }
}
