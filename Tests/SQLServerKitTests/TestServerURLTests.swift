import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// `SQLSERVER_TEST_URL` and friends, parsed into a connection configuration.
@Suite struct TestServerURLTests {
    @Test func plainServerTrustingItsCertificate() throws {
        let server = try TestServer.parse("sqlserver://sa:pass@localhost:1433/master?encrypt=mandatory&trustServerCertificate=true")
        #expect(server.hostname == "localhost")
        #expect(server.port == 1433)
        #expect(server.username == "sa")
        #expect(server.password == "pass")
        #expect(server.database == "master")
        #expect(server.encrypt == .mandatory)
        #expect(server.trustServerCertificate)
        #expect(server.caFile == nil)
        #expect(server.kerberos == nil)

        let configuration = server.configuration
        #expect(configuration.hostname == "localhost")
        #expect(configuration.port == 1433)
        #expect(configuration.login.database == "master")
        #expect(configuration.encryptionMode == .mandatory)
        guard case .sqlPassword(let username, let password) = configuration.login.authentication else {
            Issue.record("expected SQL authentication"); return
        }
        #expect(username == "sa" && password == "pass")
    }

    @Test func strictWithACAFile() throws {
        let server = try TestServer.parse("sqlserver://sa:pass@host:1433/master?encrypt=strict&trustServerCertificate=false&caFile=/path/ca.pem")
        #expect(server.encrypt == .strict)
        #expect(!server.trustServerCertificate)
        #expect(server.caFile == "/path/ca.pem")
        #expect(server.configuration.encryptionMode == .strict)
    }

    @Test func kerberosPrincipalAndSettings() throws {
        let server = try TestServer.parse(
            "sqlserver://labuser%40LAB.TEST@10.0.0.5:14330/master?authentication=kerberos&serviceHost=sql.lab.test&krb5Config=/tmp/krb5.conf",
            variable: TestServer.kerberosVariable
        )
        #expect(server.username == "labuser@LAB.TEST")
        #expect(server.password == "")
        #expect(server.kerberos == TestServer.Kerberos(serviceHost: "sql.lab.test", krb5Config: "/tmp/krb5.conf"))
        // The connection goes to the URL's host; the service ticket is for serviceHost.
        #expect(server.configuration.hostname == "10.0.0.5")
        #expect(server.configuration.serverSPN == "MSSQLSvc/sql.lab.test:14330")
        guard case .windowsIntegrated(let username, let password, let domain) = server.configuration.login.authentication else {
            Issue.record("expected Windows authentication"); return
        }
        #expect(username == "labuser")
        #expect(password == "")
        #expect(domain == "LAB.TEST")
    }

    @Test(arguments: [
        ("p%40ss", "p@ss"),
        ("p%3Ass", "p:ss"),
        ("p%2Fss", "p/ss"),
        ("a%40b%3Ac%2Fd%25e%3Ff%23g", "a@b:c/d%e?f#g"),
        ("Your_password1", "Your_password1"),
    ])
    func percentEncodedPasswords(encoded: String, decoded: String) throws {
        let server = try TestServer.parse("sqlserver://sa:\(encoded)@localhost:1433/master")
        #expect(server.password == decoded)
        #expect(server.hostname == "localhost")
    }

    @Test func defaults() throws {
        let server = try TestServer.parse("sqlserver://sa:pass@db.example.com")
        #expect(server.port == 1433)
        #expect(server.database == "master")
        #expect(server.encrypt == .mandatory)
        #expect(!server.trustServerCertificate)
    }

    @Test func databaseFromThePathAndOtherEncryptSpellings() throws {
        #expect(try TestServer.parse("sqlserver://sa:p@h/Sales%20DB").database == "Sales DB")
        #expect(try TestServer.parse("sqlserver://sa:p@h/?encrypt=optional").encrypt == .optional)
        #expect(try TestServer.parse("sqlserver://sa:p@h/?encrypt=false").encrypt == .optional)
        #expect(try TestServer.parse("sqlserver://sa:p@h/?Encrypt=True").encrypt == .mandatory)
        #expect(try TestServer.parse("sqlserver://sa:p@h/?hostNameInCertificate=sql.corp").hostNameInCertificate == "sql.corp")
        #expect(try TestServer.parse("sqlserver://sa:p@h/?columnEncryption=Enabled").configuration.columnEncryption)
        #expect(try !TestServer.parse("sqlserver://sa:p@h/").configuration.columnEncryption)
    }

    @Test(arguments: [
        "postgres://sa:pass@localhost:5432/postgres",
        "sqlserver://sa:pass@/master",
        "sqlserver://localhost:1433/master",
        "sqlserver://sa:pass@localhost/master?encrypt=sometimes",
        "sqlserver://sa:pass@localhost/master?authentication=ntlm",
    ])
    func malformedURLsAreRejected(url: String) {
        #expect(throws: TestServer.InvalidURL.self) { try TestServer.parse(url) }
    }

    @Test func missingVariableSkipsAndNamesIt() {
        guard case .skip(let message) = TestServer.availability(of: "SQLSERVER_TEST_URL", in: [:]) else {
            Issue.record("expected a skip"); return
        }
        #expect(message.contains("SQLSERVER_TEST_URL"))
        #expect(TestServer.url("SQLSERVER_TEST_URL_THAT_IS_NEVER_SET") == nil)
    }

    @Test func requiredTurnsAMissingVariableIntoAFailure() {
        let environment = ["SQLSERVER_TEST_REQUIRED": "1"]
        guard case .fail(let message) = TestServer.availability(of: "SQLSERVER_TEST_TLS_URL", in: environment) else {
            Issue.record("expected a failure"); return
        }
        #expect(message.contains("SQLSERVER_TEST_TLS_URL"))
        #expect(TestServer.isRequired(in: environment))
        #expect(!TestServer.isRequired(in: [:]))
    }

    @Test func aPresentVariableIsAvailableAndAnUnreadableOneFails() {
        let environment = ["SQLSERVER_TEST_URL": "sqlserver://sa:pass@localhost:1433/master"]
        guard case .available(let server) = TestServer.availability(of: "SQLSERVER_TEST_URL", in: environment) else {
            Issue.record("expected a server"); return
        }
        #expect(server.variable == "SQLSERVER_TEST_URL")
        guard case .fail(let message) = TestServer.availability(of: "SQLSERVER_TEST_URL", in: ["SQLSERVER_TEST_URL": "localhost:1433"]) else {
            Issue.record("expected a failure"); return
        }
        #expect(message.contains("SQLSERVER_TEST_URL"))
    }

    @Test func availabilityGroupReplicasPrimaryFirst() throws {
        let environment = [TestServer.availabilityGroupVariable:
            "sqlserver://sa:p%40ss@host:14431/master?trustServerCertificate=true, sqlserver://sa:p%40ss@host:14432/master?trustServerCertificate=true"]
        let replicas = try #require(try TestServer.availabilityGroup(environment: environment))
        #expect(replicas.map(\.port) == [14431, 14432])
        #expect(replicas.allSatisfy { $0.password == "p@ss" })
        #expect(try TestServer.availabilityGroup(environment: [:]) == nil)
    }
}
