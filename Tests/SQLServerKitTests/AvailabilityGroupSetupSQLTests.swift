import Testing
@testable import SQLServerKit

@Suite struct AvailabilityGroupSetupSQLTests {
    @Test func endpointUsesCertificateAndAES() {
        let sql = SQLServerAvailabilityGroupsClient.createEndpointSQL(name: "hadr_endpoint", port: 5022, certificate: "ag_cert")
        #expect(sql.contains("CREATE ENDPOINT [hadr_endpoint] STATE = STARTED"))
        #expect(sql.contains("LISTENER_PORT = 5022"))
        #expect(sql.contains("AUTHENTICATION = CERTIFICATE [ag_cert]"))
        #expect(sql.contains("ENCRYPTION = REQUIRED ALGORITHM AES"))
    }

    @Test func groupListsEveryReplicaWithItsModes() {
        let sql = SQLServerAvailabilityGroupsClient.createGroupSQL(
            name: "ag1",
            replicas: [
                .init(serverName: "primary", endpointURL: "TCP://primary:5022"),
                .init(serverName: "secondary", endpointURL: "TCP://secondary:5022", availabilityMode: .asynchronousCommit,
                      secondaryConnections: .readIntentOnly),
            ],
            databases: ["LabData"],
            options: .init(clusterType: .none, requiredSynchronizedSecondariesToCommit: 0)
        )
        #expect(sql.hasPrefix("CREATE AVAILABILITY GROUP [ag1] WITH (CLUSTER_TYPE = NONE, DB_FAILOVER = OFF, REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 0) FOR DATABASE [LabData] REPLICA ON "))
        #expect(sql.contains("N'primary' WITH (ENDPOINT_URL = N'TCP://primary:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL))"))
        #expect(sql.contains("N'secondary' WITH (ENDPOINT_URL = N'TCP://secondary:5022', AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT"))
        #expect(sql.contains("ALLOW_CONNECTIONS = READ_ONLY"))
    }

    @Test func certificateFilesAndServerNameAreQuoted() {
        let backup = SQLServerSecurityClient.backupCertificateSQL(name: "ag_cert", path: "/tmp/a.cer", privateKeyFile: "/tmp/a.pvk", password: "p'w")
        #expect(backup == "BACKUP CERTIFICATE [ag_cert] TO FILE = N'/tmp/a.cer' WITH PRIVATE KEY (FILE = N'/tmp/a.pvk', ENCRYPTION BY PASSWORD = N'p''w')")
        let restore = SQLServerSecurityClient.certificateFromFileSQL(name: "ag_cert", path: "/tmp/a.cer", privateKeyFile: "/tmp/a.pvk", password: "pw")
        #expect(restore.contains("FROM FILE = N'/tmp/a.cer'") && restore.contains("DECRYPTION BY PASSWORD = N'pw'"))
        #expect(SQLServerAdministrationClient.renameServerSQL("replica'1").contains("sp_addserver N'replica''1', 'local'"))
    }
}
