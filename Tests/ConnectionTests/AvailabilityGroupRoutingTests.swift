import XCTest
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// Read-only routing in an availability group (`SQLSERVER_TEST_AG_URLS`: the replicas, primary
/// first). A read-intent login to the primary receives an ENVCHANGE routing token and must
/// continue on the secondary it names.
final class AvailabilityGroupRoutingTests: XCTestCase, @unchecked Sendable {
    private struct Group {
        let primary: TestServer
        let primaryName: String
        let database: String
    }

    private func group() async throws -> Group {
        guard let replicas = try TestServer.availabilityGroup(), let primary = replicas.first else {
            if TestServer.isRequired { throw TestServer.Unavailable(TestServer.missingMessage(TestServer.availabilityGroupVariable)) }
            throw XCTSkip(TestServer.missingMessage(TestServer.availabilityGroupVariable))
        }
        let connection = try await SQLServerConnection.connect(configuration: configuration(primary, database: nil, readOnly: false))
        defer { Task { try? await connection.close() } }
        let rows = try await connection.query("""
        SELECT @@SERVERNAME AS server,
               (SELECT TOP 1 database_name FROM sys.availability_databases_cluster ORDER BY database_name) AS db
        """)
        guard let name = rows.first?.column("server")?.string, let database = rows.first?.column("db")?.string else {
            throw XCTSkip("The first replica in \(TestServer.availabilityGroupVariable) has no availability database")
        }
        return Group(primary: primary, primaryName: name, database: database)
    }

    /// Read-only routing from the primary to the first secondary, at the address in
    /// `SQLSERVER_TEST_AG_URLS` (the one this machine can reach), through
    /// `availabilityGroups.setReadOnlyRouting` when the group has no routing list. That list stays,
    /// because the API cannot clear one yet.
    ///
    /// SQL Server routes a read-intent login only when it arrives through a listener. A group
    /// without one gets a listener on the primary's own address and port for the test, so a login
    /// to the primary qualifies; `body` runs with it and it is dropped afterwards.
    private func withRoutedGroup(_ body: (Group) async throws -> Void) async throws {
        let group = try await group()
        guard let replicas = try TestServer.availabilityGroup(), replicas.count > 1 else {
            throw XCTSkip("\(TestServer.availabilityGroupVariable) names no secondary replica")
        }
        let client = try await SQLServerClient.connect(
            configuration: SQLServerClient.Configuration(connection: configuration(group.primary, database: nil, readOnly: false)),
            numberOfThreads: 1
        )
        defer { Task { try? await client.shutdownGracefully() } }
        let state = try await client.query("""
        SELECT (SELECT TOP 1 name FROM sys.availability_groups) AS ag,
               (SELECT COUNT(*) FROM sys.availability_read_only_routing_lists) AS routes,
               (SELECT COUNT(*) FROM sys.availability_group_listeners) AS listeners,
               (SELECT local_net_address FROM sys.dm_exec_connections WHERE session_id = @@SPID) AS address,
               (SELECT local_tcp_port FROM sys.dm_exec_connections WHERE session_id = @@SPID) AS port
        """).first
        guard let groupName = state?.column("ag")?.string else { throw XCTSkip("No availability group on the primary") }

        if (state?.column("routes")?.int ?? 0) == 0 {
            let secondary = replicas[1]
            let connection = try await SQLServerConnection.connect(configuration: configuration(secondary, database: nil, readOnly: false))
            let secondaryName = try await connection.query("SELECT @@SERVERNAME AS server").first?.column("server")?.string
            try? await connection.close()
            guard let secondaryName else { throw XCTSkip("Could not name the secondary replica") }
            try await client.availabilityGroups.setReadOnlyRouting(
                groupName: groupName, replicaName: secondaryName, routingUrl: "TCP://\(secondary.hostname):\(secondary.port)"
            )
            try await client.availabilityGroups.setReadOnlyRouting(
                groupName: groupName, replicaName: group.primaryName, routingList: [secondaryName]
            )
        }

        var listener: String?
        if (state?.column("listeners")?.int ?? 0) == 0 {
            guard let address = state?.column("address")?.string, let port = state?.column("port")?.int else {
                throw XCTSkip("The primary does not report its own address")
            }
            let name = "nio\(UUID().uuidString.prefix(8).lowercased())"
            try await client.availabilityGroups.createListener(
                groupName: groupName, dnsName: name, port: port, ipAddresses: [(ip: address, subnetMask: "255.255.0.0")]
            )
            listener = name
        }
        do {
            try await body(group)
        } catch {
            if let listener { try? await client.availabilityGroups.dropListener(groupName: groupName, dnsName: listener) }
            throw error
        }
        if let listener { try await client.availabilityGroups.dropListener(groupName: groupName, dnsName: listener) }
    }

    private func configuration(_ server: TestServer, database: String?, readOnly: Bool) -> SQLServerConnection.Configuration {
        var configuration = server.configuration
        if let database { configuration.login.database = database }
        configuration.readOnlyIntent = readOnly
        configuration.connectTimeoutSeconds = 20
        return configuration
    }

    private func serverAndUpdateability(_ connection: SQLServerConnection) async throws -> (String?, String?) {
        let rows = try await connection.query("SELECT @@SERVERNAME AS server, CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS NVARCHAR(20)) AS mode;")
        return (rows.first?.column("server")?.string, rows.first?.column("mode")?.string)
    }

    func testReadIntentLoginFollowsRoutingToSecondary() async throws {
        try await withRoutedGroup { group in
            // A row written on the primary, to read back on the secondary.
            let writer = try await SQLServerConnection.connect(configuration: configuration(group.primary, database: group.database, readOnly: false))
            defer { Task { try? await writer.close() } }
            let table = "routing_probe_\(UUID().uuidString.prefix(8))"
            try await writer.createTable(name: table, columns: [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "v", definition: .standard(.init(dataType: .nvarchar(length: .length(20))))),
            ])
            _ = try await writer.insertRow(into: table, values: ["id": .int(1), "v": .nString("primary")])
            defer { Task { try? await writer.dropTable(name: table) } }

            let connection = try await SQLServerConnection.connect(configuration: configuration(group.primary, database: group.database, readOnly: true))
            defer { Task { try? await connection.close() } }
            let (server, mode) = try await serverAndUpdateability(connection)
            XCTAssertNotEqual(server?.lowercased(), group.primaryName.lowercased(), "A read-intent login is routed away from the primary")
            XCTAssertEqual(mode, "READ_ONLY")

            var replicated: String?
            for _ in 0..<30 where replicated == nil {
                replicated = try? await connection.query("SELECT v FROM dbo.[\(table)] WHERE id = 1;").first?.column("v")?.string
                if replicated == nil { try await Task.sleep(for: .milliseconds(500)) }
            }
            XCTAssertEqual(replicated, "primary", "The secondary serves the primary's data")
        }
    }

    func testReadWriteLoginStaysOnPrimary() async throws {
        let group = try await group()
        let connection = try await SQLServerConnection.connect(configuration: configuration(group.primary, database: group.database, readOnly: false))
        defer { Task { try? await connection.close() } }
        let (server, mode) = try await serverAndUpdateability(connection)
        XCTAssertEqual(server?.lowercased(), group.primaryName.lowercased())
        XCTAssertEqual(mode, "READ_WRITE")
    }

    func testPooledReadIntentSessionsAreRoutedAndReset() async throws {
        try await withRoutedGroup { group in
            var configuration = SQLServerClient.Configuration(connection: configuration(group.primary, database: group.database, readOnly: true))
            configuration.poolConfiguration.maximumConcurrentConnections = 2
            let client = try await SQLServerClient.connect(configuration: configuration, numberOfThreads: 1)
            defer { Task { try? await client.shutdownGracefully() } }
            for _ in 0..<5 {
                let rows = try await client.query("SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS NVARCHAR(20)) AS mode, DB_NAME() AS db;")
                XCTAssertEqual(rows.first?.column("mode")?.string, "READ_ONLY")
                XCTAssertEqual(rows.first?.column("db")?.string, group.database, "Session reset restores the routed database")
            }
        }
    }
}
