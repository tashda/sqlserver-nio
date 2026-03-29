import Foundation

/// Client for Service Broker object inspection and queue management.
///
/// Provides read access to message types, contracts, queues, services,
/// routes, and remote service bindings, plus queue enable/disable.
///
/// Usage:
/// ```swift
/// let queues = try await client.serviceBroker.listQueues(database: "MyDB")
/// try await client.serviceBroker.enableQueue(database: "MyDB", schema: "dbo", queue: "MyQueue")
/// ```
public final class SQLServerServiceBrokerClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Message Types

    /// Lists all message types in the database.
    @available(macOS 12.0, *)
    public func listMessageTypes(database: String) async throws -> [ServiceBrokerMessageType] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT name, validation_desc
        FROM \(db).sys.service_message_types
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerMessageType(
                name: name,
                validation: row.column("validation_desc")?.string ?? "NONE",
                isSystemObject: name.hasPrefix("http://schemas.microsoft.com")
            )
        }
    }

    // MARK: - Contracts

    /// Lists all contracts in the database.
    @available(macOS 12.0, *)
    public func listContracts(database: String) async throws -> [ServiceBrokerContract] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT name
        FROM \(db).sys.service_contracts
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerContract(
                name: name,
                isSystemObject: name.hasPrefix("http://schemas.microsoft.com")
            )
        }
    }

    // MARK: - Queues

    /// Lists all queues in the database.
    @available(macOS 12.0, *)
    public func listQueues(database: String) async throws -> [ServiceBrokerQueue] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT
            s.name AS schema_name,
            q.name,
            q.is_activation_enabled,
            q.activation_procedure,
            q.max_readers,
            q.is_receive_enabled,
            q.is_retention_enabled,
            q.is_enqueue_enabled
        FROM \(db).sys.service_queues AS q
        INNER JOIN \(db).sys.schemas AS s ON s.schema_id = q.schema_id
        ORDER BY s.name, q.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard
                let schema = row.column("schema_name")?.string,
                let name = row.column("name")?.string
            else { return nil }
            return ServiceBrokerQueue(
                schema: schema,
                name: name,
                isActivationEnabled: (row.column("is_activation_enabled")?.int ?? 0) != 0,
                activationProcedure: row.column("activation_procedure")?.string,
                maxQueueReaders: row.column("max_readers")?.int ?? 0,
                isReceiveEnabled: (row.column("is_receive_enabled")?.int ?? 0) != 0,
                isRetentionEnabled: (row.column("is_retention_enabled")?.int ?? 0) != 0,
                isEnqueueEnabled: (row.column("is_enqueue_enabled")?.int ?? 0) != 0
            )
        }
    }

    /// Enables a queue for receiving messages.
    @available(macOS 12.0, *)
    public func enableQueue(database: String, schema: String, queue: String) async throws {
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(queue))"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("ALTER QUEUE \(qualified) WITH STATUS = ON")
        }
    }

    /// Disables a queue from receiving messages.
    @available(macOS 12.0, *)
    public func disableQueue(database: String, schema: String, queue: String) async throws {
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(queue))"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("ALTER QUEUE \(qualified) WITH STATUS = OFF")
        }
    }

    // MARK: - Services

    /// Lists all services in the database.
    @available(macOS 12.0, *)
    public func listServices(database: String) async throws -> [ServiceBrokerService] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT
            sv.name,
            q.name AS queue_name
        FROM \(db).sys.services AS sv
        INNER JOIN \(db).sys.service_queues AS q ON q.object_id = sv.service_queue_id
        ORDER BY sv.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerService(
                name: name,
                queueName: row.column("queue_name")?.string ?? "",
                isSystemObject: name.hasPrefix("http://schemas.microsoft.com")
            )
        }
    }

    // MARK: - Routes

    /// Lists all routes in the database.
    @available(macOS 12.0, *)
    public func listRoutes(database: String) async throws -> [ServiceBrokerRoute] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT name, address, broker_instance, lifetime, mirror_address
        FROM \(db).sys.routes
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerRoute(
                name: name,
                address: row.column("address")?.string,
                brokerInstance: row.column("broker_instance")?.string,
                lifetime: row.column("lifetime")?.int,
                mirrorAddress: row.column("mirror_address")?.string
            )
        }
    }

    // MARK: - Remote Service Bindings

    /// Lists all remote service bindings in the database.
    @available(macOS 12.0, *)
    public func listRemoteServiceBindings(database: String) async throws -> [ServiceBrokerRemoteBinding] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT
            r.name,
            r.remote_service_name,
            dp.name AS principal_name,
            r.is_anonymous_on
        FROM \(db).sys.remote_service_bindings AS r
        LEFT JOIN \(db).sys.database_principals AS dp ON dp.principal_id = r.principal_id
        ORDER BY r.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerRemoteBinding(
                name: name,
                serviceName: row.column("remote_service_name")?.string ?? "",
                principalName: row.column("principal_name")?.string,
                isAnonymous: (row.column("is_anonymous_on")?.int ?? 0) != 0
            )
        }
    }

    // MARK: - Create Operations

    /// Creates a message type.
    @available(macOS 12.0, *)
    public func createMessageType(
        database: String,
        name: String,
        validation: MessageTypeValidation = .none
    ) async throws {
        let sql = "CREATE MESSAGE TYPE \(SQLServerSQL.escapeIdentifier(name)) VALIDATION = \(validation.sqlClause)"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(sql)
        }
    }

    /// Drops a message type.
    @available(macOS 12.0, *)
    public func dropMessageType(database: String, name: String) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("DROP MESSAGE TYPE \(SQLServerSQL.escapeIdentifier(name))")
        }
    }

    /// Creates a contract with one or more message type usages.
    @available(macOS 12.0, *)
    public func createContract(
        database: String,
        name: String,
        messageUsages: [(messageType: String, sentBy: ContractSentBy)]
    ) async throws {
        guard !messageUsages.isEmpty else {
            throw SQLServerError.invalidArgument("At least one message type usage is required")
        }
        let usages = messageUsages.map { usage in
            "\(SQLServerSQL.escapeIdentifier(usage.messageType)) SENT BY \(usage.sentBy.rawValue)"
        }.joined(separator: ",\n    ")
        let sql = "CREATE CONTRACT \(SQLServerSQL.escapeIdentifier(name)) (\n    \(usages)\n)"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(sql)
        }
    }

    /// Drops a contract.
    @available(macOS 12.0, *)
    public func dropContract(database: String, name: String) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("DROP CONTRACT \(SQLServerSQL.escapeIdentifier(name))")
        }
    }

    /// Creates a queue with optional activation and poison message handling.
    @available(macOS 12.0, *)
    public func createQueue(
        database: String,
        schema: String = "dbo",
        name: String,
        options: QueueCreationOptions = .defaults
    ) async throws {
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))"
        var withParts: [String] = []
        withParts.append("STATUS = \(options.status ? "ON" : "OFF")")
        withParts.append("RETENTION = \(options.retention ? "ON" : "OFF")")
        if options.activationEnabled, let proc = options.activationProcedure, !proc.isEmpty {
            var activation = "ACTIVATION (STATUS = ON, PROCEDURE_NAME = \(SQLServerSQL.escapeIdentifier(proc))"
            activation += ", MAX_QUEUE_READERS = \(options.maxQueueReaders)"
            if let ea = options.executeAs {
                activation += ", EXECUTE AS '\(SQLServerSQL.escapeLiteral(ea))'"
            } else {
                activation += ", EXECUTE AS SELF"
            }
            activation += ")"
            withParts.append(activation)
        }
        withParts.append("POISON_MESSAGE_HANDLING (STATUS = \(options.poisonMessageHandling ? "ON" : "OFF"))")
        let sql = "CREATE QUEUE \(qualified) WITH \(withParts.joined(separator: ", "))"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(sql)
        }
    }

    /// Drops a queue.
    @available(macOS 12.0, *)
    public func dropQueue(database: String, schema: String = "dbo", name: String) async throws {
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("DROP QUEUE \(qualified)")
        }
    }

    /// Creates a service on an existing queue.
    @available(macOS 12.0, *)
    public func createService(
        database: String,
        name: String,
        queue: String,
        contracts: [String] = []
    ) async throws {
        var query = "CREATE SERVICE \(SQLServerSQL.escapeIdentifier(name)) ON QUEUE \(SQLServerSQL.escapeIdentifier(queue))"
        if !contracts.isEmpty {
            let contractList = contracts.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
            query += " (\(contractList))"
        }
        let sql = query
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(sql)
        }
    }

    /// Drops a service.
    @available(macOS 12.0, *)
    public func dropService(database: String, name: String) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("DROP SERVICE \(SQLServerSQL.escapeIdentifier(name))")
        }
    }

    /// Creates a route.
    @available(macOS 12.0, *)
    public func createRoute(
        database: String,
        name: String,
        address: String,
        serviceName: String? = nil,
        brokerInstance: String? = nil,
        lifetime: Int? = nil,
        mirrorAddress: String? = nil
    ) async throws {
        var withParts: [String] = []
        if let sn = serviceName { withParts.append("SERVICE_NAME = N'\(SQLServerSQL.escapeLiteral(sn))'") }
        if let bi = brokerInstance { withParts.append("BROKER_INSTANCE = N'\(SQLServerSQL.escapeLiteral(bi))'") }
        if let lt = lifetime { withParts.append("LIFETIME = \(lt)") }
        withParts.append("ADDRESS = N'\(SQLServerSQL.escapeLiteral(address))'")
        if let ma = mirrorAddress { withParts.append("MIRROR_ADDRESS = N'\(SQLServerSQL.escapeLiteral(ma))'") }
        let sql = "CREATE ROUTE \(SQLServerSQL.escapeIdentifier(name)) WITH \(withParts.joined(separator: ", "))"
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(sql)
        }
    }

    /// Drops a route.
    @available(macOS 12.0, *)
    public func dropRoute(database: String, name: String) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute("DROP ROUTE \(SQLServerSQL.escapeIdentifier(name))")
        }
    }
}
