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

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    private static func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }

    // MARK: - Message Types

    /// Lists all message types in the database.
    @available(macOS 12.0, *)
    public func listMessageTypes(database: String) async throws -> [ServiceBrokerMessageType] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT name, validation_desc, is_ms_shipped
        FROM \(db).sys.service_message_types
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerMessageType(
                name: name,
                validation: row.column("validation_desc")?.string ?? "NONE",
                isSystemObject: (row.column("is_ms_shipped")?.int ?? 0) != 0
            )
        }
    }

    // MARK: - Contracts

    /// Lists all contracts in the database.
    @available(macOS 12.0, *)
    public func listContracts(database: String) async throws -> [ServiceBrokerContract] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT name, is_ms_shipped
        FROM \(db).sys.service_contracts
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ServiceBrokerContract(
                name: name,
                isSystemObject: (row.column("is_ms_shipped")?.int ?? 0) != 0
            )
        }
    }

    // MARK: - Queues

    /// Lists all queues in the database.
    @available(macOS 12.0, *)
    public func listQueues(database: String) async throws -> [ServiceBrokerQueue] {
        let db = Self.escapeIdentifier(database)
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
        let db = Self.escapeIdentifier(database)
        let qualified = "\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(queue))"
        _ = try await client.execute("USE \(db); ALTER QUEUE \(qualified) WITH STATUS = ON")
    }

    /// Disables a queue from receiving messages.
    @available(macOS 12.0, *)
    public func disableQueue(database: String, schema: String, queue: String) async throws {
        let db = Self.escapeIdentifier(database)
        let qualified = "\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(queue))"
        _ = try await client.execute("USE \(db); ALTER QUEUE \(qualified) WITH STATUS = OFF")
    }

    // MARK: - Services

    /// Lists all services in the database.
    @available(macOS 12.0, *)
    public func listServices(database: String) async throws -> [ServiceBrokerService] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            sv.name,
            q.name AS queue_name,
            sv.is_ms_shipped
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
                isSystemObject: (row.column("is_ms_shipped")?.int ?? 0) != 0
            )
        }
    }

    // MARK: - Routes

    /// Lists all routes in the database.
    @available(macOS 12.0, *)
    public func listRoutes(database: String) async throws -> [ServiceBrokerRoute] {
        let db = Self.escapeIdentifier(database)
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
        let db = Self.escapeIdentifier(database)
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
}
