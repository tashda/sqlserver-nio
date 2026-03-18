import Foundation
import SQLServerTDS

extension SQLServerAdministrationClient {
    // MARK: - Insert

    @available(macOS 12.0, *)
    @discardableResult
    public func insertRow(
        into table: String,
        schema: String = "dbo",
        values: [String: SQLServerLiteralValue]
    ) async throws -> Int {
        try await client.withConnection { connection in
            try await connection.insertRow(
                into: table,
                schema: schema,
                database: self.database,
                values: values
            )
        }
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func insertRows(
        into table: String,
        schema: String = "dbo",
        columns: [String],
        values: [[SQLServerLiteralValue]]
    ) async throws -> Int {
        try await client.withConnection { connection in
            try await connection.insertRows(
                into: table,
                schema: schema,
                database: self.database,
                columns: columns,
                values: values
            )
        }
    }

    // MARK: - Update

    @available(macOS 12.0, *)
    @discardableResult
    public func updateRows(
        in table: String,
        schema: String = "dbo",
        set assignments: [String: SQLServerLiteralValue],
        where predicate: String
    ) async throws -> Int {
        try await client.withConnection { connection in
            try await connection.updateRows(
                in: table,
                schema: schema,
                database: self.database,
                set: assignments,
                where: predicate
            )
        }
    }

    // MARK: - Delete

    @available(macOS 12.0, *)
    @discardableResult
    public func deleteRows(
        from table: String,
        schema: String = "dbo",
        where predicate: String? = nil
    ) async throws -> Int {
        try await client.withConnection { connection in
            try await connection.deleteRows(
                from: table,
                schema: schema,
                database: self.database,
                where: predicate
            )
        }
    }
}
