import Foundation

extension SQLServerConnection {
    @available(macOS 12.0, *)
    public func createTable(
        name: String,
        columns: [SQLServerColumnDefinition],
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        var primaryKeyColumns: [String] = []
        let columnStrings = try columns.map { column -> String in
            if case .standard(let standardColumn) = column.definition, standardColumn.isPrimaryKey {
                primaryKeyColumns.append(column.name)
            }
            return try sqlString(for: column)
        }

        let primaryKeyConstraint: String
        if primaryKeyColumns.isEmpty {
            primaryKeyConstraint = ""
        } else {
            let keys = primaryKeyColumns.map { escapeIdentifier($0) }.joined(separator: ", ")
            primaryKeyConstraint = ", CONSTRAINT [PK_\(name)] PRIMARY KEY (\(keys))"
        }

        let createTableSql = "CREATE TABLE \(qualifiedTableName(name: name, schema: schema, database: database)) (\n  \(columnStrings.joined(separator: ", "))\(primaryKeyConstraint)\n)"
        _ = try await execute(createTableSql)

        for column in columns {
            if case .standard(let standardColumn) = column.definition, let comment = standardColumn.comment {
                _ = try await execute(commentSqlString(forColumn: column.name, inTable: name, schema: schema, comment: comment, database: database))
            }
        }
    }

    @available(macOS 12.0, *)
    public func createPartitionFunction(
        name: String,
        dataType: SQLDataType,
        boundaryOnRight: Bool = true,
        values: [String]
    ) async throws {
        let boundary = boundaryOnRight ? "RIGHT" : "LEFT"
        let valueList = values.joined(separator: ", ")
        _ = try await execute(
            "CREATE PARTITION FUNCTION \(escapeIdentifier(name)) (\(dataType.toSqlString())) AS RANGE \(boundary) FOR VALUES (\(valueList));"
        )
    }

    @available(macOS 12.0, *)
    public func dropPartitionFunction(name: String) async throws {
        _ = try await execute("DROP PARTITION FUNCTION \(escapeIdentifier(name));")
    }

    @available(macOS 12.0, *)
    public func createPartitionScheme(
        name: String,
        functionName: String,
        allTo filegroup: String = "PRIMARY"
    ) async throws {
        _ = try await execute(
            "CREATE PARTITION SCHEME \(escapeIdentifier(name)) AS PARTITION \(escapeIdentifier(functionName)) ALL TO (\(escapeIdentifier(filegroup)));"
        )
    }

    @available(macOS 12.0, *)
    public func dropPartitionScheme(name: String) async throws {
        _ = try await execute("DROP PARTITION SCHEME \(escapeIdentifier(name));")
    }

    @available(macOS 12.0, *)
    public func createPartitionedTable(
        name: String,
        columns: [SQLServerColumnDefinition],
        partitionScheme: String,
        partitionColumn: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        var primaryKeyColumns: [String] = []
        let columnStrings = try columns.map { column -> String in
            if case .standard(let standardColumn) = column.definition, standardColumn.isPrimaryKey {
                primaryKeyColumns.append(column.name)
            }
            return try sqlString(for: column)
        }

        let primaryKeyConstraint: String
        if primaryKeyColumns.isEmpty {
            primaryKeyConstraint = ""
        } else {
            let keys = primaryKeyColumns.map { escapeIdentifier($0) }.joined(separator: ", ")
            primaryKeyConstraint = ", CONSTRAINT [PK_\(name)] PRIMARY KEY CLUSTERED (\(keys))"
        }

        let sql = """
        CREATE TABLE \(qualifiedTableName(name: name, schema: schema, database: database)) (
          \(columnStrings.joined(separator: ", "))
          \(primaryKeyConstraint)
        ) ON \(escapeIdentifier(partitionScheme))(\(escapeIdentifier(partitionColumn)))
        """
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func createSystemVersionedTable(
        name: String,
        historyTableName: String? = nil,
        schema: String = "dbo",
        database: String? = nil,
        primaryKeyColumn: String = "Id",
        primaryKeyType: SQLDataType = .int
    ) async throws {
        var sql = """
        CREATE TABLE \(qualifiedTableName(name: name, schema: schema, database: database)) (
            \(escapeIdentifier(primaryKeyColumn)) \(primaryKeyType.toSqlString()) NOT NULL,
            [ValidFrom] DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
            [ValidTo] DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
            PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
            CONSTRAINT [PK_\(name)] PRIMARY KEY CLUSTERED (\(escapeIdentifier(primaryKeyColumn)))
        ) WITH (SYSTEM_VERSIONING = ON
        """
        if let historyTableName, !historyTableName.isEmpty {
            sql += " (HISTORY_TABLE = \(qualifiedTableName(name: historyTableName, schema: schema, database: nil)))"
        }
        sql += ");"
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func setSystemVersioning(
        table name: String,
        enabled: Bool,
        historyTableName: String? = nil,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let target = qualifiedTableName(name: name, schema: schema, database: database)
        if enabled {
            var sql = "ALTER TABLE \(target) SET (SYSTEM_VERSIONING = ON"
            if let historyTableName, !historyTableName.isEmpty {
                sql += " (HISTORY_TABLE = \(qualifiedTableName(name: historyTableName, schema: schema, database: nil)))"
            }
            sql += ");"
            _ = try await execute(sql)
        } else {
            _ = try await execute("ALTER TABLE \(target) SET (SYSTEM_VERSIONING = OFF);")
        }
    }

    @available(macOS 12.0, *)
    public func dropTable(name: String, schema: String = "dbo", database: String? = nil) async throws {
        _ = try await execute("DROP TABLE \(qualifiedTableName(name: name, schema: schema, database: database))")
    }

    @available(macOS 12.0, *)
    public func renameTable(
        name: String,
        newName: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let escapedNewName = newName.replacingOccurrences(of: "'", with: "''")
        let qualifiedCurrent = "\(schema).\(name)"
        let escapedCurrent = qualifiedCurrent.replacingOccurrences(of: "'", with: "''")
        let renameSql = "EXEC sp_rename N'\(escapedCurrent)', N'\(escapedNewName)', N'OBJECT';"

        if let database {
            let originalDatabase = currentDatabase
            try await changeDatabase(database)
            do {
                _ = try await execute(renameSql)
            } catch {
                try? await changeDatabase(originalDatabase)
                throw error
            }
            if originalDatabase.caseInsensitiveCompare(database) != .orderedSame {
                try await changeDatabase(originalDatabase)
            }
        } else {
            _ = try await execute(renameSql)
        }
    }

    @available(macOS 12.0, *)
    public func truncateTable(name: String, schema: String = "dbo", database: String? = nil) async throws {
        _ = try await execute("TRUNCATE TABLE \(qualifiedTableName(name: name, schema: schema, database: database))")
    }

    @available(macOS 12.0, *)
    public func createView(
        name: String,
        query: String,
        schema: String = "dbo",
        database: String? = nil,
        withEncryption: Bool = false,
        withSchemaBinding: Bool = false,
        withViewMetadata: Bool = false,
        withCheckOption: Bool = false
    ) async throws {
        var sql = "CREATE VIEW \(qualifiedTableName(name: name, schema: schema, database: database))"
        var options: [String] = []
        if withEncryption { options.append("ENCRYPTION") }
        if withSchemaBinding { options.append("SCHEMABINDING") }
        if withViewMetadata { options.append("VIEW_METADATA") }
        if !options.isEmpty {
            sql += "\nWITH \(options.joined(separator: ", "))"
        }
        sql += "\nAS\n\(query)"
        if withCheckOption {
            sql += "\nWITH CHECK OPTION"
        }
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func dropView(name: String, schema: String = "dbo", database: String? = nil) async throws {
        _ = try await execute("DROP VIEW \(qualifiedTableName(name: name, schema: schema, database: database))")
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func insertRow(
        into table: String,
        schema: String = "dbo",
        database: String? = nil,
        values: [String: SQLServerLiteralValue]
    ) async throws -> Int {
        guard !values.isEmpty else {
            throw SQLServerError.invalidArgument("insertRow requires at least one value")
        }
        let ordered = values.keys.sorted()
        let columns = ordered.map { escapeIdentifier($0) }.joined(separator: ", ")
        let literals = ordered.compactMap { values[$0]?.sqlLiteral() }.joined(separator: ", ")
        let result = try await execute("INSERT INTO \(qualifiedTableName(name: table, schema: schema, database: database)) (\(columns)) VALUES (\(literals))")
        return Int(result.totalRowCount)
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func insertRows(
        into table: String,
        schema: String = "dbo",
        database: String? = nil,
        columns: [String],
        values: [[SQLServerLiteralValue]]
    ) async throws -> Int {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("insertRows requires at least one column")
        }
        guard !values.isEmpty else {
            throw SQLServerError.invalidArgument("insertRows requires at least one row")
        }
        for (index, row) in values.enumerated() {
            guard row.count == columns.count else {
                throw SQLServerError.invalidArgument("Row \(index) has \(row.count) values but \(columns.count) columns were specified")
            }
        }
        let columnList = columns.map { escapeIdentifier($0) }.joined(separator: ", ")
        let valueRows = values.map { row in
            "(" + row.map { $0.sqlLiteral() }.joined(separator: ", ") + ")"
        }.joined(separator: ", ")
        let result = try await execute("INSERT INTO \(qualifiedTableName(name: table, schema: schema, database: database)) (\(columnList)) VALUES \(valueRows)")
        return Int(result.totalRowCount)
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func updateRows(
        in table: String,
        schema: String = "dbo",
        database: String? = nil,
        set assignments: [String: SQLServerLiteralValue],
        where predicate: String
    ) async throws -> Int {
        guard !assignments.isEmpty else {
            throw SQLServerError.invalidArgument("updateRows requires at least one assignment")
        }
        let setClause = assignments.keys.sorted().compactMap { key -> String? in
            guard let value = assignments[key] else { return nil }
            return "\(escapeIdentifier(key)) = \(value.sqlLiteral())"
        }.joined(separator: ", ")
        let result = try await execute("UPDATE \(qualifiedTableName(name: table, schema: schema, database: database)) SET \(setClause) WHERE \(predicate)")
        return Int(result.totalRowCount)
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func deleteRows(
        from table: String,
        schema: String = "dbo",
        database: String? = nil,
        where predicate: String? = nil
    ) async throws -> Int {
        var sql = "DELETE FROM \(qualifiedTableName(name: table, schema: schema, database: database))"
        if let predicate, !predicate.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            sql += " WHERE \(predicate)"
        }
        let result = try await execute(sql)
        return Int(result.totalRowCount)
    }

    @available(macOS 12.0, *)
    public func setLockTimeout(milliseconds: Int?) async throws {
        let timeout = milliseconds ?? -1
        _ = try await execute("SET LOCK_TIMEOUT \(timeout)")
    }

    @available(macOS 12.0, *)
    public func addCheckConstraint(
        name: String,
        table: String,
        expression: String,
        schema: String = "dbo"
    ) async throws {
        let sql = """
        ALTER TABLE \(qualifiedTableName(name: table, schema: schema, database: nil))
        ADD CONSTRAINT \(escapeIdentifier(name))
        CHECK (\(expression))
        """
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func addPrimaryKey(
        name: String,
        table: String,
        columns: [String],
        schema: String = "dbo",
        clustered: Bool = true
    ) async throws {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one column is required for primary key constraint")
        }
        let clusterType = clustered ? "CLUSTERED" : "NONCLUSTERED"
        let columnList = columns.map(escapeIdentifier).joined(separator: ", ")
        let sql = """
        ALTER TABLE \(qualifiedTableName(name: table, schema: schema, database: nil))
        ADD CONSTRAINT \(escapeIdentifier(name))
        PRIMARY KEY \(clusterType) (\(columnList))
        """
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func dropPrimaryKey(
        name: String,
        table: String,
        schema: String = "dbo"
    ) async throws {
        let sql = "ALTER TABLE \(qualifiedTableName(name: table, schema: schema, database: nil)) DROP CONSTRAINT \(escapeIdentifier(name))"
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func addTableComment(tableName: String, comment: String, schema: String = "dbo") async throws {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = tableName.replacingOccurrences(of: "'", with: "''")
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_addextendedproperty
            @name = N'MS_Description',
            @value = N'\(escapedComment)',
            @level0type = N'SCHEMA', @level0name = N'\(escapedSchema)',
            @level1type = N'TABLE', @level1name = N'\(escapedTable)';
        """
        _ = try await execute(sql)
    }

    @available(macOS 12.0, *)
    public func setSnapshotIsolation(database: String, enabled: Bool) async throws {
        let escapedDatabase = escapeIdentifier(database)
        let state = enabled ? "ON" : "OFF"
        _ = try await execute("ALTER DATABASE \(escapedDatabase) SET ALLOW_SNAPSHOT_ISOLATION \(state)")
    }

    private func sqlString(for column: SQLServerColumnDefinition) throws -> String {
        let name = escapeIdentifier(column.name)
        switch column.definition {
        case .computed(let expression, let persisted):
            return "\(name) AS (\(expression))\(persisted ? " PERSISTED" : "")"
        case .standard(let standardColumn):
            var parts: [String] = [name, standardColumn.dataType.toSqlString()]
            if let collation = standardColumn.collation, !collation.isEmpty {
                parts.append("COLLATE \(collation)")
            }
            if let identity = standardColumn.identity {
                parts.append("IDENTITY(\(identity.seed), \(identity.increment))")
            }
            if standardColumn.isSparse {
                parts.append("SPARSE")
            }
            parts.append(standardColumn.isNullable ? "NULL" : "NOT NULL")
            if standardColumn.isRowGuidCol {
                parts.append("ROWGUIDCOL")
            }
            if let defaultValue = standardColumn.defaultValue {
                parts.append("DEFAULT \(defaultValue)")
            }
            if standardColumn.isUnique {
                parts.append("UNIQUE")
            }
            return parts.joined(separator: " ")
        }
    }

    private func commentSqlString(
        forColumn column: String,
        inTable table: String,
        schema: String,
        comment: String,
        database: String?
    ) -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        _ = database
        return """
        EXEC sp_addextendedproperty
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'\(escapedSchema)',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
    }

    private func qualifiedTableName(name: String, schema: String, database: String?) -> String {
        if let database, !database.isEmpty {
            return "\(escapeIdentifier(database)).\(escapeIdentifier(schema)).\(escapeIdentifier(name))"
        }
        return "\(escapeIdentifier(schema)).\(escapeIdentifier(name))"
    }

    private func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
