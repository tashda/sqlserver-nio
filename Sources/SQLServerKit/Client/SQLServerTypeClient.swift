import NIO
import SQLServerTDS

// MARK: - User-Defined Table Type Types

public struct UserDefinedTableTypeColumn: Sendable {
    public let name: String
    public let dataType: SQLDataType
    public let isNullable: Bool
    public let maxLength: Int?
    public let precision: UInt8?
    public let scale: UInt8?
    public let isIdentity: Bool
    public let isRowGuidCol: Bool

    public init(
        name: String,
        dataType: SQLDataType,
        isNullable: Bool = true,
        maxLength: Int? = nil,
        precision: UInt8? = nil,
        scale: UInt8? = nil,
        isIdentity: Bool = false,
        isRowGuidCol: Bool = false
    ) {
        self.name = name
        self.dataType = dataType
        self.isNullable = isNullable
        self.maxLength = maxLength
        self.precision = precision
        self.scale = scale
        self.isIdentity = isIdentity
        self.isRowGuidCol = isRowGuidCol
    }
}

public struct UserDefinedTableTypeDefinition: Sendable {
    public let name: String
    public let schema: String
    public var columns: [UserDefinedTableTypeColumn]

    public init(name: String, schema: String = "dbo", columns: [UserDefinedTableTypeColumn]) {
        self.name = name
        self.schema = schema
        self.columns = columns
    }
}

// MARK: - SQLServerTypeClient

public final class SQLServerTypeClient {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Create User-Defined Table Type

    /// Creates a user-defined table type with the specified columns
    public func createUserDefinedTableType(_ definition: UserDefinedTableTypeDefinition) -> EventLoopFuture<Void> {
        let columnDefinitions = definition.columns.map { column in
            var columnDef = "[\(column.name)] \(column.dataType.sqlLiteral)"

            // Add nullable specification if NOT NULL
            if !column.isNullable {
                columnDef += " NOT NULL"
            }

            // Add identity specification
            if column.isIdentity {
                columnDef += " IDENTITY(1,1)"
            }

            // Add rowguidcol specification
            if column.isRowGuidCol {
                columnDef += " ROWGUIDCOL"
            }

            return columnDef
        }.joined(separator: ", ")

        let escapedName = "\(definition.schema).[\(definition.name)]"
        let sql = "CREATE TYPE \(escapedName) AS TABLE (\n\(columnDefinitions)\n)"

        return client.execute(sql).map { _ in () }
    }

    /// Creates a user-defined table type with the specified columns (async version)
    @available(macOS 12.0, *)
    public func createUserDefinedTableType(_ definition: UserDefinedTableTypeDefinition) async throws {
        try await createUserDefinedTableType(definition).get()
    }

    // MARK: - Drop User-Defined Table Type

    /// Drops a user-defined table type
    public func dropUserDefinedTableType(name: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let escapedName = "\(schema).[\(name)]"
        let sql = "DROP TYPE IF EXISTS \(escapedName)"
        return client.execute(sql).map { _ in () }
    }

    /// Drops a user-defined table type (async version)
    @available(macOS 12.0, *)
    public func dropUserDefinedTableType(name: String, schema: String = "dbo") async throws {
        try await dropUserDefinedTableType(name: name, schema: schema).get()
    }

    // MARK: - List User-Defined Table Types

    /// Lists all user-defined table types in the database
    public func listUserDefinedTableTypes(schema: String? = nil) -> EventLoopFuture<[UserDefinedTableTypeDefinition]> {
        let queryFuture: EventLoopFuture<[TDSRow]>

        if let schema = schema {
            // Query for specific schema - use string interpolation for parameter
            let sql = """
            SELECT
                s.name AS schema_name,
                t.name AS type_name,
                c.name AS column_name,
                c.column_id,
                c.system_type_id,
                c.user_type_id,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                c.is_rowguidcol,
                tp.name AS data_type_name,
                st.name AS system_type_name
            FROM sys.table_types t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.columns c ON t.type_table_object_id = c.object_id
            LEFT JOIN sys.types tp ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.types st ON c.system_type_id = st.user_type_id
            WHERE s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
            ORDER BY s.name, t.name, c.column_id
            """

            queryFuture = client.query(sql)
        } else {
            // Query for all schemas
            let sql = """
            SELECT
                s.name AS schema_name,
                t.name AS type_name,
                c.name AS column_name,
                c.column_id,
                c.system_type_id,
                c.user_type_id,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                c.is_rowguidcol,
                tp.name AS data_type_name,
                st.name AS system_type_name
            FROM sys.table_types t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.columns c ON t.type_table_object_id = c.object_id
            LEFT JOIN sys.types tp ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.types st ON c.system_type_id = st.user_type_id
            ORDER BY s.name, t.name, c.column_id
            """

            queryFuture = client.query(sql)
        }

        return queryFuture.map { rows in
            var types: [String: UserDefinedTableTypeDefinition] = [:]

            for row in rows {
                guard let schemaName = row.column("schema_name")?.string,
                      let typeName = row.column("type_name")?.string,
                      let columnName = row.column("column_name")?.string,
                      let isNullable = row.column("is_nullable")?.bool,
                      let isIdentity = row.column("is_identity")?.bool,
                      let isRowGuidCol = row.column("is_rowguidcol")?.bool else {
                    continue
                }

                let dataTypeName = row.column("data_type_name")?.string ?? row.column("system_type_name")?.string ?? "int"
                let dataType = self.parseDataType(from: dataTypeName, row: row)

                let column = UserDefinedTableTypeColumn(
                    name: columnName,
                    dataType: dataType,
                    isNullable: isNullable,
                    maxLength: row.column("max_length")?.int,
                    precision: row.column("precision")?.uint8,
                    scale: row.column("scale")?.uint8,
                    isIdentity: isIdentity,
                    isRowGuidCol: isRowGuidCol
                )

                let typeKey = "\(schemaName).\(typeName)"
                if var existingType = types[typeKey] {
                    existingType.columns.append(column)
                    types[typeKey] = existingType
                } else {
                    let newType = UserDefinedTableTypeDefinition(
                        name: typeName,
                        schema: schemaName,
                        columns: [column]
                    )
                    types[typeKey] = newType
                }
            }

            return Array(types.values)
        }
    }

    /// Lists all user-defined table types in the database (async version)
    @available(macOS 12.0, *)
    public func listUserDefinedTableTypes(schema: String? = nil) async throws -> [UserDefinedTableTypeDefinition] {
        try await listUserDefinedTableTypes(schema: schema).get()
    }

    // MARK: - Helper Methods

    private func parseDataType(from typeName: String, row: TDSRow) -> SQLDataType {
        switch typeName.uppercased() {
        case "INT": return .int
        case "BIGINT": return .bigint
        case "SMALLINT": return .smallint
        case "TINYINT": return .tinyint
        case "BIT": return .bit
        case "DECIMAL", "NUMERIC":
            let precision = row.column("precision")?.uint8 ?? 18
            let scale = row.column("scale")?.uint8 ?? 0
            return .decimal(precision: precision, scale: scale)
        case "FLOAT":
            let mantissa = row.column("precision")?.uint8 ?? 53
            return .float(mantissa: mantissa)
        case "REAL": return .real
        case "MONEY": return .money
        case "SMALLMONEY": return .money // Use .money as fallback
        case "DATE": return .date
        case "TIME":
            let precision = row.column("scale")?.uint8 ?? 7
            return .time(precision: precision)
        case "DATETIME2":
            let precision = row.column("scale")?.uint8 ?? 7
            return .datetime2(precision: precision)
        case "DATETIMEOFFSET":
            let precision = row.column("scale")?.uint8 ?? 7
            return .datetimeoffset(precision: precision)
        case "DATETIME": return .datetime2(precision: 3) // Use datetime2 as fallback
        case "SMALLDATETIME": return .datetime2(precision: 0) // Use datetime2 as fallback
        case "CHAR", "VARCHAR":
            let maxLength = row.column("max_length")?.int ?? 50
            if maxLength == -1 {
                return .varchar(length: .max)
            } else {
                return .varchar(length: .length(UInt16(maxLength)))
            }
        case "NCHAR", "NVARCHAR":
            let maxLength = row.column("max_length")?.int ?? 50
            if maxLength == -1 {
                return .nvarchar(length: .max)
            } else {
                return .nvarchar(length: .length(UInt16(maxLength / 2)))
            }
        case "BINARY", "VARBINARY":
            let maxLength = row.column("max_length")?.int ?? 50
            if maxLength == -1 {
                return .varbinary(length: .max)
            } else {
                return .varbinary(length: .length(UInt16(maxLength)))
            }
        case "TEXT", "NTEXT":
            return .text
        case "IMAGE":
            return .image
        case "UNIQUEIDENTIFIER": return .uniqueidentifier
        case "SQL_VARIANT": return .sql_variant
        case "XML": return .xml
        // Commented out types not available in SQLDataType enum
        // case "CURSOR": return .cursor
        // case "TABLE": return .table
        // case "TIMESTAMP", "ROWVERSION": return .rowversion
        case "TIMESTAMP", "ROWVERSION": return .binary(length: 8) // Use binary(8) as fallback
        default: return .int // fallback
        }
    }
}