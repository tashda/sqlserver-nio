import Foundation

public struct SQLServerColumnDefinition: Sendable {
    public let name: String
    public let definition: ColumnType

    public init(name: String, definition: ColumnType) {
        self.name = name
        self.definition = definition
    }

    public enum ColumnType: Sendable {
        /// A standard data column.
        case standard(StandardColumn)
        /// A column whose value is computed from an expression. When `persisted` is true,
        /// the computed value is stored on disk (PERSISTED).
        case computed(expression: String, persisted: Bool = false)
    }

    public struct StandardColumn: Sendable {
        public let dataType: SQLDataType
        public let isNullable: Bool
        public let isPrimaryKey: Bool
        public let identity: (seed: Int, increment: Int)?
        public let defaultValue: String?
        public let isUnique: Bool
        public let isSparse: Bool
        public let comment: String?
        /// Optional collation for character columns.
        public let collation: String?
        /// Marks the column as ROWGUIDCOL (for UNIQUEIDENTIFIER columns).
        public let isRowGuidCol: Bool
        /// Always Encrypted: the column is encrypted with a column encryption key. Deterministic
        /// character columns need a `*_BIN2` collation.
        public let alwaysEncrypted: AlwaysEncryptedColumn?

        public init(
            dataType: SQLDataType,
            isNullable: Bool = false,
            isPrimaryKey: Bool = false,
            identity: (seed: Int, increment: Int)? = nil,
            defaultValue: String? = nil,
            isUnique: Bool = false,
            isSparse: Bool = false,
            comment: String? = nil,
            collation: String? = nil,
            isRowGuidCol: Bool = false,
            alwaysEncrypted: AlwaysEncryptedColumn? = nil
        ) {
            self.dataType = dataType
            self.isNullable = isNullable
            self.isPrimaryKey = isPrimaryKey
            self.identity = identity
            self.defaultValue = defaultValue
            self.isUnique = isUnique
            self.isSparse = isSparse
            self.comment = comment
            self.collation = collation
            self.isRowGuidCol = isRowGuidCol
            self.alwaysEncrypted = alwaysEncrypted
        }
    }

    /// `ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = …, ENCRYPTION_TYPE = …, ALGORITHM = …)`.
    public struct AlwaysEncryptedColumn: Sendable, Hashable {
        public enum EncryptionType: String, Sendable, Hashable { case deterministic = "Deterministic", randomized = "Randomized" }

        public let columnEncryptionKey: String
        public let type: EncryptionType
        public let algorithm: String

        public init(columnEncryptionKey: String, type: EncryptionType, algorithm: String = "AEAD_AES_256_CBC_HMAC_SHA_256") {
            self.columnEncryptionKey = columnEncryptionKey
            self.type = type
            self.algorithm = algorithm
        }

        var clause: String {
            "ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = \(SQLServerSQL.escapeIdentifier(columnEncryptionKey)), "
                + "ENCRYPTION_TYPE = \(type.rawValue), ALGORITHM = '\(SQLServerSQL.escapeLiteral(algorithm))')"
        }
    }
}

public enum SQLDataType: Sendable {
    // Exact Numerics
    case bit
    case tinyint
    case smallint
    case int
    case bigint
    case decimal(precision: UInt8?, scale: UInt8?)
    case numeric(precision: UInt8?, scale: UInt8?)
    case money
    case smallmoney

    // Approximate Numerics
    case float(mantissa: UInt8?)
    case real

    // Date and Time
    case date
    case datetime
    case datetime2(precision: UInt8?)
    case smalldatetime
    case time(precision: UInt8?)
    case datetimeoffset(precision: UInt8?)

    // Character Strings
    case char(length: UInt16)
    case varchar(length: VarcharLength)
    case text

    // Unicode Character Strings
    case nchar(length: UInt16)
    case nvarchar(length: NvarcharLength)
    case ntext

    // Binary Strings
    case binary(length: UInt16)
    case varbinary(length: VarcharLength)
    case image

    // Other
    case uniqueidentifier
    case sql_variant
    case xml
    /// An automatically generated, unique binary number per row (`ROWVERSION`). Inserts must leave it out.
    case rowversion
    case hierarchyid
    case geometry
    case geography
    /// Native JSON (SQL Server 2025, Azure SQL).
    case json
    /// A vector of 32-bit floats (SQL Server 2025, Azure SQL).
    case vector(dimensions: UInt16)
    case userDefined(name: String, schema: String?)
    case userDefinedTable(name: String, schema: String?)

    public enum VarcharLength: Sendable {
        case length(UInt16)
        case max
    }

    public enum NvarcharLength: Sendable {
        case length(UInt16)
        case max
    }

    /// Returns the SQL literal representation of this data type
    public var sqlLiteral: String {
        return self.toSqlString()
    }

    internal func toSqlString() -> String {
        switch self {
        case .bit: return "BIT"
        case .tinyint: return "TINYINT"
        case .smallint: return "SMALLINT"
        case .int: return "INT"
        case .bigint: return "BIGINT"
        case .decimal(let p, let s):
            return "DECIMAL(\(p ?? 18), \(s ?? 0))"
        case .numeric(let p, let s):
            return "NUMERIC(\(p ?? 18), \(s ?? 0))"
        case .money: return "MONEY"
        case .smallmoney: return "SMALLMONEY"
        case .float(let m):
            return "FLOAT(\(m ?? 53))"
        case .real: return "REAL"
        case .date: return "DATE"
        case .datetime: return "DATETIME"
        case .datetime2(let p):
            return "DATETIME2(\(p ?? 7))"
        case .smalldatetime: return "SMALLDATETIME"
        case .time(let p):
            return "TIME(\(p ?? 7))"
        case .datetimeoffset(let p):
            return "DATETIMEOFFSET(\(p ?? 7))"
        case .char(let l): return "CHAR(\(l))"
        case .varchar(let l):
            switch l {
            case .length(let len): return "VARCHAR(\(len))"
            case .max: return "VARCHAR(MAX)"
            }
        case .text: return "TEXT"
        case .nchar(let l): return "NCHAR(\(l))"
        case .nvarchar(let l):
            switch l {
            case .length(let len): return "NVARCHAR(\(len))"
            case .max: return "NVARCHAR(MAX)"
            }
        case .ntext: return "NTEXT"
        case .binary(let l): return "BINARY(\(l))"
        case .varbinary(let l):
            switch l {
            case .length(let len): return "VARBINARY(\(len))"
            case .max: return "VARBINARY(MAX)"
            }
        case .image: return "IMAGE"
        case .uniqueidentifier: return "UNIQUEIDENTIFIER"
        case .sql_variant: return "SQL_VARIANT"
        case .xml: return "XML"
        case .rowversion: return "ROWVERSION"
        case .hierarchyid: return "HIERARCHYID"
        case .geometry: return "GEOMETRY"
        case .geography: return "GEOGRAPHY"
        case .json: return "JSON"
        case .vector(let dimensions): return "VECTOR(\(dimensions))"
        case .userDefined(let name, let schema):
            if let schema, !schema.isEmpty { return "[\(schema)].[\(name)]" }
            return "[\(name)]"
        case .userDefinedTable(let name, let schema):
            if let schema, !schema.isEmpty { return "[\(schema)].[\(name)]" }
            return "[\(name)]"
        }
    }
}
