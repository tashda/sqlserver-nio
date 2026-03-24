import Foundation

// MARK: - PolyBase Models

/// Type of external data source.
public enum ExternalDataSourceType: String, Sendable, Equatable {
    case hadoop = "HADOOP"
    case rdbms = "RDBMS"
    case shardMapManager = "SHARD_MAP_MANAGER"
    case blobStorage = "BLOB_STORAGE"
    case generic = "EXTERNAL_GENERICS"
    case unknown = "UNKNOWN"

    public init(fromDescription desc: String) {
        let upper = desc.uppercased().trimmingCharacters(in: .whitespaces)
        self = ExternalDataSourceType(rawValue: upper) ?? .unknown
    }
}

/// Type of external file format.
public enum ExternalFileFormatType: String, Sendable, Equatable {
    case delimitedText = "DELIMITEDTEXT"
    case rcFile = "RCFILE"
    case orc = "ORC"
    case parquet = "PARQUET"
    case json = "JSON"
    case delta = "DELTA"
    case unknown = "UNKNOWN"

    public init(fromDescription desc: String) {
        let upper = desc.uppercased().trimmingCharacters(in: .whitespaces)
        self = ExternalFileFormatType(rawValue: upper) ?? .unknown
    }
}

/// An external data source (PolyBase / data virtualization).
public struct ExternalDataSource: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let type: ExternalDataSourceType
    public let location: String
    public let credential: String?
    public let databaseName: String?
    public let shardMapName: String?

    public init(
        name: String,
        type: ExternalDataSourceType,
        location: String,
        credential: String?,
        databaseName: String?,
        shardMapName: String?
    ) {
        self.name = name
        self.type = type
        self.location = location
        self.credential = credential
        self.databaseName = databaseName
        self.shardMapName = shardMapName
    }
}

/// An external table (PolyBase / data virtualization).
public struct ExternalTable: Sendable, Equatable, Identifiable {
    public var id: String { "\(schema).\(name)" }
    public let schema: String
    public let name: String
    public let dataSourceName: String
    public let fileFormatName: String?
    public let location: String?

    public init(
        schema: String,
        name: String,
        dataSourceName: String,
        fileFormatName: String?,
        location: String?
    ) {
        self.schema = schema
        self.name = name
        self.dataSourceName = dataSourceName
        self.fileFormatName = fileFormatName
        self.location = location
    }
}

/// An external file format definition.
public struct ExternalFileFormat: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let formatType: ExternalFileFormatType
    public let fieldTerminator: String?
    public let stringDelimiter: String?

    public init(
        name: String,
        formatType: ExternalFileFormatType,
        fieldTerminator: String?,
        stringDelimiter: String?
    ) {
        self.name = name
        self.formatType = formatType
        self.fieldTerminator = fieldTerminator
        self.stringDelimiter = stringDelimiter
    }
}
