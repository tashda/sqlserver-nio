import Foundation

public struct TableSearchResult: Sendable {
    public let schema: String
    public let name: String

    public init(schema: String, name: String) {
        self.schema = schema
        self.name = name
    }
}

public struct ViewSearchResult: Sendable {
    public let schema: String
    public let name: String
    public let definitionSnippet: String?

    public init(schema: String, name: String, definitionSnippet: String?) {
        self.schema = schema
        self.name = name
        self.definitionSnippet = definitionSnippet
    }
}

public struct RoutineSearchResult: Sendable {
    public let schema: String
    public let name: String
    public let definitionSnippet: String?

    public init(schema: String, name: String, definitionSnippet: String?) {
        self.schema = schema
        self.name = name
        self.definitionSnippet = definitionSnippet
    }
}

public struct TriggerSearchResult: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let events: String
    public let timing: String

    public init(schema: String, table: String, name: String, events: String, timing: String) {
        self.schema = schema
        self.table = table
        self.name = name
        self.events = events
        self.timing = timing
    }
}

public struct ColumnSearchResult: Sendable {
    public let schema: String
    public let table: String
    public let column: String
    public let dataType: String

    public init(schema: String, table: String, column: String, dataType: String) {
        self.schema = schema
        self.table = table
        self.column = column
        self.dataType = dataType
    }
}

public struct IndexSearchResult: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let filterDefinition: String?

    public init(schema: String, table: String, name: String, filterDefinition: String?) {
        self.schema = schema
        self.table = table
        self.name = name
        self.filterDefinition = filterDefinition
    }
}

public struct ForeignKeySearchResult: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let referencedSchema: String
    public let referencedTable: String

    public init(schema: String, table: String, name: String, referencedSchema: String, referencedTable: String) {
        self.schema = schema
        self.table = table
        self.name = name
        self.referencedSchema = referencedSchema
        self.referencedTable = referencedTable
    }
}
