import Foundation

// MARK: - Row-Level Security Types

/// The type of security predicate in a security policy.
public enum PredicateType: String, Sendable, Hashable {
    case filter = "FILTER"
    case block = "BLOCK"
}

/// The specific block operation for a BLOCK predicate.
public enum BlockOperation: String, Sendable, Hashable {
    case afterInsert = "AFTER INSERT"
    case afterUpdate = "AFTER UPDATE"
    case beforeUpdate = "BEFORE UPDATE"
    case beforeDelete = "BEFORE DELETE"
}

/// A security policy from `sys.security_policies`.
public struct SecurityPolicyInfo: Sendable, Hashable, Identifiable {
    public var id: String { "\(schema).\(name)" }
    public let name: String
    public let schema: String
    public let isEnabled: Bool
    public let isSchemaBound: Bool
    public let createDate: String?
    public let modifyDate: String?

    public init(name: String, schema: String, isEnabled: Bool, isSchemaBound: Bool, createDate: String? = nil, modifyDate: String? = nil) {
        self.name = name
        self.schema = schema
        self.isEnabled = isEnabled
        self.isSchemaBound = isSchemaBound
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}

/// A security predicate within a security policy from `sys.security_predicates`.
public struct SecurityPredicateInfo: Sendable, Hashable, Identifiable {
    public var id: String { "\(targetSchema).\(targetTable).\(predicateType.rawValue).\(operation?.rawValue ?? "")" }
    public let predicateType: PredicateType
    public let predicateDefinition: String
    public let targetSchema: String
    public let targetTable: String
    public let operation: BlockOperation?

    public init(predicateType: PredicateType, predicateDefinition: String, targetSchema: String, targetTable: String, operation: BlockOperation? = nil) {
        self.predicateType = predicateType
        self.predicateDefinition = predicateDefinition
        self.targetSchema = targetSchema
        self.targetTable = targetTable
        self.operation = operation
    }
}
