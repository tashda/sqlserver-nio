import Foundation

/// Uniquely identifies a SQL Server object for dependency tracking.
public struct SQLServerObjectIdentifier: Sendable, Equatable, Hashable {
    public let schema: String
    public let name: String
    /// sys.objects type code: U (table), V (view), P (proc), FN/IF/TF (functions),
    /// TR (trigger), SN (synonym), TT (table type), SO (sequence).
    public let type: String

    public init(schema: String, name: String, type: String) {
        self.schema = schema
        self.name = name
        self.type = type
    }

    public var qualifiedName: String {
        "[\(schema)].[\(name)]"
    }

    /// Human-readable category name for display.
    public var typeDisplayName: String {
        switch type {
        case "U": return "Table"
        case "V": return "View"
        case "P": return "Stored Procedure"
        case "FN": return "Scalar Function"
        case "IF": return "Inline Function"
        case "TF": return "Table Function"
        case "TR": return "Trigger"
        case "SN": return "Synonym"
        case "TT": return "Table Type"
        case "SO": return "Sequence"
        default: return type
        }
    }

    /// Category key used to group objects in the wizard tree.
    public var typeCategory: String {
        switch type {
        case "U": return "Tables"
        case "V": return "Views"
        case "P": return "Stored Procedures"
        case "FN", "IF", "TF": return "Functions"
        case "TR": return "Triggers"
        case "SN": return "Synonyms"
        case "TT": return "Types"
        case "SO": return "Sequences"
        default: return "Other"
        }
    }
}

/// Options controlling how objects are scripted.
public struct SQLServerScriptingOptions: Sendable {
    /// What to script: schema definition, data INSERT statements, or both.
    public enum ScriptMode: String, Sendable, CaseIterable {
        case schemaOnly = "Schema only"
        case dataOnly = "Data only"
        case schemaAndData = "Schema and Data"
    }

    public var scriptMode: ScriptMode
    public var includePermissions: Bool
    public var includeTriggers: Bool
    public var includeIndexes: Bool
    public var includeExtendedProperties: Bool
    public var checkExistence: Bool
    public var scriptDropAndCreate: Bool
    public var includeUseDatabase: Bool

    public init(
        scriptMode: ScriptMode = .schemaOnly,
        includePermissions: Bool = false,
        includeTriggers: Bool = true,
        includeIndexes: Bool = true,
        includeExtendedProperties: Bool = false,
        checkExistence: Bool = true,
        scriptDropAndCreate: Bool = false,
        includeUseDatabase: Bool = true
    ) {
        self.scriptMode = scriptMode
        self.includePermissions = includePermissions
        self.includeTriggers = includeTriggers
        self.includeIndexes = includeIndexes
        self.includeExtendedProperties = includeExtendedProperties
        self.checkExistence = checkExistence
        self.scriptDropAndCreate = scriptDropAndCreate
        self.includeUseDatabase = includeUseDatabase
    }
}

/// Represents a dependency relationship between two SQL Server objects.
public struct SQLServerScriptingDependency: Sendable, Equatable {
    /// The object that depends on another.
    public let dependentObject: SQLServerObjectIdentifier
    /// The object being referenced.
    public let referencedObject: SQLServerObjectIdentifier
    
    public init(dependentObject: SQLServerObjectIdentifier, referencedObject: SQLServerObjectIdentifier) {
        self.dependentObject = dependentObject
        self.referencedObject = referencedObject
    }
}

/// A collection of objects and their dependencies, ready for topological sorting.
public struct SQLServerDependencyGraph: Sendable {
    public let objects: [SQLServerObjectIdentifier]
    public let dependencies: [SQLServerScriptingDependency]
    
    public init(objects: [SQLServerObjectIdentifier], dependencies: [SQLServerScriptingDependency]) {
        self.objects = objects
        self.dependencies = dependencies
    }
    
    /// Returns the objects in an order safe for scripting (topological sort).
    /// Objects with no dependencies come first.
    public func resolvedOrder() -> [SQLServerObjectIdentifier] {
        var result: [SQLServerObjectIdentifier] = []
        var visited = Set<SQLServerObjectIdentifier>()
        var currentlyVisiting = Set<SQLServerObjectIdentifier>()
        
        // Build adjacency list: referenced -> [dependents]
        // But for scripting we want: [referenced] -> dependent
        // So we visit dependents and ensure all referenced are visited first.
        var adj: [SQLServerObjectIdentifier: [SQLServerObjectIdentifier]] = [:]
        for dep in dependencies {
            adj[dep.dependentObject, default: []].append(dep.referencedObject)
        }
        
        func visit(_ obj: SQLServerObjectIdentifier) {
            if visited.contains(obj) { return }
            if currentlyVisiting.contains(obj) {
                // Cycle detected - in SQL Server this can happen with views/procs.
                // We break the cycle by just accepting the current state.
                return
            }
            
            currentlyVisiting.insert(obj)
            
            // Visit all objects this object depends on first
            if let refs = adj[obj] {
                for ref in refs {
                    visit(ref)
                }
            }
            
            currentlyVisiting.remove(obj)
            visited.insert(obj)
            result.append(obj)
        }
        
        for obj in objects {
            visit(obj)
        }
        
        return result
    }
}
