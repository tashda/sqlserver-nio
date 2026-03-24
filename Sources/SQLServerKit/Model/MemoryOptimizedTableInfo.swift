import Foundation

/// Durability mode for memory-optimized tables.
public enum MemoryOptimizedDurability: String, Sendable, Equatable {
    /// Both schema and data are persisted to disk (default).
    case schemaAndData = "SCHEMA_AND_DATA"
    /// Only schema is persisted; data is lost on restart.
    case schemaOnly = "SCHEMA_ONLY"
}

/// Information about a memory-optimized (In-Memory OLTP) table.
public struct MemoryOptimizedTableInfo: Sendable, Equatable {
    /// Schema of the memory-optimized table.
    public let schema: String
    /// Name of the memory-optimized table.
    public let name: String
    /// Durability setting for this memory-optimized table.
    public let durability: MemoryOptimizedDurability

    public init(schema: String, name: String, durability: MemoryOptimizedDurability) {
        self.schema = schema
        self.name = name
        self.durability = durability
    }
}
