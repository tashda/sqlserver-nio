import Foundation

extension SQLServerMetadataOperations {
    public struct Configuration: Sendable {
        public var includeSystemSchemas: Bool
        public var enableColumnCache: Bool
        public var includeRoutineDefinitions: Bool
        public var includeTriggerDefinitions: Bool
        /// Optional timeout (in seconds) for catalog queries. When set, metadata calls will
        /// fail fast instead of hanging indefinitely on blocked system views.
        public var commandTimeout: TimeInterval?
        /// When true, attempts to parse default values for procedure/function parameters
        /// by reading the module definition text. This can be expensive on large databases.
        public var extractParameterDefaults: Bool
        /// Prefer using sp_columns_100 for table column metadata. When false, uses catalog queries for tables too.
        public var preferStoredProcedureColumns: Bool

        public init(
            includeSystemSchemas: Bool = false,
            enableColumnCache: Bool = true,
            includeRoutineDefinitions: Bool = false,
            includeTriggerDefinitions: Bool = true,
            commandTimeout: TimeInterval? = nil,
            extractParameterDefaults: Bool = true,
            preferStoredProcedureColumns: Bool = false
        ) {
            self.includeSystemSchemas = includeSystemSchemas
            self.enableColumnCache = enableColumnCache
            self.includeRoutineDefinitions = includeRoutineDefinitions
            self.includeTriggerDefinitions = includeTriggerDefinitions
            self.commandTimeout = commandTimeout
            self.extractParameterDefaults = extractParameterDefaults
            self.preferStoredProcedureColumns = preferStoredProcedureColumns
        }
    }
}
