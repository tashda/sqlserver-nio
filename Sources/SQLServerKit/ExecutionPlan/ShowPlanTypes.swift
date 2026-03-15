import Foundation

// MARK: - ShowPlan

/// A parsed SQL Server execution plan (ShowPlanXML).
public struct ShowPlan: Sendable {
    /// The statements contained in this plan.
    public let statements: [ShowPlanStatement]
    /// SQL Server build version that generated the plan.
    public let buildVersion: String?
    /// The raw XML string preserved for display/export.
    public let xml: String

    public init(statements: [ShowPlanStatement], buildVersion: String?, xml: String) {
        self.statements = statements
        self.buildVersion = buildVersion
        self.xml = xml
    }
}

// MARK: - ShowPlanStatement

/// A single statement within an execution plan.
public struct ShowPlanStatement: Sendable {
    public let statementText: String
    public let statementType: String
    public let statementSubTreeCost: Double?
    public let statementEstRows: Double?
    public let queryHash: String?
    public let queryPlanHash: String?
    public let optimizationLevel: String?
    public let queryPlan: ShowPlanQueryPlan?

    public init(
        statementText: String,
        statementType: String,
        statementSubTreeCost: Double? = nil,
        statementEstRows: Double? = nil,
        queryHash: String? = nil,
        queryPlanHash: String? = nil,
        optimizationLevel: String? = nil,
        queryPlan: ShowPlanQueryPlan? = nil
    ) {
        self.statementText = statementText
        self.statementType = statementType
        self.statementSubTreeCost = statementSubTreeCost
        self.statementEstRows = statementEstRows
        self.queryHash = queryHash
        self.queryPlanHash = queryPlanHash
        self.optimizationLevel = optimizationLevel
        self.queryPlan = queryPlan
    }
}

// MARK: - ShowPlanQueryPlan

/// The query plan portion of a statement, containing the operator tree.
public struct ShowPlanQueryPlan: Sendable {
    public let cachedPlanSize: Int?
    public let compileTime: Int?
    public let compileCPU: Int?
    public let rootOperator: ShowPlanOperator?
    public let missingIndexes: [ShowPlanMissingIndex]

    public init(
        cachedPlanSize: Int? = nil,
        compileTime: Int? = nil,
        compileCPU: Int? = nil,
        rootOperator: ShowPlanOperator? = nil,
        missingIndexes: [ShowPlanMissingIndex] = []
    ) {
        self.cachedPlanSize = cachedPlanSize
        self.compileTime = compileTime
        self.compileCPU = compileCPU
        self.rootOperator = rootOperator
        self.missingIndexes = missingIndexes
    }
}

// MARK: - ShowPlanOperator

/// A single operator node in the execution plan tree (recursive).
public struct ShowPlanOperator: Sendable {
    public let nodeId: Int
    public let physicalOp: String
    public let logicalOp: String
    public let estimateRows: Double?
    public let estimateIO: Double?
    public let estimateCPU: Double?
    public let avgRowSize: Int?
    public let totalSubtreeCost: Double?
    public let isParallel: Bool
    public let estimatedExecutions: Double?
    /// Actual rows (only present in actual execution plans).
    public let actualRows: Int?
    /// Actual executions (only present in actual execution plans).
    public let actualExecutions: Int?
    /// Actual elapsed time in milliseconds (only present in actual execution plans).
    public let actualElapsedMs: Int?
    /// Actual CPU time in milliseconds (only present in actual execution plans).
    public let actualCPUMs: Int?
    /// Child operators in the plan tree.
    public let children: [ShowPlanOperator]
    /// Output columns for this operator.
    public let outputColumns: [ShowPlanColumnReference]
    /// Warnings associated with this operator.
    public let warnings: [String]

    public init(
        nodeId: Int,
        physicalOp: String,
        logicalOp: String,
        estimateRows: Double? = nil,
        estimateIO: Double? = nil,
        estimateCPU: Double? = nil,
        avgRowSize: Int? = nil,
        totalSubtreeCost: Double? = nil,
        isParallel: Bool = false,
        estimatedExecutions: Double? = nil,
        actualRows: Int? = nil,
        actualExecutions: Int? = nil,
        actualElapsedMs: Int? = nil,
        actualCPUMs: Int? = nil,
        children: [ShowPlanOperator] = [],
        outputColumns: [ShowPlanColumnReference] = [],
        warnings: [String] = []
    ) {
        self.nodeId = nodeId
        self.physicalOp = physicalOp
        self.logicalOp = logicalOp
        self.estimateRows = estimateRows
        self.estimateIO = estimateIO
        self.estimateCPU = estimateCPU
        self.avgRowSize = avgRowSize
        self.totalSubtreeCost = totalSubtreeCost
        self.isParallel = isParallel
        self.estimatedExecutions = estimatedExecutions
        self.actualRows = actualRows
        self.actualExecutions = actualExecutions
        self.actualElapsedMs = actualElapsedMs
        self.actualCPUMs = actualCPUMs
        self.children = children
        self.outputColumns = outputColumns
        self.warnings = warnings
    }
}

// MARK: - ShowPlanColumnReference

/// A column reference in an operator's output list.
public struct ShowPlanColumnReference: Sendable {
    public let database: String?
    public let schema: String?
    public let table: String?
    public let column: String

    public init(database: String? = nil, schema: String? = nil, table: String? = nil, column: String) {
        self.database = database
        self.schema = schema
        self.table = table
        self.column = column
    }
}

// MARK: - ShowPlanMissingIndex

/// A missing index suggestion from the query optimizer.
public struct ShowPlanMissingIndex: Sendable {
    public let impact: Double?
    public let database: String?
    public let schema: String?
    public let table: String?
    public let equalityColumns: [String]
    public let inequalityColumns: [String]
    public let includeColumns: [String]

    public init(
        impact: Double? = nil,
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        equalityColumns: [String] = [],
        inequalityColumns: [String] = [],
        includeColumns: [String] = []
    ) {
        self.impact = impact
        self.database = database
        self.schema = schema
        self.table = table
        self.equalityColumns = equalityColumns
        self.inequalityColumns = inequalityColumns
        self.includeColumns = includeColumns
    }
}
