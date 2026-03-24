import Foundation
import NIO

// MARK: - Query Store Types

/// Configuration and status of Query Store for a database.
public struct SQLServerQueryStoreOptions: Sendable, Equatable {
    public let actualState: String
    public let desiredState: String
    public let currentStorageSizeMB: Int
    public let maxStorageSizeMB: Int
    public let staleQueryThresholdDays: Int
    public let flushIntervalSeconds: Int
    public let intervalLengthMinutes: Int
    public let maxPlansPerQuery: Int
    public let queryCaptureMode: String
    public let sizeBasedCleanupMode: String
    public let waitStatsCaptureMode: String

    // CUSTOM capture policy (SQL Server 2019+, only relevant when queryCaptureMode == "CUSTOM")
    /// Execution count threshold for custom capture. Default 30.
    public let captureExecutionCount: Int
    /// Total compile CPU time threshold in ms. Default 1000.
    public let captureCompileCpuTimeMs: Int
    /// Total execution CPU time threshold in ms. Default 100.
    public let captureExecutionCpuTimeMs: Int
    /// Stale capture policy threshold in hours. Default 24.
    public let captureStalePolicyThresholdHours: Int

    public init(
        actualState: String,
        desiredState: String,
        currentStorageSizeMB: Int,
        maxStorageSizeMB: Int,
        staleQueryThresholdDays: Int,
        flushIntervalSeconds: Int,
        intervalLengthMinutes: Int = 60,
        maxPlansPerQuery: Int = 200,
        queryCaptureMode: String = "ALL",
        sizeBasedCleanupMode: String = "AUTO",
        waitStatsCaptureMode: String = "ON",
        captureExecutionCount: Int = 30,
        captureCompileCpuTimeMs: Int = 1000,
        captureExecutionCpuTimeMs: Int = 100,
        captureStalePolicyThresholdHours: Int = 24
    ) {
        self.actualState = actualState
        self.desiredState = desiredState
        self.currentStorageSizeMB = currentStorageSizeMB
        self.maxStorageSizeMB = maxStorageSizeMB
        self.staleQueryThresholdDays = staleQueryThresholdDays
        self.flushIntervalSeconds = flushIntervalSeconds
        self.intervalLengthMinutes = intervalLengthMinutes
        self.maxPlansPerQuery = maxPlansPerQuery
        self.queryCaptureMode = queryCaptureMode
        self.sizeBasedCleanupMode = sizeBasedCleanupMode
        self.waitStatsCaptureMode = waitStatsCaptureMode
        self.captureExecutionCount = captureExecutionCount
        self.captureCompileCpuTimeMs = captureCompileCpuTimeMs
        self.captureExecutionCpuTimeMs = captureExecutionCpuTimeMs
        self.captureStalePolicyThresholdHours = captureStalePolicyThresholdHours
    }

    /// Whether Query Store is currently active and collecting data.
    public var isActive: Bool {
        actualState.uppercased() == "READ_WRITE"
    }

    /// Whether Query Store is in read-only mode.
    public var isReadOnly: Bool {
        actualState.uppercased() == "READ_ONLY"
    }

    /// Whether Query Store is off.
    public var isOff: Bool {
        actualState.uppercased() == "OFF"
    }
}

/// An individual Query Store setting that can be altered.
public enum SQLServerQueryStoreOption: Sendable {
    case desiredState(QueryStoreDesiredState)
    case maxStorageSizeMB(Int)
    case intervalLengthMinutes(Int)
    case staleQueryThresholdDays(Int)
    case flushIntervalSeconds(Int)
    case maxPlansPerQuery(Int)
    case queryCaptureMode(QueryStoreCaptureMode)
    case sizeBasedCleanupMode(QueryStoreCleanupMode)
    case waitStatsCaptureMode(QueryStoreWaitStatsMode)
    case customCapturePolicy(executionCount: Int, compileCpuTimeMs: Int, executionCpuTimeMs: Int, stalePolicyThresholdHours: Int)
}

/// Desired operational state for Query Store.
public enum QueryStoreDesiredState: String, Sendable, CaseIterable {
    case off = "OFF"
    case readOnly = "READ_ONLY"
    case readWrite = "READ_WRITE"
}

/// Query capture mode for Query Store.
public enum QueryStoreCaptureMode: String, Sendable, CaseIterable {
    case all = "ALL"
    case auto = "AUTO"
    case none = "NONE"
    case custom = "CUSTOM"
}

/// Size-based cleanup mode for Query Store.
public enum QueryStoreCleanupMode: String, Sendable, CaseIterable {
    case auto = "AUTO"
    case off = "OFF"
}

/// Wait stats capture mode for Query Store.
public enum QueryStoreWaitStatsMode: String, Sendable, CaseIterable {
    case on = "ON"
    case off = "OFF"
}

/// Ordering options for top queries.
public enum SQLServerQueryStoreTopQueryOrder: String, Sendable {
    case totalDuration = "total_duration"
    case totalCPU = "total_cpu"
    case totalIOReads = "total_io_reads"
    case totalExecutions = "total_executions"
}

/// A top resource-consuming query from Query Store.
public struct SQLServerQueryStoreTopQuery: Sendable, Equatable, Identifiable {
    public var id: Int { queryId }

    public let queryId: Int
    public let queryText: String
    public let totalExecutions: Int
    public let totalDurationUs: Double
    public let totalCPUUs: Double
    public let totalIOReads: Double
    public let avgDurationUs: Double
    public let avgCPUUs: Double

    public init(
        queryId: Int,
        queryText: String,
        totalExecutions: Int,
        totalDurationUs: Double,
        totalCPUUs: Double,
        totalIOReads: Double,
        avgDurationUs: Double,
        avgCPUUs: Double
    ) {
        self.queryId = queryId
        self.queryText = queryText
        self.totalExecutions = totalExecutions
        self.totalDurationUs = totalDurationUs
        self.totalCPUUs = totalCPUUs
        self.totalIOReads = totalIOReads
        self.avgDurationUs = avgDurationUs
        self.avgCPUUs = avgCPUUs
    }
}

/// An execution plan associated with a query in Query Store.
public struct SQLServerQueryStorePlan: Sendable, Equatable, Identifiable {
    public var id: Int { planId }

    public let queryId: Int
    public let planId: Int
    public let isForcedPlan: Bool
    public let avgDurationUs: Double
    public let avgCPUUs: Double
    public let avgIOReads: Double
    public let executionCount: Int
    public let lastExecutionTime: Date?
    public let planXml: String?

    public init(
        queryId: Int,
        planId: Int,
        isForcedPlan: Bool,
        avgDurationUs: Double,
        avgCPUUs: Double,
        avgIOReads: Double,
        executionCount: Int,
        lastExecutionTime: Date?,
        planXml: String?
    ) {
        self.queryId = queryId
        self.planId = planId
        self.isForcedPlan = isForcedPlan
        self.avgDurationUs = avgDurationUs
        self.avgCPUUs = avgCPUUs
        self.avgIOReads = avgIOReads
        self.executionCount = executionCount
        self.lastExecutionTime = lastExecutionTime
        self.planXml = planXml
    }
}

/// A regressed query — one where newer plans perform worse than older ones.
public struct SQLServerQueryStoreRegressedQuery: Sendable, Equatable, Identifiable {
    public var id: Int { queryId }

    public let queryId: Int
    public let queryText: String
    public let planCount: Int
    public let minAvgDurationUs: Double
    public let maxAvgDurationUs: Double
    public let regressionRatio: Double

    public init(
        queryId: Int,
        queryText: String,
        planCount: Int,
        minAvgDurationUs: Double,
        maxAvgDurationUs: Double,
        regressionRatio: Double
    ) {
        self.queryId = queryId
        self.queryText = queryText
        self.planCount = planCount
        self.minAvgDurationUs = minAvgDurationUs
        self.maxAvgDurationUs = maxAvgDurationUs
        self.regressionRatio = regressionRatio
    }
}

// MARK: - SQLServerQueryStoreClient

/// Namespace client for SQL Server Query Store operations.
///
/// Query Store captures query execution history, plans, and runtime statistics.
/// This client provides typed APIs for retrieving Query Store data and managing
/// forced plans.
///
/// Usage:
/// ```swift
/// let options = try await client.queryStore.options(database: "MyDB")
/// let topQueries = try await client.queryStore.topQueries(database: "MyDB")
/// ```
public final class SQLServerQueryStoreClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    private static func formatDate(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate, .withTime, .withColonSeparatorInTime]
        return formatter.string(from: date)
    }

    // MARK: - Options

    /// Returns Query Store configuration and status for a database.
    @available(macOS 12.0, *)
    public func options(database: String) async throws -> SQLServerQueryStoreOptions {
        let sql = """
        SELECT
            actual_state_desc,
            desired_state_desc,
            CAST(current_storage_size_mb AS INT) AS current_storage_size_mb,
            CAST(max_storage_size_mb AS INT) AS max_storage_size_mb,
            CAST(stale_query_threshold_days AS INT) AS stale_query_threshold_days,
            CAST(flush_interval_seconds AS INT) AS flush_interval_seconds,
            CAST(interval_length_minutes AS INT) AS interval_length_minutes,
            CAST(max_plans_per_query AS INT) AS max_plans_per_query,
            query_capture_mode_desc,
            size_based_cleanup_mode_desc,
            wait_stats_capture_mode_desc,
            CAST(ISNULL(capture_policy_execution_count, 30) AS INT) AS capture_policy_execution_count,
            CAST(ISNULL(capture_policy_total_compile_cpu_time_ms, 1000) AS INT) AS capture_policy_total_compile_cpu_time_ms,
            CAST(ISNULL(capture_policy_total_execution_cpu_time_ms, 100) AS INT) AS capture_policy_total_execution_cpu_time_ms,
            CAST(ISNULL(capture_policy_stale_threshold_hours, 24) AS INT) AS capture_policy_stale_threshold_hours
        FROM sys.database_query_store_options
        """
        let rows = try await client.withDatabase(database) { connection in
            try await connection.query(sql)
        }
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Query Store is not available for database '\(database)'")
        }
        return SQLServerQueryStoreOptions(
            actualState: row.column("actual_state_desc")?.string ?? "OFF",
            desiredState: row.column("desired_state_desc")?.string ?? "OFF",
            currentStorageSizeMB: row.column("current_storage_size_mb")?.int ?? 0,
            maxStorageSizeMB: row.column("max_storage_size_mb")?.int ?? 0,
            staleQueryThresholdDays: row.column("stale_query_threshold_days")?.int ?? 0,
            flushIntervalSeconds: row.column("flush_interval_seconds")?.int ?? 0,
            intervalLengthMinutes: row.column("interval_length_minutes")?.int ?? 60,
            maxPlansPerQuery: row.column("max_plans_per_query")?.int ?? 200,
            queryCaptureMode: row.column("query_capture_mode_desc")?.string ?? "ALL",
            sizeBasedCleanupMode: row.column("size_based_cleanup_mode_desc")?.string ?? "AUTO",
            waitStatsCaptureMode: row.column("wait_stats_capture_mode_desc")?.string ?? "ON",
            captureExecutionCount: row.column("capture_policy_execution_count")?.int ?? 30,
            captureCompileCpuTimeMs: row.column("capture_policy_total_compile_cpu_time_ms")?.int ?? 1000,
            captureExecutionCpuTimeMs: row.column("capture_policy_total_execution_cpu_time_ms")?.int ?? 100,
            captureStalePolicyThresholdHours: row.column("capture_policy_stale_threshold_hours")?.int ?? 24
        )
    }

    // MARK: - Alter Options

    /// Alters a Query Store setting for a database.
    @available(macOS 12.0, *)
    public func alterOption(database: String, option: SQLServerQueryStoreOption) async throws {
        let escapedDB = database.replacingOccurrences(of: "]", with: "]]")
        let setting: String
        switch option {
        case .desiredState(let state):
            setting = "OPERATION_MODE = \(state.rawValue)"
        case .maxStorageSizeMB(let mb):
            setting = "MAX_STORAGE_SIZE_MB = \(mb)"
        case .intervalLengthMinutes(let mins):
            setting = "INTERVAL_LENGTH_MINUTES = \(mins)"
        case .staleQueryThresholdDays(let days):
            setting = "STALE_QUERY_THRESHOLD_DAYS = \(days)"
        case .flushIntervalSeconds(let secs):
            setting = "DATA_FLUSH_INTERVAL_SECONDS = \(secs)"
        case .maxPlansPerQuery(let count):
            setting = "MAX_PLANS_PER_QUERY = \(count)"
        case .queryCaptureMode(let mode):
            setting = "QUERY_CAPTURE_MODE = \(mode.rawValue)"
        case .sizeBasedCleanupMode(let mode):
            setting = "SIZE_BASED_CLEANUP_MODE = \(mode.rawValue)"
        case .waitStatsCaptureMode(let mode):
            setting = "WAIT_STATS_CAPTURE_MODE = \(mode.rawValue)"
        case .customCapturePolicy(let execCount, let compileCpu, let execCpu, let staleHours):
            setting = """
            QUERY_CAPTURE_MODE = CUSTOM, \
            QUERY_CAPTURE_POLICY = (STALE_CAPTURE_POLICY_THRESHOLD = \(staleHours) HOURS, \
            EXECUTION_COUNT = \(execCount), \
            TOTAL_COMPILE_CPU_TIME_MS = \(compileCpu), \
            TOTAL_EXECUTION_CPU_TIME_MS = \(execCpu))
            """
        }
        let sql = "ALTER DATABASE [\(escapedDB)] SET QUERY_STORE (\(setting))"
        _ = try await client.execute(sql)
    }

    /// Enables or disables Query Store for a database.
    @available(macOS 12.0, *)
    public func setEnabled(database: String, enabled: Bool) async throws {
        let escapedDB = database.replacingOccurrences(of: "]", with: "]]")
        let state = enabled ? "ON" : "OFF"
        let sql = "ALTER DATABASE [\(escapedDB)] SET QUERY_STORE = \(state)"
        _ = try await client.execute(sql)
    }

    /// Purges all Query Store data for a database.
    @available(macOS 12.0, *)
    public func purgeData(database: String) async throws {
        let escapedDB = database.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER DATABASE [\(escapedDB)] SET QUERY_STORE CLEAR"
        _ = try await client.execute(sql)
    }

    // MARK: - Top Queries

    /// Returns the top resource-consuming queries from Query Store.
    @available(macOS 12.0, *)
    public func topQueries(
        database: String,
        limit: Int = 20,
        orderBy: SQLServerQueryStoreTopQueryOrder = .totalDuration,
        startDate: Date? = nil,
        endDate: Date? = nil,
        minExecutionCount: Int? = nil,
        queryTextFilter: String? = nil
    ) async throws -> [SQLServerQueryStoreTopQuery] {
        let orderColumn: String
        switch orderBy {
        case .totalDuration: orderColumn = "total_duration"
        case .totalCPU: orderColumn = "total_cpu"
        case .totalIOReads: orderColumn = "total_io_reads"
        case .totalExecutions: orderColumn = "total_executions"
        }

        var whereClauses: [String] = []
        if let startDate {
            let formatted = Self.formatDate(startDate)
            whereClauses.append("rsi.start_time >= '\(formatted)'")
        }
        if let endDate {
            let formatted = Self.formatDate(endDate)
            whereClauses.append("rsi.end_time <= '\(formatted)'")
        }
        if let filter = queryTextFilter, !filter.isEmpty {
            let escaped = filter.replacingOccurrences(of: "'", with: "''")
            whereClauses.append("qt.query_sql_text LIKE N'%\(escaped)%'")
        }

        let whereClause = whereClauses.isEmpty ? "" : "WHERE " + whereClauses.joined(separator: " AND ")
        let havingClause = minExecutionCount.map { "HAVING SUM(rs.count_executions) >= \($0)" } ?? ""
        let needsInterval = startDate != nil || endDate != nil

        let sql = """
        SELECT TOP (\(limit))
            q.query_id,
            qt.query_sql_text,
            SUM(rs.count_executions) AS total_executions,
            SUM(rs.avg_duration * rs.count_executions) AS total_duration,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_io_reads,
            AVG(rs.avg_duration) AS avg_duration,
            AVG(rs.avg_cpu_time) AS avg_cpu
        FROM sys.query_store_query q
        JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
        JOIN sys.query_store_plan p ON q.query_id = p.query_id
        JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
        \(needsInterval ? "JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id" : "")
        \(whereClause)
        GROUP BY q.query_id, qt.query_sql_text
        \(havingClause)
        ORDER BY \(orderColumn) DESC
        """

        let rows = try await client.withDatabase(database) { connection in
            try await connection.query(sql)
        }

        return rows.compactMap { row in
            guard let queryId = row.column("query_id")?.int,
                  let queryText = row.column("query_sql_text")?.string else { return nil }
            return SQLServerQueryStoreTopQuery(
                queryId: queryId,
                queryText: queryText,
                totalExecutions: row.column("total_executions")?.int ?? 0,
                totalDurationUs: row.column("total_duration")?.double ?? 0,
                totalCPUUs: row.column("total_cpu")?.double ?? 0,
                totalIOReads: row.column("total_io_reads")?.double ?? 0,
                avgDurationUs: row.column("avg_duration")?.double ?? 0,
                avgCPUUs: row.column("avg_cpu")?.double ?? 0
            )
        }
    }

    // MARK: - Query Plans

    /// Returns execution plans for a specific query.
    @available(macOS 12.0, *)
    public func queryPlans(database: String, queryId: Int) async throws -> [SQLServerQueryStorePlan] {
        let sql = """
        SELECT
            p.query_id,
            p.plan_id,
            p.is_forced_plan,
            AVG(rs.avg_duration) AS avg_duration,
            AVG(rs.avg_cpu_time) AS avg_cpu_time,
            AVG(rs.avg_logical_io_reads) AS avg_logical_io_reads,
            SUM(rs.count_executions) AS count_executions,
            MAX(rs.last_execution_time) AS last_execution_time,
            CAST(p.query_plan AS NVARCHAR(MAX)) AS query_plan_xml
        FROM sys.query_store_plan p
        JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
        WHERE p.query_id = \(queryId)
        GROUP BY p.query_id, p.plan_id, p.is_forced_plan, CAST(p.query_plan AS NVARCHAR(MAX))
        ORDER BY MAX(rs.last_execution_time) DESC
        """

        let rows = try await client.withDatabase(database) { connection in
            try await connection.query(sql)
        }

        return rows.compactMap { row in
            guard let planId = row.column("plan_id")?.int else { return nil }
            return SQLServerQueryStorePlan(
                queryId: queryId,
                planId: planId,
                isForcedPlan: row.column("is_forced_plan")?.bool ?? false,
                avgDurationUs: row.column("avg_duration")?.double ?? 0,
                avgCPUUs: row.column("avg_cpu_time")?.double ?? 0,
                avgIOReads: row.column("avg_logical_io_reads")?.double ?? 0,
                executionCount: row.column("count_executions")?.int ?? 0,
                lastExecutionTime: row.column("last_execution_time")?.date,
                planXml: row.column("query_plan_xml")?.string
            )
        }
    }

    // MARK: - Regressed Queries

    /// Returns queries that have regressed — multiple plans where the worst plan is
    /// significantly slower than the best.
    @available(macOS 12.0, *)
    public func regressedQueries(
        database: String,
        regressionThreshold: Double = 2.0,
        limit: Int = 20
    ) async throws -> [SQLServerQueryStoreRegressedQuery] {
        let sql = """
        SELECT TOP (\(limit))
            q.query_id,
            qt.query_sql_text,
            COUNT(DISTINCT p.plan_id) AS plan_count,
            MIN(rs.avg_duration) AS min_avg_duration,
            MAX(rs.avg_duration) AS max_avg_duration,
            CASE WHEN MIN(rs.avg_duration) > 0
                 THEN MAX(rs.avg_duration) / MIN(rs.avg_duration)
                 ELSE 0 END AS regression_ratio
        FROM sys.query_store_query q
        JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
        JOIN sys.query_store_plan p ON q.query_id = p.query_id
        JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
        GROUP BY q.query_id, qt.query_sql_text
        HAVING COUNT(DISTINCT p.plan_id) > 1
           AND CASE WHEN MIN(rs.avg_duration) > 0
                    THEN MAX(rs.avg_duration) / MIN(rs.avg_duration)
                    ELSE 0 END >= \(regressionThreshold)
        ORDER BY regression_ratio DESC
        """

        let rows = try await client.withDatabase(database) { connection in
            try await connection.query(sql)
        }

        return rows.compactMap { row in
            guard let queryId = row.column("query_id")?.int,
                  let queryText = row.column("query_sql_text")?.string else { return nil }
            return SQLServerQueryStoreRegressedQuery(
                queryId: queryId,
                queryText: queryText,
                planCount: row.column("plan_count")?.int ?? 0,
                minAvgDurationUs: row.column("min_avg_duration")?.double ?? 0,
                maxAvgDurationUs: row.column("max_avg_duration")?.double ?? 0,
                regressionRatio: row.column("regression_ratio")?.double ?? 0
            )
        }
    }

    // MARK: - Plan Forcing

    /// Forces a specific execution plan for a query.
    @available(macOS 12.0, *)
    public func forcePlan(database: String, queryId: Int, planId: Int) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(
                "EXEC sp_query_store_force_plan @query_id = \(queryId), @plan_id = \(planId)"
            )
        }
    }

    /// Removes a forced execution plan for a query.
    @available(macOS 12.0, *)
    public func unforcePlan(database: String, queryId: Int, planId: Int) async throws {
        try await client.withDatabase(database) { connection in
            _ = try await connection.execute(
                "EXEC sp_query_store_unforce_plan @query_id = \(queryId), @plan_id = \(planId)"
            )
        }
    }
}
