import Foundation
import NIO

// MARK: - SQLServerExecutionPlanClient

/// Namespace client for retrieving SQL Server execution plans.
///
/// Usage:
/// ```swift
/// let plan = try await client.executionPlan.estimated("SELECT * FROM Users")
/// print(plan.statements.first?.queryPlan?.rootOperator?.physicalOp)
/// ```
public final class SQLServerExecutionPlanClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Estimated Plans

    /// Returns the estimated execution plan XML for the given SQL without executing the query.
    @available(macOS 12.0, *)
    public func estimatedXML(_ sql: String) async throws -> String {
        try await client.withConnection { connection in
            _ = try await connection.execute("SET SHOWPLAN_XML ON")
            do {
                let result = try await connection.execute(sql)
                _ = try await connection.execute("SET SHOWPLAN_XML OFF")
                guard let xml = result.rows.first?.values.first?.string else {
                    throw ExecutionPlanError.noPlanReturned
                }
                return xml
            } catch {
                _ = try? await connection.execute("SET SHOWPLAN_XML OFF")
                throw error
            }
        }
    }

    /// Returns the parsed estimated execution plan for the given SQL without executing the query.
    @available(macOS 12.0, *)
    public func estimated(_ sql: String) async throws -> ShowPlan {
        let xml = try await estimatedXML(sql)
        return try ShowPlanXMLParser.parse(xml: xml)
    }

    // MARK: - Actual Plans

    /// Returns the actual execution plan XML along with the query results.
    /// The query IS executed. Plan XML includes runtime metrics (actual rows, elapsed time, etc.).
    @available(macOS 12.0, *)
    public func actualXML(_ sql: String) async throws -> (result: SQLServerExecutionResult, xml: String) {
        try await client.withConnection { connection in
            _ = try await connection.execute("SET STATISTICS XML ON")
            do {
                let result = try await connection.execute(sql)
                _ = try await connection.execute("SET STATISTICS XML OFF")

                // Separate plan result sets from data result sets.
                // SQL Server appends a single-row plan result set after each statement's data.
                // Plan rows contain XML starting with "<ShowPlanXML".
                var planXMLs: [String] = []
                var dataRows: [SQLServerRow] = []

                for row in result.rows {
                    if let value = row.values.first?.string,
                       value.hasPrefix("<ShowPlanXML") || value.contains("<ShowPlanXML") {
                        planXMLs.append(value)
                    } else {
                        dataRows.append(row)
                    }
                }

                guard let xml = planXMLs.last else {
                    throw ExecutionPlanError.noPlanReturned
                }

                let cleanResult = SQLServerExecutionResult(
                    rows: dataRows,
                    done: result.done,
                    messages: result.messages
                )
                return (result: cleanResult, xml: xml)
            } catch {
                _ = try? await connection.execute("SET STATISTICS XML OFF")
                throw error
            }
        }
    }

    /// Returns the parsed actual execution plan along with the query results.
    /// The query IS executed. Plan includes runtime metrics (actual rows, elapsed time, etc.).
    @available(macOS 12.0, *)
    public func actual(_ sql: String) async throws -> (result: SQLServerExecutionResult, plan: ShowPlan) {
        let (result, xml) = try await actualXML(sql)
        let plan = try ShowPlanXMLParser.parse(xml: xml)
        return (result: result, plan: plan)
    }
}

// MARK: - ExecutionPlanError

public enum ExecutionPlanError: Error, Sendable {
    case noPlanReturned
}
