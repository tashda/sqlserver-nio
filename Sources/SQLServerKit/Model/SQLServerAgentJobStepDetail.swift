import Foundation

/// Enhanced job step information with all details
public struct SQLServerAgentJobStepDetail: Sendable {
    public let stepId: Int
    public let name: String
    public let subsystem: String
    public let command: String?
    public let databaseName: String?
    public let onSuccessAction: Int?
    public let onSuccessStepId: Int?
    public let onFailureAction: Int?
    public let onFailureStepId: Int?
    public let retryAttempts: Int?
    public let retryIntervalMinutes: Int?

    public init(
        stepId: Int,
        name: String,
        subsystem: String,
        command: String? = nil,
        databaseName: String? = nil,
        onSuccessAction: Int? = nil,
        onSuccessStepId: Int? = nil,
        onFailureAction: Int? = nil,
        onFailureStepId: Int? = nil,
        retryAttempts: Int? = nil,
        retryIntervalMinutes: Int? = nil
    ) {
        self.stepId = stepId
        self.name = name
        self.subsystem = subsystem
        self.command = command
        self.databaseName = databaseName
        self.onSuccessAction = onSuccessAction
        self.onSuccessStepId = onSuccessStepId
        self.onFailureAction = onFailureAction
        self.onFailureStepId = onFailureStepId
        self.retryAttempts = retryAttempts
        self.retryIntervalMinutes = retryIntervalMinutes
    }
}
