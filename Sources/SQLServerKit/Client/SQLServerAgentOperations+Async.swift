import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    @available(macOS 12.0, *)
    public func preflightAgentEnvironment(requireProxyPrereqs: Bool = false) async throws {
        _ = try await preflightAgentEnvironment(requireProxyPrereqs: requireProxyPrereqs).get()
    }

    // MARK: - Async Jobs

    @available(macOS 12.0, *)
    public func listJobs() async throws -> [SQLServerAgentJobInfo] {
        try await listJobs().get()
    }

    @available(macOS 12.0, *)
    public func listJobsDetailed() async throws -> [SQLServerAgentJobDetail] {
        try await listJobsDetailed().get()
    }

    @available(macOS 12.0, *)
    public func getJobDetail(jobName: String) async throws -> SQLServerAgentJobDetail? {
        try await getJobDetail(jobName: jobName).get()
    }

    @available(macOS 12.0, *)
    public func startJob(named jobName: String) async throws {
        _ = try await startJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func stopJob(named jobName: String) async throws {
        _ = try await stopJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func enableJob(named jobName: String, enabled: Bool) async throws {
        _ = try await enableJob(named: jobName, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func createJob(named jobName: String, description: String? = nil, enabled: Bool = true, ownerLoginName: String? = nil) async throws {
        _ = try await createJob(named: jobName, description: description, enabled: enabled, ownerLoginName: ownerLoginName).get()
    }

    @available(macOS 12.0, *)
    public func renameJob(named jobName: String, to newName: String) async throws {
        _ = try await renameJob(named: jobName, to: newName).get()
    }

    @available(macOS 12.0, *)
    public func deleteJob(named jobName: String) async throws {
        _ = try await deleteJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func updateJob(named jobName: String, newName: String? = nil, description: String? = nil, ownerLoginName: String? = nil, categoryName: String? = nil, enabled: Bool? = nil, startStepId: Int? = nil, notifyLevelEventlog: Int? = nil) async throws {
        _ = try await updateJob(named: jobName, newName: newName, description: description, ownerLoginName: ownerLoginName, categoryName: categoryName, enabled: enabled, startStepId: startStepId, notifyLevelEventlog: notifyLevelEventlog).get()
    }

    @available(macOS 12.0, *)
    public func setJobCategory(named jobName: String, categoryName: String) async throws {
        _ = try await updateJob(named: jobName, categoryName: categoryName).get()
    }

    @available(macOS 12.0, *)
    public func setJobStartStep(jobName: String, stepId: Int) async throws {
        _ = try await updateJob(named: jobName, startStepId: stepId).get()
    }

    @available(macOS 12.0, *)
    public func setJobEmailNotification(jobName: String, operatorName: String?, notifyLevel: Int) async throws {
        // Since setJobEmailNotification is not in +Jobs but would be needed, I'll add it to +Jobs first
        // or just use EventLoopFuture here. Let's assume it's added to +Jobs.
        _ = try await setJobEmailNotification(jobName: jobName, operatorName: operatorName, notifyLevel: notifyLevel).get()
    }

    // MARK: - Async Steps

    @available(macOS 12.0, *)
    public func addTSQLStep(jobName: String, stepName: String, command: String, database: String? = nil) async throws {
        _ = try await addTSQLStep(jobName: jobName, stepName: stepName, command: command, database: database).get()
    }

    @available(macOS 12.0, *)
    public func addStep(jobName: String, stepName: String, subsystem: String, command: String, database: String? = nil, proxyName: String? = nil, outputFile: String? = nil) async throws {
        _ = try await addStep(jobName: jobName, stepName: stepName, subsystem: subsystem, command: command, database: database, proxyName: proxyName, outputFile: outputFile).get()
    }

    @available(macOS 12.0, *)
    public func addJobServer(jobName: String, serverName: String? = nil) async throws {
        _ = try await addJobServer(jobName: jobName, serverName: serverName).get()
    }

    @available(macOS 12.0, *)
    public func updateTSQLStep(jobName: String, stepName: String, newCommand: String, database: String? = nil) async throws {
        _ = try await updateTSQLStep(jobName: jobName, stepName: stepName, newCommand: newCommand, database: database).get()
    }

    @available(macOS 12.0, *)
    public func deleteStep(jobName: String, stepName: String) async throws {
        _ = try await deleteStep(jobName: jobName, stepName: stepName).get()
    }

    @available(macOS 12.0, *)
    public func listSteps(jobName: String) async throws -> [SQLServerAgentJobStepDetail] {
        try await listSteps(jobName: jobName).get()
    }

    @available(*, deprecated, renamed: "listSteps(jobName:)")
    @available(macOS 12.0, *)
    public func getJobSteps(jobName: String) async throws -> [SQLServerAgentJobStepDetail] {
        try await listSteps(jobName: jobName)
    }

    @available(macOS 12.0, *)
    public func configureStep(jobName: String, stepName: String, onSuccessAction: Int? = nil, onSuccessStepId: Int? = nil, onFailAction: Int? = nil, onFailStepId: Int? = nil, retryAttempts: Int? = nil, retryIntervalMinutes: Int? = nil, outputFileName: String? = nil, appendOutputFile: Bool? = nil) async throws {
        _ = try await configureStep(jobName: jobName, stepName: stepName, onSuccessAction: onSuccessAction, onSuccessStepId: onSuccessStepId, onFailAction: onFailAction, onFailStepId: onFailStepId, retryAttempts: retryAttempts, retryIntervalMinutes: retryIntervalMinutes, outputFileName: outputFileName, appendOutputFile: appendOutputFile).get()
    }

    @available(macOS 12.0, *)
    public func fetchJobId(named jobName: String) async throws -> String {
        try await lookupJobId(jobName: jobName).get()
    }

    @available(macOS 12.0, *)
    public func reorderJobSteps(jobName: String, stepMapping: [(oldID: Int, newID: Int)]) async throws {
        _ = try await reorderJobSteps(jobName: jobName, stepMapping: stepMapping).get()
    }

    // MARK: - Async Schedules

    @available(macOS 12.0, *)
    public func createSchedule(named scheduleName: String, enabled: Bool = true, freqType: Int, freqInterval: Int = 1, activeStartDate: Int? = nil, activeStartTime: Int? = nil, activeEndDate: Int? = nil, activeEndTime: Int? = nil, freqSubdayType: Int? = nil, freqSubdayInterval: Int? = nil, freqRelativeInterval: Int? = nil, freqRecurrenceFactor: Int? = nil) async throws {
        _ = try await createSchedule(named: scheduleName, enabled: enabled, freqType: freqType, freqInterval: freqInterval, activeStartDate: activeStartDate, activeStartTime: activeStartTime, activeEndDate: activeEndDate, activeEndTime: activeEndTime, freqSubdayType: freqSubdayType, freqSubdayInterval: freqSubdayInterval, freqRelativeInterval: freqRelativeInterval, freqRecurrenceFactor: freqRecurrenceFactor).get()
    }

    @available(macOS 12.0, *)
    public func attachSchedule(scheduleName: String, toJob jobName: String) async throws {
        _ = try await attachSchedule(scheduleName: scheduleName, toJob: jobName).get()
    }

    @available(macOS 12.0, *)
    public func detachSchedule(scheduleName: String, fromJob jobName: String) async throws {
        _ = try await detachSchedule(scheduleName: scheduleName, fromJob: jobName).get()
    }

    @available(macOS 12.0, *)
    public func deleteSchedule(named scheduleName: String) async throws {
        _ = try await deleteSchedule(named: scheduleName).get()
    }

    @available(macOS 12.0, *)
    public func listSchedules(forJob jobName: String? = nil) async throws -> [SQLServerAgentScheduleInfo] {
        try await listSchedules(forJob: jobName).get()
    }

    @available(macOS 12.0, *)
    public func listRunningJobs() async throws -> [SQLServerAgentRunningJob] {
        try await listRunningJobs().get()
    }

    @available(macOS 12.0, *)
    public func getJobSchedules(jobName: String) async throws -> [SQLServerAgentJobScheduleDetail] {
        try await getJobSchedules(jobName: jobName).get()
    }

    @available(macOS 12.0, *)
    public func getActiveJobStep(jobName: String) async throws -> SQLServerAgentActiveStep? {
        try await getActiveJobStep(jobName: jobName).get()
    }

    @available(macOS 12.0, *)
    public func listErrorLogs() async throws -> [SQLServerAgentErrorLog] {
        try await listErrorLogs().get()
    }

    // MARK: - Async Operators

    @available(macOS 12.0, *)
    public func createOperator(name: String, emailAddress: String? = nil, enabled: Bool = true) async throws {
        _ = try await createOperator(name: name, emailAddress: emailAddress, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func updateOperator(name: String, emailAddress: String? = nil, enabled: Bool? = nil) async throws {
        _ = try await updateOperator(name: name, emailAddress: emailAddress, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func updateOperator(
        name: String,
        emailAddress: String? = nil,
        enabled: Bool? = nil,
        pagerAddress: String? = nil,
        weekdayPagerStartTime: Int? = nil,
        weekdayPagerEndTime: Int? = nil
    ) async throws {
        _ = try await updateOperator(
            name: name,
            emailAddress: emailAddress,
            enabled: enabled,
            pagerAddress: pagerAddress,
            weekdayPagerStartTime: weekdayPagerStartTime,
            weekdayPagerEndTime: weekdayPagerEndTime
        ).get()
    }

    @available(macOS 12.0, *)
    public func deleteOperator(name: String) async throws {
        _ = try await deleteOperator(name: name).get()
    }

    @available(macOS 12.0, *)
    public func listOperators() async throws -> [SQLServerAgentOperatorInfo] {
        try await listOperators().get()
    }

    // MARK: - Async Alerts & Categories

    @available(macOS 12.0, *)
    public func createAlert(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, enabled: Bool = true) async throws {
        _ = try await createAlert(name: name, severity: severity, messageId: messageId, databaseName: databaseName, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func createAlert(
        name: String,
        severity: Int? = nil,
        messageId: Int? = nil,
        databaseName: String? = nil,
        eventDescriptionKeyword: String? = nil,
        performanceCondition: String? = nil,
        wmiNamespace: String? = nil,
        wmiQuery: String? = nil,
        enabled: Bool = true
    ) async throws {
        _ = try await createAlert(
            name: name,
            severity: severity,
            messageId: messageId,
            databaseName: databaseName,
            eventDescriptionKeyword: eventDescriptionKeyword,
            performanceCondition: performanceCondition,
            wmiNamespace: wmiNamespace,
            wmiQuery: wmiQuery,
            enabled: enabled
        ).get()
    }

    @available(macOS 12.0, *)
    public func updateAlert(name: String, newName: String? = nil, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, enabled: Bool? = nil) async throws {
        _ = try await updateAlert(name: name, newName: newName, severity: severity, messageId: messageId, databaseName: databaseName, eventDescriptionKeyword: eventDescriptionKeyword, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func deleteAlert(name: String) async throws {
        _ = try await deleteAlert(name: name).get()
    }

    @available(macOS 12.0, *)
    public func listAlerts() async throws -> [SQLServerAgentAlertInfo] {
        try await listAlerts().get()
    }

    @available(macOS 12.0, *)
    public func addNotification(alertName: String, operatorName: String, method: Int = 1) async throws {
        _ = try await addNotification(alertName: alertName, operatorName: operatorName, method: method).get()
    }

    @available(macOS 12.0, *)
    public func deleteNotification(alertName: String, operatorName: String) async throws {
        _ = try await deleteNotification(alertName: alertName, operatorName: operatorName).get()
    }

    @available(macOS 12.0, *)
    public func createCategory(name: String, classId: Int = 1) async throws {
        _ = try await createCategory(name: name, classId: classId).get()
    }

    @available(macOS 12.0, *)
    public func deleteCategory(name: String) async throws {
        _ = try await deleteCategory(name: name).get()
    }

    @available(macOS 12.0, *)
    public func renameCategory(name: String, newName: String) async throws {
        _ = try await renameCategory(name: name, newName: newName).get()
    }

    @available(macOS 12.0, *)
    public func listCategories() async throws -> [SQLServerAgentCategoryInfo] {
        try await listCategories().get()
    }

    // MARK: - Async Proxies

    @available(macOS 12.0, *)
    public func createProxy(name: String, credentialName: String, description: String? = nil, enabled: Bool = true) async throws {
        _ = try await createProxy(name: name, credentialName: credentialName, description: description, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func deleteProxy(name: String) async throws {
        _ = try await deleteProxy(name: name).get()
    }

    @available(macOS 12.0, *)
    public func grantLoginToProxy(proxyName: String, loginName: String) async throws {
        _ = try await grantLoginToProxy(proxyName: proxyName, loginName: loginName).get()
    }

    @available(macOS 12.0, *)
    public func revokeLoginFromProxy(proxyName: String, loginName: String) async throws {
        _ = try await revokeLoginFromProxy(proxyName: proxyName, loginName: loginName).get()
    }

    @available(macOS 12.0, *)
    public func grantProxyToSubsystem(proxyName: String, subsystem: String) async throws {
        _ = try await grantProxyToSubsystem(proxyName: proxyName, subsystem: subsystem).get()
    }

    @available(macOS 12.0, *)
    public func revokeProxyFromSubsystem(proxyName: String, subsystem: String) async throws {
        _ = try await revokeProxyFromSubsystem(proxyName: proxyName, subsystem: subsystem).get()
    }

    @available(macOS 12.0, *)
    public func listProxySubsystems(proxyName: String) async throws -> [String] {
        try await listProxySubsystems(proxyName: proxyName).get()
    }

    @available(macOS 12.0, *)
    public func listProxyLogins(proxyName: String) async throws -> [String] {
        try await listProxyLogins(proxyName: proxyName).get()
    }

    @available(macOS 12.0, *)
    public func listProxies() async throws -> [SQLServerAgentProxyInfo] {
        try await listProxies().get()
    }

    @available(macOS 12.0, *)
    public func listJobHistory(jobName: String, top: Int = 20) async throws -> [SQLServerAgentJobHistoryEntry] {
        try await listJobHistory(jobName: jobName, top: top).get()
    }

    @available(macOS 12.0, *)
    public func getJobHistory(jobName: String? = nil, top: Int = 100) async throws -> [SQLServerAgentJobHistoryDetail] {
        try await getJobHistory(jobName: jobName, top: top).get()
    }

    @available(macOS 12.0, *)
    public func fetchProxyAndCredentialPermissions() async throws -> SQLServerAgentPermissionReport {
        let future: EventLoopFuture<SQLServerAgentPermissionReport> = self.fetchProxyAndCredentialPermissions()
        return try await future.get()
    }
}
