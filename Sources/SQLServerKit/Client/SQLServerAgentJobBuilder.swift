import Foundation
import NIO

public struct SQLServerAgentJobStep: Sendable {
    public enum Subsystem: String, Sendable { case tsql = "TSQL", cmdExec = "CmdExec", powershell = "PowerShell" }
    public enum StepAction: Sendable {
        case quitWithSuccess
        case quitWithFailure
        case goToNextStep
        case goToStep(Int)

        internal var actionCode: Int { // matches msdb semantics
            switch self {
            case .quitWithSuccess: return 1
            case .quitWithFailure: return 2
            case .goToNextStep: return 3
            case .goToStep: return 4
            }
        }
        internal var stepId: Int? {
            switch self { case .goToStep(let id): return id; default: return nil }
        }
    }
    public var name: String
    public var subsystem: Subsystem
    public var command: String
    public var database: String?
    public var proxyName: String? = nil
    public var outputFile: String? = nil
    public var appendOutputFile: Bool? = nil
    public var onSuccess: StepAction? = nil
    public var onFail: StepAction? = nil
    public var retryAttempts: Int? = nil
    public var retryIntervalMinutes: Int? = nil

    public init(name: String, subsystem: Subsystem = .tsql, command: String, database: String? = nil) {
        self.name = name
        self.subsystem = subsystem
        self.command = command
        self.database = database
    }
}

public struct SQLServerAgentJobSchedule: Sendable {
    public enum WeeklyDay: Int, Sendable { case sunday=1, monday=2, tuesday=4, wednesday=8, thursday=16, friday=32, saturday=64 }
    public enum MonthWeek: Int, Sendable { case first=1, second=2, third=4, fourth=8, last=16 }
    public enum Kind: Sendable {
        case oneTime(startDate: Int, startTime: Int)
        case daily(everyDays: Int, startTime: Int)
        case weekly(days: [WeeklyDay], everyWeeks: Int, startTime: Int)
        case monthly(day: Int, everyMonths: Int, startTime: Int)
        case monthlyRelative(week: MonthWeek, day: WeeklyDay, everyMonths: Int, startTime: Int)
        case raw(freqType: Int, freqInterval: Int, activeStartDate: Int?, activeStartTime: Int?, freqSubdayType: Int?, freqSubdayInterval: Int?, freqRelativeInterval: Int?, freqRecurrenceFactor: Int?)
    }
    public var name: String
    public var enabled: Bool
    public var kind: Kind
    // Optional schedule window/subday settings to mirror SSMS
    public var activeStartDate: Int?
    public var activeEndDate: Int?
    public var activeStartTime: Int?
    public var activeEndTime: Int?
    public var subdayType: Int? // 4=Minutes, 8=Hours
    public var subdayInterval: Int?

    public init(name: String,
                enabled: Bool = true,
                kind: Kind,
                activeStartDate: Int? = nil,
                activeEndDate: Int? = nil,
                activeStartTime: Int? = nil,
                activeEndTime: Int? = nil,
                subdayType: Int? = nil,
                subdayInterval: Int? = nil) {
        self.name = name
        self.enabled = enabled
        self.kind = kind
        self.activeStartDate = activeStartDate
        self.activeEndDate = activeEndDate
        self.activeStartTime = activeStartTime
        self.activeEndTime = activeEndTime
        self.subdayType = subdayType
        self.subdayInterval = subdayInterval
    }
}

public struct SQLServerAgentJobNotification: Sendable {
    public enum Level: Int, Sendable { case none=0, onSuccess=1, onFailure=2, onCompletion=3 }
    public var operatorName: String
    public var level: Level
    public var operatorEmail: String?
    public init(operatorName: String, level: Level, operatorEmail: String? = nil) { self.operatorName = operatorName; self.level = level; self.operatorEmail = operatorEmail }
}

/// A high-level, composable job builder that orchestrates creation of Agent jobs,
/// steps, schedules and notifications with best-effort rollback on failure.
public final class SQLServerAgentJobBuilder: @unchecked Sendable {
    private let agent: SQLServerAgentClient

    // Core job fields
    private let jobName: String
    private let description: String?
    private let enabled: Bool
    private let ownerLoginName: String?
    private let categoryName: String?
    private let autoAttachServer: Bool

    // Components
    private var steps: [SQLServerAgentJobStep] = []
    private var schedules: [SQLServerAgentJobSchedule] = []
    private var notification: SQLServerAgentJobNotification?
    private var startStepId: Int?

    public init(agent: SQLServerAgentClient,
                jobName: String,
                description: String? = nil,
                enabled: Bool = true,
                ownerLoginName: String? = nil,
                categoryName: String? = nil,
                autoAttachServer: Bool = true) {
        self.agent = agent
        self.jobName = jobName
        self.description = description
        self.enabled = enabled
        self.ownerLoginName = ownerLoginName
        self.categoryName = categoryName
        self.autoAttachServer = autoAttachServer
    }

    public func addStep(_ step: SQLServerAgentJobStep) -> Self { steps.append(step); return self }
    public func addSchedule(_ schedule: SQLServerAgentJobSchedule) -> Self { schedules.append(schedule); return self }
    public func setNotification(_ notification: SQLServerAgentJobNotification?) -> Self { self.notification = notification; return self }
    public func setStartStepId(_ id: Int?) -> Self { self.startStepId = id; return self }

    /// Creates the job and all requested components. On failure, attempts to delete the job.
    /// - Returns: The created job name (and, if resolvable, the job_id as string)
    @available(macOS 12.0, *)
    public func commit() async throws -> (name: String, jobId: String?) {
        // Create base job
        do {
            try await agent.createJob(named: jobName, description: description, enabled: enabled, ownerLoginName: ownerLoginName)
            if let category = categoryName, !category.isEmpty {
                // Ensure category exists then set
                _ = try? await agent.createCategory(name: category)
                try await agent.setJobCategory(named: jobName, categoryName: category)
            }
            if autoAttachServer {
                _ = try? await agent.addJobServer(jobName: jobName)
            }

            // Steps
            for s in steps {
                // Use generic addStep to allow proxy/output for any subsystem.
                try await agent.addStep(jobName: jobName,
                                        stepName: s.name,
                                        subsystem: s.subsystem.rawValue,
                                        command: s.command,
                                        database: s.database,
                                        proxyName: s.proxyName,
                                        outputFile: s.outputFile)
            }

            // Configure step flow if provided
            if !steps.isEmpty {
                // fetch ids
                let stepList = try await agent.listSteps(jobName: jobName)
                for s in steps {
                    if stepList.contains(where: { $0.name == s.name }) {
                        if s.onSuccess != nil || s.onFail != nil || s.retryAttempts != nil || s.retryIntervalMinutes != nil {
                            let successAction = s.onSuccess?.actionCode
                            let successStep = s.onSuccess?.stepId
                            let failAction = s.onFail?.actionCode
                            let failStep = s.onFail?.stepId
                            _ = try await agent.configureStep(jobName: jobName,
                                                              stepName: s.name,
                                                              onSuccessAction: successAction,
                                                              onSuccessStepId: successStep,
                                                              onFailAction: failAction,
                                                              onFailStepId: failStep,
                                                              retryAttempts: s.retryAttempts,
                                                              retryIntervalMinutes: s.retryIntervalMinutes,
                                                              outputFileName: s.outputFile,
                                                              appendOutputFile: s.appendOutputFile).get()
                        }
                    }
                }
            }

            // Start step id
            if let startId = startStepId {
                _ = try await agent.setJobStartStep(jobName: jobName, stepId: startId).get()
            }

            // Schedules
            var createdSchedules: [String] = []
            for sch in schedules {
                let params: (freqType: Int, freqInterval: Int, activeStartDate: Int?, activeStartTime: Int?, freqSubdayType: Int?, freqSubdayInterval: Int?, freqRelativeInterval: Int?, freqRecurrenceFactor: Int?)
                switch sch.kind {
                case .oneTime(let d, let t):
                    params = (1, 0, d, t, sch.subdayType, sch.subdayInterval, nil, nil)
                case .daily(let every, let time):
                    params = (4, every, sch.activeStartDate, sch.activeStartTime ?? time, sch.subdayType, sch.subdayInterval, nil, nil)
                case .weekly(let days, let everyWeeks, let time):
                    let bitmask = days.reduce(0) { $0 | $1.rawValue }
                    params = (8, bitmask == 0 ? 1 : bitmask, sch.activeStartDate, sch.activeStartTime ?? time, sch.subdayType, sch.subdayInterval, nil, everyWeeks)
                case .monthly(let day, let everyMonths, let time):
                    params = (16, max(1, min(day, 31)), sch.activeStartDate, sch.activeStartTime ?? time, sch.subdayType, sch.subdayInterval, nil, everyMonths)
                case .monthlyRelative(let week, let day, let everyMonths, let time):
                    params = (32, day.rawValue, sch.activeStartDate, sch.activeStartTime ?? time, sch.subdayType, sch.subdayInterval, week.rawValue, everyMonths)
                case .raw(let ft, let fi, let d, let t, let sdt, let sdi, let fri, let frf):
                    params = (ft, fi, d, t, sdt, sdi, fri, frf)
                }
                try await agent.createSchedule(named: sch.name,
                                               enabled: sch.enabled,
                                               freqType: params.freqType,
                                               freqInterval: params.freqInterval,
                                               activeStartDate: params.activeStartDate,
                                               activeStartTime: params.activeStartTime,
                                               activeEndDate: sch.activeEndDate,
                                               activeEndTime: sch.activeEndTime,
                                               freqSubdayType: params.freqSubdayType,
                                               freqSubdayInterval: params.freqSubdayInterval,
                                               freqRelativeInterval: params.freqRelativeInterval,
                                               freqRecurrenceFactor: params.freqRecurrenceFactor)
                createdSchedules.append(sch.name)
                try await agent.attachSchedule(scheduleName: sch.name, toJob: jobName)
            }

            // Notifications
            if let n = notification {
                // Ensure operator exists, update email if provided
                let present = try await agent.listOperators()
                if let op = present.first(where: { $0.name.caseInsensitiveCompare(n.operatorName) == .orderedSame }) {
                    if let email = n.operatorEmail, (op.emailAddress ?? "") != email {
                        _ = try? await agent.updateOperator(name: n.operatorName, emailAddress: email, enabled: nil, pagerAddress: nil, weekdayPagerStartTime: nil, weekdayPagerEndTime: nil).get()
                    }
                } else {
                    _ = try? await agent.createOperator(name: n.operatorName, emailAddress: n.operatorEmail, enabled: true).get()
                }
                try await agent.setJobEmailNotification(jobName: jobName, operatorName: n.operatorName, notifyLevel: n.level.rawValue)
            }

            // Resolve job id (best-effort)
            let jobId = try? await agent.fetchJobId(named: jobName)
            return (name: jobName, jobId: jobId)
        } catch {
            // Rollback: delete job and any schedules we created
            _ = try? await agent.deleteJob(named: jobName).get()
            for sch in schedules { _ = try? await agent.deleteSchedule(named: sch.name).get() }
            throw error
        }
    }
}
