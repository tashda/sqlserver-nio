import Foundation
import SQLServerKit
import SQLServerKitTesting
import XCTest

/// `updateSchedule` changes a job's schedule in place, and the job's schedule details say what it is.
final class AgentScheduleUpdateTests: AgentTestBase, @unchecked Sendable {

    func testAScheduleCanBeChangedInPlace() async throws {
        let agent = SQLServerAgentOperations(client: client)
        let jobName = "agent_sched_upd_\(UUID().uuidString.prefix(8))"
        let scheduleName = "agent_sched_\(UUID().uuidString.prefix(8))"
        let renamed = scheduleName + "_weekly"
        managedJobNames.append(jobName)
        managedScheduleNames.append(renamed)

        try await agent.createJob(named: jobName)
        try await agent.createSchedule(named: scheduleName, freqType: 4, freqInterval: 1, activeStartTime: 90_000)
        try await agent.attachSchedule(scheduleName: scheduleName, toJob: jobName)

        // Every second week on Monday and Wednesday at 23:30, between two dates.
        try await agent.updateSchedule(
            name: scheduleName, newName: renamed, enabled: false, freqType: 8, freqInterval: 2 | 8,
            activeStartDate: 20_260_101, activeStartTime: 233_000, activeEndDate: 20_261_231, freqRecurrenceFactor: 2
        )

        let schedules = try await agent.listJobSchedules(jobName: jobName)
        let schedule = try XCTUnwrap(schedules.first)
        XCTAssertEqual(schedules.count, 1)
        XCTAssertEqual(schedule.name, renamed)
        XCTAssertFalse(schedule.enabled)
        XCTAssertEqual(schedule.freqType, 8)
        XCTAssertEqual(schedule.freqInterval, 10)
        XCTAssertEqual(schedule.freqRecurrenceFactor, 2)
        XCTAssertEqual(schedule.activeStartDate, 20_260_101)
        XCTAssertEqual(schedule.activeStartTime, 233_000)
        XCTAssertEqual(schedule.activeEndDate, 20_261_231)
    }
}
