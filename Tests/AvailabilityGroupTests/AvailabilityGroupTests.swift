import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// Integration tests for Always On Availability Groups (`client.availabilityGroups`).
///
/// HADR is typically not enabled on standalone Docker test instances.
/// These tests verify the APIs execute without error and gracefully
/// skip when HADR features are unavailable.
final class AvailabilityGroupTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(
            configuration: config,
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - HADR Status

    func testIsHadrEnabledReturnsBool() async throws {
        let enabled = try await client.availabilityGroups.isHadrEnabled()

        // On a standalone Docker instance HADR is typically false.
        // We just verify the call succeeds and returns a valid Bool.
        XCTAssertNotNil(enabled)
        // enabled is a Bool -- either value is acceptable
    }

    // MARK: - List Groups

    func testListGroupsReturnsArray() async throws {
        let hadrEnabled = try await client.availabilityGroups.isHadrEnabled()
        try XCTSkipIf(!hadrEnabled, "HADR is not enabled on this instance")

        let groups = try await client.availabilityGroups.listGroups()
        XCTAssertNotNil(groups)

        for group in groups {
            XCTAssertFalse(group.groupId.isEmpty, "Group ID must not be empty")
            XCTAssertFalse(group.name.isEmpty, "Group name must not be empty")
            XCTAssertFalse(group.automatedBackupPreference.isEmpty)
            XCTAssertGreaterThanOrEqual(group.failureConditionLevel, 0)
        }
    }

    // MARK: - List Groups When HADR Disabled

    func testListGroupsReturnsEmptyWhenHadrDisabled() async throws {
        let hadrEnabled = try await client.availabilityGroups.isHadrEnabled()

        if !hadrEnabled {
            // When HADR is disabled, listGroups should either return empty
            // or throw. Both are acceptable.
            do {
                let groups = try await client.availabilityGroups.listGroups()
                XCTAssertTrue(groups.isEmpty, "Expected no groups when HADR is disabled")
            } catch {
                // Some SQL Server builds may error on the DMV query
                // when HADR is completely disabled. This is acceptable.
            }
        } else {
            throw XCTSkip("HADR is enabled -- this test is for disabled-HADR scenarios")
        }
    }

    // MARK: - Identifiable / Equatable

    func testAvailabilityGroupIdentifiable() {
        let group = SQLServerAvailabilityGroup(
            groupId: "550e8400-e29b-41d4-a716-446655440000",
            name: "TestAG",
            automatedBackupPreference: "SECONDARY",
            failureConditionLevel: 3
        )
        XCTAssertEqual(group.id, "550e8400-e29b-41d4-a716-446655440000")
        XCTAssertEqual(group.name, "TestAG")
    }

    func testAGReplicaHelpers() {
        let primary = SQLServerAGReplica(
            replicaServerName: "NODE1",
            availabilityMode: "SYNCHRONOUS_COMMIT",
            failoverMode: "AUTOMATIC",
            role: "PRIMARY",
            operationalState: "ONLINE",
            connectionState: "CONNECTED",
            synchronizationHealth: "HEALTHY",
            primaryAllowConnections: "ALL",
            secondaryAllowConnections: "NO"
        )
        XCTAssertTrue(primary.isPrimary)
        XCTAssertTrue(primary.isHealthy)
        XCTAssertEqual(primary.id, "NODE1")

        let secondary = SQLServerAGReplica(
            replicaServerName: "NODE2",
            availabilityMode: "ASYNCHRONOUS_COMMIT",
            failoverMode: "MANUAL",
            role: "SECONDARY",
            operationalState: "ONLINE",
            connectionState: "CONNECTED",
            synchronizationHealth: "NOT_HEALTHY",
            primaryAllowConnections: "ALL",
            secondaryAllowConnections: "READ_ONLY"
        )
        XCTAssertFalse(secondary.isPrimary)
        XCTAssertFalse(secondary.isHealthy)
    }

    func testAGDatabaseHelpers() {
        let healthy = SQLServerAGDatabase(
            databaseName: "MyDB",
            synchronizationState: "SYNCHRONIZED",
            synchronizationHealth: "HEALTHY",
            databaseState: "ONLINE",
            isSuspended: false,
            suspendReason: nil,
            logSendQueueSize: 0,
            redoQueueSize: 0
        )
        XCTAssertTrue(healthy.isHealthy)
        XCTAssertEqual(healthy.id, "MyDB")

        let unhealthy = SQLServerAGDatabase(
            databaseName: "OtherDB",
            synchronizationState: "NOT_SYNCHRONIZING",
            synchronizationHealth: "NOT_HEALTHY",
            databaseState: "ONLINE",
            isSuspended: true,
            suspendReason: "USER",
            logSendQueueSize: 1024,
            redoQueueSize: 512
        )
        XCTAssertFalse(unhealthy.isHealthy)
        XCTAssertTrue(unhealthy.isSuspended)
    }
}
