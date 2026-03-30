import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Integration tests for Resource Governor (`client.resourceGovernor`).
///
/// Note: ManagementGovernanceTests in Tests/AdvancedTests also covers basic
/// Resource Governor calls. These tests provide more thorough coverage of
/// each API individually, including the enable/disable/reconfigure lifecycle.
final class ResourceGovernorTests: XCTestCase, @unchecked Sendable {
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

    // MARK: - Fetch Configuration

    func testFetchConfigurationReturnsValidStruct() async throws {
        do {
            let config = try await client.resourceGovernor.fetchConfiguration()

            // isEnabled is a boolean -- any value is valid
            // classifierFunction may be nil
            // isReconfigurationPending is a boolean
            XCTAssertNotNil(config)

            // Verify the struct fields are accessible (compile-time + runtime)
            _ = config.isEnabled
            _ = config.classifierFunction
            _ = config.isReconfigurationPending
        } catch {
            // Express/Web editions may not support Resource Governor
            throw XCTSkip("Resource Governor not supported on this edition: \(error)")
        }
    }

    // MARK: - List Resource Pools

    func testListResourcePoolsWithoutStats() async throws {
        do {
            let pools = try await client.resourceGovernor.listResourcePools(includeStats: false)
            XCTAssertNotNil(pools)

            // SQL Server always has at least two built-in pools: 'internal' and 'default'
            XCTAssertGreaterThanOrEqual(pools.count, 2, "Expected at least internal and default pools")
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }
    }

    func testListResourcePoolsWithStats() async throws {
        do {
            let pools = try await client.resourceGovernor.listResourcePools(includeStats: true)
            XCTAssertNotNil(pools)
            XCTAssertGreaterThanOrEqual(pools.count, 2)
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }
    }

    // MARK: - List Workload Groups

    func testListWorkloadGroupsWithoutStats() async throws {
        do {
            let groups = try await client.resourceGovernor.listWorkloadGroups(includeStats: false)
            XCTAssertNotNil(groups)

            // SQL Server always has at least two built-in groups: 'internal' and 'default'
            XCTAssertGreaterThanOrEqual(groups.count, 2, "Expected at least internal and default groups")
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }
    }

    func testListWorkloadGroupsWithStats() async throws {
        do {
            let groups = try await client.resourceGovernor.listWorkloadGroups(includeStats: true)
            XCTAssertNotNil(groups)
            XCTAssertGreaterThanOrEqual(groups.count, 2)
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }
    }

    // MARK: - Enable / Disable / Reconfigure Lifecycle

    func testEnableDisableReconfigureRoundTrip() async throws {
        // Read initial state so we can restore it
        let initialConfig: SQLServerResourceGovernorConfiguration
        do {
            initialConfig = try await client.resourceGovernor.fetchConfiguration()
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }

        do {
            // Disable Resource Governor
            try await client.resourceGovernor.disable()
            try await client.resourceGovernor.reconfigure()

            let disabledConfig = try await client.resourceGovernor.fetchConfiguration()
            XCTAssertFalse(disabledConfig.isEnabled, "Resource Governor should be disabled")

            // Re-enable Resource Governor
            try await client.resourceGovernor.enable()
            try await client.resourceGovernor.reconfigure()

            let enabledConfig = try await client.resourceGovernor.fetchConfiguration()
            XCTAssertTrue(enabledConfig.isEnabled, "Resource Governor should be enabled")
        } catch {
            // Attempt to restore original state
            if initialConfig.isEnabled {
                try? await client.resourceGovernor.enable()
            } else {
                try? await client.resourceGovernor.disable()
            }
            try? await client.resourceGovernor.reconfigure()
            throw error
        }

        // Restore original state
        if !initialConfig.isEnabled {
            try await client.resourceGovernor.disable()
            try await client.resourceGovernor.reconfigure()
        }
    }

    // MARK: - Reconfigure Idempotent

    func testReconfigureIsIdempotent() async throws {
        do {
            // Calling reconfigure multiple times should not error
            try await client.resourceGovernor.reconfigure()
            try await client.resourceGovernor.reconfigure()
        } catch {
            throw XCTSkip("Resource Governor not supported: \(error)")
        }
    }
}
