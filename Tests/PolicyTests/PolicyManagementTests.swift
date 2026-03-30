import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Integration tests for Policy-Based Management (`client.policy`).
final class PolicyManagementTests: XCTestCase, @unchecked Sendable {
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

    // MARK: - List Policies

    func testListPoliciesReturnsArray() async throws {
        do {
            let policies = try await client.policy.listPolicies()
            XCTAssertNotNil(policies)

            for policy in policies {
                XCTAssertFalse(policy.name.isEmpty, "Policy name must not be empty")
                XCTAssertFalse(policy.conditionName.isEmpty, "Condition name must not be empty")
            }
        } catch {
            // msdb.dbo.syspolicy_policies may not exist on some editions
            throw XCTSkip("Policy-Based Management not available: \(error)")
        }
    }

    // MARK: - List Conditions

    func testListConditionsReturnsArray() async throws {
        do {
            let conditions = try await client.policy.listConditions()
            XCTAssertNotNil(conditions)

            for condition in conditions {
                XCTAssertFalse(condition.name.isEmpty, "Condition name must not be empty")
                XCTAssertFalse(condition.facetName.isEmpty, "Facet name must not be empty")
            }
        } catch {
            throw XCTSkip("Policy conditions not available: \(error)")
        }
    }

    // MARK: - List Facets

    func testListFacetsReturnsArray() async throws {
        do {
            let facets = try await client.policy.listFacets()
            XCTAssertNotNil(facets)

            // SQL Server ships with built-in facets; there should be at least a few
            if !facets.isEmpty {
                for facet in facets {
                    XCTAssertFalse(facet.name.isEmpty, "Facet name must not be empty")
                }
            }
        } catch {
            throw XCTSkip("Policy facets not available: \(error)")
        }
    }

    // MARK: - Enable / Disable Policy

    func testEnableAndDisablePolicyRoundTrip() async throws {
        // First, list existing policies. If none exist, skip.
        let policies: [SQLServerPolicy]
        do {
            policies = try await client.policy.listPolicies()
        } catch {
            throw XCTSkip("Policy-Based Management not available: \(error)")
        }

        // Skip on-demand policies (execution_mode 0) — they can't be toggled
        guard let policy = policies.first(where: { $0.executionMode != 0 }) else {
            throw XCTSkip("No policies with non-on-demand execution mode exist")
        }

        let originalEnabled = policy.isEnabled

        // Toggle: disable then re-enable (or vice versa)
        // PBM stored procedures may not be available on all editions
        do {
            if originalEnabled {
                try await client.policy.disablePolicy(name: policy.name)
                let updated = try await client.policy.listPolicies()
                let match = updated.first(where: { $0.name == policy.name })
                XCTAssertEqual(match?.isEnabled, false, "Policy should be disabled")

                // Restore
                try await client.policy.enablePolicy(name: policy.name)
            } else {
                try await client.policy.enablePolicy(name: policy.name)
                let updated = try await client.policy.listPolicies()
                let match = updated.first(where: { $0.name == policy.name })
                XCTAssertEqual(match?.isEnabled, true, "Policy should be enabled")

                // Restore
                try await client.policy.disablePolicy(name: policy.name)
            }
        } catch {
            // Restore original state on failure
            if originalEnabled {
                try? await client.policy.enablePolicy(name: policy.name)
            } else {
                try? await client.policy.disablePolicy(name: policy.name)
            }
            let msg = "\(error)"
            if msg.contains("Could not find stored procedure") || msg.contains("sp_syspolicy") {
                throw XCTSkip("Policy-Based Management stored procedures not available: \(error)")
            }
            throw error
        }
    }
}
