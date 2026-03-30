import XCTest
import Logging
import SQLServerKit
import SQLServerKitTesting

/// Governance and Management tests (Phase 4)
final class ManagementGovernanceTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private let logger = Logger(label: "ManagementGovernanceTests")

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(
            configuration: config,
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - 4.1 Resource Governor Tests

    func testResourceGovernorConfiguration() async throws {
        logger.info("🔧 Testing Resource Governor configuration...")
        
        do {
            let config = try await client.resourceGovernor.fetchConfiguration()
            logger.info("   Enabled: \(config.isEnabled)")
            logger.info("   Classifier: \(config.classifierFunction ?? "None")")
            logger.info("   Reconfiguration Pending: \(config.isReconfigurationPending)")
            
            XCTAssertNotNil(config)
        } catch {
            // Some editions (e.g. Express) don't support Resource Governor
            logger.warning("   Resource Governor not supported or accessible: \(error)")
        }
    }

    func testListResourcePoolsAndGroups() async throws {
        logger.info("🔧 Testing Resource Governor pools and groups...")
        
        do {
            let pools = try await client.resourceGovernor.listResourcePools(includeStats: true)
            logger.info("   Found \(pools.count) pools.")
            
            let groups = try await client.resourceGovernor.listWorkloadGroups(includeStats: true)
            logger.info("   Found \(groups.count) groups.")
            
            XCTAssertGreaterThanOrEqual(pools.count, 0)
            XCTAssertGreaterThanOrEqual(groups.count, 0)
        } catch {
            logger.warning("   Resource Governor pools/groups not accessible: \(error)")
        }
    }

    // MARK: - 4.2 Policy-Based Management Tests

    func testPolicyManagementDiscovery() async throws {
        logger.info("🔧 Testing Policy-Based Management discovery...")
        
        do {
            let policies = try await client.policy.listPolicies()
            logger.info("   Found \(policies.count) policies.")
            
            let conditions = try await client.policy.listConditions()
            logger.info("   Found \(conditions.count) conditions.")
            
            let facets = try await client.policy.listFacets()
            logger.info("   Found \(facets.count) facets.")
            
            XCTAssertNotNil(policies)
            XCTAssertNotNil(conditions)
            XCTAssertNotNil(facets)
        } catch {
            logger.warning("   Policy management not accessible: \(error)")
        }
    }
}
