import XCTest
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit
import SQLServerKitTesting

/// Advanced scripting and Data Movement tests (Phase 6 & 7)
final class AdvancedScriptingAndBITests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private let logger = Logger(label: "AdvancedScriptingAndBITests")

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

    // MARK: - Phase 6: Dependencies & Bulk Copy Inference

    func testDependencyEngine() async throws {
        logger.info("🔧 Testing Dependency Engine...")
        
        // This will test the DMV queries. In a fresh DB, it might be empty.
        let graph = try await client.dependencies.buildGraph()
        
        logger.info("   Found \(graph.objects.count) objects and \(graph.dependencies.count) dependencies.")
        
        let sorted = graph.resolvedOrder()
        XCTAssertEqual(graph.objects.count, sorted.count, "Topological sort should return all objects")
    }
    
    func testBulkCopySchemaInference() async throws {
        logger.info("🔧 Testing Bulk Copy Schema Inference...")
        
        let bulkClient = client.bulk
        
        let headers = ["ID", "Name", "Score", "StartDate"]
        let rows = [
            ["1", "Alice", "95.5", "2024-01-01T10:00:00Z"],
            ["2", "Bob", "88.0", "2024-01-02T11:30:00Z"]
        ]
        
        let inferences = bulkClient.inferSchema(headers: headers, sampleRows: rows)
        
        XCTAssertEqual(inferences.count, 4)
        XCTAssertEqual(inferences[0].dataType, "INT")
        XCTAssertEqual(inferences[1].dataType, "NVARCHAR(50)")
        XCTAssertEqual(inferences[2].dataType, "FLOAT")
        XCTAssertEqual(inferences[3].dataType, "DATETIME2")
        
        let sql = bulkClient.generateCreateTableSQL(schema: "dbo", table: "TestImport", columns: inferences)
        XCTAssertTrue(sql.contains("CREATE TABLE [dbo].[TestImport]"))
        XCTAssertTrue(sql.contains("[Score] FLOAT"))
    }

    // MARK: - Phase 7: SSIS Catalog

    func testSSISCatalogDiscovery() async throws {
        logger.info("🔧 Testing SSIS Catalog discovery...")
        
        do {
            let isAvailable = try await client.ssis.isSSISCatalogAvailable()
            logger.info("   SSIS Catalog Available: \(isAvailable)")
            
            if isAvailable {
                let folders = try await client.ssis.listFolders()
                logger.info("   Found \(folders.count) folders in SSISDB.")
                
                if let firstFolder = folders.first {
                    let projects = try await client.ssis.listProjects(folderId: firstFolder.folderId)
                    logger.info("   Found \(projects.count) projects in folder \(firstFolder.name).")
                }
            } else {
                logger.info("   SSISDB is not installed on this instance. Skipping deeper tests.")
            }
        } catch {
            logger.warning("   SSIS management not accessible (expected if SSISDB is absent): \(error)")
        }
    }
}
