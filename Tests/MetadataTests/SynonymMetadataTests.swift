import XCTest
import SQLServerKit
import SQLServerKitTesting

final class SynonymMetadataTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var synonymsToDrop: [(name: String, schema: String)] = []
    private var tablesToDrop: [String] = []

    override func setUp() async throws {
        try await super.setUp()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDown() async throws {
        for synonym in synonymsToDrop {
            _ = try? await client.execute("DROP SYNONYM [\(synonym.schema)].[\(synonym.name)]")
        }
        synonymsToDrop.removeAll()

        for table in tablesToDrop {
            try? await adminClient.dropTable(name: table)
        }
        tablesToDrop.removeAll()

        try? await client?.shutdownGracefully()
        client = nil
        try await super.tearDown()
    }

    // MARK: - Helpers

    private func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }

    private func createSynonym(name: String, schema: String = "dbo", forObject baseObject: String) async throws {
        let sql = "CREATE SYNONYM [\(schema)].[\(name)] FOR [\(baseObject)]"
        _ = try await client.execute(sql)
        synonymsToDrop.append((name: name, schema: schema))
    }

    // MARK: - listSynonyms

    func testListSynonymsReturnsCreatedSynonym() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        let synonyms = try await client.metadata.listSynonyms(schema: "dbo")
        let match = synonyms.first { $0.name == synonymName }

        XCTAssertNotNil(match, "Created synonym should appear in listSynonyms result")
        XCTAssertEqual(match?.schema, "dbo")
        XCTAssertTrue(match?.baseObjectName.contains(tableName) == true,
                      "Base object name should reference the target table")
    }

    func testListSynonymsWithNoSchemaFilter() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        let synonyms = try await client.metadata.listSynonyms()
        let match = synonyms.first { $0.name == synonymName }

        XCTAssertNotNil(match, "Created synonym should appear when listing without schema filter")
    }

    func testListSynonymsWithComments() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        // Add an extended property (comment) to the synonym
        _ = try await client.execute("""
            EXEC sp_addextendedproperty
                @name = N'MS_Description',
                @value = N'Test synonym comment',
                @level0type = N'SCHEMA', @level0name = N'dbo',
                @level1type = N'SYNONYM', @level1name = N'\(synonymName)'
        """)

        let synonyms = try await client.metadata.listSynonyms(schema: "dbo", includeComments: true)
        let match = synonyms.first { $0.name == synonymName }

        XCTAssertNotNil(match)
        XCTAssertEqual(match?.comment, "Test synonym comment")
    }

    func testListSynonymsExcludesOtherSchemas() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        // Filter by a schema that should not contain our synonym
        let synonyms = try await client.metadata.listSynonyms(schema: "sys")
        let match = synonyms.first { $0.name == synonymName }

        XCTAssertNil(match, "Synonym should not appear when filtering by a different schema")
    }

    func testListSynonymsEmptyWhenNoneExist() async throws {
        // Use a unique schema filter unlikely to have synonyms
        let synonyms = try await client.metadata.listSynonyms(schema: "INFORMATION_SCHEMA")
        XCTAssertTrue(synonyms.isEmpty, "INFORMATION_SCHEMA should have no synonyms")
    }

    // MARK: - Synonym in loadSchemaStructure

    func testLoadSchemaStructureIncludesSynonyms() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        let structure = try await client.metadata.loadSchemaStructure(schema: "dbo")

        let match = structure.synonyms.first { $0.name == synonymName }
        XCTAssertNotNil(match, "loadSchemaStructure should include the created synonym")
        XCTAssertEqual(match?.schema, "dbo")
        XCTAssertTrue(match?.baseObjectName.contains(tableName) == true)
    }

    func testLoadDatabaseStructureIncludesSynonymsInSchema() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        let dbStructure = try await client.metadata.loadDatabaseStructure()

        let dboSchema = dbStructure.schemas.first { $0.name.caseInsensitiveCompare("dbo") == .orderedSame }
        XCTAssertNotNil(dboSchema, "dbo schema should exist in database structure")

        let match = dboSchema?.synonyms.first { $0.name == synonymName }
        XCTAssertNotNil(match, "Synonym should appear in dbo schema within database structure")
    }

    // MARK: - Create and Drop Synonyms

    func testCreateAndDropSynonym() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)

        // Create
        _ = try await client.execute("CREATE SYNONYM [dbo].[\(synonymName)] FOR [\(tableName)]")

        var synonyms = try await client.metadata.listSynonyms(schema: "dbo")
        var match = synonyms.first { $0.name == synonymName }
        XCTAssertNotNil(match, "Synonym should exist after creation")

        // Drop
        _ = try await client.execute("DROP SYNONYM [dbo].[\(synonymName)]")

        synonyms = try await client.metadata.listSynonyms(schema: "dbo")
        match = synonyms.first { $0.name == synonymName }
        XCTAssertNil(match, "Synonym should not exist after being dropped")
    }

    func testSynonymPointsToCorrectBaseObject() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        // Insert via the base table
        _ = try await client.admin.insertRow(into: tableName, values: [
            "id": .int(1),
            "name": .nString("via_table")
        ])

        // Query via the synonym
        let result = try await client.query("SELECT * FROM [dbo].[\(synonymName)]")
        XCTAssertEqual(result.count, 1, "Synonym should resolve to the base table")
        XCTAssertEqual(result.first?.column("name")?.string, "via_table")
    }

    func testCreateSynonymForSchemaQualifiedObject() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)

        // Create synonym using fully schema-qualified base object
        _ = try await client.execute("CREATE SYNONYM [dbo].[\(synonymName)] FOR [dbo].[\(tableName)]")
        synonymsToDrop.append((name: synonymName, schema: "dbo"))

        let synonyms = try await client.metadata.listSynonyms(schema: "dbo")
        let match = synonyms.first { $0.name == synonymName }

        XCTAssertNotNil(match)
        // The base object name should contain the schema-qualified reference
        XCTAssertTrue(match?.baseObjectName.contains(tableName) == true)
    }

    func testCreateDuplicateSynonymFails() async throws {
        let tableName = "syn_tbl_\(UUID().uuidString.prefix(8))"
        let synonymName = "syn_\(UUID().uuidString.prefix(8))"

        try await createTestTable(name: tableName)
        try await createSynonym(name: synonymName, forObject: tableName)

        do {
            _ = try await client.execute("CREATE SYNONYM [dbo].[\(synonymName)] FOR [\(tableName)]")
            XCTFail("Creating a duplicate synonym should fail")
        } catch {
            // Expected — synonym already exists
        }
    }

    func testDropNonExistentSynonymFails() async throws {
        let synonymName = "nonexistent_syn_\(UUID().uuidString.prefix(8))"

        do {
            _ = try await client.execute("DROP SYNONYM [dbo].[\(synonymName)]")
            XCTFail("Dropping a non-existent synonym should fail")
        } catch {
            // Expected — synonym does not exist
        }
    }
}
