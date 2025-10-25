# SQLServerNIO

SQLServerNIO is a non-blocking Swift client for Microsoft SQL Server built on SwiftNIO. It provides both connection pooling and direct connection management with comprehensive async/await and EventLoopFuture APIs.

## Key Features

- **Connection Management**: `SQLServerClient` for pooled connections with automatic retries, `SQLServerConnection` for direct connection control
- **Modern Swift APIs**: Full async/await support with EventLoopFuture fallbacks for compatibility
- **Transaction Support**: Proper transaction descriptor management with savepoints and isolation levels
- **Batch Processing**: SQL Server batch separation with GO delimiter support via `SQLServerQuerySplitter`
- **Metadata APIs**: Comprehensive database introspection (tables, columns, indexes, procedures, etc.)
- **Administrative Tools**: Server management via `SQLServerAdministrationClient` and SQL Agent via `SQLServerAgentClient`
- **Streaming Support**: AsyncSequence-based result streaming for large datasets
- **Error Handling**: Robust error handling with proper SQL Server error propagation


## Installation

Add the dependency to `Package.swift`:

```swift
.package(url: "https://github.com/sqlserver-nio/sqlserver-nio.git", from: "0.1.0")
```

Then add the product to your target:

```swift
.target(
    name: "MyApp",
    dependencies: [
        .product(name: "SQLServerNIO", package: "sqlserver-nio")
    ]
)
```

## Quick Start

### Pooled Client (Recommended)

```swift
import SQLServerNIO

let configuration = SQLServerClient.Configuration(
    hostname: "localhost",
    port: 1433,
    login: .init(
        database: "master",
        authentication: .sqlPassword(username: "sa", password: "StrongPassword123")
    ),
    tlsConfiguration: .makeClientConfiguration()
)

let client = try await SQLServerClient.connect(configuration: configuration)

// Simple query
let rows = try await client.query("SELECT @@VERSION AS version")
print(rows.first?.column("version")?.string ?? "<unknown>")

// Execute with results
let result = try await client.execute("SELECT COUNT(*) as count FROM sys.tables")
print("Table count: \(result.rows.first?.column("count")?.int ?? 0)")

try await client.shutdownGracefully()
```

### Direct Connection

```swift
let connection = try await SQLServerConnection.connect(configuration: configuration.connection)
let databases = try await connection.listDatabases()
print("Found \(databases.count) databases")
try await connection.close()
```

## Connection Configuration

`SQLServerConnection.Configuration` lets you specify hostname, port, database, authentication, TLS, metadata caching, retry behaviour, and session defaults.

Session defaults mirror SQL Server Management Studio (ANSI settings, `SET NOCOUNT ON`, `SET FMTONLY OFF`) and can be customised via `SessionOptions`:

```swift
var configuration = makeSQLServerClientConfiguration()
configuration.connection.sessionOptions = .init(
    nocount: false,
    fmtOnlyOff: true,
    additionalStatements: ["SET DEADLOCK_PRIORITY LOW;"]
)
```

## Core Operations

### Basic Querying

```swift
// Query returning rows
let rows = try await client.query("SELECT name FROM sys.databases ORDER BY name")
for row in rows {
    print(row.column("name")?.string ?? "<unknown>")
}

// Execute statements (INSERT, UPDATE, DELETE, DDL)
let result = try await client.execute("INSERT INTO Logs(Message) VALUES (N'hello')")
print("Affected rows: \(result.done.first?.rowCount ?? 0)")

// Scalar queries
let databaseName: String? = try await client.queryScalar("SELECT DB_NAME()", as: String.self)
let tableCount: Int? = try await client.queryScalar("SELECT COUNT(*) FROM sys.tables", as: Int.self)
```

### EventLoopFuture API (Legacy Support)

```swift
client.query("SELECT COUNT(*) AS count FROM sys.tables").whenSuccess { rows in
    print(rows.first?.column("count")?.int ?? 0)
}
```

### Connection Management

```swift
// Use a specific connection for multiple operations
try await client.withConnection { connection in
    try await connection.changeDatabase("AdventureWorks")
    let tables = try await connection.listTables(schema: "dbo")
    return tables.count
}

// Connection state is automatically managed
print(connection.currentDatabase) // Shows current database
```

### Batch Processing and Scripts

```swift
// Execute multiple statements as separate batches
let statements = [
    "CREATE TABLE TestTable (id INT PRIMARY KEY, name NVARCHAR(50))",
    "INSERT INTO TestTable VALUES (1, N'Test')",
    "SELECT * FROM TestTable"
]
let results = try await client.executeSeparateBatches(statements)
print("Executed \(results.count) batches")

// Execute SQL script with GO separators
let script = """
CREATE TABLE Users (id INT PRIMARY KEY, name NVARCHAR(100))
GO
INSERT INTO Users VALUES (1, N'John Doe')
GO
SELECT COUNT(*) FROM Users
GO
"""
let scriptResults = try await client.executeScript(script)
```

### Streaming Large Results

```swift
try await client.withConnection { connection in
    for try await event in connection.streamQuery("SELECT * FROM LargeTable") {
        switch event {
        case .metadata(let columns):
            print("Columns: \(columns.map(\.name))")
        case .row(let row):
            // Process each row as it arrives
            processRow(row)
        case .done(let done):
            print("Batch complete, rows: \(done.rowCount)")
        case .message(let message):
            print("Server message: \(message.message)")
        }
    }
}
```

### Transactions

```swift
try await client.withConnection { connection in
    // Begin transaction
    _ = try await connection.execute("BEGIN TRANSACTION")
    
    do {
        // Your transactional operations
        _ = try await connection.execute("INSERT INTO Orders (customer_id) VALUES (123)")
        _ = try await connection.execute("UPDATE Inventory SET quantity = quantity - 1 WHERE product_id = 456")
        
        // Commit if all operations succeed
        _ = try await connection.execute("COMMIT")
    } catch {
        // Rollback on any error
        _ = try await connection.execute("ROLLBACK")
        throw error
    }
}
```

## Metadata Helpers

SQLServerNIO ships with a full metadata surface so application code no longer needs to hit `INFORMATION_SCHEMA` or `sys.*` directly.

- `listColumns` covers tables *and* views, exposing identity/computed flags, default definitions, collation, and ordinal position. Driver-internal artifacts (`meta_client_%`) are filtered automatically.
- `listParameters` returns stored procedure & function parameters (return values are flagged via `isReturnValue`).
- `listPrimaryKeys`, `listUniqueConstraints`, `listIndexes`, and `listForeignKeys` capture relational structure including column order, sort direction, included columns, and referential actions.
- `listDependencies` reports referencing objects (views, procedures, triggers, etc.) so you can build dependency graphs without custom SQL.
- `listProcedures`, `listFunctions`, and `listTriggers` list routines/triggers with system/shipped flags; set `metadataConfiguration.includeRoutineDefinitions = true` to hydrate definitions while listing.
- Database and parameter metadata flows through Microsoft’s stored procedures (`sp_databases`, `sp_sproc_columns_100`, `sp_fkeys`, etc.). Schema/table listings read from `sys.schemas`, `sys.tables`, and `sys.views` so they stay available even on SQL Server editions that omit the legacy `sp_schemata` procedure.

```swift
// Columns (tables & views)
let columns = try await client.listColumns(schema: "dbo", table: "Employees")
for column in columns {
    print("\(column.name) \(column.typeName) default=\(column.defaultDefinition ?? "<none>")")
}

// Procedure / function parameters
let parameters = try await client.listParameters(schema: "dbo", object: "usp_UpdateEmployee")
let inputs = parameters.filter { !$0.isReturnValue }

// Keys & indexes
let primaryKeys = try await client.listPrimaryKeys(schema: "dbo", table: "Employees")
let indexes = try await client.listIndexes(schema: "dbo", table: "EmployeeAudit")

// Foreign keys & dependencies
let foreignKeys = try await client.listForeignKeys(schema: "dbo", table: "EmployeeAudit")
let dependencies = try await client.listDependencies(schema: "dbo", object: "Employees")

// Routines & triggers
let procedures = try await client.listProcedures(schema: "dbo")
let functions = try await client.listFunctions(schema: "dbo")
let triggers = try await client.listTriggers(schema: "dbo", table: "EmployeeAudit")
```

> Routine listings return core metadata by default. Enable definition hydration by toggling the metadata configuration:

```swift
var clientConfig = makeSQLServerClientConfiguration()
clientConfig.connection.metadataConfiguration.includeRoutineDefinitions = true
let client = try await SQLServerClient.connect(configuration: clientConfig)

let procedures = try await client.listProcedures(schema: "dbo") // definitions populated
```

### Object definitions

When you need the full module text, use the definition helpers. They work with procedures, functions, views, and triggers, and report the SQL Server system flag so you can filter Microsoft-shipped objects.

```swift
let identifiers = [
    SQLServerMetadataObjectIdentifier(database: nil, schema: "dbo", name: "usp_ReportSales", kind: .procedure),
    SQLServerMetadataObjectIdentifier(database: nil, schema: "dbo", name: "vw_Sales", kind: .view)
]

let definitions = try await client.fetchObjectDefinitions(identifiers)

let single = try await client.fetchObjectDefinition(schema: "dbo", name: "usp_ReportSales", kind: .procedure)
print(single?.definition)
```

### Metadata search

Search across object names, definitions, columns, indexes, and constraints without crafting raw SQL:

```swift
let hits = try await client.searchMetadata(
    query: "Customer",
    scopes: [.objectNames, .definitions, .columns]
)

for hit in hits {
    print("\(hit.matchKind): \(hit.schema).\(hit.name) -> \(hit.detail ?? "")")
}
```

## Administration Helpers

```swift
let admin = SQLServerAdministrationClient(client: client)
let logins = try await admin.listServerLogins()

// Create tables with comprehensive options
let columns = [
    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
    SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), comment: "User name")))
]
try await admin.createTable(name: "Users", columns: columns)
```

## Stored Procedures and Functions

```swift
let routineClient = SQLServerRoutineClient(client: client)

// Create stored procedure
let parameters = [
    ProcedureParameter(name: "user_id", dataType: .int),
    ProcedureParameter(name: "result", dataType: .nvarchar(length: .length(100)), direction: .output)
]
let body = """
BEGIN
    SELECT @result = name FROM Users WHERE id = @user_id
END
"""
try await routineClient.createStoredProcedure(name: "GetUserName", parameters: parameters, body: body)

// Create scalar function
try await routineClient.createFunction(
    name: "CalculateAge",
    parameters: [FunctionParameter(name: "birth_date", dataType: .date)],
    returnType: .int,
    body: "BEGIN RETURN DATEDIFF(YEAR, @birth_date, GETDATE()) END"
)

// Create table-valued function
let tableDefinition = [
    TableValuedFunctionColumn(name: "id", dataType: .int),
    TableValuedFunctionColumn(name: "name", dataType: .nvarchar(length: .length(100)))
]
try await routineClient.createTableValuedFunction(
    name: "GetActiveUsers",
    tableDefinition: tableDefinition,
    body: "RETURN (SELECT id, name FROM Users WHERE active = 1)"
)
```

## Views and Indexed Views

```swift
let viewClient = SQLServerViewClient(client: client)

// Create view
try await viewClient.createView(
    name: "ActiveUsers",
    query: "SELECT id, name, email FROM Users WHERE active = 1"
)

// Create indexed view (materialized view)
try await viewClient.createIndexedView(
    name: "UserSummary",
    query: "SELECT department, COUNT_BIG(*) as user_count FROM dbo.Users GROUP BY department",
    indexName: "IX_UserSummary_department",
    indexColumns: ["department"]
)
```

## Index Management

```swift
let indexClient = SQLServerIndexClient(client: client)

// Create nonclustered index
try await indexClient.createIndex(
    name: "IX_Users_Email",
    table: "Users",
    columns: [IndexColumn(name: "email")]
)

// Create unique index
try await indexClient.createUniqueIndex(
    name: "IX_Users_Username_Unique",
    table: "Users",
    columns: [IndexColumn(name: "username")]
)

// Create index with included columns
try await indexClient.createIndex(
    name: "IX_Users_LastName_Incl",
    table: "Users",
    columns: [
        IndexColumn(name: "last_name"),
        IndexColumn(name: "first_name", isIncluded: true),
        IndexColumn(name: "email", isIncluded: true)
    ]
)

// Rebuild index
try await indexClient.rebuildIndex(name: "IX_Users_Email", table: "Users")
```

## Constraint Management

```swift
let constraintClient = SQLServerConstraintClient(client: client)

// Add foreign key constraint
try await constraintClient.addForeignKey(
    name: "FK_Orders_Users",
    table: "Orders",
    columns: ["user_id"],
    referencedTable: "Users",
    referencedColumns: ["id"],
    options: ForeignKeyOptions(onDelete: .cascade)
)

// Add check constraint
try await constraintClient.addCheckConstraint(
    name: "CK_Users_Age",
    table: "Users",
    expression: "age >= 0 AND age <= 150"
)

// Add unique constraint
try await constraintClient.addUniqueConstraint(
    name: "UQ_Users_Email",
    table: "Users",
    columns: ["email"]
)
```

## Trigger Management

```swift
let triggerClient = SQLServerTriggerClient(client: client)

// Create audit trigger
let auditBody = """
BEGIN
    SET NOCOUNT ON;
    INSERT INTO UserAudit (user_id, operation, audit_date)
    SELECT inserted.id, 'INSERT', GETDATE()
    FROM inserted;
END
"""
try await triggerClient.createTrigger(
    name: "tr_Users_Audit",
    table: "Users",
    timing: .after,
    events: [.insert, .update, .delete],
    body: auditBody
)

// Enable/disable triggers
try await triggerClient.disableTrigger(name: "tr_Users_Audit", table: "Users")
try await triggerClient.enableTrigger(name: "tr_Users_Audit", table: "Users")
```

## Security and User Management

```swift
let securityClient = SQLServerSecurityClient(client: client)

// Create database user
try await securityClient.createUser(name: "app_user", options: UserOptions(defaultSchema: "dbo"))

// Create custom role
try await securityClient.createRole(name: "data_analysts")

// Add user to role
try await securityClient.addUserToRole(user: "app_user", role: "data_analysts")

// Grant permissions
try await securityClient.grantPermission(permission: .select, on: "Users", to: "data_analysts")
try await securityClient.grantPermission(permission: .insert, on: "Users", to: "data_analysts")

// Add user to built-in database roles
try await securityClient.addUserToDatabaseRole(user: "app_user", role: .dbDataReader)

// List permissions
let permissions = try await securityClient.listPermissions(principal: "app_user")
```

## SQL Agent Helpers

```swift
let agent = SQLServerAgentClient(client: client)
let jobs = try await agent.listJobs()
```

## Error Handling & Retries


Errors surface as `SQLServerError`:

- `.authenticationFailed`
- `.connectionClosed`
- `.timeout(description:underlying:)`
- `.protocol(TDSError)`
- `.transient(Error)`
- `.unknown(Error)`

Configure retries via `SQLServerRetryConfiguration` on your connection/client configuration.

## Testing

1. Copy `.env.example` to `.env` and adjust credentials.
2. Start a SQL Server instance locally or via `docker/scripts/docker-compose.yml`.
3. Enable the suites you want using the `TDS_ENABLE_*` flags in `.env`.
4. Run `swift test` or open `SQLServerNIO.xctestplan` in Xcode.

```
TDS_HOSTNAME=127.0.0.1 TDS_PORT=1433 TDS_DATABASE=master TDS_USERNAME=sa TDS_PASSWORD=StrongPassword123! swift test
```

`SQLServerNIO.xctestplan` includes configurations for core, admin, and SQL Agent suites. Use environment flags such as `TDS_HOSTNAME`, `TDS_PORT`, `TDS_USERNAME`, `TDS_PASSWORD`, `TDS_DATABASE`, `TDS_ENABLE_SCHEMA_TESTS`, and `TDS_ENABLE_AGENT_TESTS`.

## Contributing

Issues and pull requests welcome. See `Docs/API_Audit.md` and `Docs/Roadmap.md` for active work. Security concerns? Please reach out privately.

## License

MIT. See [LICENSE].

[SwiftNIO]: https://github.com/apple/swift-nio
