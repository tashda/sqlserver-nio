# SQLServerNIO

SQLServerNIO is a non-blocking Swift client for Microsoft SQL Server built on SwiftNIO. It provides both connection pooling and direct connection management with comprehensive async/await and EventLoopFuture APIs.

## Key Features

- **Connection Management**: `SQLServerClient` for pooled connections with automatic retries, `SQLServerConnection` for direct connection control
- **Modern Swift APIs**: Full async/await support with EventLoopFuture fallbacks for compatibility
- **Transaction Support**: Proper transaction descriptor management with savepoints and isolation levels
- **Batch Processing**: SQL Server batch separation with GO delimiter support via `SQLServerQuerySplitter`
- **Metadata APIs**: Comprehensive database introspection (tables, columns, indexes, procedures, etc.)
- **Administrative Tools**: Server management via `SQLServerAdministrationClient` and SQL Agent via `SQLServerAgentClient`
- **Activity Monitor**: SSMS-like activity snapshots and streaming via `SQLServerActivityMonitor` (2008+)
- **Security Parity**: Securable-aware GRANT/REVOKE/DENY, application roles, schema helpers; extended server login types
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

// Server product version (e.g. 16.0.1000.5)
let productVersion = try await client.serverVersion()
print("Server version: \(productVersion)")

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

`SQLServerConnection.Configuration` lets you specify hostname, port, database, authentication, TLS, metadata caching, retry behaviour, transparent network IP resolution, and session defaults.

Session defaults mirror SQL Server Management Studio (ANSI settings, `SET NOCOUNT ON`, `SET FMTONLY OFF`) and can be customised via `SessionOptions`:

```swift
var configuration = makeSQLServerClientConfiguration()
configuration.connection.sessionOptions = .init(
    nocount: false,
    fmtOnlyOff: true,
    additionalStatements: ["SET DEADLOCK_PRIORITY LOW;"]
)
configuration.connection.transparentNetworkIPResolution = true // try all DNS answers before failing
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

### Stored Procedure Return Values (Typed)

When statements yield TDS RETURNVALUE tokens (typically via RPC), `SQLServerExecutionResult` exposes `returnValues` with typed accessors backed by the same converters used for row columns (`TDSData`).

```swift
let result = try await client.execute("EXEC dbo.usp_DoThing @Input = 1, @Out = @o OUTPUT").get()
for rv in result.returnValues {
    print("name=\(rv.name) int=\(rv.int as Any) string=\(rv.string as Any)")
}
```

Each `SQLServerReturnValue` contains:
- `name`: parameter name (e.g. `@Out`)
- `status`: status flags from TDS
- `value`: `TDSData?` with typed getters like `int`, `string`, `double`, `bool`, `bytes`, etc.

Note: SQL batches generally do not emit RETURNVALUE tokens; they are produced by RPC calls. The TDS layer fully parses these; a high‑level RPC wrapper can be added on request.

### Session State and Data Classification

The connection snapshots raw payloads from SESSIONSTATE and SQL DATACLASSIFICATION tokens for diagnostics:

```swift
let ss = connection.lastSessionStatePayload
let dc = connection.lastDataClassificationPayload
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

### Execution Options (experimental, no behavior changes yet)

You can pass optional execution options per query. Today these options are advisory and are ignored internally (behavior is unchanged). They provide a stable surface for clients to express preferences. Future versions may use these to select between simple/cursor modes, rowset sizing, and progress cadence.

```swift
import SQLServerKit

let options = SqlServerExecutionOptions(
    mode: .auto,                 // or .simple, .cursor
    rowsetFetchSize: nil,        // reserved for future rowset/cursor support
    progressThrottleMs: 120      // reserved for future package-level progress cadence
)

try await client.withConnection { connection in
    for try await event in connection.streamQuery("SELECT TOP 10000 * FROM dbo.Fixture", options: options) {
        switch event {
        case .metadata(let cols):
            // ...
            break
        case .row(let row):
            // ...
            break
        case .done(let done):
            // ...
            break
        case .message(let msg):
            // ...
            break
        }
    }
}
```

## Activity Monitor (2008+)

SQLServerKit exposes an SSMS-style Activity Monitor powered by DMVs. It supports both on-demand snapshots and a streaming API with configurable polling (default 5s). SQL text and plan retrieval are opt-in for performance.

```swift
let monitor = SQLServerActivityMonitor(client: client)

// One-shot snapshot (no text/plan by default)
let snap = try await monitor.snapshot()
print("Processes: \(snap.processes.count), waits: \(snap.waits.count)")

// Stream snapshots every 5 seconds (default)
for try await s in monitor.streamSnapshots(every: 5.0, options: .init(includeSqlText: false, includeQueryPlan: false)) {
    print("Top waits delta: \(s.waitsDelta?.first?.waitType ?? "<none>")")
}

// Include query text and/or plans (heavy; opt-in)
let rich = try await monitor.snapshot(options: .init(includeSqlText: true, includeQueryPlan: false))
print(rich.processes.first?.request?.sqlText ?? "<no text>")

// Management action: kill a session (requires privileges)
try await monitor.killSession(sessionId: 55)
```

Notes:
- Requires VIEW SERVER STATE to populate most panes.
- Designed for SQL Server 2008 and newer; fields missing on older editions are handled gracefully.
- Rates/deltas (waits, file I/O) are computed relative to the previous snapshot within the monitor instance.
- Text and plan retrieval can be expensive; keep them off unless needed.

## Security Parity (DB + Server)

Database‑scoped unified permissions and helpers:

```swift
let dbSec = SQLServerSecurityClient(client: client)

// GRANT on database (no ON clause)
try await dbSec.grant(permission: .connect, on: .database(nil), to: "public")

// GRANT on schema
try await dbSec.grant(permission: .alterAnySchema, on: .schema("dbo"), to: "dbo")

// GRANT column-level SELECT on object
let oid = ObjectIdentifier(schema: "dbo", name: "Employees", kind: .table)
try await dbSec.grant(permission: .select, on: .column(oid, ["id", "name"]), to: "public")

// Detailed permission listing including DB- and schema-scope
let perms = try await dbSec.listPermissionsDetailed(principal: "public")

// Application roles
try await dbSec.createApplicationRole(name: "AppRole", password: "p@ss", defaultSchema: "dbo")
try await dbSec.alterApplicationRole(name: "AppRole", newName: "AppRole2")
try await dbSec.dropApplicationRole(name: "AppRole2")

// Schema helpers
try await dbSec.createSchema(name: "ops", authorization: "dbo")
try await dbSec.transferObjectToSchema(objectSchema: "dbo", objectName: "SomeTable", newSchema: "ops")
try await dbSec.alterAuthorizationOnSchema(schema: "ops", principal: "dbo")
try await dbSec.dropSchema(name: "ops")
```

Server‑scoped login variants:

```swift
let srvSec = SQLServerServerSecurityClient(client: client)
try await srvSec.createCertificateLogin(name: "cert_login", certificateName: "MyCert")
try await srvSec.createAsymmetricKeyLogin(name: "asym_login", asymmetricKeyName: "MyKey")
try await srvSec.createExternalLogin(name: "aad_login")
```

Notes:
- Existing object‑level grant/revoke/deny APIs remain; the unified overloads add schema/database and column‑level support.
- For column grants, pass column names in the `.object(_,_,columns:)` securable.
- Agent credential helpers currently duplicate server credential methods; these will be deprecated in favor of the server client.

Notes:
- SQL Server uses SELECT TOP n or FETCH NEXT n ROWS ONLY, not LIMIT.
- Cursor mode may map to Simple until server cursor/rowset support is added.

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
- Comments via MS_Description (extended properties) are supported across tables, columns, routines, and triggers. Opt-in per call with `includeComments: true` to avoid extra joins when you don’t need them.
- `listParameters` returns stored procedure & function parameters (return values are flagged via `isReturnValue`).
- `listPrimaryKeys`, `listUniqueConstraints`, `listIndexes`, and `listForeignKeys` capture relational structure including column order, sort direction, included columns, and referential actions.
- `listDependencies` reports referencing objects (views, procedures, triggers, etc.) so you can build dependency graphs without custom SQL.
- `listProcedures`, `listFunctions`, and `listTriggers` list routines/triggers with system/shipped flags; set `metadataConfiguration.includeRoutineDefinitions = true` to hydrate definitions while listing.
- Database and parameter metadata flows through Microsoft’s stored procedures (`sp_databases`, `sp_sproc_columns_100`, `sp_fkeys`, etc.). Schema/table listings read from `sys.schemas`, `sys.tables`, and `sys.views` so they stay available even on SQL Server editions that omit the legacy `sp_schemata` procedure.

```swift
// Tables and columns with comments (opt-in)
let tables = try await client.listTables(schema: "dbo", includeComments: true)
let columns = try await client.listColumns(schema: "dbo", table: "Employees", includeComments: true)
for column in columns {
    print("\(column.name) \(column.typeName) comment=\(column.comment ?? "<none>")")
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

// Routines & triggers (comments opt-in)
let procedures = try await client.listProcedures(schema: "dbo", includeComments: true)
let functions = try await client.listFunctions(schema: "dbo", includeComments: true)
let triggers = try await client.listTriggers(schema: "dbo", table: "EmployeeAudit", includeComments: true)
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

// Execute-as / impersonation options
let executeAsOptions = RoutineOptions(executeAs: "dbo", withRecompile: true)
try await routineClient.createStoredProcedure(name: "GetAdminData", body: body, options: executeAsOptions)

// Create table-valued function
let tableDefinition = [
    TableValuedFunctionColumn(name: "id", dataType: .int),
    TableValuedFunctionColumn(name: "name", dataType: .nvarchar(length: .length(100)))
]
try await routineClient.createTableValuedFunction(
    name: "GetActiveUsers",
    tableDefinition: tableDefinition,
    body: """
    SELECT id, name
    FROM Users
    WHERE active = 1
    """
)
// Bodies that start with SELECT/WITH automatically become INSERT ... RETURN blocks.
// Provide your own BEGIN/INSERT statements if you need side effects or multiple phases.
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

## Bulk Copy

```swift
let bulkCopy = SQLServerBulkCopyClient(client: client)
let options = SQLServerBulkCopyOptions(
    table: "Costs",
    columns: ["category", "amount"],
    batchSize: 500
)
let rows = [
    SQLServerBulkCopyRow(values: [.nString("Hardware"), .decimal("123.45")]),
    SQLServerBulkCopyRow(values: [.nString("Software"), .decimal("300.00")])
]
let summary = try await bulkCopy.copy(rows: rows, options: options)
print("Inserted \\(summary.totalRows) rows across \\(summary.batchesExecuted) batches in \\(summary.duration)s")
```

## Table-Valued Parameters

```swift
let parameter = SQLServerTableValuedParameter(
    name: "NewUsers",
    columns: [
        .init(name: "id", dataType: .int),
        .init(name: "name", dataType: .nvarchar(length: .length(100)))
    ],
    rows: [
        .init(values: [.int(1), .nString("Alice")]),
        .init(values: [.int(2), .nString("Bob")])
    ]
)

try await client.execute("""
INSERT INTO dbo.Users (id, name)
SELECT id, name FROM @NewUsers;
""", tableParameters: [parameter])
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
// Database scope (backward compatible alias SQLServerDatabaseSecurityClient)
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

Check Agent service state via metadata and enumerate or start jobs.

```swift
// Agent status (running + XPs enabled)
let metadata = SQLServerMetadataClient(connection: try await client.withConnection { $0 })
let status = try await metadata.fetchAgentStatus()
print("Agent running=\(status.isSqlAgentRunning) enabled=\(status.isSqlAgentEnabled)")

// Optional: preflight environment and fail fast with guidance
let agent = SQLServerAgentClient(client: client)
try await agent.preflightAgentEnvironment().get() // set requireProxyPrereqs: true to validate proxy/credential perms

// List jobs and start one
let jobs = try await agent.listJobs()
if let job = jobs.first, status.isSqlAgentRunning, status.isSqlAgentEnabled {
    try await agent.startJob(named: job.name)
}

// Create a job, add a T-SQL step, attach to server, start/stop, enable/disable, fetch history
let jobName = "nio_sample_job"
try await agent.createJob(named: jobName, description: "Created from SQLServerNIO", enabled: true)
try await agent.addTSQLStep(jobName: jobName, stepName: "main", command: "SELECT 1;", database: "master")
// createJob automatically associates the job with the local server.
// addJobServer is safe to call again; it is effectively idempotent.
try await agent.addJobServer(jobName: jobName)
try await agent.startJob(named: jobName)
let running = try await agent.listRunningJobs()
print("Running jobs: \(running.map(\.name))")
let history = try await agent.listJobHistory(jobName: jobName, top: 10)
try await agent.stopJob(named: jobName) // no-op if already completed
try await agent.enableJob(named: jobName, enabled: false)
try await agent.deleteJob(named: jobName)

// Schedules, operators, alerts
// Create a daily schedule and attach to a job
try await agent.createSchedule(named: "nio_daily", enabled: true, freqType: 4, freqInterval: 1, activeStartTime: 0)
try await agent.attachSchedule(scheduleName: "nio_daily", toJob: jobName)
let schedules = try await agent.listSchedules(forJob: jobName)

// Operators and alerts
try await agent.createOperator(name: "nio_operator", emailAddress: "devnull@example.com")
try await agent.createAlert(name: "nio_severity1", severity: 1)
try await agent.addNotification(alertName: "nio_severity1", operatorName: "nio_operator", method: 1)
let operators = try await agent.listOperators()
let alerts = try await agent.listAlerts()

### Extended Agent Features

// Job categories and rename/owner
try await agent.createCategory(name: "nio_category")
try await agent.setJobCategory(named: jobName, categoryName: "nio_category")
try await agent.renameJob(named: jobName, to: "nio_job_renamed")
let currentLogin = try await client.queryScalar("SELECT SUSER_SNAME()", as: String.self) ?? "sa"
try await agent.changeJobOwner(named: "nio_job_renamed", ownerLoginName: currentLogin)

// Email notifications for jobs (requires operator, Database Mail for delivery)
try await agent.createOperator(name: "nio_operator", emailAddress: "devnull@example.com")
try await agent.setJobEmailNotification(jobName: "nio_job_renamed", operatorName: "nio_operator", notifyLevel: 2) // on failure

// Schedules: create, attach, list next run times
try await agent.createSchedule(named: "nio_daily", enabled: true, freqType: 4, freqInterval: 1, activeStartTime: 0)
try await agent.attachSchedule(scheduleName: "nio_daily", toJob: "nio_job_renamed")
let nextRuns = try await agent.listJobNextRunTimes(jobName: "nio_job_renamed")

// Generic steps for other subsystems (CmdExec, PowerShell, SSIS). Include optional proxy usage
try await agent.addStep(jobName: "nio_job_renamed", stepName: "cmd", subsystem: "TSQL", command: "SELECT 1;", database: "master")

// Step flow control and retry/output options
try await agent.configureStep(jobName: "nio_job_renamed", stepName: "cmd", onSuccessAction: 3, onFailAction: 2, retryAttempts: 1, retryIntervalMinutes: 1, outputFileName: "C:\\temp\\nio_job_cmd.txt", appendOutputFile: true)

// Start job from a specific step name (via stored procedure)
_ = try await client.execute("EXEC msdb.dbo.sp_start_job @job_name = N'nio_job_renamed', @step_name = N'cmd';")

// Proxies and credentials (require elevated permissions)
try await agent.createCredential(name: "nio_cred", identity: "id_nio", secret: "s3cr3t!")
try await agent.createProxy(name: "nio_proxy", credentialName: "nio_cred", description: "for CmdExec")
try await agent.grantLoginToProxy(proxyName: "nio_proxy", loginName: currentLogin)
try await agent.grantProxyToSubsystem(proxyName: "nio_proxy", subsystem: "CmdExec")
let proxies = try await agent.listProxies()

### Testing Agent end-to-end

The integration tests are gated by environment flags so you can selectively run them against your instance:
- `TDS_ENABLE_AGENT_TESTS=1` — core job lifecycle (create, steps, start/stop, running list, history)
- `TDS_ENABLE_AGENT_SCHEDULE_TESTS=1` — schedules (create, attach/detach, list, next run times)
- `TDS_ENABLE_AGENT_ALERT_TESTS=1` — operators and alerts
- `TDS_ENABLE_AGENT_SECURITY_TESTS=1` — agent role/permission checks (creates logins/users)
- `TDS_ENABLE_AGENT_PROXY_TESTS=1` — proxies and credentials (requires elevated permissions)

Run a focused suite:

```
env $(grep -v '^#' .env | xargs) TDS_ENABLE_AGENT_TESTS=1 swift test --filter SQLServerAgentTests
```
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
### Server Security (Logins/Roles/Credentials/Permissions)

Use `SQLServerServerSecurityClient` for server‑level management. It exposes CRUD for logins and credentials, role membership, and server permissions, with both async/await and EventLoopFuture variants. You can construct it with either a pooled `SQLServerClient` or a single `SQLServerConnection`:

```swift
let serverSec = SQLServerServerSecurityClient(client: client)

// List logins
let logins = try await serverSec.listLogins()

// Create a SQL login (CHECK_POLICY/EXPIRATION and defaults supported)
try await serverSec.createSqlLogin(
    name: "app_login",
    password: "Str0ngP@ss!",
    options: .init(defaultDatabase: "MyDb", checkPolicy: true, checkExpiration: false)
)

// Add to a server role
try await serverSec.addMemberToServerRole(role: "securityadmin", principal: "app_login")

// Grant server permission
try await serverSec.grant(permission: .viewServerState, to: "app_login")

// Credentials
try await serverSec.createCredential(name: "s3_cred", identity: "DOMAIN\\svc_user", secret: "secret")
try await serverSec.alterCredential(name: "s3_cred", identity: nil, secret: "newSecret")
try await serverSec.dropCredential(name: "s3_cred")
```

Note: Server‑level tests and operations may require `sysadmin` or specific server permissions (e.g., `ALTER ANY CREDENTIAL`). Gate integration tests with `TDS_ENABLE_SERVER_SECURITY_TESTS=1`.
