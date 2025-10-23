
<p align="center">
  <img src="https://raw.githubusercontent.com/sqlserver-nio/.github/main/images/sqlserver-nio.svg" height="96" alt="SQLServerNIO" />
  <br><br>
  <a href="https://sqlserver-nio.github.io/documentation">
    <img src="https://design.vapor.codes/images/readthedocs.svg" alt="Documentation" />
  </a>
  <a href="LICENSE">
    <img src="https://design.vapor.codes/images/mitlicense.svg" alt="MIT License" />
  </a>
  <a href="https://github.com/sqlserver-nio/sqlserver-nio/actions/workflows/test.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/sqlserver-nio/sqlserver-nio/test.yml?event=push&style=plastic&logo=github&label=tests&logoColor=%23ccc" alt="Continuous Integration" />
  </a>
  <a href="https://swift.org">
    <img src="https://design.vapor.codes/images/swift60up.svg" alt="Swift 6.0+" />
  </a>
</p>

🧱 Non-blocking, event-driven Swift client for Microsoft SQL Server built on top of [SwiftNIO].

- `SQLServerConnection` gives you fine-grained control over a single TDS session.
- `SQLServerClient` manages pooled connections with retries and metadata caching.
- Async/await-first APIs with convenient futures-based overloads for legacy code.
- Metadata helpers (databases, schemas, tables, columns) with safe fallbacks.
- Additional clients for administrative tasks and SQL Agent workflows.


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

### Pooled client

```swift
import SQLServerNIO

let configuration = SQLServerClient.Configuration(
    hostname: "localhost",
    port: 1433,
    login: .init(
        database: "master",
        authentication: .sqlPassword(username: "sa", password: "StrongPassword123")
    ),
    tlsConfiguration: nil
)

let client = try await SQLServerClient.connect(configuration: configuration)
let rows = try await client.query("SELECT @@VERSION AS version;")
print(rows.first?.column("version")?.string ?? "<unknown>")
try await client.shutdownGracefully()
```

### Single connection

```swift
let single = try await SQLServerConnection.connect(configuration: configuration.connection)
let databases = try await single.listDatabases()
try await single.close()
```

## Connection Configuration

`SQLServerConnection.Configuration` lets you specify hostname, port, database, authentication, TLS, metadata caching, and retry behaviour.

## Querying

### Async/await

```swift
let rows = try await client.query("SELECT name FROM sys.databases ORDER BY name;")
for row in rows {
    print(row.column("name")?.string ?? "<unknown>")
}
```

### Futures

```swift
client.query("SELECT COUNT(*) AS count FROM sys.tables;").whenSuccess { rows in
    print(rows.first?.column("count")?.int ?? 0)
}
```

### Parameterised SQL

Use string interpolation for safe parameter binding:

```swift
let dbName = "AdventureWorks2022"
try await client.query(
    """
    IF DB_ID(\(dbName)) IS NULL CREATE DATABASE \(dbName);
    """
)
```

## Metadata Helpers

```swift
let columns = try await client.listColumns(schema: "dbo", table: "Employees")
```

## Administration Helpers

```swift
let admin = SQLServerAdministrationClient(client: client)
let logins = try await admin.listServerLogins()
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
