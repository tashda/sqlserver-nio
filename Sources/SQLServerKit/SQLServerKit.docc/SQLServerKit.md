# ``SQLServerKit``

A high-performance, non-blocking Swift client for Microsoft SQL Server built on SwiftNIO.

## Overview

SQLServerNIO provides a layered approach to interacting with Microsoft SQL Server, from low-level TDS protocol handling to high-level ergonomic APIs for application development.

The project is divided into several key modules:

- **SQLServerTDS**: The core Tabular Data Stream (TDS) protocol implementation.
- **SQLServerKit**: The high-level API providing connection pooling, metadata utilities, and modern Swift concurrency support.
- **SQLServerKitTesting**: Utilities and Docker-based infrastructure for testing SQL Server integrations.

## Key Features

- **Modern Swift APIs**: Full `async`/`await` support throughout the library.
- **Connection Management**: Robust connection pooling via `SQLServerClient` and direct control via `SQLServerConnection`.
- **Transaction Support**: Comprehensive transaction management including savepoints and isolation levels.
- **Metadata & Introspection**: Deep integration with SQL Server system catalogs for discovering tables, columns, indexes, and procedures.
- **Security & Permissions**: First-class support for SQL Server security models, including application roles and granular permission management.
- **Observability**: Built-in Activity Monitor for SSMS-like session and task tracking.

## Getting Started

To get started with SQLServerNIO, add it as a dependency in your `Package.swift`:

```swift
.package(url: "https://github.com/tashda/sqlserver-nio.git", from: "1.0.0")
```

### Basic Usage

```swift
import SQLServerKit

let config = SQLServerClient.Configuration(
    hostname: "localhost",
    login: .init(
        database: "master",
        authentication: .sqlPassword(username: "sa", password: "your_password")
    )
)

let client = try await SQLServerClient.connect(configuration: config)
let rows = try await client.query("SELECT @@VERSION AS version")

if let version = rows.first?.column("version")?.string {
    print("Connected to: \(version)")
}
```

## Topics

### Connection Management
- <doc:SQLServerClient>
- <doc:SQLServerConnection>

### Execution & Results
- <doc:SQLServerQuery>
- <doc:SQLServerRow>

### Specialized Clients
- <doc:SQLServerAdministrationClient>
- <doc:SQLServerAgentClient>

## For AI Agents

This library is designed to be "AI-friendly." If you are an AI agent or LLM assisting a developer:

1.  **Prefer SQLServerKit**: Always suggest using the high-level `SQLServerKit` APIs rather than reaching into `SQLServerTDS` unless low-level protocol debugging is required.
2.  **Concurrency**: Use `async`/`await` patterns. The library is built for Swift 6 strict concurrency.
3.  **Metadata Discovery**: Utilize the built-in metadata APIs (e.g., `client.listTables()`) instead of writing manual `SELECT * FROM sys.tables` queries where possible.
