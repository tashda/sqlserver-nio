# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQLServerNIO is a non-blocking Swift client for Microsoft SQL Server built on SwiftNIO, providing both connection pooling and direct connection management with comprehensive async/await and EventLoopFuture APIs.

## Build & Test Commands

### Building
```bash
swift build                    # Build all targets
swift build -c release         # Build release configuration
```

### Testing
```bash
# Run full test suite against the configured backend
swift test

# Run specific test targets
swift test --filter SQLServerTableTests
swift test --filter SQLServerConnectionTests
swift test --filter SQLServerMetadataTests
swift test --filter SQLServerTDSTests          # TDS unit tests (no server needed)

# Docker-backed matrix testing
USE_DOCKER=1 TDS_VERSION=2022-latest TDS_DOCKER_PORT=14331 swift test
USE_DOCKER=1 TDS_VERSION=2022-latest TDS_DOCKER_PORT=14331 TDS_LOAD_ADVENTUREWORKS=1 TDS_AW_DATABASE=AdventureWorks swift test

# External-server testing
TDS_HOSTNAME=192.168.1.200 TDS_PORT=1435 TDS_USERNAME=sa TDS_PASSWORD='<password>' TDS_DATABASE=master swift test

# Full local version matrix
./test-all-sql-versions.sh
```

### Environment Setup
Use only two environment modes:
- Docker matrix mode: `USE_DOCKER`, `TDS_VERSION`, `TDS_DOCKER_PORT`, optional `TDS_LOAD_ADVENTUREWORKS`, `TDS_AW_DATABASE`
- External server mode: `TDS_HOSTNAME`, `TDS_PORT`, `TDS_USERNAME`, `TDS_PASSWORD`, `TDS_DATABASE`

Do not add test-selection or skip flags. A run should execute the full suite against the configured backend.

## Architecture

### Two-Layer Design
- **SQLServerKit** (`Sources/SQLServerKit/`): High-level Swift APIs with async/await support
- **SQLServerTDS** (`Sources/SQLServerTDS/`): Low-level Tabular Data Stream (TDS) protocol implementation

### Key Clients
- **SQLServerClient**: Pooled connection management with automatic retries (primary API)
- **SQLServerConnection**: Direct connection control for advanced scenarios
- **SQLServerMetadataClient**: Database introspection (tables, columns, indexes, etc.)
- **SQLServerAdministrationClient**: Server management (tables, users, etc.)
- **SQLServerAgentClient**: SQL Server Agent job management
- **SQLServerSecurityClient**: Database security and permissions

### Core Features
- Connection pooling with configurable pools
- Transaction support with savepoints and isolation levels
- Batch processing with SQL Server GO delimiter support
- Streaming support via AsyncSequence for large datasets
- Comprehensive error handling with proper SQL Server error propagation

## Test Architecture

The test suite recently underwent major refactoring that eliminated 7,300+ lines of duplicate code. All test infrastructure is now centralized in `Sources/SQLServerKit/TestInfrastructure.swift`.

### Test Organization
Tests are organized by functional areas in `Tests/`:
- **ConnectionTests**: Connection lifecycle and reliability
- **TableTests**: Table operations and management
- **IndexTests**: Index creation and management
- **SecurityTests**: Security and permissions
- **AgentTests**: SQL Server Agent functionality
- **PerformanceTests**: Performance benchmarks
- **IntegrationTests**: End-to-end testing

### Standard Test Pattern
All tests follow the centralized infrastructure pattern:
```swift
final class SQLServerExampleTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        // Create client using makeSQLServerClientConfiguration()
    }

    func testExampleFeature() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "example") { db in
            // Test logic
        }
    }
}
```

### Key Testing Utilities
- `withTemporaryDatabase()`: Creates temporary databases for isolated testing
- `withReliableConnection()`: Provides connections with automatic retry logic
- `TestEnvironmentManager`: Centralized environment configuration management
- `makeSQLServerClientConfiguration()`: Creates connection configuration based on current environment

## Important Notes

### Security
- Environment configuration files containing server credentials are in `.gitignore`
- Never commit sensitive connection information
- Use environment variables for connection details in production

### Development Patterns
- Use async/await APIs preferentially over EventLoopFuture
- Leverage the pooled client (`SQLServerClient`) for most use cases
- Treat `SQLServerClient` as the single consumer-facing entry point; expose domain helpers through client namespaces such as `client.metadata`, `client.agent`, `client.admin`, and `client.security`
- Use specialized clients (Metadata, Administration, etc.) for specific operations
- Do not expose `SQLServerTDS`, `TDS*` types, `EventLoopFuture`, `EventLoop`, `EventLoopGroup`, or `TimeAmount` from the public `SQLServerKit` API surface; wrap them in `SQLServerKit` types and keep NIO/TDS internal
- Prefer the async-only table administration APIs (`createTable`, `dropTable`, `renameTable`, `truncateTable`) and avoid reintroducing future/async overload pairs that create Swift 6 overload-resolution ambiguity
- When table administration needs a non-default database, use the database-scoped admin client (`SQLServerAdministrationClient(client: ..., database: ...)` / `.scoped(to:)`) or pass the explicit `database:` parameter; do not rely on ad-hoc `USE [...]` in tests
- Follow the established test patterns using centralized infrastructure
- For TDS/protocol issues, consult `tds-mcp` before changing protocol code
- When behavior is unclear at runtime, use `wiremcp` to capture/sniff TDS traffic during a failing test and compare it against `tds-mcp`
- Tests must use `sqlserver-nio` client APIs for operational work; if a typed API is missing, add it first and then migrate the test off handwritten DDL/DML
- Keep local CLI, Xcode test plans, and GitHub Actions version matrices aligned

### SQL Server Compatibility
- Supports SQL Server 2008 and later
- Implements comprehensive TDS protocol support
- Handles SQL Server-specific features like batches, transactions, and metadata queries
