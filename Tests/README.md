# SQLServerNIO Test Suite

This directory contains a restructured and organized test suite for SQLServerNIO, replacing the scattered EventDrivenMigrationTests with a more maintainable and logical structure.

## Test Structure

```
Tests/
├── CoreTests/                    # Core functionality tests
│   ├── ConnectionTests.swift     # Connection management, pooling, lifecycle
│   ├── QueryTests.swift          # SQL queries, parameters, results
│   └── StreamingTests.swift      # Streaming functionality, callbacks
├── IntegrationTests/             # End-to-end workflow tests
│   ├── WorkflowTests.swift       # Complete user workflows
│   └── PerformanceTests.swift    # Performance benchmarks and metrics
├── MigrationTests/               # Migration and compatibility tests
│   ├── NodeMSSQLCompatibilityTests.swift  # node-mssql compatibility
│   └── ArchitectureTests.swift   # Architecture validation
├── EventDrivenMigrationTests_Backup/  # Original tests (archived)
└── README.md                     # This file
```

## Test Categories

### CoreTests

**Purpose**: Test fundamental SQLServerNIO functionality

- **ConnectionTests.swift**: Connection lifecycle, pooling, error handling
- **QueryTests.swift**: Basic SQL queries, parameters, transactions, batch queries
- **StreamingTests.swift**: Streaming functionality, memory efficiency, performance

### IntegrationTests

**Purpose**: Test complete user workflows and performance characteristics

- **WorkflowTests.swift**: End-to-end workflows from connection to data retrieval
- **PerformanceTests.swift**: Performance benchmarks, memory usage, concurrency

### MigrationTests

**Purpose**: Test migration scenarios and compatibility with other libraries

- **NodeMSSQLCompatibilityTests.swift**: Compatibility with node-mssql patterns
- **ArchitectureTests.swift**: Architecture validation and design pattern tests

## Running Tests

### Quick Test Commands

```bash
# Run all core tests
swift test --filter CoreTests

# Run all integration tests
swift test --filter IntegrationTests

# Run all migration tests
swift test --filter MigrationTests

# Run specific test file
swift test --filter ConnectionTests
swift test --filter QueryTests
swift test --filter StreamingTests

# Run specific test method
swift test --filter ConnectionTests.testDirectTDSConnection
```

### Environment Setup

Copy the environment configuration template and configure for your SQL Server:

```bash
cp Tests/EnvironmentConfig.swift.template Tests/EnvironmentConfig.swift
# Edit Tests/EnvironmentConfig.swift with your SQL Server details
```

### Test Environment Variables

```bash
export TDS_ENV=production                    # Use production environment
export TDS_HOSTNAME=your-server             # SQL Server hostname
export TDS_PORT=1433                        # SQL Server port
export TDS_USERNAME=your-username           # SQL Server username
export TDS_PASSWORD=your-password           # SQL Server password
export TDS_DATABASE=your-database           # SQL Server database
```

## Test Descriptions

### CoreTests.ConnectionTests

- `testDirectTDSConnection`: Direct TDS connection without pooling
- `testSQLServerClientConnect`: Client-based connection with pooling
- `testWithConnectionBasicQuery`: Basic query using connection pool
- `testConnectionReuse`: Verify connection reuse in pool
- `testConnectionCleanup`: Proper connection cleanup
- `testConnectionErrorHandling`: Error handling for invalid connections

### CoreTests.QueryTests

- `testBasicSQLQuery`: Simple SELECT query
- `testMultipleRowQuery`: Query returning multiple rows
- `testParameterizedQuery`: Parameter binding
- `testBasicDataTypes`: Different SQL data types
- `testInvalidSQLQuery`: Error handling for invalid SQL
- `testSQLSyntaxError`: Syntax error handling
- `testBatchQueries`: Multiple SQL statements in one request
- `testSimpleTransaction`: Basic transaction support
- `testTransactionRollback`: Transaction rollback on error
- `testQueryPerformance`: Query performance measurement

### CoreTests.StreamingTests

- `testBasicQueryStreamWithCallbacks`: Streaming with callback functions
- `testAsyncQueryStream`: Streaming using AsyncSequence
- `testStreamingMemoryUsage`: Memory efficiency of streaming
- `testStreamingPerformance`: Streaming performance benchmarks
- `testConnectionHeldDuringStreaming`: Connection management during streaming
- `testStreamingErrorHandling`: Error handling in streaming mode
- `testLargeDatasetStreaming`: Large dataset streaming
- `testConcurrentStreamingOperations`: Multiple concurrent streams

### IntegrationTests.WorkflowTests

- `testCompleteSQLWorkflow`: End-to-end SQL workflow
- `testDataAnalysisWorkflow`: Data analysis workflow simulation
- `testErrorRecoveryWorkflow`: Error handling and recovery
- `testConcurrentWorkflowOperations`: Concurrent workflow operations

### IntegrationTests.PerformanceTests

- `testConnectionPerformance`: Connection establishment performance
- `testQueryPerformance`: Query execution performance
- `testBatchQueryPerformance`: Batch query performance
- `testStreamingPerformance`: Streaming performance
- `testMemoryEfficiency`: Memory usage efficiency
- `testConcurrentPerformance`: Concurrent operation performance

### MigrationTests.NodeMSSQLCompatibilityTests

- `testNodeMSSQLBasicQueryPattern`: Basic query compatibility
- `testNodeMSSQLStreamingPattern`: Streaming compatibility
- `testNodeMSSQLEventDrivenPattern`: Event-driven compatibility
- `testNodeMSSQLConnectionPoolPattern`: Connection pool compatibility
- `testNodeMSSQLErrorHandlingPattern`: Error handling compatibility
- `testNodeMSSQLBatchQueryPattern`: Batch query compatibility
- `testNodeMSSQLTransactionPattern`: Transaction compatibility
- `testNodeMSSQLParameterBindingPattern`: Parameter binding compatibility
- `testArchitectureGapAnalysis`: Architecture gap analysis

### MigrationTests.ArchitectureTests

- `testTDSRequestProtocolCompliance`: Protocol compliance testing
- `testConnectionArchitecturePatterns`: Connection architecture validation
- `testStreamingArchitecture`: Streaming architecture testing
- `testErrorHandlingArchitecture`: Error handling architecture
- `testMemoryManagementArchitecture`: Memory management validation
- `testThreadSafetyArchitecture`: Thread safety testing
- `testPerformanceArchitecture`: Performance architecture validation

## Benefits of New Structure

1. **Organization**: Tests are logically grouped by functionality
2. **Maintainability**: Easier to find and update specific tests
3. **Coverage**: Comprehensive coverage of all SQLServerNIO features
4. **Performance**: Dedicated performance testing suite
5. **Migration**: Clear migration path from node-mssql
6. **Documentation**: Each test is well-documented with its purpose

## Running Specific Test Categories

### Development Testing

For rapid development iteration:

```bash
# Quick functionality check
swift test --filter ConnectionTests --quiet
swift test --filter QueryTests.testBasicSQLQuery --quiet

# Performance regression testing
swift test --filter PerformanceTests.testQueryPerformance --quiet
```

### CI/CD Testing

For automated testing:

```bash
# Full test suite
swift test

# Core functionality only
swift test --filter CoreTests

# Critical path tests
swift test --filter "ConnectionTests.testDirectTDSConnection OR QueryTests.testBasicSQLQuery OR StreamingTests.testBasicQueryStreamWithCallbacks"
```

### Migration Testing

When migrating from node-mssql:

```bash
# Compatibility tests
swift test --filter NodeMSSQLCompatibilityTests

# Architecture validation
swift test --filter ArchitectureTests
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify environment variables and SQL Server accessibility
2. **Permission Errors**: Ensure the SQL user has appropriate permissions
3. **Test Timeouts**: Increase timeout values for slow networks
4. **Memory Issues**: Reduce test data sizes for memory-constrained environments

### Debug Mode

Enable debug logging:

```bash
export TDS_LOG_LEVEL=debug
swift test --filter ConnectionTests.testDirectTDSConnection
```

## Contributing

When adding new tests:

1. Choose the appropriate test category (Core, Integration, Migration)
2. Follow existing naming conventions (`test...` prefix)
3. Include proper documentation comments
4. Add performance assertions where appropriate
5. Test both success and error scenarios

## Migration from EventDrivenMigrationTests

The original EventDrivenMigrationTests have been consolidated and improved:

- **Phase0-Phase8 tests**: Consolidated into CoreTests and IntegrationTests
- **Duplicate functionality**: Removed and merged into logical groups
- **Debug tests**: Integrated into appropriate test categories
- **Performance tests**: Centralized in PerformanceTests
- **Compatibility tests**: Organized in NodeMSSQLCompatibilityTests

All test functionality has been preserved while improving organization and maintainability.