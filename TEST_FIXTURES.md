# Test Fixture Policy

`sqlserver-nio` owns the canonical MSSQL integration fixture model.

## Rules

- Fixtures always run through `bootstrap`, `validate`, and `repair/recreate`.
- Validation is mandatory on every run, even when a Docker container is reused.
- Ambient long-lived containers are never trusted without validation.
- Fixture failures must be reported as fixture/bootstrap failures, not as driver regressions.

## Canonical Fixture

- Engine: SQL Server Docker container
- Sample database: official AdventureWorks restore
- Entry points:
  - library: `ensureSQLServerTestFixture(requireAdventureWorks:)`
  - CLI: `swift run --package-path . sqlserver-test-fixture --require-adventureworks`

## Workflow Expectations

- CI may reuse containers for speed.
- Reused containers must be revalidated.
- If validation fails, the fixture must be repaired or recreated automatically before tests proceed.
