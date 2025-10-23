# SQLServerNIO Tightening Roadmap

## Completed
- Module renamed to `SQLServerNIO`; public surface centered on `SQLServerConnection`/`SQLServerClient`.
- Added async/await adapters across connection, client, and metadata helpers.
- Introduced administration and agent clients for server-wide tasks.
- Normalised errors via `SQLServerError` so callers see consistent failures.
- Implemented metadata fallback from `sys.columns` to `INFORMATION_SCHEMA` when necessary.
- Added starter Xcode test plan (`SQLServerNIO.xctestplan`) with core/admin/agent configurations.

## Next Steps
1. **Visibility tightening**
   - Mark protocol plumbing (`TDSConnection`, token parsers, raw requests) as `internal` once remaining call sites migrate.
   - Expose only the high-level clients in `public` API.
2. **Async-first samples**
   - Update README snippets and add doc comments using async variants.
   - Add a quick-start playground once async wrappers stabilise.
3. **Retry heuristics**
   - Refine `SQLServerRetryConfiguration.shouldRetry` default to detect transient vs. fatal errors.
   - Document recommended policies for long-running apps.
4. **Broader coverage**
   - Expand integration suites for permissions, backup/restore, Service Broker.
   - Introduce matrix testing for different authentication methods (SQL, Kerberos, Azure AD).
5. **Performance parity**
   - Profile metadata queries against JDBC driver and align batching strategies.
   - Investigate connection warm-up (SET options) for parity with SSMS.
6. **CLI/Tooling**
   - Ship command-line utility for quick diagnostics (optional but useful during development).
7. **Migration guide**
   - Document breaking changes from `swift-tds` (module rename, new API surface) in `Docs/Migration.md`.
