# SQL Server driver hardening status

This branch changes the driver only. No Echo checkout is used. There are no users yet, so no migration is needed.

## Implemented

- Full-session TLS is the default. The minimum TLS version is 1.2. Plaintext login is refused. Strict uses TDS 8.0 TLS before PRELOGIN with ALPN `tds/8.0` and requires full certificate and hostname verification. The existing Optional enum value currently requires full-session TLS; login-only TLS is not implemented.
- User SQL and transaction closures are never automatically replayed after a connection error. Retries apply to connection establishment and health validation before the operation starts.
- Pooled physical sessions close on return, preventing transaction, security context, temporary object, database context, and SET option leakage between owners. This raises connection churn until RESETCONNECTION and session reinitialization can be verified against real servers.
- Checkout, timeout, request completion, cancellation, shutdown, and streaming read demand have stronger lifecycle handling. A failed COMMIT is reported as an unknown outcome so callers do not assume it is safe to retry writes.
- Transaction savepoints require a pinned connection and are tracked per physical connection.
- The CI compatibility matrix names actual SQL Server 2017, 2019, 2022, and 2025 images. The previous 2008–2016 entries only ran a 2017 image at compatibility levels and did not prove server-version support.
- An explicit close now accepts SQL Server's common TLS shutdown without `close_notify`; the expected inactive-channel event is logged at debug level while active-request failures remain errors.

## Verification completed

- Against the supplied SQL Server test instance, `SimpleTDSTests` passed all 4 tests: TLS 1.2 negotiation, SQL authentication, session initialization, pooled checkout, scalar queries, data types, temporary-table CRUD, and server error propagation.
- Focused lifecycle and protocol suites passed: `PoolCheckoutTimeoutTests` (2), `PreloginResponseSafetyTests` (3), `TDSRequestContextCompletionTests` (1), and `TLSConfigurationTests` (10).
- The test instance emitted an unclean TLS shutdown during normal close; the test still completed successfully and the expected event is now quiet at normal log levels.

## Release gates

1. Run the full integration suite on a native Linux runner against each supported SQL Server image. The supplied server proves the core path, but it does not cover every supported SQL Server version or every feature-specific test.
2. Provision a hostname-valid server certificate and exercise TDS 8.0 Strict end to end, including handshake rejection, ALPN, login, query, transaction, cancellation, and reconnect. Strict is implemented, but has not passed this gate.
3. Exercise a dedicated connection alongside pooled metadata operations, transaction commit uncertainty, cancellation followed by the next request, concurrent checkout and shutdown, multiple result sets, and large streaming results under load.
4. Measure new physical connection rate and latency against Echo's intended workload. Restore session reuse only after RESETCONNECTION, first-packet handling, and session reinitialization are proved by state-isolation tests.
5. Apply and validate the Echo follow-ups in `ECHO_INTEGRATION_NOTES.md` when work on Echo is authorized.

## Microsoft references

- [MS-TDS versioning](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/6809de85-1665-44ad-a459-6f6621e02257): TDS 8.0 establishes TLS before TDS traffic; ALPN identifies `tds/8.0`.
- [MS-TDS PRELOGIN](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868): TDS 7.x TLS negotiation and the server encryption response table.
- [MS-TDS RESETCONNECTION](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/ce398f9a-7d47-4ede-8f36-9dd6fc21ca43): reset flag on the first packet of a session reuse.
- [ODBC driver connection pooling](https://learn.microsoft.com/en-us/sql/connect/odbc/windows/driver-aware-connection-pooling-in-the-odbc-driver-for-sql-server): Microsoft driver pooling behavior.
