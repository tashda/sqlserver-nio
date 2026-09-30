# Echo integration notes

Changes Echo needs when it moves to this driver revision. The driver branch does not touch the Echo repository. There are no users yet, so no migration is needed.

## Must change (Echo will not compile or will misbehave otherwise)

1. **`SQLServerError` switch is no longer exhaustive.** `DatabaseError.from(sqlServerError:)` switches over every case. Add `case .commitOutcomeUnknown:` and map it to a distinct, non-retryable error that tells the user the transaction may or may not have committed. `.sqlExecutionError` and `.deadlockDetected` gained a second associated value (`details`); Echo's pattern `case .sqlExecutionError, .deadlockDetected:` still compiles unchanged.
2. **Use the structured server error.** `error.serverDetails` carries number, severity, state, line, procedure and every message of the batch. Echo's Messages pane currently shows only `localizedDescription` for thrown errors. Also use `error.isConnectionLost` (reconnect needed, outcome of the running statement unknown) and `error.isTransient` instead of matching message text.
3. **Remove the 45-second task-group timer** in `MSSQLDedicatedQuerySession+Queries.simpleQuery`. Cancelling the query task now cancels the request on the server and waits for SQL Server's acknowledgement, so the connection stays usable. If Echo wants a deadline, call `connection.execute(sql, timeout:)` or set `SessionOptions.defaultQueryTimeout`; the timeout throws `SQLServerError.timeout` and the connection remains usable.
4. **Stop reconnecting after cancellation.** `MSSQLDedicatedQuerySession.reconnectAfterCancellation()` discards the session (temp tables, SET options, open transaction) after every cancel. It is only needed when `error.isConnectionLost` is true or `connection.isClosed`. Tell the user when a reconnect discards session state.
5. **Encryption modes.** `MSSQLNIOFactory` maps Optional to `tlsEnabled: false`. The driver now handles that safely: Optional with no TLS configuration encrypts the whole session without validating the certificate (Microsoft's Optional validates nothing and only encrypts the login). Mandatory and Strict require a TLS configuration and validate the certificate unless `trustServerCertificate` is set. Recommended defaults for new connections: Mandatory with validation. Strict needs a certificate whose name matches the host.
6. **Set the application name.** `SQLServerConnection.Configuration.applicationName` (defaults to `sqlserver-nio`) is what DBAs see in `sys.dm_exec_sessions.program_name`, audits and Resource Governor classifiers. Set it to `Echo`.

## Should change

7. **Dedicated session ownership.** `MSSQLDedicatedQuerySession` mutates `connection` and `reconnectTask` under `@unchecked Sendable`. Serialize access with an actor. Two overlapping calls on one TDS connection are now queued safely by the driver, but the reconnect swap itself is still racy in Echo.
8. **Current database.** `SQLServerConnection.currentDatabase` now follows every `USE`, including one inside a user batch or procedure (ENVCHANGE tracking). Echo can drop any manual tracking and read it after each execution.
9. **Streaming.** `streamQuery` now reads from the socket only as fast as Echo consumes the sequence, so memory stays bounded. Echo still accumulates additional result sets (`additionalResults`, `currentAdditionalRows`) and every row of `executeBatches` in memory. Apply the same spooling or a row cap there, otherwise a large second result set can still exhaust memory in Echo itself.
10. **Pooled stream leases.** Prefer `client.withStreamQuery(sql) { stream in ... }` over `client.streamQuery(sql)`, which hands the caller a pooled connection that must be closed manually.
11. **Dedicated connections share threads.** `SQLServerConnection.connect(configuration:)` now uses NIO's shared event loop group instead of one thread per connection. Nothing to change unless Echo wants its own group (`connect(configuration:eventLoopGroup:)`).
12. **Pool behaviour.** Pooled sessions are now reused after a server-side reset (RESETCONNECTION) instead of being closed on every return. Metadata-heavy screens need far fewer logins. `SQLServerClient.shutdownGracefully()` now closes still-running operations after 10 seconds instead of waiting forever.

## Verification to run in Echo

- Query tab: run, cancel mid-result, run again; the second result must be correct and the tab's temp tables must survive the cancel.
- Query tab: `BEGIN TRAN; INSERT ...;` then cancel a later long query; with `XACT_ABORT ON` (driver default) the transaction is rolled back — surface that to the user.
- Result grid: `SELECT` of several million rows with a slow grid; Echo's memory should stay flat apart from its own spooling.
- Object explorer under load while a query tab streams: no stalls, no mixed-up results.
- Kill the tab's session from another tab (`KILL <spid>`): Echo should report a lost connection and reconnect on the next run.
