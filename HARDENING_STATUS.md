# SQL Server driver hardening status

Branch `codex/enterprise-hardening`. Driver only; Echo is untouched (see `ECHO_INTEGRATION_NOTES.md`). No users yet, so no migration.

The behavioural reference is Microsoft's own drivers (ODBC 18, JDBC, Microsoft.Data.SqlClient) and MS-TDS. The implementation is SwiftNIO-native.

## What changed

### Protocol pipeline (`Sources/SQLServerTDS/TDSRequest.swift`, `Packet/TDSPacketDecoder.swift`, `Token/`)

| Problem found | Fix |
|---|---|
| The packet decoder buffered a whole TDS message before parsing. A large `SELECT` was held entirely in memory, so streaming and back-pressure could not work. | Packets are delivered as they arrive (`TDSPacketChunk`). The token parser resumes at token boundaries, compacts consumed bytes, and re-examines very large incomplete values only after the buffer doubles (linear time for multi-megabyte LOBs). |
| A late ATTENTION acknowledgement was attributed to whatever request ran next, cancelling it and shifting its results onto the following request. A stale cancel (after completion) did the same. | Explicit state machine: one active request; after ATTENTION the response is discarded until DONE+ATTN (MS-TDS 2.2.1.7) and no request is sent until then. Cancels target a specific request handle; stale cancels are no-ops. No acknowledgement within 15 s closes the connection. |
| Token parse errors failed the request but left the connection in use, desynchronised. Unsolicited bytes were silently dropped. | Any protocol or framing error, unknown token, truncated message or unsolicited data is fatal: all requests fail and the connection closes. |
| Every request registered a callback on the channel's close future, leaking for the connection's lifetime. | Removed; the handler fails pending requests when the channel closes. |
| Login completed at LOGINACK, so the rest of the login response (including a routing ENVCHANGE) was delivered to the next request. | Login completes at the final DONE. |
| `TransactionManagerRequest` dropped server errors, so a rejected COMMIT (e.g. error 3930) was reported as success. | Errors are surfaced. |
| Several token readers silently accepted partial data (`if let ... readInteger`, `?? 0`), and the CLR UDT reader fell back to a different encoding when a PLP value had not fully arrived. | Readers require complete data; UDT values are always PLP (MS-TDS PARTLENTYPE). ENVCHANGE checks its declared length before moving the reader index. A packet length below the header size fails instead of stalling forever. |
| RESETCONNECTION was set on every packet of a message. | Set on the first packet only (MS-TDS 2.2.3.1.2), per request. |

Also: `currentDatabase` follows ENVCHANGE (so a `USE` inside a user batch is reflected); routing ENVCHANGE (Azure SQL redirect, read-only routing) is captured and followed; TCP keep-alive (30 s idle) and TCP_NODELAY are set; LOGIN7 no longer calls `Host.current()` (blocking reverse DNS on the event loop); the application name is configurable (`APP_NAME()`).

### Execution, cancellation and deadlines (`SQLServerKit/Client`)

- `execute`, `query`, `call` and `streamQuery` hold a per-request handle. Task cancellation sends ATTENTION and throws `CancellationError` once SQL Server acknowledges; the connection stays usable (verified live, including inside transactions where `XACT_ABORT ON` rolls back).
- `execute(_:timeout:)` and `SessionOptions.defaultQueryTimeout` are enforced per request from when it is sent, cancelling on the server (JDBC `queryTimeout` semantics). The old wrapper that failed the caller while the request kept running and invalidated the connection is gone.
- Streaming reads the socket only on consumer demand (`pauseReading`/`resumeReading`), verified live by SQL Server waiting on `ASYNC_NETWORK_IO` while the consumer is paused. Abandoning a stream cancels the query.
- `streamBatches` is pull-driven: a stopped consumer cancels the running batch and later batches never execute (previously a detached task kept running them, writes included, with unbounded buffering).
- Server errors carry `SQLServerErrorDetails` (number, severity, state, line, procedure, all messages). `isTransient` uses Microsoft's transient error list; `isConnectionLost` covers transport loss, protocol failures and severity ≥ 20.
- `withTransaction` / `commitTransaction`: a COMMIT rejected by the server throws that error (not committed); a transport failure during COMMIT throws `commitOutcomeUnknown`.
- No SQL, RPC or transaction body is ever replayed (kept from the previous pass).

### Connection establishment (`SQLServerConnection+Open.swift`)

- One code path for pooled and dedicated sessions (previously duplicated).
- `connectTimeoutSeconds` bounds the whole attempt: DNS, TCP, TLS, login, routing and session bootstrap. Before, only the TCP connect was bounded, so a server that accepted TCP but never answered the login hung forever.
- DNS returns every address (IPv4 first), trying the next only when an address is unreachable. DNS failures produce a readable error (previously "No more addresses to try").
- Removed a silent fallback that retried a failed connection **on port 1433** — potentially a different SQL Server instance.
- Removed the "login to master and `USE` the database" fallback, which doubled failed logins for a wrong password (account lockout).
- Retries only network failures, never authentication or TLS failures. Session bootstrap errors are no longer ignored.
- `.optional` encryption without a TLS configuration encrypts the full session without certificate validation instead of failing; `.mandatory`/`.strict` without configuration fail with a clear message.
- Dedicated connections use NIO's shared event loop group (previously one thread per connection); an overload accepts a caller-owned group.

### Pool (`SQLServerConnectionPool.swift`)

- Returned sessions are reset with RESETCONNECTION plus the configured session options, database and isolation level, then reused. A session that cannot be reset is closed. Verified live: same SPID reused with temp tables, SET options, isolation level, `SESSION_CONTEXT`, `CONTEXT_INFO`, database and open transactions all cleared; an un-reverted `EXECUTE AS` session is discarded (server error 18059), not reused. The previous pass closed every session on return (a TCP+TLS+login per operation).
- No per-checkout ping; sessions idle ≥ 30 s are validated, sessions idle 5 min are closed.
- Late connection creation after a checkout timeout returns the session to the pool instead of leaking the slot.
- Shutdown closes leased sessions too, so it cannot hang behind a never-returned lease. `SQLServerClient.shutdownGracefully()` waits up to 10 s for running operations, then closes their connections.
- The per-operation close-future callback in the async `withConnection` bridge (a leak now that sessions live long) is gone; the bridge is a plain `async` function.
- `withStreamQuery(_:_:)` scopes a pooled stream lease.

## Verification

Test server: SQL Server 2025 (17.0.4015.4) on Linux, 192.168.1.152:1435, TLS with a self-signed certificate (trusted explicitly).

- Unit tests (no server): all pass, including 11 new deterministic pipeline tests (late ATTENTION acknowledgement, stale cancel, queued cancel, timeout, missing acknowledgement, unsolicited data, truncated message, byte-by-byte token reassembly, paused reads) and error/configuration contract tests.
- Live hardening suite (`ProductionHardeningTests`, 15 tests): cancellation, timeouts inside transactions, abandoned streams, server-side back-pressure, pooled stream leases, batch stream cancellation, pool reset isolation, impersonation, killed sessions, structured errors, database tracking, concurrent pool use, unreachable-host deadline.
- Incremental parser stress: the query, data type, LOB, spatial, CLR UDT, execution plan and hardening suites run with every packet split into 7-byte fragments (`TDS_DEBUG_FRAGMENT_SIZE=7`). All pass.
- Full live suite: see the latest run summary in the PR.

## Remaining gaps

These are not verified and should be before calling the driver enterprise-ready:

1. **Other server versions.** Only SQL Server 2025 was available. CI runs 2017/2019/2022/2025 containers; that matrix must pass.
2. **TDS 8.0 Strict end to end** needs a certificate whose name matches the host. The code path is unchanged from the previous pass and untested live.
3. **Routing** (Azure SQL redirect, availability-group read-only routing) is implemented per MS-TDS and SqlClient behaviour but untested: no Azure SQL or AG listener was available.
4. **Windows authentication** (Kerberos/NTLM) and **Entra ID access tokens** were not re-tested live; the login state machine they rely on changed (login now completes at the final DONE).
5. **Not implemented:** MARS, Always Encrypted column encryption, TDS bulk load (bulk copy uses batched `INSERT`), data classification (the LOGIN7 feature is never requested, so `decodeLastSensitivityClassification()` is always nil, and the DATACLASSIFICATION token framing in the parser does not match MS-TDS), transparent reconnect of idle sessions (SqlClient `ConnectRetryCount`), negotiated packet sizes other than 4096.
6. **Echo integration** — see `ECHO_INTEGRATION_NOTES.md`. Echo will not compile against this branch until it handles `commitOutcomeUnknown`.
7. **Load and soak testing**: hours-long runs with network faults (packet loss, server restart, failover) under Echo's real workload.
