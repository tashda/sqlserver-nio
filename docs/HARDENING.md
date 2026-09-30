# SQL Server driver hardening (2026-09-30)

What the enterprise hardening pass changed, how it was verified and what is still open. Merged into `dev` through PRs #11–#14. Echo follow-ups: `docs/ECHO_TASKS.md`. No users yet, so no migration.

The behavioural reference is Microsoft's own drivers (ODBC 18, JDBC, Microsoft.Data.SqlClient) and MS-TDS. The implementation is SwiftNIO-native.

## What changed

### Protocol pipeline (`Sources/SQLServerTDS/TDSRequest.swift`, `Packet/TDSPacketDecoder.swift`, `Token/`)

| Problem found | Fix |
|---|---|
| The packet decoder buffered a whole TDS message before parsing. A large `SELECT` was held entirely in memory, so streaming and back-pressure could not work. | Packets are delivered as they arrive (`TDSPacketChunk`). The token parser resumes at token boundaries, compacts consumed bytes, and re-examines very large incomplete values only after the buffer doubles (linear time for multi-megabyte LOBs). |
| A late ATTENTION acknowledgement was attributed to whatever request ran next, cancelling it and shifting its results onto the following request. A stale cancel (after completion) did the same. | Explicit state machine: one active request; after ATTENTION the response is discarded until DONE+ATTN (MS-TDS 2.2.1.7) and no request is sent until then. Cancels target a specific request handle; stale cancels are no-ops. If the server sends nothing for 15 s while a cancellation is pending, the connection closes. |
| Token parse errors failed the request but left the connection in use, desynchronised. Unsolicited bytes were silently dropped. | Any protocol or framing error, unknown token, truncated message or unsolicited data is fatal: all requests fail and the connection closes. |
| Every request registered a callback on the channel's close future, leaking for the connection's lifetime. | Removed; the handler fails pending requests when the channel closes. |
| Login completed at LOGINACK, so the rest of the login response (including a routing ENVCHANGE) was delivered to the next request. | Login completes at the final DONE. |
| `TransactionManagerRequest` dropped server errors, so a rejected COMMIT (e.g. error 3930) was reported as success. | Errors are surfaced. |
| Several token readers silently accepted partial data (`if let ... readInteger`, `?? 0`), and the CLR UDT reader fell back to a different encoding when a PLP value had not fully arrived. | Readers require complete data; UDT values are always PLP (MS-TDS PARTLENTYPE). ENVCHANGE checks its declared length before moving the reader index. A packet length below the header size fails instead of stalling forever. |
| RESETCONNECTION was set on every packet of a message. | Set on the first packet only (MS-TDS 2.2.3.1.2), per request. |

Also: bulk copy uses the TDS bulk load (INSERT BULK plus a BulkLoadBCP message), converting values on the client like SqlBulkCopy, with INSERT statements as the fallback for column types it cannot carry. SQL Server reads MAX values in a bulk load only with an unknown PLP length, and reports the rows copied in DONE without DONE_COUNT. `varchar` columns with a `_UTF8` collation are read and written as UTF-8. The network packet size is negotiated (8000 by default, `packetSize` 512–32767; requests use the size the server accepts); `currentDatabase` follows ENVCHANGE (so a `USE` inside a user batch is reflected); routing ENVCHANGE (Azure SQL redirect, read-only routing) is captured and followed; TCP keep-alive (30 s idle) and TCP_NODELAY are set; LOGIN7 no longer calls `Host.current()` (blocking reverse DNS on the event loop); the application name is configurable (`APP_NAME()`).

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

### Feature SQL

- Resource Governor: `is_reconfiguration_pending` read from the DMV, pool session counts and CPU share computed from existing columns.
- Policy-Based Management: condition facets and facet listing use the real `msdb` columns.

## Verification

Test server: SQL Server 2025 (17.0.4015.4) on Linux, 192.168.1.152:1435, TLS with a self-signed certificate (trusted explicitly).

- Unit tests (no server): all pass, including 11 new deterministic pipeline tests (late ATTENTION acknowledgement, stale cancel, queued cancel, timeout, missing acknowledgement, unsolicited data, truncated message, byte-by-byte token reassembly, paused reads) and error/configuration contract tests.
- Live hardening suite (`ProductionHardeningTests`, 15 tests): cancellation, timeouts inside transactions, abandoned streams, server-side back-pressure, pooled stream leases, batch stream cancellation, pool reset isolation, impersonation, killed sessions, structured errors, database tracking, concurrent pool use, unreachable-host deadline.
- Incremental parser stress: the query, data type, LOB, spatial, CLR UDT, execution plan and hardening suites run with every packet split into 7-byte fragments (`TDS_DEBUG_FRAGMENT_SIZE=7`). All pass.
- Full live suite, run twice: 740 XCTest tests with 0 failures (18 skipped), 4 TDS-layer tests and 68 swift-testing tests, in about 6.5 minutes. Before this pass the same suite hung indefinitely in `QueryTests.testRowDataPreservesNullColumns`.
- Remaining skips are environmental (no HADR, CDC, AdventureWorks, or on-change policies on the test server). Five other skips hid broken driver SQL (Resource Governor and Policy-Based Management queried columns that do not exist); those queries are fixed and the tests now run.

## Test lab verification

Run locally on Apple silicon (Docker Desktop, SQL Server amd64 images under Rosetta) with `testlab/testlab.sh matrix` and the scripts in `Tests/Fixtures/` (CI runs each fixture in its own job):

| Scenario | Result |
|---|---|
| Full suite on SQL Server 2017, 2019, 2022, 2025 containers | 741 tests each. 2019 and 2025 clean. 2017: two SQL Server Agent tests failed because Agent had not started in the container (lab now restarts it; the tests then pass). 2022: one timing race in `testSerializableRangeLockBlocksInsert`, fixed in the test. |
| TLS with a lab CA (`LabTLSTests`) | Valid certificate, wrong host name, untrusted CA and expired certificate behave correctly in Mandatory and Strict. |
| TDS 8.0 Strict on SQL Server 2025 (`network.forcestrict`) | TLS-first with ALPN `tds/8.0`, queries and cancellation inside the outer TLS, clear failure for a TDS 7.x client. |
| Availability group read-only routing (`LabAvailabilityGroupTests`) | A read-intent login to the primary follows the routing token to a secondary, directly and through the pool. |
| Network faults via Toxiproxy (`LabFaultTests`) | Latency, a throttled link while cancelling a large result, a TCP reset mid-result, a silent network during cancellation and during login, and every connection dropped at once. |

Defects the lab found and fixed:
- A certificate that did not name the configured host was accepted when it listed the IP address the socket connected to (NIOSSL's identity check falls back to the socket address). The driver now checks the name itself (RFC 6125) after both TLS handshakes.
- A TCP reset during a TLS session was reported as an unknown error instead of a lost connection.
- After giving up on a connection (unacknowledged cancellation, protocol failure) `isClosed` stayed false until the TLS close finished, which takes seconds on a dead network.
- `TCP_NODELAY` was set at the socket level, which is `SO_DEBUG` on Linux and failed every connection there.
- The cancellation acknowledgement deadline measured total time, so draining a large cancelled result on a slow link closed a healthy connection. It now measures silence.

## Remaining gaps

1. **SQL Server 2008 R2, 2012, 2014 and 2016, and NTLM**: need the Windows Server VM described in `testlab/README.md`. SQL Server 2008 R2 speaks TDS 7.3 and needs its TLS 1.2 update; the driver requires TLS 1.2.
2. **Azure SQL Database** (gateway redirect and Entra ID tokens): needs the Azure account described in `testlab/README.md`.
3. **Kerberos** works against Samba AD and SQL Server 2022 on Linux (`Tests/Fixtures/kerberos`, `LabKerberosTests`: `auth_scheme = KERBEROS`, pooled sessions, wrong password, unknown realm). The lab found two defects, both fixed: Windows authentication with a password always used NTLMv2, so SQL Server on Linux (Kerberos only) refused it with 18452; it now negotiates like SSPI (Kerberos first, NTLMv2 when Kerberos is unavailable for the server). And the service principal `MSSQLSvc/host:port` was imported as a GSS host-based service name, which names a different principal; it is now a Kerberos principal name in the login's realm. Still macOS-only (GSS.framework); Linux (MIT Kerberos) is planned so CI can run it. NTLM against Windows servers needs the Windows VM.
4. **Not implemented:** Always Encrypted column encryption (planned), data classification (never requested at login). Deliberately not planned: MARS (Echo gives each tab its own connection) and transparent reconnect of broken idle sessions (Echo tells the user and offers Reconnect; the pool replaces dead idle sessions).
5. **Echo integration**: see `docs/ECHO_TASKS.md`.
6. **Soak testing**: hours of mixed load under faults.
