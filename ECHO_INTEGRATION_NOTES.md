# Echo integration follow-ups

These are notes for Echo's owners. This driver branch does not edit the Echo repository.
There are no existing users, so no data or configuration migration is needed.

1. **Encryption configuration:** `MSSQLNIOFactory.makeClientConfiguration` currently maps Optional or a disabled generic TLS switch to a nil TLS configuration. The driver now refuses an unencrypted login. Give SQL Server connections a TLS configuration, default new connections to Mandatory, and show certificate validation failures clearly. Keep Strict unavailable in Echo until the TLS-first TDS 8.0 path passes live server integration tests.
2. **Dedicated query session ownership:** `MSSQLDedicatedQuerySession` has mutable `connection` and `reconnectTask` under `@unchecked Sendable`. Serialize access through an actor or a single owner. Reconnect after cancellation only if the driver reports the physical connection unusable; tell the user when reconnecting discards temp tables, SET options, and transaction context.
3. **Query deadlines:** Replace the independent 45-second task-group timer in `MSSQLDedicatedQuerySession+Queries` with the driver's request deadline and cancellation outcome once available. A timer that returns before the TDS request ends can allow the next operation to overlap with it.
4. **Large results:** The first result set is spooled, but additional result sets accumulate in `SQLServerSessionAdapter+Queries` and `MSSQLDedicatedQuerySession+Queries`. Apply the same bounded storage policy to every result set and propagate stream demand back to the driver.
5. **Integration verification:** Pin Echo to the tested driver revision only after the SQL Server integration tests pass. Exercise pooled metadata operations, dedicated tab session state, explicit transactions, cancellation, multi-result queries, and reconnect against supported SQL Server versions.

The driver branch currently closes physical pooled sessions on check-in to prevent state leakage. This is safe but increases connection churn; measure Echo's metadata workload against the intended deployment before release. Reuse should return only after RESETCONNECTION, session re-bootstrap, and state isolation are verified against a real server.
