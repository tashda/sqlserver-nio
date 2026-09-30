# Echo tasks

What Echo must do after driver changes, and where it stands. Commits are Echo commits on the shared
Echo branch unless noted. Add new rows at the bottom when a driver change needs Echo work.

| # | Task | Status |
|---|---|---|
| 1 | Handle the new `SQLServerError` cases (`commitOutcomeUnknown`, `tlsFailed`) in `DatabaseError.from(sqlServerError:)`; `sqlExecutionError`/`deadlockDetected` carry `details`. | Done (`156893a1`) |
| 2 | Use the structured server error: every message of the batch with number, severity, line and procedure; `isConnectionLost`, `isTransient`. | Done (`8e8ea135` data, `d6cf8cf3` Messages with the SSMS header and line links, editor error mark) |
| 3 | Remove the 45-second task-group timer in `MSSQLDedicatedQuerySession+Queries`. | Done (`057a797b`) |
| 4 | Stop reconnecting after every cancellation; reconnect only when the connection is closed. | Done (`057a797b`); a dropped connection is told at once with Reconnect (`a5ed0ffb`) |
| 5 | Encryption modes: Mandatory by default, the menu says what is checked, Strict, Allow TLS 1.0, certificate failures explained at Test. | Done (`057a797b`) |
| 6 | Set `applicationName` to `Echo`. | Done (`057a797b`) |
| 7 | Serialize the dedicated session's connection swap. | Done with a lock (`057a797b`) |
| 8 | Read `SQLServerConnection.currentDatabase` after each run instead of tracking `USE` by hand. | Open: Echo still parses `USE` (`detectAndApplyDatabaseSwitch`) |
| 9 | Spool extra result sets instead of holding them in memory. | Done (`20688328`); `executeBatches` (GO batches) still keeps every row in memory |
| 10 | Prefer `client.withStreamQuery(sql) { … }` over `client.streamQuery(sql)` for pooled streams. | Open |
| 11 | Spool every row as wire bytes and format with `SQLServerCellFormatter` (values after row 200). | Done (`ce98d8e4`) |
| 12 | "Transaction rolled back" after a cancel inside a transaction (`isInTransaction`, driver #14). | Done (`c051c455`) |
| 13 | Imports use the TDS bulk load (`client.bulk.copy`, unchanged call). New options to offer in the import sheet: `tableLock`, `checkConstraints`, `fireTriggers`, `keepNulls` (and `identityInsert`); show `summary.method`. Text converts as before (SQL Server converts what the client cannot read), so today's files import unchanged. Lab round first. | Open |

Checks to run in the app (owner): run, cancel mid-result, run again (temp tables survive); a cancel
inside `BEGIN TRAN` says Transaction rolled back; several million rows keep memory flat; `KILL` the
tab's session from another tab (told at once; Reconnect when a transaction was open).
