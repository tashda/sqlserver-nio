# Public API Audit (2025-10-23)

The table below captures the current publicly exported symbols in the `TDS` target (soon to become `SQLServerNIO`). They are grouped into three categories:

- **Keep Public**: Forms part of the desired high-level surface (connection/client, configuration, metadata value types).
- **Wrap & Hide**: Should be replaced by higher-level façades and then downgraded to `internal`.
- **Implementation Detail**: Internal plumbing that should never be exposed.

| File | Symbol(s) | Current Role | Proposed Category |
| --- | --- | --- | --- |
| `Client/SQLServerConnection.swift` | `SQLServerConnection`, nested `Configuration` | New high-level connection façade | **Keep Public** |
| `Client/SQLServerClient.swift` | `SQLServerClient`, nested `Configuration`, `EventLoopGroupProvider` | Pooled façade | **Keep Public** |
| `Client/SQLServerRetryConfiguration.swift` | `SQLServerRetryConfiguration` | Shared retry policy | **Keep Public** |
| `Metadata/SQLServerMetadata.swift` | `DatabaseMetadata`, `SchemaMetadata`, `TableMetadata`, `ColumnMetadata`, `SQLServerMetadataClient` | Metadata access | **Keep Public** (after async/await + fallback work) |
| `Connection/SQLServerConnectionPool.swift` | `SQLServerConnectionPool` and nested types | Pool implementation | **Wrap & Hide** (expose via `SQLServerClient` only) |
| `Connection/TDSConnection*.swift` | `TDSConnection`, extensions for connect/login/prelogin, `PipelineOrganizationHandler` | Raw protocol objects | **Wrap & Hide** – will become `internal` once `SQLServerConnection` fully replaces them |
| `Authentication/TDSLoginConfiguration.swift` | `TDSAuthentication`, `TDSLoginConfiguration` | Login payloads | **Wrap & Hide** (fold into `SQLServerConnection.Configuration`) |
| `Data/TDSData.swift` and extensions | `TDSData`, conversions, `TDSRow` | Row materialisation | **Keep Public** (needed for query results) but consider namespacing |
| `RawSqlBatchRequest.swift` & `TDSRequest.swift` | `TDSRequest` protocol, helpers | Request plumbing | **Implementation Detail** |
| `Token/TDSTokenParser+*.swift`, `Packet/*`, `Message/*` | Token/packet/message parsers | Protocol plumbing | **Implementation Detail** |
| `Utilities` (if any exposed) |  |  |  |

### Notes

- Several internal protocols/classes (`TDSClient`, `PipelineOrganizationHandler`, token parsers) are currently `public` purely because they were never scoped. They will need to be marked `internal` once higher-level APIs are finalised.
- Test utilities import `TDSConnection` directly; these call sites must be updated before tightening visibility.
- Async/await adapters and fallback logic will live on `SQLServerConnection`/`SQLServerClient` and the specialised clients (metadata, admin, agent) to keep application code free of implementation details.
