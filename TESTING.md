# Testing sqlserver-nio

Unit tests need nothing. Integration tests need a SQL Server and find it through one URL variable
per setup; a test whose variable is not set is skipped, and the skip names the variable. Anyone
with Docker can run them.

```bash
swift test                     # unit tests; integration tests skip
```

## A plain server

```bash
docker run -d --name sqlserver-test -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='Your_password1' \
    -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
SQLSERVER_TEST_URL='sqlserver://sa:Your_password1@localhost:1433/master?trustServerCertificate=true' swift test
docker rm -f sqlserver-test
```

Any version from 2017 on works (`2017-latest`, `2019-latest`, `2022-latest`, `2025-latest`); CI runs
all four. SQL Server takes about 20 seconds to accept logins after the container starts. Tests
create the databases and objects they need and remove them afterwards.

The tests written against the AdventureWorks sample (`AdventureWorksRoutineTests`,
`MetadataLoadingTests`, `ViewColumnsTests`, `SQLServerMetadataAnalysisTests` and a few others) skip
unless the server has an AdventureWorks database. To restore one into the container above:

```bash
curl -fLO https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2022.bak
docker cp AdventureWorks2022.bak sqlserver-test:/var/opt/mssql/data/
docker exec sqlserver-test /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Your_password1' -Q "RESTORE DATABASE AdventureWorks FROM DISK = '/var/opt/mssql/data/AdventureWorks2022.bak' WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks.mdf', MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks.ldf'"
```

## The variables

| Variable | What it points at | Tests |
|---|---|---|
| `SQLSERVER_TEST_URL` | a plain server | almost every integration test |
| `SQLSERVER_TEST_TLS_URL` | a server that requires TLS; the URL carries the mode and the CA (`caFile`) | `TLSCertificateTests` (Strict tests when `encrypt=strict`) |
| `SQLSERVER_TEST_KERBEROS_URL` | Kerberos logins; `serviceHost` and `krb5Config` | `KerberosLoginTests` |
| `SQLSERVER_TEST_AG_URLS` | availability-group replicas, primary first, comma-separated | `AvailabilityGroupRoutingTests` |
| `SQLSERVER_TEST_PROXY_URL`, `SQLSERVER_TEST_PROXY_CONTROL` | the server through a Toxiproxy, and Toxiproxy's HTTP API | `NetworkFaultTests` |
| `SQLSERVER_TEST_REQUIRED=1` | a missing variable fails the test instead of skipping it | CI |

### URL form

```
sqlserver://sa:pass@localhost:1433/master?encrypt=mandatory&trustServerCertificate=true
sqlserver://sa:pass@host:1433/master?encrypt=strict&trustServerCertificate=false&caFile=/path/ca.pem
sqlserver://labuser%40LAB.TEST@10.0.0.5:1433/master?authentication=kerberos&serviceHost=sql.lab.test&krb5Config=/path/krb5.conf
```

- User and password are percent-encoded (`@` is `%40`, `:` is `%3A`, `/` is `%2F`).
- The path names the database to connect to first (`master` when empty).
- `encrypt`: `optional`, `mandatory` (the default) or `strict` (TDS 8.0).
- `trustServerCertificate=true` accepts any certificate. `caFile` validates the server's
  certificate against that CA. Neither means the system's trust store.
- `hostNameInCertificate`: the name to check the certificate against, when it differs from the host.
- `authentication=kerberos`: the user is the principal (`user@REALM`). With a password in the URL
  the driver gets the ticket itself; without one it uses the ticket in the cache (`kinit` first).
  The driver connects to the URL's host and asks for a ticket for `MSSQLSvc/<serviceHost>:<port>`;
  `krb5Config` becomes `KRB5_CONFIG`.

In tests, `TestServer` (in `SQLServerKitTesting`) parses the URL into a connection configuration:

```swift
@Suite(.testServer)                                   // or .testServer("SQLSERVER_TEST_TLS_URL")
struct MyTests {
    @Test func selectsOne() async throws {
        let server = try #require(TestServer.current)
        let connection = try await SQLServerConnection.connect(configuration: server.configuration)
        …
    }
}
```

XCTest suites call `try requireSQLServerTestServer()` (in `SQLServerKitXCTestSupport`) in `setUp`.

## TLS with plain Docker

A server with a certificate from your own CA, forcing encryption (add `forcestrict = 1` under
`[network]` and use SQL Server 2025 for TDS 8.0 Strict):

```bash
mkdir -p tls && cd tls
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=test CA" -keyout ca.key -out ca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 30 -extfile server.ext -out server.pem
printf '[network]\ntlscert = /var/opt/mssql/tls/server.pem\ntlskey = /var/opt/mssql/tls/server.key\ntlsprotocols = 1.2\nforceencryption = 1\n' > mssql.conf
docker create --name sqlserver-tls -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='Your_password1' -p 14331:1433 mcr.microsoft.com/mssql/server:2022-latest
docker cp mssql.conf sqlserver-tls:/var/opt/mssql/mssql.conf
docker cp server.pem sqlserver-tls:/var/opt/mssql/server.pem && docker cp server.key sqlserver-tls:/var/opt/mssql/server.key
docker start sqlserver-tls
docker exec -u 0 sqlserver-tls sh -c 'mkdir -p /var/opt/mssql/tls && mv /var/opt/mssql/server.* /var/opt/mssql/tls/ && chown -R mssql /var/opt/mssql/tls /var/opt/mssql/mssql.conf'
docker restart sqlserver-tls
cd ..
SQLSERVER_TEST_TLS_URL="sqlserver://sa:Your_password1@localhost:14331/master?encrypt=mandatory&caFile=$PWD/tls/ca.pem" \
    swift test --filter TLSCertificateTests
```

## Network faults with plain Docker

```bash
docker network create sqlserver-faults
docker run -d --name sqlserver-faults-db --network sqlserver-faults -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='Your_password1' mcr.microsoft.com/mssql/server:2022-latest
docker run -d --name sqlserver-faults-proxy --network sqlserver-faults -p 8474:8474 -p 21433:21433 ghcr.io/shopify/toxiproxy:2.12.0
curl -X POST http://localhost:8474/proxies -d '{"name":"sqlserver","listen":"0.0.0.0:21433","upstream":"sqlserver-faults-db:1433"}'
SQLSERVER_TEST_PROXY_URL='sqlserver://sa:Your_password1@localhost:21433/master?trustServerCertificate=true' \
SQLSERVER_TEST_PROXY_CONTROL=http://localhost:8474 swift test --filter NetworkFaultTests
```

## Setups that need more than Docker (optional)

These are skipped without their variable.

- **Availability groups** (`SQLSERVER_TEST_AG_URLS`): two or more SQL Servers in a read-scale
  group (`CLUSTER_TYPE = NONE`) with a readable secondary; the URLs are addresses this machine can
  reach. The routing tests give a group without read-only routing a routing list to the first
  secondary (it stays) and a temporary listener on the primary, since SQL Server routes
  read-intent logins only through a listener.
- **Kerberos** (`SQLSERVER_TEST_KERBEROS_URL`): SQL Server joined to an Active Directory domain
  (Samba AD works) with a keytab for `MSSQLSvc/<host>:<port>`, and a domain user. macOS only for
  now (Apple's GSS framework).
- **SQL Server 2008 R2 to 2016 and NTLM**: a Windows Server VM with the older versions as named
  instances (Windows Server 2016 is the newest release SQL Server 2008 R2 SP3 installs on), TLS 1.2
  enabled (2008 R2 and 2012 need the TLS 1.2 updates), and SQL and Windows logins. Point
  `SQLSERVER_TEST_URL` at one instance at a time. SQL Server 2008 R2 speaks TDS 7.3, while the
  driver asks for 7.4 and requires TLS 1.2.
- **Azure SQL Database**: a database whose server uses the **Redirect** connectivity policy (the
  default for connections from outside Azure, Proxy, never redirects), as `SQLSERVER_TEST_URL`.
  Entra ID token tests are not covered by a URL yet.

## CI

`.github/workflows/test.yml` runs the unit tests on every push, then the integration tests against
GitHub service containers for SQL Server 2017, 2019, 2022 and 2025 (with AdventureWorks restored),
the parser with every packet split into 7-byte fragments (`TDS_DEBUG_FRAGMENT_SIZE=7`), TLS
(Mandatory on 2022, Strict on 2025) and network faults through Toxiproxy, each with
`SQLSERVER_TEST_REQUIRED=1`.

## Debug switches

`LOG_LEVEL` (`trace` … `critical`), `TDS_DEBUG_FRAGMENT_SIZE` (split every incoming packet into
pieces of this many bytes) and `TDS_TEST_OPERATION_TIMEOUT_SECONDS` keep working as before.
