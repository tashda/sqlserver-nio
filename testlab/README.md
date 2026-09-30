# sqlserver-nio test lab

Everything the driver claims must be verified against a real server. The lab
brings those servers up on demand on the lab server (`testlab`, 192.168.1.153,
Docker context `testlab`), the same host echo-server-lab uses. Set
`NIO_LAB_HOST=local` for Docker on this machine (CI does); Apple silicon runs
the amd64 SQL Server images under Rosetta.

## Scenarios

| Scenario | Where | Status |
|---|---|---|
| SQL Server 2017, 2019, 2022, 2025 | Linux containers (`testlab/testlab.sh matrix`) | Verified |
| TLS: own CA, wrong host, expired, untrusted, TLS 1.0 only | `Tests/Fixtures/tls/start-server.sh` | Verified, runs in CI |
| TDS 8.0 Strict (`network.forcestrict`) | `Tests/Fixtures/tls/start-server.sh` (SQL Server 2025) | Verified, runs in CI |
| Network faults: latency, throttling, resets, silence | `Tests/Fixtures/faults/start-server.sh` (Toxiproxy) | Verified, runs in CI |
| Availability group read-only routing | `Tests/Fixtures/availability-group/start-servers.sh` (3 containers, `CLUSTER_TYPE = NONE`) | Verified, runs in CI |
| Kerberos | `Tests/Fixtures/kerberos/start-server.sh` (Samba AD + SQL Server with a keytab) | Verified from macOS; CI once the driver's Kerberos runs on Linux |
| SQL Server 2008 R2, 2012, 2014, 2016; NTLM | Windows Server VM on Proxmox (below) | Needs the VM |
| Azure SQL Database: gateway redirect, Entra ID tokens | Azure free tier (below) | Needs an Azure account |
| Soak: hours of mixed load under faults | Local or a lab host | Planned |

Data and feature-API tests use echo-server-lab instead (see `AGENTS.md`).

Rules for lab tests:

- A test that needs a scenario reads its connection details from `NIO_LAB_*`
  environment variables. When they are missing it skips locally, but CI sets
  `NIO_LAB_REQUIRE=1`, which turns a missing scenario into a failure. Skips
  must never hide a broken feature again.
- Each fixture script prints only `export` lines on stdout:
  `eval "$(Tests/Fixtures/tls/start-server.sh)"`. `Tests/Fixtures/stop-all.sh`
  removes every fixture container.
- Long-running tests carry a watchdog that aborts with the test name.

## Running the version matrix

```bash
swift build --build-tests
testlab/testlab.sh matrix          # full suite on 2017, 2019, 2022, 2025
testlab/testlab.sh test 2022 'ProductionHardeningTests'
testlab/testlab.sh down
```

## Windows VM (SQL Server 2008 R2 to 2016, NTLM)

One Windows Server VM hosts all older versions as named instances. Please
create it; I script everything after first boot.

1. **Proxmox VM**: 4 vCPU, 8 GB RAM, 90 GB disk on `local-lvm` (thin), VirtIO
   network on the same bridge as `devservices`, machine type q35, OVMF (UEFI)
   is fine. Attach the VirtIO driver ISO (`virtio-win.iso`) for the disk and
   network drivers.
2. **Windows Server 2016 Standard (Desktop Experience), evaluation** from the
   Microsoft Evaluation Center. 2016 is the newest Windows release on which
   SQL Server 2008 R2 SP3 still installs. The evaluation runs 180 days.
3. After first boot:
   - Set a static IP (for example `192.168.1.160`) and a hostname such as
     `niowin`.
   - Enable OpenSSH Server:
     `Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0`,
     `Start-Service sshd`, `Set-Service sshd -StartupType Automatic`,
     and set PowerShell as its default shell:
     `New-ItemProperty -Path HKLM:\SOFTWARE\OpenSSH -Name DefaultShell -Value C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -PropertyType String -Force`
   - Allow inbound TCP 22 and 1433–1440 in Windows Firewall.
4. Tell me the IP and an administrator account (a dedicated lab admin, not a
   personal password). I then install SQL Server 2008 R2 SP3, 2012 SP4, 2014
   SP3 and 2016 SP3 as named instances on fixed ports, enable TLS 1.2 (2008 R2
   and 2012 need the TLS 1.2 updates), create SQL and Windows (NTLM) test
   logins, and add the VM to the lab.

Two driver risks this VM checks directly: SQL Server 2008 R2 speaks TDS 7.3
(the driver requests 7.4), and it supports TLS 1.2 only with the TLS 1.2 update
applied, while the driver requires TLS 1.2 or newer.

## Azure SQL Database (redirect routing, Entra ID tokens)

1. **Create a free Azure account** at https://azure.microsoft.com/free (a
   Microsoft account, a phone number and a card for identity verification;
   the free database below is not charged).
2. **Create the database** in the portal: *Azure SQL* → *Create* → *SQL
   databases* → *Single database*, and choose **Apply offer** for the free
   serverless database (100,000 vCore-seconds and 32 GB per month).
   - New server, for example `nio-lab-<yourname>`, region close to you.
   - Authentication: **Use both SQL and Microsoft Entra authentication**, set
     yourself as the Entra admin and create a SQL admin login for the lab.
   - Behaviour when the free limit is reached: **Auto-pause until next month**,
     so nothing is ever billed.
3. **Networking**: add a firewall rule for your current public IP. Under
   *Connectivity policy* choose **Redirect** so the gateway sends the routing
   token the driver must follow (the default for connections from outside
   Azure is Proxy, which never redirects).
4. **Entra ID app for token tests**: *Microsoft Entra ID* → *App
   registrations* → *New registration* (`sqlserver-nio-lab`), then
   *Certificates & secrets* → new client secret. In the database, run
   `CREATE USER [sqlserver-nio-lab] FROM EXTERNAL PROVIDER; ALTER ROLE db_datareader ADD MEMBER [sqlserver-nio-lab];`
   as the Entra admin.
5. Send me the server name, database name, SQL login, tenant ID, client ID
   and client secret through a local file that stays out of git (for example
   `testlab/azure.env`, which is ignored), not in chat.
