#!/usr/bin/env bash
# LabKerberosTests: a Samba Active Directory domain controller and SQL Server
# 2022 on Linux configured for AD authentication with a keytab.
#
# Domain LAB.TEST (NetBIOS LAB). Accounts: nio-sql (SQL Server's service
# account, holds the SPNs) and nio-user (the test login, LAB\nio-user).
# The test process reaches the KDC on 127.0.0.1:1088 and SQL Server on
# localhost:14450; the SPN is MSSQLSvc/localhost:14450. Its krb5.conf and
# ticket cache live under .build/testlab/kerberos, never in the user's own
# Kerberos setup.
#
#   eval "$(Tests/Fixtures/kerberos/start-server.sh)"
set -euo pipefail
source "$(dirname "$0")/../common.sh"

HERE="$(cd "$(dirname "$0")" && pwd)"
REALM=LAB.TEST
DOMAIN=LAB
NET=nio-lab-ad
SUBNET=172.30.0.0/24
DC_IP=172.30.0.10
SQL_IP=172.30.0.20
KDC_PORT=1088
SQL_PORT=14450
DC=nio-lab-dc
SQL=nio-lab-kerberos
SPN="MSSQLSvc/localhost:${SQL_PORT}"
STATE_DIR="$STATE_ROOT/kerberos"

dc() { docker exec "$DC" "$@"; }

sqlcmd_sa() { sql_in "$SQL" "$1"; }

wait_for() {
    local what=$1 seconds=$2; shift 2
    for _ in $(seq 1 "$seconds"); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        for container in "$DC" "$SQL"; do
            if [ "$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null)" = "exited" ]; then
                log "$container exited:"; docker logs --tail 30 "$container" >&2; return 1
            fi
        done
        sleep 1
    done
    log "$what did not become ready in ${seconds}s"
    return 1
}

up_dc() {
    docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NET" >/dev/null
    docker build -q -t nio-lab-samba-dc "$HERE" >/dev/null
    if [ "$(docker inspect -f '{{.State.Running}}' "$DC" 2>/dev/null)" != "true" ]; then
        docker rm -f "$DC" >/dev/null 2>&1 || true
        docker run -d --name "$DC" --label nio-lab=1 --privileged \
            --network "$NET" --ip "$DC_IP" --hostname dc1 --domainname lab.test \
            -e REALM="$REALM" -e DOMAIN="$DOMAIN" -e ADMIN_PASSWORD="$PASSWORD" -e HOST_IP="$DC_IP" \
            -p "$KDC_PORT:88/tcp" -p "$KDC_PORT:88/udp" \
            nio-lab-samba-dc >/dev/null
    fi
    wait_for "Samba" 180 dc samba-tool user list
    # Accounts, SPNs and the keytab (idempotent).
    dc samba-tool user show nio-sql >/dev/null 2>&1 || dc samba-tool user create nio-sql "$PASSWORD" >/dev/null
    dc samba-tool user show nio-user >/dev/null 2>&1 || dc samba-tool user create nio-user "$PASSWORD" >/dev/null
    dc samba-tool user setexpiry nio-sql --noexpiry >/dev/null
    dc samba-tool user setexpiry nio-user --noexpiry >/dev/null
    for spn in "$SPN" "MSSQLSvc/sql.lab.test:1433" "MSSQLSvc/sql.lab.test"; do
        dc samba-tool spn list nio-sql | grep -qF "$spn" || dc samba-tool spn add "$spn" nio-sql >/dev/null
    done
    # SQL Server's name in the domain's DNS, forward and reverse. `CREATE LOGIN
    # [LAB\user] FROM WINDOWS` resolves the short domain name LAB (through the
    # search domain: lab.lab.test) to a domain controller, then needs its
    # reverse lookup to return the controller's FQDN.
    local admin="administrator%$PASSWORD"
    dc samba-tool dns query 127.0.0.1 lab.test lab A -U "$admin" >/dev/null 2>&1 \
        || dc samba-tool dns add 127.0.0.1 lab.test lab A "$DC_IP" -U "$admin" >/dev/null
    dc samba-tool dns query 127.0.0.1 lab.test sql A -U "$admin" >/dev/null 2>&1 \
        || dc samba-tool dns add 127.0.0.1 lab.test sql A "$SQL_IP" -U "$admin" >/dev/null
    dc samba-tool dns zoneinfo 127.0.0.1 0.30.172.in-addr.arpa -U "$admin" >/dev/null 2>&1 \
        || dc samba-tool dns zonecreate 127.0.0.1 0.30.172.in-addr.arpa -U "$admin" >/dev/null
    dc samba-tool dns query 127.0.0.1 0.30.172.in-addr.arpa 20 PTR -U "$admin" >/dev/null 2>&1 \
        || dc samba-tool dns add 127.0.0.1 0.30.172.in-addr.arpa 20 PTR sql.lab.test -U "$admin" >/dev/null
    dc samba-tool dns query 127.0.0.1 0.30.172.in-addr.arpa 10 PTR -U "$admin" >/dev/null 2>&1 \
        || dc samba-tool dns add 127.0.0.1 0.30.172.in-addr.arpa 10 PTR dc1.lab.test -U "$admin" >/dev/null
    dc rm -f /tmp/mssql.keytab
    for principal in "$SPN" "MSSQLSvc/sql.lab.test:1433" "MSSQLSvc/sql.lab.test" nio-sql; do
        dc samba-tool domain exportkeytab /tmp/mssql.keytab --principal="$principal" >/dev/null
    done
    mkdir -p "$STATE_DIR"
    docker cp "$DC:/tmp/mssql.keytab" "$STATE_DIR/mssql.keytab"
}

up_sql() {
    if [ "$(docker inspect -f '{{.State.Running}}' "$SQL" 2>/dev/null)" != "true" ]; then
        docker rm -f "$SQL" >/dev/null 2>&1 || true
        # shellcheck disable=SC2046
        docker run -d --name "$SQL" --label nio-lab=1 $(platform_args) \
            --network "$NET" --ip "$SQL_IP" --hostname sql --domainname lab.test \
            --dns "$DC_IP" --dns-search lab.test \
            -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" \
            -p "$SQL_PORT:1433" \
            mcr.microsoft.com/mssql/server:2022-latest >/dev/null
    fi
    wait_for "SQL Server" 240 sqlcmd_sa "SELECT 1"
    # Keytab, Kerberos configuration and mssql.conf (writable copy; a
    # read-only mount keeps SQL Server from starting), then restart.
    docker exec -u 0 "$SQL" mkdir -p /var/opt/mssql/secrets
    docker cp "$STATE_DIR/mssql.keytab" "$SQL:/var/opt/mssql/secrets/mssql.keytab"
    docker cp "$HERE/krb5-sql.conf" "$SQL:/etc/krb5.conf"
    docker cp "$HERE/mssql.conf" "$SQL:/var/opt/mssql/mssql.conf"
    # Kerberos and LDAP diagnostics in /var/opt/mssql/log/security.log.
    docker cp "$HERE/logger.ini" "$SQL:/var/opt/mssql/logger.ini"
    docker exec -u 0 "$SQL" /bin/bash -c "chown -R mssql /var/opt/mssql/secrets /var/opt/mssql/mssql.conf /var/opt/mssql/logger.ini && chmod 400 /var/opt/mssql/secrets/mssql.keytab && chmod 644 /etc/krb5.conf"
    docker restart "$SQL" >/dev/null
    wait_for "SQL Server after restart" 240 sqlcmd_sa "SELECT 1"
    # Ask the domain controller directly: Docker's resolver answers reverse
    # lookups of containers itself (nio-lab-dc.nio-lab-ad), not with the FQDN.
    # Docker rewrites the file on every restart, so this comes after it.
    docker exec -u 0 "$SQL" /bin/bash -c "printf 'nameserver $DC_IP\nsearch lab.test\n' > /etc/resolv.conf"
    sqlcmd_sa "IF SUSER_ID(N'LAB\\nio-user') IS NULL CREATE LOGIN [LAB\\nio-user] FROM WINDOWS;"
}

up_dc >&2
up_sql >&2
mkdir -p "$STATE_DIR"
cp "$HERE/krb5-client.conf" "$STATE_DIR/krb5.conf"
log "Kerberos fixture ready: SQL Server on localhost:$SQL_PORT, KDC on 127.0.0.1:$KDC_PORT"

cat <<VARS
export KRB5_CONFIG=$STATE_DIR/krb5.conf
export KRB5CCNAME=FILE:$STATE_DIR/ccache
export NIO_LAB_KRB_HOST=localhost
export NIO_LAB_KRB_PORT=$SQL_PORT
export NIO_LAB_KRB_USERNAME=nio-user
export NIO_LAB_KRB_PASSWORD=$PASSWORD
export NIO_LAB_KRB_DOMAIN=$REALM
export NIO_LAB_KRB_LOGIN='LAB\nio-user'
VARS
