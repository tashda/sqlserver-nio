#!/usr/bin/env bash
# LabAvailabilityGroupTests: a read-scale availability group (CLUSTER_TYPE =
# NONE) on three SQL Server 2022 containers with read-only routing to the
# secondaries. Replicas are published on 127.0.0.1:14451-14453 and route to
# those host addresses, so a client on the host can follow the routing token.
#
#   eval "$(Tests/Fixtures/availability-group/start-servers.sh)"
set -euo pipefail
source "$(dirname "$0")/../common.sh"

IMAGE="$SQL_IMAGE_PREFIX:2022-latest"
REPLICAS=(ag1 ag2 ag3)
PORTS=(14451 14452 14453)
# Read-only routing lists per replica when it is primary.
ROUTES=("N'ag2', N'ag3'" "N'ag1', N'ag3'" "N'ag1', N'ag2'")

sql() {
    local name=$1 query=$2
    sql_in "nio-lab-$name" "$query"
}


up() {
    ensure_network nio-lab
    for i in 0 1 2; do
        local name=${REPLICAS[$i]}
        docker rm -f "nio-lab-$name" >/dev/null 2>&1 || true
        # shellcheck disable=SC2046
        docker run -d --name "nio-lab-$name" --hostname "$name" --label nio-lab=1 --network nio-lab $(platform_args) \
            -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -e MSSQL_ENABLE_HADR=1 -e MSSQL_AGENT_ENABLED=true \
            -p "${PORTS[$i]}:1433" "$IMAGE" >/dev/null
    done
    for name in "${REPLICAS[@]}"; do wait_sql "nio-lab-$name"; done

    # Certificate-authenticated mirroring endpoints: each replica trusts the
    # others' endpoint certificates.
    for name in "${REPLICAS[@]}"; do
        sql "$name" "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$PASSWORD';
            CREATE CERTIFICATE ${name}_cert WITH SUBJECT = '$name endpoint', EXPIRY_DATE = '2099-01-01';
            BACKUP CERTIFICATE ${name}_cert TO FILE = '/var/opt/mssql/data/${name}_cert.cer'
                WITH PRIVATE KEY (FILE = '/var/opt/mssql/data/${name}_cert.pvk', ENCRYPTION BY PASSWORD = '$PASSWORD');
            CREATE ENDPOINT hadr_endpoint STATE = STARTED AS TCP (LISTENER_PORT = 5022)
                FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE ${name}_cert, ENCRYPTION = REQUIRED ALGORITHM AES);" >/dev/null
    done
    local tmp; tmp=$(mktemp -d)
    for name in "${REPLICAS[@]}"; do
        docker cp "nio-lab-$name:/var/opt/mssql/data/${name}_cert.cer" "$tmp/" >/dev/null
    done
    for name in "${REPLICAS[@]}"; do
        for other in "${REPLICAS[@]}"; do
            [ "$name" = "$other" ] && continue
            docker cp "$tmp/${other}_cert.cer" "nio-lab-$name:/var/opt/mssql/data/${other}_cert.cer" >/dev/null
            docker exec -u 0 "nio-lab-$name" chown mssql "/var/opt/mssql/data/${other}_cert.cer" 2>/dev/null || true
            sql "$name" "CREATE LOGIN ${other}_login WITH PASSWORD = '$PASSWORD';
                CREATE USER ${other}_user FOR LOGIN ${other}_login;
                CREATE CERTIFICATE ${other}_cert AUTHORIZATION ${other}_user FROM FILE = '/var/opt/mssql/data/${other}_cert.cer';
                GRANT CONNECT ON ENDPOINT::hadr_endpoint TO ${other}_login;" >/dev/null
        done
    done
    rm -rf "$tmp"

    local replicas=""
    for i in 0 1 2; do
        local name=${REPLICAS[$i]}
        [ -n "$replicas" ] && replicas+=","
        replicas+="N'$name' WITH (ENDPOINT_URL = N'tcp://$name:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
            FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC,
            SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL, READ_ONLY_ROUTING_URL = N'tcp://127.0.0.1:${PORTS[$i]}'),
            PRIMARY_ROLE (ALLOW_CONNECTIONS = READ_WRITE, READ_ONLY_ROUTING_LIST = (${ROUTES[$i]})))"
    done
    sql ag1 "CREATE AVAILABILITY GROUP nioag WITH (CLUSTER_TYPE = NONE) FOR REPLICA ON $replicas;
        ALTER AVAILABILITY GROUP nioag GRANT CREATE ANY DATABASE;" >/dev/null
    for name in ag2 ag3; do
        sql "$name" "ALTER AVAILABILITY GROUP nioag JOIN WITH (CLUSTER_TYPE = NONE);
            ALTER AVAILABILITY GROUP nioag GRANT CREATE ANY DATABASE;" >/dev/null
    done
    sql ag1 "CREATE DATABASE nioagdb; ALTER DATABASE nioagdb SET RECOVERY FULL;
        BACKUP DATABASE nioagdb TO DISK = N'/var/opt/mssql/data/nioagdb.bak';
        ALTER AVAILABILITY GROUP nioag ADD DATABASE nioagdb;
        EXEC('USE nioagdb; CREATE TABLE dbo.probe (id INT PRIMARY KEY, v NVARCHAR(50)); INSERT dbo.probe VALUES (1, N''primary'');');" >/dev/null
    for name in ag2 ag3; do
        for _ in $(seq 1 60); do
            [ "$(sql "$name" "SELECT COUNT(*) FROM sys.databases WHERE name = 'nioagdb' AND state_desc = 'ONLINE'" 2>/dev/null | tr -d '[:space:]')" = "1" ] && break
            sleep 2
        done
    done
    # SQL Server routes read-intent logins only when they arrive through a
    # listener. Without a cluster manager the listener uses the primary's
    # own address, so logins published on 127.0.0.1:14451 qualify.
    local ip; ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "nio-lab").IPAddress}}' nio-lab-ag1)
    sql ag1 "ALTER AVAILABILITY GROUP nioag ADD LISTENER N'niolsnr' (WITH IP ((N'$ip', N'255.255.0.0')), PORT = 1433);" >/dev/null
    log "availability group nioag ready: primary 127.0.0.1:14451, secondaries 14452 and 14453"
}

up

cat <<VARS
export NIO_LAB_AG_PRIMARY=127.0.0.1:14451
export NIO_LAB_AG_SECONDARIES=127.0.0.1:14452,127.0.0.1:14453
export NIO_LAB_AG_DATABASE=nioagdb
export NIO_LAB_AG_USERNAME=sa
export NIO_LAB_AG_PASSWORD=$PASSWORD
VARS
