#!/usr/bin/env bash
# Test lab for sqlserver-nio: starts SQL Server containers and runs the test
# suite against them. Containers are named nio-lab-* and labelled nio-lab=1.
#
#   testlab/testlab.sh up 2017 2019 2022 2025   start and wait until ready
#   testlab/testlab.sh test 2022 [filter]         run swift test against one
#   testlab/testlab.sh matrix [filter]            up + test every version
#   testlab/testlab.sh env 2022                   print the TDS_* variables
#   testlab/testlab.sh tls                        TLS/Strict servers + certificates
#   testlab/testlab.sh tls-env                    print the NIO_LAB_TLS_* variables
#   testlab/testlab.sh faults                     Toxiproxy in front of SQL Server 2022
#   testlab/testlab.sh faults-env                 print the NIO_LAB_FAULT_* variables
#   testlab/testlab.sh down                       remove all lab containers
set -euo pipefail

PASSWORD="${NIO_LAB_PASSWORD:-NioLab#Pass123}"
VERSIONS_DEFAULT=(2017 2019 2022 2025)
LOG_DIR="${NIO_LAB_LOG_DIR:-.build/testlab}"

ensure_network() {
    docker network inspect nio-lab >/dev/null 2>&1 || docker network create nio-lab >/dev/null
}

port_for() { echo "144${1: -2}"; }
name_for() { echo "nio-lab-$1"; }

platform_args() {
    case "$(uname -m)" in
        arm64|aarch64) echo "--platform linux/amd64" ;;
        *) echo "" ;;
    esac
}

up_one() {
    local version=$1 name port
    name=$(name_for "$version"); port=$(port_for "$version")
    if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = "true" ]; then
        echo "$name already running on port $port"; return 0
    fi
    docker rm -f "$name" >/dev/null 2>&1 || true
    ensure_network
    # shellcheck disable=SC2046
    docker run -d --name "$name" --label nio-lab=1 --network nio-lab $(platform_args) \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -e MSSQL_AGENT_ENABLED=true \
        -p "$port:1433" "mcr.microsoft.com/mssql/server:$version-latest" >/dev/null
}

lab_sql() {
    local name=$1 sql=$2
    docker exec "$name" /bin/bash -c "for t in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do [ -x \$t ] && exec \$t -S localhost -U sa -P '$PASSWORD' -C -h -1 -W -b -Q \"SET NOCOUNT ON; $sql\"; done; exit 1"
}

# SQL Server Agent gives up if it cannot log in while SQL Server is still
# starting, which happens under emulation. Restart the container once so the
# Agent-dependent tests exercise a running Agent instead of failing.
wait_agent() {
    local version=$1 name; name=$(name_for "$version")
    for attempt in 1 2; do
        for _ in $(seq 1 30); do
            [ "$(lab_sql "$name" "SELECT value_in_use FROM sys.configurations WHERE name = 'Agent XPs'" 2>/dev/null | tr -d '[:space:]')" = "1" ] && return 0
            sleep 2
        done
        [ "$attempt" = 2 ] && break
        echo "$name: SQL Server Agent did not start; restarting the container once" >&2
        docker restart "$name" >/dev/null
        for _ in $(seq 1 120); do lab_sql "$name" "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
    done
    echo "error: $name: SQL Server Agent is not running" >&2
    return 1
}

wait_one() {
    local version=$1 name; name=$(name_for "$version")
    for _ in $(seq 1 120); do
        if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
            echo "error: $name stopped while starting" >&2
            docker logs "$name" 2>&1 | grep -v '^find:' | tail -20 >&2 || true
            return 1
        fi
        if docker logs "$name" 2>&1 | grep "SQL Server is now ready for client connections" >/dev/null; then
            # The log line precedes the end of recovery; wait for a login.
            if lab_sql "$name" "SELECT 1" >/dev/null 2>&1; then
                wait_agent "$version" || return 1
                echo "$name ready on port $(port_for "$version")"; return 0
            fi
        fi
        sleep 2
    done
    echo "error: $name did not become ready within 4 minutes" >&2
    return 1
}

env_for() {
    local version=$1
    cat <<VARS
export TDS_HOSTNAME=127.0.0.1
export TDS_PORT=$(port_for "$version")
export TDS_USERNAME=sa
export TDS_PASSWORD='$PASSWORD'
export TDS_DATABASE=master
export TDS_VERSION=$version-latest
export USE_DOCKER=0
VARS
}

test_one() {
    local version=$1; shift
    mkdir -p "$LOG_DIR"
    local log="$LOG_DIR/test-$version.log"
    eval "$(env_for "$version")"
    export LOG_LEVEL="${LOG_LEVEL:-warning}"
    local args=(--skip-build)
    [ $# -gt 0 ] && args+=(--filter "$1")
    echo "== SQL Server $version (log: $log)"
    set +e
    swift test "${args[@]}" >"$log" 2>&1
    local status=$?
    set -e
    grep -E "Executed [0-9]+ tests" "$log" | tail -1 || true
    grep -E "error: -\[|' failed \(" "$log" | grep -v "Test Suite" || true
    return $status
}

CERT_DIR="${NIO_LAB_CERT_DIR:-testlab/certs}"
case "$CERT_DIR" in /*) CERT_PATH="$CERT_DIR" ;; *) CERT_PATH="$PWD/$CERT_DIR" ;; esac
TLS_HOST="sql-tls.nio.test"

# Lab CA plus server certificates: valid (names the lab host, localhost and
# 127.0.0.1) and expired. Regenerated only when missing.
make_certs() {
    mkdir -p "$CERT_DIR"
    cd "$CERT_DIR"
    if [ ! -f ca.pem ]; then
        openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=sqlserver-nio lab CA" \
            -keyout ca.key -out ca.pem 2>/dev/null
    fi
    cat > server.ext <<EXT
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:$TLS_HOST,DNS:localhost,IP:127.0.0.1
EXT
    if [ ! -f valid.pem ]; then
        openssl req -newkey rsa:2048 -nodes -subj "/CN=$TLS_HOST" -keyout valid.key -out valid.csr 2>/dev/null
        openssl x509 -req -in valid.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 \
            -extfile server.ext -out valid.pem 2>/dev/null
    fi
    if [ ! -f expired.pem ]; then
        # `openssl ca` accepts explicit validity dates on every OpenSSL and
        # LibreSSL release (x509 -not_before needs OpenSSL 3.4).
        openssl req -newkey rsa:2048 -nodes -subj "/CN=$TLS_HOST" -keyout expired.key -out expired.csr 2>/dev/null
        mkdir -p ca-db && : > ca-db/index.txt && echo 1000 > ca-db/serial
        cat > ca-db/ca.cnf <<CNF
[ ca ]
default_ca = lab
[ lab ]
database = ca-db/index.txt
serial = ca-db/serial
new_certs_dir = ca-db
default_md = sha256
policy = anything
copy_extensions = none
[ anything ]
commonName = supplied
CNF
        openssl ca -batch -config ca-db/ca.cnf -cert ca.pem -keyfile ca.key -in expired.csr \
            -startdate 20200101000000Z -enddate 20210101000000Z -extfile server.ext \
            -out expired.pem 2>/dev/null
    fi
    # A second CA the driver is never told about.
    if [ ! -f other-ca.pem ]; then
        openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=untrusted lab CA" \
            -keyout other-ca.key -out other-ca.pem 2>/dev/null
    fi
    chmod 644 ./*.pem ./*.key
    cd - >/dev/null
}

# SQL Server rewrites mssql.conf at startup, so it is copied into the
# container rather than mounted read-only (a read-only mount crashes it).
up_tls_server() {
    local name=$1 port=$2 cert=$3 strict=$4 version=${5:-2025} protocols=${6:-1.2}
    docker rm -f "$name" >/dev/null 2>&1 || true
    local conf; conf=$(mktemp)
    printf '[network]\ntlscert = /var/opt/mssql/tls/server.pem\ntlskey = /var/opt/mssql/tls/server.key\ntlsprotocols = %s\nforceencryption = 1\n' "$protocols" > "$conf"
    [ "$strict" = 1 ] && printf 'forcestrict = 1\n' >> "$conf"
    chmod 666 "$conf"
    # shellcheck disable=SC2046
    docker create --name "$name" --label nio-lab=1 $(platform_args) \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -p "$port:1433" \
        -v "$CERT_PATH/$cert.pem:/var/opt/mssql/tls/server.pem:ro" \
        -v "$CERT_PATH/$cert.key:/var/opt/mssql/tls/server.key:ro" \
        "mcr.microsoft.com/mssql/server:$version-latest" >/dev/null
    docker cp "$conf" "$name:/var/opt/mssql/mssql.conf" >/dev/null
    rm -f "$conf"
    if [ "$protocols" != "1.2" ]; then
        # OpenSSL 3 disables TLS 1.0 and 1.1 above security level 0. SQL
        # Server on Windows (the real legacy case) uses SChannel instead.
        local ssl; ssl=$(mktemp)
        docker cp "$name:/etc/ssl/openssl.cnf" "$ssl" >/dev/null
        perl -pi -e 's/CipherString = DEFAULT:\@SECLEVEL=2/CipherString = DEFAULT:\@SECLEVEL=0\nMinProtocol = TLSv1/' "$ssl"
        docker cp "$ssl" "$name:/etc/ssl/openssl.cnf" >/dev/null
        rm -f "$ssl"
    fi
    docker start "$name" >/dev/null
}

wait_log() {
    local name=$1
    for _ in $(seq 1 120); do
        if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
            echo "error: $name stopped while starting" >&2
            docker logs "$name" 2>&1 | grep -v '^find:' | tail -20 >&2 || true
            return 1
        fi
        docker logs "$name" 2>&1 | grep "SQL Server is now ready for client connections" >/dev/null && { sleep 3; echo "$name ready"; return 0; }
        sleep 2
    done
    echo "error: $name did not become ready" >&2
    return 1
}

tls_env() {
    cat <<VARS
export NIO_LAB_TLS_HOST=127.0.0.1
export NIO_LAB_TLS_CERT_NAME=$TLS_HOST
export NIO_LAB_TLS_CA=$CERT_PATH/ca.pem
export NIO_LAB_TLS_OTHER_CA=$CERT_PATH/other-ca.pem
export NIO_LAB_TLS_PORT=14431
export NIO_LAB_STRICT_PORT=14432
export NIO_LAB_EXPIRED_PORT=14433
export NIO_LAB_TLS10_PORT=14434
export NIO_LAB_SELFSIGNED_PORT=14422
export NIO_LAB_TLS_USERNAME=sa
export NIO_LAB_TLS_PASSWORD='$PASSWORD'
VARS
}

case "${1:-}" in
    tls)
        make_certs
        up_tls_server nio-lab-tls 14431 valid 0
        up_tls_server nio-lab-strict 14432 valid 1
        up_tls_server nio-lab-expired 14433 expired 0
        up_tls_server nio-lab-tls10 14434 valid 0 2022 1.0
        # SQL Server's own self-signed certificate (no certificate configured).
        up_one 2022
        for n in nio-lab-tls nio-lab-strict nio-lab-expired nio-lab-tls10; do wait_log "$n"; done
        wait_one 2022
        ;;
    tls-env)
        tls_env ;;
    faults)
        # Toxiproxy in front of the SQL Server 2022 lab container. Tests add
        # and remove faults through its HTTP API (port 8474).
        up_one 2022; wait_one 2022
        ensure_network
        docker network connect nio-lab nio-lab-2022 2>/dev/null || true
        docker rm -f nio-lab-toxiproxy >/dev/null 2>&1 || true
        docker run -d --name nio-lab-toxiproxy --label nio-lab=1 --network nio-lab \
            -p 8474:8474 -p 14440:14440 ghcr.io/shopify/toxiproxy:latest >/dev/null
        for _ in $(seq 1 30); do curl -sf http://127.0.0.1:8474/version >/dev/null && break; sleep 1; done
        curl -sf -X POST http://127.0.0.1:8474/proxies -d \
            '{"name":"sql","listen":"0.0.0.0:14440","upstream":"nio-lab-2022:1433","enabled":true}' >/dev/null
        echo "toxiproxy ready: 127.0.0.1:14440 -> nio-lab-2022:1433"
        ;;
    faults-env)
        cat <<VARS
export NIO_LAB_FAULT_HOST=127.0.0.1
export NIO_LAB_FAULT_PORT=14440
export NIO_LAB_TOXIPROXY=http://127.0.0.1:8474
export NIO_LAB_FAULT_PROXY=sql
export NIO_LAB_FAULT_USERNAME=sa
export NIO_LAB_FAULT_PASSWORD='$PASSWORD'
VARS
        ;;
    certs)
        make_certs; echo "certificates in $CERT_DIR" ;;
    up)
        shift; versions=("$@"); [ ${#versions[@]} -eq 0 ] && versions=("${VERSIONS_DEFAULT[@]}")
        for v in "${versions[@]}"; do up_one "$v"; done
        for v in "${versions[@]}"; do wait_one "$v"; done
        ;;
    test)
        shift; test_one "$@" ;;
    matrix)
        shift; filter=("$@")
        for v in "${VERSIONS_DEFAULT[@]}"; do up_one "$v"; done
        for v in "${VERSIONS_DEFAULT[@]}"; do wait_one "$v"; done
        failed=()
        for v in "${VERSIONS_DEFAULT[@]}"; do test_one "$v" "${filter[@]}" || failed+=("$v"); done
        if [ ${#failed[@]} -gt 0 ]; then echo "FAILED on: ${failed[*]}"; exit 1; fi
        echo "All versions passed."
        ;;
    env)
        shift; env_for "$1" ;;
    down)
        ids=$(docker ps -aq --filter label=nio-lab=1)
        [ -n "$ids" ] && docker rm -f $ids >/dev/null
        echo "lab containers removed" ;;
    *)
        sed -n '2,9p' "$0"; exit 2 ;;
esac
