#!/usr/bin/env bash
# Test lab for sqlserver-nio: starts SQL Server containers (on the lab server
# unless NIO_LAB_HOST=local) and runs the test suite against them. Containers
# are named nio-lab-* and labelled nio-lab=1.
#
#   testlab/testlab.sh up 2017 2019 2022 2025   start and wait until ready
#   testlab/testlab.sh test 2022 [filter]         run swift test against one
#   testlab/testlab.sh matrix [filter]            up + test every version
#   testlab/testlab.sh env 2022                   print the TDS_* variables
#   testlab/testlab.sh down                       remove all lab containers
#
# TLS, fault, availability-group and Kerberos servers: Tests/Fixtures/.
set -euo pipefail

# Runs on the lab server unless NIO_LAB_HOST=local (see Tests/Fixtures/common.sh).
source "$(cd "$(dirname "$0")" && pwd)/../Tests/Fixtures/common.sh"
VERSIONS_DEFAULT=(2017 2019 2022 2025)
LOG_DIR="${NIO_LAB_LOG_DIR:-.build/testlab}"

port_for() { echo "144${1: -2}"; }
name_for() { echo "nio-lab-$1"; }

up_one() {
    local version=$1 name port
    name=$(name_for "$version"); port=$(port_for "$version")
    if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = "true" ]; then
        echo "$name already running on port $port"; return 0
    fi
    remove_containers "$name"
    ensure_network
    # shellcheck disable=SC2046
    docker run -d --name "$name" --label nio-lab=1 --network nio-lab $(platform_args) "${SQL_MEMORY_ARGS[@]}" \
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
export TDS_HOSTNAME=$LAB_ADDRESS
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

case "${1:-}" in
    up)
        shift; versions=("$@"); [ ${#versions[@]} -eq 0 ] && versions=("${VERSIONS_DEFAULT[@]}")
        for v in "${versions[@]}"; do up_one "$v"; done
        for v in "${versions[@]}"; do wait_one "$v"; done
        ;;
    test)
        shift; test_one "$@" ;;
    matrix)
        # The full suite on every version is CI's job (GitHub runners). On the
        # shared lab server it runs only when asked for explicitly.
        if [ "$NIO_LAB_HOST" != "local" ] && [ "${NIO_LAB_ALLOW_MATRIX:-0}" != "1" ]; then
            log "The version matrix runs in CI. To run it on the lab server anyway: NIO_LAB_ALLOW_MATRIX=1"
            log "For a targeted check: testlab/testlab.sh up 2022 && testlab/testlab.sh test 2022 FILTER"
            exit 2
        fi
        # One version at a time, removed after its run.
        shift; filter=("$@")
        failed=()
        for v in "${VERSIONS_DEFAULT[@]}"; do
            up_one "$v"
            if wait_one "$v"; then
                test_one "$v" ${filter[@]+"${filter[@]}"} || failed+=("$v")
            else
                failed+=("$v")
            fi
            remove_containers "$(name_for "$v")"
        done
        if [ ${#failed[@]} -gt 0 ]; then echo "FAILED on: ${failed[*]}"; exit 1; fi
        echo "All versions passed."
        ;;
    env)
        shift; env_for "$1" ;;
    down)
        # shellcheck disable=SC2046
        remove_containers $(docker ps -aq --filter label=nio-lab=1)
        log "lab containers removed ($NIO_LAB_HOST)" ;;
    *)
        sed -n '2,9p' "$0"; exit 2 ;;
esac
