#!/usr/bin/env bash
# Test lab for sqlserver-nio: starts SQL Server containers and runs the test
# suite against them. Containers are named nio-lab-* and labelled nio-lab=1.
#
#   testlab/testlab.sh up 2017 2019 2022 2025   start and wait until ready
#   testlab/testlab.sh test 2022 [filter]         run swift test against one
#   testlab/testlab.sh matrix [filter]            up + test every version
#   testlab/testlab.sh env 2022                   print the TDS_* variables
#   testlab/testlab.sh down                       remove all lab containers
set -euo pipefail

PASSWORD="${NIO_LAB_PASSWORD:-NioLab#Pass123}"
VERSIONS_DEFAULT=(2017 2019 2022 2025)
LOG_DIR="${NIO_LAB_LOG_DIR:-.build/testlab}"

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
    # shellcheck disable=SC2046
    docker run -d --name "$name" --label nio-lab=1 $(platform_args) \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -e MSSQL_AGENT_ENABLED=true \
        -p "$port:1433" "mcr.microsoft.com/mssql/server:$version-latest" >/dev/null
}

wait_one() {
    local version=$1 name; name=$(name_for "$version")
    for _ in $(seq 1 120); do
        if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
            echo "error: $name stopped while starting" >&2
            docker logs "$name" 2>&1 | grep -v '^find:' | tail -20 >&2 || true
            return 1
        fi
        if docker logs "$name" 2>&1 | grep -q "SQL Server is now ready for client connections"; then
            # The log line precedes the end of recovery; wait for a login.
            if docker exec "$name" /bin/bash -c "for t in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do [ -x \$t ] && exec \$t -S localhost -U sa -P '$PASSWORD' -C -Q 'SELECT 1' -b; done; exit 1" >/dev/null 2>&1; then
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

case "${1:-}" in
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
