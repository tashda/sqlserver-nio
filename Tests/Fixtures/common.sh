# Shared by the fixture scripts (sourced, not run). Progress goes to stderr;
# each start script prints only the `export` lines its tests read on stdout,
# so `eval "$(Tests/Fixtures/<scenario>/start-server.sh)"` works, and CI can
# append them to $GITHUB_ENV with `sed 's/^export //'`.
#
# Containers are named nio-lab-* and labelled nio-lab=1
# (`Tests/Fixtures/stop-all.sh` removes them). Generated files live under
# .build/testlab, never in the user's home or system configuration.

FIXTURES="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$FIXTURES/../.." && pwd)"
STATE_ROOT="${NIO_LAB_STATE_DIR:-$REPO/.build/testlab}"
PASSWORD="${NIO_LAB_PASSWORD:-NioLab#Pass123}"
SQL_IMAGE_PREFIX="mcr.microsoft.com/mssql/server"

log() { echo "$@" >&2; }

platform_args() {
    case "$(uname -m)" in
        arm64|aarch64) echo "--platform linux/amd64" ;;
        *) echo "" ;;
    esac
}

ensure_network() {
    local name=${1:-nio-lab}
    docker network inspect "$name" >/dev/null 2>&1 || docker network create "$name" >/dev/null
}

# sqlcmd as sa inside a container (tools18 on current images, tools on 2017).
sql_in() {
    local container=$1 query=$2
    docker exec "$container" /bin/bash -c "for t in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do [ -x \$t ] && exec \$t -S localhost -U sa -P '$PASSWORD' -C -h -1 -W -b -Q \"SET NOCOUNT ON; $query\"; done; exit 1"
}

# Waits until sa can log in; fails at once when the container stops.
wait_sql() {
    local container=$1 seconds=${2:-300}
    local deadline=$((SECONDS + seconds))
    while [ $SECONDS -lt $deadline ]; do
        if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ]; then
            log "error: $container stopped while starting"
            docker logs "$container" 2>&1 | grep -v '^find:' | tail -20 >&2 || true
            return 1
        fi
        sql_in "$container" "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 2
    done
    log "error: $container did not accept logins within ${seconds}s"
    return 1
}

# Waits for the start-up log line (for servers whose sa login needs TLS the
# tools cannot negotiate, such as Strict or TLS 1.0 only).
wait_log() {
    local container=$1 seconds=${2:-300}
    local deadline=$((SECONDS + seconds))
    while [ $SECONDS -lt $deadline ]; do
        if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ]; then
            log "error: $container stopped while starting"
            docker logs "$container" 2>&1 | grep -v '^find:' | tail -20 >&2 || true
            return 1
        fi
        if docker logs "$container" 2>&1 | grep "SQL Server is now ready for client connections" >/dev/null; then
            sleep 3; return 0
        fi
        sleep 2
    done
    log "error: $container did not start within ${seconds}s"
    return 1
}

# run_sql_server <container> <host port> [version] [extra docker run arguments…]
run_sql_server() {
    local container=$1 port=$2 version=${3:-2022}; shift 3 || shift $#
    docker rm -f "$container" >/dev/null 2>&1 || true
    # shellcheck disable=SC2046
    docker run -d --name "$container" --label nio-lab=1 $(platform_args) \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -p "$port:1433" "$@" \
        "$SQL_IMAGE_PREFIX:$version-latest" >/dev/null
}
