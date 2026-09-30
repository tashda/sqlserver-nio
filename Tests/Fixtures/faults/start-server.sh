#!/usr/bin/env bash
# LabFaultTests: Toxiproxy (HTTP API on 8474) in front of SQL Server 2022.
# The tests add latency, throttling, resets and silence through the API.
#   nio-lab-fault-sql   SQL Server 2022 (only reachable through the proxy)
#   nio-lab-toxiproxy   127.0.0.1:14440 -> nio-lab-fault-sql:1433
#
#   eval "$(Tests/Fixtures/faults/start-server.sh)"
set -euo pipefail
source "$(dirname "$0")/../common.sh"

ensure_network nio-lab
docker rm -f nio-lab-fault-sql nio-lab-toxiproxy >/dev/null 2>&1 || true
# shellcheck disable=SC2046
docker run -d --name nio-lab-fault-sql --label nio-lab=1 --network nio-lab $(platform_args) \
    -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" "$SQL_IMAGE_PREFIX:2022-latest" >/dev/null
docker run -d --name nio-lab-toxiproxy --label nio-lab=1 --network nio-lab \
    -p 8474:8474 -p 14440:14440 ghcr.io/shopify/toxiproxy:latest >/dev/null
wait_sql nio-lab-fault-sql
for _ in $(seq 1 30); do curl -sf http://127.0.0.1:8474/version >/dev/null && break; sleep 1; done
curl -sf -X POST http://127.0.0.1:8474/proxies -d \
    '{"name":"sql","listen":"0.0.0.0:14440","upstream":"nio-lab-fault-sql:1433","enabled":true}' >/dev/null
log "fault fixture ready: 127.0.0.1:14440 -> nio-lab-fault-sql:1433"

cat <<VARS
export NIO_LAB_FAULT_HOST=127.0.0.1
export NIO_LAB_FAULT_PORT=14440
export NIO_LAB_TOXIPROXY=http://127.0.0.1:8474
export NIO_LAB_FAULT_PROXY=sql
export NIO_LAB_FAULT_USERNAME=sa
export NIO_LAB_FAULT_PASSWORD=$PASSWORD
VARS
