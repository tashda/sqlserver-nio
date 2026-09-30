#!/usr/bin/env bash
# LabFaultTests: Toxiproxy (HTTP API on 8474) in front of SQL Server 2022.
# The tests add latency, throttling, resets and silence through the API.
#   nio-lab-fault-sql   SQL Server 2022 (only reachable through the proxy)
#   nio-lab-toxiproxy   <lab address>:14440 -> nio-lab-fault-sql:1433
#
#   eval "$(Tests/Fixtures/faults/start-server.sh)"
set -euo pipefail
source "$(dirname "$0")/../common.sh"

ensure_network nio-lab
remove_containers nio-lab-fault-sql nio-lab-toxiproxy
# shellcheck disable=SC2046
docker run -d --name nio-lab-fault-sql --label nio-lab=1 --network nio-lab $(platform_args) "${SQL_MEMORY_ARGS[@]}" \
    -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" "$SQL_IMAGE_PREFIX:2022-latest" >/dev/null
docker run -d --name nio-lab-toxiproxy --label nio-lab=1 --network nio-lab "${SMALL_MEMORY_ARGS[@]}" \
    -p 8474:8474 -p 14440:14440 ghcr.io/shopify/toxiproxy:latest >/dev/null
wait_sql nio-lab-fault-sql
for _ in $(seq 1 30); do curl -sf http://$LAB_ADDRESS:8474/version >/dev/null && break; sleep 1; done
curl -sf -X POST http://$LAB_ADDRESS:8474/proxies -d \
    '{"name":"sql","listen":"0.0.0.0:14440","upstream":"nio-lab-fault-sql:1433","enabled":true}' >/dev/null
log "fault fixture ready: $LAB_ADDRESS:14440 -> nio-lab-fault-sql:1433"

cat <<VARS
export NIO_LAB_FAULT_HOST=$LAB_ADDRESS
export NIO_LAB_FAULT_PORT=14440
export NIO_LAB_TOXIPROXY=http://$LAB_ADDRESS:8474
export NIO_LAB_FAULT_PROXY=sql
export NIO_LAB_FAULT_USERNAME=sa
export NIO_LAB_FAULT_PASSWORD=$PASSWORD
VARS
