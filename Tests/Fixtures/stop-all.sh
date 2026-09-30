#!/usr/bin/env bash
# Removes every fixture container (label nio-lab=1), with its volumes, and the
# fixture networks, on the host NIO_LAB_HOST selects (the lab server unless
# NIO_LAB_HOST=local).
set -euo pipefail
source "$(dirname "$0")/common.sh"
# shellcheck disable=SC2046
remove_containers $(docker ps -aq --filter label=nio-lab=1)
for network in nio-lab nio-lab-ad; do docker network rm "$network" >/dev/null 2>&1 || true; done
log "fixture containers removed ($NIO_LAB_HOST)"
