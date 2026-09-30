#!/usr/bin/env bash
# Removes every fixture container (label nio-lab=1) and the fixture networks.
set -euo pipefail
ids=$(docker ps -aq --filter label=nio-lab=1)
[ -n "$ids" ] && docker rm -f $ids >/dev/null
for network in nio-lab nio-lab-ad; do docker network rm "$network" >/dev/null 2>&1 || true; done
echo "fixture containers removed" >&2
