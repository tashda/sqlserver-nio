#!/usr/bin/env bash
# Runs sqlcmd inside a SQL Server container (CI only; tests never call it).
#   .github/scripts/sqlcmd.sh <container> <password> <sqlcmd arguments…>
set -euo pipefail
container=$1 password=$2
shift 2
if docker exec "$container" test -x /opt/mssql-tools18/bin/sqlcmd; then
    exec docker exec "$container" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$password" -b "$@"
fi
exec docker exec "$container" /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$password" -b "$@"
