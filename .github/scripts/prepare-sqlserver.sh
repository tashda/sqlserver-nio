#!/usr/bin/env bash
# CI only: waits for a SQL Server service container to take logins, then restores AdventureWorks
# (microsoft/sql-server-samples) for the tests written against it.
#   .github/scripts/prepare-sqlserver.sh <container> <password> <version: 2017|2019|2022|2025> [--no-adventureworks]
set -euo pipefail
container=$1 password=$2 version=$3
here=$(dirname "$0")

for _ in $(seq 1 90); do
    if "$here/sqlcmd.sh" "$container" "$password" -Q "SELECT 1" >/dev/null 2>&1; then break; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ]; then
        echo "::error::The SQL Server container stopped:"
        docker inspect -f 'exit code {{.State.ExitCode}}, OOM killed {{.State.OOMKilled}}' "$container" || true
        docker logs --tail 80 "$container" || true
        exit 1
    fi
    sleep 2
done
"$here/sqlcmd.sh" "$container" "$password" -Q "SELECT @@VERSION" -h -1

[ "${4:-}" = "--no-adventureworks" ] && exit 0
backup="AdventureWorks$version.bak"
curl -fsSL --retry 3 --retry-delay 5 -o "$RUNNER_TEMP/$backup" \
    "https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/$backup"
docker cp "$RUNNER_TEMP/$backup" "$container:/var/opt/mssql/data/AdventureWorks.bak"
"$here/sqlcmd.sh" "$container" "$password" -Q "
DECLARE @Files TABLE (LogicalName nvarchar(128), PhysicalName nvarchar(260), [Type] char(1), FileGroupName nvarchar(128), Size numeric(20,0), MaxSize numeric(20,0), FileId bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0), UniqueId uniqueidentifier, ReadOnlyLSN numeric(25,0), ReadWriteLSN numeric(25,0), BackupSizeInBytes bigint, SourceBlockSize int, FileGroupId int, LogGroupGUID uniqueidentifier, DifferentialBaseLSN numeric(25,0), DifferentialBaseGUID uniqueidentifier, IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32), SnapshotURL nvarchar(360));
INSERT INTO @Files EXEC('RESTORE FILELISTONLY FROM DISK = ''/var/opt/mssql/data/AdventureWorks.bak''');
DECLARE @Data nvarchar(128) = (SELECT TOP 1 LogicalName FROM @Files WHERE [Type] = 'D' ORDER BY FileId);
DECLARE @Log nvarchar(128) = (SELECT TOP 1 LogicalName FROM @Files WHERE [Type] = 'L');
DECLARE @Restore nvarchar(max) = 'RESTORE DATABASE AdventureWorks FROM DISK = ''/var/opt/mssql/data/AdventureWorks.bak'' WITH MOVE ''' + @Data + ''' TO ''/var/opt/mssql/data/AdventureWorks.mdf'', MOVE ''' + @Log + ''' TO ''/var/opt/mssql/data/AdventureWorks.ldf'', REPLACE';
EXEC(@Restore);"
echo "AdventureWorks restored from $backup"
