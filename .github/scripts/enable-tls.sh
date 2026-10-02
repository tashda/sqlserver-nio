#!/usr/bin/env bash
# CI only: gives a SQL Server service container a certificate from a throwaway CA, forces
# encryption (and TDS 8.0 Strict with --strict), restarts it and prints the CA's path.
#   .github/scripts/enable-tls.sh <container> <directory for the CA> [--strict]
set -euo pipefail
container=$1 dir=$2 strict=${3:-}
mkdir -p "$dir"
cd "$dir"
if [ ! -f ca.pem ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=sqlserver-nio CI CA" -keyout ca.key -out ca.pem 2>/dev/null
fi
printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n' > server.ext
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 30 -extfile server.ext -out server.pem 2>/dev/null
printf '[network]\ntlscert = /var/opt/mssql/tls/server.pem\ntlskey = /var/opt/mssql/tls/server.key\ntlsprotocols = 1.2\nforceencryption = 1\n' > mssql.conf
[ "$strict" = "--strict" ] && printf 'forcestrict = 1\n' >> mssql.conf
docker exec -u 0 "$container" mkdir -p /var/opt/mssql/tls
docker cp server.pem "$container:/var/opt/mssql/tls/server.pem"
docker cp server.key "$container:/var/opt/mssql/tls/server.key"
docker cp mssql.conf "$container:/var/opt/mssql/mssql.conf"
docker exec -u 0 "$container" chown -R mssql /var/opt/mssql/tls /var/opt/mssql/mssql.conf
docker restart "$container" >/dev/null
echo "$dir/ca.pem"
