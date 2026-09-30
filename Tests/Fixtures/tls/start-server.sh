#!/usr/bin/env bash
# LabTLSTests: SQL Server with certificates from a lab CA.
#   nio-lab-tls        :14431  valid certificate (names sql-tls.nio.test, localhost, 127.0.0.1), TLS 1.2
#   nio-lab-strict     :14432  the same, TDS 8.0 Strict (SQL Server 2025)
#   nio-lab-expired    :14433  expired certificate
#   nio-lab-tls10      :14434  TLS 1.0 only (SQL Server 2022, OpenSSL security level 0)
#   nio-lab-selfsigned :14435  SQL Server's own self-signed certificate
#
#   eval "$(Tests/Fixtures/tls/start-server.sh)"
set -euo pipefail
source "$(dirname "$0")/../common.sh"

CERT_DIR="$STATE_ROOT/tls-certs"
TLS_HOST="sql-tls.nio.test"

# Lab CA plus server certificates: valid and expired. Made only when missing.
make_certs() {
    mkdir -p "$CERT_DIR"
    (
        cd "$CERT_DIR"
        [ -f ca.pem ] || openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=sqlserver-nio lab CA" \
            -keyout ca.key -out ca.pem 2>/dev/null
        cat > server.ext <<EXT
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:$TLS_HOST,DNS:localhost,IP:127.0.0.1
EXT
        if [ ! -f valid.pem ]; then
            openssl req -newkey rsa:2048 -nodes -subj "/CN=$TLS_HOST" -keyout valid.key -out valid.csr 2>/dev/null
            openssl x509 -req -in valid.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 \
                -extfile server.ext -out valid.pem 2>/dev/null
        fi
        if [ ! -f expired.pem ]; then
            # `openssl ca` takes explicit validity dates on every OpenSSL and
            # LibreSSL release (x509 -not_before needs OpenSSL 3.4).
            openssl req -newkey rsa:2048 -nodes -subj "/CN=$TLS_HOST" -keyout expired.key -out expired.csr 2>/dev/null
            mkdir -p ca-db && : > ca-db/index.txt && echo 1000 > ca-db/serial
            cat > ca-db/ca.cnf <<CNF
[ ca ]
default_ca = lab
[ lab ]
database = ca-db/index.txt
serial = ca-db/serial
new_certs_dir = ca-db
default_md = sha256
policy = anything
copy_extensions = none
[ anything ]
commonName = supplied
CNF
            openssl ca -batch -config ca-db/ca.cnf -cert ca.pem -keyfile ca.key -in expired.csr \
                -startdate 20200101000000Z -enddate 20210101000000Z -extfile server.ext \
                -out expired.pem 2>/dev/null
        fi
        # A second CA the driver is never told about.
        [ -f other-ca.pem ] || openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=untrusted lab CA" \
            -keyout other-ca.key -out other-ca.pem 2>/dev/null
        chmod 644 ./*.pem ./*.key
    )
}

# SQL Server rewrites mssql.conf at start-up, so it is copied into the
# container; a read-only mount keeps it from starting.
start_tls_server() {
    local name=$1 port=$2 cert=$3 strict=$4 version=$5 protocols=$6
    docker rm -f "$name" >/dev/null 2>&1 || true
    local conf; conf=$(mktemp)
    printf '[network]\ntlscert = /var/opt/mssql/tls/server.pem\ntlskey = /var/opt/mssql/tls/server.key\ntlsprotocols = %s\nforceencryption = 1\n' "$protocols" > "$conf"
    [ "$strict" = 1 ] && printf 'forcestrict = 1\n' >> "$conf"
    chmod 666 "$conf"
    # shellcheck disable=SC2046
    docker create --name "$name" --label nio-lab=1 $(platform_args) \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PASSWORD" -p "$port:1433" \
        -v "$CERT_DIR/$cert.pem:/var/opt/mssql/tls/server.pem:ro" \
        -v "$CERT_DIR/$cert.key:/var/opt/mssql/tls/server.key:ro" \
        "$SQL_IMAGE_PREFIX:$version-latest" >/dev/null
    docker cp "$conf" "$name:/var/opt/mssql/mssql.conf" >/dev/null
    rm -f "$conf"
    if [ "$protocols" != "1.2" ]; then
        # OpenSSL 3 disables TLS 1.0 and 1.1 above security level 0. SQL
        # Server on Windows (the real legacy case) uses SChannel instead.
        local ssl; ssl=$(mktemp)
        docker cp "$name:/etc/ssl/openssl.cnf" "$ssl" >/dev/null
        perl -pi -e 's/CipherString = DEFAULT:\@SECLEVEL=2/CipherString = DEFAULT:\@SECLEVEL=0\nMinProtocol = TLSv1/' "$ssl"
        docker cp "$ssl" "$name:/etc/ssl/openssl.cnf" >/dev/null
        rm -f "$ssl"
    fi
    docker start "$name" >/dev/null
}

make_certs
start_tls_server nio-lab-tls 14431 valid 0 2025 1.2
start_tls_server nio-lab-strict 14432 valid 1 2025 1.2
start_tls_server nio-lab-expired 14433 expired 0 2025 1.2
start_tls_server nio-lab-tls10 14434 valid 0 2022 1.0
run_sql_server nio-lab-selfsigned 14435 2022
for name in nio-lab-tls nio-lab-strict nio-lab-expired nio-lab-tls10; do wait_log "$name"; done
wait_sql nio-lab-selfsigned
log "TLS fixture ready (ports 14431-14435)"

cat <<VARS
export NIO_LAB_TLS_HOST=127.0.0.1
export NIO_LAB_TLS_CERT_NAME=$TLS_HOST
export NIO_LAB_TLS_CA=$CERT_DIR/ca.pem
export NIO_LAB_TLS_OTHER_CA=$CERT_DIR/other-ca.pem
export NIO_LAB_TLS_PORT=14431
export NIO_LAB_STRICT_PORT=14432
export NIO_LAB_EXPIRED_PORT=14433
export NIO_LAB_TLS10_PORT=14434
export NIO_LAB_SELFSIGNED_PORT=14435
export NIO_LAB_TLS_USERNAME=sa
export NIO_LAB_TLS_PASSWORD=$PASSWORD
VARS
