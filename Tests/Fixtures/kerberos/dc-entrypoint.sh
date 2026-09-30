#!/bin/bash
# Provisions the domain on first start, then runs Samba in the foreground.
set -euo pipefail
if [ ! -f /var/lib/samba/private/sam.ldb ]; then
    rm -f /etc/samba/smb.conf
    samba-tool domain provision \
        --realm="$REALM" --domain="$DOMAIN" --server-role=dc \
        --dns-backend=SAMBA_INTERNAL --adminpass="$ADMIN_PASSWORD" \
        --host-ip="$HOST_IP" \
        --option="dns forwarder = 127.0.0.11" \
        --option="ldap server require strong auth = no"
    cp /var/lib/samba/private/krb5.conf /etc/krb5.conf
    samba-tool domain passwordsettings set --complexity=off --min-pwd-age=0 --max-pwd-age=0 >/dev/null
fi
exec samba --foreground --no-process-group
