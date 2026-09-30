# Samba Active Directory domain controller for the sqlserver-nio Kerberos lab.
FROM ubuntu:24.04
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      samba samba-ad-provision samba-ad-dc krb5-user winbind smbclient ldb-tools ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY dc-entrypoint.sh /usr/local/bin/dc-entrypoint.sh
RUN chmod +x /usr/local/bin/dc-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/dc-entrypoint.sh"]
