#!/bin/bash
# Скрипт запускается ПОСЛЕ initdb но ДО основного postgres
# По умолчанию добавляем парольную auth для Docker сети.
# Trust можно включить только явно для локальных одноразовых стендов.
auth_method="${POSTGRES_DOCKER_AUTH_METHOD:-scram-sha-256}"
if [ "${NL_ALLOW_TRUSTED_DOCKER_HBA:-}" = "1" ]; then
  auth_method="trust"
fi

cat >> "$PGDATA/pg_hba.conf" << EOF

# Custom: Docker internal networks
host    all             all             172.16.0.0/12          ${auth_method}
host    all             all             192.168.0.0/16         ${auth_method}
EOF
