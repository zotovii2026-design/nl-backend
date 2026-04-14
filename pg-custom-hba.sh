#!/bin/bash
# Скрипт запускается ПОСЛЕ initdb но ДО основного postgres
# Добавляем trust для Docker сети
cat >> "$PGDATA/pg_hba.conf" << EOF

# Custom: trust для Docker внутренней сети
host    all             all             172.16.0.0/12          trust
host    all             all             192.168.0.0/16         trust
EOF
