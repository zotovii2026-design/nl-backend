#!/bin/bash
set -e
# Гарантируем пароль при первом init
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER USER postgres WITH PASSWORD 'postgres';
EOSQL
