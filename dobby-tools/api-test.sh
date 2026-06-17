#!/bin/bash
# dobby-tools/api-test.sh — дёрнуть API эндпоинт и показать ответ
# Пример: ./api-test.sh /health
#         ./api-test.sh /api/v1/users

ENDPOINT=${1:-/health}
echo "=== GET http://localhost:8000$ENDPOINT ==="
curl -s -w "\nHTTP_CODE: %{http_code}\n" "http://localhost:8000$ENDPOINT" 2>/dev/null
