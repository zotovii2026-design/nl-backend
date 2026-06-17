#!/bin/bash
# dobby-tools/status.sh — read-only проверка состояния NL Table
# Ничего не меняет, только читает

echo "=== NL TABLE STATUS ==="
echo "Время: $(date)"
echo ""

echo "--- Docker контейнеры ---"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null
echo ""

echo "--- Health check ---"
HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null)
echo "GET /health → $HTTP"
echo ""

echo "--- Последние логи backend (10 строк) ---"
docker logs nl-backend-app --tail 10 2>&1
echo ""

echo "--- PM2 ---"
pm2 list 2>/dev/null
echo ""
echo "=== END ==="
