#!/bin/bash
# dobby-tools/logs.sh — хвост логов backend
# Аргумент: число строк (по умолчанию 30)
# Пример: ./logs.sh 50

LINES=${1:-30}
echo "=== Последние $LINES строк логов nl-backend-app ==="
docker logs nl-backend-app --tail "$LINES" 2>&1
echo ""
echo "=== Celery worker (последние 10) ==="
docker logs nl-backend-celery-worker --tail 10 2>&1
