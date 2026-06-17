#!/bin/bash
# dobby-tools/restart.sh — безопасный рестарт docker compose
# Делает бэкап config, пишет лог действий, использует restart (не down/up)

set -e
COMPOSE_DIR="/opt/nl-table/nl-backend"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
BACKUP_DIR="$COMPOSE_DIR/dobby-tools/backups"
LOG_FILE="$COMPOSE_DIR/dobby-tools/action.log"

mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

echo "[$TIMESTAMP] restart.sh start" >> "$LOG_FILE"

# Бэкап compose файла
if [ -f "$COMPOSE_FILE" ]; then
    cp "$COMPOSE_FILE" "$BACKUP_DIR/docker-compose.yml.$TIMESTAMP"
    echo "[$TIMESTAMP] backup → backups/docker-compose.yml.$TIMESTAMP" >> "$LOG_FILE"
else
    echo "[$TIMESTAMP] ERROR: compose file not found" >> "$LOG_FILE"
    echo "ERROR: $COMPOSE_FILE не найден!"
    exit 1
fi

echo "Бэкап создан: backups/docker-compose.yml.$TIMESTAMP"
echo "Рестарт docker compose..."

cd "$COMPOSE_DIR"
docker compose restart
RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo "[$TIMESTAMP] restart OK" >> "$LOG_FILE"
    echo "✅ Рестарт успешен"
    echo ""
    echo "--- Health check ---"
    sleep 3
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null)
    echo "GET /health → $HTTP"
else
    echo "[$TIMESTAMP] restart FAILED ($RESULT)" >> "$LOG_FILE"
    echo "❌ Ошибка рестарта (код $RESULT)"
    echo "Откат: cp $BACKUP_DIR/docker-compose.yml.$TIMESTAMP $COMPOSE_FILE && docker compose restart"
fi
echo "[$TIMESTAMP] restart.sh end" >> "$LOG_FILE"
