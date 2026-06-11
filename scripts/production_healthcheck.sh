#!/usr/bin/env bash
set -uo pipefail

PROJECT_DIR="${NL_TABLE_PROJECT_DIR:-/opt/nl-table/nl-backend}"
DISK_LIMIT_PERCENT="${NL_TABLE_DISK_LIMIT_PERCENT:-85}"
ALERT_WEBHOOK_URL="${NL_TABLE_ALERT_WEBHOOK_URL:-}"
failures=()

add_failure() {
    failures+=("$1")
}

cd "$PROJECT_DIR" || exit 2

curl --fail --silent --show-error --max-time 10 \
    http://127.0.0.1:8000/health >/dev/null \
    || add_failure "HTTP health check failed"

for service in app postgres redis celery-worker celery-beat; do
    container_id="$(docker compose ps -q "$service" 2>/dev/null)"
    if [[ -z "$container_id" ]]; then
        add_failure "$service container is missing"
        continue
    fi
    state="$(docker inspect --format '{{.State.Status}}' "$container_id" 2>/dev/null)"
    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null)"
    [[ "$state" == "running" ]] || add_failure "$service state=$state"
    [[ "$health" == "healthy" || "$health" == "none" ]] \
        || add_failure "$service health=$health"
done

docker compose exec -T postgres sh -c \
    'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null \
    || add_failure "PostgreSQL is not ready"

[[ "$(docker compose exec -T redis redis-cli ping 2>/dev/null | tr -d '\r')" == "PONG" ]] \
    || add_failure "Redis ping failed"

if celery_ping="$(
    docker compose exec -T celery-worker \
        celery -A tasks.celery_app:celery_app inspect ping --timeout=10 \
        2>/dev/null
)"; then
    grep -q pong <<<"$celery_ping" || add_failure "Celery worker ping failed"
else
    add_failure "Celery worker ping failed"
fi

disk_usage="$(df -P / | awk 'NR==2 {gsub("%", "", $5); print $5}')"
if [[ "$disk_usage" =~ ^[0-9]+$ ]] && (( disk_usage >= DISK_LIMIT_PERCENT )); then
    add_failure "Disk usage is ${disk_usage}%"
fi

if ((${#failures[@]} == 0)); then
    echo "NL Table health check: OK"
    exit 0
fi

message="$(printf '%s; ' "${failures[@]}")"
echo "NL Table health check: FAILED: $message" >&2

if [[ -n "$ALERT_WEBHOOK_URL" ]]; then
    payload="$(python3 -c 'import json,sys; print(json.dumps({"source":"nl-table-monitor","status":"failed","message":sys.argv[1]}))' "$message")"
    curl --fail --silent --show-error --max-time 10 \
        -H "Content-Type: application/json" \
        -d "$payload" "$ALERT_WEBHOOK_URL" >/dev/null \
        || true
fi

exit 1
