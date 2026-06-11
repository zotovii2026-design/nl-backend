#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${NL_TABLE_PROJECT_DIR:-/opt/nl-table/nl-backend}"
BACKUP_DIR="${NL_TABLE_BACKUP_DIR:-/opt/nl-table/backups}"
TARGET_REF="${1:-origin/main}"

cd "$PROJECT_DIR"
exec 9>/var/lock/nl-table-deploy.lock
flock -n 9 || {
    echo "Another NL Table deploy is running" >&2
    exit 1
}

if [[ -n "$(git status --porcelain)" ]]; then
    echo "Production worktree is dirty" >&2
    exit 1
fi
git fetch origin --prune

previous_ref="$(git rev-parse HEAD)"
target_ref="$(git rev-parse "$TARGET_REF")"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="$BACKUP_DIR/pre-deploy-$timestamp.dump"

mkdir -p "$BACKUP_DIR"
docker compose exec -T postgres sh -c \
    'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' >"$backup_path"
chmod 600 "$backup_path"

rollback() {
    exit_code=$?
    trap - ERR
    echo "Deploy failed; restoring code $previous_ref" >&2
    git checkout --detach "$previous_ref"
    docker compose build app celery-worker celery-beat
    docker compose up -d app celery-worker celery-beat
    exit "$exit_code"
}
trap rollback ERR

git checkout --detach "$target_ref"
docker compose config --quiet
docker compose build app celery-worker celery-beat
docker compose run --rm app alembic upgrade head
docker compose up -d app celery-worker celery-beat

for attempt in $(seq 1 30); do
    if curl --fail --silent --max-time 5 \
        http://127.0.0.1:8000/health >/dev/null; then
        break
    fi
    if [[ "$attempt" == "30" ]]; then
        echo "Application did not become healthy" >&2
        false
    fi
    sleep 2
done

if [[ "$EUID" -eq 0 ]]; then
    "$PROJECT_DIR/scripts/install_production_monitoring.sh" "$PROJECT_DIR"
elif systemctl is-enabled nl-table-healthcheck.timer >/dev/null 2>&1; then
    /usr/local/sbin/nl-table-healthcheck
fi
docker compose ps

trap - ERR
echo "Deployed $target_ref (previous $previous_ref)"
echo "Backup: $backup_path"
