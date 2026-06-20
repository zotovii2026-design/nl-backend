#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${NL_TABLE_PROJECT_DIR:-/opt/nl-table/nl-backend}"
BACKUP_DIR="${NL_TABLE_BACKUP_DIR:-/opt/nl-table/backups}"
TARGET_REF="${1:-origin/main}"

cd "$PROJECT_DIR"
GIT_COMMON_DIR="$(git rev-parse --git-common-dir)"
LOCK_PATH="${NL_TABLE_DEPLOY_LOCK:-$GIT_COMMON_DIR/nl-table-deploy.lock}"
touch "$LOCK_PATH"
if [[ "$EUID" -eq 0 ]]; then
    chmod 0666 "$LOCK_PATH"
fi
exec 9>"$LOCK_PATH"
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
echo "Creating pre-deploy database backup"
docker compose exec -T postgres sh -c \
    'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
    </dev/null >"$backup_path"
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

echo "Checking out $target_ref"
git checkout --detach "$target_ref"
echo "Building production images"
docker compose config --quiet
docker compose build app celery-worker celery-beat
echo "Applying database migrations"
docker compose run --rm app alembic upgrade head
echo "Restarting application services"
docker compose up -d app celery-worker celery-beat

echo "Waiting for application health"
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

echo "Waiting for Docker health checks"
for attempt in $(seq 1 60); do
    all_healthy=true
    for service in app celery-worker celery-beat; do
        container_id="$(docker compose ps -q "$service")"
        health="$(
            docker inspect \
                --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
                "$container_id"
        )"
        if [[ "$health" != "healthy" ]]; then
            all_healthy=false
            break
        fi
    done
    if [[ "$all_healthy" == "true" ]]; then
        break
    fi
    if [[ "$attempt" == "60" ]]; then
        echo "Docker health checks did not become healthy" >&2
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
