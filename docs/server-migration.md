# Server and Domain Migration

This runbook moves NL Table without changing application code. Orders and
stocks can be rebuilt from the WB API, but the database dump should still be
restored to preserve users, organizations, settings, reference data, and
historical calculations.

## 1. Prepare the destination

Install Git, Docker Engine, and Docker Compose. Allow inbound traffic only to
SSH and the reverse proxy. PostgreSQL and Redis bind to `127.0.0.1` by default.

Clone the repository and create the environment file:

```bash
git clone git@github.com:zotovii2026-design/nl-backend.git
cd nl-backend
cp .env.example .env
chmod 600 .env
```

Copy the production secrets through a secure channel. Do not create new
`SECRET_KEY` or `ENCRYPTION_KEY` values when restoring an existing database:
old sessions and encrypted WB tokens depend on them.

Set a strong `POSTGRES_PASSWORD` and use the same value in `DATABASE_URL`.
Change `APP_PORT` if port 8000 is unavailable. Keep the database hostname
`postgres` and Redis hostname `redis`; these are Docker service names and do
not depend on the server IP or public domain.

## 2. Validate before starting

```bash
docker compose config --quiet
docker compose build
```

The rendered configuration must not contain the old server IP or an unexpected
host path. Do not paste the rendered configuration into tickets or chat because
it includes secrets from `.env`.

## 3. Start infrastructure and restore data

```bash
docker compose up -d postgres redis
docker compose exec -T postgres pg_isready \
  -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-nl_table}"
```

Restore the checkpoint dump before starting API and Celery. For a custom-format
dump:

```bash
docker compose exec -T postgres pg_restore \
  --clean --if-exists --no-owner \
  -U "${POSTGRES_USER:-postgres}" \
  -d "${POSTGRES_DB:-nl_table}" < checkpoint.dump
```

Then apply repository migrations:

```bash
docker compose run --rm app alembic upgrade head
```

## 4. Start and verify the application

```bash
docker compose up -d app celery-worker celery-beat
docker compose ps
curl --fail "http://127.0.0.1:${APP_PORT:-8000}/health"
docker compose logs --tail=100 app celery-worker celery-beat
```

Verify login, organization selection, WB token decryption, reference-book
settings, and one manual synchronization before changing DNS.

## 5. Attach a domain

Point the domain's DNS record to the destination server. Configure a reverse
proxy to send HTTPS traffic to `127.0.0.1:${APP_PORT:-8000}`. The frontend uses
relative API and static paths, so changing the public hostname does not require
an application-code change.

Keep the old server available until the domain, login, key pages, API sync, and
Celery schedules have all been verified. Stop Celery Beat on the old server
before enabling it on the destination to avoid duplicate scheduled jobs.

## 6. Roll back

If verification fails, stop the destination stack, restore DNS to the old
server, and restart Celery Beat there. Preserve the failed destination and its
logs for diagnosis.

## Portability variables

- `APP_BIND_HOST`, `APP_PORT`: published API address and port.
- `POSTGRES_BIND_HOST`, `POSTGRES_PORT`: local PostgreSQL publication.
- `REDIS_BIND_HOST`, `REDIS_PORT`: local Redis publication.
- `POSTGRES_VOLUME_NAME`: explicit database volume to restore or reattach.
- `*_CONTAINER_NAME`: optional compatibility names for operational scripts.
- `COMPOSE_PROJECT_NAME`: stable Compose project name.
