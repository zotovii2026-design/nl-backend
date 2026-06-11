# Test environment

This environment runs an isolated application, PostgreSQL database, Redis
instance, network, and volume. It does not start Celery worker or Celery Beat,
so scheduled Wildberries synchronization cannot run.

## First start

```bash
cp .env.test.example .env.test
# Replace POSTGRES_PASSWORD, SECRET_KEY, and ENCRYPTION_KEY.
# Keep the password in DATABASE_URL equal to POSTGRES_PASSWORD.

docker compose \
  --env-file .env.test \
  -f docker-compose.test.yml \
  up -d --build
```

The application listens on `127.0.0.1:18000` by default:

```bash
curl http://127.0.0.1:18000/health
```

Use an SSH tunnel to open it from another machine:

```bash
ssh -L 18000:127.0.0.1:18000 root@5.42.115.20
```

Then open `http://127.0.0.1:18000/nl/login`.

## Operations

```bash
# Status
docker compose --env-file .env.test -f docker-compose.test.yml ps

# Logs
docker compose --env-file .env.test -f docker-compose.test.yml logs app

# Stop without deleting test data
docker compose --env-file .env.test -f docker-compose.test.yml down

# Delete the isolated test database
docker compose --env-file .env.test -f docker-compose.test.yml down -v
```

Never copy the production `.env` into this directory. Keep `.env.test`
untracked and use only synthetic accounts and data.
