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
ssh -L 18000:127.0.0.1:18000 root@your-test-server
```

Then open `http://127.0.0.1:18000/nl/login`.

## Characterization tests

The minimal suite checks health, public pages, registration, login, profile,
organization listing, and rejection of access to another organization. It
creates only synthetic records in the isolated test database.

```bash
python -m venv .venv-test
.venv-test/bin/pip install -r requirements-dev.txt
NL_TEST_BASE_URL=http://127.0.0.1:18000 \
  .venv-test/bin/pytest tests/characterization -v
```

The suite refuses to run when `NL_TEST_BASE_URL` is not a loopback address.

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
