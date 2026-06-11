# Production

## Public entry point

NL Table is published at `https://nl.vsepro100ai.ru`. Nginx terminates TLS and
proxies to `127.0.0.1:8000`. PostgreSQL, Redis and the application port are not
intended to be publicly accessible.

The Nginx source configuration is stored in
`deploy/nginx/nl.vsepro100ai.ru.conf`. Certificates are issued and renewed by
Certbot.

## Continuous integration

`.github/workflows/ci.yml` runs for `main`, refactor branches and pull requests.
It:

1. compiles the Python packages;
2. verifies that Alembic has one head;
3. migrates an empty PostgreSQL 16 database;
4. runs unit and contract tests;
5. starts an isolated HTTP application and runs characterization tests;
6. builds the production Docker image.

## Production deployment

Production deployment is manual through the `Deploy production` GitHub Actions
workflow. The workflow reruns CI before connecting to the server.

Required GitHub production secrets:

- `PRODUCTION_HOST`;
- `PRODUCTION_USER`;
- `PRODUCTION_KNOWN_HOSTS`;
- `DEPLOY_SSH_KEY`.

The deployment script:

1. refuses a dirty production worktree and concurrent deployment;
2. creates a custom-format PostgreSQL dump;
3. checks out the tested commit;
4. builds images and applies Alembic migrations;
5. restarts app, worker and beat;
6. waits for `/health`;
7. restores the previous code if build or health verification fails.

Backups are written to `/opt/nl-table/backups`.

## Monitoring

`nl-table-healthcheck.timer` runs every five minutes and verifies:

- HTTP and database health;
- all five Compose services;
- Docker health status;
- PostgreSQL readiness;
- Redis ping;
- Celery worker ping;
- root filesystem utilization.

Failures are visible in the system journal:

```bash
journalctl -u nl-table-healthcheck.service
```

Set `NL_TABLE_ALERT_WEBHOOK_URL` in `/etc/nl-table-monitor.env` to deliver
structured failure notifications to an external receiver.
