# Endpoint security audit: 2026-06-11

## Scope

The audit used the 94 FastAPI routes registered at runtime on commit
`72797ea`. Dynamic checks ran only against the isolated test environment with
synthetic accounts and data. Production was not probed.

The test and production deployments use the same application commit, so the
production impact below is inferred from identical code.

## Executive summary

The application is not ready for public SaaS exposure.

- 55 runtime registrations (54 unique method/path pairs) accept `org_id`
  without a Bearer authentication dependency.
- 20 runtime registrations have a Bearer dependency, but five sync endpoints
  do not verify that the selected WB key belongs to the current user.
- One organization endpoint has a broken membership dependency and returns an
  organization to an unauthenticated caller.
- Refresh tokens are accepted as access tokens.
- The technical admin page uses a hardcoded token committed to the repository.
- CORS accepts arbitrary origins together with credentials.
- Eight legacy account endpoints transmit JWTs in query strings.

## Critical findings

### SEC-001: organization data is authorized by `org_id` alone

Severity: critical.

Any caller who knows or obtains an organization UUID can read or modify data
without a token. The affected surface includes reference data, cost prices,
tax settings, WB key management, analytics, sales plans, advertising,
promotions, prices, and external advertising.

Dynamic proof in the test environment:

- unauthenticated `POST /api/v1/nl/external-ads` returned 200 and created data;
- unauthenticated `GET /api/v1/nl/external-ads` returned that data;
- unauthenticated `GET /api/v1/nl/reference` returned 200.

Affected unique routes:

```text
GET,POST /api/v1/nl/reference
GET /api/v1/nl/products
GET,POST /api/v1/nl/tax-settings
GET,POST /api/v1/nl/wb-keys
DELETE /api/v1/nl/wb-keys/{key_id}
GET /api/v1/nl/fbs-warehouses
GET /api/v1/nl/dates
GET /api/v1/nl/control
GET /api/v1/nl/rnp
GET /api/v1/nl/analytics
GET /api/v1/nl/warehouses
GET,POST /api/v1/nl/operating-expenses
GET /api/v1/nl/opiu
GET,POST /api/v1/nl/cost-prices
POST /api/v1/nl/cost-prices/batch
GET /api/v1/nl/commission-rate
POST /api/v1/nl/cost-prices/auto-fill
GET /api/v1/nl/fbo-needs
POST /api/v1/nl/cost-prices/upload
GET,POST /api/v1/nl/sellers
GET,POST /api/v1/nl/seo-keywords
POST /api/v1/nl/seo-keywords/upload
GET,POST /api/v1/nl/sales-plans
PUT,DELETE /api/v1/nl/sales-plans/{plan_id}
POST /api/v1/nl/sales-plans/batch
GET /api/v1/nl/sales-plans/summary
GET /api/v1/nl/ad-stats
GET /api/v1/nl/ad-stats/by-art
GET /api/v1/nl/marketer/products
GET /api/v1/nl/marketer/product/{nm_id}
POST /api/v1/nl/prices/refresh
GET /api/v1/nl/prices/last-refresh
GET /api/v1/nl/promotions
GET /api/v1/nl/promotions/products
POST /api/v1/nl/promotions/products/save
POST /api/v1/nl/promotions/upload-excel
POST /api/v1/nl/promotions/sync-api
GET,POST /api/v1/nl/external-ads
GET,PUT,DELETE /api/v1/nl/external-ads/{ad_id}
POST /api/v1/nl/external-ads/bulk-update
GET /api/v1/nl/external-ads/sources/list
```

Required fix:

- require Bearer JWT for every organization-scoped route;
- resolve membership using `(current_user.id, organization_id)`;
- require viewer for reads and admin or owner for writes;
- constrain object lookups by both object ID and organization ID.

### SEC-002: unauthenticated organization lookup

Severity: critical.

`GET /api/v1/organizations/{org_id}` uses a lambda dependency that does not
bind the requested organization or current user correctly. An unauthenticated
request returned the organization record with status 200 in the test
environment.

Required fix: use the same explicit `current_user` plus
`require_organization_role(org_id, Role.VIEWER, current_user, db)` pattern as
the other organization handlers.

### SEC-003: sync endpoints allow cross-tenant WB key use

Severity: critical.

The five `/api/v1/sync/*` routes require a valid user but select a WB key only
by `api_key_id`. They do not verify membership in the key's organization.
An authenticated user who obtains another key UUID can make the server decrypt
and use that organization's WB credential.

Affected routes:

```text
POST /api/v1/sync/products
POST /api/v1/sync/sales
POST /api/v1/sync/orders
POST /api/v1/sync/stocks
GET /api/v1/sync/logs
```

Required fix: derive or load the organization from the selected key and require
an admin role before decrypting the token or starting network activity. Filter
sync logs by memberships of the current user.

### SEC-004: refresh token accepted as access token

Severity: high.

`get_current_user()` calls `decode_token()` but does not require
`payload["type"] == "access"`. A refresh token successfully authenticated
`GET /api/v1/auth/me` in the test environment. This turns the 30-day refresh
credential into a general API credential.

Required fix: validate token type in access dependencies and keep refresh-token
validation exclusive to `/auth/refresh`.

### SEC-005: hardcoded technical admin token

Severity: high.

`api/v1/admin_tech.py` contains `ADMIN_TOKEN = "nl-tech-2026"`. The repository
is public and the token is also transmitted in the URL, so it must be treated
as compromised.

Required fix: disable the route externally until it uses normal superuser
authentication. At minimum, rotate the token, load it from a secret, and stop
putting it in URLs.

## High and medium findings

### SEC-006: arbitrary credentialed CORS

Severity: high.

The application configures `allow_origins=["*"]` with credentials enabled.
The test environment reflected `https://evil.example` and returned
`Access-Control-Allow-Credentials: true`.

Required fix: configure an explicit environment-specific origin allowlist.

### SEC-007: JWTs in query strings

Severity: high.

Eight legacy endpoints accept `?token=...`. URLs leak into access logs, browser
history, monitoring, screenshots, and referrer data.

Affected routes include `/me`, organization management, profile, WB key
verification, rename, and invitation operations under `/api/v1/nl`.

Required fix: migrate all clients to `Authorization: Bearer` and remove query
token support after a short compatibility period.

### SEC-008: weak secret defaults

Severity: high for a misconfigured deployment.

`core/config.py` contains usable defaults for `SECRET_KEY`,
`ENCRYPTION_KEY`, database credentials, and WB API base URL. A missing `.env`
can start an apparently functional deployment with known secrets.

Required fix: make production secrets mandatory and fail startup when defaults
or short values are detected.

### SEC-009: no authentication rate limiting

Severity: medium.

Registration, login, refresh, WB key verification, and invitation endpoints
have no visible rate limiting or lockout policy.

Required fix: add per-IP and per-account limits, generic login errors, and
monitoring for repeated failures.

### SEC-010: detailed internal errors returned to clients

Severity: medium.

Sync handlers and health checks return raw exception text. This can expose
network, database, and integration details.

Required fix: log detailed errors with a request ID and return stable generic
messages to clients.

## Remediation order

1. Restrict public network access to the application until SEC-001 through
   SEC-005 are fixed.
2. Add a shared Bearer plus membership dependency and a frontend request
   wrapper that always sends the access token.
3. Protect the 54 organization-scoped routes in small domain batches, with
   tests for no token, own organization, and foreign organization.
4. Fix sync ownership checks and token-type validation.
5. Replace the technical admin token and query-token endpoints.
6. Lock down CORS and mandatory secret validation.
7. Add rate limiting, sanitized errors, and security monitoring.

Do not attempt a single untested authorization rewrite of `nl.py`. Protect one
domain at a time and run the characterization suite plus visual checks after
each batch.
