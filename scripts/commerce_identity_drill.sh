#!/usr/bin/env bash
set -Eeuo pipefail

readonly TARGET_REF="${1:-origin/rollout/common-reference}"
readonly SOURCE_REPO="${NL_DRILL_SOURCE_REPO:-/opt/nl-table/nl-backend}"
readonly DUMP_INPUT="${NL_DRILL_DUMP:-/opt/nl-table/backup-agent/state/latest.dump}"
readonly DUMP="$(readlink -f "$DUMP_INPUT")"
readonly APP_IMAGE="${NL_DRILL_APP_IMAGE:-nl-backend-app}"
readonly BASE_REVISION="b4c5d6e7f8a9"
readonly TARGET_REVISION="e5f6g7h8i9j0"
readonly STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
readonly WORKTREE="/tmp/nl-commerce-drill-${STAMP}"
readonly PG_CONTAINER="nl-commerce-pg-${STAMP,,}"
readonly NETWORK="nl-commerce-net-${STAMP,,}"
readonly DB_URL="postgresql+asyncpg://postgres@postgres:5432/restore_drill"

cleanup() {
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    git -C "$SOURCE_REPO" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

[[ -s "$DUMP" ]] || { echo "missing dump: $DUMP" >&2; exit 1; }
git -C "$SOURCE_REPO" fetch origin rollout/common-reference
git -C "$SOURCE_REPO" worktree add --detach "$WORKTREE" "$TARGET_REF"

docker network create --internal "$NETWORK" >/dev/null
docker run -d --name "$PG_CONTAINER" --network "$NETWORK" --network-alias postgres \
    --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=3g \
    -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16-alpine >/dev/null

for _ in $(seq 1 90); do
    docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null
docker exec "$PG_CONTAINER" createdb -U postgres restore_drill
docker cp "$DUMP" "$PG_CONTAINER:/tmp/source.dump" >/dev/null
docker exec "$PG_CONTAINER" pg_restore -U postgres -d restore_drill \
    --no-owner --no-acl --exit-on-error /tmp/source.dump

sql() {
    docker exec "$PG_CONTAINER" psql -U postgres -d restore_drill -Atqc "$1"
}

run_alembic() {
    docker run --rm --network "$NETWORK" \
        -e DATABASE_URL="$DB_URL" -e REDIS_URL=redis://127.0.0.1:6379/0 \
        -v "$WORKTREE:/app" -w /app "$APP_IMAGE" alembic "$@"
}

legacy_entities_before="$(sql 'select count(*) from product_entities')"
legacy_products_before="$(sql 'select count(*) from (select organization_id,nm_id from product_entities group by organization_id,nm_id) q')"
legacy_barcodes_before="$(sql 'select count(*) from entity_barcodes')"
reference_before="$(sql 'select count(*) from reference_book')"

echo "source_ref=$(git -C "$WORKTREE" rev-parse HEAD)"
echo "baseline entities=$legacy_entities_before products=$legacy_products_before barcodes=$legacy_barcodes_before reference=$reference_before"
echo "heads=$(run_alembic heads | tr '\n' ' ')"

run_alembic upgrade head
[[ "$(run_alembic current 2>/dev/null | tail -1 | awk '{print $1}')" == "$TARGET_REVISION" ]]

products_after="$(sql 'select count(*) from products')"
variants_after="$(sql 'select count(*) from product_variants')"
listings_after="$(sql 'select count(*) from marketplace_listings')"
listing_variants_after="$(sql 'select count(*) from listing_variants')"
barcodes_after="$(sql 'select count(*) from variant_barcodes')"
links_after="$(sql 'select count(*) from legacy_product_entity_links')"
duplicate_active_barcodes="$(sql "select count(*) from (select organization_id,barcode from variant_barcodes where is_active group by organization_id,barcode having count(*)>1) q")"
tenant_mismatches="$(sql 'select count(*) from listing_variants lv join marketplace_listings ml on ml.id=lv.marketplace_listing_id where lv.organization_id<>ml.organization_id')"

[[ "$products_after" == "$legacy_products_before" ]]
[[ "$variants_after" == "$legacy_entities_before" ]]
[[ "$listings_after" == "$legacy_products_before" ]]
[[ "$listing_variants_after" == "$legacy_entities_before" ]]
[[ "$barcodes_after" == "$legacy_barcodes_before" ]]
[[ "$links_after" == "$legacy_entities_before" ]]
[[ "$duplicate_active_barcodes" == "0" ]]
[[ "$tenant_mismatches" == "0" ]]
[[ "$(sql 'select count(*) from reference_book')" == "$reference_before" ]]

run_alembic upgrade head
echo "upgrade_ok products=$products_after variants=$variants_after listings=$listings_after listing_variants=$listing_variants_after barcodes=$barcodes_after links=$links_after"

run_alembic downgrade "$BASE_REVISION"
[[ "$(run_alembic current 2>/dev/null | tail -1 | awk '{print $1}')" == "$BASE_REVISION" ]]
[[ "$(sql "select count(*) from pg_tables where schemaname='public' and tablename in ('marketplace_accounts','products','product_variants','marketplace_listings','listing_variants','variant_barcodes','legacy_product_entity_links')")" == "0" ]]
[[ "$(sql 'select count(*) from product_entities')" == "$legacy_entities_before" ]]
[[ "$(sql 'select count(*) from entity_barcodes')" == "$legacy_barcodes_before" ]]
[[ "$(sql 'select count(*) from reference_book')" == "$reference_before" ]]
echo "downgrade_ok revision=$BASE_REVISION"

run_alembic upgrade head
[[ "$(run_alembic current 2>/dev/null | tail -1 | awk '{print $1}')" == "$TARGET_REVISION" ]]
[[ "$(sql 'select count(*) from products')" == "$legacy_products_before" ]]
[[ "$(sql 'select count(*) from product_variants')" == "$legacy_entities_before" ]]
echo "reupgrade_ok revision=$TARGET_REVISION"
echo "commerce_identity_drill=success"
