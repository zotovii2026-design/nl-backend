#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${1:-/opt/nl-table/nl-backend}"

install -m 0755 \
    "$PROJECT_DIR/scripts/production_healthcheck.sh" \
    /usr/local/sbin/nl-table-healthcheck
install -m 0644 \
    "$PROJECT_DIR/deploy/systemd/nl-table-healthcheck.service" \
    /etc/systemd/system/nl-table-healthcheck.service
install -m 0644 \
    "$PROJECT_DIR/deploy/systemd/nl-table-healthcheck.timer" \
    /etc/systemd/system/nl-table-healthcheck.timer

systemctl daemon-reload
systemctl enable --now nl-table-healthcheck.timer
systemctl start nl-table-healthcheck.service
