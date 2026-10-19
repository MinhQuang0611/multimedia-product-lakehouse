#!/bin/bash
set -euo pipefail

superset db upgrade
superset fab create-admin --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD" \
    --firstname Admin --lastname BTL --email admin@btl.local || true
superset init
DW_URI="postgresql+psycopg2://${DW_BI_USER}:${DW_BI_PASSWORD}@postgres:5432/${DW_DBNAME}"
superset set-database-uri --database_name "DW (PostgreSQL)" --uri "$DW_URI"

exec /usr/bin/run-server.sh
