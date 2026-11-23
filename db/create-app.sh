#!/usr/bin/env bash
set -euo pipefail

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set=ON_ERROR_STOP=1 \
  --set=app_password="$PARTNER_APP_PASSWORD" <<'SQL'
CREATE ROLE delivery_app LOGIN PASSWORD :'app_password';
CREATE DATABASE delivery_app OWNER delivery_app;
REVOKE CONNECT ON DATABASE airflow_meta FROM PUBLIC;
REVOKE CONNECT ON DATABASE delivery_app FROM PUBLIC;
SQL
