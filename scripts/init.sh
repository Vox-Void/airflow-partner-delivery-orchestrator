#!/usr/bin/env bash
set -euo pipefail

airflow db migrate
if ! airflow users list --output json | python -c 'import json, os, sys; sys.exit(not any(user["username"] == os.environ["PARTNER_AIRFLOW_USER"] for user in json.load(sys.stdin)))'; then
  airflow users create --username "$PARTNER_AIRFLOW_USER" --password "$PARTNER_AIRFLOW_PASSWORD" \
    --firstname Local --lastname Operator --role Admin --email operator@example.invalid
fi
airflow pools set partner_read 1 'Read partner input files'
airflow pools set reporting_write 1 'Commit curated releases and reports'
python -m partner_delivery.store
