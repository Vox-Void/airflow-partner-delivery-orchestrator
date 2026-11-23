# Airflow Partner Delivery Orchestrator

Schedule daily inventory and shipment reports for two fictional retail partners. The workflow checks each delivery, publishes the two partners together, and preserves separate revisions for corrections.

The demo follows a real Airflow schedule, a publish task that fails after its database commit, and a later revision. The report for January 13 changes from five to seven delivered units for partner p1. Both versions remain available for review.

## Run it

Use Linux or WSL2 with Docker Compose v2.14 or later and uv 0.13.9. Allocate at least 4 GB to Docker. The stack uses Airflow 3.4.0 and Celery, with PostgreSQL for storage and Redis for the worker queue. It downloads the pinned images on a fresh machine.

```bash
./scripts/dev bootstrap
./scripts/dev test
./scripts/dev demo
./scripts/dev eval
```

Bootstrap starts the local UI and API on loopback. The test and demo commands start isolated Compose projects, so rerunning them does not change the regular local environment. Their evidence goes under `artifacts/`.

Use [the project guide](docs/project-guide.md) to follow one release through the code. The [runbook](docs/runbook.md) covers the scheduler, backfill, PostgreSQL reconciliation and manual recovery.
