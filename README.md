# Airflow Partner Delivery Orchestrator

Schedule daily inventory and shipment reports for two fictional retail partners. The workflow checks each delivery, publishes the two partners together, and preserves separate revisions for corrections.

The demo follows a real Airflow schedule, a publish task that fails after its database commit, and a later revision. The report for January 13 changes from five to seven delivered units for partner p1. Both versions remain available for review.

Watch the [captioned walkthrough](docs/demo.mp4) for the scheduler run, recovery and backfill.

## Run it

Use Linux or WSL2 with Docker Compose v2.14 or later and uv 0.13.9. Allocate at least 4 GB to Docker. The stack uses Airflow 3.4.0 and Celery, with PostgreSQL for storage and Redis for the worker queue. It downloads the pinned images on a fresh machine.

```bash
./scripts/dev bootstrap
./scripts/dev test
./scripts/dev demo
./scripts/dev eval
```

Bootstrap starts the local UI and API on loopback. The test and demo commands start isolated Compose projects, so rerunning them does not change the regular local environment. Their evidence goes under `artifacts/`.

## Verified results

- The January 12 schedule published the release and triggered the reporting DAG. The application verified the Johannesburg interval and both partner totals.
- A simulated failure after commit recovered on attempt two. The report had one receipt for the release. A separate recovery run reported a committed release without an asset event.
- The three-day backfill produced nine curated rows and six reports. Reprocessing the same three DagRuns emitted three new events and kept the data digest and receipt count unchanged.
- A corrupted file stopped on the first attempt and produced no committed release, report or asset event.

The [GitHub Actions run](https://github.com/Vox-Void/airflow-partner-delivery-orchestrator/actions/runs/45219673481) has the JSON reports for these scenarios.

Use [the project guide](docs/project-guide.md) to follow one release through the code. The [runbook](docs/runbook.md) covers the scheduler, backfill, PostgreSQL reconciliation and manual recovery.
