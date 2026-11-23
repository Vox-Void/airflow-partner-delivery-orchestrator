# Run the Airflow workflows

## Start and inspect the local stack

Run `./scripts/dev bootstrap` from the repository root. It installs the locked host tools, creates a local `.env` with generated credentials, and starts the pinned Compose services. The API and UI are at `http://127.0.0.1:8016` by default. Use `PARTNER_AIRFLOW_USER` and `PARTNER_AIRFLOW_PASSWORD` from the environment file to sign in. Keep the database and worker ports on the internal Compose network.

The regular stack starts with both DAGs paused. The fixtures are January 2026 releases. Use the isolated demo for a scheduled historical run, or unpause the consumer before manually triggering the producer for a fixture date. The producer can use this run configuration.

```json
{"business_date":"2026-01-13","revision":2}
```

The consumer usually starts from the producer's asset. For a committed release that has no event, manually trigger `partner_delivery_report` using the same date and revision. The consumer looks up the ledger digest itself.

## Reproduce the evidence

- Run `./scripts/dev checks` to inspect formatting, types, fixtures and pure validation rules. PostgreSQL integration tests require `PARTNER_DELIVERY_TEST_DSN`, which should point to a disposable application database. The isolated corrupted scenario also runs those tests inside the pinned stack.
- Run `./scripts/dev probe scheduled` for the actual scheduled interval, asset-triggered report, post-commit retry, correction and manual consumer recovery.
- Run `./scripts/dev probe backfill` for the three-day dry run, the six successful DagRuns after reprocessing, worker intervals and unchanged data digest.
- Run `./scripts/dev probe corrupted` for the permanent checksum failure and unchanged visible state.
- Run `./scripts/dev eval` to produce the three JSON reports and one Markdown summary in `artifacts/`.

Each probe creates its own Compose project and volume. It polls the Airflow REST API with a deadline, verifies both DAGs and the running services, and compares ledger, curated and reported rows with the checked-in expectations. If a check fails, the probe prints the service state and recent logs before shutting down its stack.

## Work on a historical range

The probe configures the end of the historical schedule separately from the normal deployment. It uses the native CLI with an explicit business-date range, a backfill concurrency of two and revision one. The dry run converts the resulting logical datetimes to Johannesburg dates before verifying all three slots.

The stock figures in `fixtures/goldens.json` are calculated by hand. Start with these when a report differs, then inspect the file checksums, staged digests and Airflow task attempts.

## Runtime source

The Compose topology was adapted from the [Airflow 3.4.0 quick start](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/docker-compose/index.html). The project adds a separate application role and database, generated local configuration, read-only fixture mounts and test-specific service settings. The [upstream license](../LICENSE-AIRFLOW-COMPOSE.txt) covers the adapted Compose material.
