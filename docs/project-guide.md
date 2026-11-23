# Project guide

## The reporting problem

Two partners send daily inventory and shipment files. If one file is missing or wrong, publishing a partial day can give a misleading report. A later reissue needs to correct the result while preserving the numbers originally reported. The same data should stay the same when an operator retries a task or backfills past dates.

## Follow one delivery

1. The producer DAG uses the start of its Johannesburg data interval as the business date. An operator triggering a correction supplies the date and revision explicitly.
2. Each partner sends an inventory CSV and a shipment-lines CSV. The validation task checks the manifest digest and row counts, confirms that all rows belong to the requested day and partner, then removes exact duplicates. It aggregates delivered units by warehouse and product before joining them to inventory.
3. The publisher checks both staged partner results inside one PostgreSQL transaction. It writes three curated inventory rows for this fixture and records the release. Repeating the same date, revision and digest returns the existing commit.
4. The producer emits an Airflow asset event carrying the commit identity. The consumer verifies the digest against the release ledger and writes both reports and a receipt in one transaction.
5. The latest reporting view selects the highest revision with a finished report. After the January 13 correction, p1 has seven delivered units in revision two. Revision one still reports five.

## How it was built

The CSV contract and hand-calculated expected rows came first. One January 12 shipment occurred at 22:05 UTC on January 11, which falls on January 12 in Johannesburg. The fixture also repeats one shipment line to check that retries at the transport layer do not inflate deliveries.

Next came the PostgreSQL ledger and report receipts. The producer lock is scoped to the date and revision. Both staged digests must agree with the manifest before publishing. The report transaction checks against the same committed digest. A correction needs a new revision, so changing bytes under an existing revision cannot rewrite history.

The Airflow tasks call these same application functions. The two validation tasks share a one-slot read pool. Publication and reporting share a separate one-slot write pool. Small identifiers and counts cross task boundaries. The source files stay in the read-only fixture mount, and staged and curated rows stay in PostgreSQL.

## Recovery paths

- **Failure after the commit.** The test forces the worker to fail between the database commit and its asset event. The retry sees the existing ledger row and emits the event once it completes successfully.
- **Invalid delivery.** A bad checksum stops validation without another attempt. Staged data for the other partner can remain, but there is no visible release or report receipt.
- **Missing event.** Manually trigger the consumer with one date and revision. It retrieves the digest from the ledger and records reports only for that exact commit.
- **Backfill.** Airflow dry-runs January 12 to 14 and then schedules those historical intervals. Reprocessing the same three days preserves the curated data, report counts and digest.

## Assumptions and next steps

- The first release knows the partner, warehouse and product identifiers. A connected system would need a separately versioned reference table.
- The setup uses a single local PostgreSQL and Redis stack. A hosted installation would need its own identity, backups, logs and resource limits.
- The service runs with one writer slot to make shared database pressure predictable. Measure file and query costs before increasing those limits for larger partner feeds.
- A central retention job could clean up internal staging rows after committed reports have been reconciled.
