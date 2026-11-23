CREATE SCHEMA IF NOT EXISTS partner_delivery;
CREATE SCHEMA IF NOT EXISTS partner_delivery_internal;

-- Each release key has one committed manifest and a fixed set of curated rows.
CREATE TABLE IF NOT EXISTS partner_delivery.release_ledger (
    business_date date NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
    curated_count integer NOT NULL CHECK (curated_count >= 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (business_date, revision),
    UNIQUE (business_date, revision, manifest_digest)
);

-- The staging tables hold one validated partner output for each candidate manifest.
CREATE TABLE IF NOT EXISTS partner_delivery_internal.staged_partners (
    business_date date NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
    partner_id text NOT NULL CHECK (partner_id IN ('p1', 'p2')),
    partner_digest text NOT NULL CHECK (partner_digest ~ '^[0-9a-f]{64}$'),
    row_count integer NOT NULL CHECK (row_count >= 0),
    PRIMARY KEY (business_date, revision, manifest_digest, partner_id)
);

CREATE TABLE IF NOT EXISTS partner_delivery_internal.staged_inventory (
    business_date date NOT NULL,
    revision integer NOT NULL,
    manifest_digest text NOT NULL,
    partner_id text NOT NULL,
    warehouse_id text NOT NULL,
    sku text NOT NULL,
    on_hand integer NOT NULL CHECK (on_hand >= 0),
    reserved integer NOT NULL CHECK (reserved >= 0 AND reserved <= on_hand),
    available integer NOT NULL CHECK (available = on_hand - reserved),
    delivered integer NOT NULL CHECK (delivered >= 0),
    PRIMARY KEY (business_date, revision, manifest_digest, partner_id, warehouse_id, sku),
    FOREIGN KEY (business_date, revision, manifest_digest, partner_id)
        REFERENCES partner_delivery_internal.staged_partners
);

CREATE TABLE IF NOT EXISTS partner_delivery.curated_inventory (
    business_date date NOT NULL,
    revision integer NOT NULL,
    partner_id text NOT NULL CHECK (partner_id IN ('p1', 'p2')),
    warehouse_id text NOT NULL,
    sku text NOT NULL,
    on_hand integer NOT NULL CHECK (on_hand >= 0),
    reserved integer NOT NULL CHECK (reserved >= 0 AND reserved <= on_hand),
    available integer NOT NULL CHECK (available = on_hand - reserved),
    delivered integer NOT NULL CHECK (delivered >= 0),
    PRIMARY KEY (business_date, revision, partner_id, warehouse_id, sku),
    FOREIGN KEY (business_date, revision) REFERENCES partner_delivery.release_ledger
);

-- Each reported release has two partner summaries and a receipt.
CREATE TABLE IF NOT EXISTS partner_delivery.partner_reports (
    business_date date NOT NULL,
    revision integer NOT NULL,
    partner_id text NOT NULL CHECK (partner_id IN ('p1', 'p2')),
    on_hand bigint NOT NULL CHECK (on_hand >= 0),
    reserved bigint NOT NULL CHECK (reserved >= 0 AND reserved <= on_hand),
    available bigint NOT NULL CHECK (available = on_hand - reserved),
    delivered bigint NOT NULL CHECK (delivered >= 0),
    PRIMARY KEY (business_date, revision, partner_id),
    FOREIGN KEY (business_date, revision) REFERENCES partner_delivery.release_ledger
);

CREATE TABLE IF NOT EXISTS partner_delivery.report_receipts (
    business_date date NOT NULL,
    revision integer NOT NULL,
    manifest_digest text NOT NULL,
    report_count integer NOT NULL CHECK (report_count = 2),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (business_date, revision),
    FOREIGN KEY (business_date, revision, manifest_digest)
        REFERENCES partner_delivery.release_ledger (business_date, revision, manifest_digest)
);

-- A fault marker records the single injected worker failure for a committed release.
CREATE TABLE IF NOT EXISTS partner_delivery_internal.post_commit_faults (
    business_date date NOT NULL,
    revision integer NOT NULL,
    manifest_digest text NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (business_date, revision),
    FOREIGN KEY (business_date, revision, manifest_digest)
        REFERENCES partner_delivery.release_ledger (business_date, revision, manifest_digest)
);

-- The latest view selects the highest revision with a receipt and both partner summaries.
CREATE OR REPLACE VIEW partner_delivery.latest_reports AS
WITH fully_reported AS (
    SELECT receipt.business_date, receipt.revision
    FROM partner_delivery.report_receipts AS receipt
    JOIN partner_delivery.partner_reports AS report USING (business_date, revision)
    GROUP BY receipt.business_date, receipt.revision, receipt.report_count
    HAVING count(*) = receipt.report_count
),
latest AS (
    SELECT business_date, max(revision) AS revision
    FROM fully_reported
    GROUP BY business_date
)
SELECT report.*
FROM latest
JOIN partner_delivery.partner_reports AS report USING (business_date, revision);
