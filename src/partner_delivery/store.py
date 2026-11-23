"""Persist validated partner releases and their report receipts in PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import TracebackType
from typing import TypedDict

import psycopg
from psycopg.rows import DictRow, dict_row

from partner_delivery.contract import PartnerData, ReleaseSpec

PARTNERS = ("p1", "p2")
DSN_ENV = "PARTNER_DELIVERY_DSN"


class StoreError(ValueError):
    """A permanent mismatch in a release, stage or report."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ReleaseCommit:
    business_date: date
    revision: int
    digest: str
    curated_count: int
    replayed: bool

    def metadata(self) -> dict[str, str | int]:
        return {
            "business_date": self.business_date.isoformat(),
            "revision": self.revision,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class ReportCommit:
    business_date: date
    revision: int
    digest: str
    report_count: int
    replayed: bool


SnapshotRow = dict[str, str | int]


class Snapshot(TypedDict):
    releases: list[SnapshotRow]
    curated_rows: list[SnapshotRow]
    reports: list[SnapshotRow]
    report_receipts: list[SnapshotRow]
    latest_reports: list[SnapshotRow]
    fault_markers: list[SnapshotRow]
    canonical_digest: str


class Store:
    """Open a connection for work done by one Airflow task or probe."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn
        self.connection: psycopg.Connection[DictRow] | None = None

    def __enter__(self) -> Store:
        dsn = self.dsn if self.dsn is not None else os.environ.get(DSN_ENV)
        if not dsn:
            raise StoreError(
                "missing_dsn", f"Set {DSN_ENV} to the application database connection."
            )
        self.connection = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @property
    def db(self) -> psycopg.Connection[DictRow]:
        if self.connection is None:
            raise RuntimeError("Open the store in a with block before accessing the database.")
        return self.connection

    def _lock_release(self, business_date: date, revision: int) -> None:
        if type(revision) is not int or not 0 < revision <= 2_147_483_647:
            raise StoreError(
                "invalid_revision", "Revision must be a positive signed 32-bit integer."
            )
        # The database holds this lock for the transaction on the date and revision pair.
        self.db.execute(
            "SELECT pg_advisory_xact_lock(%s::integer, %s::integer)",
            (business_date.toordinal(), revision),
        )

    def _find_commit(self, business_date: date, revision: int) -> ReleaseCommit | None:
        row = self.db.execute(
            """
            SELECT business_date, revision, manifest_digest, curated_count
            FROM partner_delivery.release_ledger
            WHERE business_date = %s AND revision = %s
            """,
            (business_date, revision),
        ).fetchone()
        if row is None:
            return None
        return ReleaseCommit(
            row["business_date"],
            row["revision"],
            row["manifest_digest"],
            row["curated_count"],
            replayed=True,
        )

    def get_release(self, business_date: date, revision: int) -> ReleaseCommit:
        commit = self._find_commit(business_date, revision)
        if commit is None:
            raise StoreError(
                "release_missing", f"No committed release for {business_date} revision {revision}."
            )
        return commit

    @staticmethod
    def _verify_digest(commit: ReleaseCommit, digest: str) -> None:
        if commit.digest != digest:
            raise StoreError(
                "release_conflict",
                f"Manifest digest differs for {commit.business_date} revision {commit.revision}.",
            )

    def stage_partner(self, spec: ReleaseSpec, data: PartnerData) -> bool:
        """Save a validated partner and return whether staging added any rows."""
        if data.partner_id not in PARTNERS or data.partner_id not in spec.partners:
            raise StoreError("stage_contract", "Partner is absent from the release manifest.")
        partner = spec.partners[data.partner_id]
        if (data.business_date, data.revision, data.digest) != (
            spec.business_date,
            spec.revision,
            partner.digest,
        ):
            raise StoreError(
                "stage_contract", "Partner identity or digest differs from the manifest."
            )
        if len(data.rows) != partner.inventory.row_count:
            raise StoreError(
                "stage_contract", "Curated row count differs from the inventory manifest."
            )

        key = (spec.business_date, spec.revision, spec.digest, data.partner_id)
        rows = [
            (row.warehouse_id, row.sku, row.on_hand, row.reserved, row.available, row.delivered)
            for row in data.rows
        ]
        with self.db.transaction():
            self._lock_release(spec.business_date, spec.revision)
            commit = self._find_commit(spec.business_date, spec.revision)
            if commit is not None:
                self._verify_digest(commit, spec.digest)

            staged = self.db.execute(
                """
                INSERT INTO partner_delivery_internal.staged_partners
                    (business_date, revision, manifest_digest, partner_id, partner_digest, row_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (business_date, revision, manifest_digest, partner_id) DO NOTHING
                RETURNING partner_id
                """,
                (*key, data.digest, len(rows)),
            ).fetchone()
            if staged is None:
                saved = self.db.execute(
                    """
                    SELECT partner_digest, row_count
                    FROM partner_delivery_internal.staged_partners
                    WHERE (business_date, revision, manifest_digest, partner_id) = (%s, %s, %s, %s)
                    """,
                    key,
                ).fetchone()
                if saved != {"partner_digest": data.digest, "row_count": len(rows)}:
                    raise StoreError(
                        "stage_conflict", "Staged partner has different validated input."
                    )
                staged_rows = self.db.execute(
                    """
                    SELECT warehouse_id, sku, on_hand, reserved, available, delivered
                    FROM partner_delivery_internal.staged_inventory
                    WHERE (business_date, revision, manifest_digest, partner_id) = (%s, %s, %s, %s)
                    """,
                    key,
                ).fetchall()
                if sorted(tuple(row.values()) for row in staged_rows) != sorted(rows):
                    raise StoreError(
                        "stage_conflict", "Staged partner has different normalized rows."
                    )
                return False

            with self.db.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO partner_delivery_internal.staged_inventory
                        (business_date, revision, manifest_digest, partner_id, warehouse_id, sku,
                         on_hand, reserved, available, delivered)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [(*key, *row) for row in rows],
                )
        return True

    def publish_release(self, spec: ReleaseSpec) -> ReleaseCommit:
        """Check both staged partners and commit one curated release."""
        key = (spec.business_date, spec.revision)
        if set(spec.partners) != set(PARTNERS):
            raise StoreError(
                "stage_contract", "Publication requires the p1 and p2 manifest entries."
            )
        expected = {
            partner_id: (
                spec.partners[partner_id].digest,
                spec.partners[partner_id].inventory.row_count,
            )
            for partner_id in PARTNERS
        }
        with self.db.transaction():
            self._lock_release(*key)
            commit = self._find_commit(*key)
            if commit is not None:
                self._verify_digest(commit, spec.digest)
                return commit

            staged = self.db.execute(
                """
                SELECT partner_id, partner_digest, row_count
                FROM partner_delivery_internal.staged_partners
                WHERE (business_date, revision, manifest_digest) = (%s, %s, %s)
                """,
                (*key, spec.digest),
            ).fetchall()
            if {row["partner_id"] for row in staged} != set(PARTNERS):
                raise StoreError(
                    "stage_incomplete", "Both partners must be staged for this manifest."
                )
            if {
                row["partner_id"]: (row["partner_digest"], row["row_count"]) for row in staged
            } != expected:
                raise StoreError(
                    "stage_conflict", "Staged partner digest or count differs from the manifest."
                )

            curated_count = sum(row["row_count"] for row in staged)
            self.db.execute(
                """
                INSERT INTO partner_delivery.release_ledger
                    (business_date, revision, manifest_digest, curated_count)
                VALUES (%s, %s, %s, %s)
                """,
                (*key, spec.digest, curated_count),
            )
            inserted = self.db.execute(
                """
                INSERT INTO partner_delivery.curated_inventory
                    (business_date, revision, partner_id, warehouse_id, sku,
                     on_hand, reserved, available, delivered)
                SELECT business_date, revision, partner_id, warehouse_id, sku,
                       on_hand, reserved, available, delivered
                FROM partner_delivery_internal.staged_inventory
                WHERE (business_date, revision, manifest_digest) = (%s, %s, %s)
                """,
                (*key, spec.digest),
            ).rowcount
            if inserted != curated_count:
                raise StoreError("stage_conflict", "Staged row count changed before publication.")
        return ReleaseCommit(*key, spec.digest, curated_count, replayed=False)

    def report_release(self, business_date: date, revision: int, digest: str) -> ReportCommit:
        """Roll up a ledger release and commit both summaries with their receipt."""
        key = (business_date, revision)
        with self.db.transaction():
            self._lock_release(*key)
            self._verify_digest(self.get_release(*key), digest)
            receipt = self.db.execute(
                """
                SELECT report_count FROM partner_delivery.report_receipts
                WHERE (business_date, revision) = (%s, %s)
                """,
                key,
            ).fetchone()
            if receipt is not None:
                return ReportCommit(*key, digest, receipt["report_count"], replayed=True)

            # The fixed partner set yields a zero summary for a partner with no rows.
            report_count = self.db.execute(
                """
                INSERT INTO partner_delivery.partner_reports
                    (business_date, revision, partner_id, on_hand, reserved, available, delivered)
                SELECT %s, %s, partners.partner_id,
                       coalesce(sum(curated.on_hand), 0),
                       coalesce(sum(curated.reserved), 0),
                       coalesce(sum(curated.available), 0),
                       coalesce(sum(curated.delivered), 0)
                FROM (VALUES ('p1'), ('p2')) AS partners(partner_id)
                LEFT JOIN partner_delivery.curated_inventory AS curated
                    ON curated.partner_id = partners.partner_id
                    AND (curated.business_date, curated.revision) = (%s, %s)
                GROUP BY partners.partner_id
                """,
                (*key, *key),
            ).rowcount
            self.db.execute(
                """
                INSERT INTO partner_delivery.report_receipts
                    (business_date, revision, manifest_digest, report_count)
                VALUES (%s, %s, %s, %s)
                """,
                (*key, digest, report_count),
            )
        return ReportCommit(*key, digest, report_count, replayed=False)

    def claim_post_commit_fault(self, business_date: date, revision: int, digest: str) -> bool:
        """Return true only for the first fault injection on a committed release."""
        key = (business_date, revision)
        with self.db.transaction():
            self._lock_release(*key)
            self._verify_digest(self.get_release(*key), digest)
            marker = self.db.execute(
                """
                INSERT INTO partner_delivery_internal.post_commit_faults
                    (business_date, revision, manifest_digest)
                VALUES (%s, %s, %s)
                ON CONFLICT (business_date, revision) DO NOTHING
                RETURNING revision
                """,
                (*key, digest),
            ).fetchone()
        return marker is not None

    def _snapshot_rows(self, query: str) -> list[SnapshotRow]:
        rows = self.db.execute(query).fetchall()
        for row in rows:
            row["business_date"] = row["business_date"].isoformat()
        return rows

    def snapshot(self) -> Snapshot:
        """Read a consistent copy of the committed tables and stable data digest."""
        with self.db.transaction():
            self.db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            releases = self._snapshot_rows(
                """
                SELECT business_date, revision, manifest_digest, curated_count
                FROM partner_delivery.release_ledger
                ORDER BY business_date, revision
                """
            )
            curated_rows = self._snapshot_rows(
                """
                SELECT * FROM partner_delivery.curated_inventory
                ORDER BY business_date, revision, partner_id, warehouse_id, sku
                """
            )
            reports = self._snapshot_rows(
                """
                SELECT * FROM partner_delivery.partner_reports
                ORDER BY business_date, revision, partner_id
                """
            )
            report_receipts = self._snapshot_rows(
                """
                SELECT business_date, revision, manifest_digest, report_count
                FROM partner_delivery.report_receipts
                ORDER BY business_date, revision
                """
            )
            latest_reports = self._snapshot_rows(
                """
                SELECT * FROM partner_delivery.latest_reports
                ORDER BY business_date, revision, partner_id
                """
            )
            fault_markers = self._snapshot_rows(
                """
                SELECT business_date, revision, manifest_digest
                FROM partner_delivery_internal.post_commit_faults
                ORDER BY business_date, revision
                """
            )
        # The digest hashes the normalized ledger, curated rows, summaries and receipts.
        canonical = json.dumps(
            [releases, curated_rows, reports, report_receipts],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return {
            "releases": releases,
            "curated_rows": curated_rows,
            "reports": reports,
            "report_receipts": report_receipts,
            "latest_reports": latest_reports,
            "fault_markers": fault_markers,
            "canonical_digest": hashlib.sha256(canonical).hexdigest(),
        }


def migrate(schema_path: Path, dsn: str | None = None) -> None:
    with Store(dsn) as store, store.db.transaction():
        store.db.execute("SELECT pg_advisory_xact_lock(0, 0)")
        store.db.execute(schema_path.read_text(encoding="utf-8"), prepare=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the partner delivery schema.")
    parser.add_argument(
        "--schema", type=Path, default=Path(__file__).resolve().parents[2] / "db" / "schema.sql"
    )
    args = parser.parse_args()
    migrate(args.schema)
    print("Partner delivery schema is ready.")


if __name__ == "__main__":
    main()
