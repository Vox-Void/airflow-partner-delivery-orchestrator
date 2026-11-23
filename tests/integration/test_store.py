import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from hashlib import sha256
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest

from partner_delivery.contract import PartnerData, ReleaseSpec, load_manifest, validate_partner
from partner_delivery.fixtures import generate
from partner_delivery.store import ReleaseCommit, Store, StoreError, migrate

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def app_dsn() -> str:
    dsn = os.environ.get("PARTNER_DELIVERY_TEST_DSN")
    if not dsn:
        pytest.skip("Set PARTNER_DELIVERY_TEST_DSN to an isolated PostgreSQL application database.")
    migrate(ROOT / "db" / "schema.sql", dsn)
    return dsn


@pytest.fixture
def partition() -> date:
    # Store checks use isolated dates starting in 3000.
    return date.fromordinal(date(3000, 1, 1).toordinal() + uuid4().int % 1_000_000)


@pytest.fixture(scope="module")
def fixture_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("store-fixtures")
    generate(root)
    return root


def release_for(
    fixture_root: Path, partition: date, fixture_day: int, revision: int
) -> tuple[ReleaseSpec, list[PartnerData]]:
    source = load_manifest(fixture_root, date(2026, 1, fixture_day), revision)
    partners = [
        replace(validate_partner(source, partner), business_date=partition)
        for partner in ("p1", "p2")
    ]
    return replace(source, business_date=partition), partners


def test_partial_stage_stays_internal_and_replay_is_stable(
    app_dsn: str, fixture_root: Path, partition: date
) -> None:
    spec, (p1, p2) = release_for(fixture_root, partition, 12, 1)
    with Store(app_dsn) as store:
        assert store.stage_partner(spec, p1)
        assert not store.stage_partner(spec, p1)
        with pytest.raises(StoreError) as incomplete:
            store.publish_release(spec)
        assert incomplete.value.code == "stage_incomplete"
        with pytest.raises(StoreError) as missing:
            store.get_release(partition, 1)
        assert missing.value.code == "release_missing"

        assert store.stage_partner(spec, p2)
        published = store.publish_release(spec)
        assert published.curated_count == 3
        assert not published.replayed
        assert published.metadata() == {
            "business_date": partition.isoformat(),
            "revision": 1,
            "digest": spec.digest,
        }
        with pytest.raises(StoreError) as conflict:
            store.report_release(partition, 1, "0" * 64)
        assert conflict.value.code == "release_conflict"
        assert not store.report_release(partition, 1, spec.digest).replayed
        first = store.snapshot()
        assert store.publish_release(spec).replayed
        assert store.report_release(partition, 1, spec.digest).replayed
        assert store.snapshot() == first
        totals = [
            (row["partner_id"], row["on_hand"], row["reserved"], row["available"], row["delivered"])
            for row in first["reports"]
            if row["business_date"] == partition.isoformat()
        ]
        assert totals == [("p1", 20, 3, 17, 5), ("p2", 7, 2, 5, 1)]


def test_latest_advances_only_after_both_reports_commit(
    app_dsn: str, fixture_root: Path, partition: date
) -> None:
    revisions = [release_for(fixture_root, partition, 13, revision) for revision in (1, 2)]
    with Store(app_dsn) as store:
        for spec, partners in revisions:
            for partner in partners:
                assert store.stage_partner(spec, partner)
            assert not store.publish_release(spec).replayed
            if spec.revision == 1:
                store.report_release(partition, 1, spec.digest)
        before = store.snapshot()
        assert [
            row["revision"]
            for row in before["latest_reports"]
            if row["business_date"] == partition.isoformat()
        ] == [1, 1]

        corrected, _ = revisions[1]
        receipt = store.report_release(partition, 2, corrected.digest)
        assert receipt.report_count == 2
        after = store.snapshot()
        assert [
            (row["revision"], row["partner_id"], row["delivered"])
            for row in after["reports"]
            if row["business_date"] == partition.isoformat()
        ] == [(1, "p1", 5), (1, "p2", 1), (2, "p1", 7), (2, "p2", 1)]
        assert [
            (row["revision"], row["partner_id"], row["delivered"])
            for row in after["latest_reports"]
            if row["business_date"] == partition.isoformat()
        ] == [(2, "p1", 7), (2, "p2", 1)]


def test_concurrent_publish_keeps_one_digest_and_fault_is_claimed_once(
    app_dsn: str, fixture_root: Path, partition: date
) -> None:
    spec, partners = release_for(fixture_root, partition, 14, 1)
    competing = replace(spec, digest=sha256(f"{spec.digest}-other".encode()).hexdigest())
    with Store(app_dsn) as store:
        for candidate in (spec, competing):
            for partner in partners:
                assert store.stage_partner(candidate, partner)
    start = Barrier(2)

    def publish(candidate: ReleaseSpec) -> ReleaseCommit | str:
        with Store(app_dsn) as store:
            start.wait(timeout=5)
            try:
                return store.publish_release(candidate)
            except StoreError as exc:
                return exc.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(publish, [spec, competing]))
    [committed] = [outcome for outcome in outcomes if isinstance(outcome, ReleaseCommit)]
    assert not committed.replayed
    assert "release_conflict" in outcomes

    winner = next(
        candidate for candidate in (spec, competing) if candidate.digest == committed.digest
    )
    with Store(app_dsn) as first_connection:
        assert first_connection.claim_post_commit_fault(partition, 1, winner.digest)
    with Store(app_dsn) as retry_connection:
        assert retry_connection.publish_release(winner).replayed
        assert not retry_connection.claim_post_commit_fault(partition, 1, winner.digest)
        assert retry_connection.report_release(partition, 1, winner.digest).report_count == 2
        observed = retry_connection.snapshot()
    assert (
        len([row for row in observed["releases"] if row["business_date"] == partition.isoformat()])
        == 1
    )
    assert (
        len(
            [
                row
                for row in observed["fault_markers"]
                if row["business_date"] == partition.isoformat()
            ]
        )
        == 1
    )


def test_stale_staged_count_rolls_back_the_inserted_ledger(
    app_dsn: str, fixture_root: Path, partition: date
) -> None:
    spec, partners = release_for(fixture_root, partition, 12, 1)
    with Store(app_dsn) as store:
        for partner in partners:
            assert store.stage_partner(spec, partner)
        store.db.execute(
            """
            DELETE FROM partner_delivery_internal.staged_inventory
            WHERE business_date = %s AND revision = %s AND partner_id = 'p2'
            """,
            (partition, 1),
        )
        with pytest.raises(StoreError) as stale:
            store.publish_release(spec)
        assert stale.value.code == "stage_conflict"
        with pytest.raises(StoreError) as missing:
            store.get_release(partition, 1)
        assert missing.value.code == "release_missing"
        observed = store.snapshot()
        assert all(
            partition.isoformat() != row["business_date"] for row in observed["curated_rows"]
        )
