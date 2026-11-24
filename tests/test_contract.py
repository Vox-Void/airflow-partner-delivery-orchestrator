import json
from dataclasses import asdict
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from partner_delivery.contract import (
    MAX_INTEGER,
    MAX_ROW_BYTES,
    InputError,
    load_manifest,
    validate_partner,
)
from partner_delivery.fixtures import (
    FIRST_DATE,
    FIXTURE_ROOT,
    NUMBER_FIELDS,
    RELEASES,
    fixture_files,
    generate,
    load_goldens,
)


@pytest.fixture
def fixture_root(tmp_path: Path) -> Path:
    generate(tmp_path)
    return tmp_path


def manifest_path(root: Path, day: date = FIRST_DATE, revision: int = 1) -> Path:
    return root / "releases" / day.isoformat() / f"r{revision}" / "manifest.json"


def save_manifest(path: Path, doc: dict[str, Any]) -> None:
    path.write_bytes(json.dumps(doc, sort_keys=True).encode("utf-8"))


def replace_source(root: Path, kind: str, raw: bytes, row_count: int) -> None:
    path = manifest_path(root)
    doc = json.loads(path.read_bytes())
    file = doc["partners"]["p1"][kind]
    (root / file["path"]).write_bytes(raw)
    file["row_count"] = row_count
    file["sha256"] = sha256(raw).hexdigest()
    save_manifest(path, doc)


def source_bytes(root: Path, kind: str) -> bytes:
    doc = json.loads(manifest_path(root).read_bytes())
    return (root / doc["partners"]["p1"][kind]["path"]).read_bytes()


def assert_rejected(root: Path, reason: str) -> None:
    spec = load_manifest(root, FIRST_DATE, 1)
    with pytest.raises(InputError) as result:
        validate_partner(spec, "p1")
    assert result.value.code == reason


def test_reproducible_fixture_bytes(fixture_root: Path) -> None:
    for path, contents in fixture_files().items():
        assert (FIXTURE_ROOT / path).read_bytes() == (fixture_root / path).read_bytes() == contents


@pytest.mark.parametrize("business_date,revision", RELEASES)
def test_curated_rows_match_independently_authored_goldens(
    business_date: date, revision: int
) -> None:
    expected = next(
        item
        for item in load_goldens()["releases"]
        if item["business_date"] == business_date.isoformat() and item["revision"] == revision
    )
    partners = expected["partners"]
    assert isinstance(partners, dict)
    spec = load_manifest(FIXTURE_ROOT, business_date, revision)
    assert (
        spec.digest
        == sha256(manifest_path(FIXTURE_ROOT, business_date, revision).read_bytes()).hexdigest()
    )
    for partner_id in ("p1", "p2"):
        actual = validate_partner(spec, partner_id)
        rows = [asdict(row) for row in actual.rows]
        assert rows == partners[partner_id]["rows"]
        assert {field: sum(row[field] for row in rows) for field in NUMBER_FIELDS} == partners[
            partner_id
        ]["totals"]
        assert actual.duplicate_delivery_lines == partners[partner_id]["duplicate_delivery_lines"]
        assert actual.digest == spec.partners[partner_id].digest
        assert actual.business_date == business_date and actual.revision == revision


def test_revision_changes_only_the_p1_jnb_delivery() -> None:
    day = date(2026, 1, 13)
    first = load_manifest(FIXTURE_ROOT, day, 1)
    second = load_manifest(FIXTURE_ROOT, day, 2)
    assert first.partners["p1"].inventory.sha256 == second.partners["p1"].inventory.sha256
    assert first.partners["p2"].inventory.sha256 == second.partners["p2"].inventory.sha256
    assert first.partners["p2"].deliveries.sha256 == second.partners["p2"].deliveries.sha256
    previous_data = (FIXTURE_ROOT / first.partners["p1"].deliveries.path).read_bytes()
    corrected = (FIXTURE_ROOT / second.partners["p1"].deliveries.path).read_bytes()
    assert previous_data.replace(b",JNB,SKU2,2,", b",JNB,SKU2,4,") == corrected


def test_malformed_checksum_is_partner_specific(tmp_path: Path) -> None:
    generate(tmp_path, corrupt_checksum=True)
    spec = load_manifest(tmp_path, FIRST_DATE, 1)
    assert spec.digest == load_manifest(FIXTURE_ROOT, FIRST_DATE, 1).digest
    assert validate_partner(spec, "p1").duplicate_delivery_lines == 1
    with pytest.raises(InputError) as result:
        validate_partner(spec, "p2")
    assert result.value.code == "CHECKSUM_MISMATCH"


@pytest.mark.parametrize(
    ("override", "code"),
    [
        ({"schema_version": True}, "INVALID_SCHEMA_VERSION"),
        ({"schema_version": 2}, "INVALID_SCHEMA_VERSION"),
        ({"extra": True}, "INVALID_MANIFEST"),
        ({"revision": "1"}, "INVALID_REVISION"),
        ({"revision": 2}, "RELEASE_ID_MISMATCH"),
        ({"business_date": "2026-01-13"}, "RELEASE_ID_MISMATCH"),
        ({"business_date": "2026-02-30"}, "INVALID_BUSINESS_DATE"),
        ({"partners": {"p1": {}, "p3": {}}}, "INVALID_PARTNERS"),
    ],
)
def test_manifest_fields_are_strict(
    fixture_root: Path,
    override: dict[str, Any],
    code: str,
) -> None:
    path = manifest_path(fixture_root)
    save_manifest(path, {**json.loads(path.read_bytes()), **override})
    with pytest.raises(InputError) as result:
        load_manifest(fixture_root, FIRST_DATE, 1)
    assert result.value.code == code


@pytest.mark.parametrize(
    "value", ["../../escape.csv", "/outside.csv", "C:/outside.csv", "releases//source.csv"]
)
def test_manifest_rejects_paths_outside_fixture_root(fixture_root: Path, value: str) -> None:
    path = manifest_path(fixture_root)
    doc = json.loads(path.read_bytes())
    doc["partners"]["p1"]["deliveries"]["path"] = value
    save_manifest(path, doc)
    with pytest.raises(InputError) as result:
        load_manifest(fixture_root, FIRST_DATE, 1)
    assert result.value.code == "INVALID_PATH"


def test_reader_rejects_symbolic_link_outside_root(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixture-root"
    generate(fixture_root)
    data = source_bytes(fixture_root, "deliveries")
    outside = tmp_path / "outside.csv"
    outside.write_bytes(data)
    path = manifest_path(fixture_root)
    doc = json.loads(path.read_bytes())
    link = fixture_root / doc["partners"]["p1"]["deliveries"]["path"]
    link.unlink()
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Creating symbolic links is unavailable on this host.")
    assert_rejected(fixture_root, "INVALID_PATH")


@pytest.mark.parametrize(
    ("before", "after", "code"),
    [
        (b",3,2026-01-11T22:05:00Z", b",4,2026-01-11T22:05:00Z", "CONFLICTING_DELIVERY_LINE"),
        (b"22:05:00Z", b"21:59:59Z", "DELIVERY_OUTSIDE_PARTITION"),
        (b"22:05:00Z", b"22:05:00+00:00", "INVALID_DELIVERY_TIME"),
        (b",2026-01-11T22:05:00Z", b",2026-01-12T22:00:00Z", "DELIVERY_OUTSIDE_PARTITION"),
        (b",JNB,SKU2,2,", b",JNB,SKU1,2,", "DELIVERY_WITHOUT_INVENTORY"),
        (b",JNB,SKU2,2,", b",JNB,SKU9,2,", "UNKNOWN_INVENTORY_KEY"),
        (b",JNB,SKU2,2,", b",JNB,SKU2,-2,", "INVALID_QUANTITY"),
        (b",JNB,SKU2,2,", b",JNB,SKU2,2.0,", "INVALID_QUANTITY"),
        (b",JNB,SKU2,2,", b",p2,JNB,SKU2,2,", "INVALID_ROW"),
        (b"2026-01-12,p1,SHIP-P1-JNB", b"2026-01-12,p2,SHIP-P1-JNB", "ROW_IDENTITY_MISMATCH"),
        (b"2026-01-12,p1,SHIP-P1-JNB", b"2026-01-13,p1,SHIP-P1-JNB", "ROW_IDENTITY_MISMATCH"),
        (b"warehouse_id", b"warehouse", "INVALID_HEADER"),
        (b"SHIP-P1-CPT", b"S" * MAX_ROW_BYTES, "ROW_TOO_LARGE"),
    ],
)
def test_delivery_contract(fixture_root: Path, before: bytes, after: bytes, code: str) -> None:
    raw = source_bytes(fixture_root, "deliveries")
    assert before in raw
    replace_source(fixture_root, "deliveries", raw.replace(before, after, 1), row_count=3)
    assert_rejected(fixture_root, code)


def test_inventory_requires_one_row_per_key_and_reserved_below_stock(fixture_root: Path) -> None:
    raw = source_bytes(fixture_root, "inventory")
    replace_source(fixture_root, "inventory", raw + raw.splitlines(keepends=True)[1], row_count=3)
    assert_rejected(fixture_root, "DUPLICATE_INVENTORY_KEY")
    replace_source(fixture_root, "inventory", raw.replace(b",12,2", b",12,13"), row_count=2)
    assert_rejected(fixture_root, "INVALID_STOCK")


def test_row_count_checks_original_rows_before_deduplicating(fixture_root: Path) -> None:
    raw = source_bytes(fixture_root, "deliveries")
    replace_source(fixture_root, "deliveries", raw, row_count=2)
    assert_rejected(fixture_root, "ROW_COUNT_MISMATCH")
    replace_source(fixture_root, "deliveries", raw, row_count=4)
    assert_rejected(fixture_root, "ROW_COUNT_MISMATCH")


def test_delivery_units_are_aggregated_before_join(fixture_root: Path) -> None:
    raw = source_bytes(fixture_root, "deliveries")
    fourth = (
        raw.splitlines(keepends=True)[1].replace(b",L1,", b",L2,").replace(b",SKU1,3,", b",SKU1,2,")
    )
    replace_source(fixture_root, "deliveries", raw + fourth, row_count=4)
    data = validate_partner(load_manifest(fixture_root, FIRST_DATE, 1), "p1")
    assert len(data.rows) == 2
    assert data.rows[0].delivered == 5 and data.rows[0].available == 10
    assert data.duplicate_delivery_lines == 1


def test_delivered_total_must_fit_inventory_grain_integer(fixture_root: Path) -> None:
    raw = source_bytes(fixture_root, "deliveries")
    overflow = (
        raw.splitlines(keepends=True)[1]
        .replace(b",L1,", b",L2,")
        .replace(b",SKU1,3,", f",SKU1,{MAX_INTEGER},".encode("ascii"))
    )
    replace_source(fixture_root, "deliveries", raw + overflow, row_count=4)
    assert_rejected(fixture_root, "DELIVERY_TOTAL_OVERFLOW")


def test_empty_delivery_file_is_a_valid_zero(fixture_root: Path) -> None:
    raw = source_bytes(fixture_root, "deliveries")
    replace_source(fixture_root, "deliveries", raw.splitlines(keepends=True)[0], row_count=0)
    data = validate_partner(load_manifest(fixture_root, FIRST_DATE, 1), "p1")
    assert len(data.rows) == 2
    assert all(row.delivered == 0 for row in data.rows)


def test_csv_validation_runs_after_manifest_resolution(fixture_root: Path) -> None:
    spec = load_manifest(fixture_root, FIRST_DATE, 1)
    (fixture_root / spec.partners["p1"].deliveries.path).unlink()
    assert load_manifest(fixture_root, FIRST_DATE, 1).digest == spec.digest
    assert_rejected(fixture_root, "MISSING_FILE")


def test_directory_at_source_path_is_rejected_as_a_missing_file(fixture_root: Path) -> None:
    spec = load_manifest(fixture_root, FIRST_DATE, 1)
    path = fixture_root / spec.partners["p1"].deliveries.path
    path.unlink()
    path.mkdir()
    assert_rejected(fixture_root, "MISSING_FILE")
