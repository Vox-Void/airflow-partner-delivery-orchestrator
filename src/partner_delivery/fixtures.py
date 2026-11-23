"""Generate the dated partner deliveries and independently authored expected results."""

import argparse
import csv
import io
import json
from collections.abc import Sequence
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path

from .contract import DELIVERY_HEADER, INVENTORY_HEADER, SCHEMA_VERSION

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
FIRST_DATE = date(2026, 1, 12)
RELEASES = (
    (date(2026, 1, 12), 1),
    (date(2026, 1, 13), 1),
    (date(2026, 1, 14), 1),
    (date(2026, 1, 13), 2),
)
NUMBER_FIELDS = ("on_hand", "reserved", "available", "delivered")
NumericRow = tuple[int, int, int, int]
CsvRow = tuple[str | int, ...]


def _csv_bytes(header: Sequence[str], rows: Sequence[CsvRow]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def _numbers(values: NumericRow) -> dict[str, int]:
    return dict(zip(NUMBER_FIELDS, values, strict=True))


def _row(warehouse_id: str, sku: str, values: NumericRow) -> dict[str, str | int]:
    return {"warehouse_id": warehouse_id, "sku": sku, **_numbers(values)}


def _golden_release(
    day: str,
    revision: int,
    p1_cpt: NumericRow,
    p1_jnb: NumericRow,
    p2_cpt: NumericRow,
    p1_total: NumericRow,
    p2_total: NumericRow,
    *,
    duplicates: int = 0,
) -> dict[str, object]:
    return {
        "business_date": day,
        "revision": revision,
        "partners": {
            "p1": {
                "rows": [_row("CPT", "SKU1", p1_cpt), _row("JNB", "SKU2", p1_jnb)],
                "totals": _numbers(p1_total),
                "duplicate_delivery_lines": duplicates,
            },
            "p2": {
                "rows": [_row("CPT", "SKU1", p2_cpt)],
                "totals": _numbers(p2_total),
                "duplicate_delivery_lines": 0,
            },
        },
    }


def _goldens() -> dict[str, object]:
    # Each tuple gives on hand, reserved, available and delivered units in that order.
    return {
        "releases": [
            _golden_release(
                "2026-01-12",
                1,
                (12, 2, 10, 3),
                (8, 1, 7, 2),
                (7, 2, 5, 1),
                (20, 3, 17, 5),
                (7, 2, 5, 1),
                duplicates=1,
            ),
            _golden_release(
                "2026-01-13",
                1,
                (13, 2, 11, 3),
                (8, 1, 7, 2),
                (8, 2, 6, 1),
                (21, 3, 18, 5),
                (8, 2, 6, 1),
            ),
            _golden_release(
                "2026-01-14",
                1,
                (14, 2, 12, 3),
                (8, 1, 7, 2),
                (9, 2, 7, 1),
                (22, 3, 19, 5),
                (9, 2, 7, 1),
            ),
            _golden_release(
                "2026-01-13",
                2,
                (13, 2, 11, 3),
                (8, 1, 7, 4),
                (8, 2, 6, 1),
                (21, 3, 18, 7),
                (8, 2, 6, 1),
            ),
        ],
    }


def _source_rows(
    business_date: date, revision: int
) -> dict[str, tuple[list[CsvRow], list[CsvRow]]]:
    # The first CPT arrival falls five minutes after Johannesburg midnight.
    day = business_date.isoformat()
    previous_day = (business_date - timedelta(days=1)).isoformat()
    offset = (business_date - FIRST_DATE).days
    cpt_delivery = (day, "p1", "SHIP-P1-CPT", "L1", "CPT", "SKU1", 3, f"{previous_day}T22:05:00Z")
    jnb_delivery = (
        day,
        "p1",
        "SHIP-P1-JNB",
        "L1",
        "JNB",
        "SKU2",
        4 if revision == 2 else 2,
        f"{day}T09:20:00Z",
    )
    p1_deliveries: list[CsvRow] = [cpt_delivery, jnb_delivery]
    if business_date == FIRST_DATE:
        p1_deliveries.append(cpt_delivery)
    return {
        "p1": (
            [(day, "p1", "CPT", "SKU1", 12 + offset, 2), (day, "p1", "JNB", "SKU2", 8, 1)],
            p1_deliveries,
        ),
        "p2": (
            [(day, "p2", "CPT", "SKU1", 7 + offset, 2)],
            [(day, "p2", "SHIP-P2-CPT", "L1", "CPT", "SKU1", 1, f"{day}T07:15:00Z")],
        ),
    }


def fixture_files(*, corrupt_checksum: bool = False) -> dict[Path, bytes]:
    result = {Path("goldens.json"): _json_bytes(_goldens())}
    for day, revision in RELEASES:
        folder = Path("releases") / day.isoformat() / f"r{revision}"
        sources: dict[str, dict[str, object]] = {}
        for partner, (inventory, deliveries) in _source_rows(day, revision).items():
            sources[partner] = {}
            for kind, header, rows in (
                ("inventory", INVENTORY_HEADER, inventory),
                ("deliveries", DELIVERY_HEADER, deliveries),
            ):
                path = folder / f"{partner}-{kind}.csv"
                data = _csv_bytes(header, rows)
                sources[partner][kind] = {
                    "path": path.as_posix(),
                    "row_count": len(rows),
                    "sha256": sha256(data).hexdigest(),
                }
                if (
                    corrupt_checksum
                    and day == FIRST_DATE
                    and revision == 1
                    and partner == "p2"
                    and kind == "deliveries"
                ):
                    data = data.replace(b",1,2026-01-12T07:15:00Z", b",2,2026-01-12T07:15:00Z")
                result[path] = data
        result[folder / "manifest.json"] = _json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "business_date": day.isoformat(),
                "revision": revision,
                "partners": sources,
            }
        )
    return result


def generate(root: Path = FIXTURE_ROOT, *, corrupt_checksum: bool = False) -> None:
    for relative, contents in fixture_files(corrupt_checksum=corrupt_checksum).items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)


def load_goldens(root: Path = FIXTURE_ROOT) -> dict[str, list[dict[str, object]]]:
    return json.loads((root / "goldens.json").read_bytes())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=FIXTURE_ROOT, help="Directory for generated files"
    )
    parser.add_argument(
        "--corrupt-checksum",
        action="store_true",
        help="Alter one CSV without changing its manifest",
    )
    args = parser.parse_args()
    generate(args.root, corrupt_checksum=args.corrupt_checksum)
    print(f"Generated partner fixtures in {args.root}")


if __name__ == "__main__":
    main()
