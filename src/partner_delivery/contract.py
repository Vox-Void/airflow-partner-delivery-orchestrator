"""Read release manifests and reconcile each partner's two source files."""

import csv
import io
import json
import re
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from zoneinfo import ZoneInfo

SCHEMA_VERSION = 1
PARTNER_IDS = ("p1", "p2")
WAREHOUSES = frozenset({"CPT", "JNB"})
PRODUCTS = frozenset({"SKU1", "SKU2"})
JOHANNESBURG = ZoneInfo("Africa/Johannesburg")
MAX_MANIFEST_BYTES = 16 * 1024
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_ROW_BYTES = 4096
MAX_ROWS = 20_000
MAX_INTEGER = (1 << 31) - 1
DATE_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
UTC_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
ID_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9-]{0,63}")
INTEGER_PATTERN = re.compile(r"0|[1-9][0-9]{0,9}")
PATH_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9./_-]{0,199}")
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
INVENTORY_HEADER = ("business_date", "partner_id", "warehouse_id", "sku", "on_hand", "reserved")
DELIVERY_HEADER = (
    "business_date",
    "partner_id",
    "shipment_id",
    "line_id",
    "warehouse_id",
    "sku",
    "units",
    "delivered_at_utc",
)


class InputError(ValueError):
    """An input failed a contract check."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def parse_business_date(value: object) -> date:
    if type(value) is not str or DATE_PATTERN.fullmatch(value) is None:
        raise InputError("INVALID_BUSINESS_DATE", "Business date must use YYYY-MM-DD.")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise InputError(
            "INVALID_BUSINESS_DATE", "Business date is outside the calendar."
        ) from error


def parse_revision(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_INTEGER:
        raise InputError("INVALID_REVISION", "Revision must be a positive signed 32-bit integer.")
    return value


@dataclass(frozen=True)
class FileSpec:
    path: str
    row_count: int
    sha256: str


@dataclass(frozen=True)
class PartnerSpec:
    partner_id: str
    inventory: FileSpec
    deliveries: FileSpec

    @property
    def digest(self) -> str:
        body = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(body).hexdigest()


@dataclass(frozen=True)
class ReleaseSpec:
    root: Path
    business_date: date
    revision: int
    digest: str
    partners: Mapping[str, PartnerSpec]


@dataclass(frozen=True)
class CuratedRow:
    warehouse_id: str
    sku: str
    on_hand: int
    reserved: int
    available: int
    delivered: int


@dataclass(frozen=True)
class PartnerData:
    business_date: date
    revision: int
    partner_id: str
    digest: str
    rows: tuple[CuratedRow, ...]
    duplicate_delivery_lines: int


@dataclass(frozen=True)
class _Delivery:
    warehouse_id: str
    sku: str
    units: int
    delivered_at_utc: str


def _inside(root: Path, relative: str) -> Path:
    try:
        candidate = (root / relative).resolve()
    except RuntimeError as error:
        raise InputError("INVALID_PATH", "Source path contains a symbolic link loop.") from error
    if not candidate.is_relative_to(root):
        raise InputError("INVALID_PATH", "Source path leaves the configured fixture root.")
    return candidate


def _read(root: Path, relative: str, limit: int) -> bytes:
    path = _inside(root, relative)
    if not path.is_file():
        raise InputError("MISSING_FILE", f"Could not open source file {relative}.")
    try:
        with path.open("rb") as file:
            data = file.read(limit + 1)
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as error:
        raise InputError("MISSING_FILE", f"Could not open source file {relative}.") from error
    if len(data) > limit:
        raise InputError("FILE_TOO_LARGE", f"Source file {relative} exceeds the byte limit.")
    return data


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise InputError("INVALID_MANIFEST", "Manifest repeats a JSON field.")
        result[key] = value
    return result


def _file_spec(value: object) -> FileSpec:
    if not isinstance(value, dict) or set(value) != {"path", "row_count", "sha256"}:
        raise InputError(
            "INVALID_MANIFEST", "Each source must declare a path, row count and digest."
        )
    path, count, digest = value["path"], value["row_count"], value["sha256"]
    if (
        type(path) is not str
        or PATH_PATTERN.fullmatch(path) is None
        or any(part in {"", ".", ".."} for part in path.split("/"))
        or not path.endswith(".csv")
    ):
        raise InputError("INVALID_PATH", "Expected a relative CSV path inside the fixture root.")
    if type(count) is not int or not 0 <= count <= MAX_ROWS:
        raise InputError("INVALID_MANIFEST", "CSV record count is outside the allowed range.")
    if type(digest) is not str or DIGEST_PATTERN.fullmatch(digest) is None:
        raise InputError("INVALID_MANIFEST", "CSV digest must be a lowercase SHA-256 hash.")
    return FileSpec(path, count, digest)


def load_manifest(root: Path, business_date: date, revision: int) -> ReleaseSpec:
    """Read the exact release manifest and validate its four source declarations."""
    if type(business_date) is not date:
        raise InputError("INVALID_BUSINESS_DATE", "Expected a business date.")
    revision = parse_revision(revision)
    root = root.resolve()
    relative = f"releases/{business_date.isoformat()}/r{revision}/manifest.json"
    data = _read(root, relative, MAX_MANIFEST_BYTES)
    try:
        raw = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except InputError:
        raise
    except (ValueError, RecursionError) as error:
        raise InputError("INVALID_MANIFEST", "Manifest must be valid UTF-8 JSON.") from error
    expected_fields = {"schema_version", "business_date", "revision", "partners"}
    if not isinstance(raw, dict) or set(raw) != expected_fields:
        raise InputError("INVALID_MANIFEST", "Manifest fields do not match the release schema.")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
        raise InputError("INVALID_SCHEMA_VERSION", "Expected release schema version 1.")
    if (
        parse_business_date(raw["business_date"]) != business_date
        or parse_revision(raw["revision"]) != revision
    ):
        raise InputError(
            "RELEASE_ID_MISMATCH", "Manifest date or revision differs from its release path."
        )
    partners = raw["partners"]
    if not isinstance(partners, dict) or set(partners) != set(PARTNER_IDS):
        raise InputError("INVALID_PARTNERS", "Manifest must contain exactly p1 and p2.")
    specs: dict[str, PartnerSpec] = {}
    for partner_id in PARTNER_IDS:
        sources = partners[partner_id]
        if not isinstance(sources, dict) or set(sources) != {"inventory", "deliveries"}:
            raise InputError(
                "INVALID_MANIFEST",
                "Each partner must declare one inventory file and one delivery file.",
            )
        specs[partner_id] = PartnerSpec(
            partner_id=partner_id,
            inventory=_file_spec(sources["inventory"]),
            deliveries=_file_spec(sources["deliveries"]),
        )
    paths = {file.path for spec in specs.values() for file in (spec.inventory, spec.deliveries)}
    if len(paths) != 4:
        raise InputError("INVALID_PATH", "Manifest must name four separate CSV files.")
    return ReleaseSpec(
        root, business_date, revision, sha256(data).hexdigest(), MappingProxyType(specs)
    )


def _lines(data: bytes, path: str) -> Iterator[str]:
    for raw in io.BytesIO(data):
        if len(raw) > MAX_ROW_BYTES:
            raise InputError("ROW_TOO_LARGE", f"A CSV line exceeds the byte limit in {path}.")
        try:
            yield raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise InputError("INVALID_CSV", f"Expected UTF-8 text in {path}.") from error


def _records(
    root: Path, file: FileSpec, header: tuple[str, ...]
) -> Iterator[tuple[int, list[str]]]:
    data = _read(root, file.path, MAX_FILE_BYTES)
    if sha256(data).hexdigest() != file.sha256:
        raise InputError("CHECKSUM_MISMATCH", f"Checksum does not match manifest for {file.path}.")
    reader = csv.reader(_lines(data, file.path), strict=True)
    count = 0
    try:
        if next(reader, None) != list(header):
            raise InputError(
                "INVALID_HEADER", f"CSV header does not match the contract in {file.path}."
            )
        for count, row in enumerate(reader, 1):
            if count > file.row_count:
                raise InputError("ROW_COUNT_MISMATCH", f"Too many data rows in {file.path}.")
            if len(row) != len(header):
                raise InputError(
                    "INVALID_ROW", f"Wrong number of values at data row {count} in {file.path}."
                )
            if sum(len(field.encode("utf-8")) for field in row) > MAX_ROW_BYTES:
                raise InputError(
                    "ROW_TOO_LARGE", f"CSV row {count} exceeds the byte limit in {file.path}."
                )
            yield count, row
    except csv.Error as error:
        raise InputError("INVALID_CSV", f"Malformed CSV in {file.path}.") from error
    if count != file.row_count:
        raise InputError("ROW_COUNT_MISMATCH", f"Too few data rows in {file.path}.")


def _identity(spec: ReleaseSpec, partner_id: str, business_date: str, identity: str) -> None:
    if business_date != spec.business_date.isoformat() or identity != partner_id:
        raise InputError("ROW_IDENTITY_MISMATCH", "Row date or partner differs from the manifest.")


def _inventory_key(warehouse_id: str, sku: str) -> tuple[str, str]:
    if warehouse_id not in WAREHOUSES or sku not in PRODUCTS:
        raise InputError("UNKNOWN_INVENTORY_KEY", "Row uses an unknown warehouse or SKU.")
    return warehouse_id, sku


def _units(value: str) -> int:
    if INTEGER_PATTERN.fullmatch(value) is None or int(value) > MAX_INTEGER:
        raise InputError("INVALID_QUANTITY", "Units must be a nonnegative signed 32-bit integer.")
    return int(value)


def _stock(spec: ReleaseSpec, partner: PartnerSpec) -> dict[tuple[str, str], tuple[int, int]]:
    inventory: dict[tuple[str, str], tuple[int, int]] = {}
    for number, row in _records(spec.root, partner.inventory, INVENTORY_HEADER):
        business_date, partner_id, warehouse_id, sku, stock_units, reserved_units = row
        try:
            _identity(spec, partner.partner_id, business_date, partner_id)
            key = _inventory_key(warehouse_id, sku)
            on_hand, reserved = _units(stock_units), _units(reserved_units)
            if reserved > on_hand:
                raise InputError("INVALID_STOCK", "Reserved units exceed on-hand units.")
            if key in inventory:
                raise InputError(
                    "DUPLICATE_INVENTORY_KEY", "Inventory repeats a warehouse and SKU."
                )
            inventory[key] = (on_hand, reserved)
        except InputError as error:
            raise InputError(error.code, f"Inventory data row {number}. {error}") from error
    return inventory


def _validate_delivery_time(value: str, business_date: date) -> None:
    if UTC_PATTERN.fullmatch(value) is None:
        raise InputError("INVALID_DELIVERY_TIME", "Delivery time must use YYYY-MM-DDTHH:MM:SSZ.")
    try:
        local_date = datetime.fromisoformat(value).astimezone(JOHANNESBURG).date()
    except (ValueError, OverflowError) as error:
        raise InputError(
            "INVALID_DELIVERY_TIME", "Delivery time is outside the UTC calendar."
        ) from error
    if local_date != business_date:
        raise InputError(
            "DELIVERY_OUTSIDE_PARTITION",
            "Delivery time falls outside the Johannesburg business date.",
        )


def _delivered(
    spec: ReleaseSpec,
    partner: PartnerSpec,
    inventory: Mapping[tuple[str, str], tuple[int, int]],
) -> tuple[dict[tuple[str, str], int], int]:
    deliveries: dict[tuple[str, str], _Delivery] = {}
    totals: dict[tuple[str, str], int] = defaultdict(int)
    duplicates = 0
    for number, row in _records(spec.root, partner.deliveries, DELIVERY_HEADER):
        business_date, partner_id, shipment, line, warehouse, sku, units, timestamp = row
        try:
            _identity(spec, partner.partner_id, business_date, partner_id)
            if ID_PATTERN.fullmatch(shipment) is None or ID_PATTERN.fullmatch(line) is None:
                raise InputError(
                    "INVALID_DELIVERY_ID",
                    "Shipment and line IDs must use uppercase letters, digits and hyphens.",
                )
            key = _inventory_key(warehouse, sku)
            if key not in inventory:
                raise InputError(
                    "DELIVERY_WITHOUT_INVENTORY", "Delivery references a missing inventory row."
                )
            _validate_delivery_time(timestamp, spec.business_date)
            quantity = _units(units)
            delivery = _Delivery(warehouse, sku, quantity, timestamp)
            previous = deliveries.get((shipment, line))
            if previous is None:
                deliveries[(shipment, line)] = delivery
                totals[key] += quantity
                if totals[key] > MAX_INTEGER:
                    raise InputError(
                        "DELIVERY_TOTAL_OVERFLOW",
                        "Delivered units exceed the inventory-grain limit.",
                    )
            elif previous != delivery:
                raise InputError(
                    "CONFLICTING_DELIVERY_LINE", "A delivery line ID has different values."
                )
            else:
                duplicates += 1
        except InputError as error:
            raise InputError(error.code, f"Delivery data row {number}. {error}") from error
    return totals, duplicates


def validate_partner(spec: ReleaseSpec, partner_id: str) -> PartnerData:
    """Join validated delivery totals to one inventory row per warehouse and SKU."""
    partner = spec.partners.get(partner_id)
    if partner is None:
        raise InputError("INVALID_PARTNER", "Partner does not appear in the manifest.")
    inventory = _stock(spec, partner)
    delivered, duplicates = _delivered(spec, partner, inventory)
    rows = tuple(
        CuratedRow(
            warehouse,
            sku,
            on_hand,
            reserved,
            on_hand - reserved,
            delivered.get((warehouse, sku), 0),
        )
        for (warehouse, sku), (on_hand, reserved) in sorted(inventory.items())
    )
    return PartnerData(
        spec.business_date, spec.revision, partner_id, partner.digest, rows, duplicates
    )
