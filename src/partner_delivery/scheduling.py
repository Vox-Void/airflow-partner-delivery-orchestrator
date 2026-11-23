"""Choose release keys from data intervals, explicit requests and asset events."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from partner_delivery.contract import (
    DIGEST_PATTERN,
    JOHANNESBURG,
    InputError,
    parse_business_date,
    parse_revision,
)

TIMEZONE = JOHANNESBURG.key
FIRST_BUSINESS_DATE = date(2026, 1, 12)


@dataclass(frozen=True, order=True)
class ReleaseKey:
    business_date: date
    revision: int


@dataclass(frozen=True)
class ReleaseSelection:
    key: ReleaseKey
    test_fail_after_commit: bool


@dataclass(frozen=True)
class ReportSelection:
    key: ReleaseKey
    digest: str | None


def interval_business_date(start: datetime | None, end: datetime | None) -> date:
    """Return the local date of an interval between adjacent local midnights."""
    if start is None or end is None or start.utcoffset() is None or end.utcoffset() is None:
        raise InputError("INVALID_INTERVAL", "Both interval boundaries must include a timezone.")
    local_start, local_end = start.astimezone(JOHANNESBURG), end.astimezone(JOHANNESBURG)
    if (
        local_start.time() != time.min
        or local_end.time() != time.min
        or local_end.date() - local_start.date() != timedelta(days=1)
    ):
        raise InputError(
            "INVALID_INTERVAL", "The interval must span adjacent Johannesburg midnights."
        )
    return local_start.date()


def select_release(
    run_type: str,
    conf: Mapping[str, object],
    start: datetime | None,
    end: datetime | None,
    *,
    enable_test_faults: bool = False,
) -> ReleaseSelection:
    """Select a producer partition from its run type and configuration."""
    if run_type == "manual":
        if set(conf) - {"business_date", "revision", "test_fail_after_commit"}:
            raise InputError("INVALID_RUN_CONF", "Unexpected manual producer configuration field.")
        key = ReleaseKey(
            parse_business_date(conf.get("business_date")), parse_revision(conf.get("revision"))
        )
        fault = conf.get("test_fail_after_commit", False)
        if type(fault) is not bool:
            raise InputError("INVALID_TEST_FAULT", "The test fault flag must be true or false.")
        if fault and not enable_test_faults:
            raise InputError("TEST_FAULT_DISABLED", "Post-commit fault injection is disabled.")
        return ReleaseSelection(key, fault)

    if run_type not in {"scheduled", "backfill"}:
        raise InputError("INVALID_RUN_TYPE", "This run type cannot publish a daily release.")
    if set(conf) - {"revision"}:
        raise InputError(
            "INVALID_RUN_CONF", "Scheduled and backfill runs use their assigned data interval."
        )
    revision = parse_revision(
        conf.get("revision", 1) if run_type == "scheduled" else conf.get("revision")
    )
    if run_type == "scheduled" and revision != 1:
        raise InputError("INVALID_REVISION", "Scheduled releases use revision 1.")
    return ReleaseSelection(ReleaseKey(interval_business_date(start, end), revision), False)


def select_reports(
    run_type: str,
    conf: Mapping[str, object],
    event_metadata: Iterable[object],
) -> tuple[ReportSelection, ...]:
    """Keep each distinct event release, or select one manual ledger key."""
    if run_type == "manual":
        if set(conf) != {"business_date", "revision"}:
            raise InputError(
                "INVALID_RUN_CONF", "Provide only a business date and revision for recovery."
            )
        key = ReleaseKey(
            parse_business_date(conf["business_date"]), parse_revision(conf["revision"])
        )
        return (ReportSelection(key, None),)
    if run_type != "asset_triggered":
        raise InputError(
            "INVALID_RUN_TYPE", "Reporting runs require asset events or manual recovery."
        )
    if conf:
        raise InputError("INVALID_RUN_CONF", "Asset runs use the release keys in their events.")

    digests: dict[ReleaseKey, str] = {}
    for extra in event_metadata:
        if not isinstance(extra, Mapping) or set(extra) != {"business_date", "revision", "digest"}:
            raise InputError(
                "INVALID_ASSET_EVENT", "The asset event must include date, revision and digest."
            )
        key = ReleaseKey(
            parse_business_date(extra["business_date"]), parse_revision(extra["revision"])
        )
        digest = extra["digest"]
        if not isinstance(digest, str) or DIGEST_PATTERN.fullmatch(digest) is None:
            raise InputError("INVALID_ASSET_EVENT", "The event digest must be a SHA-256 value.")
        if digests.setdefault(key, digest) != digest:
            raise InputError(
                "ASSET_DIGEST_CONFLICT", "Asset events disagree for the same release key."
            )
    if not digests:
        raise InputError(
            "MISSING_ASSET_EVENTS", "No release events were supplied to the reporting run."
        )
    return tuple(ReportSelection(key, digest) for key, digest in sorted(digests.items()))
