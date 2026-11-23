from datetime import UTC, date, datetime

import pytest

from partner_delivery.contract import InputError
from partner_delivery.scheduling import ReleaseKey, select_release, select_reports

START = datetime(2026, 1, 11, 22, tzinfo=UTC)
END = datetime(2026, 1, 12, 22, tzinfo=UTC)


def test_scheduled_and_backfill_use_the_local_interval_start() -> None:
    for run_type, conf in (("scheduled", {}), ("backfill", {"revision": 1})):
        result = select_release(run_type, conf, START, END)
        assert result.key == ReleaseKey(date(2026, 1, 12), 1)
        assert not result.test_fail_after_commit


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (START, START),
        (START, datetime(2026, 1, 13, 22, tzinfo=UTC)),
        (datetime(2026, 1, 12, tzinfo=UTC), datetime(2026, 1, 13, tzinfo=UTC)),
        (START.replace(tzinfo=None), END),
        (None, END),
    ],
)
def test_interval_must_span_adjacent_johannesburg_midnights(start, end) -> None:
    with pytest.raises(InputError) as failure:
        select_release("scheduled", {}, start, end)
    assert failure.value.code == "INVALID_INTERVAL"


@pytest.mark.parametrize(
    ("run_type", "conf", "code"),
    [
        ("scheduled", {"business_date": "2026-01-13"}, "INVALID_RUN_CONF"),
        ("scheduled", {"revision": 2}, "INVALID_REVISION"),
        ("backfill", {}, "INVALID_REVISION"),
        ("backfill", {"revision": True}, "INVALID_REVISION"),
        ("manual", {"revision": 2}, "INVALID_BUSINESS_DATE"),
        ("manual", {"business_date": "20260113", "revision": 2}, "INVALID_BUSINESS_DATE"),
        ("manual", {"business_date": "2026-02-30", "revision": 2}, "INVALID_BUSINESS_DATE"),
    ],
)
def test_producer_rejects_ambiguous_run_configuration(run_type, conf, code) -> None:
    with pytest.raises(InputError) as failure:
        select_release(run_type, conf, START, END)
    assert failure.value.code == code


def test_manual_correction_uses_explicit_date_and_gates_test_fault() -> None:
    conf = {"business_date": "2026-01-13", "revision": 2, "test_fail_after_commit": True}
    with pytest.raises(InputError) as failure:
        select_release("manual", conf, None, None)
    assert failure.value.code == "TEST_FAULT_DISABLED"
    result = select_release("manual", conf, None, None, enable_test_faults=True)
    assert result.key == ReleaseKey(date(2026, 1, 13), 2)
    assert result.test_fail_after_commit


def test_asset_reporting_keeps_every_distinct_release() -> None:
    first = {"business_date": "2026-01-12", "revision": 1, "digest": "a" * 64}
    revisions = [{**first, "revision": revision} for revision in range(1, 301)]
    previous = {**first, "business_date": "2026-01-11", "digest": "b" * 64}
    selections = select_reports("asset_triggered", {}, [*reversed(revisions), first, previous])
    assert len(selections) == 301
    assert selections[0].key == ReleaseKey(date(2026, 1, 11), 1)
    assert selections[0].digest == "b" * 64
    assert [row.key.revision for row in selections[1:]] == list(range(1, 301))


def test_asset_reporting_rejects_a_conflicting_digest_for_the_same_release() -> None:
    event = {"business_date": "2026-01-12", "revision": 1, "digest": "a" * 64}
    with pytest.raises(InputError) as failure:
        select_reports("asset_triggered", {}, [event, {**event, "digest": "b" * 64}])
    assert failure.value.code == "ASSET_DIGEST_CONFLICT"


def test_manual_report_selects_one_ledger_key_without_supplied_digest() -> None:
    conf = {"business_date": "2026-01-13", "revision": 2}
    (selection,) = select_reports("manual", conf, [])
    assert selection.key == ReleaseKey(date(2026, 1, 13), 2)
    assert selection.digest is None
    with pytest.raises(InputError) as failure:
        select_reports("manual", {**conf, "digest": "a" * 64}, [])
    assert failure.value.code == "INVALID_RUN_CONF"
