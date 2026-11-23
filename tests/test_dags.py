import runpy
from datetime import UTC, datetime
from pathlib import Path

import pytest


def test_dags_parse_with_an_inclusive_last_start_and_declared_pools(monkeypatch) -> None:
    sdk = pytest.importorskip("airflow.sdk")
    from airflow.serialization.serialized_objects import DagSerialization

    monkeypatch.setenv("PARTNER_DELIVERY_CATCHUP", "true")
    monkeypatch.setenv("PARTNER_DELIVERY_LAST_START", "2026-01-12")
    dag_file = Path(__file__).resolve().parents[1] / "dags" / "partner_delivery.py"
    dags = runpy.run_path(str(dag_file))
    producer = dags["partner_delivery_daily"]
    consumer = dags["partner_delivery_report"]

    assert producer.max_active_runs == 2
    assert isinstance(producer.timetable, sdk.CronDataIntervalTimetable)
    scheduled = DagSerialization.from_dict(DagSerialization.to_dict(producer))
    first = scheduled.next_dagrun_info(last_automated_run_info=None)
    assert first is not None and first.data_interval is not None
    assert first.data_interval.start == datetime(2026, 1, 11, 22, tzinfo=UTC)
    assert first.data_interval.end == datetime(2026, 1, 12, 22, tzinfo=UTC)
    assert first.run_after == first.data_interval.end
    assert scheduled.next_dagrun_info(last_automated_run_info=first) is None

    tasks = producer.task_dict
    assert set(tasks) == {"resolve_release", "validate_p1", "validate_p2", "publish_release"}
    for partner in ("validate_p1", "validate_p2"):
        assert tasks[partner].pool == "partner_read"
        assert tasks[partner].upstream_task_ids == {"resolve_release"}
    publish = tasks["publish_release"]
    assert publish.upstream_task_ids == {"validate_p1", "validate_p2"}
    assert publish.pool == consumer.task_dict["report_releases"].pool == "reporting_write"
    assert len(publish.outlets) == 1
    assert publish.outlets[0].uri == "x-partner://reporting/validated-deliveries"


def test_serialized_timetable_keeps_three_historical_backfill_slots(monkeypatch) -> None:
    pytest.importorskip("airflow.sdk")
    import pendulum
    from airflow.serialization.serialized_objects import DagSerialization
    from airflow.timetables import interval

    monkeypatch.setenv("PARTNER_DELIVERY_CATCHUP", "false")
    monkeypatch.setenv("PARTNER_DELIVERY_LAST_START", "2026-01-14")
    monkeypatch.setattr(interval, "utcnow", lambda: pendulum.datetime(2026, 1, 20, tz="UTC"))
    dag_file = Path(__file__).resolve().parents[1] / "dags" / "partner_delivery.py"
    producer = runpy.run_path(str(dag_file))["partner_delivery_daily"]
    scheduled = DagSerialization.from_dict(DagSerialization.to_dict(producer))
    assert scheduled.next_dagrun_info(last_automated_run_info=None) is None

    start = pendulum.datetime(2026, 1, 12, tz="Africa/Johannesburg")
    end = pendulum.datetime(2026, 1, 14, tz="Africa/Johannesburg")
    slots = list(scheduled.iter_dagrun_infos_between(start, end))
    assert len(slots) == 3
    assert [slot.data_interval.start for slot in slots] == [start.add(days=n) for n in range(3)]
    assert [slot.data_interval.end for slot in slots] == [start.add(days=n) for n in range(1, 4)]
