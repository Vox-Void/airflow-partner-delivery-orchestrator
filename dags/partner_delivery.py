"""Publish daily delivery partitions and report from committed release events."""

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import TypedDict

import pendulum
from airflow.sdk import Asset, CronDataIntervalTimetable, Metadata, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from partner_delivery.contract import (
    InputError,
    ReleaseSpec,
    load_manifest,
    parse_business_date,
    parse_revision,
    validate_partner,
)
from partner_delivery.scheduling import (
    FIRST_BUSINESS_DATE,
    TIMEZONE,
    select_release,
    select_reports,
)
from partner_delivery.store import Store, StoreError

LOG = logging.getLogger(__name__)
VALIDATED = Asset(
    uri="x-partner://reporting/validated-deliveries", name="validated_partner_deliveries"
)
TASK_DEFAULTS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "execution_timeout": timedelta(minutes=2),
}


class ResolvedRelease(TypedDict):
    business_date: str
    revision: int
    digest: str
    test_fail_after_commit: bool


def env_flag(name: str) -> bool:
    value = os.environ.get(name, "false").strip().casefold()
    if value not in {"true", "false", "1", "0"}:
        raise InputError("INVALID_ENV_FLAG", f"{name} must be true or false.")
    return value in {"true", "1"}


def midnight(value: date) -> pendulum.DateTime:
    return pendulum.datetime(value.year, value.month, value.day, tz=TIMEZONE)


def last_allowed_start() -> pendulum.DateTime | None:
    configured = os.environ.get("PARTNER_DELIVERY_LAST_START")
    if not configured:
        return None
    value = parse_business_date(configured)
    if value < FIRST_BUSINESS_DATE:
        raise InputError(
            "INVALID_LAST_START", "The last interval start precedes the first interval."
        )
    return midnight(value)


def fixture_root() -> Path:
    return Path(os.environ.get("PARTNER_DELIVERY_FIXTURE_ROOT", "/opt/airflow/fixtures"))


@contextmanager
def fail_invalid_input() -> Iterator[None]:
    """Mark contract and ledger failures as permanent task failures."""
    try:
        yield
    except (InputError, StoreError) as error:
        raise AirflowFailException(f"{error.code} | {error}") from error


def resolved_manifest(release: ResolvedRelease) -> ReleaseSpec:
    spec = load_manifest(
        fixture_root(),
        parse_business_date(release["business_date"]),
        parse_revision(release["revision"]),
    )
    if spec.digest != release["digest"]:
        raise InputError("MANIFEST_CHANGED", "The release manifest changed after resolution.")
    return spec


@dag(
    dag_id="partner_delivery_daily",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=TIMEZONE),
    start_date=midnight(FIRST_BUSINESS_DATE),
    end_date=last_allowed_start(),
    catchup=env_flag("PARTNER_DELIVERY_CATCHUP"),
    max_active_runs=2,
    is_paused_upon_creation=True,
    default_args=TASK_DEFAULTS,
    tags=["partner-delivery"],
)
def producer():
    @task(multiple_outputs=False)
    def resolve_release() -> ResolvedRelease:
        context = get_current_context()
        run = context["dag_run"]
        with fail_invalid_input():
            selection = select_release(
                run.run_type.value,
                run.conf or {},
                context.get("data_interval_start"),
                context.get("data_interval_end"),
                enable_test_faults=env_flag("PARTNER_DELIVERY_ENABLE_TEST_FAULTS"),
            )
            spec = load_manifest(
                fixture_root(), selection.key.business_date, selection.key.revision
            )
        return {
            "business_date": spec.business_date.isoformat(),
            "revision": spec.revision,
            "digest": spec.digest,
            "test_fail_after_commit": selection.test_fail_after_commit,
        }

    @task(pool="partner_read", multiple_outputs=False)
    def validate_release_partner(release: ResolvedRelease, partner_id: str) -> ResolvedRelease:
        with fail_invalid_input():
            spec = resolved_manifest(release)
            data = validate_partner(spec, partner_id)
            with Store() as store:
                inserted = store.stage_partner(spec, data)
        LOG.info(
            "Staged partner %s rows=%s duplicates=%s inserted=%s",
            partner_id,
            len(data.rows),
            data.duplicate_delivery_lines,
            inserted,
        )
        return release

    @task(pool="reporting_write", outlets=[VALIDATED], do_xcom_push=False)
    def publish_release(p1: ResolvedRelease, p2: ResolvedRelease) -> Iterator[Metadata]:
        with fail_invalid_input():
            if p1 != p2:
                raise InputError(
                    "STAGED_RELEASE_MISMATCH", "Partner tasks resolved different releases."
                )
            spec = resolved_manifest(p1)
            with Store() as store:
                commit = store.publish_release(spec)
                inject_fault = p1["test_fail_after_commit"] and store.claim_post_commit_fault(
                    commit.business_date,
                    commit.revision,
                    commit.digest,
                )
        LOG.info(
            "Release committed date=%s revision=%s rows=%s replayed=%s",
            commit.business_date,
            commit.revision,
            commit.curated_count,
            commit.replayed,
        )
        if inject_fault:
            raise RuntimeError("TEST_POST_COMMIT_FAULT")

        # Airflow records this event only after the publish task succeeds.
        yield Metadata(VALIDATED, extra=commit.metadata())

    release = resolve_release()
    partner_1 = validate_release_partner.override(task_id="validate_p1")(release, "p1")
    partner_2 = validate_release_partner.override(task_id="validate_p2")(release, "p2")
    publish_release(partner_1, partner_2)


@dag(
    dag_id="partner_delivery_report",
    schedule=[VALIDATED],
    start_date=midnight(FIRST_BUSINESS_DATE),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=TASK_DEFAULTS,
    tags=["partner-delivery"],
)
def consumer():
    @task(pool="reporting_write", multiple_outputs=False)
    def report_releases() -> dict[str, int]:
        context = get_current_context()
        run = context["dag_run"]
        event_groups = context.get("triggering_asset_events", {})
        metadata = (event.extra for events in event_groups.values() for event in events)
        with fail_invalid_input():
            selections = select_reports(run.run_type.value, run.conf or {}, metadata)
            with Store() as store:
                # Check every event against the ledger before writing a report.
                confirmed = []
                for selection in selections:
                    key = selection.key
                    release = store.get_release(key.business_date, key.revision)
                    if selection.digest is not None and selection.digest != release.digest:
                        raise InputError(
                            "ASSET_LEDGER_MISMATCH",
                            "The asset digest differs from the release ledger.",
                        )
                    confirmed.append(release)

                reports = 0
                for release in confirmed:
                    receipt = store.report_release(
                        release.business_date, release.revision, release.digest
                    )
                    reports += receipt.report_count
                    LOG.info(
                        "Release reported date=%s revision=%s rows=%s replayed=%s",
                        release.business_date,
                        release.revision,
                        receipt.report_count,
                        receipt.replayed,
                    )
        return {"releases": len(selections), "reports": reports}

    report_releases()


partner_delivery_daily = producer()
partner_delivery_report = consumer()
