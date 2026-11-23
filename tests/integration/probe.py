"""Run the pinned Airflow stack and reconcile its API with PostgreSQL."""

import argparse
import base64
import json
import os
import re
import secrets
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from time import monotonic, sleep
from typing import Any, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

from partner_delivery.fixtures import generate

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
LOCAL = ZoneInfo("Africa/Johannesburg")
DAILY = "partner_delivery_daily"
REPORT = "partner_delivery_report"
TERMINAL = {"success", "failed"}
DAYS = ("2026-01-12", "2026-01-13", "2026-01-14")
EXPECTED = json.loads((ROOT / "fixtures" / "goldens.json").read_text("utf-8"))
T = TypeVar("T")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def until(description: str, inspect: Callable[[], T], timeout: int = 180) -> T:
    deadline = monotonic() + timeout
    last_failure = "Condition returned no result"
    while monotonic() < deadline:
        try:
            answer = inspect()
            if answer:
                return answer
        except (HTTPError, URLError) as error:
            if isinstance(error, HTTPError) and error.code not in {404, 502, 503, 504}:
                raise
            last_failure = str(error)
        sleep(2)
    raise TimeoutError(f"{description} did not become ready. {last_failure}")


def create_config(path: Path) -> dict[str, str]:
    if not path.exists():
        values = {
            "PARTNER_METADATA_PASSWORD": secrets.token_hex(24),
            "PARTNER_APP_PASSWORD": secrets.token_hex(24),
            "PARTNER_AIRFLOW_USER": "operator",
            "PARTNER_AIRFLOW_PASSWORD": secrets.token_hex(24),
            "PARTNER_AIRFLOW_API_SECRET": secrets.token_hex(32),
            "PARTNER_AIRFLOW_JWT_SECRET": secrets.token_hex(32),
            "PARTNER_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii"),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.writelines(f"{name}={value}\n" for name, value in values.items())
    return dict(
        line.split("=", 1)
        for line in path.read_text("utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


class Stack:
    def __init__(self, name: str, config: Path, options: dict[str, str] | None = None):
        self.config = config
        self.values = create_config(config)
        self.name = name
        self.environment = {**os.environ, **self.values, **(options or {})}
        self.url = ""
        self.token = ""

    def compose(self, *args: str, timeout: int = 180, check: bool = True) -> str:
        command = ("docker", "compose", "--project-name", self.name, "--env-file", str(self.config))
        result = subprocess.run(
            (*command, *args),
            cwd=ROOT,
            env=self.environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        if check and result.returncode:
            raise RuntimeError(self.redact(result.stdout + result.stderr))
        return result.stdout if check else result.stdout + result.stderr

    def redact(self, output: str) -> str:
        if self.token:
            output = output.replace(self.token, "[redacted]")
        for key, value in self.values.items():
            if key != "PARTNER_AIRFLOW_USER" and value:
                output = output.replace(value, "[redacted]")
        return output

    def start(self, *, build: bool) -> dict[str, Any]:
        options = ("--build",) if build else ()
        self.compose("up", "-d", "--wait", *options, timeout=600)
        version = self.compose("exec", "-T", "airflow-scheduler", "airflow", "version").strip()
        require(version.splitlines()[-1] == "3.4.0", f"Unexpected Airflow version {version}")
        port = self.compose("port", "airflow-apiserver", "8080").strip().rsplit(":", 1)[1]
        self.url = f"http://127.0.0.1:{int(port)}"
        health = until("Airflow service health", self.health)
        self.token = self.api(
            "/auth/token",
            method="POST",
            body={
                "username": self.values["PARTNER_AIRFLOW_USER"],
                "password": self.values["PARTNER_AIRFLOW_PASSWORD"],
            },
        )["access_token"]
        for dag in (DAILY, REPORT):
            until(f"parse {dag}", lambda: self.api(f"/api/v2/dags/{dag}"))
        return health

    def health(self) -> dict[str, Any] | None:
        data = self.api("/api/v2/monitor/health")
        services = ("metadatabase", "scheduler", "dag_processor", "triggerer")
        worker = self.compose("ps", "--format", "json", "airflow-worker").strip()
        containers = [json.loads(line) for line in worker.splitlines()]
        if containers and isinstance(containers[0], list):
            containers = containers[0]
        if all(data.get(item, {}).get("status") == "healthy" for item in services) and any(
            row.get("Health") == "healthy" for row in containers
        ):
            return {item: data[item]["status"] for item in services} | {"worker": "healthy"}
        return None

    def api(self, path: str, *, method: str = "GET", body: dict | None = None) -> Any:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(
            f"{self.url}{path}",
            method=method,
            headers=headers,
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urlopen(request, timeout=15) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code not in {404, 502, 503, 504}:
                detail = self.redact(error.read().decode("utf-8", "replace")[:1200])
                raise RuntimeError(
                    f"Airflow request {method} {path} returned {error.code}: {detail}"
                ) from error
            raise

    def unpause(self, dag: str) -> None:
        self.api(
            f"/api/v2/dags/{dag}?update_mask=is_paused", method="PATCH", body={"is_paused": False}
        )

    def trigger(self, dag: str, conf: dict[str, Any]) -> dict[str, Any]:
        return self.api(
            f"/api/v2/dags/{dag}/dagRuns",
            method="POST",
            body={"dag_run_id": f"probe__{uuid4().hex}", "logical_date": None, "conf": conf},
        )

    def runs(self, dag: str) -> list[dict[str, Any]]:
        return list(self.api(f"/api/v2/dags/{dag}/dagRuns?limit=100")["dag_runs"])

    def wait_run(self, dag: str, run_id: str, status: str = "success") -> dict[str, Any]:
        path = f"/api/v2/dags/{dag}/dagRuns/{quote(run_id, safe='')}"

        def find() -> dict[str, Any] | None:
            run = self.api(path)
            if run["state"] in TERMINAL:
                require(run["state"] == status, f"{dag} {run_id} ended with {run['state']}")
                return dict(run)
            return None

        return until(f"{dag} {run_id} to finish", find)

    def tasks(self, dag: str, run_id: str, suffix: str = "") -> list[dict[str, Any]]:
        path = f"/api/v2/dags/{dag}/dagRuns/{quote(run_id, safe='')}/taskInstances{suffix}"
        return list(self.api(path)["task_instances"])

    def task_log(self, dag: str, run_id: str, task_id: str, attempt: int) -> str:
        path = f"/api/v2/dags/{dag}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/logs/{attempt}"
        return json.dumps(self.api(f"{path}?full_content=true"))

    def app(self, code: str) -> Any:
        output = self.compose("exec", "-T", "airflow-scheduler", "python", "-c", dedent(code))
        return json.loads(output)

    def snapshot(self) -> dict[str, Any]:
        return dict(
            self.app("""
            import json
            from partner_delivery.store import Store

            with Store() as store:
                print(json.dumps(store.snapshot()))
        """)
        )

    def wait_receipts(self, count: int) -> dict[str, Any]:
        def inspect() -> dict[str, Any] | None:
            snapshot = self.snapshot()
            if len(snapshot["report_receipts"]) >= count:
                require(
                    len(snapshot["report_receipts"]) == count, "Unexpected extra report receipt"
                )
                return snapshot
            return None

        return until(f"{count} reporting receipts", inspect)

    def print_failures(self) -> None:
        if not self.token:
            return
        for dag in (DAILY, REPORT):
            for run in self.runs(dag):
                run_id = run["dag_run_id"]
                for task in self.tasks(dag, run_id):
                    if task["state"] != "failed":
                        continue
                    task_id = task["task_id"]
                    print(f"Failed task {dag}/{run_id}/{task_id}", file=sys.stderr)
                    detail = self.task_log(dag, run_id, task_id, task["try_number"])
                    print(self.redact(detail[-6000:]), file=sys.stderr)


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def local_date(value: str) -> str:
    return timestamp(value).astimezone(LOCAL).date().isoformat()


def run_evidence(row: dict[str, Any]) -> dict[str, Any]:
    return {
        field: row.get(field)
        for field in (
            "dag_run_id",
            "run_type",
            "state",
            "logical_date",
            "data_interval_start",
            "data_interval_end",
        )
    }


def require_release(snapshot: dict[str, Any], day: str, revision: int) -> None:
    def matches(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [row for row in rows if row["business_date"] == day and row["revision"] == revision]

    golden = next(
        item
        for item in EXPECTED["releases"]
        if item["business_date"] == day and item["revision"] == revision
    )
    ledger = matches(snapshot["releases"])
    receipts = matches(snapshot["report_receipts"])
    require(
        len(ledger) == len(receipts) == 1, f"{day} r{revision} needs one ledger row and receipt"
    )
    require(
        ledger[0]["manifest_digest"] == receipts[0]["manifest_digest"], "Manifest digests differ"
    )
    curated = matches(snapshot["curated_rows"])
    reports = matches(snapshot["reports"])
    require(len(curated) == 3 and len(reports) == 2, f"{day} r{revision} row counts differ")
    for partner, expected in golden["partners"].items():
        for summary in (row for row in reports if row["partner_id"] == partner):
            actual = {key: summary[key] for key in expected["totals"]}
            require(
                actual == expected["totals"], f"Unexpected report for {day} r{revision} {partner}"
            )
        require(
            len([row for row in reports if row["partner_id"] == partner]) == 1,
            "Partner report missing",
        )
        row_fields = ("warehouse_id", "sku", "on_hand", "reserved", "available", "delivered")
        source = [
            {field: row[field] for field in row_fields}
            for row in curated
            if row["partner_id"] == partner
        ]
        require(
            source == expected["rows"], f"Unexpected curated rows for {day} r{revision} {partner}"
        )


def manual_recovery(stack: Stack) -> tuple[dict[str, Any], dict[str, Any]]:
    # Create one committed release before any Airflow task has published its asset event.
    committed = stack.app("""
        import json
        from datetime import date
        from pathlib import Path
        from partner_delivery.contract import load_manifest, validate_partner
        from partner_delivery.store import Store

        spec = load_manifest(Path("/opt/airflow/fixtures"), date(2026, 1, 14), 1)
        with Store() as store:
            for partner in spec.partners:
                store.stage_partner(spec, validate_partner(spec, partner))
            store.publish_release(spec)
            print(json.dumps(store.snapshot()))
    """)
    require(
        len(committed["releases"]) == 4 and len(committed["report_receipts"]) == 3,
        "A committed release without an asset must remain unreported",
    )
    started = stack.trigger(REPORT, {"business_date": DAYS[2], "revision": 1})
    run = stack.wait_run(REPORT, started["dag_run_id"])
    recovered = stack.wait_receipts(4)
    require_release(recovered, DAYS[2], 1)
    return run, recovered


def scheduled(stack: Stack) -> dict[str, Any]:
    dag_checks = stack.compose(
        "exec",
        "-T",
        "airflow-scheduler",
        "python",
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        "/opt/airflow/tests/test_dags.py",
    )
    stack.unpause(REPORT)
    stack.unpause(DAILY)
    initial = until("the scheduled producer run", lambda: stack.runs(DAILY))
    require(
        len(initial) == 1 and initial[0]["run_type"] == "scheduled", "Expected one scheduled run"
    )
    run = stack.wait_run(DAILY, initial[0]["dag_run_id"])
    require(
        timestamp(run["data_interval_start"]).isoformat() == "2026-01-11T22:00:00+00:00",
        "Unexpected scheduled interval start",
    )
    require(
        timestamp(run["data_interval_end"]).isoformat() == "2026-01-12T22:00:00+00:00",
        "Unexpected scheduled interval end",
    )
    require_release(stack.wait_receipts(1), DAYS[0], 1)
    consumer = until("the asset-triggered consumer", lambda: stack.runs(REPORT))
    require(
        len(consumer) == 1 and consumer[0]["run_type"] == "asset_triggered",
        "Expected one asset-triggered consumer",
    )
    asset_run = stack.wait_run(REPORT, consumer[0]["dag_run_id"])
    event_path = f"/api/v2/dags/{REPORT}/dagRuns/{quote(asset_run['dag_run_id'], safe='')}/upstreamAssetEvents"
    events = stack.api(event_path)["asset_events"]
    require(
        any(event.get("extra", {}).get("business_date") == DAYS[0] for event in events),
        "Consumer journal missing the expected asset event",
    )

    failed_once = stack.trigger(
        DAILY,
        {
            "business_date": DAYS[1],
            "revision": 1,
            "test_fail_after_commit": True,
        },
    )
    retried = stack.wait_run(DAILY, failed_once["dag_run_id"])
    tries = stack.tasks(DAILY, retried["dag_run_id"], "/publish_release/tries")
    require(len({item["try_number"] for item in tries}) >= 2, "Publish did not retry")
    first_try = min(item["try_number"] for item in tries)
    require(
        "TEST_POST_COMMIT_FAULT"
        in stack.task_log(DAILY, retried["dag_run_id"], "publish_release", first_try),
        "First publish attempt is missing the expected post-commit failure",
    )
    repaired = stack.wait_receipts(2)
    require(len(repaired["fault_markers"]) == 1, "Expected one post-commit failure marker")
    require_release(repaired, DAYS[1], 1)

    correction = stack.trigger(DAILY, {"business_date": DAYS[1], "revision": 2})
    corrected = stack.wait_run(DAILY, correction["dag_run_id"])
    revised = stack.wait_receipts(3)
    require_release(revised, DAYS[1], 1)
    require_release(revised, DAYS[1], 2)
    latest = next(
        row
        for row in revised["latest_reports"]
        if row["business_date"] == DAYS[1] and row["partner_id"] == "p1"
    )
    require((latest["revision"], latest["delivered"]) == (2, 7), "Correction did not supersede r1")
    recovery, final = manual_recovery(stack)
    require(
        sum(item["run_type"] == "scheduled" for item in stack.runs(DAILY)) == 1,
        "The scheduler created an interval outside the historical bound",
    )
    return {
        "scheduled": run_evidence(run),
        "asset": run_evidence(asset_run),
        "asset_events": events,
        "retry": run_evidence(retried),
        "publish_tries": [
            {key: item[key] for key in ("try_number", "state", "start_date", "end_date")}
            for item in tries
        ],
        "correction": run_evidence(corrected),
        "consumer_recovery": run_evidence(recovery),
        "database": final,
        "parser_checks": dag_checks.strip(),
    }


def expect_consumed(
    stack: Stack,
    producer_ids: set[str],
    previous_events: set[int] | None = None,
) -> list[dict[str, Any]]:
    def consumed() -> list[dict[str, Any]] | None:
        event_batches = []
        for run in stack.runs(REPORT):
            require(run["state"] != "failed", "An asset consumer failed")
            if run["state"] != "success":
                continue
            run_id = quote(run["dag_run_id"], safe="")
            path = f"/api/v2/dags/{REPORT}/dagRuns/{run_id}/upstreamAssetEvents"
            event_batches.extend(
                event
                for event in stack.api(path)["asset_events"]
                if not previous_events or event["id"] not in previous_events
            )
        actual = {event["source_run_id"] for event in event_batches}
        if actual == producer_ids:
            require(len(event_batches) == len(producer_ids), "Unexpected repeated asset event")
            return event_batches
        return None

    return until(f"asset consumption for {len(producer_ids)} releases", consumed)


def wait_backfill(stack: Stack, behavior: str) -> tuple[int, list[dict[str, Any]]]:
    def inspect() -> tuple[int, list[dict[str, Any]]] | None:
        runs = stack.runs(DAILY)
        require(
            all(row["run_type"] == "backfill" for row in runs),
            "Unexpected scheduled run during backfill",
        )
        require(all(row["state"] != "failed" for row in runs), "An Airflow backfill run failed")
        records = stack.api(f"/api/v2/backfills?dag_id={DAILY}")["backfills"]
        batch = next((row for row in records if row["reprocess_behavior"] == behavior), None)
        if batch is None or batch["completed_at"] is None:
            return None
        batch_id = int(batch["id"])
        slots = stack.api(f"/api/v2/backfills/{batch_id}/dag_runs")["backfill_dag_runs"]
        require(batch["max_active_runs"] == 2, "Unexpected backfill concurrency")
        require(
            len(slots) == 3
            and all(
                row["exception_reason"] is None and row["dag_run_state"] == "success"
                for row in slots
            ),
            "Backfill did not run all three slots successfully",
        )
        require(
            {slot["dag_run_id"] for slot in slots} == {run["dag_run_id"] for run in runs},
            "Backfill records do not match the producer runs",
        )
        return batch_id, runs

    return until(f"completed {behavior} backfill", inspect, timeout=240)


def backfill(stack: Stack) -> dict[str, Any]:
    stack.unpause(REPORT)
    stack.unpause(DAILY)
    args = (
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "backfill",
        "create",
        "--dag-id",
        DAILY,
        "--from-date",
        f"{DAYS[0]}T00:00:00+02:00",
        "--to-date",
        f"{DAYS[2]}T00:00:00+02:00",
        "--max-active-runs",
        "2",
        "--dag-run-conf",
        '{"revision":1}',
    )
    grid = stack.compose(*args, "--reprocess-behavior", "none", "--dry-run")
    dates = [
        local_date(match.group(1))
        for line in grid.splitlines()
        if (
            match := re.match(
                r"\|?\s*(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2}))", line
            )
        )
    ]
    require(dates == list(DAYS), f"Unexpected Airflow dry-run slots {dates}")

    stack.compose(*args, "--reprocess-behavior", "none")
    first_id, first = wait_backfill(stack, "none")
    producer_ids = {row["dag_run_id"] for row in first}
    first_events = expect_consumed(stack, producer_ids)
    before = stack.wait_receipts(3)
    require(
        len(before["curated_rows"]) == 9 and len(before["reports"]) == 6,
        "Backfill row counts differ",
    )
    for day in DAYS:
        require_release(before, day, 1)
    require(
        sorted(local_date(run["data_interval_start"]) for run in first) == list(DAYS),
        "Backfill intervals differ from the dry run",
    )
    read_tasks = sorted(
        (
            item
            for run in first
            for item in stack.tasks(DAILY, run["dag_run_id"])
            if item["task_id"] in {"validate_p1", "validate_p2"}
        ),
        key=lambda task: timestamp(task["start_date"]),
    )
    require(
        len(read_tasks) == 6 and all(item["pool"] == "partner_read" for item in read_tasks),
        "Expected six partner reads on the one-slot pool",
    )
    require(
        all(
            timestamp(previous["end_date"]) <= timestamp(current["start_date"])
            for previous, current in zip(read_tasks, read_tasks[1:], strict=False)
        ),
        "Partner validations overlapped in the one-slot pool",
    )
    stack.compose(*args, "--reprocess-behavior", "completed")
    second_id, second = wait_backfill(stack, "completed")
    require(
        first_id != second_id and {row["dag_run_id"] for row in second} == producer_ids,
        "Airflow reprocessing did not reuse the original DagRuns",
    )
    events = expect_consumed(stack, producer_ids, {event["id"] for event in first_events})
    after = stack.wait_receipts(3)
    require(
        after["canonical_digest"] == before["canonical_digest"],
        "Backfill replay changed curated data",
    )
    require(
        len(after["report_receipts"]) == len(before["report_receipts"]),
        "Backfill replay duplicated a receipt",
    )
    return {
        "dry_run_dates": dates,
        "initial_backfill_id": first_id,
        "reprocess_backfill_id": second_id,
        "runs": [run_evidence(row) for row in second],
        "read_intervals": [
            {key: item[key] for key in ("task_id", "pool", "start_date", "end_date")}
            for item in read_tasks
        ],
        "digest_before_reprocess": before["canonical_digest"],
        "digest_after_reprocess": after["canonical_digest"],
        "database": after,
        "asset_event_count": len(first_events) + len(events),
    }


def corrupted(stack: Stack) -> dict[str, Any]:
    stack.unpause(REPORT)
    stack.unpause(DAILY)
    manual = stack.trigger(DAILY, {"business_date": DAYS[0], "revision": 1})
    run = stack.wait_run(DAILY, manual["dag_run_id"], "failed")
    checks = stack.tasks(DAILY, run["dag_run_id"], "/validate_p2/tries")
    require(len(checks) == 1 and checks[0]["state"] == "failed", "Malformed input was retried")
    require(
        "CHECKSUM_MISMATCH"
        in stack.task_log(DAILY, run["dag_run_id"], "validate_p2", checks[0]["try_number"]),
        "The rejected input is missing the checksum reason",
    )
    snapshot = stack.snapshot()
    require(
        not snapshot["releases"]
        and not snapshot["curated_rows"]
        and not snapshot["report_receipts"],
        "Malformed release produced a visible write",
    )
    require(not stack.runs(REPORT), "Malformed release emitted an asset")
    db_tests = stack.compose(
        "exec",
        "-T",
        "airflow-scheduler",
        "bash",
        "-c",
        'PARTNER_DELIVERY_TEST_DSN="$PARTNER_DELIVERY_DSN" python -m pytest -q -p no:cacheprovider /opt/airflow/tests/integration/test_store.py',
    )
    return {
        "failed_run": run_evidence(run),
        "rejected_attempt": checks[0]["try_number"],
        "database": snapshot,
        "storage_checks": db_tests.strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("bootstrap", "scheduled", "backfill", "corrupted"))
    args = parser.parse_args()
    if args.scenario == "bootstrap":
        stack = Stack("partner-delivery", ROOT / ".env")
        health = stack.start(build=True)
        print(f"DAGs are available at {stack.url}. Service status: {health}")
        return 0

    scratch = ARTIFACTS / f"probe-{args.scenario}-{uuid4().hex[:10]}"
    options = {
        "AIRFLOW_HTTP_PORT": "0",
        "PARTNER_DELIVERY_CATCHUP": "true" if args.scenario == "scheduled" else "false",
        "PARTNER_DELIVERY_LAST_START": DAYS[0] if args.scenario == "scheduled" else DAYS[2],
        "PARTNER_DELIVERY_ENABLE_TEST_FAULTS": "true" if args.scenario == "scheduled" else "false",
    }
    if args.scenario == "corrupted":
        fixtures = scratch / "fixtures"
        generate(fixtures, corrupt_checksum=True)
        options["PARTNER_FIXTURE_BIND"] = str(fixtures)
    stack = Stack(scratch.name, scratch / "probe.env", options)
    scenarios = {"scheduled": scheduled, "backfill": backfill, "corrupted": corrupted}
    try:
        health = stack.start(build=True)
        details = scenarios[args.scenario](stack)
        report = {
            "version": 1,
            "app_commit": os.environ.get("GITHUB_SHA"),
            "engine": "Airflow 3.4.0",
            "scenario": args.scenario,
            "health": health,
            **details,
        }
        (ARTIFACTS / f"{args.scenario}.json").write_text(
            json.dumps(report, indent=2) + "\n", "utf-8"
        )
        print(f"Airflow {args.scenario} evidence saved to artifacts/{args.scenario}.json")
    except Exception:
        try:
            stack.print_failures()
        except (HTTPError, OSError, RuntimeError, ValueError) as log_error:
            print(f"Could not collect task logs: {stack.redact(str(log_error))}", file=sys.stderr)
        print(stack.redact(stack.compose("ps", "-a", check=False)), file=sys.stderr)
        print(stack.redact(stack.compose("logs", "--tail", "55", check=False)), file=sys.stderr)
        raise
    finally:
        stack.compose("down", "--volumes", "--remove-orphans", timeout=180, check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
