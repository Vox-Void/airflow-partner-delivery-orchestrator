"""Write a concise run summary from completed Airflow evidence."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    root = ROOT / "artifacts"
    scheduled, backfill, corrupted = (
        json.loads((root / f"{name}.json").read_text("utf-8"))
        for name in ("scheduled", "backfill", "corrupted")
    )
    steps = [
        "# Airflow verification",
        "",
        f"- The scheduler created `{scheduled['scheduled']['dag_run_id']}`.",
        f"- The asset triggered `{scheduled['asset']['dag_run_id']}`.",
        f"- Publishing recovered in {len(scheduled['publish_tries'])} attempts.",
        f"- Backfill reconciled {len(backfill['database']['curated_rows'])} curated rows and "
        f"{len(backfill['database']['reports'])} reports.",
        f"- Reprocessing kept the digest `{backfill['digest_after_reprocess']}`.",
        f"- The corrupted release finished `{corrupted['failed_run']['state']}` on "
        f"attempt {corrupted['rejected_attempt']} without a published release.",
    ]
    text = "\n".join(steps) + "\n"
    (root / "summary.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
