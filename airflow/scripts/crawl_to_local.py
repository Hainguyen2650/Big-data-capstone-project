"""Run the Airflow source collector locally and save documents to disk."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from disclosure_sources import collect_disclosures  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("hnx", "ssc", "both"), default="both")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--output-dir", type=Path, default=Path("data/landing"))
    args = parser.parse_args()

    sources = ("hnx", "ssc") if args.source == "both" else (args.source,)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {"started_at": datetime.now().astimezone().isoformat(), "sources": {}}
    failures = []

    for source in sources:
        try:
            records = collect_disclosures(source, args.lookback_days, str(args.output_dir))
            manifest["sources"][source] = records
            downloaded = sum(1 for row in records for item in row["local_files"] if item.get("path"))
            failed_downloads = sum(1 for row in records for item in row["local_files"] if item.get("error"))
            print(f"{source}: {len(records)} announcements; {downloaded} files saved; {failed_downloads} download errors")
        except Exception as exc:
            failures.append((source, exc))
            manifest["sources"][source] = {"error": str(exc)}
            print(f"{source}: crawl failed: {exc}", file=sys.stderr)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = args.output_dir / f"crawl_manifest_{stamp}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=lambda value: value.isoformat()),
        encoding="utf-8",
    )
    print(f"Manifest: {manifest_path.resolve()}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
