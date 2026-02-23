#!/usr/bin/env python3
"""
Validate exported JSON files for structure and completeness.
Run after compute_snapshots.py to ensure exports are well-formed.
"""

import sys
import os
import json
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline.config import WEBSITE_DATA_DIR, SNAPSHOTS_DIR


def validate_journal_file(path: Path) -> list:
    errors = []
    with open(path) as f:
        data = json.load(f)

    required_top = ["journal", "slug", "issn", "last_updated", "latest", "timeseries"]
    for key in required_top:
        if key not in data:
            errors.append(f"{path.name}: missing top-level key '{key}'")

    latest = data.get("latest", {})
    for key in ["rolling_if", "paper_count", "citation_count"]:
        if key not in latest:
            errors.append(f"{path.name}: latest missing '{key}'")

    timeseries = data.get("timeseries", [])
    if not timeseries:
        errors.append(f"{path.name}: empty timeseries")
    else:
        for i, entry in enumerate(timeseries[:3]):  # spot-check first 3
            for key in ["month", "rolling_if", "papers", "citations"]:
                if key not in entry:
                    errors.append(f"{path.name}: timeseries[{i}] missing '{key}'")

    return errors


def validate_index(path: Path) -> list:
    errors = []
    with open(path) as f:
        data = json.load(f)

    if "journals" not in data:
        errors.append("index.json: missing 'journals' key")
    elif not data["journals"]:
        errors.append("index.json: journals list is empty")
    else:
        for j in data["journals"]:
            for key in ["slug", "name", "issn", "latest_if"]:
                if key not in j:
                    errors.append(f"index.json: journal entry missing '{key}'")

    return errors


def main():
    all_errors = []

    # Validate index
    index_path = Path(WEBSITE_DATA_DIR) / "index.json"
    if index_path.exists():
        all_errors.extend(validate_index(index_path))
    else:
        all_errors.append("Missing: website/data/index.json")

    # Validate journal files
    journals_dir = Path(SNAPSHOTS_DIR)
    journal_files = list(journals_dir.glob("*.json"))
    if not journal_files:
        all_errors.append(f"No journal JSON files found in {SNAPSHOTS_DIR}")

    for jf in journal_files:
        all_errors.extend(validate_journal_file(jf))

    # Report
    if all_errors:
        print(f"Validation FAILED ({len(all_errors)} error(s)):")
        for e in all_errors:
            print(f"  ✗ {e}")
        sys.exit(1)
    else:
        print(f"Validation PASSED — {len(journal_files)} journal files, index.json ✓")


if __name__ == "__main__":
    main()
