#!/usr/bin/env python3
"""
Backfill historical rolling IF snapshots and merge into existing JSON.

Computes rolling IF for a date range (e.g. 2005-01 to 2011-12) and prepends
the results to existing journal JSON files. Also re-exports authors + papers
JSONs to include papers from the backfilled years.

Usage:
    python scripts/backfill_snapshots.py --start-year 2005 --end-year 2011 --workers 4
"""

import sys
import os
import json
import logging
import argparse
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH, SNAPSHOTS_DIR
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.impact_calculator import ImpactCalculator
from src.pipeline.json_exporter import JSONExporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")


def process_chunk(journal_chunk, db_path, start_year, end_year, worker_id):
    """Process a list of journals in a worker process."""
    from src.pipeline.db_manager import DatabaseManager
    from src.pipeline.impact_calculator import ImpactCalculator
    from src.pipeline.json_exporter import JSONExporter

    log = logging.getLogger(f"backfill.w{worker_id}")
    db = DatabaseManager(db_path)
    calc = ImpactCalculator(db)
    exporter = JSONExporter()

    index_entries = []
    now = datetime.now()

    for i, journal in enumerate(journal_chunk):
        jid = journal["id"]
        name = journal["name"]
        issn = journal["issn"]
        slug = journal["slug"]

        log.info(f"[{i+1}/{len(journal_chunk)}] {name}")

        # --- Phase A: Compute historical snapshots and merge into JSON ---
        window_data = calc.compute_all_window_timeseries(
            journal_id=jid,
            start_year=start_year, start_month=1,
            end_year=end_year, end_month=12,
        )

        # Read existing JSON
        json_path = os.path.join(SNAPSHOTS_DIR, f"{slug}.json")
        existing = None
        if os.path.exists(json_path):
            with open(json_path) as f:
                existing = json.load(f)

        if existing is None:
            # No existing file — compute full range through current month
            full_data = calc.compute_all_window_timeseries(
                journal_id=jid,
                start_year=start_year, start_month=1,
                end_year=now.year, end_month=now.month,
            )
            ts = full_data["default"]
            if not ts:
                log.warning(f"  No data for {name}")
                continue
            exporter.export_journal(
                slug=slug, name=name, issn=issn,
                timeseries=ts,
                timeseries_12mo=full_data["12mo"],
                timeseries_5yr=full_data["5yr"],
            )
        else:
            # Merge: prepend historical entries before existing ones
            fmt = JSONExporter._format_timeseries

            for ts_key, wd_key in [("timeseries", "default"),
                                    ("timeseries_12mo", "12mo"),
                                    ("timeseries_5yr", "5yr")]:
                new_entries = fmt(window_data[wd_key])
                old_entries = existing.get(ts_key, [])

                if old_entries:
                    first_existing_month = old_entries[0]["month"]
                    # Keep only backfill entries strictly before existing data
                    new_entries = [e for e in new_entries
                                  if e["month"] < first_existing_month]

                existing[ts_key] = new_entries + old_entries

            existing["last_updated"] = datetime.now().strftime("%Y-%m-%d")

            with open(json_path, "w") as f:
                json.dump(existing, f, indent=2)

            log.info(f"  Merged {len(new_entries)} historical months into {slug}.json")

        # --- Phase B: Re-export authors + papers (picks up old papers) ---
        author_rows = db.get_paper_authors_for_journal(jid)
        exporter.export_journal_authors(slug, author_rows)

        paper_rows = db.get_papers_for_export(jid)
        geo_rows = db.get_country_by_year(jid)
        pmids = [r["pmid"] for r in paper_rows]
        cits_by_year = db.get_citations_by_year_for_pmids(pmids)
        exporter.export_journal_papers(slug, paper_rows, geo_rows, cits_by_year)

        # Collect index entry from the latest snapshot (read merged JSON)
        if os.path.exists(json_path):
            with open(json_path) as f:
                merged = json.load(f)
            ts = merged.get("timeseries", [])
            if ts:
                latest = ts[-1]
                index_entries.append({
                    "slug": slug,
                    "name": name,
                    "issn": issn,
                    "abbreviation": journal.get("abbreviation", ""),
                    "latest_if": latest.get("rolling_if", 0),
                    "latest_if_no_reviews": latest.get("rolling_if_no_reviews", 0),
                    "official_jif": None,
                    "paper_count": latest.get("papers", 0),
                    "latest_month": latest.get("month", ""),
                })

    db.close()
    return index_entries


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start-year", type=int, default=2005,
                        help="First year to compute snapshots for (default: 2005)")
    parser.add_argument("--end-year", type=int, default=2011,
                        help="Last year to compute snapshots for (default: 2011)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel worker processes (default: 1)")
    args = parser.parse_args()

    db = DatabaseManager(DB_PATH)
    journals = db.get_all_journals()
    db.close()

    total = len(journals)
    logger.info(f"Backfilling {args.start_year}-{args.end_year} snapshots "
                f"for {total} journals with {args.workers} worker(s)")

    if args.workers == 1:
        index_entries = process_chunk(
            journals, DB_PATH, args.start_year, args.end_year, 0
        )
    else:
        chunks = [journals[i::args.workers] for i in range(args.workers)]
        index_entries = []

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    process_chunk, chunk, DB_PATH,
                    args.start_year, args.end_year, wid
                ): wid
                for wid, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                wid = futures[future]
                try:
                    entries = future.result()
                    index_entries.extend(entries)
                    logger.info(f"Worker {wid} finished: {len(entries)} journals")
                except Exception as e:
                    logger.error(f"Worker {wid} failed: {e}", exc_info=True)

    # Re-export index.json
    index_entries.sort(key=lambda x: x["name"].lower())
    exporter = JSONExporter()
    exporter.export_index(index_entries)

    logger.info(f"Done: {len(index_entries)} journals backfilled.")


if __name__ == "__main__":
    main()
