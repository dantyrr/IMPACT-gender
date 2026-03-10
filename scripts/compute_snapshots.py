#!/usr/bin/env python3
"""
Compute rolling IF snapshots for all journals and export JSON.
Run after the data pipeline has populated the database.

Usage:
    python scripts/compute_snapshots.py              # single-threaded
    python scripts/compute_snapshots.py --workers 4  # parallel workers
"""

import sys
import os
import logging
import argparse
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH, JOURNALS
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.impact_calculator import ImpactCalculator
from src.pipeline.json_exporter import JSONExporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("compute")

OFFICIAL_JIFS = {}


def process_journals(journal_chunk, db_path, official_jifs, worker_id):
    """Process a list of journals in a worker process. Returns index entries."""
    # Each worker opens its own DB connection
    from src.pipeline.db_manager import DatabaseManager
    from src.pipeline.impact_calculator import ImpactCalculator
    from src.pipeline.json_exporter import JSONExporter

    log = logging.getLogger(f"compute.w{worker_id}")
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

        window_data = calc.compute_all_window_timeseries(
            journal_id=jid,
            start_year=2005, start_month=1,
            end_year=now.year, end_month=now.month,
        )

        timeseries = window_data["default"]
        if not timeseries:
            log.warning(f"  No data for {name}")
            continue

        latest = timeseries[-1]

        exporter.export_journal(
            slug=slug,
            name=name,
            issn=issn,
            timeseries=timeseries,
            timeseries_12mo=window_data["12mo"],
            timeseries_5yr=window_data["5yr"],
            official_if=official_jifs.get(issn),
        )

        author_rows = db.get_paper_authors_for_journal(jid)
        exporter.export_journal_authors(slug, author_rows)

        paper_rows = db.get_papers_for_export(jid)
        geo_rows = db.get_country_by_year(jid)
        pmids = [r['pmid'] for r in paper_rows]
        cits_by_year = db.get_citations_by_year_for_pmids(pmids)
        exporter.export_journal_papers(slug, paper_rows, geo_rows, cits_by_year)

        index_entries.append({
            "slug": slug,
            "name": name,
            "issn": issn,
            "abbreviation": journal.get("abbreviation", ""),
            "latest_if": latest["rolling_if"],
            "latest_if_no_reviews": latest["rolling_if_no_reviews"],
            "official_jif": official_jifs.get(issn),
            "paper_count": latest["paper_count"],
            "latest_month": latest["month"],
        })

    db.close()
    return index_entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel worker processes (default: 1)")
    parser.add_argument("--slug", type=str, nargs="+", default=None,
                        help="Only process these journal slug(s)")
    args = parser.parse_args()

    db = DatabaseManager(DB_PATH)
    journals = db.get_all_journals()
    db.close()

    if args.slug:
        slug_set = set(args.slug)
        journals = [j for j in journals if j["slug"] in slug_set]
        if not journals:
            logger.error(f"No journals found for slug(s): {args.slug}")
            sys.exit(1)

    total = len(journals)
    logger.info(f"Processing {total} journals with {args.workers} worker(s)")

    if args.workers == 1:
        index_entries = process_journals(journals, DB_PATH, OFFICIAL_JIFS, 0)
    else:
        # Split journals across workers (interleaved so each worker gets a mix
        # of big and small journals rather than alphabetical blocks)
        chunks = [journals[i::args.workers] for i in range(args.workers)]
        index_entries = []

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_journals, chunk, DB_PATH, OFFICIAL_JIFS, wid): wid
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

    # When filtering by slug, merge new entries into existing index
    if args.slug:
        import json
        index_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "docs", "data", "index.json")
        if os.path.exists(index_path):
            with open(index_path) as f:
                existing = json.load(f)
            existing_journals = existing.get("journals", existing)
            updated_slugs = {e["slug"] for e in index_entries}
            merged = [e for e in existing_journals if e["slug"] not in updated_slugs]
            merged.extend(index_entries)
            index_entries = merged

    # Sort index by name for consistent output
    index_entries.sort(key=lambda x: x["name"].lower())

    exporter = JSONExporter()
    exporter.export_index(index_entries)

    logger.info(f"Done: {len(index_entries)} journals exported.")


if __name__ == "__main__":
    main()
