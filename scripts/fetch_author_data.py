#!/usr/bin/env python3
"""
Backfill first/last author names and affiliations for all papers in the DB.

Runs the DB migration first (safe to re-run), then fetches EFetch XML from
PubMed in batches for any paper that doesn't have author data yet.

Usage:
    python scripts/fetch_author_data.py
    python scripts/fetch_author_data.py --journal nat-aging   # single journal
    python scripts/fetch_author_data.py --limit 500           # test run
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH, JOURNALS
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.pubmed_fetcher import PubMedFetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_authors")


def main():
    parser = argparse.ArgumentParser(description="Fetch author data for IMPACT papers")
    parser.add_argument("--journal", type=str, default=None,
                        help="Limit to one journal slug (e.g. 'nat-aging')")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after processing this many PMIDs (for testing)")
    args = parser.parse_args()

    db = DatabaseManager(DB_PATH)

    # Ensure author columns exist
    logger.info("Running schema migration...")
    db.migrate_add_author_columns()

    # Resolve optional journal filter
    journal_id = None
    if args.journal:
        match = next(
            (meta for meta in JOURNALS.values() if meta.get("slug") == args.journal),
            None,
        )
        if not match:
            logger.error(f"Unknown journal slug '{args.journal}'. "
                         f"Available: {[m['slug'] for m in JOURNALS.values()]}")
            sys.exit(1)
        issn = next(k for k, v in JOURNALS.items() if v.get("slug") == args.journal)
        row = db.get_journal_by_issn(issn)
        if row:
            journal_id = row["id"]
        logger.info(f"Filtering to journal: {match['name']} (id={journal_id})")

    pmids = db.get_pmids_missing_authors(journal_id)
    logger.info(f"Found {len(pmids)} papers without author data")

    if args.limit:
        pmids = pmids[: args.limit]
        logger.info(f"Limiting to first {len(pmids)} PMIDs")

    if not pmids:
        logger.info("Nothing to do.")
        db.close()
        return

    pubmed = PubMedFetcher()
    batch_size = 100
    total_updated = 0

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i: i + batch_size]
        try:
            author_rows = pubmed.fetch_author_details(batch, batch_size=batch_size)
            db.update_paper_authors_bulk(author_rows)
            total_updated += len(author_rows)
        except Exception as e:
            logger.error(f"Error on batch starting at index {i}: {e}", exc_info=True)
            continue

        pct = min(100, round((i + len(batch)) / len(pmids) * 100))
        logger.info(
            f"Progress: {i + len(batch)}/{len(pmids)} PMIDs ({pct}%), "
            f"{total_updated} rows updated"
        )

    db.close()
    logger.info(f"Done. Updated author data for {total_updated} papers.")


if __name__ == "__main__":
    main()
