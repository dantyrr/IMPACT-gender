#!/usr/bin/env python3
"""
Seed (or refresh) the persistent PMID date cache from the citations table.

Run this once after fix_citation_months.py finishes to persist the resolved
dates into data/pmid_dates.db.  Future runs of fix_citation_months will load
from this cache first and only query PubMed for PMIDs not yet seen, making
each subsequent run much faster.

Usage:
    python scripts/build_date_cache.py

The cache is additive — safe to run repeatedly; existing entries are kept.
"""

import sys
import os
import sqlite3
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH, PMID_DATE_CACHE_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_date_cache")


def main():
    logger.info(f"Source DB:  {DB_PATH}")
    logger.info(f"Cache path: {PMID_DATE_CACHE_PATH}")

    # Open cache
    cache = sqlite3.connect(PMID_DATE_CACHE_PATH)
    cache.execute("PRAGMA journal_mode=WAL")
    cache.execute("""
        CREATE TABLE IF NOT EXISTS pmid_dates (
            pmid      INTEGER PRIMARY KEY,
            pub_year  INTEGER NOT NULL,
            pub_month INTEGER NOT NULL
        )
    """)
    cache.commit()

    cur_count = cache.execute("SELECT COUNT(*) FROM pmid_dates").fetchone()[0]
    logger.info(f"  Cache currently has {cur_count:,} entries")

    # Read all unique citing PMIDs with resolved dates from the citations table
    logger.info("Reading citations table...")
    src = sqlite3.connect(DB_PATH)
    rows = src.execute(
        "SELECT DISTINCT citing_pmid, citing_year, citing_month FROM citations"
    ).fetchall()
    src.close()
    logger.info(f"  Found {len(rows):,} unique (pmid, year, month) rows")

    # Bulk insert into cache (INSERT OR REPLACE so newer data wins)
    chunk = 50_000
    inserted = 0
    for i in range(0, len(rows), chunk):
        cache.executemany(
            "INSERT OR REPLACE INTO pmid_dates (pmid, pub_year, pub_month) VALUES (?,?,?)",
            rows[i : i + chunk],
        )
        cache.commit()
        inserted += len(rows[i : i + chunk])
        logger.info(f"  Written {inserted:,} / {len(rows):,}")

    new_count = cache.execute("SELECT COUNT(*) FROM pmid_dates").fetchone()[0]
    logger.info(f"Cache now has {new_count:,} entries (was {cur_count:,})")

    # Report month distribution of cache
    dist = cache.execute(
        "SELECT pub_month, COUNT(*) FROM pmid_dates GROUP BY pub_month ORDER BY pub_month"
    ).fetchall()
    total = sum(r[1] for r in dist)
    real = sum(r[1] for r in dist if r[0] != 6)
    logger.info(
        f"Month precision: {real:,} / {total:,} ({real/total*100:.1f}%) have a real month"
    )

    cache.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
