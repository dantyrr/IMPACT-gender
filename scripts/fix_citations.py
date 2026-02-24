#!/usr/bin/env python3
"""
Fix: Re-resolve citations for 2023-2026 papers that have 0 citation data.

Root cause: Only 2022 papers have citation events in the DB. This causes
rolling_if = 0 for any month where the paper window doesn't include 2022.

This script:
1. Gets all Aging Cell (and any other) paper PMIDs with pub_year >= 2023
2. Re-fetches their iCite data to get fresh cited_by lists
3. Resolves citing paper dates and stores new citation events
4. Reports how many citations were added

Run from IMPACT project root:
    python scripts/fix_citations.py
    python scripts/fix_citations.py --year-from 2023   # default
    python scripts/fix_citations.py --year-from 2022   # re-do everything
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.icite_fetcher import IciteFetcher
from src.pipeline.citation_resolver import CitationResolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fix_citations")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-from", type=int, default=2023,
                        help="Re-resolve citations for papers with pub_year >= this (default: 2023)")
    args = parser.parse_args()

    logger.info(f"DB path: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        logger.error(f"DB not found at {DB_PATH}")
        sys.exit(1)

    db = DatabaseManager(DB_PATH)
    icite = IciteFetcher()
    resolver = CitationResolver(db, icite)

    # ── 1. Show current state ────────────────────────────────────────────
    cursor = db.conn.cursor()

    logger.info("Current citation state by pub_year:")
    rows = cursor.execute("""
        SELECT p.pub_year,
               COUNT(DISTINCT p.pmid) as papers,
               COUNT(c.id) as citations
        FROM papers p
        LEFT JOIN citations c ON c.cited_pmid = p.pmid
        GROUP BY p.pub_year
        ORDER BY p.pub_year
    """).fetchall()
    for r in rows:
        logger.info(f"  pub_year={r[0]}: {r[1]} papers, {r[2]} citation events")

    # ── 2. Get paper PMIDs with pub_year >= year_from ───────────────────
    rows = cursor.execute(
        "SELECT pmid FROM papers WHERE pub_year >= ? ORDER BY pmid",
        (args.year_from,)
    ).fetchall()
    target_pmids = [r[0] for r in rows]

    logger.info(f"\nFound {len(target_pmids)} papers with pub_year >= {args.year_from}")
    logger.info("Step 1: Fetching fresh iCite data for these papers...")

    icite_data = icite.fetch_batch(target_pmids)
    logger.info(f"  iCite returned records for {len(icite_data)} papers")

    # Show a quick sample of cited_by lengths
    sample = list(icite_data.items())[:5]
    for pmid, rec in sample:
        cb = rec.get("cited_by", [])
        logger.info(f"  Sample PMID {pmid}: cited_by has {len(cb)} entries, year={rec.get('year')}")

    # ── 3. Check if cited_by is populated ───────────────────────────────
    papers_with_citations = sum(1 for r in icite_data.values() if r.get("cited_by"))
    logger.info(f"\n{papers_with_citations}/{len(icite_data)} papers have non-empty cited_by")

    if papers_with_citations == 0:
        logger.warning("iCite returned empty cited_by for ALL papers!")
        logger.warning("This may mean iCite's cited_by data isn't available for these papers.")
        logger.warning("The rolling IF may need to rely on older citation data.")
        db.close()
        return

    # ── 4. Resolve citations for all journals ───────────────────────────
    # Group by journal
    journal_rows = cursor.execute(
        "SELECT id, name FROM journals"
    ).fetchall()

    total_new = 0
    for jid, jname in journal_rows:
        # Get icite_data for papers in this journal
        jpapers = cursor.execute(
            "SELECT pmid FROM papers WHERE journal_id=? AND pub_year >= ?",
            (jid, args.year_from)
        ).fetchall()
        jpmids = {r[0] for r in jpapers}

        j_icite = {pmid: rec for pmid, rec in icite_data.items() if pmid in jpmids}
        if not j_icite:
            continue

        logger.info(f"\nResolving citations for {jname} ({len(j_icite)} papers)...")

        # Count citations before
        before = cursor.execute(
            "SELECT COUNT(*) FROM citations c JOIN papers p ON c.cited_pmid=p.pmid "
            "WHERE p.journal_id=? AND p.pub_year >= ?", (jid, args.year_from)
        ).fetchone()[0]

        result = resolver.resolve_journal_papers(jid, j_icite)

        # Count citations after
        after = cursor.execute(
            "SELECT COUNT(*) FROM citations c JOIN papers p ON c.cited_pmid=p.pmid "
            "WHERE p.journal_id=? AND p.pub_year >= ?", (jid, args.year_from)
        ).fetchone()[0]

        new_cites = after - before
        total_new += new_cites
        logger.info(f"  {jname}: {result['total_citations_resolved']} citation events resolved, "
                    f"{new_cites} net new added to DB")

    logger.info(f"\nTotal new citation events added: {total_new}")

    # ── 5. Show updated state ────────────────────────────────────────────
    logger.info("\nUpdated citation state by pub_year:")
    rows = cursor.execute("""
        SELECT p.pub_year,
               COUNT(DISTINCT p.pmid) as papers,
               COUNT(c.id) as citations
        FROM papers p
        LEFT JOIN citations c ON c.cited_pmid = p.pmid
        GROUP BY p.pub_year
        ORDER BY p.pub_year
    """).fetchall()
    for r in rows:
        logger.info(f"  pub_year={r[0]}: {r[1]} papers, {r[2]} citation events")

    db.close()
    logger.info("\nDone! Now run: python scripts/compute_snapshots.py")


if __name__ == "__main__":
    main()
