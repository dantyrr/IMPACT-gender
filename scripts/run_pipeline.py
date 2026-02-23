#!/usr/bin/env python3
"""
IMPACT Pipeline Orchestrator
Fetches papers from PubMed, resolves citations via iCite, computes rolling IF,
and exports JSON for the website.

Usage:
    python scripts/run_pipeline.py                  # All journals, default years
    python scripts/run_pipeline.py --journal jci    # Single journal by slug
    python scripts/run_pipeline.py --years 2022-2026
"""

import sys
import os
import argparse
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import JOURNALS, DB_PATH
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.pubmed_fetcher import PubMedFetcher
from src.pipeline.icite_fetcher import IciteFetcher
from src.pipeline.citation_resolver import CitationResolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def process_journal(issn: str, meta: dict, db: DatabaseManager,
                    pubmed: PubMedFetcher, icite: IciteFetcher,
                    resolver: CitationResolver,
                    year_start: int, year_end: int):
    """Full pipeline for one journal."""
    name = meta["name"]
    logger.info(f"{'='*60}")
    logger.info(f"Processing: {name} (ISSN: {issn})")
    logger.info(f"Year range: {year_start}–{year_end}")

    # Get or create journal in DB
    journal = db.get_journal_by_issn(issn)
    if not journal:
        journal_id = db.add_journal(issn, name, meta.get("abbreviation"), meta.get("slug"))
    else:
        journal_id = journal["id"]

    # Step 1: Search PubMed for PMIDs
    logger.info(f"Step 1: Searching PubMed...")
    pmids = pubmed.search_journal(issn, year_start, year_end)
    logger.info(f"  Found {len(pmids)} PMIDs")

    if not pmids:
        logger.warning(f"  No papers found for {name}")
        return

    # Step 2: Fetch paper details from PubMed ESummary
    logger.info(f"Step 2: Fetching paper details...")
    papers = pubmed.fetch_paper_details(pmids)
    logger.info(f"  Got details for {len(papers)} papers")

    # Step 3: Store papers in DB
    logger.info(f"Step 3: Storing papers in DB...")
    for paper in papers:
        paper["journal_id"] = journal_id
    db.add_papers_bulk(papers)
    logger.info(f"  Papers stored. Total in DB: {db.get_paper_count_for_journal(journal_id)}")

    # Step 4: Fetch iCite data (citation counts + cited_by lists)
    logger.info(f"Step 4: Fetching iCite data...")
    paper_pmids = [p["pmid"] for p in papers]
    icite_data = icite.fetch_batch(paper_pmids)
    logger.info(f"  iCite returned data for {len(icite_data)} papers")

    # Update is_research from iCite (more reliable than PubMed pub type)
    for pmid, record in icite_data.items():
        is_research = 1 if record.get("is_research_article") else 0
        db.conn.execute(
            "UPDATE papers SET is_research = ? WHERE pmid = ?",
            (is_research, pmid),
        )
    db.commit()

    # Step 5: Resolve citations (get dates of each citing paper)
    logger.info(f"Step 5: Resolving citations...")
    result = resolver.resolve_journal_papers(journal_id, icite_data)
    logger.info(
        f"  Resolved {result['total_citations_resolved']} citation events "
        f"for {result['total_papers']} papers"
    )

    logger.info(f"Done with {name}!")


def main():
    parser = argparse.ArgumentParser(description="IMPACT Data Pipeline")
    parser.add_argument("--journal", type=str, default=None,
                        help="Process single journal by slug (e.g., 'jci')")
    parser.add_argument("--years", type=str, default=None,
                        help="Year range as START-END (e.g., '2022-2026')")
    args = parser.parse_args()

    # Determine year range
    now = datetime.now()
    if args.years:
        parts = args.years.split("-")
        year_start, year_end = int(parts[0]), int(parts[1])
    else:
        year_start = now.year - 2
        year_end = now.year

    # Initialize components
    db = DatabaseManager(DB_PATH)
    db.init_schema()
    pubmed = PubMedFetcher()
    icite = IciteFetcher()
    resolver = CitationResolver(db, icite)

    # Filter journals if --journal specified
    if args.journal:
        journals_to_process = {
            issn: meta for issn, meta in JOURNALS.items()
            if meta.get("slug") == args.journal
        }
        if not journals_to_process:
            logger.error(f"Unknown journal slug: {args.journal}")
            logger.info(f"Available: {[m['slug'] for m in JOURNALS.values()]}")
            sys.exit(1)
    else:
        journals_to_process = JOURNALS

    # Process each journal
    for issn, meta in journals_to_process.items():
        try:
            process_journal(issn, meta, db, pubmed, icite, resolver,
                            year_start, year_end)
        except Exception as e:
            logger.error(f"Error processing {meta['name']}: {e}", exc_info=True)
            continue

    db.close()
    logger.info("Pipeline complete!")


if __name__ == "__main__":
    main()
