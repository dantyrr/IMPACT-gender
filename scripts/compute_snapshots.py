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

# Official 2024 JIFs for comparison
OFFICIAL_JIFS = {
    # Original 5
    "1474-9718": 8.0,    # Aging Cell
    "0021-9738": 13.3,   # JCI
    "2662-8465": 17.0,   # Nature Aging
    "2050-084X": 6.4,    # eLife
    "2047-9980": 5.0,    # JAHA
    # New journals (2023 JIF; 2024 values pending release)
    "0028-4793": 96.2,   # NEJM
    "0140-6736": 168.9,  # The Lancet
    "0098-7484": 63.1,   # JAMA
    "0003-4819": 39.2,   # Annals of Internal Medicine
    "1078-8956": 82.9,   # Nature Medicine
    "0092-8674": 64.5,   # Cell
    "1946-6234": 17.1,   # Science Translational Medicine
    "0009-7322": 37.8,   # Circulation
    "0006-4971": 20.3,   # Blood
    "1074-7613": 32.4,   # Immunity
    "1529-2908": 27.7,   # Nature Immunology
    "0016-5085": 29.4,   # Gastroenterology
    "0017-5749": 24.5,   # Gut
    "1554-8627": 14.9,   # Autophagy
    "2213-2317": 11.4,   # Redox Biology
    "2470-9468": 17.6,   # Science Immunology
    "0036-8075": 44.7,   # Science
    "0028-0836": 50.5,   # Nature
    "2041-1723": 14.7,   # Nature Communications
    # Nature specialty journals
    "1061-4036": 41.1,   # Nature Genetics
    "1548-7091": 48.0,   # Nature Methods
    "1087-0156": 33.1,   # Nature Biotechnology
    "1465-7392": 21.3,   # Nature Cell Biology
    "1545-9993": 12.5,   # Nature Structural and Molecular Biology
    # Clinical
    "0959-8138": 105.7,  # The BMJ
    "0923-7534": 51.8,   # Annals of Oncology
    "2374-2437": 33.0,   # JAMA Oncology
    "2168-6106": 22.5,   # JAMA Internal Medicine
    # Cell biology / molecular
    "1097-2765": 14.9,   # Molecular Cell
    "1534-5807": 11.1,   # Developmental Cell
    "0261-4189": 9.4,    # EMBO Journal
    # Experimental medicine / open access
    "0022-1007": 14.6,   # Journal of Experimental Medicine
    "1544-9173": 9.8,    # PLOS Biology
}


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
            start_year=2012, start_month=1,
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
    args = parser.parse_args()

    db = DatabaseManager(DB_PATH)
    journals = db.get_all_journals()
    db.close()

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

    # Sort index by name for consistent output
    index_entries.sort(key=lambda x: x["name"].lower())

    exporter = JSONExporter()
    exporter.export_index(index_entries)

    logger.info(f"Done: {len(index_entries)} journals exported.")


if __name__ == "__main__":
    main()
