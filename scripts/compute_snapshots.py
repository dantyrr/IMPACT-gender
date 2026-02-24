#!/usr/bin/env python3
"""
Compute rolling IF snapshots for all journals and export JSON.
Run after the data pipeline has populated the database.
"""

import sys
import os
import logging
from datetime import datetime

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
    "1474-9718": 8.0,
    "0021-9738": 13.3,
    "2662-8465": 17.0,
    "2050-084X": 6.4,
    "2047-9980": 5.0,
}


def main():
    db = DatabaseManager(DB_PATH)
    calc = ImpactCalculator(db)
    exporter = JSONExporter()

    journals = db.get_all_journals()
    index_entries = []

    for journal in journals:
        jid = journal["id"]
        name = journal["name"]
        issn = journal["issn"]
        slug = journal["slug"]

        logger.info(f"Computing timeseries for {name}...")

        # Compute from Jan 2022 to current month
        now = datetime.now()
        timeseries = calc.compute_and_store_timeseries(
            journal_id=jid,
            start_year=2022, start_month=1,
            end_year=now.year, end_month=now.month,
        )

        if not timeseries:
            logger.warning(f"  No data for {name}")
            continue

        latest = timeseries[-1]
        logger.info(
            f"  Latest IF: {latest['rolling_if']:.2f} "
            f"(official: {OFFICIAL_JIFS.get(issn, 'N/A')}), "
            f"papers: {latest['paper_count']}, "
            f"citations: {latest['citation_count']}"
        )

        # Export journal JSON
        exporter.export_journal(
            slug=slug,
            name=name,
            issn=issn,
            timeseries=timeseries,
            official_if=OFFICIAL_JIFS.get(issn),
        )

        # Add to index
        index_entries.append({
            "slug": slug,
            "name": name,
            "issn": issn,
            "abbreviation": journal.get("abbreviation", ""),
            "latest_if": latest["rolling_if"],
            "latest_if_no_reviews": latest["rolling_if_no_reviews"],
            "official_jif": OFFICIAL_JIFS.get(issn),
            "paper_count": latest["paper_count"],
            "latest_month": latest["month"],
        })

    # Export index
    exporter.export_index(index_entries)

    db.close()
    logger.info("All snapshots computed and exported!")


if __name__ == "__main__":
    main()
