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

        # Compute all three window variants from Jan 2012 to current month
        now = datetime.now()
        window_data = calc.compute_all_window_timeseries(
            journal_id=jid,
            start_year=2012, start_month=1,
            end_year=now.year, end_month=now.month,
        )

        timeseries = window_data["default"]

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

        # Export journal JSON with all window variants
        exporter.export_journal(
            slug=slug,
            name=name,
            issn=issn,
            timeseries=timeseries,
            timeseries_12mo=window_data["12mo"],
            timeseries_5yr=window_data["5yr"],
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
