#!/usr/bin/env python3
"""
Generate realistic sample data for testing IMPACT's computation engine
and website without requiring live API access.

Uses realistic paper counts and citation distributions based on known
journal characteristics.
"""

import sys
import os
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import JOURNALS, DB_PATH
from src.pipeline.db_manager import DatabaseManager

random.seed(42)  # Reproducible

# Approximate papers/year and average citations for each journal
JOURNAL_PROFILES = {
    "1474-9718": {  # Aging Cell
        "papers_per_year": 200,
        "review_pct": 0.12,
        "mean_cites_2yr": 16,
        "official_if": 8.0,
    },
    "0021-9738": {  # JCI
        "papers_per_year": 350,
        "review_pct": 0.08,
        "mean_cites_2yr": 28,
        "official_if": 13.3,
    },
    "2662-8465": {  # Nature Aging
        "papers_per_year": 120,
        "review_pct": 0.15,
        "mean_cites_2yr": 34,
        "official_if": 17.0,
    },
    "2050-084X": {  # eLife
        "papers_per_year": 800,
        "review_pct": 0.05,
        "mean_cites_2yr": 13,
        "official_if": 6.4,
    },
    "2047-9980": {  # JAHA
        "papers_per_year": 600,
        "review_pct": 0.10,
        "mean_cites_2yr": 10,
        "official_if": 5.0,
    },
}


def generate_papers(journal_id: int, issn: str, profile: dict,
                    year_start: int, year_end: int) -> list:
    """Generate realistic paper records for a journal."""
    papers = []
    pmid_counter = journal_id * 100000 + 1  # Fake PMIDs in distinct ranges

    for year in range(year_start, year_end + 1):
        n_papers = profile["papers_per_year"] + random.randint(-20, 20)
        for i in range(n_papers):
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            is_review = random.random() < profile["review_pct"]

            papers.append({
                "pmid": pmid_counter,
                "journal_id": journal_id,
                "title": f"Sample paper {pmid_counter} in journal {journal_id}",
                "pub_date": f"{year}-{month:02d}-{day:02d}",
                "pub_year": year,
                "pub_month": month,
                "pub_type": "Review" if is_review else "Journal Article",
                "is_research": 0 if is_review else 1,
                "doi": f"10.{10000 + journal_id}/{pmid_counter}",
            })
            pmid_counter += 1

    return papers


def generate_citations(papers: list, profile: dict) -> list:
    """Generate realistic citation events for a set of papers."""
    citations = []
    citing_pmid_counter = 9000000  # Distinct range for citing papers

    mean_cites = profile["mean_cites_2yr"]

    for paper in papers:
        pub_year = paper["pub_year"]
        pub_month = paper["pub_month"]

        # Number of citations follows a log-normal-ish distribution
        n_cites = max(0, int(random.gauss(mean_cites, mean_cites * 0.6)))

        for _ in range(n_cites):
            # Citations spread over ~30 months after publication
            delay_months = random.randint(0, 30)
            cite_date = datetime(pub_year, pub_month, 1) + timedelta(days=delay_months * 30)

            # Don't generate citations in the future
            if cite_date > datetime(2026, 2, 1):
                continue

            citations.append({
                "cited_pmid": paper["pmid"],
                "citing_pmid": citing_pmid_counter,
                "citing_date": cite_date.strftime("%Y-%m-%d"),
                "citing_year": cite_date.year,
                "citing_month": cite_date.month,
            })
            citing_pmid_counter += 1

    return citations


def main():
    print(f"Generating sample data in: {DB_PATH}")

    # Re-initialize DB
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    db = DatabaseManager(DB_PATH)
    db.init_schema()

    year_start = 2020
    year_end = 2025

    for issn, meta in JOURNALS.items():
        journal_id = db.add_journal(
            issn=issn,
            name=meta["name"],
            abbreviation=meta.get("abbreviation"),
            slug=meta.get("slug"),
        )

        profile = JOURNAL_PROFILES[issn]
        print(f"\n{meta['name']}:")

        # Generate papers
        papers = generate_papers(journal_id, issn, profile, year_start, year_end)
        db.add_papers_bulk(papers)
        print(f"  Papers: {len(papers)}")

        # Generate citations
        citations = generate_citations(papers, profile)
        db.add_citations_bulk(citations)
        print(f"  Citations: {len(citations)}")

    db.close()
    print(f"\nDone! Database: {DB_PATH}")
    print("Run `python scripts/compute_snapshots.py` next to compute rolling IFs.")


if __name__ == "__main__":
    main()
