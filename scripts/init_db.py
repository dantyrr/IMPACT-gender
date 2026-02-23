#!/usr/bin/env python3
"""
Initialize the IMPACT database and seed it with starter journals.
"""

import sys
import os

# Add repo root to path so we can import src.pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import JOURNALS, DB_PATH
from src.pipeline.db_manager import DatabaseManager


def main():
    print(f"Initializing database at: {DB_PATH}")
    db = DatabaseManager(DB_PATH)
    db.init_schema()

    for issn, meta in JOURNALS.items():
        journal_id = db.add_journal(
            issn=issn,
            name=meta["name"],
            abbreviation=meta.get("abbreviation"),
            slug=meta.get("slug"),
        )
        print(f"  Added journal: {meta['name']} (id={journal_id})")

    journals = db.get_all_journals()
    print(f"\nTotal journals in DB: {len(journals)}")
    db.close()
    print("Done.")


if __name__ == "__main__":
    main()
