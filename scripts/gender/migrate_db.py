"""Add gender columns to papers table and create gender_citation_stats table."""
import sqlite3
import sys


def migrate(db_path: str = "data/impact.db"):
    conn = sqlite3.connect(db_path)

    # Add gender columns to papers table
    existing = {row[1] for row in conn.execute("PRAGMA table_info(papers)")}

    new_cols = [
        ("first_author_forename", "TEXT"),
        ("last_author_forename", "TEXT"),
        ("first_author_gender", "TEXT"),   # 'W', 'M', 'U'
        ("first_author_gender_prob", "REAL"),
        ("last_author_gender", "TEXT"),
        ("last_author_gender_prob", "REAL"),
        ("gender_pair", "TEXT"),           # 'WW', 'WM', 'MW', 'MM', or NULL
    ]

    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE papers ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")

    # Create index on gender_pair for fast aggregation
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_gender_pair
        ON papers(gender_pair, journal_id, pub_year)
    """)

    # Create gender_citation_stats table for precomputed per-journal aggregates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gender_citation_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id      INTEGER NOT NULL,
            snapshot_month  TEXT NOT NULL,
            gender_pair     TEXT NOT NULL,
            paper_count     INTEGER DEFAULT 0,
            citation_count  INTEGER DEFAULT 0,
            rolling_if_24m  REAL,
            rolling_if_12m  REAL,
            rolling_if_5yr  REAL,
            FOREIGN KEY (journal_id) REFERENCES journals(id),
            UNIQUE(journal_id, snapshot_month, gender_pair)
        )
    """)

    conn.commit()
    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/impact.db"
    migrate(db_path)
