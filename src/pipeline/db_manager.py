"""
IMPACT Database Manager
SQLite CRUD operations for journals, papers, citations, and monthly snapshots.
"""

import sqlite3
import os
from typing import List, Dict, Optional, Tuple


class DatabaseManager:
    """Manages the SQLite database for IMPACT."""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    # ------------------------------------------------------------------ #
    #  Schema
    # ------------------------------------------------------------------ #

    def init_schema(self):
        """Create all tables if they don't exist."""
        cursor = self.conn.cursor()

        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS journals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issn TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                abbreviation TEXT,
                slug TEXT
            );

            CREATE TABLE IF NOT EXISTS papers (
                pmid INTEGER PRIMARY KEY,
                journal_id INTEGER NOT NULL REFERENCES journals(id),
                title TEXT NOT NULL,
                pub_date TEXT NOT NULL,
                pub_year INTEGER NOT NULL,
                pub_month INTEGER,
                pub_type TEXT,
                is_research INTEGER DEFAULT 1,
                doi TEXT
            );

            CREATE TABLE IF NOT EXISTS citations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cited_pmid INTEGER NOT NULL,
                citing_pmid INTEGER NOT NULL,
                citing_date TEXT NOT NULL,
                citing_year INTEGER NOT NULL,
                citing_month INTEGER,
                UNIQUE(cited_pmid, citing_pmid)
            );

            CREATE TABLE IF NOT EXISTS monthly_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                journal_id INTEGER NOT NULL REFERENCES journals(id),
                snapshot_month TEXT NOT NULL,
                rolling_if REAL,
                rolling_if_no_reviews REAL,
                paper_count INTEGER,
                citation_count INTEGER,
                review_count INTEGER,
                UNIQUE(journal_id, snapshot_month)
            );

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_papers_journal_year
                ON papers(journal_id, pub_year);
            CREATE INDEX IF NOT EXISTS idx_papers_pub_date
                ON papers(journal_id, pub_date);
            CREATE INDEX IF NOT EXISTS idx_citations_cited
                ON citations(cited_pmid);
            CREATE INDEX IF NOT EXISTS idx_citations_citing_date
                ON citations(citing_year, citing_month);
            CREATE INDEX IF NOT EXISTS idx_snapshots_journal_month
                ON monthly_snapshots(journal_id, snapshot_month);
        """)

        self.conn.commit()

    # ------------------------------------------------------------------ #
    #  Journals
    # ------------------------------------------------------------------ #

    def add_journal(self, issn: str, name: str,
                    abbreviation: str = None, slug: str = None) -> int:
        """Insert a journal. Returns the journal id."""
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT OR IGNORE INTO journals (issn, name, abbreviation, slug)
               VALUES (?, ?, ?, ?)""",
            (issn, name, abbreviation, slug),
        )
        self.conn.commit()
        # Return existing or new id
        cursor.execute("SELECT id FROM journals WHERE issn = ?", (issn,))
        return cursor.fetchone()["id"]

    def get_journal_by_issn(self, issn: str) -> Optional[Dict]:
        """Look up a journal by ISSN."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM journals WHERE issn = ?", (issn,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_journals(self) -> List[Dict]:
        """Return all registered journals."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM journals ORDER BY name")
        return [dict(r) for r in cursor.fetchall()]

    # ------------------------------------------------------------------ #
    #  Papers
    # ------------------------------------------------------------------ #

    def add_paper(self, pmid: int, journal_id: int, title: str,
                  pub_date: str, pub_year: int, pub_month: int = None,
                  pub_type: str = None, is_research: int = 1,
                  doi: str = None):
        """Insert a paper (skip if already exists)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO papers
               (pmid, journal_id, title, pub_date, pub_year, pub_month,
                pub_type, is_research, doi)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pmid, journal_id, title, pub_date, pub_year, pub_month,
             pub_type, is_research, doi),
        )
        self.conn.commit()

    def add_papers_bulk(self, papers: List[Dict]):
        """Bulk-insert papers (skip duplicates)."""
        self.conn.executemany(
            """INSERT OR IGNORE INTO papers
               (pmid, journal_id, title, pub_date, pub_year, pub_month,
                pub_type, is_research, doi)
               VALUES (:pmid, :journal_id, :title, :pub_date, :pub_year,
                        :pub_month, :pub_type, :is_research, :doi)""",
            papers,
        )
        self.conn.commit()

    def get_paper(self, pmid: int) -> Optional[Dict]:
        """Look up a single paper by PMID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM papers WHERE pmid = ?", (pmid,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_papers_in_window(self, journal_id: int,
                             start_date: str, end_date: str) -> List[Dict]:
        """
        Get papers published within a date range.
        start_date, end_date are 'YYYY-MM-DD' strings (inclusive).
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM papers
               WHERE journal_id = ?
                 AND pub_date >= ? AND pub_date <= ?
               ORDER BY pub_date""",
            (journal_id, start_date, end_date),
        )
        return [dict(r) for r in cursor.fetchall()]

    def get_paper_count_for_journal(self, journal_id: int) -> int:
        """Total papers stored for a journal."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM papers WHERE journal_id = ?",
            (journal_id,),
        )
        return cursor.fetchone()["cnt"]

    # ------------------------------------------------------------------ #
    #  Citations
    # ------------------------------------------------------------------ #

    def add_citation(self, cited_pmid: int, citing_pmid: int,
                     citing_date: str, citing_year: int,
                     citing_month: int = None):
        """Record a citation event (skip if duplicate pair)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO citations
               (cited_pmid, citing_pmid, citing_date, citing_year, citing_month)
               VALUES (?, ?, ?, ?, ?)""",
            (cited_pmid, citing_pmid, citing_date, citing_year, citing_month),
        )

    def add_citations_bulk(self, citations: List[Dict]):
        """Bulk-insert citation events."""
        self.conn.executemany(
            """INSERT OR IGNORE INTO citations
               (cited_pmid, citing_pmid, citing_date, citing_year, citing_month)
               VALUES (:cited_pmid, :citing_pmid, :citing_date,
                        :citing_year, :citing_month)""",
            citations,
        )
        self.conn.commit()

    def get_citations_for_paper(self, pmid: int) -> List[Dict]:
        """Get all citation events where this paper is cited."""
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM citations
               WHERE cited_pmid = ? ORDER BY citing_date""",
            (pmid,),
        )
        return [dict(r) for r in cursor.fetchall()]

    def count_citations_in_period(self, pmid: int,
                                  start_date: str, end_date: str) -> int:
        """Count citations of a paper within a date range."""
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT COUNT(*) as cnt FROM citations
               WHERE cited_pmid = ?
                 AND citing_date >= ? AND citing_date <= ?""",
            (pmid, start_date, end_date),
        )
        return cursor.fetchone()["cnt"]

    def count_citations_for_papers(self, pmids: List[int],
                                   start_date: str, end_date: str) -> int:
        """Count total citations for a set of papers within a date range."""
        if not pmids:
            return 0
        placeholders = ",".join("?" * len(pmids))
        cursor = self.conn.cursor()
        cursor.execute(
            f"""SELECT COUNT(*) as cnt FROM citations
                WHERE cited_pmid IN ({placeholders})
                  AND citing_date >= ? AND citing_date <= ?""",
            pmids + [start_date, end_date],
        )
        return cursor.fetchone()["cnt"]

    def get_citation_count_for_paper(self, pmid: int) -> int:
        """Total citation count for a paper (all time)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM citations WHERE cited_pmid = ?",
            (pmid,),
        )
        return cursor.fetchone()["cnt"]

    # ------------------------------------------------------------------ #
    #  Monthly Snapshots
    # ------------------------------------------------------------------ #

    def save_snapshot(self, journal_id: int, snapshot_month: str,
                      rolling_if: float, rolling_if_no_reviews: float,
                      paper_count: int, citation_count: int,
                      review_count: int):
        """Upsert a monthly snapshot."""
        self.conn.execute(
            """INSERT OR REPLACE INTO monthly_snapshots
               (journal_id, snapshot_month, rolling_if, rolling_if_no_reviews,
                paper_count, citation_count, review_count)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (journal_id, snapshot_month, rolling_if, rolling_if_no_reviews,
             paper_count, citation_count, review_count),
        )
        self.conn.commit()

    def get_snapshots(self, journal_id: int,
                      start_month: str = None,
                      end_month: str = None) -> List[Dict]:
        """Get monthly snapshots for a journal, optionally filtered by range."""
        cursor = self.conn.cursor()
        query = "SELECT * FROM monthly_snapshots WHERE journal_id = ?"
        params: list = [journal_id]

        if start_month:
            query += " AND snapshot_month >= ?"
            params.append(start_month)
        if end_month:
            query += " AND snapshot_month <= ?"
            params.append(end_month)

        query += " ORDER BY snapshot_month"
        cursor.execute(query, params)
        return [dict(r) for r in cursor.fetchall()]

    # ------------------------------------------------------------------ #
    #  Housekeeping
    # ------------------------------------------------------------------ #

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()
