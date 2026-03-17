import pytest
import sqlite3
import tempfile
import os


def create_test_db(path):
    """Create a minimal test database with gender-coded papers and citations."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE journals (id INTEGER PRIMARY KEY, name TEXT, slug TEXT);
        INSERT INTO journals VALUES (1, 'Test Journal', 'test-journal');

        CREATE TABLE papers (
            pmid INTEGER PRIMARY KEY, journal_id INTEGER,
            pub_year INTEGER, pub_month INTEGER, is_research INTEGER,
            gender_pair TEXT, first_author_gender TEXT, last_author_gender TEXT,
            first_author_forename TEXT, last_author_forename TEXT,
            first_author_country TEXT
        );
        -- 2023 papers: 3 MM, 2 WW, 2 WM, 1 MW
        INSERT INTO papers VALUES (1, 1, 2023, 1, 1, 'MM', 'M', 'M', 'John', 'Robert', 'USA');
        INSERT INTO papers VALUES (2, 1, 2023, 2, 1, 'MM', 'M', 'M', 'David', 'James', 'USA');
        INSERT INTO papers VALUES (3, 1, 2023, 3, 1, 'MM', 'M', 'M', 'Wei', 'Chen', 'China');
        INSERT INTO papers VALUES (4, 1, 2023, 1, 1, 'WW', 'W', 'W', 'Sarah', 'Maria', 'USA');
        INSERT INTO papers VALUES (5, 1, 2023, 2, 1, 'WW', 'W', 'W', 'Anna', 'Laura', 'Germany');
        INSERT INTO papers VALUES (6, 1, 2023, 3, 1, 'WM', 'W', 'M', 'Jennifer', 'John', 'USA');
        INSERT INTO papers VALUES (7, 1, 2023, 4, 1, 'WM', 'W', 'M', 'Laura', 'David', 'UK');
        INSERT INTO papers VALUES (8, 1, 2023, 5, 1, 'MW', 'M', 'W', 'Michael', 'Sarah', 'USA');

        CREATE TABLE citations (
            id INTEGER PRIMARY KEY, cited_pmid INTEGER, citing_pmid INTEGER,
            citing_year INTEGER, citing_month INTEGER
        );
        -- MM papers get more citations
        INSERT INTO citations VALUES (1, 1, 101, 2024, 1);
        INSERT INTO citations VALUES (2, 1, 102, 2024, 2);
        INSERT INTO citations VALUES (3, 2, 103, 2024, 3);
        INSERT INTO citations VALUES (4, 3, 104, 2024, 4);
        -- WW papers get fewer
        INSERT INTO citations VALUES (5, 4, 105, 2024, 1);
        INSERT INTO citations VALUES (6, 5, 106, 2024, 2);
        -- WM papers
        INSERT INTO citations VALUES (7, 6, 107, 2024, 1);
        INSERT INTO citations VALUES (8, 7, 108, 2024, 3);
        -- MW paper
        INSERT INTO citations VALUES (9, 8, 109, 2024, 2);
    """)
    conn.commit()
    conn.close()
    return path


class TestCitationByGender:
    def test_per_paper_citation_rate(self):
        """MM papers: 4 cites / 3 papers = 1.33; WW: 2/2 = 1.0"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            create_test_db(db_path)
            from scripts.gender.analyze_citations import compute_gender_citation_rates
            rates = compute_gender_citation_rates(db_path, journal_id=1, year=2023)
            assert rates["MM"]["papers"] == 3
            assert rates["MM"]["citations"] == 4
            assert abs(rates["MM"]["rate"] - 4/3) < 0.01
            assert rates["WW"]["papers"] == 2
            assert rates["WW"]["citations"] == 2
            assert abs(rates["WW"]["rate"] - 1.0) < 0.01
            assert rates["WM"]["papers"] == 2
            assert rates["WM"]["citations"] == 2
            assert rates["MW"]["papers"] == 1
            assert rates["MW"]["citations"] == 1
        finally:
            os.unlink(db_path)

    def test_empty_year(self):
        """No papers in 2020 should return zero counts."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            create_test_db(db_path)
            from scripts.gender.analyze_citations import compute_gender_citation_rates
            rates = compute_gender_citation_rates(db_path, journal_id=1, year=2020)
            for pair in ["WW", "WM", "MW", "MM"]:
                assert rates[pair]["papers"] == 0
                assert rates[pair]["citations"] == 0
        finally:
            os.unlink(db_path)
