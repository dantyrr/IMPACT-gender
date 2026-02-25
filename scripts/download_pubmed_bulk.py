#!/usr/bin/env python3
"""
Download the full PubMed baseline and build data/pubmed_bulk.db.

Streams each .xml.gz file (no disk storage needed for source files), parses
the XML in memory, and inserts into a local SQLite database.

Fields stored per record:
    pmid, issn, pub_year, pub_month, title,
    first_author, first_affil, last_author, last_affil, pub_type, doi

After this runs, fix_citation_months.py will use pubmed_bulk.db as a fast
local lookup instead of querying PubMed ESummary for every citing PMID.

Runtime: ~2-4 hours for ~1,300 files. Safe to interrupt and resume.

Usage:
    python scripts/download_pubmed_bulk.py
    python scripts/download_pubmed_bulk.py --limit 5     # test with 5 files
    python scripts/download_pubmed_bulk.py --updatefiles  # also process updatefiles/
"""

import sys
import os
import re
import gzip
import io
import sqlite3
import logging
import time
import argparse
import xml.etree.ElementTree as ET
from typing import Optional, List, Tuple

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BULK_DB_PATH = os.path.join(_REPO_ROOT, "data", "pubmed_bulk.db")
BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pubmed_bulk")


# ---- Database ----

def open_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-131072")   # 128 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pubmed (
            pmid         INTEGER PRIMARY KEY,
            issn         TEXT,
            pub_year     INTEGER,
            pub_month    INTEGER,
            title        TEXT,
            first_author TEXT,
            first_affil  TEXT,
            last_author  TEXT,
            last_affil   TEXT,
            pub_type     TEXT,
            doi          TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pubmed_issn
            ON pubmed(issn);
        CREATE INDEX IF NOT EXISTS idx_pubmed_year
            ON pubmed(pub_year, pub_month);
        CREATE TABLE IF NOT EXISTS processed_files (
            filename      TEXT PRIMARY KEY,
            processed_at  TEXT NOT NULL,
            record_count  INTEGER NOT NULL
        );
    """)
    conn.commit()
    return conn


def get_processed(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT filename FROM processed_files").fetchall()
    return {r[0] for r in rows}


def mark_processed(conn: sqlite3.Connection, filename: str, count: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO processed_files (filename, processed_at, record_count) "
        "VALUES (?, datetime('now'), ?)",
        (filename, count),
    )
    conn.commit()


# ---- XML Parsing ----

def _parse_month(text: str) -> Optional[int]:
    if not text:
        return None
    text = text.strip()
    if text.isdigit():
        v = int(text)
        return v if 1 <= v <= 12 else None
    # Handle "Jan-Feb" → "Jan"
    return MONTH_MAP.get(text.split("-")[0][:3])


def _get_date(article_elem) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract (year, month) from an Article element.
    Prefer ArticleDate (electronic pub) over JournalIssue/PubDate.
    """
    # 1. Electronic pub date (most precise — when paper was first citable)
    art_date = article_elem.find("ArticleDate[@DateType='Electronic']")
    if art_date is not None:
        y = art_date.findtext("Year")
        if y and y.isdigit():
            return int(y), _parse_month(art_date.findtext("Month", ""))

    # 2. Journal issue pub date
    ji = article_elem.find("Journal/JournalIssue/PubDate")
    if ji is not None:
        y = ji.findtext("Year")
        if y and y.isdigit():
            return int(y), _parse_month(ji.findtext("Month", ""))
        # MedlineDate fallback e.g. "2023 Jan-Feb"
        ml = ji.findtext("MedlineDate", "")
        if ml:
            parts = ml.strip().split()
            if parts and parts[0].isdigit():
                month = _parse_month(parts[1]) if len(parts) > 1 else None
                return int(parts[0]), month

    return None, None


def _get_author(elem) -> Tuple[Optional[str], Optional[str]]:
    """Return (name, affiliation) for one Author element."""
    last = (elem.findtext("LastName") or "").strip()
    fore = (elem.findtext("ForeName") or "").strip()
    if last:
        name = f"{last}, {fore}".strip(", ")
    else:
        name = (elem.findtext("CollectiveName") or "").strip() or None

    affil_el = elem.find("AffiliationInfo/Affiliation")
    affil = (affil_el.text or "").strip()[:500] if affil_el is not None else None

    return name or None, affil or None


def extract_article(article_elem) -> Optional[dict]:
    """Parse one PubmedArticle element into a flat dict."""
    mc = article_elem.find("MedlineCitation")
    if mc is None:
        return None

    pmid_el = mc.find("PMID")
    if pmid_el is None or not pmid_el.text:
        return None
    try:
        pmid = int(pmid_el.text.strip())
    except ValueError:
        return None

    art = mc.find("Article")
    if art is None:
        return None

    # ISSN — ISSNLinking is most reliable (same as what ESearch uses)
    issn = None
    mji = mc.find("MedlineJournalInfo")
    if mji is not None:
        issn = (mji.findtext("ISSNLinking") or "").strip() or None
    if not issn:
        journal = art.find("Journal")
        if journal is not None:
            for issn_type in ("Electronic", "Print"):
                el = journal.find(f"ISSN[@IssnType='{issn_type}']")
                if el is not None and el.text:
                    issn = el.text.strip()
                    break

    # Dates
    pub_year, pub_month = _get_date(art)
    if not pub_year:
        return None  # skip records with no date at all

    # Title
    title_el = art.find("ArticleTitle")
    title = ""
    if title_el is not None:
        # Flatten any nested XML tags (e.g. <i>, <sub>) to plain text
        title = "".join(title_el.itertext()).strip().rstrip(".")[:500]

    # Authors
    author_list = art.find("AuthorList")
    authors = author_list.findall("Author") if author_list is not None else []
    first_author = first_affil = last_author = last_affil = None
    if authors:
        first_author, first_affil = _get_author(authors[0])
        if len(authors) > 1:
            last_author, last_affil = _get_author(authors[-1])
        else:
            last_author, last_affil = first_author, first_affil

    # Publication type
    pub_type = None
    ptl = art.find("PublicationTypeList")
    if ptl is not None:
        types = [pt.text for pt in ptl.findall("PublicationType") if pt.text]
        if any(t in ("Review", "Systematic Review") for t in types):
            pub_type = "Review"
        elif "Journal Article" in types:
            pub_type = "Journal Article"
        elif types:
            pub_type = types[0]

    # DOI (in PubmedData, sibling of MedlineCitation)
    doi = None
    pd_data = article_elem.find("PubmedData")
    if pd_data is not None:
        for ai in pd_data.findall("ArticleIdList/ArticleId"):
            if ai.get("IdType") == "doi" and ai.text:
                doi = ai.text.strip()[:200]
                break

    return {
        "pmid":         pmid,
        "issn":         issn,
        "pub_year":     pub_year,
        "pub_month":    pub_month if pub_month is not None else 6,
        "title":        title,
        "first_author": first_author,
        "first_affil":  first_affil,
        "last_author":  last_author,
        "last_affil":   last_affil,
        "pub_type":     pub_type,
        "doi":          doi,
    }


def process_gz_bytes(data: bytes, conn: sqlite3.Connection) -> int:
    """Parse gzipped PubMed XML bytes and bulk-insert into DB. Returns count."""
    records = []
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        for event, elem in ET.iterparse(gz, events=("end",)):
            if elem.tag == "PubmedArticle":
                rec = extract_article(elem)
                if rec:
                    records.append(rec)
                elem.clear()

    if records:
        conn.executemany(
            """INSERT OR REPLACE INTO pubmed
               (pmid, issn, pub_year, pub_month, title,
                first_author, first_affil, last_author, last_affil, pub_type, doi)
               VALUES
               (:pmid, :issn, :pub_year, :pub_month, :title,
                :first_author, :first_affil, :last_author, :last_affil, :pub_type, :doi)""",
            records,
        )
        conn.commit()
    return len(records)


# ---- Download ----

def list_files(base_url: str, session: requests.Session) -> List[str]:
    """Return sorted list of .xml.gz filenames from an NCBI directory listing."""
    resp = session.get(base_url, timeout=30)
    resp.raise_for_status()
    filenames = re.findall(r'href="(pubmed\d+n\d+\.xml\.gz)"', resp.text)
    return sorted(set(filenames))


def download_file(base_url: str, filename: str,
                  session: requests.Session, retries: int = 3) -> bytes:
    """Download one .xml.gz file, return raw bytes. Retries on error."""
    url = base_url + filename
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=600, stream=True)
            resp.raise_for_status()
            buf = io.BytesIO()
            for chunk in resp.iter_content(chunk_size=131072):  # 128 KB chunks
                buf.write(chunk)
            return buf.getvalue()
        except Exception as e:
            if attempt == retries:
                raise
            wait = 2 ** attempt
            logger.warning(f"  Download failed ({e}), retry {attempt}/{retries} in {wait}s")
            time.sleep(wait)


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0,
                        help="Process at most N files (0 = all; use for testing)")
    parser.add_argument("--updatefiles", action="store_true",
                        help="After baseline, also process pubmed/updatefiles/")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = "IMPACT/1.0 (dantyrr@gmail.com)"

    conn = open_db(BULK_DB_PATH)
    processed = get_processed(conn)
    existing = conn.execute("SELECT COUNT(*) FROM pubmed").fetchone()[0]
    logger.info(f"Output DB: {BULK_DB_PATH}")
    logger.info(f"DB already has {existing:,} records, {len(processed)} files processed")

    # Collect files to process
    logger.info("Fetching baseline file list...")
    files = list_files(BASE_URL, session)
    if args.updatefiles:
        logger.info("Fetching updatefiles list...")
        files += list_files(UPDATE_URL, session)

    todo = [f for f in files if f not in processed]
    if args.limit:
        todo = todo[: args.limit]

    logger.info(f"{len(files)} total files, {len(processed)} done, {len(todo)} to process")
    if not todo:
        logger.info("Nothing to do.")
        conn.close()
        return

    wall_start = time.time()
    total_records = 0

    for idx, filename in enumerate(todo, 1):
        t0 = time.time()
        try:
            base = BASE_URL if not filename.startswith("pubmed") or "updatefiles" not in filename else UPDATE_URL
            data = download_file(BASE_URL, filename, session)
            count = process_gz_bytes(data, conn)
            mark_processed(conn, filename, count)
            total_records += count

            elapsed = time.time() - t0
            done_frac = idx / len(todo)
            wall_elapsed = time.time() - wall_start
            eta_sec = (wall_elapsed / done_frac) * (1 - done_frac) if done_frac > 0 else 0
            mb = len(data) / 1024 ** 2

            logger.info(
                f"[{idx:4d}/{len(todo)}] {filename}  "
                f"{count:,} records  {mb:.0f} MB  {elapsed:.1f}s  "
                f"| total={total_records:,}  ETA {eta_sec/3600:.1f}h"
            )

        except Exception as e:
            logger.error(f"[{idx}/{len(todo)}] {filename}: {e}")
            time.sleep(10)

    final = conn.execute("SELECT COUNT(*) FROM pubmed").fetchone()[0]
    wall_total = time.time() - wall_start
    logger.info(f"\nFinished in {wall_total/3600:.1f}h")
    logger.info(f"DB has {final:,} records total")
    logger.info(f"Database: {BULK_DB_PATH}")

    db_size_gb = os.path.getsize(BULK_DB_PATH) / 1024 ** 3
    logger.info(f"DB file size: {db_size_gb:.1f} GB")

    conn.close()


if __name__ == "__main__":
    main()
