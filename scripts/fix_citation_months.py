#!/usr/bin/env python3
"""
Fix citation months by fetching actual publication dates from PubMed ESummary.

The original pipeline defaulted all citation months to June (6) because iCite
does not return month-level data. PubMed ESummary provides epubdate and pubdate
with month precision for virtually all modern papers.

This script:
  1. Fetches all distinct citing PMIDs from the citations table
  2. Looks up their publication dates via PubMed ESummary
     (preferring epubdate — the online-first date — over pubdate)
  3. Updates citing_year, citing_month, and citing_date in the citations table
  4. Reports coverage statistics

Checkpoint support: progress is saved to a .json file every 50 batches so the
script can resume after an interruption (e.g. lost internet) without re-fetching
already-retrieved dates.

After this completes, re-run:  python scripts/compute_snapshots.py
"""

import sys
import os
import json
import sqlite3
import logging
import time
import requests
import argparse
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.config import DB_PATH, PUBMED_API_KEY, PUBMED_EMAIL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fix_months")

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

BATCH_SIZE = 500  # Use POST to avoid URL length limits
RATE_LIMIT = 10.0 if PUBMED_API_KEY else 3.0
SAVE_EVERY = 50   # Save checkpoint every N batches (~25K PMIDs)
_last_request = 0.0

CHECKPOINT_PATH = str(DB_PATH).replace(".db", "_month_fix_checkpoint.json")


# ---- Checkpoint helpers ----

def load_checkpoint() -> Tuple[Dict[int, Tuple[int, int]], int]:
    """Load saved progress. Returns (date_map, next_start_idx)."""
    if not os.path.exists(CHECKPOINT_PATH):
        return {}, 0
    try:
        with open(CHECKPOINT_PATH) as f:
            cp = json.load(f)
        date_map = {int(k): tuple(v) for k, v in cp["date_map"].items()}
        next_start = cp.get("next_start", 0)
        logger.info(
            f"Checkpoint loaded: resuming from PMID index {next_start:,} "
            f"({len(date_map):,} dates already fetched)"
        )
        return date_map, next_start
    except Exception as e:
        logger.warning(f"Could not load checkpoint ({e}), starting fresh")
        return {}, 0


def save_checkpoint(date_map: Dict[int, Tuple[int, int]], next_start: int):
    """Atomically save progress to disk."""
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(
            {
                "next_start": next_start,
                "date_map": {str(k): list(v) for k, v in date_map.items()},
            },
            f,
        )
    os.replace(tmp, CHECKPOINT_PATH)


# ---- PubMed helpers ----

def _wait():
    global _last_request
    elapsed = time.time() - _last_request
    wait = (1.0 / RATE_LIMIT) - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request = time.time()


def _base_params() -> Dict:
    params = {"tool": "IMPACT", "email": PUBMED_EMAIL, "retmode": "json"}
    if PUBMED_API_KEY:
        params["api_key"] = PUBMED_API_KEY
    return params


def parse_date(date_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse a PubMed date string like '2024 Jan 15', '2024 Mar', '2024'.
    Returns (year, month) where month is None if not present in the string
    (distinguishing "no month available" from "January").
    """
    if not date_str:
        return None, None
    parts = date_str.strip().split()
    if not parts:
        return None, None
    try:
        year = int(parts[0])
    except ValueError:
        return None, None

    month = None
    if len(parts) >= 2:
        m_part = parts[1].split("-")[0]   # handle "Jan-Feb" → "Jan"
        month = MONTH_MAP.get(m_part)
        if month is None:
            try:
                m_int = int(m_part)
                month = m_int if 1 <= m_int <= 12 else None
            except ValueError:
                pass
    return year, month


def fetch_dates_batch(pmids: List[int]) -> Dict[int, Tuple[int, int]]:
    """
    Fetch pub dates from PubMed ESummary for one batch of PMIDs.
    Returns {pmid: (year, month)} — month defaults to 6 only when
    neither epubdate nor pubdate contain a month.
    Returns {} on network error (will be retried on next run via checkpoint).
    """
    _wait()
    params = {
        **_base_params(),
        "db": "pubmed",
        "id": ",".join(str(p) for p in pmids),
    }
    try:
        resp = requests.post(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            data=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"PubMed request failed: {e}")
        return {}

    results = {}
    result = data.get("result", {})
    for pid_str in result.get("uids", []):
        article = result.get(pid_str, {})
        if not article or "error" in article:
            continue
        pmid = int(pid_str)

        epub_str = article.get("epubdate", "")
        pub_str  = article.get("pubdate", "")

        year, month = None, None

        # Prefer epubdate (online-first date — when the paper was citable)
        if epub_str:
            year, month = parse_date(epub_str)

        # If epubdate had no month, check pubdate for the month
        if year and month is None and pub_str:
            _, month = parse_date(pub_str)

        # If epubdate had no date at all, fall back entirely to pubdate
        if not year and pub_str:
            year, month = parse_date(pub_str)

        if year:
            # Only use 6 as last resort — it means "no month info available"
            results[pmid] = (year, month if month is not None else 6)

    return results


# ---- Main ----

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fill-missing", action="store_true",
        help="Only re-fetch PMIDs currently assigned month=6 (faster gap-fill)"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    cur = conn.cursor()
    if args.fill_missing:
        logger.info("--fill-missing: fetching only PMIDs currently at month=6...")
        cur.execute(
            "SELECT DISTINCT citing_pmid FROM citations "
            "WHERE citing_month = 6 ORDER BY citing_pmid"
        )
    else:
        logger.info("Fetching distinct citing PMIDs from database...")
        cur.execute("SELECT DISTINCT citing_pmid FROM citations ORDER BY citing_pmid")
    all_pmids = [row[0] for row in cur.fetchall()]
    total = len(all_pmids)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    # Load checkpoint if present
    date_map, start_idx = load_checkpoint()

    remaining = total - start_idx
    remaining_batches = (remaining + BATCH_SIZE - 1) // BATCH_SIZE
    eta_sec = remaining_batches / RATE_LIMIT

    logger.info(f"Found {total:,} unique citing PMIDs total")
    logger.info(
        f"Using {'API key (10 req/sec)' if PUBMED_API_KEY else 'no API key (3 req/sec)'}, "
        f"batch size {BATCH_SIZE}"
    )
    logger.info(
        f"Remaining: {remaining:,} PMIDs ({remaining_batches} batches, "
        f"~{eta_sec/60:.1f} min)"
    )

    # Fetch dates from PubMed, resuming from checkpoint
    for i in range(start_idx, total, BATCH_SIZE):
        batch = all_pmids[i: i + BATCH_SIZE]
        batch_result = fetch_dates_batch(batch)
        date_map.update(batch_result)

        batch_num = i // BATCH_SIZE + 1
        is_last = (i + BATCH_SIZE) >= total

        # Save checkpoint periodically
        if batch_num % SAVE_EVERY == 0 or is_last:
            save_checkpoint(date_map, i + BATCH_SIZE)

        if batch_num % 100 == 0 or is_last:
            pct = min(100, (i + BATCH_SIZE) / total * 100)
            logger.info(
                f"Batch {batch_num}/{total_batches} — "
                f"{len(date_map):,} dates fetched ({pct:.0f}%)"
            )

    with_real_month = sum(1 for _, m in date_map.values() if m != 6)
    logger.info(
        f"\nPubMed coverage: {len(date_map):,}/{total:,} PMIDs resolved "
        f"({len(date_map)/total*100:.1f}%)"
    )
    logger.info(
        f"Month precision: {with_real_month:,} ({with_real_month/len(date_map)*100:.1f}%) "
        f"have a real month — {len(date_map)-with_real_month:,} defaulted to June (no PubMed month)"
    )

    # Update citations table
    logger.info("\nUpdating citations table...")
    updates = [
        (year, month, f"{year}-{month:02d}-01", pmid)
        for pmid, (year, month) in date_map.items()
    ]

    chunk_size = 5000
    n_done = 0
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i: i + chunk_size]
        conn.executemany(
            """UPDATE citations
               SET citing_year = ?, citing_month = ?, citing_date = ?
               WHERE citing_pmid = ?""",
            chunk,
        )
        conn.commit()
        n_done += len(chunk)
        if n_done % 50000 == 0 or n_done == len(updates):
            logger.info(f"  {n_done:,} / {len(updates):,} PMID entries updated")

    # Report final month distribution
    cur.execute(
        "SELECT citing_month, COUNT(*) as cnt FROM citations "
        "GROUP BY citing_month ORDER BY citing_month"
    )
    rows = cur.fetchall()
    logger.info("\nMonth distribution after fix:")
    total_cites = sum(r[1] for r in rows)
    for month, cnt in rows:
        bar = "█" * (cnt * 40 // total_cites)
        logger.info(f"  Month {month:2d}: {cnt:7,}  {bar}")

    conn.close()

    # Clean up checkpoint — we're done
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)
        logger.info("Checkpoint file removed.")

    logger.info(
        "\nDone! Run the following to regenerate JSON files:\n"
        "  python scripts/compute_snapshots.py"
    )


if __name__ == "__main__":
    main()
