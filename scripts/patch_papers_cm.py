"""
Patch existing docs/data/papers/*.json files to add:
  - 'cm' field per paper (citations by month, {"YYYY-MM": count})
  - 'monthly_cits' top-level field (journal-wide monthly citation counts)

Much faster than rerunning compute_snapshots.py — only touches papers JSONs.
"""

import json
import os
import sys
import glob
import logging
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DB_PATH = 'data/impact.db'
PAPERS_DIR = 'docs/data/papers'
WORKERS = 4


def patch_file(path):
    """Patch a single papers JSON file in-place."""
    with open(path) as f:
        data = json.load(f)

    papers = data.get('papers', [])
    if not papers:
        return os.path.basename(path), 0

    # Check if already patched
    if all('cm' in p for p in papers) and 'monthly_cits' in data:
        return os.path.basename(path), -1  # skipped

    pmids = [p['pmid'] for p in papers]

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Per-paper citations by month (only for top-2000 papers in the file)
    placeholders = ','.join('?' * len(pmids))
    cursor.execute(
        f"""SELECT cited_pmid, citing_year, citing_month, COUNT(*) as cnt
            FROM citations
            WHERE cited_pmid IN ({placeholders})
              AND citing_year IS NOT NULL
              AND citing_month IS NOT NULL
            GROUP BY cited_pmid, citing_year, citing_month""",
        pmids,
    )
    cm_map = {}
    for row in cursor.fetchall():
        pmid = str(row['cited_pmid'])
        key = f"{row['citing_year']}-{row['citing_month']:02d}"
        cm_map.setdefault(pmid, {})[key] = row['cnt']

    # 2. Journal-wide monthly citations — get ALL journal pmids, then query citations
    cursor.execute("SELECT journal_id FROM papers WHERE pmid = ?", (pmids[0],))
    jrow = cursor.fetchone()
    journal_monthly = {}
    if jrow:
        journal_id = jrow['journal_id']
        # Get all PMIDs for this journal (not just top 2000)
        cursor.execute("SELECT pmid FROM papers WHERE journal_id = ?", (journal_id,))
        all_pmids = [r['pmid'] for r in cursor.fetchall()]

        # Batch query: monthly citation counts across all journal papers
        batch_size = 500
        monthly_counts = {}
        for i in range(0, len(all_pmids), batch_size):
            batch = all_pmids[i:i + batch_size]
            ph = ','.join('?' * len(batch))
            cursor.execute(
                f"""SELECT citing_year, citing_month, COUNT(*) as cnt
                    FROM citations
                    WHERE cited_pmid IN ({ph})
                      AND citing_year IS NOT NULL
                      AND citing_month IS NOT NULL
                    GROUP BY citing_year, citing_month""",
                batch,
            )
            for row in cursor.fetchall():
                key = f"{row['citing_year']}-{row['citing_month']:02d}"
                monthly_counts[key] = monthly_counts.get(key, 0) + row['cnt']

        journal_monthly = dict(sorted(monthly_counts.items()))

    conn.close()

    # Patch papers
    patched = 0
    for p in papers:
        cm = cm_map.get(str(p['pmid']))
        if cm:
            p['cm'] = cm
            patched += 1

    # Patch top-level
    if journal_monthly:
        data['monthly_cits'] = journal_monthly

    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))

    return os.path.basename(path), patched


def main():
    files = sorted(glob.glob(os.path.join(PAPERS_DIR, '*.json')))
    log.info(f"Found {len(files)} papers files to patch")

    if not files:
        log.warning("No files found. Check PAPERS_DIR.")
        return

    done = 0
    skipped = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(patch_file, f): f for f in files}
        for future in as_completed(futures):
            fname, count = future.result()
            if count == -1:
                skipped += 1
            else:
                done += 1
                if done % 100 == 0:
                    log.info(f"  Patched {done} files...")

    log.info(f"Done. Patched {done}, skipped {skipped} (already done)")


if __name__ == '__main__':
    main()
