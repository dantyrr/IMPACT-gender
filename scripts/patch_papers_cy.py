"""
Patch existing docs/data/papers/*.json files to add the 'cy' field
(citations by year) for each paper, sourced from the local DB.

This is much faster than rerunning compute_snapshots.py — it only
reads/writes the papers JSONs without touching journals/authors/snapshots.
"""

import json
import os
import sys
import glob
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DB_PATH = 'data/impact.db'
PAPERS_DIR = 'docs/data/papers'
WORKERS = 4


def patch_file(path):
    """Patch a single papers JSON file in-place."""
    import sqlite3

    with open(path) as f:
        data = json.load(f)

    papers = data.get('papers', [])
    if not papers:
        return os.path.basename(path), 0

    # Check if already patched (all papers have cy)
    if all('cy' in p for p in papers):
        return os.path.basename(path), -1  # -1 = skipped (already done)

    pmids = [p['pmid'] for p in papers]

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    placeholders = ','.join('?' * len(pmids))
    cursor = conn.cursor()
    cursor.execute(
        f"""SELECT cited_pmid, citing_year, COUNT(*) as cnt
            FROM citations
            WHERE cited_pmid IN ({placeholders})
              AND citing_year IS NOT NULL
            GROUP BY cited_pmid, citing_year""",
        pmids,
    )
    cy_map = {}
    for row in cursor.fetchall():
        pmid = str(row['cited_pmid'])
        year = str(row['citing_year'])
        cy_map.setdefault(pmid, {})[year] = row['cnt']
    conn.close()

    for p in papers:
        p['cy'] = cy_map.get(str(p['pmid']), {})

    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))

    return os.path.basename(path), len(papers)


def main():
    paths = sorted(glob.glob(os.path.join(PAPERS_DIR, '*.json')))
    total = len(paths)
    log.info(f"Patching {total} papers files with cy field ({WORKERS} workers)...")

    done = 0
    skipped = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(patch_file, p): p for p in paths}
        for fut in as_completed(futures):
            try:
                name, count = fut.result()
                if count == -1:
                    skipped += 1
                else:
                    done += 1
                if done % 500 == 0:
                    log.info(f"  {done}/{total} patched ({skipped} already had cy)...")
            except Exception as e:
                log.error(f"Error on {futures[fut]}: {e}")

    log.info(f"Done. {done} files patched, {skipped} already had cy field.")


if __name__ == '__main__':
    main()
