#!/usr/bin/env python3
"""
Quick check: what does iCite actually return for sample 2023 Aging Cell papers?
This diagnoses whether iCite has cited_by data for newer papers.

Run: python scripts/check_icite_coverage.py
"""
import sys
import os
import sqlite3
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline.config import DB_PATH, ICITE_BASE_URL

print(f"Checking iCite coverage for 2023+ papers...\n")

conn = sqlite3.connect(DB_PATH)

# Get 10 sample PMIDs from each year 2022-2025
for year in [2022, 2023, 2024, 2025]:
    rows = conn.execute(
        "SELECT pmid FROM papers WHERE journal_id=1 AND pub_year=? LIMIT 10",
        (year,)
    ).fetchall()
    pmids = [r[0] for r in rows]
    if not pmids:
        print(f"Year {year}: no papers in DB")
        continue

    pmid_str = ",".join(str(p) for p in pmids)
    try:
        resp = requests.get(
            ICITE_BASE_URL,
            params={"pmids": pmid_str, "format": "json"},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data", [])

        total_cited_by = 0
        has_cited_by = 0
        for rec in records:
            cb = rec.get("cited_by", [])
            if isinstance(cb, str):
                cb = [x for x in cb.split() if x]
            n = len(cb)
            total_cited_by += n
            if n > 0:
                has_cited_by += 1

        print(f"Year {year}: queried {len(pmids)} papers, got {len(records)} records from iCite")
        print(f"  Papers with non-empty cited_by: {has_cited_by}/{len(records)}")
        print(f"  Total cited_by entries: {total_cited_by}")
        if records:
            sample = records[0]
            cb = sample.get("cited_by", [])
            if isinstance(cb, str):
                cb = cb.split()
            print(f"  Sample PMID {sample.get('pmid')}: citation_count={sample.get('citation_count')}, cited_by_count={len(cb)}")
    except Exception as e:
        print(f"Year {year}: API error: {e}")
    print()

conn.close()
