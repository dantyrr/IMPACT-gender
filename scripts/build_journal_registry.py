#!/usr/bin/env python3
"""
Build a registry of all PubMed journals eligible for IMPACT processing.

Downloads NLM's J_Medline.txt (the authoritative list of all PubMed journals)
and cross-references with pubmed_bulk.db to find journals with enough papers.

Output: data/journal_registry.json
  [{issn, name, abbreviation, slug, paper_count}, ...]
  Sorted by paper_count descending (most important journals first).

Usage:
    python scripts/build_journal_registry.py
    python scripts/build_journal_registry.py --min-papers 500
"""

import sys
import os
import re
import json
import logging
import sqlite3
import argparse
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline.config import PUBMED_BULK_DB_PATH

NLM_URL   = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt"
OUTPUT    = Path("data/journal_registry.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_registry")

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dX]$")


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def parse_nlm(text: str) -> dict:
    """Parse J_Medline.txt → {issn: {name, abbr}} for all print + online ISSNs."""
    result = {}
    cur = {}

    def flush(cur):
        if not cur.get("name"):
            return
        for issn in [cur.get("issn_p"), cur.get("issn_e")]:
            if issn and ISSN_RE.match(issn) and issn not in result:
                result[issn] = {
                    "name": cur["name"],
                    "abbr": cur.get("abbr", ""),
                }

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("---"):
            flush(cur)
            cur = {}
        elif line.startswith("JournalTitle:"):
            cur["name"] = line.split(":", 1)[1].strip()
        elif line.startswith("MedAbbr:"):
            cur["abbr"] = line.split(":", 1)[1].strip()
        elif line.startswith("ISSN (Print):"):
            v = line.split(":", 1)[1].strip()
            if v:
                cur["issn_p"] = v
        elif line.startswith("ISSN (Online):"):
            v = line.split(":", 1)[1].strip()
            if v:
                cur["issn_e"] = v
    flush(cur)
    return result


def get_paper_counts(min_year: int = 2010) -> dict:
    """Return {issn: count} for papers published >= min_year in pubmed_bulk.db."""
    conn = sqlite3.connect(f"file:{PUBMED_BULK_DB_PATH}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT issn, COUNT(*) FROM pubmed "
        "WHERE pub_year >= ? AND issn IS NOT NULL AND issn != '' "
        "GROUP BY issn",
        (min_year,),
    ).fetchall()
    conn.close()
    return {issn: cnt for issn, cnt in rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-papers", type=int, default=100,
                        help="Minimum papers since 2010 to include (default: 100)")
    args = parser.parse_args()

    logger.info(f"Downloading NLM journal list from {NLM_URL}...")
    r = requests.get(NLM_URL, timeout=120)
    r.raise_for_status()
    nlm = parse_nlm(r.text)
    logger.info(f"  {len(nlm):,} ISSNs parsed from NLM list")

    logger.info("Counting papers per ISSN in pubmed_bulk.db (2010–present)...")
    counts = get_paper_counts()
    logger.info(f"  {len(counts):,} distinct ISSNs in bulk DB")

    # Build registry — merge NLM metadata + paper counts
    registry = []
    slugs_seen: dict = {}  # slug → issn

    for issn, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        if cnt < args.min_papers:
            continue
        if issn not in nlm:
            continue  # no name available from NLM

        meta = nlm[issn]
        base_slug = slugify(meta["name"])

        # Disambiguate slug collisions (different journals, same slug)
        slug = base_slug
        if slug in slugs_seen and slugs_seen[slug] != issn:
            slug = f"{base_slug}-{issn.replace('-', '')}"
        slugs_seen[slug] = issn

        registry.append({
            "issn":         issn,
            "name":         meta["name"],
            "abbreviation": meta["abbr"] or meta["name"],
            "slug":         slug,
            "paper_count":  cnt,
        })

    logger.info(f"Registry: {len(registry):,} journals with ≥{args.min_papers} papers")

    buckets = {"≥10,000": 0, "5,000–9,999": 0, "1,000–4,999": 0, "100–999": 0}
    for j in registry:
        c = j["paper_count"]
        if c >= 10000:   buckets["≥10,000"] += 1
        elif c >= 5000:  buckets["5,000–9,999"] += 1
        elif c >= 1000:  buckets["1,000–4,999"] += 1
        else:            buckets["100–999"] += 1
    for k, v in buckets.items():
        logger.info(f"  {k} papers: {v:,} journals")

    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(registry, f)
    logger.info(f"Written to {OUTPUT}")


if __name__ == "__main__":
    main()
