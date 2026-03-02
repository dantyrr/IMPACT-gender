#!/usr/bin/env python3
"""
IMPACT Weekly Update

Incrementally adds papers published in the last N days and the citations
they create, then updates affected journals' JSON snapshots.

Three phases:
  1. Discover new PubMed papers via ESearch → add to impact.db
  2. Fetch iCite for new papers → get their reference lists → cross-reference
     against papers already in the DB → add citation events
  3. Recompute current month's rolling IF for affected journals → update JSONs

Usage:
    python scripts/run_weekly_update.py              # last 10 days (default)
    python scripts/run_weekly_update.py --days 14    # last 14 days
    python scripts/run_weekly_update.py --dry-run    # show plan without changes
    python scripts/run_weekly_update.py --skip-snapshots  # phases 1-2 only
"""

import sys
import os
import json
import time
import logging
import argparse
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.pipeline.config import (
    DB_PATH, PUBMED_BASE_URL, PUBMED_API_KEY, PUBMED_EMAIL,
    ICITE_BASE_URL, ICITE_RATE_LIMIT, PUBMED_RATE_LIMIT,
    WEBSITE_DATA_DIR,
)
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.impact_calculator import ImpactCalculator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("weekly")


# ------------------------------------------------------------------ #
#  Phase 1: Discover new papers from PubMed
# ------------------------------------------------------------------ #

def esearch_recent(days: int) -> list:
    """Search PubMed for all papers added in the last N days.
    Splits into per-day queries to stay under the 9999-result ESearch limit.
    Returns list of integer PMIDs (deduplicated)."""
    today = datetime.now()
    all_pmids = set()

    for d in range(days):
        day = today - timedelta(days=d)
        day_str = day.strftime("%Y/%m/%d")
        term = f'"{day_str}"[EDAT]'

        params = {
            "db": "pubmed",
            "term": term,
            "retmax": 9999,
            "retmode": "xml",
        }
        if PUBMED_API_KEY:
            params["api_key"] = PUBMED_API_KEY
        if PUBMED_EMAIL:
            params["email"] = PUBMED_EMAIL

        resp = requests.get(
            f"{PUBMED_BASE_URL}/esearch.fcgi", params=params, timeout=60
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text)

        count = int(root.findtext("Count", "0"))
        ids = [int(el.text) for el in root.findall(".//IdList/Id")]
        all_pmids.update(ids)

        logger.info(f"  {day_str}: {len(ids):,} PMIDs (total {count:,})")

        if count > 9999:
            logger.warning(
                f"  Day {day_str} has {count:,} papers (>9999) — "
                f"some may be missed. Consider using --days with smaller range."
            )

        time.sleep(1.0 / PUBMED_RATE_LIMIT)

    return list(all_pmids)


def esummary_batch(pmids: list, batch_size: int = 200) -> list:
    """Fetch PubMed ESummary for a list of PMIDs.
    Returns list of parsed paper dicts with ISSN, title, date, type, DOI."""
    papers = []

    for i in range(0, len(pmids), batch_size):
        chunk = pmids[i:i + batch_size]
        params = {
            "db": "pubmed",
            "id": ",".join(str(p) for p in chunk),
            "retmode": "json",
        }
        if PUBMED_API_KEY:
            params["api_key"] = PUBMED_API_KEY

        try:
            resp = requests.get(
                f"{PUBMED_BASE_URL}/esummary.fcgi", params=params, timeout=120
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"  ESummary error for batch starting at {i}: {e}")
            continue

        result = data.get("result", {})
        for uid in result.get("uids", []):
            article = result.get(uid, {})
            if not article or "error" in article:
                continue
            parsed = _parse_esummary(article)
            if parsed:
                papers.append(parsed)

        done = i + len(chunk)
        if done % 2000 < batch_size or done >= len(pmids):
            logger.info(f"  ESummary: {len(papers):,} parsed ({done:,}/{len(pmids):,})")

        time.sleep(1.0 / PUBMED_RATE_LIMIT)

    return papers


def _parse_esummary(article: dict):
    """Parse an ESummary article into a paper dict."""
    pmid = int(article.get("uid", 0))
    if not pmid:
        return None

    issn = article.get("issn", "").strip()
    essn = article.get("essn", "").strip()

    # Parse publication date
    pubdate = article.get("sortpubdate", "") or article.get("pubdate", "")
    pub_year, pub_month = _parse_date(pubdate)
    if not pub_year:
        return None

    # Article type
    pubtypes = article.get("pubtype", [])
    pub_type = pubtypes[0].lower() if pubtypes else "journal article"

    # DOI
    doi = ""
    elocationid = article.get("elocationid", "")
    if elocationid.startswith("doi:"):
        doi = elocationid[4:].strip()
    else:
        for aid in article.get("articleids", []):
            if aid.get("idtype") == "doi":
                doi = aid.get("value", "")
                break

    return {
        "pmid": pmid,
        "issn": issn,
        "essn": essn,
        "title": article.get("title", "") or "",
        "pub_year": pub_year,
        "pub_month": pub_month,
        "pub_date": f"{pub_year}-{pub_month:02d}-01",
        "pub_type": pub_type,
        "is_research": 1 if pub_type == "journal article" else 0,
        "doi": doi,
    }


MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date(datestr: str):
    """Parse PubMed date string. Returns (year, month)."""
    if not datestr:
        return 0, 0

    # sortpubdate: "2024/03/15 00:00"
    m = re.match(r"(\d{4})/(\d{2})", datestr)
    if m:
        return int(m.group(1)), int(m.group(2))

    # pubdate: "2024 Mar 15" or "2024 Mar"
    m = re.match(r"(\d{4})\s+(\w{3})", datestr)
    if m:
        yr = int(m.group(1))
        mo = MONTHS.get(m.group(2).lower(), 1)
        return yr, mo

    # Year only: "2024"
    m = re.match(r"(\d{4})", datestr)
    if m:
        return int(m.group(1)), 1

    return 0, 0


# ------------------------------------------------------------------ #
#  Phase 2: Citation events via iCite references
# ------------------------------------------------------------------ #

def fetch_icite_references(pmids: list, batch_size: int = 200) -> dict:
    """Fetch iCite for PMIDs. Returns {pmid: {"references": [...], "is_research": bool}}.
    Papers not yet indexed by iCite will be missing from the result."""
    results = {}
    min_interval = 1.0 / ICITE_RATE_LIMIT
    last_req = 0.0

    for i in range(0, len(pmids), batch_size):
        chunk = pmids[i:i + batch_size]
        chunk_str = ",".join(str(p) for p in chunk)

        # Rate limit
        elapsed = time.time() - last_req
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        last_req = time.time()

        try:
            resp = requests.get(
                ICITE_BASE_URL,
                params={"pmids": chunk_str, "format": "json"},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"  iCite error for batch starting at {i}: {e}")
            continue

        for record in data.get("data", []):
            pmid = record.get("pmid")
            if pmid is None:
                continue
            pmid = int(pmid)

            refs_raw = record.get("references", [])
            if isinstance(refs_raw, str):
                refs = [int(x) for x in refs_raw.split() if x.strip()]
            elif isinstance(refs_raw, list):
                refs = [int(x) for x in refs_raw if x]
            else:
                refs = []

            results[pmid] = {
                "references": refs,
                "is_research": bool(record.get("is_research_article")),
            }

        done = i + len(chunk)
        if done % 2000 < batch_size or done >= len(pmids):
            logger.info(
                f"  iCite: {len(results):,} records ({done:,}/{len(pmids):,})"
            )

    return results


# ------------------------------------------------------------------ #
#  Phase 3: Update snapshots for affected journals
# ------------------------------------------------------------------ #

def format_snapshot(s: dict) -> dict:
    """Format a raw compute_rolling_if() result for the JSON timeseries."""
    return {
        "month": s["month"],
        "rolling_if": s["rolling_if"],
        "rolling_if_no_reviews": s["rolling_if_no_reviews"],
        "papers": s["paper_count"],
        "research": s.get("research_count", 0),
        "reviews": s["review_count"],
        "citations": s["citation_count"],
        "by_type": s.get("by_type", {}),
    }


def update_journal_snapshot(db, calc, journal_id, slug, name, issn,
                            now, data_dir):
    """Recompute current month and update the journal's JSON file.
    Returns the raw 24-month snapshot dict, or None if no existing JSON."""
    journals_dir = os.path.join(data_dir, "journals")
    filepath = os.path.join(journals_dir, f"{slug}.json")

    if not os.path.exists(filepath):
        # No existing file — can't do partial update.
        # This journal needs a full compute_snapshots.py run.
        return None

    with open(filepath) as f:
        existing = json.load(f)

    current_month_str = f"{now.year}-{now.month:02d}"

    # Compute current month for all 3 window variants
    variants = [
        ("timeseries",      24,  0),
        ("timeseries_12mo", 12,  0),
        ("timeseries_5yr",  60, 12),
    ]

    raw_snapshots = {}
    for key, window, skip in variants:
        raw_snapshots[key] = calc.compute_rolling_if(
            journal_id, now.year, now.month,
            paper_window_months=window, paper_skip_months=skip,
        )

    # Update each timeseries in the JSON
    for key, _, _ in variants:
        ts = existing.get(key, [])
        new_entry = format_snapshot(raw_snapshots[key])

        if ts and ts[-1]["month"] == current_month_str:
            ts[-1] = new_entry
        else:
            ts.append(new_entry)
        existing[key] = ts

    # Update "latest" block
    latest = raw_snapshots["timeseries"]
    existing["latest"] = {
        "month": latest["month"],
        "rolling_if": latest["rolling_if"],
        "rolling_if_no_reviews": latest["rolling_if_no_reviews"],
        "paper_count": latest["paper_count"],
        "research_count": latest.get("research_count", 0),
        "review_count": latest["review_count"],
        "citation_count": latest["citation_count"],
    }
    existing["last_updated"] = now.strftime("%Y-%m-%d")

    os.makedirs(journals_dir, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(existing, f, indent=2)

    return raw_snapshots["timeseries"]


def update_index(data_dir, dirty_journals_data):
    """Patch index.json with updated latest_if for dirty journals."""
    index_path = os.path.join(data_dir, "index.json")
    if not os.path.exists(index_path):
        logger.warning("index.json not found — skipping")
        return

    with open(index_path) as f:
        index = json.load(f)

    slug_to_idx = {
        j["slug"]: i for i, j in enumerate(index.get("journals", []))
    }

    updated = 0
    for slug, snapshot in dirty_journals_data.items():
        if slug in slug_to_idx:
            entry = index["journals"][slug_to_idx[slug]]
            entry["latest_if"] = snapshot["rolling_if"]
            entry["latest_if_no_reviews"] = snapshot["rolling_if_no_reviews"]
            entry["paper_count"] = snapshot["paper_count"]
            entry["latest_month"] = snapshot["month"]
            updated += 1

    index["generated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)

    logger.info(f"  Updated {updated:,} entries in index.json")


# ------------------------------------------------------------------ #
#  Main
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--days", type=int, default=10,
        help="Look back N days for new papers (default: 10)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show plan without making changes",
    )
    parser.add_argument(
        "--skip-snapshots", action="store_true",
        help="Run phases 1-2 only (add papers + citations, skip JSON updates)",
    )
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"IMPACT Weekly Update — {start_time.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"Looking back {args.days} days")

    # ==================================================================
    #  PHASE 1: Discover new PubMed papers
    # ==================================================================
    logger.info("=" * 60)
    logger.info("PHASE 1: Discovering new PubMed papers")

    all_pmids = esearch_recent(args.days)
    logger.info(f"  Total from ESearch: {len(all_pmids):,} PMIDs")

    if not all_pmids:
        logger.info("No papers found. Done.")
        return

    # Filter out PMIDs already in our DB
    db = DatabaseManager(DB_PATH)
    existing_pmids = set()
    for i in range(0, len(all_pmids), 5000):
        chunk = all_pmids[i:i + 5000]
        placeholders = ",".join("?" * len(chunk))
        rows = db.conn.execute(
            f"SELECT pmid FROM papers WHERE pmid IN ({placeholders})", chunk
        ).fetchall()
        existing_pmids.update(r[0] for r in rows)

    new_pmids = [p for p in all_pmids if p not in existing_pmids]
    logger.info(
        f"  {len(existing_pmids):,} already in DB, "
        f"{len(new_pmids):,} new"
    )

    if args.dry_run:
        logger.info(f"[DRY RUN] Would fetch metadata for {len(new_pmids):,} papers")
        db.close()
        return

    # Build ISSN → journal lookup from all journals in the DB
    all_journals = db.get_all_journals()
    issn_lookup = {}  # ISSN → journal dict
    for j in all_journals:
        issn_lookup[j["issn"]] = j
    journal_by_id = {j["id"]: j for j in all_journals}

    dirty_journal_ids = set()
    # Keep metadata in memory for Phase 2 (even unmatched papers cite
    # tracked papers, so we need their pub dates for citation events)
    pmid_meta = {}  # pmid → {pub_year, pub_month, ...}

    if new_pmids:
        logger.info(f"  Fetching ESummary for {len(new_pmids):,} papers...")
        papers_meta = esummary_batch(new_pmids)
        logger.info(f"  Parsed {len(papers_meta):,} papers from ESummary")

        # Store all metadata for Phase 2
        for p in papers_meta:
            pmid_meta[p["pmid"]] = p

        # Match ISSN to journals and insert
        matched = []
        unmatched_count = 0

        for p in papers_meta:
            journal = issn_lookup.get(p["issn"]) or issn_lookup.get(p["essn"])
            if journal:
                p["journal_id"] = journal["id"]
                matched.append(p)
                dirty_journal_ids.add(journal["id"])
            else:
                unmatched_count += 1

        logger.info(
            f"  {len(matched):,} matched to tracked journals, "
            f"{unmatched_count:,} from untracked journals"
        )

        if matched:
            db.add_papers_bulk(matched)
            logger.info(f"  Inserted {len(matched):,} new papers into DB")
    else:
        logger.info("  No new papers to fetch")

    # ==================================================================
    #  PHASE 2: Citation events from new papers' reference lists
    # ==================================================================
    logger.info("=" * 60)
    logger.info("PHASE 2: Fetching iCite references for new papers")

    if not new_pmids:
        logger.info("  No new papers — skipping iCite")
    else:
        logger.info(f"  Fetching iCite for {len(new_pmids):,} papers...")
        icite_data = fetch_icite_references(new_pmids)

        n_missing = len(new_pmids) - len(icite_data)
        logger.info(
            f"  iCite returned data for {len(icite_data):,} papers "
            f"({n_missing:,} not yet indexed — will be caught next run)"
        )

        # Update is_research flag from iCite (more reliable than PubMed pubtype)
        updates = []
        for pmid, record in icite_data.items():
            is_res = 1 if record.get("is_research") else 0
            updates.append((is_res, pmid))
        if updates:
            db.conn.executemany(
                "UPDATE papers SET is_research = ? WHERE pmid = ?", updates
            )
            db.conn.commit()

        # Collect all referenced PMIDs across all new papers
        all_refs = set()
        paper_refs = {}  # citing_pmid → [referenced_pmids]
        for pmid, record in icite_data.items():
            refs = record.get("references", [])
            if refs:
                paper_refs[pmid] = refs
                all_refs.update(refs)

        logger.info(
            f"  {len(all_refs):,} unique referenced PMIDs "
            f"across {len(paper_refs):,} papers"
        )

        # Find which referenced PMIDs exist in our papers table
        # (these are the papers that gain a new citation)
        tracked_refs = {}  # pmid → journal_id
        ref_list = list(all_refs)
        for i in range(0, len(ref_list), 5000):
            chunk = ref_list[i:i + 5000]
            placeholders = ",".join("?" * len(chunk))
            rows = db.conn.execute(
                f"SELECT pmid, journal_id FROM papers "
                f"WHERE pmid IN ({placeholders})",
                chunk,
            ).fetchall()
            for r in rows:
                tracked_refs[r[0]] = r[1]

        logger.info(
            f"  {len(tracked_refs):,} referenced papers are in tracked journals"
        )

        # Create citation events
        citation_events = []
        for citing_pmid, refs in paper_refs.items():
            # Get citing paper's pub date — from in-memory metadata if available,
            # otherwise from DB (for papers that matched a tracked journal)
            meta = pmid_meta.get(citing_pmid)
            if meta:
                citing_year = meta["pub_year"]
                citing_month = meta["pub_month"]
            else:
                paper = db.get_paper(citing_pmid)
                if paper:
                    citing_year = paper["pub_year"]
                    citing_month = paper["pub_month"] or 1
                else:
                    continue  # shouldn't happen

            citing_date = f"{citing_year}-{citing_month:02d}-01"

            for ref_pmid in refs:
                if ref_pmid in tracked_refs:
                    citation_events.append({
                        "cited_pmid": ref_pmid,
                        "citing_pmid": citing_pmid,
                        "citing_date": citing_date,
                        "citing_year": citing_year,
                        "citing_month": citing_month,
                    })
                    dirty_journal_ids.add(tracked_refs[ref_pmid])

        if citation_events:
            db.add_citations_bulk(citation_events)
            logger.info(f"  Added {len(citation_events):,} new citation events")
        else:
            logger.info("  No new citation events")

    # ==================================================================
    #  PHASE 3: Update snapshots for affected journals
    # ==================================================================
    if args.skip_snapshots:
        logger.info("=" * 60)
        logger.info("Skipping Phase 3 (--skip-snapshots)")
        db.close()
        _print_summary(start_time, new_pmids, dirty_journal_ids, {})
        return

    logger.info("=" * 60)
    logger.info(
        f"PHASE 3: Updating snapshots for "
        f"{len(dirty_journal_ids):,} affected journals"
    )

    if not dirty_journal_ids:
        logger.info("  No journals affected — nothing to update")
        db.close()
        _print_summary(start_time, new_pmids, dirty_journal_ids, {})
        return

    now = datetime.now()
    calc = ImpactCalculator(db)

    dirty_journals_data = {}  # slug → latest snapshot (for index update)
    skipped = 0

    for i, jid in enumerate(sorted(dirty_journal_ids)):
        journal = journal_by_id.get(jid)
        if not journal:
            continue

        slug = journal["slug"]
        name = journal["name"]
        issn = journal["issn"]

        result = update_journal_snapshot(
            db, calc, jid, slug, name, issn, now, WEBSITE_DATA_DIR,
        )

        if result:
            dirty_journals_data[slug] = result
        else:
            skipped += 1

        if (i + 1) % 500 == 0 or (i + 1) == len(dirty_journal_ids):
            logger.info(
                f"  Progress: {i + 1:,}/{len(dirty_journal_ids):,} journals"
            )

    logger.info(
        f"  Updated {len(dirty_journals_data):,} journal JSONs"
        + (f" (skipped {skipped} without existing JSON)" if skipped else "")
    )

    # Update index.json
    update_index(WEBSITE_DATA_DIR, dirty_journals_data)

    db.close()
    _print_summary(start_time, new_pmids, dirty_journal_ids, dirty_journals_data)


def _print_summary(start_time, new_pmids, dirty_journal_ids, dirty_journals_data):
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"Weekly update complete in {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    logger.info(f"  New papers added:     {len(new_pmids):,}")
    logger.info(f"  Journals affected:    {len(dirty_journal_ids):,}")
    logger.info(f"  Snapshots updated:    {len(dirty_journals_data):,}")
    logger.info("")
    logger.info("Next step: python scripts/upload_to_r2.py")


if __name__ == "__main__":
    main()
