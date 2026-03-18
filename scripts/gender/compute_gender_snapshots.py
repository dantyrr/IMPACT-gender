"""
Compute monthly rolling 24-month citation rates by gender pair.

Uses the same formula as the main IMPACT rolling IF:
  For target month T:
    Citation window: 12 months ending at T (months T-11 through T)
    Paper window:    24 months ending 1 month before the citation window
                     (months T-36 through T-13)
    Rolling rate = citations_in_window / papers_in_window

This is computed separately for each gender pair (WW, WM, MW, MM)
to compare citation rates controlling for publication age.

Usage:
  python scripts/gender/compute_gender_snapshots.py                    # All journals + aggregate
  python scripts/gender/compute_gender_snapshots.py --aggregate-only   # Just aggregate
  python scripts/gender/compute_gender_snapshots.py --limit 100        # Top 100 journals
  python scripts/gender/compute_gender_snapshots.py --workers 4        # Parallel
"""
import argparse
import sqlite3
import json
import logging
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GENDER_PAIRS = ["WW", "WM", "MW", "MM"]


def _month_offset(year, month, offset):
    """Add offset months to a (year, month) pair. Returns (year, month)."""
    total = (year * 12 + month - 1) + offset
    return total // 12, total % 12 + 1


def _ym_key(year, month):
    """Convert (year, month) to a sortable string key like '2008-01'."""
    return f"{year}-{month:02d}"


def _ym_to_int(year, month):
    """Convert (year, month) to an integer YYYYMM for range comparisons."""
    return year * 100 + month


def _generate_target_months():
    """
    Generate list of (year, month) target months for computation.

    Earliest valid target needs paper window start >= START_YEAR-01:
      paper_start = target - 36 months >= (START_YEAR, 1)
      So target >= (START_YEAR + 3, 1) = (2008, 1)

    Latest target: (END_YEAR - 2, 12) to allow citation data to be reasonably complete.
    """
    start_y, start_m = START_YEAR + 3, 1   # 2008-01
    end_y, end_m = END_YEAR - 2, 12        # 2024-12

    targets = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        targets.append((y, m))
        y, m = _month_offset(y, m, 1)
    return targets


def compute_journal_rolling_if(conn, journal_id):
    """
    Compute monthly rolling 24-month citation rate by gender pair for one journal.

    Strategy: 2 SQL queries to load all paper counts and citation counts by
    (gender_pair, year, month), then assemble all target months in Python.

    Returns dict: {"2008-01": {"WW": {"p": N, "c": N, "if": float, "norm": float}, ...}, ...}
    """
    targets = _generate_target_months()
    if not targets:
        return {}

    # Determine the full range of pub years and citing years needed
    earliest_target = targets[0]
    latest_target = targets[-1]
    paper_start_y, _ = _month_offset(earliest_target[0], earliest_target[1], -36)
    paper_end_y, _ = _month_offset(latest_target[0], latest_target[1], -13)
    cite_start_y, _ = _month_offset(earliest_target[0], earliest_target[1], -11)
    cite_end_y, cite_end_m = latest_target

    # Step 1: Paper counts by (gender_pair, pub_year, pub_month)
    paper_counts = {}  # (pair, year, month) -> count
    rows = conn.execute("""
        SELECT gender_pair, pub_year, pub_month, COUNT(*) as cnt
        FROM papers
        WHERE journal_id = ?
          AND gender_pair IN ('WW','WM','MW','MM')
          AND is_research = 1
          AND pub_year BETWEEN ? AND ?
        GROUP BY gender_pair, pub_year, pub_month
    """, (journal_id, paper_start_y, paper_end_y)).fetchall()

    if not rows:
        return {}

    for pair, year, month, cnt in rows:
        if month and month > 0:
            paper_counts[(pair, year, month)] = cnt

    # Pre-index paper counts by pair -> {ym_int: count}
    paper_by_pair = {p: {} for p in GENDER_PAIRS}
    for (pair, y, m), cnt in paper_counts.items():
        paper_by_pair[pair][_ym_to_int(y, m)] = cnt

    # Step 2: Citation counts grouped by (gender_pair, pub_ym, cite_ym)
    logger.debug(f"  Journal {journal_id}: querying citation counts...")
    from collections import defaultdict
    cite_by_pub = {p: defaultdict(dict) for p in GENDER_PAIRS}
    rows = conn.execute("""
        SELECT p.gender_pair,
               p.pub_year * 100 + p.pub_month as pub_ym,
               c.citing_year * 100 + c.citing_month as cite_ym,
               COUNT(*) as cnt
        FROM papers p
        JOIN citations c ON c.cited_pmid = p.pmid
        WHERE p.journal_id = ?
          AND p.gender_pair IN ('WW','WM','MW','MM')
          AND p.is_research = 1
          AND p.pub_year BETWEEN ? AND ?
          AND p.pub_month IS NOT NULL AND p.pub_month > 0
          AND c.citing_year BETWEEN ? AND ?
          AND c.citing_month IS NOT NULL AND c.citing_month > 0
        GROUP BY p.gender_pair, pub_ym, cite_ym
    """, (journal_id, paper_start_y, paper_end_y, cite_start_y, cite_end_y)).fetchall()

    for pair, pub_ym, cite_ym, cnt in rows:
        cite_by_pub[pair][pub_ym][cite_ym] = cnt

    # Step 3: Assemble results for each target month
    result = {}
    for t_y, t_m in targets:
        pw_start_int = _ym_to_int(*_month_offset(t_y, t_m, -36))
        pw_end_int = _ym_to_int(*_month_offset(t_y, t_m, -13))
        cw_start_int = _ym_to_int(*_month_offset(t_y, t_m, -11))
        cw_end_int = _ym_to_int(t_y, t_m)

        month_data = {}
        for pair in GENDER_PAIRS:
            # Papers in window
            p_count = sum(
                cnt for ym_int, cnt in paper_by_pair[pair].items()
                if pw_start_int <= ym_int <= pw_end_int
            )

            # Citations in window
            c_count = 0
            for pub_ym, cite_dict in cite_by_pub[pair].items():
                if pw_start_int <= pub_ym <= pw_end_int:
                    for cite_ym, cnt in cite_dict.items():
                        if cw_start_int <= cite_ym <= cw_end_int:
                            c_count += cnt

            rolling_if = round(c_count / p_count, 3) if p_count > 0 else 0
            month_data[pair] = {"p": p_count, "c": c_count, "if": rolling_if}

        total_papers = sum(d["p"] for d in month_data.values())
        if total_papers >= 5:
            mm_if = month_data["MM"]["if"]
            for pair in GENDER_PAIRS:
                if mm_if > 0 and month_data[pair]["p"] > 0:
                    month_data[pair]["norm"] = round(month_data[pair]["if"] / mm_if, 4)
                else:
                    month_data[pair]["norm"] = None
            result[_ym_key(t_y, t_m)] = month_data

    return result


def compute_aggregate_rolling_if(conn):
    """
    Compute aggregate monthly rolling 24-month citation rate by gender pair across ALL journals.

    Strategy: Two large SQL queries with monthly GROUP BY, then assemble in Python.
    """
    targets = _generate_target_months()
    if not targets:
        return {}

    earliest_target = targets[0]
    latest_target = targets[-1]
    paper_start_y, _ = _month_offset(earliest_target[0], earliest_target[1], -36)
    paper_end_y, _ = _month_offset(latest_target[0], latest_target[1], -13)
    cite_start_y, _ = _month_offset(earliest_target[0], earliest_target[1], -11)
    cite_end_y = latest_target[0]

    # Step 1: Paper counts by (gender_pair, pub_year, pub_month)
    logger.info("Counting papers by gender pair, year, and month...")
    paper_counts = {}  # (pair, year, month) -> count
    rows = conn.execute("""
        SELECT gender_pair, pub_year, pub_month, COUNT(*) as cnt
        FROM papers
        WHERE gender_pair IN ('WW','WM','MW','MM')
          AND is_research = 1
          AND pub_year BETWEEN ? AND ?
          AND pub_month IS NOT NULL AND pub_month > 0
        GROUP BY gender_pair, pub_year, pub_month
    """, (paper_start_y, paper_end_y)).fetchall()
    for pair, year, month, cnt in rows:
        paper_counts[(pair, year, month)] = cnt
    logger.info(f"  Paper counts loaded: {len(rows)} (pair, year, month) groups")

    # Step 2: Citation counts by (gender_pair, pub_year, pub_month, citing_year, citing_month)
    logger.info("Counting citations by gender pair and month (this may take a while)...")
    cite_counts = {}  # (pair, pub_ym_int, cite_ym_int) -> count
    rows = conn.execute("""
        SELECT p.gender_pair,
               p.pub_year * 100 + p.pub_month as pub_ym,
               c.citing_year * 100 + c.citing_month as cite_ym,
               COUNT(*) as cnt
        FROM papers p
        JOIN citations c ON c.cited_pmid = p.pmid
        WHERE p.gender_pair IN ('WW','WM','MW','MM')
          AND p.is_research = 1
          AND p.pub_year BETWEEN ? AND ?
          AND p.pub_month IS NOT NULL AND p.pub_month > 0
          AND c.citing_year BETWEEN ? AND ?
          AND c.citing_month IS NOT NULL AND c.citing_month > 0
        GROUP BY p.gender_pair, pub_ym, cite_ym
    """, (paper_start_y, paper_end_y, cite_start_y, cite_end_y)).fetchall()
    # Index by (pair, pub_ym_int) -> {cite_ym_int: count}
    from collections import defaultdict
    cite_by_pub = {p: defaultdict(dict) for p in GENDER_PAIRS}
    for pair, pub_ym, cite_ym, cnt in rows:
        cite_by_pub[pair][pub_ym][cite_ym] = cnt
    logger.info(f"  Citation counts loaded: {len(rows)} groups")

    # Pre-compute paper count lookups by pair -> {ym_int: count}
    paper_by_pair = {p: {} for p in GENDER_PAIRS}
    for (pair, y, m), cnt in paper_counts.items():
        paper_by_pair[pair][_ym_to_int(y, m)] = cnt

    # Step 3: Assemble results for each target month
    logger.info(f"Assembling {len(targets)} monthly snapshots...")
    result = {}
    for idx, (t_y, t_m) in enumerate(targets):
        pw_start_y, pw_start_m = _month_offset(t_y, t_m, -36)
        pw_end_y, pw_end_m = _month_offset(t_y, t_m, -13)
        pw_start_int = _ym_to_int(pw_start_y, pw_start_m)
        pw_end_int = _ym_to_int(pw_end_y, pw_end_m)

        cw_start_y, cw_start_m = _month_offset(t_y, t_m, -11)
        cw_start_int = _ym_to_int(cw_start_y, cw_start_m)
        cw_end_int = _ym_to_int(t_y, t_m)

        month_data = {}
        for pair in GENDER_PAIRS:
            # Papers in window
            p_count = sum(
                cnt for ym_int, cnt in paper_by_pair[pair].items()
                if pw_start_int <= ym_int <= pw_end_int
            )

            # Citations in window
            c_count = 0
            for pub_ym, cite_dict in cite_by_pub[pair].items():
                if pw_start_int <= pub_ym <= pw_end_int:
                    for cite_ym, cnt in cite_dict.items():
                        if cw_start_int <= cite_ym <= cw_end_int:
                            c_count += cnt

            rolling_if = round(c_count / p_count, 3) if p_count > 0 else 0
            month_data[pair] = {"p": p_count, "c": c_count, "if": rolling_if}

        total_papers = sum(d["p"] for d in month_data.values())
        if total_papers > 0:
            mm_if = month_data["MM"]["if"]
            for pair in GENDER_PAIRS:
                if mm_if > 0 and month_data[pair]["p"] > 0:
                    month_data[pair]["norm"] = round(month_data[pair]["if"] / mm_if, 4)
                else:
                    month_data[pair]["norm"] = None
            result[_ym_key(t_y, t_m)] = month_data

        if (idx + 1) % 12 == 0:
            logger.info(f"  {_ym_key(t_y, t_m)}: {total_papers:,} papers, "
                         f"MM IF={month_data['MM']['if']:.2f}, WW IF={month_data['WW']['if']:.2f}")

    return result


def store_to_db(conn, journal_id, rolling_data):
    """Store rolling IF data in gender_citation_stats table."""
    for month_str, month_data in rolling_data.items():
        for pair, d in month_data.items():
            if pair not in GENDER_PAIRS:
                continue
            conn.execute("""
                INSERT OR REPLACE INTO gender_citation_stats
                    (journal_id, snapshot_month, gender_pair, paper_count, citation_count, rolling_if_24m)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (journal_id, month_str, pair, d["p"], d["c"], d["if"]))
    conn.commit()


def process_journal_batch(journals, db_path, output_dir):
    """Process a batch of journals. Returns count processed."""
    conn = sqlite3.connect(db_path)
    count = 0

    for i, (jid, slug, name) in enumerate(journals):
        rolling_data = compute_journal_rolling_if(conn, jid)

        if rolling_data:
            store_to_db(conn, jid, rolling_data)

            # Update per-journal JSON with rolling IF data
            json_path = Path(output_dir) / "journals" / f"{slug}.json"
            if json_path.exists():
                with open(json_path) as f:
                    journal_data = json.load(f)
                journal_data["rolling_if"] = rolling_data
                with open(json_path, "w") as f:
                    json.dump(journal_data, f, separators=(",", ":"))

        if (i + 1) % 10 == 0:
            logger.info(f"  Processed {i + 1}/{len(journals)} journals...")
        count += 1

    conn.close()
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate-only", action="store_true",
                        help="Only compute aggregate (skip per-journal)")
    parser.add_argument("--journals-only", action="store_true",
                        help="Only compute per-journal (skip aggregate)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit to top N journals by paper count")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers")
    parser.add_argument("--output-dir", type=str, default="docs/data/gender",
                        help="Output directory for JSON files")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = sqlite3.connect(IMPACT_DB)

    # ── Per-journal computation ──
    if not args.aggregate_only:
        if args.limit:
            journals = conn.execute("""
                SELECT j.id, j.slug, j.name
                FROM journals j
                JOIN (SELECT journal_id, COUNT(*) as cnt FROM papers
                      WHERE gender_pair IS NOT NULL GROUP BY journal_id) p
                  ON p.journal_id = j.id
                ORDER BY p.cnt DESC
                LIMIT ?
            """, (args.limit,)).fetchall()
        else:
            journals = conn.execute(
                "SELECT id, slug, name FROM journals ORDER BY name"
            ).fetchall()

        logger.info(f"Computing monthly rolling IF for {len(journals)} journals...")

        if args.workers <= 1:
            process_journal_batch(journals, IMPACT_DB, args.output_dir)
        else:
            chunks = [journals[i::args.workers] for i in range(args.workers)]
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(process_journal_batch, chunk, IMPACT_DB, args.output_dir): i
                    for i, chunk in enumerate(chunks)
                }
                for future in as_completed(futures):
                    wid = futures[future]
                    try:
                        n = future.result()
                        logger.info(f"Worker {wid} finished: {n} journals")
                    except Exception as e:
                        logger.error(f"Worker {wid} failed: {e}", exc_info=True)

        logger.info("Per-journal computation complete.")

    # ── Aggregate computation ──
    if not args.journals_only:
        logger.info("Computing aggregate monthly rolling IF...")
        agg_rolling = compute_aggregate_rolling_if(conn)

        # Update aggregate.json
        agg_path = output_dir / "aggregate.json"
        if agg_path.exists():
            with open(agg_path) as f:
                agg_data = json.load(f)
        else:
            agg_data = {}

        agg_data["rolling_if_24m"] = agg_rolling

        with open(agg_path, "w") as f:
            json.dump(agg_data, f, indent=2)

        keys = sorted(agg_rolling.keys())
        logger.info(f"Aggregate monthly rolling IF written to {agg_path}")
        logger.info(f"  Months: {keys[0]} to {keys[-1]} ({len(keys)} data points)")

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
