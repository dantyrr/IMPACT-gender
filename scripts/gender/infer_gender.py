"""
Main script: extract forenames from pubmed_bulk.db, infer gender, update impact.db.

Usage:
  python scripts/gender/infer_gender.py --mode offline    # Free, ~80% accuracy
  python scripts/gender/infer_gender.py --mode api        # Paid, ~95% accuracy
  python scripts/gender/infer_gender.py --dry-run         # Just count names, no inference
"""
import argparse
import sqlite3
import logging
import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.pipeline.gender_inference import (
    extract_forename, GenderCache, GenderInferenceEngine,
    OfflineGenderEngine, GenderAPIEngine,
)
from scripts.gender.config import (
    GENDER_API_KEY, GENDER_CONFIDENCE_THRESHOLD, GENDER_CACHE_DB,
    IMPACT_DB, PUBMED_BULK_DB, START_YEAR, END_YEAR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_paper_forenames(pubmed_db: str, start_year: int, end_year: int):
    """
    Extract forenames from pubmed_bulk.db for papers in scope.
    Returns:
      paper_names: dict {pmid: (first_forename, last_forename)}
      unique_names: set of lowercase unique first names to look up
    """
    conn = sqlite3.connect(pubmed_db)

    cursor = conn.execute("""
        SELECT pmid, first_author, last_author
        FROM pubmed
        WHERE pub_year BETWEEN ? AND ?
          AND first_author IS NOT NULL
    """, (start_year, end_year))

    paper_names = {}
    unique_names = set()

    count = 0
    for row in cursor:
        pmid, fa_raw, la_raw = row
        fa = extract_forename(fa_raw)
        la = extract_forename(la_raw)
        if fa or la:
            paper_names[pmid] = (fa, la)
            if fa:
                unique_names.add(fa.lower())
            if la:
                unique_names.add(la.lower())
        count += 1
        if count % 5000000 == 0:
            logger.info(f"  Scanned {count} papers, {len(unique_names)} unique names so far...")

    conn.close()
    logger.info(f"  Scanned {count} total papers")
    return paper_names, unique_names


def run_inference(unique_names: set, engine: GenderInferenceEngine):
    """Infer gender for all unique names."""
    name_list = sorted(unique_names)
    logger.info(f"Inferring gender for {len(name_list)} unique names...")

    for i in range(0, len(name_list), 5000):
        batch = name_list[i:i + 5000]
        engine.infer_batch(batch)
        done = min(i + 5000, len(name_list))
        if done % 50000 == 0 or done == len(name_list):
            logger.info(f"  Processed {done}/{len(name_list)} names")


def map_genders_to_papers(paper_names: dict, cache: GenderCache,
                          threshold: float):
    """Map cached gender results back to all papers."""
    results = {}
    for pmid, (fa_name, la_name) in paper_names.items():
        fa_result = cache.get(fa_name.lower()) if fa_name else None
        la_result = cache.get(la_name.lower()) if la_name else None

        fa_code = fa_result.to_code(threshold) if fa_result else "U"
        la_code = la_result.to_code(threshold) if la_result else "U"
        fa_prob = fa_result.probability if fa_result else 0.0
        la_prob = la_result.probability if la_result else 0.0

        pair = f"{fa_code}{la_code}" if fa_code != "U" and la_code != "U" else None

        results[pmid] = (fa_code, fa_prob, la_code, la_prob, pair, fa_name, la_name)

    return results


def update_impact_db(db_path: str, results: dict):
    """Write gender results to impact.db papers table."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    count = 0
    batch = []
    for pmid, (fa_g, fa_p, la_g, la_p, pair, fa_name, la_name) in results.items():
        batch.append((fa_name, la_name, fa_g, fa_p, la_g, la_p, pair, pmid))
        if len(batch) >= 10000:
            conn.executemany("""
                UPDATE papers SET
                    first_author_forename = ?,
                    last_author_forename = ?,
                    first_author_gender = ?,
                    first_author_gender_prob = ?,
                    last_author_gender = ?,
                    last_author_gender_prob = ?,
                    gender_pair = ?
                WHERE pmid = ?
            """, batch)
            conn.commit()
            count += len(batch)
            batch = []
            if count % 500000 == 0:
                logger.info(f"  Updated {count} papers in impact.db")

    if batch:
        conn.executemany("""
            UPDATE papers SET
                first_author_forename = ?,
                last_author_forename = ?,
                first_author_gender = ?,
                first_author_gender_prob = ?,
                last_author_gender = ?,
                last_author_gender_prob = ?,
                gender_pair = ?
            WHERE pmid = ?
        """, batch)
        conn.commit()
        count += len(batch)

    logger.info(f"Updated {count} papers in impact.db")
    conn.close()


def print_summary(results: dict):
    """Print gender inference summary statistics."""
    fa_codes = Counter()
    la_codes = Counter()
    pair_counts = Counter()

    for pmid, (fa_g, fa_p, la_g, la_p, pair, fa_name, la_name) in results.items():
        fa_codes[fa_g] += 1
        la_codes[la_g] += 1
        if pair:
            pair_counts[pair] += 1

    total = len(results)
    logger.info(f"\n{'='*60}")
    logger.info(f"GENDER INFERENCE SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total papers processed: {total:,}")
    logger.info(f"\nFirst author gender:")
    for code in ["W", "M", "U"]:
        n = fa_codes[code]
        logger.info(f"  {code}: {n:>10,} ({n/total*100:.1f}%)")
    logger.info(f"\nLast author gender:")
    for code in ["W", "M", "U"]:
        n = la_codes[code]
        logger.info(f"  {code}: {n:>10,} ({n/total*100:.1f}%)")
    logger.info(f"\nGender pair distribution:")
    assigned = sum(pair_counts.values())
    for pair in ["WW", "WM", "MW", "MM"]:
        n = pair_counts[pair]
        logger.info(f"  {pair}: {n:>10,} ({n/assigned*100:.1f}% of assigned)")
    unassigned = total - assigned
    logger.info(f"  Unassigned (has U): {unassigned:,} ({unassigned/total*100:.1f}%)")
    logger.info(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Infer gender of paper authors")
    parser.add_argument("--mode", choices=["offline", "api"], default="offline",
                        help="offline = gender-guesser (free), api = Gender API (paid)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count unique names only, don't infer")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    args = parser.parse_args()

    logger.info(f"Extracting forenames from {PUBMED_BULK_DB} "
                f"({args.start_year}-{args.end_year})...")
    paper_names, unique_names = get_paper_forenames(
        PUBMED_BULK_DB, args.start_year, args.end_year
    )
    logger.info(f"Found {len(paper_names):,} papers with usable names, "
                f"{len(unique_names):,} unique first names")

    if args.dry_run:
        logger.info("Dry run complete.")
        return

    cache = GenderCache(GENDER_CACHE_DB)

    if args.mode == "offline":
        backend = OfflineGenderEngine()
    else:
        if not GENDER_API_KEY:
            logger.error("GENDER_API_KEY not set. Add to .env or environment.")
            sys.exit(1)
        backend = GenderAPIEngine(GENDER_API_KEY)

    engine = GenderInferenceEngine(cache=cache, backend=backend)

    run_inference(unique_names, engine)

    logger.info("Mapping gender results to papers...")
    results = map_genders_to_papers(paper_names, cache, GENDER_CONFIDENCE_THRESHOLD)

    print_summary(results)

    logger.info("Writing to impact.db...")
    update_impact_db(IMPACT_DB, results)
    cache.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
