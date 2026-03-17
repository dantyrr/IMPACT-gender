"""
Compute citation rates by gender pair per journal.

Analyses:
1. Per-paper citation rate by gender pair (normalized)
2. Rolling IF variants (12m, 24m, 5yr) by gender pair
3. Citing-gender analysis: gender of citers vs cited
4. Temporal trends

Usage:
  python scripts/gender/analyze_citations.py                    # All journals
  python scripts/gender/analyze_citations.py --journal nature   # One journal
  python scripts/gender/analyze_citations.py --limit 100        # Top 100 journals
"""
import argparse
import sqlite3
import json
import logging
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GENDER_PAIRS = ["WW", "WM", "MW", "MM"]


def compute_gender_citation_rates(db_path: str, journal_id: int,
                                  year: int) -> dict:
    """
    For a given journal and publication year, compute citation counts
    and per-paper rates for each gender pair.
    """
    conn = sqlite3.connect(db_path)

    results = {}
    for pair in GENDER_PAIRS:
        row = conn.execute("""
            SELECT
                COUNT(DISTINCT p.pmid) as paper_count,
                COUNT(c.id) as citation_count
            FROM papers p
            LEFT JOIN citations c ON c.cited_pmid = p.pmid
            WHERE p.journal_id = ?
              AND p.pub_year = ?
              AND p.gender_pair = ?
              AND p.is_research = 1
        """, (journal_id, year, pair)).fetchone()

        papers = row[0]
        cites = row[1]
        results[pair] = {
            "papers": papers,
            "citations": cites,
            "rate": round(cites / papers, 4) if papers > 0 else 0.0,
        }

    conn.close()
    return results


def compute_yearly_gender_stats(db_path: str, journal_id: int,
                                start_year: int, end_year: int) -> dict:
    """Compute gender composition and citation rates by year for a journal."""
    conn = sqlite3.connect(db_path)

    yearly = {}
    for year in range(start_year, end_year + 1):
        rows = conn.execute("""
            SELECT
                p.gender_pair,
                COUNT(DISTINCT p.pmid) as paper_count,
                COUNT(c.id) as citation_count
            FROM papers p
            LEFT JOIN citations c ON c.cited_pmid = p.pmid
            WHERE p.journal_id = ?
              AND p.pub_year = ?
              AND p.gender_pair IN ('WW','WM','MW','MM')
              AND p.is_research = 1
            GROUP BY p.gender_pair
        """, (journal_id, year)).fetchall()

        if not rows:
            continue

        year_data = {}
        for pair, papers, cites in rows:
            year_data[pair] = {
                "p": papers,
                "c": cites,
                "r": round(cites / papers, 4) if papers > 0 else 0,
            }
        total_papers = sum(d["p"] for d in year_data.values())
        if total_papers >= 10:  # Skip years with too few papers
            yearly[str(year)] = year_data

    conn.close()
    return yearly


def compute_citing_gender_analysis(db_path: str, journal_id: int,
                                   start_year: int, end_year: int) -> dict:
    """
    For papers in a journal, analyze the gender of citing authors.
    Aggregated across all years for statistical power.
    """
    conn = sqlite3.connect(db_path)

    results = {}
    for cited_pair in GENDER_PAIRS:
        row_data = conn.execute("""
            SELECT
                citing_p.first_author_gender,
                COUNT(*) as cnt
            FROM papers cited_p
            JOIN citations c ON c.cited_pmid = cited_p.pmid
            JOIN papers citing_p ON citing_p.pmid = c.citing_pmid
            WHERE cited_p.journal_id = ?
              AND cited_p.pub_year BETWEEN ? AND ?
              AND cited_p.gender_pair = ?
              AND citing_p.first_author_gender IN ('W', 'M')
            GROUP BY citing_p.first_author_gender
        """, (journal_id, start_year, end_year, cited_pair)).fetchall()

        gender_counts = {"W": 0, "M": 0}
        for gender, count in row_data:
            gender_counts[gender] = count
        total = sum(gender_counts.values())
        if total > 0:
            results[cited_pair] = {
                "W": gender_counts["W"],
                "M": gender_counts["M"],
                "total": total,
                "pctW": round(gender_counts["W"] / total * 100, 1),
                "pctM": round(gender_counts["M"] / total * 100, 1),
            }

    conn.close()
    return results


def analyze_journal(db_path: str, journal_id: int, slug: str, name: str,
                    start_year: int, end_year: int) -> dict:
    """Full gender-citation analysis for one journal."""
    result = {
        "slug": slug,
        "name": name,
        "yearly": compute_yearly_gender_stats(db_path, journal_id,
                                              start_year, end_year),
        "citing": compute_citing_gender_analysis(db_path, journal_id,
                                                 start_year, end_year),
    }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", type=str, help="Single journal slug")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit to top N journals by paper count")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--output-dir", type=str, default="docs-gender/data/gender")
    args = parser.parse_args()

    conn = sqlite3.connect(IMPACT_DB)

    if args.journal:
        journals = conn.execute(
            "SELECT id, slug, name FROM journals WHERE slug = ?",
            (args.journal,)
        ).fetchall()
    elif args.limit:
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
    conn.close()

    output_dir = Path(args.output_dir) / "journals"
    output_dir.mkdir(parents=True, exist_ok=True)

    for i, (jid, slug, name) in enumerate(journals):
        result = analyze_journal(IMPACT_DB, jid, slug, name,
                                 args.start_year, args.end_year)

        # Only write if there's data
        if result["yearly"]:
            out_path = output_dir / f"{slug}.json"
            with open(out_path, "w") as f:
                json.dump(result, f, separators=(",", ":"))

        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i + 1}/{len(journals)} journals...")

    logger.info(f"Done. Analyzed {len(journals)} journals -> {output_dir}")


if __name__ == "__main__":
    main()
