"""
Compute aggregate gender-citation statistics across all journals.
Outputs summary JSON for the dashboard overview page.

Key metrics:
- Overall gender pair proportions over time
- Mean citation rate ratio (women-led vs men-led papers)
- Citing-gender analysis across all journals
- Gender-guesser accuracy stats by country

Usage:
  python scripts/gender/compute_aggregates.py
"""
import sqlite3
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GENDER_PAIRS = ["WW", "WM", "MW", "MM"]


def compute_yearly_composition(conn, start_year, end_year):
    """Gender pair composition by year across all journals."""
    yearly = {}
    for year in range(start_year, end_year + 1):
        rows = conn.execute("""
            SELECT gender_pair, COUNT(*) as cnt
            FROM papers
            WHERE pub_year = ? AND gender_pair IN ('WW','WM','MW','MM')
              AND is_research = 1
            GROUP BY gender_pair
        """, (year,)).fetchall()

        if not rows:
            continue

        comp = {pair: 0 for pair in GENDER_PAIRS}
        for pair, cnt in rows:
            comp[pair] = cnt
        total = sum(comp.values())
        yearly[str(year)] = {
            pair: {"n": comp[pair], "pct": round(comp[pair] / total * 100, 2)}
            for pair in GENDER_PAIRS
        }
        yearly[str(year)]["total"] = total

    return yearly


def compute_yearly_citation_rates(conn, start_year, end_year):
    """Citation rate by gender pair and year."""
    yearly = {}
    for year in range(start_year, end_year + 1):
        rows = conn.execute("""
            SELECT
                p.gender_pair,
                COUNT(DISTINCT p.pmid) as paper_count,
                COUNT(c.id) as cite_count
            FROM papers p
            LEFT JOIN citations c ON c.cited_pmid = p.pmid
            WHERE p.pub_year = ?
              AND p.gender_pair IN ('WW','WM','MW','MM')
              AND p.is_research = 1
            GROUP BY p.gender_pair
        """, (year,)).fetchall()

        rates = {}
        for pair, papers, cites in rows:
            rates[pair] = {
                "p": papers,
                "c": cites,
                "r": round(cites / papers, 4) if papers > 0 else 0,
            }
        if rates:
            # Normalized: ratio relative to MM rate
            mm_rate = rates.get("MM", {}).get("r", 0)
            for pair in rates:
                rates[pair]["norm"] = round(rates[pair]["r"] / mm_rate, 4) if mm_rate > 0 else None
            yearly[str(year)] = rates

    return yearly


def compute_citing_gender_aggregate(conn, start_year, end_year):
    """Who cites whom — aggregate across all journals."""
    results = {}
    for cited_pair in GENDER_PAIRS:
        rows = conn.execute("""
            SELECT
                citing_p.first_author_gender,
                COUNT(*) as cnt
            FROM papers cited_p
            JOIN citations c ON c.cited_pmid = cited_p.pmid
            JOIN papers citing_p ON citing_p.pmid = c.citing_pmid
            WHERE cited_p.pub_year BETWEEN ? AND ?
              AND cited_p.gender_pair = ?
              AND citing_p.first_author_gender IN ('W', 'M')
            GROUP BY citing_p.first_author_gender
        """, (start_year, end_year, cited_pair)).fetchall()

        gender_counts = {"W": 0, "M": 0}
        for gender, count in rows:
            gender_counts[gender] = count
        total = sum(gender_counts.values())
        if total > 0:
            results[cited_pair] = {
                "W": gender_counts["W"],
                "M": gender_counts["M"],
                "total": total,
                "pctW": round(gender_counts["W"] / total * 100, 1),
            }

    return results


def compute_inference_quality(conn, start_year, end_year):
    """
    Gender-guesser accuracy stats by country.
    Tracks: classification rates, unknown rates, confidence distribution.
    """
    # Overall classification distribution
    overall = conn.execute("""
        SELECT first_author_gender, COUNT(*) as cnt
        FROM papers
        WHERE pub_year BETWEEN ? AND ?
          AND first_author_forename IS NOT NULL
        GROUP BY first_author_gender
    """, (start_year, end_year)).fetchall()

    overall_dist = {}
    total = 0
    for gender, cnt in overall:
        key = gender if gender else "NULL"
        overall_dist[key] = cnt
        total += cnt

    # By country — top 20 countries
    country_stats = conn.execute("""
        SELECT
            first_author_country,
            first_author_gender,
            COUNT(*) as cnt
        FROM papers
        WHERE pub_year BETWEEN ? AND ?
          AND first_author_country IS NOT NULL
          AND first_author_country != ''
          AND first_author_forename IS NOT NULL
        GROUP BY first_author_country, first_author_gender
        ORDER BY first_author_country
    """, (start_year, end_year)).fetchall()

    by_country = defaultdict(lambda: {"W": 0, "M": 0, "U": 0, "total": 0})
    for country, gender, cnt in country_stats:
        key = gender if gender else "U"
        by_country[country][key] += cnt
        by_country[country]["total"] += cnt

    # Sort by total, take top 30
    top_countries = sorted(by_country.items(), key=lambda x: -x[1]["total"])[:30]
    country_quality = {}
    for country, counts in top_countries:
        t = counts["total"]
        country_quality[country] = {
            "total": t,
            "W": counts["W"],
            "M": counts["M"],
            "U": counts["U"],
            "pctAssigned": round((counts["W"] + counts["M"]) / t * 100, 1) if t > 0 else 0,
            "pctW": round(counts["W"] / (counts["W"] + counts["M"]) * 100, 1)
                    if (counts["W"] + counts["M"]) > 0 else None,
        }

    return {
        "overall": {
            "total": total,
            **{k: {"n": v, "pct": round(v/total*100, 1)} for k, v in overall_dist.items()},
        },
        "by_country": country_quality,
    }


def main():
    output_dir = Path("docs-gender/data/gender")
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(IMPACT_DB)
    logger.info("Computing yearly composition...")
    composition = compute_yearly_composition(conn, START_YEAR, END_YEAR)

    logger.info("Computing yearly citation rates...")
    rates = compute_yearly_citation_rates(conn, START_YEAR, END_YEAR)

    logger.info("Computing citing-gender analysis...")
    citing = compute_citing_gender_aggregate(conn, START_YEAR, END_YEAR)

    logger.info("Computing inference quality stats...")
    quality = compute_inference_quality(conn, START_YEAR, END_YEAR)

    conn.close()

    stats = {
        "composition": composition,
        "citation_rates": rates,
        "citing_gender": citing,
        "inference_quality": quality,
    }

    out_path = output_dir / "aggregate.json"
    with open(out_path, "w") as f:
        json.dump(stats, f, indent=2)
    logger.info(f"Aggregate stats written to {out_path}")


if __name__ == "__main__":
    main()
