"""
Validate gender inference quality and coverage.

Reports:
- Overall classification distribution (W/M/U by first author and last author)
- Classification rates by author country (top 30)
- Known-name spot checks
- Confidence distribution (male vs mostly_male vs female vs mostly_female vs unknown vs andy)

Usage:
  python scripts/gender/validate_gender.py
"""
import sqlite3
import json
import logging
from pathlib import Path
from collections import defaultdict

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB, GENDER_CACHE_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def validate_overall(conn):
    """Overall classification stats."""
    print("\n" + "=" * 70)
    print("OVERALL CLASSIFICATION")
    print("=" * 70)

    for role in ["first", "last"]:
        col = f"{role}_author_gender"
        rows = conn.execute(f"""
            SELECT {col}, COUNT(*) as cnt
            FROM papers
            WHERE pub_year BETWEEN ? AND ?
            GROUP BY {col}
            ORDER BY cnt DESC
        """, (START_YEAR, END_YEAR)).fetchall()

        total = sum(r[1] for r in rows)
        print(f"\n{role.title()} author gender (papers {START_YEAR}-{END_YEAR}):")
        for gender, cnt in rows:
            label = gender if gender else "NULL (no forename extracted)"
            pct = cnt / total * 100
            print(f"  {label:>35s}: {cnt:>12,} ({pct:5.1f}%)")
        print(f"  {'TOTAL':>35s}: {total:>12,}")


def validate_by_country(conn):
    """Classification rates by first author country."""
    print("\n" + "=" * 70)
    print("CLASSIFICATION BY COUNTRY (top 30)")
    print("=" * 70)

    rows = conn.execute("""
        SELECT
            first_author_country,
            first_author_gender,
            COUNT(*) as cnt
        FROM papers
        WHERE pub_year BETWEEN ? AND ?
          AND first_author_country IS NOT NULL
          AND first_author_country != ''
        GROUP BY first_author_country, first_author_gender
    """, (START_YEAR, END_YEAR)).fetchall()

    by_country = defaultdict(lambda: {"W": 0, "M": 0, "U": 0, "NULL": 0, "total": 0})
    for country, gender, cnt in rows:
        key = gender if gender else "NULL"
        by_country[country][key] += cnt
        by_country[country]["total"] += cnt

    sorted_countries = sorted(by_country.items(), key=lambda x: -x[1]["total"])[:30]

    print(f"\n{'Country':>20s} {'Total':>10s} {'W':>8s} {'M':>8s} {'U':>8s} {'%Assigned':>10s} {'%W(of W+M)':>12s}")
    print("-" * 80)
    for country, d in sorted_countries:
        t = d["total"]
        assigned = d["W"] + d["M"]
        pct_assigned = assigned / t * 100 if t > 0 else 0
        pct_w = d["W"] / assigned * 100 if assigned > 0 else 0
        print(f"{country:>20s} {t:>10,} {d['W']:>8,} {d['M']:>8,} {d['U']:>8,} {pct_assigned:>9.1f}% {pct_w:>11.1f}%")


def validate_gender_accuracy(conn):
    """Compare male vs female accuracy using known names."""
    print("\n" + "=" * 70)
    print("GENDER-GUESSER CONFIDENCE DISTRIBUTION")
    print("=" * 70)

    # Check the cache DB for raw classification categories
    cache_conn = sqlite3.connect(GENDER_CACHE_DB)
    rows = cache_conn.execute("""
        SELECT gender, COUNT(*) as cnt
        FROM gender_lookups
        GROUP BY gender
        ORDER BY cnt DESC
    """).fetchall()

    total = sum(r[1] for r in rows)
    print(f"\nGender cache ({total:,} unique names):")
    for gender, cnt in rows:
        label = gender if gender else "NULL/unknown"
        print(f"  {label:>20s}: {cnt:>10,} ({cnt/total*100:5.1f}%)")

    # Check probability distribution
    print("\nProbability distribution:")
    for prob_label, prob_val in [("0.95 (definitive)", 0.95), ("0.75 (mostly_X)", 0.75),
                                  ("0.50 (androgynous)", 0.50), ("0.00 (unknown)", 0.0)]:
        cnt = cache_conn.execute(
            "SELECT COUNT(*) FROM gender_lookups WHERE probability = ?",
            (prob_val,)
        ).fetchone()[0]
        print(f"  prob={prob_label}: {cnt:>10,} ({cnt/total*100:5.1f}%)")

    # Male vs female classification rates
    print("\nClassification by inferred gender:")
    male_definitive = cache_conn.execute(
        "SELECT COUNT(*) FROM gender_lookups WHERE gender = 'male' AND probability = 0.95"
    ).fetchone()[0]
    male_mostly = cache_conn.execute(
        "SELECT COUNT(*) FROM gender_lookups WHERE gender = 'male' AND probability = 0.75"
    ).fetchone()[0]
    female_definitive = cache_conn.execute(
        "SELECT COUNT(*) FROM gender_lookups WHERE gender = 'female' AND probability = 0.95"
    ).fetchone()[0]
    female_mostly = cache_conn.execute(
        "SELECT COUNT(*) FROM gender_lookups WHERE gender = 'female' AND probability = 0.75"
    ).fetchone()[0]

    print(f"  Male (definitive):   {male_definitive:>10,}")
    print(f"  Male (mostly):       {male_mostly:>10,}")
    print(f"  Female (definitive): {female_definitive:>10,}")
    print(f"  Female (mostly):     {female_mostly:>10,}")

    total_male = male_definitive + male_mostly
    total_female = female_definitive + female_mostly
    if total_male > 0:
        print(f"  Male confidence:     {male_definitive/total_male*100:.1f}% definitive")
    if total_female > 0:
        print(f"  Female confidence:   {female_definitive/total_female*100:.1f}% definitive")

    cache_conn.close()


def validate_known_names():
    """Spot-check known names against gender-guesser."""
    print("\n" + "=" * 70)
    print("KNOWN NAME SPOT CHECKS")
    print("=" * 70)

    cache_conn = sqlite3.connect(GENDER_CACHE_DB)

    test_names = [
        # Common Western female
        ("sarah", "female"), ("jennifer", "female"), ("maria", "female"),
        ("anna", "female"), ("laura", "female"),
        # Common Western male
        ("david", "male"), ("john", "male"), ("michael", "male"),
        ("robert", "male"), ("james", "male"),
        # East Asian (often ambiguous)
        ("wei", None), ("yong", None), ("jun", None),
        ("ming", None), ("lei", None),
        # Androgynous Western
        ("andrea", None), ("robin", None), ("alex", None),
    ]

    print(f"\n{'Name':>12s} {'Expected':>10s} {'Got':>10s} {'Prob':>6s} {'Match':>6s}")
    print("-" * 50)
    for name, expected in test_names:
        row = cache_conn.execute(
            "SELECT gender, probability FROM gender_lookups WHERE first_name = ?",
            (name,)
        ).fetchone()
        if row:
            got, prob = row
            match = "OK" if expected is None or got == expected else "MISS"
            print(f"{name:>12s} {str(expected):>10s} {str(got):>10s} {prob:>6.2f} {match:>6s}")
        else:
            print(f"{name:>12s} {str(expected):>10s} {'N/A':>10s} {'N/A':>6s} {'N/A':>6s}")

    cache_conn.close()


def main():
    conn = sqlite3.connect(IMPACT_DB)
    validate_overall(conn)
    validate_by_country(conn)
    conn.close()

    validate_gender_accuracy(conn)
    validate_known_names()

    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
