"""Export a journal index JSON for the gender analysis dashboard."""
import sqlite3
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB


def main():
    conn = sqlite3.connect(IMPACT_DB)
    rows = conn.execute("""
        SELECT j.slug, j.name, COUNT(p.pmid) as paper_count
        FROM journals j
        JOIN papers p ON p.journal_id = j.id
        WHERE p.gender_pair IS NOT NULL
        GROUP BY j.id
        HAVING paper_count >= 10
        ORDER BY j.name
    """).fetchall()
    conn.close()

    index = [{"slug": slug, "name": name, "n": count} for slug, name, count in rows]

    output = Path("docs-gender/data/index.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(index, f, separators=(",", ":"))

    print(f"Exported {len(index)} journals to {output}")


if __name__ == "__main__":
    main()
