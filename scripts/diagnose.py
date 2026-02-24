#!/usr/bin/env python3
"""
Diagnostic script: investigate why rolling_if = 0 despite having real data.
Run this from the IMPACT project root:
    python scripts/diagnose.py
"""
import sys, os, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline.config import DB_PATH

print(f"DB path: {DB_PATH}")
print(f"DB exists: {os.path.exists(DB_PATH)}\n")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── 1. Basic counts ────────────────────────────────────────────────────────
print("=" * 60)
print("1. BASIC COUNTS")
print("=" * 60)
for row in conn.execute("SELECT id, issn, name FROM journals ORDER BY id"):
    jid, issn, name = row["id"], row["issn"], row["name"]
    pc = conn.execute("SELECT COUNT(*) FROM papers WHERE journal_id=?", (jid,)).fetchone()[0]
    cc = conn.execute(
        "SELECT COUNT(*) FROM citations c JOIN papers p ON c.cited_pmid=p.pmid WHERE p.journal_id=?",
        (jid,)
    ).fetchone()[0]
    print(f"  Journal {jid}: {name}")
    print(f"    papers: {pc}, citations resolved: {cc}")

# ── 2. pub_date distribution for journal 1 ─────────────────────────────────
print("\n" + "=" * 60)
print("2. PUB_DATE DISTRIBUTION (Aging Cell, journal_id=1)")
print("=" * 60)
rows = conn.execute("""
    SELECT substr(pub_date,1,7) as month, COUNT(*) as cnt
    FROM papers WHERE journal_id=1
    GROUP BY month ORDER BY month
""").fetchall()
for r in rows:
    print(f"  {r['month']}: {r['cnt']} papers")

# ── 3. is_research breakdown ───────────────────────────────────────────────
print("\n" + "=" * 60)
print("3. IS_RESEARCH BREAKDOWN (journal 1)")
print("=" * 60)
rows = conn.execute("""
    SELECT is_research, pub_type, COUNT(*) as cnt
    FROM papers WHERE journal_id=1
    GROUP BY is_research, pub_type
    ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  is_research={r['is_research']}  pub_type={r['pub_type']!r:30s}  count={r['cnt']}")

# ── 4. Sample papers ───────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("4. SAMPLE PAPERS (first 5)")
print("=" * 60)
rows = conn.execute("""
    SELECT pmid, pub_date, pub_type, is_research
    FROM papers WHERE journal_id=1
    ORDER BY pub_date
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"  PMID {r['pmid']}: date={r['pub_date']}  type={r['pub_type']!r}  research={r['is_research']}")

# ── 5. citing_date distribution ────────────────────────────────────────────
print("\n" + "=" * 60)
print("5. CITING_DATE DISTRIBUTION (journal 1 papers)")
print("=" * 60)
rows = conn.execute("""
    SELECT substr(c.citing_date,1,4) as year, COUNT(*) as cnt
    FROM citations c
    JOIN papers p ON c.cited_pmid = p.pmid
    WHERE p.journal_id = 1
    GROUP BY year ORDER BY year
""").fetchall()
for r in rows:
    print(f"  {r['year']}: {r['cnt']} citation events")

# ── 6. Manually trace rolling IF for target 2026-01 ──────────────────────
print("\n" + "=" * 60)
print("6. MANUAL ROLLING IF TRACE FOR TARGET 2026-01")
print("=" * 60)

paper_start = "2023-02-01"
paper_end   = "2025-01-31"
cite_start  = "2025-02-01"
cite_end    = "2026-01-31"

print(f"  Paper window: {paper_start} to {paper_end}")
print(f"  Cite  window: {cite_start} to {cite_end}")

# Papers in window
papers_in_window = conn.execute("""
    SELECT pmid, pub_date, pub_type, is_research
    FROM papers
    WHERE journal_id=1 AND pub_date >= ? AND pub_date <= ?
""", (paper_start, paper_end)).fetchall()
print(f"\n  Papers in window: {len(papers_in_window)}")

research = [p for p in papers_in_window if p["is_research"] == 1]
non_research = [p for p in papers_in_window if p["is_research"] == 0]
print(f"    is_research=1: {len(research)}")
print(f"    is_research=0: {len(non_research)}")

if non_research:
    print(f"  Sample non-research pub_types: {[r['pub_type'] for r in non_research[:5]]}")

# Citations for papers in window
all_pmids = [p["pmid"] for p in papers_in_window]
research_pmids = [p["pmid"] for p in research]

if all_pmids:
    placeholders = ",".join("?" * len(all_pmids))
    cnt_all = conn.execute(
        f"SELECT COUNT(*) FROM citations WHERE cited_pmid IN ({placeholders}) "
        f"AND citing_date >= ? AND citing_date <= ?",
        all_pmids + [cite_start, cite_end]
    ).fetchone()[0]
    print(f"\n  Citations for ALL {len(all_pmids)} window papers in cite window: {cnt_all}")
else:
    print("\n  No papers in window!")

if research_pmids:
    placeholders = ",".join("?" * len(research_pmids))
    cnt_research = conn.execute(
        f"SELECT COUNT(*) FROM citations WHERE cited_pmid IN ({placeholders}) "
        f"AND citing_date >= ? AND citing_date <= ?",
        research_pmids + [cite_start, cite_end]
    ).fetchone()[0]
    print(f"  Citations for {len(research_pmids)} RESEARCH papers in cite window: {cnt_research}")

    rolling_if = cnt_all / len(research) if research else 0.0
    print(f"\n  → COMPUTED rolling_if: {rolling_if:.3f}")
    print(f"  → (total_citations={cnt_all}, n_research={len(research)})")
else:
    print("  → No research papers in window! rolling_if = 0.0 (this is the bug)")

# ── 7. Check a wider cite window ──────────────────────────────────────────
print("\n" + "=" * 60)
print("7. CITATIONS WITH WIDER DATE RANGES (sanity check)")
print("=" * 60)
if all_pmids:
    placeholders = ",".join("?" * len(all_pmids))
    for y_start, y_end in [("2020-01-01", "2027-12-31"), ("2024-01-01", "2026-12-31"),
                            ("2025-01-01", "2026-01-31"), ("2025-06-01", "2025-06-30")]:
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM citations WHERE cited_pmid IN ({placeholders}) "
            f"AND citing_date >= ? AND citing_date <= ?",
            all_pmids + [y_start, y_end]
        ).fetchone()[0]
        print(f"  citing_date [{y_start} – {y_end}]: {cnt} citations")

# ── 8. Sample citations for window papers ─────────────────────────────────
print("\n" + "=" * 60)
print("8. SAMPLE CITATIONS FOR WINDOW PAPERS")
print("=" * 60)
if all_pmids:
    rows = conn.execute(
        f"SELECT cited_pmid, citing_pmid, citing_date FROM citations "
        f"WHERE cited_pmid IN ({','.join('?' * len(all_pmids[:20]))}) "
        f"ORDER BY citing_date DESC LIMIT 10",
        all_pmids[:20]
    ).fetchall()
    for r in rows:
        print(f"  {r['cited_pmid']} cited by {r['citing_pmid']} on {r['citing_date']}")

conn.close()
print("\nDiagnostic complete.")
