# Gender-Citation Analysis Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Analyze citation rate differences by inferred gender of first and last authors across all PubMed-indexed journals, with gender-pair categories (WW, WM, MW, MM) and controlled comparisons of 12-month, 24-month, and 5-year rolling citation rates.

**Architecture:** New `gender-analysis` branch with a standalone analysis pipeline. Gender inference via `gender-guesser` Python package (free, offline, instant) on ~500K unique first names extracted from `pubmed_bulk.db`. Analysis outputs per-journal and aggregate JSON, visualized on a separate GitHub Pages site deployed from the branch.

**Tech Stack:** Python 3.11+, SQLite, `gender-guesser` (offline, upgradeable to Gender API later), Chart.js, existing IMPACT infrastructure

**Reference:** Woitowich et al. (2025) PMID 39970134 — analyzed gender gaps in COVID-19 literature authorship using similar methodology.

---

## Key Design Decisions

### 1. Name Source: pubmed_bulk.db (not impact.db)

`impact.db` stores author names as "LastName, Initials" (e.g., "Curtis, M J") — unusable for gender inference. `pubmed_bulk.db` stores "LastName, ForeName" (e.g., "Gill, Natasha") with full first names available for ~91-97% of papers from 2005 onward. We'll join on PMID to get first names.

### 2. Scope: Papers from 2005-2026

- ~23.5M papers in pubmed_bulk.db from 2005+
- ~20M have usable ForeName (length > 2 chars)
- Aligns with existing snapshot computation start_year=2005

### 3. Gender Inference: `gender-guesser` (Free, Offline)

**~502K unique first names** (not 24M papers) need lookup — we de-duplicate first, then map back.

| Service | Free Tier | Time for 500K names | Accuracy |
|---------|-----------|---------------------|----------|
| `gender-guesser` (Python) | Unlimited, offline | ~Minutes | ~80-85% |
| gender-api.com | 100/month | ~417 years | ~95-98% |
| genderize.io | 100/day | ~14 years | ~93-95% |

**Decision:** Use `gender-guesser` for initial analysis. It classifies names as: male, female, mostly_male, mostly_female, androgynous, unknown. We track accuracy metrics by gender and by author country to understand biases. The pluggable backend design allows upgrading to a paid API later for the ~15-20% of names that return "unknown" or "androgynous".

### 3a. Accuracy Tracking by Gender and Region

We must track and report:
- **By inferred gender:** What % of names classified as male vs female? What % are "mostly_X" (lower confidence) vs definitive? What % are unknown/androgynous?
- **By author country:** Cross-reference `first_author_country` from impact.db with gender classification. Report accuracy/unknown rates for: USA, China, Japan, South Korea, India, Germany, UK, Brazil, etc. East Asian names are known to have higher "unknown" rates.
- **Confidence breakdown:** Distribution of classification categories (male, mostly_male, female, mostly_female, androgynous, unknown) overall and by country.

This data goes into a "Gender Inference Quality" section on the dashboard and in METHODOLOGY.md.

### 4. Gender Pair Categories

Each paper is coded by first author (FA) and last author (FA/LA) inferred gender:
- **WW** — Woman first author, Woman last author
- **WM** — Woman first author, Man last author
- **MW** — Man first author, Woman last author
- **MM** — Man first author, Man last author
- **UX/XU/UU** — Unknown/unclassifiable (excluded from analysis, reported separately)

### 5. Analysis Outputs

Per journal + aggregate:
- Gender composition over time (% WW/WM/MW/MM by year)
- Citation rate by gender pair (12-mo, 24-mo, 5-yr rolling IF variants)
- Normalized citation ratio: (actual citations / expected citations based on gender proportion)
- Citing-gender analysis: among papers that cite a given paper, what is the gender breakdown of the citers vs the cited?

### 6. Branch Strategy

- Branch: `gender-analysis` off `main`
- Separate site at `docs-gender/` (or configure GitHub Pages to serve from this branch)
- No changes to main IMPACT site until analysis is validated

---

## File Structure

### New Files (all on `gender-analysis` branch)

```
scripts/gender/
  infer_gender.py          — Main script: extract names → infer gender → store results
  analyze_citations.py     — Compute citation rates by gender pair per journal
  export_gender_json.py    — Export analysis results as JSON for frontend
  config.py                — API keys, thresholds, constants

src/pipeline/
  gender_inference.py      — Gender inference engine (pluggable: offline vs API)

data/
  gender_cache.db          — SQLite cache of name→gender lookups (persistent)

docs-gender/
  index.html               — Standalone analysis dashboard
  js/app.js                — Dashboard controller
  js/chart-manager.js      — Chart rendering (reuse patterns from main site)
  js/data-loader.js        — Load gender analysis JSON
  css/style.css            — Styling (fork from main site)
  data/                    — Exported JSON files

tests/
  test_gender_inference.py — Unit tests for gender inference
  test_citation_analysis.py — Unit tests for citation analysis
```

### Modified Files

```
src/pipeline/db_manager.py  — Add gender table schema + queries (on branch only)
```

---

## Chunk 1: Data Infrastructure

### Task 1: Create gender-analysis branch

**Files:**
- No file changes, git operation only

- [ ] **Step 1: Create and switch to branch**

```bash
git checkout -b gender-analysis main
```

- [ ] **Step 2: Verify branch**

```bash
git branch --show-current
```
Expected: `gender-analysis`

---

### Task 2: Create gender cache database schema

**Files:**
- Create: `scripts/gender/config.py`
- Create: `src/pipeline/gender_inference.py`
- Test: `tests/test_gender_inference.py`

- [ ] **Step 1: Write the config module**

```python
# scripts/gender/config.py
"""Configuration for gender analysis pipeline."""
import os

# Gender API settings (set in .env or environment)
GENDER_API_KEY = os.getenv("GENDER_API_KEY", "")

# Thresholds
GENDER_CONFIDENCE_THRESHOLD = 0.60  # Minimum probability to assign gender
MIN_FORENAME_LENGTH = 3             # Skip initials-only names

# Scope
START_YEAR = 2005
END_YEAR = 2026

# Paths
GENDER_CACHE_DB = "data/gender_cache.db"
IMPACT_DB = "data/impact.db"
PUBMED_BULK_DB = "data/pubmed_bulk.db"
```

- [ ] **Step 2: Write failing test for gender inference module**

```python
# tests/test_gender_inference.py
import pytest
from src.pipeline.gender_inference import (
    extract_forename,
    GenderInferenceEngine,
    GenderResult,
)

class TestExtractForename:
    def test_standard_name(self):
        assert extract_forename("Gill, Natasha") == "Natasha"

    def test_initials_only(self):
        assert extract_forename("Curtis, M J") is None  # Too short

    def test_single_name(self):
        assert extract_forename("Madonna") is None  # No comma

    def test_empty(self):
        assert extract_forename("") is None
        assert extract_forename(None) is None

    def test_hyphenated_first(self):
        assert extract_forename("Kim, Soo-Hyun") == "Soo-Hyun"

    def test_compound_first(self):
        assert extract_forename("Crump, Aria Davis") == "Aria"  # Use first token

class TestGenderResult:
    def test_creation(self):
        r = GenderResult(gender="female", probability=0.95, count=5000)
        assert r.gender == "female"
        assert r.is_confident(threshold=0.60)

    def test_below_threshold(self):
        r = GenderResult(gender="male", probability=0.55, count=10)
        assert not r.is_confident(threshold=0.60)
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_gender_inference.py -v
```
Expected: FAIL (module not found)

- [ ] **Step 4: Implement gender inference module**

```python
# src/pipeline/gender_inference.py
"""
Gender inference engine with pluggable backends.
Supports offline (gender-guesser) and API (Gender API) modes.
"""
import sqlite3
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class GenderResult:
    gender: Optional[str]   # "female", "male", or None
    probability: float      # 0.0 - 1.0
    count: int              # Sample size from API (0 for offline)

    def is_confident(self, threshold: float = 0.60) -> bool:
        return self.gender is not None and self.probability >= threshold

    def to_code(self, threshold: float = 0.60) -> str:
        """Return 'W', 'M', or 'U' (unknown)."""
        if not self.is_confident(threshold):
            return "U"
        return "W" if self.gender == "female" else "M"


def extract_forename(full_name: Optional[str]) -> Optional[str]:
    """
    Extract usable first name from 'LastName, ForeName' format.
    Returns None if name is missing, has no comma, or forename is
    initials-only (< 3 chars).
    """
    if not full_name or "," not in full_name:
        return None
    parts = full_name.split(",", 1)
    if len(parts) < 2:
        return None
    forename = parts[1].strip()
    if len(forename) < 3:
        return None
    # Use first token of compound names (e.g., "Aria Davis" → "Aria")
    first_token = forename.split()[0] if forename else None
    return first_token


class GenderCache:
    """SQLite-backed cache for gender lookups."""

    def __init__(self, db_path: str = "data/gender_cache.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS gender_lookups (
                first_name  TEXT PRIMARY KEY,
                gender      TEXT,
                probability REAL,
                count       INTEGER,
                source      TEXT,
                looked_up   TEXT DEFAULT (datetime('now'))
            )
        """)
        self.conn.commit()

    def get(self, name: str) -> Optional[GenderResult]:
        row = self.conn.execute(
            "SELECT gender, probability, count FROM gender_lookups WHERE first_name = ?",
            (name.lower(),)
        ).fetchone()
        if row:
            return GenderResult(gender=row[0], probability=row[1], count=row[2])
        return None

    def put(self, name: str, result: GenderResult, source: str = "api"):
        self.conn.execute(
            """INSERT OR REPLACE INTO gender_lookups
               (first_name, gender, probability, count, source)
               VALUES (?, ?, ?, ?, ?)""",
            (name.lower(), result.gender, result.probability, result.count, source)
        )
        self.conn.commit()

    def get_batch(self, names: List[str]) -> Dict[str, Optional[GenderResult]]:
        results = {}
        for name in names:
            results[name] = self.get(name)
        return results

    def close(self):
        self.conn.close()


class OfflineGenderEngine:
    """
    Free offline gender inference using gender-guesser package.
    Install: pip install gender-guesser
    Accuracy: ~80-85% (good for initial exploration).
    """

    def __init__(self):
        import gender_guesser.detector as gd
        self.detector = gd.Detector()

    def infer(self, first_name: str) -> GenderResult:
        result = self.detector.get_gender(first_name.capitalize())
        # gender-guesser returns: unknown, andy, male, female, mostly_male, mostly_female
        mapping = {
            "male": ("male", 0.95),
            "mostly_male": ("male", 0.75),
            "female": ("female", 0.95),
            "mostly_female": ("female", 0.75),
            "andy": (None, 0.50),       # Androgynous
            "unknown": (None, 0.0),
        }
        gender, prob = mapping.get(result, (None, 0.0))
        return GenderResult(gender=gender, probability=prob, count=0)

    def infer_batch(self, names: List[str]) -> Dict[str, GenderResult]:
        return {name: self.infer(name) for name in names}


class GenderAPIEngine:
    """
    Gender API (gender-api.com) — paid, high accuracy.
    Batch endpoint: up to 100 names per request.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://gender-api.com/v2/gender"

    def infer(self, first_name: str) -> GenderResult:
        import requests
        resp = requests.get(
            self.base_url,
            params={"name": first_name},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return GenderResult(
            gender=data.get("gender"),      # "male" or "female"
            probability=data.get("probability", 0),
            count=data.get("count", 0),
        )

    def infer_batch(self, names: List[str]) -> Dict[str, GenderResult]:
        """Batch lookup — up to 100 names per request."""
        import requests
        results = {}
        for i in range(0, len(names), 100):
            batch = names[i:i+100]
            resp = requests.post(
                self.base_url,
                json=[{"name": n} for n in batch],
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=30,
            )
            resp.raise_for_status()
            for item in resp.json():
                name = item.get("input", {}).get("name", batch[0])
                results[name] = GenderResult(
                    gender=item.get("gender"),
                    probability=item.get("probability", 0),
                    count=item.get("count", 0),
                )
        return results


class GenderInferenceEngine:
    """
    Orchestrator: cache-first, then backend (offline or API).
    """

    def __init__(self, cache: GenderCache, backend):
        self.cache = cache
        self.backend = backend

    def infer(self, first_name: str) -> GenderResult:
        cached = self.cache.get(first_name)
        if cached is not None:
            return cached
        result = self.backend.infer(first_name)
        source = "offline" if isinstance(self.backend, OfflineGenderEngine) else "api"
        self.cache.put(first_name, result, source=source)
        return result

    def infer_batch(self, names: List[str]) -> Dict[str, GenderResult]:
        """Batch inference with cache layer."""
        results = {}
        uncached = []
        for name in names:
            cached = self.cache.get(name)
            if cached is not None:
                results[name] = cached
            else:
                uncached.append(name)

        if uncached:
            backend_results = self.backend.infer_batch(uncached)
            source = "offline" if isinstance(self.backend, OfflineGenderEngine) else "api"
            for name, result in backend_results.items():
                self.cache.put(name, result, source=source)
                results[name] = result

        return results
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pip install gender-guesser  # For offline backend
python -m pytest tests/test_gender_inference.py -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/gender/config.py src/pipeline/gender_inference.py tests/test_gender_inference.py
git commit -m "feat: add gender inference engine with cache and pluggable backends"
```

---

### Task 3: Add gender columns to impact.db

**Files:**
- Modify: `src/pipeline/db_manager.py` (add migration + queries)
- Create: `scripts/gender/migrate_db.py`

- [ ] **Step 1: Create migration script**

```python
# scripts/gender/migrate_db.py
"""Add gender columns to papers table and create gender_stats table."""
import sqlite3
import sys

def migrate(db_path: str = "data/impact.db"):
    conn = sqlite3.connect(db_path)

    # Add gender columns to papers table
    existing = {row[1] for row in conn.execute("PRAGMA table_info(papers)")}

    new_cols = [
        ("first_author_forename", "TEXT"),
        ("last_author_forename", "TEXT"),
        ("first_author_gender", "TEXT"),   # 'W', 'M', 'U'
        ("first_author_gender_prob", "REAL"),
        ("last_author_gender", "TEXT"),
        ("last_author_gender_prob", "REAL"),
        ("gender_pair", "TEXT"),           # 'WW', 'WM', 'MW', 'MM', or NULL
    ]

    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE papers ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")

    # Create index on gender_pair for fast aggregation
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_gender_pair
        ON papers(gender_pair, journal_id, pub_year)
    """)

    # Create gender_stats table for precomputed per-journal aggregates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gender_citation_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id      INTEGER NOT NULL,
            snapshot_month   TEXT NOT NULL,       -- 'YYYY-MM'
            gender_pair     TEXT NOT NULL,         -- 'WW','WM','MW','MM'
            paper_count     INTEGER DEFAULT 0,
            citation_count  INTEGER DEFAULT 0,
            rolling_if_24m  REAL,
            rolling_if_12m  REAL,
            rolling_if_5yr  REAL,
            FOREIGN KEY (journal_id) REFERENCES journals(id),
            UNIQUE(journal_id, snapshot_month, gender_pair)
        )
    """)

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/impact.db"
    migrate(db_path)
```

- [ ] **Step 2: Run migration**

```bash
python scripts/gender/migrate_db.py
```
Expected: "Added column: ..." messages, "Migration complete."

- [ ] **Step 3: Verify schema**

```bash
sqlite3 data/impact.db "PRAGMA table_info(papers)" | grep gender
```
Expected: gender columns listed

- [ ] **Step 4: Commit**

```bash
git add scripts/gender/migrate_db.py
git commit -m "feat: add gender columns to papers table and gender_citation_stats table"
```

---

## Chunk 2: Gender Inference Pipeline

### Task 4: Build the name extraction + gender inference script

**Files:**
- Create: `scripts/gender/infer_gender.py`
- Test: `tests/test_gender_inference.py` (extend)

- [ ] **Step 1: Write failing test for name extraction from pubmed_bulk.db**

Add to `tests/test_gender_inference.py`:

```python
class TestNameExtraction:
    def test_extract_forename_long_name(self):
        assert extract_forename("Woitowich, Nicole C") == "Nicole"

    def test_extract_forename_non_latin(self):
        # Chinese/Korean names often stored as single token
        assert extract_forename("Wang, Xiaoming") == "Xiaoming"

    def test_extract_forename_initials_with_periods(self):
        assert extract_forename("Smith, J. R.") is None  # < 3 chars after cleanup
```

- [ ] **Step 2: Run test to verify it fails/passes as expected**

```bash
python -m pytest tests/test_gender_inference.py::TestNameExtraction -v
```

- [ ] **Step 3: Write the main inference script**

```python
# scripts/gender/infer_gender.py
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

BATCH_SIZE = 5000  # Papers to process per batch


def get_unique_forenames(pubmed_db: str, start_year: int, end_year: int) -> dict:
    """
    Extract unique first names from pubmed_bulk.db for papers in scope.
    Returns dict: {pmid: (first_forename, last_forename)}
    """
    conn = sqlite3.connect(pubmed_db)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute("""
        SELECT pmid, first_author, last_author
        FROM pubmed
        WHERE pub_year BETWEEN ? AND ?
          AND first_author IS NOT NULL
    """, (start_year, end_year))

    paper_names = {}
    unique_names = set()

    for row in cursor:
        fa = extract_forename(row["first_author"])
        la = extract_forename(row["last_author"])
        if fa or la:
            paper_names[row["pmid"]] = (fa, la)
            if fa:
                unique_names.add(fa.lower())
            if la:
                unique_names.add(la.lower())

    conn.close()
    return paper_names, unique_names


def run_inference(paper_names: dict, unique_names: set, engine: GenderInferenceEngine,
                  threshold: float) -> dict:
    """
    Infer gender for all unique names, then map back to papers.
    Returns dict: {pmid: (fa_gender, fa_prob, la_gender, la_prob, pair)}
    """
    name_list = sorted(unique_names)
    logger.info(f"Inferring gender for {len(name_list)} unique names...")

    # Process in batches
    for i in range(0, len(name_list), 500):
        batch = name_list[i:i+500]
        engine.infer_batch(batch)
        if (i + 500) % 10000 == 0:
            logger.info(f"  Processed {i + 500}/{len(name_list)} names")

    logger.info("Mapping gender to papers...")
    results = {}
    for pmid, (fa_name, la_name) in paper_names.items():
        fa_result = engine.cache.get(fa_name.lower()) if fa_name else None
        la_result = engine.cache.get(la_name.lower()) if la_name else None

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
            if count % 100000 == 0:
                logger.info(f"  Updated {count} papers")

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


def main():
    parser = argparse.ArgumentParser(description="Infer gender of paper authors")
    parser.add_argument("--mode", choices=["offline", "api"], default="offline",
                        help="offline = gender-guesser (free), api = Gender API (paid)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count unique names only, don't infer")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    args = parser.parse_args()

    logger.info(f"Extracting forenames from {PUBMED_BULK_DB} ({args.start_year}-{args.end_year})...")
    paper_names, unique_names = get_unique_forenames(
        PUBMED_BULK_DB, args.start_year, args.end_year
    )
    logger.info(f"Found {len(paper_names)} papers with usable names, "
                f"{len(unique_names)} unique first names")

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

    results = run_inference(paper_names, unique_names, engine, GENDER_CONFIDENCE_THRESHOLD)

    # Summary stats
    pairs = [r[4] for r in results.values() if r[4]]
    from collections import Counter
    pair_counts = Counter(pairs)
    logger.info(f"Gender pair distribution: {dict(pair_counts)}")
    logger.info(f"Papers with assignable gender pair: {len(pairs)}/{len(results)}")

    update_impact_db(IMPACT_DB, results)
    cache.close()

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Test dry-run mode**

```bash
python scripts/gender/infer_gender.py --dry-run --start-year 2023 --end-year 2023
```
Expected: Prints count of papers and unique names for 2023

- [ ] **Step 5: Test offline inference on small scope**

```bash
python scripts/gender/infer_gender.py --mode offline --start-year 2025 --end-year 2025
```
Expected: Processes 2025 papers, prints gender pair distribution

- [ ] **Step 6: Verify results in DB**

```bash
sqlite3 data/impact.db "SELECT gender_pair, COUNT(*) FROM papers WHERE gender_pair IS NOT NULL GROUP BY gender_pair"
```

- [ ] **Step 7: Commit**

```bash
git add scripts/gender/infer_gender.py tests/test_gender_inference.py
git commit -m "feat: add gender inference pipeline script with offline and API backends"
```

---

## Chunk 3: Citation Analysis by Gender

### Task 5: Compute citation rates by gender pair

**Files:**
- Create: `scripts/gender/analyze_citations.py`
- Test: `tests/test_citation_analysis.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_citation_analysis.py
import pytest
import sqlite3
import tempfile
import os

def create_test_db(path):
    """Create a minimal test database with gender-coded papers and citations."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE journals (id INTEGER PRIMARY KEY, name TEXT, slug TEXT);
        INSERT INTO journals VALUES (1, 'Test Journal', 'test-journal');

        CREATE TABLE papers (
            pmid INTEGER PRIMARY KEY, journal_id INTEGER,
            pub_year INTEGER, pub_month INTEGER, is_research INTEGER,
            gender_pair TEXT, first_author_gender TEXT, last_author_gender TEXT
        );
        -- 2023 papers: 3 MM, 2 WW, 2 WM, 1 MW
        INSERT INTO papers VALUES (1, 1, 2023, 1, 1, 'MM', 'M', 'M');
        INSERT INTO papers VALUES (2, 1, 2023, 2, 1, 'MM', 'M', 'M');
        INSERT INTO papers VALUES (3, 1, 2023, 3, 1, 'MM', 'M', 'M');
        INSERT INTO papers VALUES (4, 1, 2023, 1, 1, 'WW', 'W', 'W');
        INSERT INTO papers VALUES (5, 1, 2023, 2, 1, 'WW', 'W', 'W');
        INSERT INTO papers VALUES (6, 1, 2023, 3, 1, 'WM', 'W', 'M');
        INSERT INTO papers VALUES (7, 1, 2023, 4, 1, 'WM', 'W', 'M');
        INSERT INTO papers VALUES (8, 1, 2023, 5, 1, 'MW', 'M', 'W');

        CREATE TABLE citations (
            id INTEGER PRIMARY KEY, cited_pmid INTEGER, citing_pmid INTEGER,
            citing_year INTEGER, citing_month INTEGER
        );
        -- MM papers get more citations
        INSERT INTO citations VALUES (1, 1, 101, 2024, 1);
        INSERT INTO citations VALUES (2, 1, 102, 2024, 2);
        INSERT INTO citations VALUES (3, 2, 103, 2024, 3);
        INSERT INTO citations VALUES (4, 3, 104, 2024, 4);
        -- WW papers get fewer
        INSERT INTO citations VALUES (5, 4, 105, 2024, 1);
        INSERT INTO citations VALUES (6, 5, 106, 2024, 2);
        -- WM papers
        INSERT INTO citations VALUES (7, 6, 107, 2024, 1);
        INSERT INTO citations VALUES (8, 7, 108, 2024, 3);
        -- MW paper
        INSERT INTO citations VALUES (9, 8, 109, 2024, 2);
    """)
    conn.commit()
    conn.close()
    return path

class TestCitationByGender:
    def test_per_paper_citation_rate(self):
        """MM papers: 4 cites / 3 papers = 1.33; WW: 2/2 = 1.0"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            create_test_db(db_path)
            from scripts.gender.analyze_citations import compute_gender_citation_rates
            rates = compute_gender_citation_rates(db_path, journal_id=1, year=2023)
            assert rates["MM"]["papers"] == 3
            assert rates["MM"]["citations"] == 4
            assert abs(rates["MM"]["rate"] - 4/3) < 0.01
            assert rates["WW"]["papers"] == 2
            assert rates["WW"]["citations"] == 2
            assert abs(rates["WW"]["rate"] - 1.0) < 0.01
        finally:
            os.unlink(db_path)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_citation_analysis.py -v
```
Expected: FAIL

- [ ] **Step 3: Implement citation analysis**

```python
# scripts/gender/analyze_citations.py
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
"""
import argparse
import sqlite3
import json
import logging
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.gender.config import IMPACT_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GENDER_PAIRS = ["WW", "WM", "MW", "MM"]


def compute_gender_citation_rates(db_path: str, journal_id: int,
                                   year: int) -> Dict:
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
            "rate": cites / papers if papers > 0 else 0.0,
        }

    conn.close()
    return results


def compute_rolling_if_by_gender(db_path: str, journal_id: int,
                                  snapshot_month: str,
                                  paper_window_months: int = 24,
                                  cite_window_months: int = 12) -> Dict:
    """
    Compute rolling impact factor by gender pair for a specific snapshot month.

    Same formula as main IMPACT rolling IF, but stratified by gender_pair:
      citations_in_cite_window / papers_in_paper_window

    For each gender pair, the denominator is only papers of that pair.
    """
    conn = sqlite3.connect(db_path)

    # Parse snapshot month
    snap_year, snap_month = int(snapshot_month[:4]), int(snapshot_month[5:7])

    # Citation window: snap_month - cite_window_months + 1 .. snap_month
    # Paper window: snap_month - paper_window_months - cite_window_months + 1 .. snap_month - cite_window_months
    # (Papers published in the paper window, citations received in the cite window)

    results = {}
    for pair in GENDER_PAIRS:
        row = conn.execute("""
            WITH paper_window AS (
                SELECT pmid FROM papers
                WHERE journal_id = ?
                  AND gender_pair = ?
                  AND is_research = 1
                  AND (pub_year * 12 + pub_month)
                      BETWEEN (? * 12 + ? - ? - ? + 1)
                          AND (? * 12 + ? - ?)
            )
            SELECT
                COUNT(DISTINCT pw.pmid) as paper_count,
                COUNT(c.id) as citation_count
            FROM paper_window pw
            LEFT JOIN citations c ON c.cited_pmid = pw.pmid
                AND (c.citing_year * 12 + c.citing_month)
                    BETWEEN (? * 12 + ? - ? + 1)
                        AND (? * 12 + ?)
        """, (journal_id, pair,
              snap_year, snap_month, paper_window_months, cite_window_months,
              snap_year, snap_month, cite_window_months,
              snap_year, snap_month, cite_window_months,
              snap_year, snap_month)).fetchone()

        papers = row[0]
        cites = row[1]
        results[pair] = {
            "papers": papers,
            "citations": cites,
            "rolling_if": cites / papers if papers > 0 else None,
        }

    conn.close()
    return results


def compute_citing_gender_analysis(db_path: str, journal_id: int,
                                    year: int) -> Dict:
    """
    For papers in a journal+year, analyze the gender of citing authors.

    Question: When a paper by WW/WM/MW/MM is cited, what is the gender
    breakdown of the first author of the citing paper?

    Returns: {cited_pair: {citing_fa_gender: count}}
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
              AND cited_p.pub_year = ?
              AND cited_p.gender_pair = ?
              AND citing_p.first_author_gender IN ('W', 'M')
            GROUP BY citing_p.first_author_gender
        """, (journal_id, year, cited_pair)).fetchall()

        gender_counts = {"W": 0, "M": 0}
        for gender, count in row_data:
            gender_counts[gender] = count
        total = sum(gender_counts.values())
        results[cited_pair] = {
            "citing_W": gender_counts["W"],
            "citing_M": gender_counts["M"],
            "total": total,
            "pct_W": gender_counts["W"] / total * 100 if total > 0 else None,
            "pct_M": gender_counts["M"] / total * 100 if total > 0 else None,
        }

    conn.close()
    return results


def analyze_journal(db_path: str, journal_id: int, slug: str,
                    start_year: int, end_year: int) -> Dict:
    """Full gender-citation analysis for one journal."""
    results = {
        "slug": slug,
        "journal_id": journal_id,
        "yearly_rates": {},
        "citing_gender": {},
    }

    for year in range(start_year, end_year + 1):
        rates = compute_gender_citation_rates(db_path, journal_id, year)
        total_papers = sum(r["papers"] for r in rates.values())
        if total_papers > 0:
            results["yearly_rates"][str(year)] = rates

        citing = compute_citing_gender_analysis(db_path, journal_id, year)
        total_citing = sum(c["total"] for c in citing.values())
        if total_citing > 0:
            results["citing_gender"][str(year)] = citing

    # Rolling IF by gender (most recent available months)
    # Compute for last 12 months that have data
    results["rolling_if"] = {}
    for variant_name, paper_window in [("12m", 12), ("24m", 24), ("5yr", 60)]:
        results["rolling_if"][variant_name] = {}
        for year in range(max(start_year + 3, 2008), end_year + 1):
            for month in range(1, 13):
                snapshot = f"{year}-{month:02d}"
                rif = compute_rolling_if_by_gender(
                    db_path, journal_id, snapshot,
                    paper_window_months=paper_window
                )
                total_papers = sum(r["papers"] for r in rif.values())
                if total_papers > 0:
                    results["rolling_if"][variant_name][snapshot] = rif

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", type=str, help="Single journal slug")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--output-dir", type=str, default="docs-gender/data")
    args = parser.parse_args()

    conn = sqlite3.connect(IMPACT_DB)

    if args.journal:
        journals = conn.execute(
            "SELECT id, slug, name FROM journals WHERE slug = ?",
            (args.journal,)
        ).fetchall()
    else:
        journals = conn.execute(
            "SELECT id, slug, name FROM journals ORDER BY name"
        ).fetchall()
    conn.close()

    output_dir = Path(args.output_dir) / "gender"
    output_dir.mkdir(parents=True, exist_ok=True)

    for jid, slug, name in journals:
        logger.info(f"Analyzing {name} ({slug})...")
        result = analyze_journal(IMPACT_DB, jid, slug, args.start_year, args.end_year)

        out_path = output_dir / f"{slug}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, separators=(",", ":"))

        logger.info(f"  → {out_path}")

    logger.info(f"Done. Analyzed {len(journals)} journals.")

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
python -m pytest tests/test_citation_analysis.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/gender/analyze_citations.py tests/test_citation_analysis.py
git commit -m "feat: add citation analysis by gender pair with rolling IF and citing-gender breakdown"
```

---

### Task 6: Aggregate cross-journal statistics

**Files:**
- Create: `scripts/gender/compute_aggregates.py`

- [ ] **Step 1: Write aggregate computation script**

This computes the "headline numbers" — across all journals, what are the trends?

```python
# scripts/gender/compute_aggregates.py
"""
Compute aggregate gender-citation statistics across all journals.
Outputs summary JSON for the dashboard overview page.

Key metrics:
- Overall gender pair proportions over time
- Mean citation rate ratio (women-led vs men-led papers), controlling for paper count
- Citing-gender homophily index
"""
import sqlite3
import json
import logging
from pathlib import Path
from collections import defaultdict

from scripts.gender.config import IMPACT_DB, START_YEAR, END_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GENDER_PAIRS = ["WW", "WM", "MW", "MM"]


def compute_aggregate_stats(db_path: str, start_year: int, end_year: int) -> dict:
    conn = sqlite3.connect(db_path)

    stats = {
        "yearly_composition": {},
        "yearly_citation_rates": {},
        "yearly_normalized_rates": {},
        "overall": {},
    }

    for year in range(start_year, end_year + 1):
        # Gender composition
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
        stats["yearly_composition"][str(year)] = {
            pair: {"count": comp[pair], "pct": round(comp[pair] / total * 100, 2)}
            for pair in GENDER_PAIRS
        }
        stats["yearly_composition"][str(year)]["total"] = total

        # Citation rates by gender pair
        rate_rows = conn.execute("""
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
        for pair, papers, cites in rate_rows:
            rates[pair] = {
                "papers": papers,
                "citations": cites,
                "rate": round(cites / papers, 4) if papers > 0 else 0,
            }
        if rates:
            stats["yearly_citation_rates"][str(year)] = rates

            # Normalized: ratio relative to MM rate
            mm_rate = rates.get("MM", {}).get("rate", 0)
            if mm_rate > 0:
                stats["yearly_normalized_rates"][str(year)] = {
                    pair: round(r["rate"] / mm_rate, 4)
                    for pair, r in rates.items()
                }

    # Overall summary
    overall = conn.execute("""
        SELECT gender_pair, COUNT(DISTINCT pmid) as papers,
               (SELECT COUNT(*) FROM citations c
                JOIN papers p2 ON p2.pmid = c.cited_pmid
                WHERE p2.gender_pair = papers.gender_pair) as cites
        FROM papers
        WHERE gender_pair IN ('WW','WM','MW','MM')
          AND is_research = 1
          AND pub_year BETWEEN ? AND ?
        GROUP BY gender_pair
    """, (start_year, end_year)).fetchall()

    # Simpler overall query
    for pair in GENDER_PAIRS:
        row = conn.execute("""
            SELECT COUNT(DISTINCT p.pmid), COUNT(c.id)
            FROM papers p
            LEFT JOIN citations c ON c.cited_pmid = p.pmid
            WHERE p.gender_pair = ? AND p.is_research = 1
              AND p.pub_year BETWEEN ? AND ?
        """, (pair, start_year, end_year)).fetchone()
        stats["overall"][pair] = {
            "papers": row[0],
            "citations": row[1],
            "rate": round(row[1] / row[0], 4) if row[0] > 0 else 0,
        }

    conn.close()
    return stats


def main():
    output_dir = Path("docs-gender/data/gender")
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = compute_aggregate_stats(IMPACT_DB, START_YEAR, END_YEAR)

    out_path = output_dir / "aggregate.json"
    with open(out_path, "w") as f:
        json.dump(stats, f, indent=2)
    logger.info(f"Aggregate stats written to {out_path}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/gender/compute_aggregates.py
git commit -m "feat: add aggregate cross-journal gender-citation statistics"
```

---

## Chunk 4: Frontend Dashboard

### Task 7: Create standalone gender analysis dashboard

**Files:**
- Create: `docs-gender/index.html`
- Create: `docs-gender/js/app.js`
- Create: `docs-gender/js/chart-manager.js`
- Create: `docs-gender/css/style.css`
- Create: `docs-gender/js/data-loader.js`

This is a standalone site on the `gender-analysis` branch. It visualizes:
1. **Overview**: Aggregate gender composition trends + citation rate ratios
2. **Journal detail**: Per-journal gender breakdown with 12m/24m/5yr IF by gender pair
3. **Citing analysis**: Who cites whom — gender homophily in citations

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p docs-gender/{js,css,data/gender}
```

- [ ] **Step 2: Write index.html**

Minimal structure with sections for overview, journal detail, and citing analysis. Uses Chart.js from CDN (same as main site).

- [ ] **Step 3: Write data-loader.js**

Loads JSON from `data/gender/` directory. Similar pattern to main site but simpler.

- [ ] **Step 4: Write chart-manager.js**

Chart types needed:
- **Stacked area chart**: Gender pair composition over time (% WW/WM/MW/MM by year)
- **Grouped bar chart**: Citation rate by gender pair per year
- **Line chart**: Normalized citation ratio over time (MM=1.0 baseline)
- **Rolling IF overlay**: 12m/24m/5yr IF lines, one per gender pair
- **Heatmap or grouped bar**: Citing-gender analysis (who cites whom)

Color scheme (colorblind-safe, distinct from main site):
- WW: `#882255` (wine)
- WM: `#CC6677` (rose)
- MW: `#44AA99` (teal)
- MM: `#332288` (indigo)

- [ ] **Step 5: Write app.js**

Main controller: journal picker, section switching, chart rendering.

- [ ] **Step 6: Write CSS**

Fork key styles from main site, adapt for gender analysis dashboard.

- [ ] **Step 7: Test locally**

```bash
python -m http.server -d docs-gender 8001
# Open http://localhost:8001
```

- [ ] **Step 8: Commit**

```bash
git add docs-gender/
git commit -m "feat: add standalone gender-citation analysis dashboard"
```

---

## Chunk 5: Validation and Documentation

### Task 8: Add validation and limitations documentation

**Files:**
- Create: `scripts/gender/validate_gender.py`
- Create: `docs-gender/METHODOLOGY.md`

- [ ] **Step 1: Write validation script**

Checks to run:
- Coverage: What % of papers have gender assignments?
- Confidence distribution: histogram of gender probabilities
- Known-name validation: spot-check common names
- East Asian name accuracy: these are systematically harder for gender APIs
- Single-author papers: ensure FA == LA gender pair is correct

```python
# scripts/gender/validate_gender.py
"""Validate gender inference quality and coverage."""
```

- [ ] **Step 2: Write methodology documentation**

Key points to document:
- Gender is *inferred* from first names — this is a probabilistic estimate, not self-reported identity
- Binary classification (W/M) is a limitation — non-binary identities are not captured
- East Asian, African, and other naming conventions have lower accuracy
- Initials-only names (~5-10% of papers) are excluded (coded as "U")
- Confidence threshold of 0.60 used (document sensitivity analysis)
- "Gender" here refers to the *likely gender associated with a given name* — this is a proxy
- Results should be interpreted as population-level trends, not individual-level claims

- [ ] **Step 3: Commit**

```bash
git add scripts/gender/validate_gender.py docs-gender/METHODOLOGY.md
git commit -m "docs: add gender analysis validation and methodology documentation"
```

---

## Execution Order

```
Task 1: Create branch                    (1 min)
Task 2: Gender inference engine + cache   (15 min)
Task 3: DB migration                      (5 min)
Task 4: Name extraction + inference       (20 min code, hours to run on full data)
Task 5: Citation analysis per journal     (20 min)
Task 6: Aggregate statistics              (10 min)
Task 7: Frontend dashboard                (60 min)
Task 8: Validation + methodology          (20 min)
```

**Total coding time:** ~2.5 hours
**Data processing time:** ~2-6 hours (one-time, for 23.5M papers with offline engine)
**Gender API cost (if used):** ~1.3M unique names × $0.10/1000 = ~$130

## Decisions Made

1. **Gender inference:** `gender-guesser` (free, offline) — upgrade to paid API later if needed
2. **Scope:** All 8,630 journals, all 24M papers in PubMed DB
3. **Deployment:** GitHub Pages from `gender-analysis` branch (`docs-gender/`)
4. **Citing-gender:** Limited to citations within PubMed-indexed set (24M papers) — acceptable
5. **Statistics:** Descriptive only for now, significance testing can be added later
6. **Accuracy tracking:** Must track gender-guesser performance by gender (M vs F) and by author country (US, China, Japan, etc.)
