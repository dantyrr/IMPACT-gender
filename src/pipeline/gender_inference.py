"""
Gender inference engine with pluggable backends.
Supports offline (gender-guesser) and API (Gender API) modes.
"""
import sqlite3
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict
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
    # Use first token of compound names (e.g., "Aria Davis" -> "Aria")
    first_token = forename.split()[0] if forename else None
    if not first_token or len(first_token) < 3:
        return None
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
    Gender API (gender-api.com) -- paid, high accuracy.
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
            gender=data.get("gender"),
            probability=data.get("probability", 0),
            count=data.get("count", 0),
        )

    def infer_batch(self, names: List[str]) -> Dict[str, GenderResult]:
        """Batch lookup -- up to 100 names per request."""
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
