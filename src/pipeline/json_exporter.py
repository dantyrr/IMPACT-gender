"""
IMPACT JSON Exporter
Exports computed metrics as JSON files for the GitHub Pages website.
"""

import json
import os
from datetime import datetime
from typing import List, Dict
import logging

from src.pipeline.config import WEBSITE_DATA_DIR, SNAPSHOTS_DIR

logger = logging.getLogger(__name__)


class JSONExporter:
    """Exports IMPACT data as JSON for the static website."""

    def __init__(self, data_dir: str = WEBSITE_DATA_DIR,
                 journals_dir: str = SNAPSHOTS_DIR):
        self.data_dir = data_dir
        self.journals_dir = journals_dir
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.journals_dir, exist_ok=True)

    @staticmethod
    def _format_timeseries(timeseries: List[Dict]) -> List[Dict]:
        """Convert raw calculator snapshots to compact JSON-ready dicts."""
        return [
            {
                "month": s["month"],
                "rolling_if": s["rolling_if"],
                "rolling_if_no_reviews": s["rolling_if_no_reviews"],
                "papers": s["paper_count"],
                "research": s.get("research_count", 0),
                "reviews": s["review_count"],
                "citations": s["citation_count"],
                "by_type": s.get("by_type", {}),
            }
            for s in timeseries
        ]

    def export_journal(self, slug: str, name: str, issn: str,
                       timeseries: List[Dict],
                       timeseries_12mo: List[Dict] = None,
                       timeseries_5yr: List[Dict] = None,
                       official_if: float = None) -> str:
        """
        Export a journal's full timeseries to a JSON file.

        Args:
            slug: URL-friendly journal name (e.g., 'aging-cell')
            name: Full journal name
            issn: Journal ISSN
            timeseries: 24-mo window monthly snapshots (default)
            timeseries_12mo: 12-mo paper window snapshots (optional)
            timeseries_5yr: 5-yr yr2-6 paper window snapshots (optional)
            official_if: Official JIF for comparison (optional)

        Returns:
            Path to the exported file
        """
        latest = timeseries[-1] if timeseries else {}

        data = {
            "journal": name,
            "slug": slug,
            "issn": issn,
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "official_jif_2024": official_if,
            "latest": {
                "month": latest.get("month", ""),
                "rolling_if": latest.get("rolling_if", 0),
                "rolling_if_no_reviews": latest.get("rolling_if_no_reviews", 0),
                "paper_count": latest.get("paper_count", 0),
                "research_count": latest.get("research_count", 0),
                "review_count": latest.get("review_count", 0),
                "citation_count": latest.get("citation_count", 0),
            },
            "timeseries": self._format_timeseries(timeseries),
        }

        if timeseries_12mo is not None:
            data["timeseries_12mo"] = self._format_timeseries(timeseries_12mo)
        if timeseries_5yr is not None:
            data["timeseries_5yr"] = self._format_timeseries(timeseries_5yr)

        filepath = os.path.join(self.journals_dir, f"{slug}.json")
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Exported {name} → {filepath} ({len(timeseries)} months)")
        return filepath

    def export_index(self, journals: List[Dict]) -> str:
        """
        Export index.json listing all available journals.

        Args:
            journals: List of dicts with keys: slug, name, issn, latest_if
        """
        data = {
            "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "journal_count": len(journals),
            "journals": journals,
        }

        filepath = os.path.join(self.data_dir, "index.json")
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Exported index → {filepath} ({len(journals)} journals)")
        return filepath

    @staticmethod
    def _format_affiliation(row: Dict, prefix: str) -> str:
        """Build a readable affiliation string from structured DB fields."""
        parts = []
        inst = row.get(f"{prefix}_institution")
        if inst:
            parts.append(inst)
        city = row.get(f"{prefix}_city")
        state = row.get(f"{prefix}_state")
        city_state = ", ".join(filter(None, [city, state]))
        if city_state:
            parts.append(city_state)
        country = row.get(f"{prefix}_country")
        if country:
            parts.append(country)
        return ", ".join(parts) if parts else ""

    def export_journal_authors(self, slug: str, rows: List[Dict]) -> str:
        """
        Export per-PMID author data for one journal.

        Args:
            slug: Journal slug (e.g., 'jci')
            rows: Records from db.get_paper_authors_for_journal()

        Returns:
            Path to the exported file
        """
        authors_dir = os.path.join(self.data_dir, "authors")
        os.makedirs(authors_dir, exist_ok=True)

        entries = {}
        for row in rows:
            first_name = row.get("first_author_name") or ""
            last_name = row.get("last_author_name") or ""
            first_aff = self._format_affiliation(row, "first_author")
            last_aff = self._format_affiliation(row, "last_author")

            entry = {}
            if first_name:
                entry["f"] = first_name
            if first_aff:
                entry["fa"] = first_aff
            if last_name:
                entry["l"] = last_name
            if last_aff:
                entry["la"] = last_aff

            if entry:
                entries[str(row["pmid"])] = entry

        data = {
            "slug": slug,
            "generated": datetime.now().strftime("%Y-%m-%d"),
            "authors": entries,
        }

        filepath = os.path.join(authors_dir, f"{slug}.json")
        with open(filepath, "w") as f:
            json.dump(data, f, separators=(",", ":"))  # compact, no whitespace

        size_kb = os.path.getsize(filepath) // 1024
        logger.info(f"Exported author data → {filepath} ({len(entries)} entries, {size_kb} KB)")
        return filepath

    def export_journal_papers(self, slug: str, rows: list,
                              geo_rows: list = None,
                              cits_by_year: dict = None,
                              cits_by_month: dict = None,
                              journal_monthly_cits: dict = None) -> str:
        """
        Export per-paper data for the papers browser tab, plus country-by-year geo data.
        Saves to docs/data/papers/{slug}.json

        cits_by_year: optional {pmid_str: {year_str: count}} from
                      db.get_citations_by_year_for_pmids(); adds a compact "cy"
                      field to each entry for exact Influence-tab computation.
        cits_by_month: optional {pmid_str: {"YYYY-MM": count}} from
                       db.get_citations_by_month_for_pmids(); adds "cm" field.
        journal_monthly_cits: optional {"YYYY-MM": total} from
                              db.get_journal_monthly_citations(); adds top-level
                              "monthly_cits" field for the Influence monthly chart.
        """
        papers_dir = os.path.join(self.data_dir, "papers")
        os.makedirs(papers_dir, exist_ok=True)

        entries = []
        for row in rows:
            entry = {
                "pmid": row["pmid"],
                "y": row.get("pub_year"),
                "m": row.get("pub_month"),
                "c": row.get("citation_count", 0),
            }
            title = row.get("title") or ""
            if title:
                entry["t"] = title[:120]
            fa = row.get("first_author_name") or ""
            if fa:
                entry["fa"] = fa
            fc = row.get("first_author_country") or ""
            if fc:
                entry["fc"] = fc
            la = row.get("last_author_name") or ""
            if la:
                entry["la"] = la
            lc = row.get("last_author_country") or ""
            if lc:
                entry["lc"] = lc
            pt = row.get("pub_type") or ""
            if pt:
                entry["pt"] = pt
            cy = (cits_by_year or {}).get(str(row["pmid"]))
            if cy:
                entry["cy"] = cy  # {year_str: count} — used by Influence tab
            cm = (cits_by_month or {}).get(str(row["pmid"]))
            if cm:
                entry["cm"] = cm  # {"YYYY-MM": count} — Influence monthly chart
            entries.append(entry)

        # Build compact geo summary: {year: {country: count}}
        geo = {}
        for row in (geo_rows or []):
            yr = str(row["pub_year"])
            country = row["first_author_country"]
            n = row["n"]
            if yr not in geo:
                geo[yr] = {}
            geo[yr][country] = n

        data = {
            "slug": slug,
            "generated": datetime.now().strftime("%Y-%m-%d"),
            "papers": entries,
        }
        if geo:
            data["geo"] = geo
        if journal_monthly_cits:
            data["monthly_cits"] = journal_monthly_cits

        path = os.path.join(papers_dir, f"{slug}.json")
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))

        size_kb = os.path.getsize(path) // 1024
        logger.info(f"Exported papers data → {path} ({len(entries)} entries, {size_kb} KB)")
        return path

    def export_author_profile(self, author_name: str,
                               metrics: Dict) -> str:
        """Export author metrics as JSON."""
        os.makedirs(os.path.join(self.data_dir, "authors"), exist_ok=True)

        safe_name = author_name.lower().replace(" ", "-")
        filepath = os.path.join(self.data_dir, "authors", f"{safe_name}.json")

        data = {
            "author": author_name,
            "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "metrics": metrics,
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Exported author profile → {filepath}")
        return filepath
