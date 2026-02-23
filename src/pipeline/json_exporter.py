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

    def export_journal(self, slug: str, name: str, issn: str,
                       timeseries: List[Dict],
                       official_if: float = None) -> str:
        """
        Export a journal's full timeseries to a JSON file.

        Args:
            slug: URL-friendly journal name (e.g., 'aging-cell')
            name: Full journal name
            issn: Journal ISSN
            timeseries: List of monthly snapshot dicts from ImpactCalculator
            official_if: Official JIF for comparison (optional)

        Returns:
            Path to the exported file
        """
        # Get the latest data point
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
            "timeseries": [
                {
                    "month": s["month"],
                    "rolling_if": s["rolling_if"],
                    "rolling_if_no_reviews": s["rolling_if_no_reviews"],
                    "papers": s["paper_count"],
                    "research": s.get("research_count", 0),
                    "reviews": s["review_count"],
                    "citations": s["citation_count"],
                }
                for s in timeseries
            ],
        }

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
