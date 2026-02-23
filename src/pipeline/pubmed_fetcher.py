"""
IMPACT PubMed Fetcher
Queries NCBI E-utilities to find papers by journal and fetch metadata.
"""

import time
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import logging

from src.pipeline.config import (
    PUBMED_BASE_URL, PUBMED_API_KEY, PUBMED_EMAIL, PUBMED_RATE_LIMIT
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for API calls."""

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


class PubMedFetcher:
    """Fetches paper data from PubMed E-utilities."""

    def __init__(self, api_key: str = PUBMED_API_KEY,
                 email: str = PUBMED_EMAIL):
        self.api_key = api_key
        self.email = email
        self.rate_limiter = RateLimiter(PUBMED_RATE_LIMIT)

    def _base_params(self) -> Dict:
        """Common params for all requests."""
        params = {"tool": "IMPACT", "email": self.email}
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    # ------------------------------------------------------------------ #
    #  ESearch — find PMIDs by journal
    # ------------------------------------------------------------------ #

    def search_journal(self, issn: str, year_start: int,
                       year_end: int) -> List[int]:
        """
        Search PubMed for all papers in a journal within a year range.
        Returns list of PMIDs.
        """
        query = f'{issn}[IS] AND {year_start}:{year_end}[PDAT]'
        pmids = []
        retstart = 0
        retmax = 10000  # fetch in chunks

        while True:
            self.rate_limiter.wait()
            params = {
                **self._base_params(),
                "db": "pubmed",
                "term": query,
                "retmax": retmax,
                "retstart": retstart,
                "retmode": "json",
            }

            resp = requests.get(
                f"{PUBMED_BASE_URL}/esearch.fcgi", params=params, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            result = data.get("esearchresult", {})
            id_list = result.get("idlist", [])
            total_count = int(result.get("count", 0))

            pmids.extend(int(pid) for pid in id_list)
            logger.info(
                f"ESearch: fetched {len(pmids)}/{total_count} PMIDs "
                f"for ISSN {issn} ({year_start}-{year_end})"
            )

            retstart += retmax
            if retstart >= total_count:
                break

        return pmids

    # ------------------------------------------------------------------ #
    #  ESummary — fetch paper metadata
    # ------------------------------------------------------------------ #

    def fetch_paper_details(self, pmids: List[int],
                            batch_size: int = 200) -> List[Dict]:
        """
        Fetch metadata for a list of PMIDs using ESummary.
        Returns list of paper dicts.
        """
        papers = []

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i: i + batch_size]
            batch_str = ",".join(str(p) for p in batch)

            self.rate_limiter.wait()
            params = {
                **self._base_params(),
                "db": "pubmed",
                "id": batch_str,
                "retmode": "json",
            }

            resp = requests.get(
                f"{PUBMED_BASE_URL}/esummary.fcgi", params=params, timeout=60
            )
            resp.raise_for_status()
            data = resp.json()

            result = data.get("result", {})

            for pid_str in result.get("uids", []):
                article = result.get(pid_str, {})
                if not article or "error" in article:
                    continue

                parsed = self._parse_esummary_article(article)
                if parsed:
                    papers.append(parsed)

            logger.info(
                f"ESummary: fetched {len(papers)} papers "
                f"(batch {i // batch_size + 1})"
            )

        return papers

    def _parse_esummary_article(self, article: Dict) -> Optional[Dict]:
        """Parse an ESummary article record into our standard format."""
        try:
            pmid = int(article.get("uid", 0))
            title = article.get("title", "").strip()
            if not pmid or not title:
                return None

            # Parse publication date
            pub_date_str = article.get("pubdate", "")
            pub_year, pub_month, pub_date = self._parse_pub_date(pub_date_str)
            if not pub_year:
                # Try epubdate as fallback
                epub = article.get("epubdate", "")
                pub_year, pub_month, pub_date = self._parse_pub_date(epub)

            if not pub_year:
                logger.warning(f"PMID {pmid}: Could not parse date '{pub_date_str}'")
                return None

            # Determine publication type
            pub_types = article.get("pubtype", [])
            pub_type = self._classify_pub_type(pub_types)
            is_research = 1 if pub_type == "Journal Article" else 0

            # DOI
            doi = ""
            for id_entry in article.get("articleids", []):
                if id_entry.get("idtype") == "doi":
                    doi = id_entry.get("value", "")
                    break

            return {
                "pmid": pmid,
                "title": title,
                "pub_date": pub_date,
                "pub_year": pub_year,
                "pub_month": pub_month,
                "pub_type": pub_type,
                "is_research": is_research,
                "doi": doi,
            }
        except Exception as e:
            logger.warning(f"Error parsing article: {e}")
            return None

    def _parse_pub_date(self, date_str: str):
        """
        Parse PubMed date strings like '2024 Jan 15', '2024 Mar', '2024'.
        Returns (year, month, date_iso) or (None, None, None).
        """
        if not date_str:
            return None, None, None

        MONTH_MAP = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }

        parts = date_str.strip().split()
        year = None
        month = 1
        day = 1

        if len(parts) >= 1:
            try:
                year = int(parts[0])
            except ValueError:
                return None, None, None

        if len(parts) >= 2:
            # Could be "Jan" or "Jan-Feb" or a number
            month_part = parts[1].split("-")[0]  # Handle "Jan-Feb" → "Jan"
            if month_part in MONTH_MAP:
                month = MONTH_MAP[month_part]
            else:
                try:
                    month = int(month_part)
                except ValueError:
                    pass

        if len(parts) >= 3:
            try:
                day = int(parts[2])
            except ValueError:
                pass

        date_iso = f"{year}-{month:02d}-{day:02d}"
        return year, month, date_iso

    def _classify_pub_type(self, pub_types: List[str]) -> str:
        """
        Classify a paper based on its PubMed publication types.
        Priority: Review > Editorial > Letter > Journal Article
        """
        review_types = {
            "Review", "Systematic Review", "Meta-Analysis",
            "Practice Guideline", "Guideline", "Consensus Development Conference",
        }
        non_research = {
            "Editorial", "Letter", "Comment", "Published Erratum",
            "Retraction of Publication", "News",
        }

        for pt in pub_types:
            if pt in review_types:
                return "Review"
        for pt in pub_types:
            if pt in non_research:
                return pt
        if "Journal Article" in pub_types:
            return "Journal Article"
        return pub_types[0] if pub_types else "Unknown"
