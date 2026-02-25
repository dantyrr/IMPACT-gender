"""
IMPACT PubMed Fetcher
Queries NCBI E-utilities to find papers by journal and fetch metadata.
"""

import re
import json
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

    @staticmethod
    def _safe_json(resp) -> Dict:
        """Parse JSON, allowing stray control chars if needed (PubMed API quirk)."""
        try:
            return resp.json()
        except Exception:
            # strict=False allows control chars (\x00-\x1f) inside JSON strings
            return json.loads(resp.text, strict=False)

    # ------------------------------------------------------------------ #
    #  ESearch — find PMIDs by journal
    # ------------------------------------------------------------------ #

    def search_journal(self, issn: str, year_start: int,
                       year_end: int) -> List[int]:
        """
        Search PubMed for all papers in a journal within a year range.
        PubMed limits results to 9999 per query, so if the total exceeds that
        we automatically split into per-year sub-queries.
        Returns list of PMIDs (deduplicated).
        """
        pmids_set: set = set()
        pmids_set.update(self._esearch_range(issn, year_start, year_end))
        return list(pmids_set)

    def _esearch_range(self, issn: str, year_start: int, year_end: int) -> List[int]:
        """
        Run ESearch for a year range. If the result exceeds 9999 records,
        splits into individual years (and halves if a single year still exceeds limit).
        """
        query = f'{issn}[IS] AND {year_start}:{year_end}[PDAT]'

        # First: get just the count (retmax=0 is fast)
        self.rate_limiter.wait()
        resp = requests.get(
            f"{PUBMED_BASE_URL}/esearch.fcgi",
            params={
                **self._base_params(),
                "db": "pubmed",
                "term": query,
                "retmax": 0,
                "retmode": "xml",
            },
            timeout=30,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        count_el = root.find("Count")
        total_count = int(count_el.text) if count_el is not None else 0

        logger.info(
            f"ESearch: {total_count} PMIDs for ISSN {issn} ({year_start}-{year_end})"
        )

        if total_count == 0:
            return []

        # If too many for one query, split by year
        if total_count > 9999 and year_start < year_end:
            pmids: List[int] = []
            for yr in range(year_start, year_end + 1):
                pmids.extend(self._esearch_range(issn, yr, yr))
            return pmids

        # Fetch all results (total_count <= 9999)
        self.rate_limiter.wait()
        resp = requests.get(
            f"{PUBMED_BASE_URL}/esearch.fcgi",
            params={
                **self._base_params(),
                "db": "pubmed",
                "term": query,
                "retmax": 9999,
                "retstart": 0,
                "retmode": "xml",
            },
            timeout=30,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        pmids = [int(el.text) for el in root.findall(".//Id") if el.text]
        logger.info(
            f"ESearch: fetched {len(pmids)}/{total_count} PMIDs "
            f"for ISSN {issn} ({year_start}-{year_end})"
        )
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
            data = self._safe_json(resp)

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

    # ------------------------------------------------------------------ #
    #  EFetch — author names and affiliations
    # ------------------------------------------------------------------ #

    def fetch_author_details(self, pmids: List[int],
                             batch_size: int = 100) -> List[Dict]:
        """
        Fetch first and last author name + parsed affiliation for each PMID.
        Uses PubMed EFetch XML.
        Returns list of dicts ready for db.update_paper_authors_bulk().
        """
        results = []

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i: i + batch_size]
            batch_str = ",".join(str(p) for p in batch)

            self.rate_limiter.wait()
            resp = requests.get(
                f"{PUBMED_BASE_URL}/efetch.fcgi",
                params={
                    **self._base_params(),
                    "db": "pubmed",
                    "id": batch_str,
                    "retmode": "xml",
                    "rettype": "abstract",
                },
                timeout=60,
            )
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            for article in root.findall(".//PubmedArticle"):
                pmid_el = article.find(".//PMID")
                if pmid_el is None:
                    continue
                pmid = int(pmid_el.text)

                author_els = article.findall(".//AuthorList/Author")
                if not author_els:
                    results.append({
                        "pmid": pmid,
                        **{k: None for k in (
                            "first_author_name", "first_author_institution",
                            "first_author_city", "first_author_state",
                            "first_author_country", "last_author_name",
                            "last_author_institution", "last_author_city",
                            "last_author_state", "last_author_country",
                        )},
                    })
                    continue

                def _extract(el):
                    last = el.findtext("LastName") or ""
                    initials = el.findtext("Initials") or ""
                    name = f"{last} {initials}".strip() or None
                    aff_raw = el.findtext(".//AffiliationInfo/Affiliation") or ""
                    parsed = self._parse_affiliation(aff_raw)
                    return name, parsed

                first_name, first_aff = _extract(author_els[0])
                last_name, last_aff = _extract(author_els[-1])

                results.append({
                    "pmid": pmid,
                    "first_author_name":        first_name,
                    "first_author_institution": first_aff.get("institution"),
                    "first_author_city":        first_aff.get("city"),
                    "first_author_state":       first_aff.get("state"),
                    "first_author_country":     first_aff.get("country"),
                    "last_author_name":         last_name,
                    "last_author_institution":  last_aff.get("institution"),
                    "last_author_city":         last_aff.get("city"),
                    "last_author_state":        last_aff.get("state"),
                    "last_author_country":      last_aff.get("country"),
                })

            logger.info(
                f"EFetch authors: batch {i // batch_size + 1} "
                f"({len(batch)} PMIDs, {len(results)} total)"
            )

        return results

    @staticmethod
    def _parse_affiliation(raw: str) -> Dict:
        """
        Parse a PubMed affiliation string into structured fields.
        Returns dict: institution, city, state (US/Canada only), country.
        All values may be None if not parseable.
        """
        if not raw:
            return {"institution": None, "city": None,
                    "state": None, "country": None}

        DEPT_RE = re.compile(
            r'^(dept\.?|department|division|div\.?|laboratory|lab\.?|'
            r'center|centre|school|college|graduate|program|unit|section|'
            r'group|institute of|institutes of)\b',
            re.IGNORECASE,
        )
        US_NAMES  = {"USA", "United States", "US", "U.S.A.",
                     "United States of America"}
        CA_NAMES  = {"Canada"}

        # Strip trailing email address
        s = re.sub(r'\s*[\w.+-]+@[\w.-]+\.\w+\.?\s*$', '', raw)
        s = s.strip().rstrip('.;').strip()

        parts = [p.strip() for p in s.split(',') if p.strip()]

        # Skip leading department / division tokens
        start = 0
        while start < len(parts) - 1 and DEPT_RE.match(parts[start]):
            start += 1
        parts = parts[start:]

        out = {"institution": None, "city": None,
               "state": None, "country": None}
        if not parts:
            return out

        out["institution"] = parts[0]
        if len(parts) == 1:
            return out

        # Country: last part, strip any trailing postal/zip tokens.
        # Also handles "35205 USA" where zip precedes country name.
        country_raw = re.sub(r'\s+\d[\d\s-]*$', '', parts[-1]).strip()
        if re.match(r'^\d', country_raw):
            # Starts with digit (e.g. "35205 USA") — extract trailing alpha word(s)
            m_c = re.search(r'([A-Za-z].*)$', country_raw)
            country_raw = m_c.group(1).strip() if m_c else ''
        if country_raw and not re.match(r'^\d', country_raw):
            out["country"] = country_raw

        if len(parts) < 3:
            return out

        is_us = out["country"] in US_NAMES
        is_ca = out["country"] in CA_NAMES

        if is_us or is_ca:
            # Penultimate part should be "ST" or "ST ZIPCODE" (including
            # Canadian postal codes with spaces like "ON M5S 1A8")
            state_part = parts[-2]
            m = re.match(r'^([A-Z]{2})(?:\s+.*)?$', state_part)
            if m:
                out["state"] = m.group(1)
                if len(parts) >= 4:
                    out["city"] = parts[-3]
            else:
                # Doesn't look like a state — treat as city
                out["city"] = re.sub(r'\s+\d{4,}$', '', state_part).strip() or None
        else:
            # International: penultimate part is city (strip postal codes)
            city_raw = parts[-2]
            c = re.sub(r'\s+\d[\d\s-]{3,}$', '', city_raw).strip()   # numeric postal
            c = re.sub(r'\s+[A-Z]{1,2}\d[\w ]{2,}$', '', c).strip()  # UK/CA style
            c = re.sub(r'\s+[A-Z0-9-]{4,}$', '', c).strip()           # generic
            out["city"] = c if c and not re.match(r'^\d{4,}$', c) else None

        return out

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
