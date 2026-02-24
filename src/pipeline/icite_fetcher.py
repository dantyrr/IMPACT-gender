"""
IMPACT iCite Fetcher
Queries the NIH iCite API for citation data, including cited_by lists.
"""

import time
import requests
from typing import List, Dict
import logging

from src.pipeline.config import ICITE_BASE_URL, ICITE_RATE_LIMIT

logger = logging.getLogger(__name__)


class IciteFetcher:
    """Fetches citation data from the NIH iCite API."""

    # iCite accepts up to 1000 PMIDs, but GET URL length limits cause
    # 414 errors with large batches. 200 keeps URLs safely short.
    MAX_BATCH_SIZE = 200

    def __init__(self):
        self.min_interval = 1.0 / ICITE_RATE_LIMIT
        self.last_request_time = 0.0

    def _wait(self):
        """Rate limit enforcement."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def fetch_batch(self, pmids: List[int]) -> Dict[int, Dict]:
        """
        Fetch iCite data for a list of PMIDs.
        Automatically chunks into batches of 1000.

        Returns dict mapping PMID → iCite record with keys:
            pmid, year, title, journal, doi, citation_count,
            cited_by (list of ints), is_research_article (bool),
            relative_citation_ratio, expected_citations_per_year, etc.
        """
        results = {}

        for i in range(0, len(pmids), self.MAX_BATCH_SIZE):
            chunk = pmids[i: i + self.MAX_BATCH_SIZE]
            chunk_str = ",".join(str(p) for p in chunk)

            self._wait()
            try:
                resp = requests.get(
                    ICITE_BASE_URL,
                    params={"pmids": chunk_str, "format": "json"},
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                logger.error(f"iCite API error for batch {i}: {e}")
                continue

            records = data.get("data", [])
            for record in records:
                pmid = record.get("pmid")
                if pmid is None:
                    continue

                # Normalize cited_by: iCite returns space-separated string or list
                cited_by_raw = record.get("cited_by", [])
                if isinstance(cited_by_raw, str):
                    cited_by = [int(x) for x in cited_by_raw.split() if x.strip()]
                elif isinstance(cited_by_raw, list):
                    cited_by = [int(x) for x in cited_by_raw if x]
                else:
                    cited_by = []

                record["cited_by"] = cited_by
                record["pmid"] = int(pmid)
                results[int(pmid)] = record

            logger.info(
                f"iCite: fetched {len(results)} records "
                f"(batch {i // self.MAX_BATCH_SIZE + 1}, "
                f"chunk size {len(chunk)})"
            )

        return results

    def get_citing_paper_dates(self, citing_pmids: List[int]) -> Dict[int, Dict]:
        """
        For a list of citing PMIDs, fetch their year/month info.
        Returns dict mapping PMID → {pmid, year, title, journal}.
        This is used to reconstruct when citations occurred.
        """
        return self.fetch_batch(citing_pmids)
