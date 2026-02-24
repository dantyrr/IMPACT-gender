"""
IMPACT Citation Resolver
Reconstructs historical monthly citation counts using iCite cited_by data.

Key insight: iCite returns a list of PMIDs that cite each paper.
By looking up the publication date of each citing paper, we can determine
WHEN each citation occurred, enabling historical rolling IF computation.

Month resolution: iCite does not return month data, only year. Publication
months are fetched from PubMed ESummary (preferring epubdate for online-first
accuracy). When neither source provides a month, defaults to 6 (June,
midpoint) as a last resort.
"""

import logging
from typing import Dict, List, Optional, Tuple
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.icite_fetcher import IciteFetcher

logger = logging.getLogger(__name__)

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _parse_date_for_month(date_str: str) -> Optional[int]:
    """
    Extract just the month from a PubMed date string like '2024 Jan 15'.
    Returns the month int (1-12) if present, else None.
    """
    if not date_str:
        return None
    parts = date_str.strip().split()
    if len(parts) < 2:
        return None
    m_part = parts[1].split("-")[0]
    month = MONTH_MAP.get(m_part)
    if month is None:
        try:
            m_int = int(m_part)
            month = m_int if 1 <= m_int <= 12 else None
        except ValueError:
            pass
    return month


class CitationResolver:
    """Resolves citation events by fetching citing paper dates from iCite + PubMed."""

    def __init__(self, db: DatabaseManager, icite: IciteFetcher,
                 pubmed=None):
        """
        Args:
            db:      Database manager
            icite:   iCite fetcher (provides year + cited_by lists)
            pubmed:  PubMedFetcher instance (optional but strongly recommended).
                     Used to look up actual publication months for citing papers,
                     since iCite only returns year.
        """
        self.db = db
        self.icite = icite
        self.pubmed = pubmed

    def _fetch_pubmed_months(self, pmids: List[int]) -> Dict[int, int]:
        """
        Fetch actual publication months from PubMed for a list of PMIDs.
        Returns {pmid: month}.  Prefers epubdate over pubdate.
        Only called if self.pubmed is set.
        """
        if not pmids or not self.pubmed:
            return {}

        month_map: Dict[int, int] = {}
        batch_size = 200
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i: i + batch_size]
            try:
                papers = self.pubmed.fetch_paper_details(batch, batch_size=batch_size)
                for paper in papers:
                    pmid = paper["pmid"]
                    # fetch_paper_details already prefers epubdate as fallback,
                    # but we want epubdate-first — use pub_month which was parsed
                    # from whichever date had more info.
                    month = paper.get("pub_month")
                    if month:
                        month_map[pmid] = month
            except Exception as e:
                logger.warning(f"PubMed month lookup failed for batch {i}: {e}")

        return month_map

    def resolve_paper(self, cited_pmid: int,
                      cited_by_list: List[int],
                      pubmed_month_map: Optional[Dict[int, int]] = None) -> int:
        """
        For a given paper, resolve its cited_by list into dated citation events.

        Args:
            cited_pmid:       PMID of the paper being cited
            cited_by_list:    PMIDs of papers that cite it (from iCite)
            pubmed_month_map: Pre-fetched {citing_pmid: month} from PubMed.
                              If provided, used to set precise months.

        Returns:
            Number of citation events stored
        """
        if not cited_by_list:
            return 0

        citing_data = self.icite.get_citing_paper_dates(cited_by_list)
        if pubmed_month_map is None:
            pubmed_month_map = {}

        citation_events = []
        for citing_pmid in cited_by_list:
            info = citing_data.get(citing_pmid)
            if not info:
                continue

            citing_year = info.get("year")
            if not citing_year:
                continue

            # Real month from PubMed if available; fall back to 6 (midpoint)
            citing_month = pubmed_month_map.get(citing_pmid) or 6
            citing_date = f"{citing_year}-{citing_month:02d}-01"

            citation_events.append({
                "cited_pmid": cited_pmid,
                "citing_pmid": citing_pmid,
                "citing_date": citing_date,
                "citing_year": citing_year,
                "citing_month": citing_month,
            })

        if citation_events:
            self.db.add_citations_bulk(citation_events)

        logger.debug(
            f"PMID {cited_pmid}: resolved {len(citation_events)}/{len(cited_by_list)} "
            f"citation events"
        )
        return len(citation_events)

    def resolve_journal_papers(self, journal_id: int,
                               icite_data: Dict[int, Dict]) -> Dict:
        """
        Resolve citations for all papers in a journal using pre-fetched iCite data.
        Fetches publication months from PubMed in one batch before resolving.

        Args:
            journal_id: Journal DB id
            icite_data: Dict mapping PMID → iCite record (with cited_by)

        Returns:
            Summary dict {total_papers, total_citations_resolved}
        """
        # Collect all unique citing PMIDs across this journal's papers
        all_citing_pmids: List[int] = []
        for record in icite_data.values():
            all_citing_pmids.extend(record.get("cited_by", []))
        unique_citing = list(set(all_citing_pmids))

        # Batch-fetch actual months from PubMed (if fetcher available)
        pubmed_month_map: Dict[int, int] = {}
        if self.pubmed and unique_citing:
            logger.info(
                f"Journal {journal_id}: fetching PubMed months for "
                f"{len(unique_citing):,} unique citing PMIDs..."
            )
            pubmed_month_map = self._fetch_pubmed_months(unique_citing)
            with_month = sum(1 for m in pubmed_month_map.values() if m != 6)
            logger.info(
                f"Journal {journal_id}: got months for "
                f"{len(pubmed_month_map):,}/{len(unique_citing):,} citing papers "
                f"({with_month:,} with real month)"
            )
        else:
            logger.warning(
                f"Journal {journal_id}: no PubMed fetcher — citation months "
                f"will default to June. Run scripts/fix_citation_months.py "
                f"afterward for accurate monthly data."
            )

        total_resolved = 0
        papers_processed = 0

        for pmid, record in icite_data.items():
            cited_by = record.get("cited_by", [])
            if not cited_by:
                papers_processed += 1
                continue

            resolved = self.resolve_paper(pmid, cited_by, pubmed_month_map)
            total_resolved += resolved
            papers_processed += 1

            if papers_processed % 50 == 0:
                logger.info(
                    f"Journal {journal_id}: processed {papers_processed} papers, "
                    f"{total_resolved} citation events resolved"
                )

        logger.info(
            f"Journal {journal_id}: DONE — {papers_processed} papers, "
            f"{total_resolved} total citation events"
        )

        return {
            "total_papers": papers_processed,
            "total_citations_resolved": total_resolved,
        }
