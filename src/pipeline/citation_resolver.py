"""
IMPACT Citation Resolver
Reconstructs historical monthly citation counts using iCite cited_by data.

Key insight: iCite returns a list of PMIDs that cite each paper.
By looking up the publication date of each citing paper, we can determine
WHEN each citation occurred, enabling historical rolling IF computation.
"""

import logging
from typing import List, Dict
from src.pipeline.db_manager import DatabaseManager
from src.pipeline.icite_fetcher import IciteFetcher

logger = logging.getLogger(__name__)


class CitationResolver:
    """Resolves citation events by fetching citing paper dates from iCite."""

    def __init__(self, db: DatabaseManager, icite: IciteFetcher):
        self.db = db
        self.icite = icite

    def resolve_paper(self, cited_pmid: int,
                      cited_by_list: List[int]) -> int:
        """
        For a given paper, resolve its cited_by list into dated citation events.

        Args:
            cited_pmid: PMID of the paper being cited
            cited_by_list: PMIDs of papers that cite it (from iCite)

        Returns:
            Number of citation events stored
        """
        if not cited_by_list:
            return 0

        # Fetch publication dates of all citing papers
        citing_data = self.icite.get_citing_paper_dates(cited_by_list)

        citation_events = []
        for citing_pmid in cited_by_list:
            info = citing_data.get(citing_pmid)
            if not info:
                continue

            citing_year = info.get("year")
            if not citing_year:
                continue

            # iCite doesn't always have month — default to 6 (midpoint)
            citing_month = info.get("month", 6) or 6
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

        Args:
            journal_id: Journal DB id
            icite_data: Dict mapping PMID → iCite record (with cited_by)

        Returns:
            Summary dict {total_papers, total_citations_resolved}
        """
        total_resolved = 0
        papers_processed = 0

        for pmid, record in icite_data.items():
            cited_by = record.get("cited_by", [])
            if not cited_by:
                papers_processed += 1
                continue

            resolved = self.resolve_paper(pmid, cited_by)
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
