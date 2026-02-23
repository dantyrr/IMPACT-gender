"""
IMPACT Calculator
Computes rolling 24-month impact factors, author metrics, and paper trajectories.

Rolling IF formula (JIF-equivalent):
    IF(month M, year Y) =
        Citations received in year Y by papers published in years Y-1 and Y-2
        ÷ Number of citable (research) articles published in Y-1 and Y-2

For our ROLLING monthly version:
    IF(YYYY-MM) =
        Citations received in the 12 months ending YYYY-MM
        by papers published in the 24 months before that 12-month window
        ÷ Number of research articles in that 24-month window
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta

from src.pipeline.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


def _month_offset(year: int, month: int, offset: int) -> Tuple[int, int]:
    """Add offset months to a (year, month) pair."""
    total = (year * 12 + month - 1) + offset
    return total // 12, total % 12 + 1


def _month_to_date(year: int, month: int, day: int = 1) -> str:
    """Convert year/month to ISO date string."""
    return f"{year}-{month:02d}-{day:02d}"


def _end_of_month(year: int, month: int) -> str:
    """Last day of a given month as ISO date string."""
    if month == 12:
        return f"{year}-12-31"
    next_month = datetime(year, month + 1, 1) - timedelta(days=1)
    return next_month.strftime("%Y-%m-%d")


class ImpactCalculator:
    """Computes rolling impact factors and related metrics."""

    def __init__(self, db: DatabaseManager, window_months: int = 24):
        self.db = db
        self.window_months = window_months

    # ------------------------------------------------------------------ #
    #  Journal-level rolling IF
    # ------------------------------------------------------------------ #

    def compute_rolling_if(self, journal_id: int,
                           target_year: int,
                           target_month: int) -> Dict:
        """
        Compute rolling IF for a journal at a specific month.

        The JIF methodology:
        - Denominator window: papers published in the 24 months BEFORE
          the citation counting year
        - Numerator: citations those papers received during the
          citation counting year

        For our rolling version at target YYYY-MM:
        - Citation window: the 12 months ending at target_month
          i.e., from (target - 11 months) to target_month
        - Paper window: the 24 months before the citation window start
          i.e., from (target - 35 months) to (target - 12 months)

        Returns dict with rolling_if, paper counts, citation counts, etc.
        """
        # Citation counting window: 12 months ending at target
        cite_end_y, cite_end_m = target_year, target_month
        cite_start_y, cite_start_m = _month_offset(target_year, target_month, -11)

        cite_start_date = _month_to_date(cite_start_y, cite_start_m)
        cite_end_date = _end_of_month(cite_end_y, cite_end_m)

        # Paper publication window: 24 months before citation window start
        paper_end_y, paper_end_m = _month_offset(cite_start_y, cite_start_m, -1)
        paper_start_y, paper_start_m = _month_offset(paper_end_y, paper_end_m, -23)

        paper_start_date = _month_to_date(paper_start_y, paper_start_m)
        paper_end_date = _end_of_month(paper_end_y, paper_end_m)

        # Get papers in the publication window
        papers = self.db.get_papers_in_window(journal_id, paper_start_date, paper_end_date)

        if not papers:
            return self._empty_snapshot(target_year, target_month)

        # Separate research from reviews
        research_papers = [p for p in papers if p["is_research"]]
        review_papers = [p for p in papers if not p["is_research"]]

        # Count citations in the citation window
        all_pmids = [p["pmid"] for p in papers]
        research_pmids = [p["pmid"] for p in research_papers]

        total_citations = self.db.count_citations_for_papers(
            all_pmids, cite_start_date, cite_end_date
        )
        research_citations = self.db.count_citations_for_papers(
            research_pmids, cite_start_date, cite_end_date
        )

        # Compute IFs
        # Standard IF: all citations / research articles
        n_research = len(research_papers)
        n_all = len(papers)
        n_reviews = len(review_papers)

        rolling_if = total_citations / n_research if n_research > 0 else 0.0
        rolling_if_no_reviews = research_citations / n_research if n_research > 0 else 0.0

        return {
            "month": f"{target_year}-{target_month:02d}",
            "rolling_if": round(rolling_if, 3),
            "rolling_if_no_reviews": round(rolling_if_no_reviews, 3),
            "paper_count": n_all,
            "research_count": n_research,
            "review_count": n_reviews,
            "citation_count": total_citations,
            "research_citation_count": research_citations,
            "paper_window": f"{paper_start_date} to {paper_end_date}",
            "citation_window": f"{cite_start_date} to {cite_end_date}",
        }

    def compute_journal_timeseries(self, journal_id: int,
                                    start_year: int, start_month: int,
                                    end_year: int, end_month: int) -> List[Dict]:
        """
        Compute rolling IF for every month in a range.
        Returns list of monthly snapshots.
        """
        timeseries = []
        y, m = start_year, start_month

        while (y, m) <= (end_year, end_month):
            snapshot = self.compute_rolling_if(journal_id, y, m)
            timeseries.append(snapshot)

            # Advance one month
            y, m = _month_offset(y, m, 1)

        return timeseries

    def compute_and_store_timeseries(self, journal_id: int,
                                      start_year: int, start_month: int,
                                      end_year: int, end_month: int):
        """Compute timeseries and store each month as a snapshot in DB."""
        timeseries = self.compute_journal_timeseries(
            journal_id, start_year, start_month, end_year, end_month
        )

        for snap in timeseries:
            self.db.save_snapshot(
                journal_id=journal_id,
                snapshot_month=snap["month"],
                rolling_if=snap["rolling_if"],
                rolling_if_no_reviews=snap["rolling_if_no_reviews"],
                paper_count=snap["paper_count"],
                citation_count=snap["citation_count"],
                review_count=snap["review_count"],
            )

        logger.info(
            f"Journal {journal_id}: stored {len(timeseries)} monthly snapshots"
        )
        return timeseries

    # ------------------------------------------------------------------ #
    #  Author-level metrics
    # ------------------------------------------------------------------ #

    def compute_author_metrics(self, pmid_list: List[int]) -> Dict:
        """
        Compute metrics for an author given their list of PMIDs.
        Returns average citations per paper, total citations, per-paper breakdown.
        """
        papers_data = []
        total_cites = 0

        for pmid in pmid_list:
            paper = self.db.get_paper(pmid)
            if not paper:
                continue

            cite_count = self.db.get_citation_count_for_paper(pmid)
            total_cites += cite_count

            papers_data.append({
                "pmid": pmid,
                "title": paper["title"],
                "pub_date": paper["pub_date"],
                "pub_type": paper["pub_type"],
                "citations": cite_count,
            })

        n_papers = len(papers_data)
        avg_cites = total_cites / n_papers if n_papers > 0 else 0

        return {
            "paper_count": n_papers,
            "total_citations": total_cites,
            "avg_citations_per_paper": round(avg_cites, 2),
            "papers": sorted(papers_data, key=lambda x: -x["citations"]),
        }

    # ------------------------------------------------------------------ #
    #  Paper-level trajectory
    # ------------------------------------------------------------------ #

    def compute_paper_trajectory(self, pmid: int) -> Optional[Dict]:
        """
        Get month-by-month citation trajectory for a single paper.
        Returns dict with paper info and monthly citation counts.
        """
        paper = self.db.get_paper(pmid)
        if not paper:
            return None

        citations = self.db.get_citations_for_paper(pmid)

        # Aggregate by month
        monthly = {}
        for cite in citations:
            key = f"{cite['citing_year']}-{cite['citing_month']:02d}"
            monthly[key] = monthly.get(key, 0) + 1

        # Sort chronologically
        sorted_months = sorted(monthly.items())

        # Build cumulative
        cumulative = []
        running_total = 0
        for month_str, count in sorted_months:
            running_total += count
            cumulative.append({
                "month": month_str,
                "citations": count,
                "cumulative": running_total,
            })

        return {
            "pmid": pmid,
            "title": paper["title"],
            "pub_date": paper["pub_date"],
            "pub_type": paper["pub_type"],
            "total_citations": len(citations),
            "trajectory": cumulative,
        }

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _empty_snapshot(self, year: int, month: int) -> Dict:
        return {
            "month": f"{year}-{month:02d}",
            "rolling_if": 0.0,
            "rolling_if_no_reviews": 0.0,
            "paper_count": 0,
            "research_count": 0,
            "review_count": 0,
            "citation_count": 0,
            "research_citation_count": 0,
            "paper_window": "",
            "citation_window": "",
        }
