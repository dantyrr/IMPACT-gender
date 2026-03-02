# IMPACT

**Independent Metrics for Publication Analysis and Citation Tracking**

An open-source system for computing **monthly rolling citation rates** for every PubMed-indexed journal — built entirely from freely available public data. Designed to study publication bias, impact factor inflation, and citation patterns in academic publishing.

**Live site:** https://dantyrr.github.io/IMPACT

---

## The Problem

The way science is evaluated — through journal impact factors, h-indexes, and citation counts — shapes what gets funded, published, and promoted. These metrics have well-known flaws:

- **Calendar bias** — The JIF is calculated annually using only the first two years after publication. A paper published in January gets a full 24 months to accumulate citations, while a paper published in November gets only 14 months. This structural flaw penalizes late-year publications and distorts the metric.
- **Gaming** — Journals can inflate their JIF through editorial practices: soliciting review articles (which are cited far more than original research), publishing consensus statements, or encouraging self-citation.
- **Field and type skew** — Citation rates vary enormously by field (a cardiology paper and a mycology paper are not comparable), by geographic region, and by article type (reviews vs. original research vs. editorials). The JIF collapses all of this into a single number.
- **Opacity** — JIFs are published once a year by Clarivate, behind a paywall. Researchers, tenure committees, and funding agencies rely on these numbers with no way to audit or reproduce them.

IMPACT provides open tools to study how these metrics can be gamed or inflated:

- **Impact factor inflation** — Which journals boost their JIF with highly-cited reviews or clinical guidelines? How much does a single landmark paper raise a journal's citation rate?
- **H-index padding** — How do h-indexes vary when you filter by article type (reviews vs. original research)? Are self-citations or invited review series inflating an author's index?
- **Publication bias** — Which journals, institutions, or countries dominate the literature in a given field?

We are actively building out the site to illustrate all of these issues. The goal is to promote transparency, improve how metrics are used, and give researchers free access to the data behind the numbers.

## What IMPACT Does

IMPACT computes a rolling monthly citation rate — equivalent to the JIF but updated every month instead of annually — from public data, for all 8,000+ PubMed-indexed journals going back to 2012. The results are browsable on a free website with interactive charts, and all code and data are open source.

---

## Where the Data Comes From

IMPACT uses two public bulk datasets, each downloaded once, plus two APIs for ongoing updates.

### Bulk downloads (one-time setup)

| Source | What it provides | Size |
|--------|-----------------|------|
| [PubMed Annual Baseline](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) | Every paper indexed by PubMed (~40 million records): title, journal ISSN, publication year and month, article type, author names and affiliations, DOI | ~25 GB compressed |
| [iCite Open Citation Collection](https://icite.od.nih.gov/stats) | For each paper, the list of PMIDs that cite it (`cited_by`) and the list of PMIDs it references (`references`). Also whether iCite classifies it as a research article. | ~10 GB |

These two datasets contain everything needed to compute citation rates for any journal. The PubMed baseline tells us *what was published and when*. The iCite collection tells us *what cited what*.

### APIs (for weekly updates)

| Source | What it provides |
|--------|-----------------|
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25499/) | Search for papers published in the last 7 days (ESearch), fetch metadata for new papers (ESummary/EFetch) |
| [iCite API](https://icite.od.nih.gov/api) | For newly published papers, fetch their `references` list — tells us which older papers they cite, creating new citation events |

The bulk downloads cover the full history. The APIs handle the incremental weekly additions.

---

## How the Rolling Citation Rate Is Calculated

The standard 2-year Journal Impact Factor works like this:

> JIF(2025) = citations received in 2025 by papers published in 2023–2024 / number of papers published in 2023–2024

IMPACT computes the same thing, but **monthly** instead of annually:

> Rolling IF(March 2025) = citations received in the 12 months ending March 2025, by papers published in the 24 months before that 12-month window / number of those papers

```
Example for March 2025:

  Paper window:    March 2022 → February 2024  (24 months of published papers)
  Citation window: March 2024 → February 2025  (12 months of citations to those papers)

  Rolling IF = citation count / paper count
```

Three window variants are available:

| Variant | Paper window | Gap before citation window |
|---------|-------------|---------------------------|
| **24-month** (default) | 24 months | 0 |
| **12-month** | 12 months | 0 |
| **5-year** (years 2–6) | 60 months | 12 months |

### The month-resolution problem

This calculation requires knowing the **month** each citation occurred — meaning the month the *citing* paper was published. But iCite's bulk data only provides the *year* of each paper, not the month. Without months, all citations would pile up in a single annual bucket, defeating the purpose of monthly tracking.

The solution: every citing paper is also a PubMed record, and PubMed records include publication month. So after loading citations from iCite, we look up each citing paper's month from `pubmed_bulk.db` (which has month-level dates for all 40M records). The small number of very new citing papers not yet in the bulk download are resolved via the PubMed API.

This is what turns a yearly citation dataset into a monthly one.

---

## Data Pipeline

### Step 1: One-time bulk setup

Download and index both bulk datasets locally:

```bash
python scripts/download_pubmed_bulk.py    # → data/pubmed_bulk.db  (~40M records)
python scripts/download_icite_bulk.py     # → data/icite_bulk.db
```

### Step 2: Process all journals

For each of the ~8,500 journals found in the PubMed baseline:

1. **Papers** — Query `pubmed_bulk.db` by journal ISSN to get all papers (title, date, authors, type)
2. **Citations** — Query `icite_bulk.db` to get the `cited_by` list for each paper
3. **Citation months** — Look up the publication month of each citing paper in `pubmed_bulk.db`
4. **Store** — Write everything to `data/impact.db` (the main SQLite database)

```bash
# Process all journals in parallel
python scripts/run_all_journals.py --workers 4

# Or a single journal
python scripts/run_pipeline_bulk.py --journal aging-cell --years 2010-2026
```

No API calls are made during bulk processing — everything comes from the local databases.

### Step 3: Compute snapshots and export

For each journal, compute the rolling citation rate at every month from 2012 to present, then export as JSON:

```bash
python scripts/compute_snapshots.py --workers 4
```

This produces three JSON files per journal:
- `journals/{slug}.json` — monthly timeseries of rolling IF, paper counts, citation counts
- `authors/{slug}.json` — first/last author names and affiliations for each paper
- `papers/{slug}.json` — top 2,000 papers by citation count, plus country-by-year geography data

Plus `index.json` — the master list of all journals with their latest metrics.

### Step 4: Publish

JSON files are uploaded to Cloudflare R2 (a CDN) and served to the website:

```bash
python scripts/upload_to_r2.py
```

The website itself is static HTML/JS hosted on GitHub Pages. It fetches data from R2 at runtime.

---

## Weekly Updates

Once the bulk data is loaded, keeping it current only requires adding each week's newly published papers and the citations they create:

```bash
python scripts/run_weekly_update.py          # last 10 days (default)
python scripts/run_weekly_update.py --days 7  # last 7 days
python scripts/upload_to_r2.py                # sync changed JSONs to CDN
```

The script runs three phases:

**Phase 1 — New papers** (~2 min). Queries PubMed ESearch for papers added in the last N days (one query per day to stay under the 9,999-result limit). Fetches metadata via ESummary, matches each paper's ISSN to our tracked journals, and inserts into `impact.db`. A typical week adds ~30,000 papers.

**Phase 2 — New citation events** (~2 min). Fetches iCite records for the new papers. Each record includes a `references` list — the older papers it cites. Cross-references those against papers already in the database. For each match, records a citation event dated to the citing paper's publication month. A typical week adds ~200,000+ citation events. (iCite has a processing lag of days to weeks, so very new papers without iCite data yet are automatically caught on the next run — overlapping 10-day windows handle this.)

**Phase 3 — Update snapshots** (~25 min). For each journal that received new papers or citations, recomputes the current month's rolling IF (all three window variants) and merges it into the existing JSON. Updates `index.json` with new latest values.

This avoids recomputing the full history for all 8,000+ journals. Only the current month changes, and only for journals that had new activity.

---

## The Website

The site at [dantyrr.github.io/IMPACT](https://dantyrr.github.io/IMPACT) is a single-page app that loads journal data on demand from Cloudflare R2.

### Sections

- **Journals** — Browse all 8,000+ journals. Select one to see its rolling citation rate over time, with toggles for window variant and article type. Download charts as PNG/JPG/PDF or data as CSV.
- **Compare** — Overlay multiple journals on a single chart. Choose metric (citation rate, raw citations, paper count) and window variant.
- **Papers** — Browse a journal's top 2,000 most-cited papers. Sort by citations, year, or author. Filter by article type, year range, or search text.
- **Authors** — Search any author by name to see their h-index, publication timeline, top journals, and most-cited papers. Uses PubMed and iCite APIs for live lookup.
- **Geography** — Country-by-year breakdown of first-author affiliations for any journal. Stacked bar chart and choropleth map.
- **PMID Influence** — Enter a specific PubMed ID to see how much it contributes to its journal's citation rate. Shows a counterfactual "censored IF" curve with that paper removed.

Charts use the [Okabe-Ito colorblind-safe palette](https://www.nature.com/articles/nmeth.1618).

---

## Project Structure

```
IMPACT/
├── src/pipeline/
│   ├── db_manager.py          # SQLite operations for papers, citations, journals
│   ├── pubmed_fetcher.py      # PubMed E-utilities (for weekly updates + live search)
│   ├── icite_fetcher.py       # iCite API client
│   ├── impact_calculator.py   # Rolling IF computation engine
│   └── json_exporter.py       # JSON export (timeseries, authors, papers, geography)
├── scripts/
│   ├── download_pubmed_bulk.py    # One-time: PubMed baseline → pubmed_bulk.db
│   ├── download_icite_bulk.py     # One-time: iCite bulk → icite_bulk.db
│   ├── run_pipeline_bulk.py       # Per-journal processing from bulk DBs
│   ├── run_all_journals.py        # Parallel orchestrator for all journals
│   ├── compute_snapshots.py       # Rolling IF calculation + JSON export
│   ├── run_weekly_update.py        # Incremental weekly update (new papers + citations)
│   ├── upload_to_r2.py            # Sync JSON files to Cloudflare R2
│   ├── fix_citation_months.py     # Resolve citation months from yearly → monthly
│   └── build_date_cache.py        # Seed PMID date cache from DB
├── docs/                          # GitHub Pages static site
│   ├── index.html
│   ├── css/style.css
│   └── js/
│       ├── app.js                 # Main application
│       ├── chart-manager.js       # Chart.js wrapper
│       ├── data-loader.js         # Fetches from R2 (production) or local (dev)
│       └── journal-picker.js      # Searchable journal selector
└── data/                          # Local only (gitignored)
    ├── impact.db                  # Main database (papers, citations, journals)
    ├── pubmed_bulk.db             # PubMed baseline (~40M records)
    ├── icite_bulk.db              # iCite citation links
    ├── pmid_dates.db              # PMID → publication date cache
    └── journal_registry.json      # All ~8,500 PubMed-indexed journals
```

---

## Data Sources

| Source | Records | What IMPACT uses from it |
|--------|---------|------------------------|
| [PubMed Baseline](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) | ~40M papers | ISSN, pub year+month, title, authors, affiliations, article type |
| [iCite Bulk](https://icite.od.nih.gov/stats) | ~40M papers | `cited_by` lists (who cites each paper), `references` lists (what each paper cites), research article classification |
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25499/) | Live API | Weekly new-paper discovery, author search, citation month resolution |
| [iCite API](https://icite.od.nih.gov/api) | Live API | Citation data for new papers, live author/paper lookups |

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

Pull requests welcome. Please [open an issue](https://github.com/dantyrr/IMPACT/issues) first to discuss.
