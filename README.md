# IMPACT

**Independent Metrics for Publication Analysis and Citation Tracking**

An open-source toolkit for studying **publication bias**, **impact factor inflation**, and **h-index gaming** in academic publishing — computed from freely available [PubMed](https://pubmed.ncbi.nlm.nih.gov/) and [NIH iCite](https://icite.od.nih.gov/) data.

🌐 **Live site:** https://dantyrr.github.io/IMPACT

---

## Why IMPACT?

The way science is evaluated — through journal impact factors, h-indexes, and citation counts — shapes what gets funded, published, and promoted. IMPACT provides open tools to study how these metrics can be gamed or inflated:

- **Impact factor inflation** — Which journals boost their JIF with highly-cited reviews or clinical guidelines? How much does a single landmark paper raise a journal's citation rate?
- **H-index padding** — How do h-indexes vary when you filter by article type (reviews vs. original research)? Are self-citations or invited review series inflating an author's index?
- **Publication bias** — Which journals, institutions, or countries dominate the literature in a given field?

IMPACT provides the transparent, reproducible metrics needed to investigate these questions:

- **Rolling 24-month citation rate** updated continuously, not just annually
- **Three window variants** — 12-month, 24-month (standard), and 5-year (years 2–6)
- **Review-excluded rate** to detect inflation from highly-cited review articles
- **PMID Influence tab** — quantify how much a specific paper (or group of papers) inflates a journal's citation rate, with counterfactual "censored IF" curves
- **Author analysis** — h-index, publication timeline, top journals, article type breakdown, filterable by type
- **8,000+ journals** — all PubMed-indexed journals processed from the bulk data
- **Open data** — all code, metrics, and methodology are public on GitHub

---

## Live Site Features

### Journals
Compare citation rate trends across any combination of journals. Select one or more journals from the searchable picker to overlay their trends on a single chart. Use the window toggle to switch between 12-month, 24-month, and 5-year rates. Filter by article type (all, research, reviews, etc.). Journal info cards below the chart update to show only the selected journals.

**Download the chart** as PNG, JPG, or PDF, or export the underlying data as a CSV file — directly from the browser.

### Authors
Search any author by name (e.g. `Horvath S`) to pull their publications from PubMed and citation data from iCite. Displays:
- Estimated h-index, total citations, papers loaded
- Publications per year and citations by publication year (bar charts)
- Top journals (horizontal bar chart)
- Most-cited papers table with PubMed links

### Papers
Enter any PubMed ID to visualize all papers that cite it as an interactive force-directed network. Node size reflects each citing paper's own citation count; color reflects publication decade. Click any node to see title, authors, journal, and a direct PubMed link.

### Geography *(beta)*
Select a journal to see first-author country breakdowns over time — stacked bar chart by year, top countries overall, and recent-years comparison.

### Compare
Overlay multiple journals on a single chart with a choice of metric (citation rate, raw citations, or paper count) and time window.

---

## How It Works

```
PubMed Baseline Bulk Files (~40M records)
        ↓
  pubmed_bulk.db  ←─── download_pubmed_bulk.py
        ↓
  run_pipeline_bulk.py  (per journal: papers + authors)
        ↓
  iCite Bulk DB  ←────── download_icite_bulk.py
        ↓
  Citation data joined locally (no API calls)
        ↓
  SQLite database  (data/impact.db)
        ↓
  compute_snapshots.py  (rolling IF calculator)
        ↓
  JSON exports  →  docs/data/
        ↓
  GitHub Pages website
```

The key innovation over the original pipeline is using the **PubMed annual baseline** and **iCite Open Citation Collection** bulk downloads rather than hitting APIs for every paper — reducing processing time from weeks to hours for all 8,000+ PubMed-indexed journals.

---

## Quick Start

### Requirements

- Python 3.9+
- ~50 GB free disk space for bulk databases

### Setup

```bash
git clone https://github.com/dantyrr/IMPACT.git
cd IMPACT
pip install -r requirements.txt
```

### 1. Download bulk data

```bash
# PubMed baseline (~25 GB compressed, builds pubmed_bulk.db ~40M records, ~1 hr)
python scripts/download_pubmed_bulk.py

# iCite Open Citation Collection (~10 GB, builds icite_bulk.db)
python scripts/download_icite_bulk.py
```

### 2. Run the pipeline

```bash
# Process all ~8,490 PubMed-indexed journals in parallel (4 workers)
python scripts/run_all_journals.py --workers 4

# Or process a single journal
python scripts/run_pipeline_bulk.py --journal aging-cell --years 2010-2026

# Resume an interrupted run
python scripts/run_all_journals.py --resume --workers 4
```

### 3. Compute snapshots and export JSON

```bash
python scripts/compute_snapshots.py
```

This writes one JSON file per journal to `docs/data/journals/`, `docs/data/authors/`, and `docs/data/papers/`, plus `docs/data/index.json`.

### 4. View locally

Open `docs/index.html` in your browser (served via `file://` — no server needed).

---

## Project Structure

```
IMPACT/
├── src/pipeline/
│   ├── db_manager.py        # SQLite CRUD (papers, citations, snapshots)
│   ├── pubmed_fetcher.py    # PubMed E-utilities (legacy / small-scale)
│   ├── icite_fetcher.py     # iCite API (legacy / small-scale)
│   ├── impact_calculator.py # Rolling IF computation (all three window variants)
│   └── json_exporter.py     # JSON export for website (journal, authors, papers, geo)
├── scripts/
│   ├── download_pubmed_bulk.py   # Download PubMed baseline → pubmed_bulk.db
│   ├── download_icite_bulk.py    # Download iCite bulk → icite_bulk.db
│   ├── run_pipeline_bulk.py      # Per-journal pipeline using bulk DBs
│   ├── run_all_journals.py       # Parallel orchestrator for all journals
│   ├── compute_snapshots.py      # Compute rolling IFs and export all JSONs
│   ├── fix_citation_months.py    # Backfill exact citation months (June→real month)
│   └── build_date_cache.py       # Seed pmid_dates.db cache from DB
├── docs/                    # GitHub Pages site
│   ├── index.html
│   ├── css/style.css
│   ├── js/
│   │   ├── app.js           # Main app logic
│   │   ├── chart-manager.js # Chart.js wrapper (Okabe-Ito palette)
│   │   ├── data-loader.js   # Fetch routing (local vs CDN)
│   │   ├── journal-picker.js # Searchable multi- and single-select pickers
│   │   └── ui-helpers.js    # Shared UI utilities
│   └── data/                # Pre-computed JSON (committed to repo)
│       ├── index.json        # Journal index (name, ISSN, latest IF, ...)
│       ├── journals/         # One JSON per journal (full timeseries)
│       ├── authors/          # One JSON per journal (author lookup table)
│       └── papers/           # One JSON per journal (top papers + geo data)
└── data/                    # Local only (gitignored)
    ├── impact.db             # Main SQLite database
    ├── pubmed_bulk.db        # PubMed baseline (~40M records)
    ├── icite_bulk.db         # iCite Open Citation Collection
    └── pmid_dates.db         # PMID → pub date cache
```

---

## Adding a Journal

All PubMed-indexed journals are already in `data/journal_registry.json` (auto-populated from the bulk download). To process a specific journal:

```bash
python scripts/run_pipeline_bulk.py --journal <slug> --years 2010-2026
python scripts/compute_snapshots.py
```

The slug is the journal's name lowercased with spaces replaced by hyphens (e.g. `nature-medicine`). If a journal returns 0 papers, check its ISSNLinking in `pubmed_bulk.db` — sometimes the print ISSN differs from the e-ISSN in the config.

---

## Methodology

### Rolling Citation Rate

For each target month *M*, IMPACT counts:

- **Papers window**: articles published in the 24 months ending 12 months before *M* (i.e. 13–36 months prior)
- **Citation window**: citations those papers received in the 12 months ending at *M*

```
Rolling IF(M) = Citations(M-12 → M) / Papers(M-36 → M-13)
```

This matches the structure of the traditional 2-year JIF while being computed monthly instead of annually. Three variants are available:

| Variant | Paper window | Skip |
|---------|-------------|------|
| 24-month (default) | 24 months | 0 months |
| 12-month | 12 months | 0 months |
| 5-year (yr 2–6) | 60 months | 12 months |

### Citation Month Resolution

iCite provides only year-level citation data. IMPACT backfills exact months by querying PubMed ESummary, resolving the common "June artifact" (the default month iCite assigns when no month is known). A local SQLite cache (`pmid_dates.db`) means subsequent runs only fetch newly-seen PMIDs.

---

## Data Sources

| Source | What We Use |
|--------|-------------|
| [PubMed Baseline](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) | All ~40M PubMed records: titles, ISSNs, pub dates, pub types, author affiliations |
| [iCite Open Citation Collection](https://icite.od.nih.gov/stats) | `cited_by` lists for all papers — no API calls needed for bulk processing |
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25499/) | Live author search (Authors tab) and exact pub dates for new citations |
| [NIH iCite API](https://icite.od.nih.gov/api) | Live citation data for Papers and Authors tabs |

---

## Accessibility

Charts use the **[Okabe-Ito colorblind-safe palette](https://www.nature.com/articles/nmeth.1618)**, recommended by *Nature Methods* for scientific visualization. All eight core colors remain distinguishable under deuteranopia, protanopia, and tritanopia (the most common forms of color vision deficiency, affecting ~8% of males). Yellow and black are substituted with purple and gray for better legibility on white backgrounds.

Have a suggestion for improving accessibility or the color scheme? [Open an issue on GitHub](https://github.com/dantyrr/IMPACT/issues/new).

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

Pull requests are welcome. If you'd like to add a feature, fix a bug, or improve the methodology, please [open an issue](https://github.com/dantyrr/IMPACT/issues) first to discuss.
