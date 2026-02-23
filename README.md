# IMPACT

**Independent Metrics for Publication Analysis and Citation Tracking**

An open-source alternative to Clarivate's Journal Impact Factor, computed from freely available [PubMed](https://pubmed.ncbi.nlm.nih.gov/) and [NIH iCite](https://icite.od.nih.gov/) data.

🌐 **Live site:** https://dantyrr.github.io/IMPACT

---

## Why IMPACT?

The official Journal Impact Factor (JIF) is computed by Clarivate using proprietary Web of Science citation data, behind a paywall, with limited transparency. IMPACT provides:

- **Rolling 24-month IF** updated monthly, not just annually
- **Review-excluded IF** to detect inflation from highly-cited review articles
- **Open data** — all code and computed metrics are public on GitHub
- **Author-level metrics** — rolling average citations per paper
- **Paper-level trends** — month-by-month citation trajectories

---

## How It Works

```
PubMed E-utilities ──► Paper discovery (by ISSN)
                         ↓
NIH iCite API ──────► Citation data (cited_by lists)
                         ↓
                    SQLite database
                         ↓
                    Rolling IF calculator
                         ↓
                    JSON exports
                         ↓
                    GitHub Pages website
```

The rolling IF is recomputed monthly via GitHub Actions and the pre-computed JSON is served directly from this repo.

---

## Quick Start (Running Locally)

### Requirements

- Python 3.9+
- Optional: [NCBI API key](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/) (increases rate limit from 3 → 10 req/sec)

### Setup

```bash
git clone https://github.com/dantyrr/IMPACT.git
cd IMPACT
pip install -r requirements.txt
```

### Configure (optional)

Create a `.env` file for your NCBI API key:

```
PUBMED_API_KEY=your_key_here
PUBMED_EMAIL=your@email.com
```

### Initialize the database

```bash
python scripts/init_db.py
```

### Fetch data and compute metrics

```bash
# All journals (can take 30-60 min due to API rate limits)
python scripts/run_pipeline.py

# Single journal only
python scripts/run_pipeline.py --journal aging-cell

# Specific year range
python scripts/run_pipeline.py --journal aging-cell --years 2022-2026
```

### Compute rolling IF snapshots and export JSON

```bash
python scripts/compute_snapshots.py
```

### View the website locally

Open `website/index.html` in your browser. No server needed — just open the file directly.

---

## Adding Journals

Edit `src/pipeline/config.py` and add entries to the `JOURNALS` dictionary:

```python
JOURNALS = {
    "1234-5678": {
        "name": "Journal of Example Research",
        "abbreviation": "J Example Res",
        "slug": "j-example-res",
    },
    # ... existing journals
}
```

Then re-run the pipeline and `compute_snapshots.py`.

---

## Project Structure

```
IMPACT/
├── .github/workflows/       # GitHub Actions (monthly update + Pages deploy)
├── src/pipeline/            # Python data pipeline
│   ├── config.py            # Journals, API keys, paths
│   ├── db_manager.py        # SQLite CRUD
│   ├── pubmed_fetcher.py    # PubMed E-utilities
│   ├── icite_fetcher.py     # NIH iCite API
│   ├── citation_resolver.py # Reconstruct historical citations
│   ├── impact_calculator.py # Rolling IF computation
│   └── json_exporter.py     # Export JSON for website
├── scripts/
│   ├── init_db.py           # Initialize database
│   ├── run_pipeline.py      # Main pipeline orchestrator
│   ├── compute_snapshots.py # Compute & export rolling IFs
│   ├── validate_exports.py  # Validate JSON outputs
│   └── generate_sample_data.py  # Sample data for testing
├── website/                 # GitHub Pages site
│   ├── index.html
│   ├── css/style.css
│   ├── js/                  # App logic, Chart.js wrapper, data loader
│   └── data/                # Pre-computed JSON (committed to repo)
│       ├── index.json
│       └── journals/        # One JSON file per journal
├── data/impact.db           # SQLite (local only, gitignored)
└── docs/METHODOLOGY.md      # Detailed methodology
```

---

## Methodology

See [docs/METHODOLOGY.md](docs/METHODOLOGY.md) for a detailed explanation of how the rolling IF is calculated and how it compares to the official JIF.

---

## Data Sources

| Source | What We Use |
|--------|------------|
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25499/) | Paper discovery, titles, pub dates, publication types |
| [NIH iCite API](https://icite.od.nih.gov/api) | `cited_by` lists for historical citation reconstruction |

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

Pull requests welcome! If you'd like to add a journal, fix a bug, or improve the methodology, please open an issue first.