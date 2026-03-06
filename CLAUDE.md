# IMPACT - Independent Metrics for Publication Analysis and Citation Tracking

## Project Overview
Open-source system computing monthly rolling citation rates for 8,500+ PubMed-indexed journals.
Live site: https://dantyrr.github.io/IMPACT
Data sources: PubMed Annual Baseline (~40M records) + iCite Open Citation Collection.

## Architecture
- **Backend**: Python 3.11+ pipeline → SQLite (WAL mode) → JSON export
- **Frontend**: Vanilla JS SPA in `docs/` served via GitHub Pages, data from Cloudflare R2
- **Charts**: Chart.js 4.4.0 + Cytoscape.js 3.30.2 (CDN, no build step)
- **CI/CD**: GitHub Actions (monthly data pipeline + deploy-pages)

## Key Directories
- `src/pipeline/` — Core modules: config, db_manager, pubmed_fetcher, icite_fetcher, impact_calculator, json_exporter
- `scripts/` — Standalone pipeline scripts (download, process, compute, upload)
- `docs/` — Static SPA (index.html, js/, css/, data/)
- `docs/js/` — app.js (main controller), chart-manager.js, data-loader.js, journal-picker.js, ui-helpers.js
- `data/` — Local SQLite databases (gitignored): impact.db, pubmed_bulk.db, pmid_dates.db

## Data Flow
1. Bulk download: PubMed baseline + iCite → local SQLite DBs
2. Per-journal processing: `run_pipeline_bulk.py` → impact.db
3. Snapshot computation: `compute_snapshots.py` → rolling IF at every month
4. JSON export → `docs/data/` + Cloudflare R2
5. Frontend fetches JSON from R2 (prod) or local `data/` (dev)

## Commands
- `python scripts/run_pipeline_bulk.py --journal <slug>` — Process one journal
- `python scripts/compute_snapshots.py` — Recompute all rolling IFs + export JSON
- `python scripts/run_weekly_update.py` — Incremental update (new papers + citations)
- `python scripts/upload_to_r2.py` — Sync JSON to Cloudflare R2
- `python scripts/validate_exports.py` — Verify JSON export completeness
- `sqlite3 data/impact.db` — Query the main database directly

## Conventions
- Commit messages: `feat:`, `fix:`, `docs:`, `remove:` prefixes (conventional commits style)
- Python: snake_case functions/variables, PascalCase classes, UPPER_SNAKE_CASE constants
- JavaScript: camelCase methods/variables, PascalCase classes, UPPER_SNAKE_CASE constants
- CSS: kebab-case classes (BEM-inspired), CSS custom properties for theming
- HTML IDs: kebab-case
- Journal slugs: kebab-case (e.g., `aging-cell`, `nature-aging`)
- Colorblind-safe palette (Okabe-Ito) for all charts

## Frontend Notes
- No framework, no build step — edit JS/CSS/HTML directly in `docs/`
- `app.js` is the main controller (~2000 lines, IMPACTApp class)
- `data-loader.js` handles R2 CDN fetch with local fallback + in-memory cache
- 7 sections: Journals, Compare, Papers, Authors, Geography, Influence, About
- Test locally by serving `docs/` (e.g., `python -m http.server -d docs`)

## Database Schema (SQLite)
- `journals` (id, issn, name, abbreviation, slug)
- `papers` (pmid PK, journal_id FK, title, pub_date, pub_year, pub_month, pub_type, is_research, doi)
- `citations` (id, cited_pmid, citing_pmid, citing_date, citing_year, citing_month) — UNIQUE(cited_pmid, citing_pmid)
- `monthly_snapshots` (id, journal_id FK, snapshot_month, rolling_if, rolling_if_no_reviews, paper_count, citation_count, review_count)

## Environment
- Requires `.env` with: PUBMED_API_KEY, PUBMED_EMAIL, R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_PUBLIC_URL, R2_SECRET_ACCESS_KEY
- See `.env.example` for template
- Rate limits: PubMed 10 rps (with key), iCite 5 rps

## Weekly Update (Manual for Now)
Weekly updates are triggered manually — there is no automated GitHub Actions workflow for this yet.
The old `monthly-update.yml` workflow was deleted (it used the outdated API-based pipeline and wrong paths).

To update:
```bash
python scripts/run_weekly_update.py --days 10
python scripts/upload_to_r2.py
```

If automation is desired in future: create a new GitHub Actions workflow on a weekly cron that runs those
two commands, using repository secrets for `PUBMED_API_KEY`, `R2_ACCOUNT_ID`, `R2_BUCKET_NAME`,
`R2_SECRET_ACCESS_KEY`, and `R2_PUBLIC_URL`. The script itself is fully implemented and ready.

## Gotchas
- PubMed ESearch has a 9,999 result limit per query — pipeline splits by year to handle this
- iCite batch API max 200 PMIDs per request (414 errors if exceeded)
- SQLite WAL mode needed for concurrent access from parallel workers
- `docs/data/journals/` and `docs/data/papers/` are gitignored — data served from R2 in production
- Rolling IF formula uses 24-mo paper window + 12-mo citation window (see METHODOLOGY.md)
