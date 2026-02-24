# IMPACT — Next Steps to Fix the Rolling IF

## What's Wrong

**Root cause**: Only 2022 Aging Cell papers have citation data in the database.
2023–2026 papers have 0 citations, so the rolling IF comes out as 0 for any month
whose paper window doesn't include 2022.

This happened because iCite's `cited_by` data was apparently empty for 2023+ papers
when the pipeline last ran (possibly a data lag issue — iCite updates on a schedule).

## Step 1 — Check iCite Coverage (fast, ~30 sec)

```bash
cd /Users/dtyrrell/Projects/IMPACT
python scripts/check_icite_coverage.py
```

**If you see** `Papers with non-empty cited_by: X/10` (X > 0) for 2023/2024 → iCite
now has the data, proceed to Step 2.

**If you see** `cited_by_count=0` for ALL 2023+ papers → iCite may not cover these.
Still proceed to Step 2 — it will tell you clearly how many citations it found.

## Step 2 — Fix Citations for 2023-2026 Papers (~10-20 min, API-limited)

```bash
python scripts/fix_citations.py
```

This re-fetches iCite `cited_by` for all 2023-2026 Aging Cell papers and stores
any citing events it finds. At the end it shows updated citation counts by year.

**What success looks like:**
```
pub_year=2022: 201 papers, 5479 citation events
pub_year=2023: 245 papers, 3xxx citation events   ← new!
pub_year=2024: 302 papers, 1xxx citation events   ← new!
...
```

**If 2023+ still shows 0** → iCite genuinely doesn't have this data. Skip to
the "Fallback" section at the bottom.

## Step 3 — Recompute Snapshots

```bash
python scripts/compute_snapshots.py
```

Check the output for Aging Cell — the latest rolling IF should now be in the
range 5-10 (Aging Cell's official JIF is 8.0).

## Step 4 — Run Pipeline for Remaining Journals (~60-90 min)

Run each journal separately so you can monitor progress:

```bash
# Each takes 15-30 min due to API rate limits
python scripts/run_pipeline.py --journal jci
python scripts/run_pipeline.py --journal nature-aging
python scripts/run_pipeline.py --journal elife
python scripts/run_pipeline.py --journal jaha
```

Or all at once (runs sequentially):
```bash
python scripts/run_pipeline.py
```

## Step 5 — Validate and Push

```bash
python scripts/validate_exports.py

git add website/data/ scripts/
git commit -m "feat: add real citation data for all journals"
git push
```

## Step 6 — Enable GitHub Pages

1. Go to https://github.com/dantyrr/IMPACT/settings/pages
2. Under "Source", select **"Deploy from a branch"**
3. Branch: **main**, Folder: **/ (root)** or **/website**
   - If root doesn't work, try deploying from `/website` by pointing Pages to the `website/` subdirectory
4. Wait 2-5 minutes, then visit: https://dantyrr.github.io/IMPACT

---

## Fallback: If iCite Has No cited_by Data

If `fix_citations.py` reports 0 new citations for 2023+ papers, the issue is
that iCite's API doesn't include `cited_by` for newer papers.

**Option A: Re-run the full pipeline** (iCite updates regularly):
```bash
python scripts/run_pipeline.py --journal aging-cell --years 2020-2026
```

**Option B: Use OpenAlex** (better coverage, no key required):
The OpenAlex API at `https://api.openalex.org` provides comprehensive citation
data for all papers. An updated pipeline using OpenAlex is available if needed —
just ask Claude to implement it.

**Option C: Accept 2022-era data for now** and display the rolling IF for months
where the paper window includes 2022. The site will show a gradually improving
trend line as we accumulate data.
