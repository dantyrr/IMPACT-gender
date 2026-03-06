#!/usr/bin/env python3
"""
Run the IMPACT pipeline for all journals in journal_registry.json.

Processes journals in parallel (N workers), tracks progress so the run
can be safely interrupted and resumed. After each batch, exports snapshots
and uploads to R2.

Usage:
    python scripts/run_all_journals.py                  # all journals, 4 workers
    python scripts/run_all_journals.py --workers 6
    python scripts/run_all_journals.py --min-papers 1000  # only larger journals
    python scripts/run_all_journals.py --resume           # skip already-done journals
    python scripts/run_all_journals.py --dry-run          # show plan without running
"""

import sys
import os
import json
import time
import logging
import argparse
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT      = Path(__file__).parent.parent
REGISTRY_PATH  = REPO_ROOT / "data" / "journal_registry.json"
PROGRESS_PATH  = REPO_ROOT / "data" / "pipeline_progress.json"
ICITE_BULK_DB  = REPO_ROOT / "data" / "icite_bulk.db"
PYTHON         = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_all")

# How often to export snapshots + upload to R2 (every N completed journals)
SNAPSHOT_EVERY = 200


def load_progress() -> dict:
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH) as f:
            return json.load(f)
    return {"completed": [], "failed": [], "skipped": []}


def save_progress(progress: dict):
    tmp = str(PROGRESS_PATH) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(progress, f, indent=2)
    os.replace(tmp, PROGRESS_PATH)


def run_journal(entry: dict, years: str) -> tuple:
    """Run run_pipeline_bulk.py for one journal. Returns (slug, success, log)."""
    slug = entry["slug"]
    cmd = [
        PYTHON, "scripts/run_pipeline_bulk.py",
        "--journal", slug,
        "--years", years,
        "--registry", str(REGISTRY_PATH),
    ]
    # Use local iCite bulk DB if available (no API calls, unlimited parallelism)
    if ICITE_BULK_DB.exists():
        cmd += ["--icite-db", str(ICITE_BULK_DB)]
    start = time.time()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=str(REPO_ROOT), timeout=7200,  # 2hr max per journal
        )
        elapsed = time.time() - start
        success = result.returncode == 0
        log = result.stdout + result.stderr
        return slug, success, elapsed, log
    except subprocess.TimeoutExpired:
        return slug, False, 7200, "TIMEOUT after 2 hours"
    except Exception as e:
        return slug, False, time.time() - start, str(e)


def checkpoint_db():
    """Checkpoint impact.db WAL to prevent unbounded growth."""
    import sqlite3
    try:
        conn = sqlite3.connect(str(REPO_ROOT / "data" / "impact.db"))
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        logger.info("WAL checkpoint complete")
    except Exception as e:
        logger.warning(f"WAL checkpoint failed: {e}")


def run_snapshots_and_upload():
    logger.info("=" * 60)
    logger.info("Running compute_snapshots...")
    r = subprocess.run(
        [PYTHON, "scripts/compute_snapshots.py"],
        capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=21600,
    )
    if r.returncode != 0:
        logger.error(f"compute_snapshots failed:\n{r.stderr[-2000:]}")
        return False
    logger.info("compute_snapshots done.")

    logger.info("Uploading to R2...")
    r = subprocess.run(
        [PYTHON, "scripts/upload_to_r2.py"],
        capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=3600,
    )
    if r.returncode != 0:
        logger.error(f"upload_to_r2 failed:\n{r.stderr[-2000:]}")
        return False
    logger.info("R2 upload done.")
    logger.info("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel workers (default: 4)")
    parser.add_argument("--min-papers", type=int, default=100,
                        help="Skip journals with fewer papers (default: 100)")
    parser.add_argument("--years", type=str, default="2003-2026",
                        help="Year range (default: 2003-2026)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip journals already in progress file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show plan without running anything")
    parser.add_argument("--snapshot-every", type=int, default=SNAPSHOT_EVERY,
                        help=f"Export snapshots every N journals (default: {SNAPSHOT_EVERY})")
    args = parser.parse_args()

    if not REGISTRY_PATH.exists():
        logger.error(f"Registry not found: {REGISTRY_PATH}")
        logger.error("Run: python scripts/build_journal_registry.py")
        sys.exit(1)

    with open(REGISTRY_PATH) as f:
        registry = json.load(f)

    # Filter by paper count
    registry = [j for j in registry if j["paper_count"] >= args.min_papers]
    logger.info(f"Registry: {len(registry):,} journals with ≥{args.min_papers} papers")

    # Load progress
    progress = load_progress() if args.resume else {"completed": [], "failed": [], "skipped": []}
    done_slugs = set(progress["completed"] + progress["skipped"])

    # Determine what to run
    to_run = [j for j in registry if j["slug"] not in done_slugs]
    logger.info(f"  {len(done_slugs):,} already done, {len(to_run):,} to process")

    if args.dry_run:
        logger.info("Dry run — journals that would be processed:")
        for j in to_run[:20]:
            logger.info(f"  {j['slug']:50s} ({j['paper_count']:,} papers)")
        if len(to_run) > 20:
            logger.info(f"  ... and {len(to_run) - 20:,} more")
        return

    if not to_run:
        logger.info("Nothing to do — all journals already processed.")
        run_snapshots_and_upload()
        return

    logger.info(f"Starting with {args.workers} workers, snapshots every {args.snapshot_every} journals")
    logger.info(f"Progress saved to {PROGRESS_PATH}")

    start_time = time.time()
    n_done = 0
    last_snapshot_at = len(progress["completed"])

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_journal, j, args.years): j for j in to_run}

        for future in as_completed(futures):
            entry = futures[future]
            slug = entry["slug"]
            try:
                slug, success, elapsed, log = future.result()
            except Exception as e:
                success, elapsed, log = False, 0, str(e)

            n_done += 1
            total_done = len(progress["completed"]) + len(progress["failed"]) + n_done

            if success:
                progress["completed"].append(slug)
                logger.info(
                    f"[{total_done}/{len(to_run)+len(done_slugs)}] "
                    f"OK  {slug} ({elapsed:.0f}s)"
                )
            else:
                progress["failed"].append(slug)
                logger.warning(
                    f"[{total_done}/{len(to_run)+len(done_slugs)}] "
                    f"FAIL {slug} ({elapsed:.0f}s)"
                )
                # Log last 500 chars of output for debugging
                if log:
                    logger.debug(f"  Output tail: {log[-500:]}")

            save_progress(progress)

            # Periodic snapshot + upload
            newly_done = len(progress["completed"]) - last_snapshot_at
            if newly_done >= args.snapshot_every:
                last_snapshot_at = len(progress["completed"])
                elapsed_total = time.time() - start_time
                rate = n_done / elapsed_total * 3600
                remaining = len(to_run) - n_done
                logger.info(
                    f"Batch checkpoint: {len(progress['completed'])} done, "
                    f"{len(progress['failed'])} failed, "
                    f"~{remaining/rate:.1f}h remaining at {rate:.0f} journals/hr"
                )
                checkpoint_db()
                run_snapshots_and_upload()

    # Final snapshot + upload
    elapsed_total = time.time() - start_time
    logger.info(f"Pipeline complete in {elapsed_total/3600:.1f}h")
    logger.info(f"  Completed: {len(progress['completed']):,}")
    logger.info(f"  Failed:    {len(progress['failed']):,}")
    logger.info(f"  Skipped:   {len(progress['skipped']):,}")

    if progress["failed"]:
        logger.warning(f"Failed slugs: {progress['failed'][:20]}")

    run_snapshots_and_upload()


if __name__ == "__main__":
    main()
