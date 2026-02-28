"""One-off script to process the 152 journals newly added via J_Entrez fix."""
import json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

with open('data/journal_registry.json') as f:
    reg = json.load(f)

existing = {f.replace('.json', '') for f in os.listdir('docs/data/journals')}
new_journals = sorted([j for j in reg if j['slug'] not in existing], key=lambda x: -x['paper_count'])
print(f'Processing {len(new_journals)} journals with 4 workers...')

def run_one(j):
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline_bulk.py',
         '--registry', 'data/journal_registry.json',
         '--journal', j['slug'], '--years', '2010-2026'],
        capture_output=True, text=True
    )
    return j['slug'], result.returncode, result.stderr[-300:] if result.returncode != 0 else ''

done = errors = 0
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = {ex.submit(run_one, j): j for j in new_journals}
    for fut in as_completed(futures):
        slug, rc, err = fut.result()
        done += 1
        if rc != 0:
            errors += 1
            print(f'  FAIL [{done}/{len(new_journals)}] {slug}: {err[-150:]}', flush=True)
        elif done % 10 == 0 or done <= 3:
            print(f'  [{done}/{len(new_journals)}] done', flush=True)

print(f'Finished: {done - errors} ok, {errors} errors')
