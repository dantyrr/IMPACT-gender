"""Export snapshots (journals/authors/papers JSON) for only the newly added journals."""
import json, os, sys, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING)

DB_PATH = 'data/impact.db'
DOCS_DATA = 'docs/data'

with open('data/journal_registry.json') as f:
    reg = json.load(f)

existing_journals = {f.replace('.json', '') for f in os.listdir(f'{DOCS_DATA}/journals')}
new_slugs = {j['slug'] for j in reg if j['slug'] not in existing_journals}
print(f'Journals to snapshot: {len(new_slugs)}')


def process_one(slug):
    from src.pipeline.db_manager import DatabaseManager
    from src.pipeline.impact_calculator import ImpactCalculator
    from src.pipeline.json_exporter import JSONExporter

    db = DatabaseManager(DB_PATH)
    calc = ImpactCalculator(db)
    exporter = JSONExporter()
    now = datetime.now()

    journal = next((j for j in db.get_all_journals() if j['slug'] == slug), None)
    if not journal:
        return slug, 'not in DB'

    jid = journal['id']
    name = journal['name']
    issn = journal['issn']

    window_data = calc.compute_all_window_timeseries(
        journal_id=jid, start_year=2012, start_month=1,
        end_year=now.year, end_month=now.month,
    )
    timeseries = window_data['default']
    if not timeseries:
        return slug, 'no data'

    exporter.export_journal(
        slug=slug, name=name, issn=issn,
        timeseries=timeseries,
        timeseries_12mo=window_data['12mo'],
        timeseries_5yr=window_data['5yr'],
        official_if=None,
    )

    author_rows = db.get_paper_authors_for_journal(jid)
    exporter.export_journal_authors(slug, author_rows)

    paper_rows = db.get_papers_for_export(jid)
    geo_rows = db.get_country_by_year(jid)
    pmids = [r['pmid'] for r in paper_rows]
    cits_by_year = db.get_citations_by_year_for_pmids(pmids)
    exporter.export_journal_papers(slug, paper_rows, geo_rows, cits_by_year)

    return slug, 'ok'


done = errors = 0
with ThreadPoolExecutor(max_workers=2) as ex:
    futures = {ex.submit(process_one, slug): slug for slug in new_slugs}
    for fut in as_completed(futures):
        slug, status = fut.result()
        done += 1
        if status != 'ok':
            errors += 1
            print(f'  SKIP [{done}/{len(new_slugs)}] {slug}: {status}', flush=True)
        elif done % 20 == 0 or done <= 3:
            print(f'  [{done}/{len(new_slugs)}] done', flush=True)

print(f'Finished: {done - errors} exported, {errors} skipped')
