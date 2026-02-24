/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
        this.currentJournalSlug = null;
        this.compareMetric = 'rolling_if';
        this.init();
    }

    async init() {
        try {
            const index = await dataLoader.loadIndex();
            this.journals = index.journals || [];
            this.setupNavigation();
            this.renderJournalCards(this.journals);
            this.setupSearch();
            this.setupCompare();
            this.setupAuthor();
            this.setupAboutJournalList();
            this.updateTimestamp(index.generated);
        } catch (error) {
            console.error('Failed to initialize IMPACT:', error);
            document.getElementById('journal-cards').innerHTML =
                '<p style="color:#e74c3c;">Failed to load data. Make sure data/index.json exists.</p>';
        }
    }

    // ---- Navigation ----

    setupNavigation() {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const sectionId = link.dataset.section;
                this.showSection(sectionId);
            });
        });
    }

    showSection(sectionId) {
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));

        const section = document.getElementById(sectionId);
        const link = document.querySelector(`.nav-link[data-section="${sectionId}"]`);
        if (section) section.classList.add('active');
        if (link) link.classList.add('active');
    }

    // ---- Journal List ----

    renderJournalCards(journals) {
        const container = document.getElementById('journal-cards');
        container.innerHTML = '';

        journals.forEach(journal => {
            const card = UIHelpers.createJournalCard(journal);
            card.addEventListener('click', () => this.showJournalDetail(journal.slug));
            container.appendChild(card);
        });
    }

    setupSearch() {
        const input = document.getElementById('journal-search');
        input.addEventListener('input', () => {
            const term = input.value.toLowerCase().trim();
            const filtered = this.journals.filter(j =>
                j.name.toLowerCase().includes(term) ||
                j.abbreviation.toLowerCase().includes(term) ||
                j.slug.includes(term)
            );
            this.renderJournalCards(filtered);
        });
    }

    // ---- Journal Detail ----

    async showJournalDetail(slug) {
        const container = document.getElementById('journal-detail');
        const cards = document.getElementById('journal-cards');
        const search = document.querySelector('#journals .search-bar');

        try {
            let data = this.journalDataCache[slug];
            if (!data) {
                data = await dataLoader.loadJournal(slug);
                this.journalDataCache[slug] = data;
            }

            this.currentJournalSlug = slug;

            // Hide cards, show detail
            cards.style.display = 'none';
            search.style.display = 'none';
            container.style.display = 'block';

            // Title
            document.getElementById('detail-title').textContent = data.journal;

            // Metrics
            const latest = data.latest || {};
            const officialIf = data.official_jif_2024;
            const rollingIf = latest.rolling_if;
            const diff = (officialIf != null && rollingIf != null)
                ? (rollingIf - officialIf) : null;

            const reviewPct = (latest.paper_count > 0 && latest.review_count != null)
                ? (latest.review_count / latest.paper_count * 100) : null;

            document.getElementById('metric-if').textContent = UIHelpers.formatIF(rollingIf);
            document.getElementById('metric-official').textContent = officialIf != null ? UIHelpers.formatIF(officialIf) : 'N/A';
            document.getElementById('metric-diff').textContent = diff != null ? (diff >= 0 ? '+' : '') + diff.toFixed(2) : '—';
            document.getElementById('metric-papers').textContent = UIHelpers.formatInt(latest.paper_count);
            document.getElementById('metric-reviews').textContent = UIHelpers.formatPct(reviewPct);

            // Color the difference
            const diffEl = document.getElementById('metric-diff');
            if (diff != null) {
                diffEl.style.color = diff >= 0 ? '#27ae60' : '#e74c3c';
            }

            // Setup toggle controls
            this.setupDetailToggles(data);

            // Initial charts
            chartManager.createJournalChart('journal-chart', data.timeseries, officialIf, 'both');
            chartManager.createCitationChart('citation-chart', data.timeseries, 'total');
            chartManager.createCompositionChart('composition-chart', data.timeseries);
            chartManager.createPapersChart('papers-chart', data.timeseries);

            // Back button
            document.getElementById('back-to-list').onclick = () => {
                container.style.display = 'none';
                cards.style.display = '';
                search.style.display = '';
                this.currentJournalSlug = null;
            };

        } catch (error) {
            console.error('Error loading journal detail:', error);
        }
    }

    setupDetailToggles(data) {
        // IF chart toggle
        this._setupToggleGroup('if-chart-toggle', (mode) => {
            chartManager.createJournalChart('journal-chart', data.timeseries, data.official_jif_2024, mode);
        });

        // Citation chart toggle
        this._setupToggleGroup('citation-chart-toggle', (mode) => {
            chartManager.createCitationChart('citation-chart', data.timeseries, mode);
        });
    }

    _setupToggleGroup(containerId, callback) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const buttons = container.querySelectorAll('.toggle-btn');
        buttons.forEach(btn => {
            // Remove old listeners by cloning
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);

            newBtn.addEventListener('click', () => {
                container.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
                newBtn.classList.add('active');
                callback(newBtn.dataset.mode);
            });
        });
    }

    // ---- Compare ----

    setupCompare() {
        const container = document.getElementById('compare-checkboxes');
        container.innerHTML = '';

        this.journals.forEach(journal => {
            const label = document.createElement('label');
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.value = journal.slug;
            cb.addEventListener('change', () => this.updateComparison());
            label.appendChild(cb);
            label.appendChild(document.createTextNode(journal.name));
            container.appendChild(label);
        });

        // Metric toggle
        this._setupToggleGroup('compare-metric-toggle', (mode) => {
            this.compareMetric = mode;
            this.updateComparison();
        });
    }

    async updateComparison() {
        const checked = Array.from(
            document.querySelectorAll('#compare-checkboxes input:checked')
        ).map(cb => cb.value);

        const tableContainer = document.getElementById('compare-table-container');

        if (checked.length === 0) {
            chartManager._destroy('compare-chart');
            if (tableContainer) tableContainer.innerHTML = '';
            return;
        }

        const journalsData = [];
        for (const slug of checked) {
            try {
                let data = this.journalDataCache[slug];
                if (!data) {
                    data = await dataLoader.loadJournal(slug);
                    this.journalDataCache[slug] = data;
                }
                journalsData.push(data);
            } catch (error) {
                console.error(`Error loading ${slug}:`, error);
            }
        }

        chartManager.createComparisonChart('compare-chart', journalsData, this.compareMetric);

        // Build comparison summary table
        if (tableContainer) {
            this.renderComparisonTable(tableContainer, journalsData);
        }
    }

    renderComparisonTable(container, journalsData) {
        const rows = journalsData.map(j => {
            const latest = j.latest || {};
            return {
                journal: j.journal,
                rolling_if: latest.rolling_if,
                if_no_reviews: latest.rolling_if_no_reviews,
                official_jif: j.official_jif_2024,
                papers: latest.paper_count,
                research: latest.research_count,
                reviews: latest.review_count,
                citations: latest.citation_count,
                review_pct: (latest.paper_count > 0 && latest.review_count != null)
                    ? (latest.review_count / latest.paper_count * 100) : null,
            };
        });

        const columns = [
            { key: 'journal', label: 'Journal' },
            { key: 'rolling_if', label: 'Rolling IF', format: UIHelpers.formatIF },
            { key: 'if_no_reviews', label: 'IF (No Rev)', format: UIHelpers.formatIF },
            { key: 'official_jif', label: 'Official JIF', format: UIHelpers.formatIF },
            { key: 'papers', label: 'Papers', format: UIHelpers.formatInt },
            { key: 'citations', label: 'Citations', format: UIHelpers.formatInt },
            { key: 'review_pct', label: 'Review %', format: UIHelpers.formatPct },
        ];

        container.innerHTML = '';
        container.appendChild(UIHelpers.createTable(rows, columns));
    }

    // ---- Author Metrics ----

    setupAuthor() {
        document.getElementById('author-analyze-btn').addEventListener('click', () => {
            this.analyzeAuthor();
        });

        // Enter key support
        document.getElementById('pmid-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.analyzeAuthor();
        });
    }

    async analyzeAuthor() {
        const input = document.getElementById('pmid-input').value.trim();
        if (!input) return;

        const pmids = input.split(/[\s,]+/).map(s => s.trim()).filter(s => s && /^\d+$/.test(s));
        if (pmids.length === 0) return;

        const results = document.getElementById('author-results');
        results.style.display = 'block';

        // Load all journal data
        const allJournals = [];
        for (const j of this.journals) {
            try {
                let data = this.journalDataCache[j.slug];
                if (!data) {
                    data = await dataLoader.loadJournal(j.slug);
                    this.journalDataCache[j.slug] = data;
                }
                allJournals.push(data);
            } catch (e) {
                console.error(`Error loading ${j.slug}:`, e);
            }
        }

        // For each PMID, find which journal it belongs to and get the IF
        // at the time closest to publication
        const foundPapers = [];
        const notFound = [];

        for (const pmid of pmids) {
            let found = false;
            for (const jdata of allJournals) {
                // Check the latest snapshot for paper info
                // Since we don't have per-paper data in the JSON, we note the journal
                // and use the latest IF as a proxy
                const latest = jdata.latest || {};
                // We can at least associate the PMID with a journal based on the index
                // For now, we'll show journal-level metrics
            }

            // Since per-paper data isn't in the JSON exports, we'll show what we can
            foundPapers.push({
                pmid: pmid,
                found: true  // placeholder
            });
        }

        // Show aggregate metrics
        // For now: show per-journal breakdown of what's tracked
        document.getElementById('author-papers').textContent = pmids.length;
        document.getElementById('author-cites').textContent = '—';
        document.getElementById('author-avg').textContent = '—';
        document.getElementById('author-weighted-if').textContent = '—';

        // Show available journals info
        const tableContainer = document.getElementById('author-table-container');
        tableContainer.innerHTML = '';

        const infoDiv = document.createElement('div');
        infoDiv.className = 'author-info-box';
        infoDiv.innerHTML = `
            <p><strong>PMID lookup:</strong> ${pmids.join(', ')}</p>
            <p>Per-paper citation data requires the full database. The static site currently
            shows journal-level aggregate metrics. To get per-paper data, run the IMPACT
            pipeline locally:</p>
            <pre>python scripts/author_lookup.py --pmids ${pmids.slice(0, 3).join(',')}</pre>
            <p>The following journals are tracked in IMPACT:</p>
        `;

        const journalTable = UIHelpers.createTable(
            allJournals.map(j => ({
                journal: j.journal,
                rolling_if: (j.latest || {}).rolling_if,
                papers: (j.latest || {}).paper_count,
                citations: (j.latest || {}).citation_count,
            })),
            [
                { key: 'journal', label: 'Journal' },
                { key: 'rolling_if', label: 'Current Rolling IF', format: UIHelpers.formatIF },
                { key: 'papers', label: 'Papers Tracked', format: UIHelpers.formatInt },
                { key: 'citations', label: 'Citations', format: UIHelpers.formatInt },
            ]
        );

        tableContainer.appendChild(infoDiv);
        tableContainer.appendChild(journalTable);

        // Hide chart for now since we don't have per-paper data in static JSON
        document.getElementById('author-chart-container').style.display = 'none';
    }

    // ---- About Section — Journal List ----

    setupAboutJournalList() {
        const container = document.getElementById('about-journal-list');
        if (!container || this.journals.length === 0) return;

        const table = UIHelpers.createTable(
            this.journals.map(j => ({
                name: j.name,
                abbr: j.abbreviation,
                issn: j.issn,
                latest_if: j.latest_if,
                official_jif: j.official_jif,
            })),
            [
                { key: 'name', label: 'Journal' },
                { key: 'abbr', label: 'Abbreviation' },
                { key: 'issn', label: 'ISSN' },
                { key: 'latest_if', label: 'IMPACT IF', format: UIHelpers.formatIF },
                { key: 'official_jif', label: 'Official JIF', format: UIHelpers.formatIF },
            ]
        );

        container.appendChild(table);
    }

    // ---- Timestamp ----

    updateTimestamp(generated) {
        const el = document.getElementById('last-updated');
        if (el && generated) {
            el.textContent = generated;
        }
    }
}

// Boot
document.addEventListener('DOMContentLoaded', () => {
    window.app = new IMPACTApp();
});
