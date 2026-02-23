/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
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
        const search = document.querySelector('.search-bar');

        try {
            let data = this.journalDataCache[slug];
            if (!data) {
                data = await dataLoader.loadJournal(slug);
                this.journalDataCache[slug] = data;
            }

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

            // Charts
            chartManager.createJournalChart('journal-chart', data.timeseries, officialIf);
            chartManager.createPapersChart('papers-chart', data.timeseries);

            // Back button
            document.getElementById('back-to-list').onclick = () => {
                container.style.display = 'none';
                cards.style.display = '';
                search.style.display = '';
            };

        } catch (error) {
            console.error('Error loading journal detail:', error);
        }
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
    }

    async updateComparison() {
        const checked = Array.from(
            document.querySelectorAll('#compare-checkboxes input:checked')
        ).map(cb => cb.value);

        if (checked.length === 0) {
            chartManager._destroy('compare-chart');
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

        chartManager.createComparisonChart('compare-chart', journalsData);
    }

    // ---- Author Metrics ----

    setupAuthor() {
        document.getElementById('author-analyze-btn').addEventListener('click', () => {
            this.analyzeAuthor();
        });
    }

    async analyzeAuthor() {
        const input = document.getElementById('pmid-input').value.trim();
        if (!input) return;

        const pmids = input.split(',').map(s => s.trim()).filter(s => s);
        const results = document.getElementById('author-results');

        // For now, show a placeholder — full functionality requires server-side
        // or pre-computed author data
        results.style.display = 'block';
        document.getElementById('author-papers').textContent = pmids.length;
        document.getElementById('author-cites').textContent = '—';
        document.getElementById('author-avg').textContent = '—';

        const tableContainer = document.getElementById('author-table-container');
        tableContainer.innerHTML = '<p style="color: var(--text-light);">Author analysis requires papers to be in the IMPACT database. Run the pipeline first, then author data will be available here.</p>';
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
