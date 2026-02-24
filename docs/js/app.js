/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
        this.currentJournalSlug = null;
        this.currentWindow = 'timeseries';
        this.currentType = 'all';
        this.compareMetric = 'rolling_if';
        this.compareWindow = 'timeseries';
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

            // Reset window/type state for new journal
            this.currentWindow = 'timeseries';
            this.currentType = 'all';
            this._citationMode = 'total';

            // Sync toggle button active states
            document.querySelectorAll('#window-toggle .toggle-btn').forEach(b =>
                b.classList.toggle('active', b.getAttribute('data-window') === 'timeseries')
            );
            document.querySelectorAll('#type-toggle .toggle-btn').forEach(b =>
                b.classList.toggle('active', b.getAttribute('data-type') === 'all')
            );

            // Setup toggle controls
            this.setupDetailToggles(data);

            // Initial charts
            const ts = this._getDisplayTimeseries(data);
            chartManager.createJournalChart('journal-chart', ts, officialIf, 'Citation Rate — All Articles (24-month)');
            chartManager.createCitationChart('citation-chart', ts, 'total');
            chartManager.createCompositionChart('composition-chart', ts);
            chartManager.createPapersChart('papers-chart', ts);

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

    // ---- Display timeseries helper ----

    /**
     * Return the timeseries array for the currently selected window, with
     * rolling_if values recomputed for the selected article type.
     */
    _getDisplayTimeseries(data) {
        const raw = data[this.currentWindow] || data.timeseries;
        const typeKey = this.currentType;

        if (typeKey === 'all') {
            return raw;
        }

        return raw.map(entry => {
            let rate;
            if (typeKey === 'research') {
                rate = entry.rolling_if_no_reviews;
            } else {
                const bt = entry.by_type && entry.by_type[typeKey];
                rate = (bt && bt.papers > 0) ? +(bt.citations / bt.papers).toFixed(3) : 0;
            }
            return Object.assign({}, entry, { rolling_if: rate, rolling_if_no_reviews: rate });
        });
    }

    _typeLabel(typeKey) {
        const labels = {
            all: 'All Articles', research: 'Research',
            review: 'Reviews', editorial: 'Editorials',
            letter: 'Letters', other: 'Other',
        };
        return labels[typeKey] || typeKey;
    }

    _windowLabel(windowKey) {
        const labels = {
            timeseries: '24-month', timeseries_12mo: '12-month', timeseries_5yr: '5-yr (yr 2–6)',
        };
        return labels[windowKey] || windowKey;
    }

    setupDetailToggles(data) {
        const redraw = () => {
            const ts = this._getDisplayTimeseries(data);
            const rateLabel = `Citation Rate — ${this._typeLabel(this.currentType)} (${this._windowLabel(this.currentWindow)})`;
            chartManager.createJournalChart('journal-chart', ts, data.official_jif_2024, rateLabel);
            chartManager.createCitationChart('citation-chart', ts, this._citationChartMode());
            chartManager.createCompositionChart('composition-chart', ts);
            chartManager.createPapersChart('papers-chart', ts);
        };

        // Window toggle
        this._setupToggleGroup('window-toggle', (windowKey) => {
            this.currentWindow = windowKey;
            redraw();
        }, 'data-window');

        // Article type toggle
        this._setupToggleGroup('type-toggle', (typeKey) => {
            this.currentType = typeKey;
            redraw();
        }, 'data-type');

        // Citation chart mode toggle (independent)
        this._setupToggleGroup('citation-chart-toggle', (mode) => {
            this._citationMode = mode;
            chartManager.createCitationChart('citation-chart', this._getDisplayTimeseries(data), mode);
        });
    }

    _citationChartMode() {
        return this._citationMode || 'total';
    }

    _setupToggleGroup(containerId, callback, dataAttr = 'data-mode') {
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
                callback(newBtn.getAttribute(dataAttr));
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

        // Window toggle
        this._setupToggleGroup('compare-window-toggle', (windowKey) => {
            this.compareWindow = windowKey;
            this.updateComparison();
        }, 'data-window');

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

        chartManager.createComparisonChart('compare-chart', journalsData, this.compareMetric, this.compareWindow);

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
            { key: 'rolling_if', label: 'Citation Rate', format: UIHelpers.formatIF },
            { key: 'if_no_reviews', label: 'Rate (No Rev)', format: UIHelpers.formatIF },
            { key: 'official_jif', label: 'Official JIF', format: UIHelpers.formatIF },
            { key: 'papers', label: 'Papers', format: UIHelpers.formatInt },
            { key: 'citations', label: 'Citations', format: UIHelpers.formatInt },
            { key: 'review_pct', label: 'Review %', format: UIHelpers.formatPct },
        ];

        container.innerHTML = '';
        container.appendChild(UIHelpers.createTable(rows, columns));
    }

    // ---- Author Metrics ----

    // NLM journal abbreviations → slug mapping for our tracked journals
    static get NLM_MAP() {
        return {
            'n engl j med': 'nejm',
            'new england journal of medicine': 'nejm',
            'lancet': 'lancet',
            'the lancet': 'lancet',
            'jama': 'jama',
            'ann intern med': 'annals-internal-medicine',
            'annals of internal medicine': 'annals-internal-medicine',
            'nat med': 'nature-medicine',
            'nature medicine': 'nature-medicine',
            'cell': 'cell',
            'sci transl med': 'science-translational-medicine',
            'science translational medicine': 'science-translational-medicine',
            'circulation': 'circulation',
            'blood': 'blood',
            'immunity': 'immunity',
            'nat immunol': 'nature-immunology',
            'nature immunology': 'nature-immunology',
            'gastroenterology': 'gastroenterology',
            'gut': 'gut',
            'aging cell': 'aging-cell',
            'j clin invest': 'jci',
            'journal of clinical investigation': 'jci',
            'nat aging': 'nature-aging',
            'nature aging': 'nature-aging',
            'elife': 'elife',
            'j am heart assoc': 'jaha',
            'journal of the american heart association': 'jaha',
        };
    }

    setupAuthor() {
        document.getElementById('author-analyze-btn').addEventListener('click', () => {
            this.analyzeAuthor();
        });
        document.getElementById('pmid-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.analyzeAuthor();
        });
    }

    async analyzeAuthor() {
        const input = document.getElementById('pmid-input').value.trim();
        if (!input) return;

        const pmids = input.split(/[\s,]+/).map(s => s.trim()).filter(s => s && /^\d+$/.test(s));
        if (pmids.length === 0) return;

        const resultsEl = document.getElementById('author-results');
        const tableContainer = document.getElementById('author-table-container');
        resultsEl.style.display = 'block';
        tableContainer.innerHTML = '<p class="loading-text">Fetching citation data from iCite...</p>';
        document.getElementById('author-chart-container').style.display = 'none';
        ['author-papers', 'author-cites', 'author-avg', 'author-weighted-if'].forEach(id => {
            document.getElementById(id).textContent = '—';
        });

        // Step 1: fetch paper metadata from iCite
        const iciteMap = await this._fetchICite(pmids);

        // Step 2: for each PMID, compute 24-month citations from publication date
        const paperResults = [];

        for (const pmid of pmids) {
            const paper = iciteMap[String(pmid)];
            if (!paper) {
                paperResults.push({ pmid, found: false });
                continue;
            }

            const paperPubYear = paper.year;
            const citedBy = paper.cited_by || [];
            let cit24mo = 0;
            let approx = false;

            if (citedBy.length > 0 && paperPubYear) {
                tableContainer.innerHTML = `<p class="loading-text">Fetching citation dates for PMID ${pmid} (${citedBy.length} citations)…</p>`;
                const fetchList = citedBy.length > 600 ? citedBy.slice(-600) : citedBy;
                if (citedBy.length > 600) approx = true;
                const citedByMap = await this._fetchICite(fetchList);
                // Count citations received in the 24 months after publication
                const recentCount = Object.values(citedByMap).filter(
                    p => p.year != null && p.year >= paperPubYear && p.year < paperPubYear + 2
                ).length;
                cit24mo = approx
                    ? Math.round(recentCount * citedBy.length / fetchList.length)
                    : recentCount;
            }

            // Get time-matched journal benchmark (journal rate ~24 months after publication)
            const journalMatch = this._matchJournal(paper.journal);
            let journalRate = null;
            let journalRateMonth = null;
            if (journalMatch && paperPubYear) {
                const targetMonth = `${paperPubYear + 2}-01`;
                const rateInfo = await this._getJournalRateForPeriod(journalMatch.slug, targetMonth);
                if (rateInfo) {
                    journalRate = rateInfo.rate;
                    journalRateMonth = rateInfo.month;
                } else {
                    journalRate = journalMatch.latest_if;
                }
            }

            paperResults.push({
                pmid,
                found: true,
                title: paper.title || '',
                journal: paper.journal || '—',
                year: paperPubYear || '—',
                total_citations: paper.citation_count || 0,
                citations_24mo: cit24mo,
                approx,
                journal_name: journalMatch ? journalMatch.name : null,
                journal_rate: journalRate,
                journal_rate_month: journalRateMonth,
            });
        }

        // Summary metrics
        const found = paperResults.filter(p => p.found);
        const totalCites = found.reduce((s, p) => s + p.total_citations, 0);
        const avg24mo = found.length > 0
            ? found.reduce((s, p) => s + p.citations_24mo, 0) / found.length
            : 0;

        document.getElementById('author-papers').textContent = `${found.length} / ${pmids.length}`;
        document.getElementById('author-cites').textContent = UIHelpers.formatInt(totalCites);
        document.getElementById('author-avg').textContent = found.length > 0 ? UIHelpers.formatIF(totalCites / found.length) : '—';
        document.getElementById('author-weighted-if').textContent = found.length > 0 ? UIHelpers.formatIF(avg24mo) : '—';

        this._renderAuthorTable(tableContainer, paperResults);

        if (found.length > 0) {
            document.getElementById('author-chart-container').style.display = 'block';
            chartManager.createAuthorChart('author-chart', found);
        }
    }

    /**
     * Load a journal's full timeseries and find the rolling citation rate
     * entry closest to targetMonth (YYYY-MM). Returns {rate, month} or null.
     */
    async _getJournalRateForPeriod(slug, targetMonth) {
        let jData = this.journalDataCache[slug];
        if (!jData) {
            try {
                jData = await dataLoader.loadJournal(slug);
                this.journalDataCache[slug] = jData;
            } catch (e) {
                return null;
            }
        }

        const ts = jData.timeseries || [];
        const target = new Date(targetMonth + '-01').getTime();
        let best = null;
        let bestDist = Infinity;

        for (const t of ts) {
            if (!t.rolling_if || t.rolling_if === 0 || !t.papers) continue;
            const dist = Math.abs(new Date(t.month + '-01').getTime() - target);
            if (dist < bestDist) {
                bestDist = dist;
                best = t;
            }
        }

        return best ? { rate: best.rolling_if, month: best.month } : null;
    }

    async _fetchICite(pmids) {
        if (!pmids || pmids.length === 0) return {};
        const out = {};
        const batchSize = 200;
        for (let i = 0; i < pmids.length; i += batchSize) {
            const batch = pmids.slice(i, i + batchSize);
            try {
                const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${batch.join(',')}`);
                if (!resp.ok) continue;
                const json = await resp.json();
                const items = Array.isArray(json) ? json : (json.data || []);
                items.forEach(p => { out[String(p.pmid)] = p; });
            } catch (e) {
                console.error('iCite fetch error:', e);
            }
        }
        return out;
    }

    _matchJournal(journalStr) {
        if (!journalStr) return null;
        const s = journalStr.toLowerCase().trim();
        const slug = IMPACTApp.NLM_MAP[s];
        if (slug) {
            return this.journals.find(j => j.slug === slug) || null;
        }
        // Fallback: partial string match against names/abbreviations
        for (const j of this.journals) {
            const name = j.name.toLowerCase();
            const abbr = (j.abbreviation || '').toLowerCase();
            if (s === name || s === abbr || name.includes(s) || (abbr && abbr.includes(s))) {
                return j;
            }
        }
        return null;
    }

    _renderAuthorTable(container, paperResults) {
        container.innerHTML = '';
        const found = paperResults.filter(p => p.found);

        if (found.length === 0) {
            container.innerHTML = '<p class="error-text">No papers found in iCite. Check that the PMIDs are valid.</p>';
            return;
        }

        const rows = paperResults.map(p => ({
            pmid: p.pmid,
            title: p.found ? (p.title.length > 60 ? p.title.slice(0, 60) + '…' : p.title) : 'Not found',
            journal: p.found ? p.journal : '—',
            year: p.found ? String(p.year) : '—',
            cit24mo: p.found ? `${p.approx ? '~' : ''}${p.citations_24mo}` : '—',
            total: p.found ? p.total_citations : null,
            journal_rate: p.found ? p.journal_rate : null,
            benchmark_month: p.found ? (p.journal_rate_month || (p.journal_rate != null ? 'latest' : '—')) : '—',
        }));

        container.appendChild(UIHelpers.createTable(rows, [
            { key: 'pmid', label: 'PMID' },
            { key: 'title', label: 'Title' },
            { key: 'journal', label: 'Journal' },
            { key: 'year', label: 'Published' },
            { key: 'cit24mo', label: '24-mo Citations' },
            { key: 'total', label: 'Total Citations', format: UIHelpers.formatInt },
            { key: 'journal_rate', label: 'Journal Rate (benchmark)', format: UIHelpers.formatIF },
            { key: 'benchmark_month', label: 'Benchmark month' },
        ]));

        const note = document.createElement('p');
        note.className = 'data-note';
        note.textContent = '24-mo Citations: citations received in the 24 months after the paper\'s publication year (from iCite). ~ = estimate (>600 citations sampled). Journal Rate (benchmark): the journal\'s rolling citation rate at approximately 24 months after the paper was published — the same period when most of the paper\'s 24-mo citations were accumulating.';
        container.appendChild(note);
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
                { key: 'latest_if', label: 'Citation Rate', format: UIHelpers.formatIF },
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
