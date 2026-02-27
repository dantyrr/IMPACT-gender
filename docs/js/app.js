/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
        this.authorDataCache = {};  // slug → {pmid: {f,fa,l,la}} or null if not available
        this._cyNetwork = null;
        this._networkCenter = null;
        this._networkCitedBy = [];
        this._networkPaperCache = new Map();
        this._networkColorMode = 'year';
        this._networkLayoutMode = 'force';
        this._influenceJournalSlug = null;
        this._authorAllPapers = [];
        this._authorExcluded = new Set();
        this._authorTotalFound = 0;
        this._authorSort = { col: 'citations', dir: 'desc' };
        this.currentJournalSlug = null;
        this.currentWindow = 'timeseries';
        this.currentType = 'all';
        this.compareMetric = 'rolling_if';
        this.compareWindow = 'timeseries';
        this.jcWindow = 'timeseries';
        this.init();
    }

    async init() {
        try {
            const index = await dataLoader.loadIndex();
            this.journals = index.journals || [];
            this.setupNavigation();
            this.renderJournalCards(this.journals);
            this.setupSearch();
            this.setupJournalTrendsPanel();
            this.setupCompare();
            this.setupCitationNetwork();
            this.setupAuthorSearch();
            this.setupGeography();
            this.setupInfluence();
            this.setupAboutJournalList();
            this.updateTimestamp(index.generated);
        } catch (error) {
            console.error('Failed to initialize IMPACT:', error);
            document.getElementById('journal-cards').innerHTML =
                `<p style="color:#e74c3c;">Failed to initialize: ${error.message}</p>`;
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

        // Re-measure cy canvas when papers tab becomes visible
        if (sectionId === 'papers' && this._cyNetwork) {
            requestAnimationFrame(() => this._cyNetwork.resize());
        }
    }

    // ---- Journal List ----

    renderJournalCards(journals) {
        const container = document.getElementById('journal-cards');
        container.innerHTML = '';

        journals.forEach(journal => {
            const card = UIHelpers.createJournalCard(journal);
            card.dataset.slug = journal.slug;
            card.addEventListener('click', () => this.showJournalDetail(journal.slug));
            container.appendChild(card);
        });
    }

    setupSearch() {
        document.getElementById('journal-search').addEventListener('input', () => this._renderFilteredCards());
    }

    _renderFilteredCards() {
        const term = (document.getElementById('journal-search').value || '').toLowerCase().trim();
        const selected = this.jcPicker ? this.jcPicker.getSelected() : [];
        document.querySelectorAll('#journal-cards [data-slug]').forEach(card => {
            const slug = card.dataset.slug;
            const j = this.journals.find(j => j.slug === slug);
            if (!j) { card.style.display = 'none'; return; }
            const matchesSel = selected.length === 0 || selected.includes(slug);
            const matchesSearch = !term || j.name.toLowerCase().includes(term) ||
                (j.abbreviation || '').toLowerCase().includes(term) || j.slug.includes(term);
            card.style.display = matchesSel && matchesSearch ? '' : 'none';
        });
    }

    // ---- Journal Trends Comparison ----

    setupJournalTrendsPanel() {
        this.jcPicker = new JournalPicker(
            'jc-picker', this.journals, chartManager.palette,
            () => this.updateJournalsTrendsChart()
        );

        document.querySelectorAll('#jc-type-checkboxes input').forEach(cb => {
            cb.addEventListener('change', () => this.updateJournalsTrendsChart());
        });

        this._setupToggleGroup('jc-window-toggle', (windowKey) => {
            this.jcWindow = windowKey;
            this.updateJournalsTrendsChart();
        }, 'data-window');

        document.getElementById('dl-png').addEventListener('click', () => this._downloadChart('png'));
        document.getElementById('dl-jpg').addEventListener('click', () => this._downloadChart('jpg'));
        document.getElementById('dl-pdf').addEventListener('click', () => this._downloadPDF());
        document.getElementById('dl-csv').addEventListener('click', () => this._downloadCSV());
    }

    async updateJournalsTrendsChart() {
        const checkedJournals = this.jcPicker.getSelected();

        const checkedTypes = Array.from(
            document.querySelectorAll('#jc-type-checkboxes input:checked')
        ).map(cb => cb.value);

        const chartContainer = document.getElementById('jc-chart-container');
        const hint = document.getElementById('jc-hint');

        if (checkedJournals.length === 0 || checkedTypes.length === 0) {
            chartManager._destroy('jc-chart');
            chartContainer.style.display = 'none';
            hint.style.display = '';
            document.getElementById('jc-download-bar').style.display = 'none';
            this._jcSeriesData = null;
            this._renderFilteredCards();
            return;
        }

        const journalsData = [];
        for (const slug of checkedJournals) {
            let data = this.journalDataCache[slug];
            if (!data) {
                try {
                    data = await dataLoader.loadJournal(slug);
                    this.journalDataCache[slug] = data;
                } catch (e) {
                    console.error(`Error loading ${slug}:`, e);
                    continue;
                }
            }
            journalsData.push(data);
        }

        if (journalsData.length === 0) return;

        hint.style.display = 'none';
        chartContainer.style.display = 'block';

        const typeDashes = {
            all: [], research: [8, 4], review: [4, 4],
            editorial: [2, 2], letter: [8, 4, 2, 4], other: [12, 3],
        };
        const typeLabels = {
            all: 'All Articles', research: 'Research', review: 'Reviews',
            editorial: 'Editorials', letter: 'Letters', other: 'Other',
        };

        const multiJournal = journalsData.length > 1;
        const multiType = checkedTypes.length > 1;
        const series = [];
        const colorMap = this.jcPicker.getColorMap();

        journalsData.forEach((jData, jIdx) => {
            const color = colorMap[jData.slug] || chartManager.palette[jIdx % chartManager.palette.length];
            const raw = jData[this.jcWindow] || jData.timeseries;
            const startIdx = raw.findIndex(d => d.papers > 0);
            const ts = startIdx >= 0 ? raw.slice(startIdx) : raw;

            checkedTypes.forEach(typeKey => {
                const values = ts.map(entry => {
                    if (typeKey === 'all') return entry.rolling_if;
                    if (typeKey === 'research') return entry.rolling_if_no_reviews;
                    const bt = entry.by_type && entry.by_type[typeKey];
                    return (bt && bt.papers > 0) ? +(bt.citations / bt.papers).toFixed(3) : null;
                });

                let label;
                if (multiJournal && multiType) {
                    label = `${jData.journal} — ${typeLabels[typeKey]}`;
                } else if (multiJournal) {
                    label = jData.journal;
                } else {
                    label = typeLabels[typeKey];
                }

                series.push({
                    label,
                    color,
                    // Only use dashes to distinguish types when multiple types are selected
                    dash: multiType ? (typeDashes[typeKey] || []) : [],
                    months: ts.map(d => d.month),
                    values,
                });
            });
        });

        this._jcSeriesData = series;
        chartManager.createMultiSeriesChart('jc-chart', series);
        document.getElementById('jc-download-bar').style.display = '';
        this._renderFilteredCards();
    }

    // ---- Chart Downloads ----

    _downloadChart(format) {
        const chart = chartManager.charts['jc-chart'];
        if (!chart) return;
        const mime = format === 'jpg' ? 'image/jpeg' : 'image/png';
        const url = chart.toBase64Image(mime, 1);
        const a = document.createElement('a');
        a.href = url;
        a.download = `impact-citation-rates.${format}`;
        a.click();
    }

    _downloadPDF() {
        const chart = chartManager.charts['jc-chart'];
        if (!chart || !window.jspdf) return;
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
        const pw = doc.internal.pageSize.getWidth();
        const ph = doc.internal.pageSize.getHeight();
        const imgW = pw - 20;
        const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
        doc.setFontSize(11);
        doc.text('IMPACT — Journal Citation Rate Trends', 10, 10);
        doc.addImage(chart.toBase64Image('image/png', 1), 'PNG', 10, 16, imgW, imgH);
        doc.save('impact-citation-rates.pdf');
    }

    _downloadCSV() {
        if (!this._jcSeriesData || !this._jcSeriesData.length) return;
        const allMonths = [...new Set(this._jcSeriesData.flatMap(s => s.months))].sort();
        const header = ['Month', ...this._jcSeriesData.map(s => `"${s.label.replace(/"/g, '""')}"`)].join(',');
        const rows = allMonths.map(m => {
            const vals = this._jcSeriesData.map(s => {
                const i = s.months.indexOf(m);
                const v = i >= 0 ? s.values[i] : null;
                return v != null ? v : '';
            });
            return [m, ...vals].join(',');
        });
        const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'impact-citation-rates.csv';
        a.click();
        URL.revokeObjectURL(url);
    }

    // ---- Journal Detail ----

    async showJournalDetail(slug) {
        const container = document.getElementById('journal-detail');
        const journalsMain = document.getElementById('journals-main');

        try {
            let data = this.journalDataCache[slug];
            if (!data) {
                data = await dataLoader.loadJournal(slug);
                this.journalDataCache[slug] = data;
            }

            this.currentJournalSlug = slug;

            // Hide browse + comparison panel, show detail
            journalsMain.style.display = 'none';
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
                journalsMain.style.display = '';
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
        this.comparePicker = new JournalPicker(
            'compare-picker', this.journals, chartManager.palette,
            () => this.updateComparison()
        );

        this._setupToggleGroup('compare-window-toggle', (windowKey) => {
            this.compareWindow = windowKey;
            this.updateComparison();
        }, 'data-window');

        this._setupToggleGroup('compare-metric-toggle', (mode) => {
            this.compareMetric = mode;
            this.updateComparison();
        });
    }

    async updateComparison() {
        const checked = this.comparePicker.getSelected();

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

    // ---- Citation Network ----

    setupCitationNetwork() {
        document.getElementById('network-search-btn').addEventListener('click', () => this.loadCitationNetwork());
        document.getElementById('network-pmid-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.loadCitationNetwork();
        });
    }

    async loadCitationNetwork() {
        const pmid = document.getElementById('network-pmid-input').value.trim().replace(/\D/g, '');
        if (!pmid) return;

        const hint = document.getElementById('network-hint');
        const results = document.getElementById('network-results');
        hint.textContent = 'Fetching paper data…';
        hint.style.display = '';
        results.style.display = 'none';
        document.getElementById('network-selected-panel').style.display = 'none';
        document.getElementById('network-controls').style.display = 'none';
        document.getElementById('network-download-bar').style.display = 'none';

        // Reset state for new paper
        this._networkPaperCache = new Map();
        this._networkCenter = null;
        this._networkCitedBy = [];
        this._networkLayoutMode = 'force';

        try {
            const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${pmid}`);
            if (!resp.ok) throw new Error('iCite API error');
            const json = await resp.json();
            const items = Array.isArray(json) ? json : (json.data || []);
            if (!items.length) { hint.textContent = 'Paper not found in iCite. Check the PMID.'; return; }

            const center = items[0];
            const citedBy = center.cited_by || [];
            if (!citedBy.length) {
                hint.textContent = 'No citing papers found for this PMID yet.';
                return;
            }

            this._networkCenter = center;
            this._networkCitedBy = citedBy;

            document.getElementById('network-metrics').innerHTML = [
                [center.citation_count.toLocaleString(), 'Total Citations'],
                [citedBy.length.toLocaleString(), 'Citing Papers'],
                [center.year || '—', 'Year Published'],
            ].map(([v, l]) =>
                `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
            ).join('');

            hint.style.display = 'none';
            results.style.display = '';

            const count = parseInt(document.getElementById('network-count-select').value) || 200;
            hint.style.display = '';
            hint.textContent = `Loading ${Math.min(count, citedBy.length).toLocaleString()} citing papers…`;
            await this._fetchAndRenderNetwork(count);
            hint.style.display = 'none';

            this._setupNetworkControls();

        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('Citation network error:', e);
        }
    }

    async _fetchAndRenderNetwork(count) {
        const toFetchPmids = this._networkCitedBy.slice(0, count).map(String);
        const uncached = toFetchPmids.filter(p => !this._networkPaperCache.has(p));
        if (uncached.length) {
            const papers = await this._fetchICiteBatch(uncached);
            papers.forEach(p => this._networkPaperCache.set(String(p.pmid), p));
        }
        const papers = toFetchPmids
            .map(p => this._networkPaperCache.get(p))
            .filter(Boolean);
        papers.sort((a, b) => (b.citation_count || 0) - (a.citation_count || 0));

        document.getElementById('network-note').textContent =
            this._networkCitedBy.length > papers.length
                ? `Showing ${papers.length.toLocaleString()} of ${this._networkCitedBy.length.toLocaleString()} total citers. Click any node for details.`
                : `Showing all ${papers.length.toLocaleString()} citing papers. Click any node for details.`;

        await new Promise(resolve => requestAnimationFrame(() => {
            try { this._renderCitationNetwork(this._networkCenter, papers); }
            catch (e) {
                const hint = document.getElementById('network-hint');
                hint.style.display = '';
                hint.textContent = `Render error: ${e.message}`;
                console.error(e);
            }
            resolve();
        }));
    }

    _setupNetworkControls() {
        const ctrl = document.getElementById('network-controls');
        ctrl.style.display = '';

        // Download buttons
        const dlBar = document.getElementById('network-download-bar');
        dlBar.style.display = '';
        document.getElementById('net-dl-png').onclick = () => this._downloadNetwork('png');
        document.getElementById('net-dl-jpg').onclick = () => this._downloadNetwork('jpg');
        document.getElementById('net-dl-pdf').onclick = () => this._downloadNetwork('pdf');

        // Layout buttons
        ctrl.querySelectorAll('.btn-view[data-view]').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.view === this._networkLayoutMode);
            btn.onclick = () => {
                ctrl.querySelectorAll('.btn-view[data-view]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._networkLayoutMode = btn.dataset.view;
                if (btn.dataset.view === 'force') this._applyForceLayout();
                else if (btn.dataset.view === 'concentric') this._applyConcentricLayout();
                else this._applyTimelineLayout();
            };
        });

        // Count selector
        const countSel = document.getElementById('network-count-select');
        countSel.onchange = async () => {
            const hint = document.getElementById('network-hint');
            hint.style.display = '';
            hint.textContent = `Loading ${parseInt(countSel.value).toLocaleString()} citing papers…`;
            const savedLayout = this._networkLayoutMode;
            await this._fetchAndRenderNetwork(parseInt(countSel.value));
            hint.style.display = 'none';
            // Re-apply saved layout (render always resets to force internally)
            this._networkLayoutMode = savedLayout;
            ctrl.querySelectorAll('.btn-view[data-view]').forEach(b =>
                b.classList.toggle('active', b.dataset.view === savedLayout));
            if (savedLayout === 'concentric') this._applyConcentricLayout();
            else if (savedLayout === 'timeline') this._applyTimelineLayout();
        };

        // Color buttons
        ctrl.querySelectorAll('.btn-view[data-color]').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.color === this._networkColorMode);
            btn.onclick = () => {
                ctrl.querySelectorAll('.btn-view[data-color]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._networkColorMode = btn.dataset.color;
                this._applyNodeColors();
                // Toggle RCR legend
                document.getElementById('network-rcr-legend').style.display =
                    btn.dataset.color === 'rcr' ? '' : 'none';
            };
        });
    }

    _nodeColor(paper) {
        if (!paper) return '#90A4AE';
        if (this._networkColorMode === 'rcr') {
            const rcr = paper.relative_citation_ratio;
            if (rcr == null) return '#CFD8DC';
            if (rcr >= 10)  return '#b71c1c';
            if (rcr >= 5)   return '#e53935';
            if (rcr >= 2)   return '#FB8C00';
            if (rcr >= 1)   return '#FDD835';
            if (rcr >= 0.5) return '#66BB6A';
            return '#90A4AE';
        }
        // year mode
        const yr = paper.year;
        if (!yr) return '#90CAF9';
        if (yr >= 2022) return '#1565C0';
        if (yr >= 2018) return '#1976D2';
        if (yr >= 2014) return '#42A5F5';
        if (yr >= 2010) return '#64B5F6';
        return '#90CAF9';
    }

    _applyNodeColors() {
        if (!this._cyNetwork) return;
        this._cyNetwork.nodes().forEach(node => {
            if (node.hasClass('year-label') || node.data('isCenter')) return;
            node.style('background-color', this._nodeColor(node.data('paper')));
        });
    }

    _fmtAuthors(authors) {
        if (!authors) return '';
        if (typeof authors === 'string') return authors;
        if (Array.isArray(authors)) {
            return authors.map(a => {
                if (typeof a === 'string') return a;
                return a.name || a.lastname || a.family || a.collective || '';
            }).filter(Boolean).join(', ');
        }
        return '';
    }

    _renderCitationNetwork(center, citingPapers) {
        const container = document.getElementById('citation-network');
        if (this._cyNetwork) { this._cyNetwork.destroy(); this._cyNetwork = null; }

        const elements = [];
        elements.push({ data: {
            id: String(center.pmid),
            color: '#e74c3c', borderColor: '#c0392b',
            size: 44, isCenter: true, paper: center,
        }});
        citingPapers.forEach(p => {
            const cits = p.citation_count || 0;
            const size = cits === 0 ? 8 : Math.max(8, Math.min(62, 8 + Math.log2(cits + 1) * 6));
            elements.push({ data: {
                id: String(p.pmid),
                color: this._nodeColor(p), borderColor: '#1a5276',
                size, paper: p,
            }});
            elements.push({ data: { source: String(p.pmid), target: String(center.pmid) } });
        });

        this._cyNetwork = cytoscape({
            container, elements,
            style: [
                { selector: 'node', style: {
                    'background-color': 'data(color)', 'border-color': 'data(borderColor)',
                    'border-width': 1.5, 'width': 'data(size)', 'height': 'data(size)', 'label': '',
                }},
                { selector: 'node[?isCenter]', style: { 'border-width': 3 }},
                { selector: 'node.year-label', style: {
                    'background-opacity': 0, 'border-width': 0, 'label': 'data(label)',
                    'color': '#555', 'font-size': 13, 'font-weight': 'bold',
                    'text-valign': 'center', 'text-halign': 'center',
                    'width': 55, 'height': 22, 'events': 'no',
                }},
                { selector: 'node.axis-anchor', style: {
                    'background-opacity': 0, 'border-width': 0, 'label': '',
                    'width': 1, 'height': 1, 'events': 'no',
                }},
                { selector: 'edge.axis-edge', style: {
                    'width': 1.5, 'line-color': '#aaa', 'line-style': 'solid',
                    'target-arrow-shape': 'none', 'source-arrow-shape': 'none',
                    'opacity': 0.7,
                }},
                { selector: 'edge', style: {
                    'width': 0.6, 'line-color': '#ccc', 'opacity': 0.4,
                    'target-arrow-color': '#aaa', 'target-arrow-shape': 'triangle',
                    'curve-style': 'straight',
                }},
                { selector: 'node:selected', style: { 'border-color': '#f39c12', 'border-width': 3 }},
            ],
            layout: {
                name: 'cose', animate: false,
                nodeRepulsion: () => 8000, nodeOverlap: 20,
                idealEdgeLength: () => 80, edgeElasticity: () => 100,
                nestingFactor: 5, gravity: 0.25, numIter: 1000,
                initialTemp: 200, coolingFactor: 0.95, minTemp: 1.0,
            },
        });

        this._cyNetwork.fit(undefined, 40);
        this._cyNetwork.resize();

        this._cyNetwork.on('tap', 'node', (evt) => {
            const paper = evt.target.data('paper');
            if (paper) this._showNetworkSelected(paper);
        });
    }

    _applyForceLayout() {
        if (!this._cyNetwork) return;
        this._cyNetwork.remove('node.year-label, node.axis-anchor, edge.axis-edge');
        this._cyNetwork.edges().style('display', 'element');
        this._cyNetwork.layout({
            name: 'cose', animate: false,
            nodeRepulsion: () => 8000, nodeOverlap: 20,
            idealEdgeLength: () => 80, edgeElasticity: () => 100,
            nestingFactor: 5, gravity: 0.25, numIter: 1000,
            initialTemp: 200, coolingFactor: 0.95, minTemp: 1.0,
        }).run();
        this._cyNetwork.fit(undefined, 40);
    }

    _applyConcentricLayout() {
        if (!this._cyNetwork) return;
        this._cyNetwork.remove('node.year-label, node.axis-anchor, edge.axis-edge');
        this._cyNetwork.edges().style('display', 'element');
        let maxCit = 1;
        this._cyNetwork.nodes().forEach(n => {
            if (!n.data('isCenter')) maxCit = Math.max(maxCit, n.data('paper')?.citation_count || 0);
        });
        this._cyNetwork.layout({
            name: 'concentric', animate: true, animationDuration: 500,
            concentric: node => {
                if (node.data('isCenter')) return 100;
                // More-cited papers closer to center
                return Math.ceil(((node.data('paper')?.citation_count || 0) / maxCit) * 9) + 1;
            },
            levelWidth: () => 1,
            minNodeSpacing: 4, spacingFactor: 1.15, equidistant: true,
            padding: 30, startAngle: Math.PI / 2, clockwise: true,
        }).run();
        setTimeout(() => this._cyNetwork && this._cyNetwork.fit(undefined, 40), 550);
    }

    _applyTimelineLayout() {
        if (!this._cyNetwork) return;
        this._cyNetwork.remove('node.year-label, node.axis-anchor, edge.axis-edge');

        const byYear = {};
        this._cyNetwork.nodes().forEach(node => {
            const yr = node.data('paper')?.year;
            if (!yr) return;
            const key = String(yr);
            if (!byYear[key]) byYear[key] = [];
            byYear[key].push(node);
        });

        const sortedYears = Object.keys(byYear).sort();
        const xStep = 130, yStep = 50;
        const maxNodes = Math.max(...sortedYears.map(y => byYear[y].length), 1);

        const positions = {};
        sortedYears.forEach((yr, xi) => {
            byYear[yr].sort((a, b) =>
                (b.data('paper')?.citation_count || 0) - (a.data('paper')?.citation_count || 0));
            byYear[yr].forEach((node, yi) => {
                positions[node.id()] = { x: xi * xStep, y: yi * yStep };
            });
        });

        const axisY = maxNodes * yStep + 30;
        // Year label nodes
        this._cyNetwork.add(sortedYears.map((yr, xi) => ({
            data: { id: `yl-${yr}`, label: yr }, classes: 'year-label',
            position: { x: xi * xStep, y: axisY },
        })));
        // Axis tick nodes (invisible, just anchors for the axis line)
        if (sortedYears.length > 1) {
            this._cyNetwork.add([
                { data: { id: 'axis-start' }, classes: 'axis-anchor',
                  position: { x: 0, y: axisY - 18 } },
                { data: { id: 'axis-end' }, classes: 'axis-anchor',
                  position: { x: (sortedYears.length - 1) * xStep, y: axisY - 18 } },
            ]);
            this._cyNetwork.add([{
                data: { id: 'axis-line', source: 'axis-start', target: 'axis-end' },
                classes: 'axis-edge',
            }]);
        }
        this._cyNetwork.edges().not('.axis-edge').style('display', 'none');

        this._cyNetwork.layout({
            name: 'preset', positions: node => positions[node.id()],
            animate: true, animationDuration: 500, fit: false,
        }).run();
        setTimeout(() => this._cyNetwork && this._cyNetwork.fit(undefined, 50), 550);
    }

    _showNetworkSelected(paper) {
        document.getElementById('network-sel-title').textContent = paper.title || 'Unknown title';
        const authors = this._fmtAuthors(paper.authors);
        const rcr = paper.relative_citation_ratio;
        const rcrStr = rcr != null ? ` · RCR ${rcr.toFixed(2)}` : '';
        document.getElementById('network-sel-meta').textContent =
            `${authors} · ${paper.journal || ''} · ${paper.year || ''} · ${(paper.citation_count || 0).toLocaleString()} citations${rcrStr}`;
        document.getElementById('network-sel-link').href = `https://pubmed.ncbi.nlm.nih.gov/${paper.pmid}/`;
        document.getElementById('network-selected-panel').style.display = '';
    }

    _downloadNetwork(format) {
        if (!this._cyNetwork) return;
        const pmid = this._networkCenter?.pmid || 'export';
        const filename = `citation-network-pmid${pmid}`;
        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const imgData = this._cyNetwork.png({ full: true, scale: 2, bg: 'white' });
            const img = new Image();
            img.onload = () => {
                const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
                const pw = doc.internal.pageSize.getWidth();
                const ph = doc.internal.pageSize.getHeight();
                const ratio = img.naturalWidth / img.naturalHeight;
                const imgW = pw - 20;
                const imgH = Math.min(imgW / ratio, ph - 28);
                doc.setFontSize(11);
                doc.text(`IMPACT — Citation Network (PMID ${pmid})`, 10, 10);
                doc.addImage(imgData, 'PNG', 10, 16, imgW, imgH);
                doc.save(`${filename}.pdf`);
            };
            img.src = imgData;
            return;
        }
        const imgData = format === 'jpg'
            ? this._cyNetwork.jpg({ full: true, scale: 2, bg: 'white' })
            : this._cyNetwork.png({ full: true, scale: 2, bg: 'white' });
        const a = document.createElement('a');
        a.href = imgData;
        a.download = `${filename}.${format}`;
        a.click();
    }

    async _fetchICiteBatch(pmids, batchSize = 100) {
        const results = [];
        for (let i = 0; i < pmids.length; i += batchSize) {
            const batch = pmids.slice(i, i + batchSize);
            try {
                const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${batch.join(',')}`);
                if (!resp.ok) continue;
                const json = await resp.json();
                results.push(...(Array.isArray(json) ? json : (json.data || [])));
            } catch (e) { console.error('iCite batch error:', e); }
        }
        return results;
    }

    _computeHIndex(citations) {
        const sorted = [...citations].sort((a, b) => b - a);
        let h = 0;
        for (let i = 0; i < sorted.length; i++) {
            if (sorted[i] >= i + 1) h = i + 1; else break;
        }
        return h;
    }

    // ---- Influence Analysis ----

    setupInfluence() {
        this._influencePicker = new SingleJournalPicker('influence-journal-picker', this.journals, (slug) => {
            this._influenceJournalSlug = slug;
        });
        document.getElementById('influence-analyze-btn').addEventListener('click', () => this.loadInfluenceData());
        document.getElementById('influence-pmid-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.loadInfluenceData();
        });
        document.getElementById('influence-censor-toggle').addEventListener('change', (e) => {
            this._toggleCensoredLine(e.target.checked);
        });
        document.getElementById('inf-dl-png').onclick = () => this._downloadInfluence('png');
        document.getElementById('inf-dl-jpg').onclick = () => this._downloadInfluence('jpg');
        document.getElementById('inf-dl-pdf').onclick = () => this._downloadInfluence('pdf');
    }

    async loadInfluenceData() {
        const raw = document.getElementById('influence-pmid-input').value;
        const pmids = [...new Set(raw.split(',').map(s => s.trim().replace(/\D/g, '')).filter(Boolean))];
        const slug = this._influenceJournalSlug;
        const hint = document.getElementById('influence-hint');
        const results = document.getElementById('influence-results');

        if (!pmids.length || !slug) {
            hint.textContent = 'Please select a journal and enter at least one PMID.';
            hint.style.display = '';
            return;
        }

        hint.textContent = `Fetching journal and ${pmids.length} PMID(s)…`;
        hint.style.display = '';
        results.style.display = 'none';

        try {
            const [journalData, seedPapers] = await Promise.all([
                this.journalDataCache[slug]
                    ? Promise.resolve(this.journalDataCache[slug])
                    : dataLoader.loadJournal(slug).then(d => { this.journalDataCache[slug] = d; return d; }),
                this._fetchICiteBatch(pmids),
            ]);

            if (!seedPapers.length) { hint.textContent = 'None of the PMIDs were found in iCite.'; return; }

            // No need to fetch citing papers individually — iCite returns citations_per_year
            // directly on each seed paper, giving us the full year-by-year citation distribution
            // for ALL citations with no 2k cap.
            this._renderInfluenceChart(journalData, seedPapers);

            hint.style.display = 'none';
            results.style.display = '';
        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('Influence error:', e);
        }
    }

    _renderInfluenceChart(journalData, seedPapers) {
        // Paper info card — list each seed paper
        const listEl = document.getElementById('inf-paper-list');
        listEl.innerHTML = seedPapers.map(p => `
            <div class="inf-paper-row">
                <div class="inf-paper-title">${p.title || 'Unknown title'}</div>
                <div class="inf-paper-meta">${this._fmtAuthors(p.authors)} · ${p.journal || ''} · ${p.year || ''} · ${(p.citation_count || 0).toLocaleString()} citations ·
                    <a href="https://pubmed.ncbi.nlm.nih.gov/${p.pmid}/" class="pubmed-link" target="_blank" rel="noopener">PMID ${p.pmid}</a>
                </div>
            </div>`).join('<hr class="inf-paper-divider">');

        // Use 24-month timeseries
        const ts = journalData.timeseries || [];
        if (!ts.length) return;

        // Aggregate citations_per_year across all seed papers.
        // iCite returns this field directly — no need to fetch citing papers individually,
        // so there is no 2k cap and all citations are included.
        const citsByYear = {};
        for (const p of seedPapers) {
            for (const [yr, cnt] of Object.entries(p.citations_per_year || {})) {
                citsByYear[yr] = (citsByYear[yr] || 0) + cnt;
            }
        }

        // For each timeseries snapshot month, estimate how many seed-paper citations
        // fall within its 12-month citation window using proportional year allocation.
        const adjIf = ts.map(point => {
            const [y, m] = point.month.split('-').map(Number);
            const endOrd = y * 12 + m;
            const startOrd = endOrd - 11;
            let seedCitsInWindow = 0;
            for (const [yr, cnt] of Object.entries(citsByYear)) {
                const k = parseInt(yr);
                // Ordinal month range for year k: [k*12+1, k*12+12]
                const overlapMonths = Math.max(0,
                    Math.min(endOrd, k * 12 + 12) - Math.max(startOrd, k * 12 + 1) + 1);
                seedCitsInWindow += cnt * overlapMonths / 12;
            }
            const papers = point.papers || 1;
            const citations = point.citations || 0;
            return Math.max(0, citations - seedCitsInWindow) / papers;
        });

        // Metric cards
        const contributions = ts.map((pt, i) => Math.max(0, (pt.rolling_if || 0) - adjIf[i]));
        const maxContrib = Math.max(...contributions);
        const peakIdx = contributions.indexOf(maxContrib);
        const peakMonth = ts[peakIdx]?.month || '—';
        const meanContrib = contributions.reduce((s, v) => s + v, 0) / contributions.length;
        const totalCitations = seedPapers.reduce((s, p) => s + (p.citation_count || 0), 0);
        const trackedCits = Object.values(citsByYear).reduce((s, v) => s + v, 0);
        const censorLabel = seedPapers.length === 1
            ? `PMID ${seedPapers[0].pmid}`
            : `${seedPapers.length} PMIDs`;

        document.getElementById('influence-metrics').innerHTML = [
            [totalCitations.toLocaleString(), seedPapers.length === 1 ? 'Total Citations' : 'Combined Citations'],
            [trackedCits.toLocaleString(), 'Citations with Year Data (all included)'],
            [maxContrib.toFixed(3), `Peak IF Boost (${peakMonth})`],
            [meanContrib.toFixed(3), 'Mean Monthly IF Contribution'],
        ].map(([v, l]) =>
            `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
        ).join('');

        // Chart
        const labels = ts.map(d => d.month);
        chartManager._destroy('influence-chart');
        const ctx = document.getElementById('influence-chart');
        const isCensored = document.getElementById('influence-censor-toggle').checked;
        chartManager.charts['influence-chart'] = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Original IF (with PMIDs)',
                        data: ts.map(d => d.rolling_if),
                        borderColor: chartManager.palette[0],
                        backgroundColor: 'rgba(0, 114, 178, 0.08)',
                        borderWidth: 2.5,
                        tension: 0.3,
                        fill: true,
                        pointRadius: 0,
                        pointHoverRadius: 5,
                    },
                    {
                        label: `Censored IF (without ${censorLabel})`,
                        data: adjIf,
                        borderColor: chartManager.palette[1],
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        borderDash: [6, 3],
                        tension: 0.3,
                        fill: false,
                        pointRadius: 0,
                        pointHoverRadius: 5,
                        hidden: !isCensored,
                    },
                ],
            },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: {
                        display: true,
                        text: `${journalData.journal} — Rolling 24-Month Citation Rate`,
                        font: { size: 14 },
                    },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(3)}`,
                            afterBody: (items) => {
                                const orig = items.find(i => i.datasetIndex === 0);
                                const cens = items.find(i => i.datasetIndex === 1);
                                if (orig && cens) {
                                    const diff = orig.parsed.y - cens.parsed.y;
                                    return `Contribution: +${diff.toFixed(3)}`;
                                }
                                return '';
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                    },
                    y: {
                        title: { display: true, text: 'Citation Rate (24-mo)' },
                        beginAtZero: false,
                    },
                },
            },
        });
    }

    _toggleCensoredLine(show) {
        const chart = chartManager.charts['influence-chart'];
        if (!chart) return;
        chart.data.datasets[1].hidden = !show;
        chart.update();
    }

    _downloadInfluence(format) {
        const chart = chartManager.charts['influence-chart'];
        if (!chart) return;
        const slug = this._influenceJournalSlug || 'journal';
        const filename = `influence-${slug}`;
        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            doc.setFontSize(11);
            doc.text('IMPACT — PMID Influence Analysis', 10, 10);
            doc.addImage(chart.toBase64Image('image/png', 1), 'PNG', 10, 16, imgW, imgH);
            doc.save(`${filename}.pdf`);
            return;
        }
        const mime = format === 'jpg' ? 'image/jpeg' : 'image/png';
        const url = chart.toBase64Image(mime, 1);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${filename}.${format}`;
        a.click();
    }

    // ---- Author Name Search ----

    setupAuthorSearch() {
        document.getElementById('author-name-search-btn').addEventListener('click', () => this.searchAuthorByName());
        document.getElementById('author-name-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.searchAuthorByName();
        });
        document.getElementById('author-ncbi-btn').addEventListener('click', () => this.loadFromNCBIUrl());
        document.getElementById('author-ncbi-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.loadFromNCBIUrl();
        });
    }

    async loadFromNCBIUrl() {
        const val = document.getElementById('author-ncbi-input').value.trim();
        if (!val) return;

        const hint = document.getElementById('author-search-hint');
        const results = document.getElementById('author-search-results');
        hint.style.display = '';
        results.style.display = 'none';

        try {
            let pmids = [];

            hint.textContent = 'Fetching NCBI bibliography…';

            // Race all proxies in parallel — whichever responds first with valid HTML wins.
            // allorigins.win/get returns JSON {contents, status}, not raw HTML.
            const enc = encodeURIComponent(val);
            const fetchProxy = async (url, json) => {
                const resp = await fetch(url, { signal: AbortSignal.timeout(15000) });
                if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
                const body = json ? (await resp.json()).contents : await resp.text();
                if (!body || !body.includes('pubmed')) throw new Error('no pubmed links');
                return body;
            };
            const html = await Promise.any([
                fetchProxy(`https://corsproxy.io/?url=${enc}`, false),
                fetchProxy(`https://api.allorigins.win/get?url=${enc}`, true),
                fetchProxy(`https://api.codetabs.com/v1/proxy?quest=${enc}`, false),
            ]).catch(() => { throw new Error('All CORS proxies failed — try again in a moment.'); });
            const matches = [...html.matchAll(/\/pubmed\/(\d+)/g)];
            pmids = [...new Set(matches.map(m => m[1]))];

            if (!pmids.length) {
                hint.textContent = 'No PMIDs found on that page. Make sure the bibliography is set to public.';
                return;
            }

            hint.textContent = `Found ${pmids.length} papers. Fetching citation data…`;
            const papers = await this._fetchICiteBatch(pmids);

            if (!papers.length) {
                hint.textContent = 'Papers found but no iCite data available yet.';
                return;
            }

            this._authorTotalFound = pmids.length;
            hint.style.display = 'none';
            results.style.display = '';
            this._renderAuthorSearchResults(papers, pmids.length);

        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('NCBI bibliography load error:', e);
        }
    }

    async searchAuthorByName() {
        const input = document.getElementById('author-name-input').value.trim();
        if (!input) return;

        const names = input.split(',').map(n => n.trim()).filter(Boolean);

        const hint = document.getElementById('author-search-hint');
        const results = document.getElementById('author-search-results');
        hint.style.display = '';
        results.style.display = 'none';

        try {
            const allPmids = new Set();
            let totalFound = 0;

            for (const name of names) {
                hint.textContent = names.length > 1
                    ? `Searching PubMed for "${name}" (${names.indexOf(name) + 1}/${names.length})…`
                    : 'Searching PubMed…';
                const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(name)}[Author]&retmax=500&retmode=json&tool=IMPACT&email=impact-tool@umich.edu`;
                const resp = await fetch(url);
                if (!resp.ok) throw new Error('PubMed search failed');
                const data = await resp.json();
                (data.esearchresult?.idlist || []).forEach(id => allPmids.add(id));
                totalFound += parseInt(data.esearchresult?.count || 0);
            }

            if (!allPmids.size) {
                hint.textContent = 'No papers found. Try "Lastname AB" format (e.g. "Smith J").';
                return;
            }

            const label = names.length > 1 ? names.join(' + ') : names[0];
            hint.textContent = `Found ${allPmids.size.toLocaleString()} unique papers for ${label}. Fetching citation data…`;
            const papers = await this._fetchICiteBatch([...allPmids]);

            if (!papers.length) {
                hint.textContent = 'Papers found on PubMed but no citation data available yet.';
                return;
            }

            this._authorTotalFound = totalFound;
            hint.style.display = 'none';
            results.style.display = '';
            this._renderAuthorSearchResults(papers, allPmids.size);

        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('Author search error:', e);
        }
    }

    _renderAuthorSearchResults(papers, totalFound) {
        this._authorAllPapers = papers;
        this._authorExcluded = new Set();
        this._authorTotalFound = totalFound;
        this._renderAuthorPapersTable();
        this._refreshAuthorMetrics();
    }

    _refreshAuthorMetrics() {
        const active = this._authorAllPapers.filter(p => !this._authorExcluded.has(String(p.pmid)));
        const excluded = this._authorAllPapers.length - active.length;
        const totalCitations = active.reduce((s, p) => s + (p.citation_count || 0), 0);
        const hIndex = this._computeHIndex(active.map(p => p.citation_count || 0));

        const excTag = excluded ? ` <span style="font-size:.7em;color:#c0392b;font-weight:normal">(−${excluded} excluded)</span>` : '';
        document.getElementById('author-search-metrics').innerHTML = [
            [`${active.length.toLocaleString()}${excTag}`, 'Papers Included'],
            [this._authorTotalFound > this._authorAllPapers.length
                ? `${this._authorTotalFound.toLocaleString()} total` : this._authorTotalFound.toLocaleString(), 'Papers on PubMed'],
            [totalCitations.toLocaleString(), 'Total Citations'],
            [hIndex, 'h-index (est.)'],
        ].map(([v, l]) => {
            const isHIndex = l === 'h-index (est.)';
            const label = isHIndex
                ? `${l} <span class="metric-info" title="Based only on PubMed-indexed citing articles. Google Scholar casts a wider net (preprints, books, non-indexed journals), so its h-index is typically higher.">ⓘ</span>`
                : l;
            return `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${label}</span></div>`;
        }).join('');

        const pubsByYear = {};
        active.forEach(p => { if (p.year) pubsByYear[p.year] = (pubsByYear[p.year] || 0) + 1; });
        const sortedYears = Object.keys(pubsByYear).sort();
        chartManager.createBarChart('author-pubs-chart', sortedYears,
            sortedYears.map(y => pubsByYear[y]), 'Papers', { horizontal: false });

        const citsByYear = {};
        active.forEach(p => { if (p.year) citsByYear[p.year] = (citsByYear[p.year] || 0) + (p.citation_count || 0); });
        chartManager.createBarChart('author-cits-chart', sortedYears,
            sortedYears.map(y => citsByYear[y] || 0), 'Citations', { horizontal: false });

        const jCounts = {};
        active.forEach(p => { if (p.journal) jCounts[p.journal] = (jCounts[p.journal] || 0) + 1; });
        const topJ = Object.entries(jCounts).sort((a, b) => b[1] - a[1]).slice(0, 15);
        chartManager.createBarChart('author-journals-chart',
            topJ.map(x => x[0]), topJ.map(x => x[1]), 'Papers');
    }

    _renderAuthorPapersTable() {
        this._authorSort = { col: 'citations', dir: 'desc' };

        const container = document.getElementById('author-papers-list');
        container.innerHTML =
            `<h4 style="margin-bottom:.25rem">All Papers (${this._authorAllPapers.length})</h4>` +
            `<p class="data-note" style="margin-bottom:.5rem">Click column headers to sort. Uncheck papers to exclude from metrics.</p>` +
            `<div class="table-scroll"><table class="data-table"><thead><tr>` +
            `<th style="width:2rem"></th>` +
            `<th class="sort-header" data-col="title">Title</th>` +
            `<th class="sort-header" data-col="journal">Journal</th>` +
            `<th class="sort-header" data-col="year">Year</th>` +
            `<th class="sort-header" data-col="citations">Citations ↓</th>` +
            `</tr></thead><tbody id="author-papers-tbody"></tbody></table></div>`;

        container.querySelectorAll('.sort-header').forEach(th => {
            th.addEventListener('click', () => this._sortAuthorTable(th.dataset.col));
        });

        // Delegated listeners on the tbody parent (stable across tbody replacements)
        const tableWrap = container.querySelector('.table-scroll');
        tableWrap.addEventListener('change', (e) => {
            if (!e.target.matches('.paper-cb')) return;
            const pmid = String(e.target.dataset.pmid);
            if (e.target.checked) this._authorExcluded.delete(pmid);
            else this._authorExcluded.add(pmid);
            this._refreshAuthorMetrics();
        });
        tableWrap.addEventListener('click', (e) => {
            if (e.target.matches('.paper-cb')) return;
            const tr = e.target.closest('tr[data-pmid]');
            if (tr) window.open(`https://pubmed.ncbi.nlm.nih.gov/${tr.dataset.pmid}/`, '_blank');
        });

        this._renderAuthorTableBody();
    }

    _sortAuthorTable(col) {
        if (this._authorSort.col === col) {
            this._authorSort.dir = this._authorSort.dir === 'asc' ? 'desc' : 'asc';
        } else {
            this._authorSort.col = col;
            this._authorSort.dir = (col === 'title' || col === 'journal') ? 'asc' : 'desc';
        }
        this._renderAuthorTableBody();
    }

    _renderAuthorTableBody() {
        const tbody = document.getElementById('author-papers-tbody');
        if (!tbody) return;

        const { col, dir } = this._authorSort;
        const sorted = [...this._authorAllPapers].sort((a, b) => {
            let va, vb;
            if      (col === 'year')     { va = a.year || 0;              vb = b.year || 0; }
            else if (col === 'citations'){ va = a.citation_count || 0;    vb = b.citation_count || 0; }
            else if (col === 'journal')  { va = (a.journal||'').toLowerCase(); vb = (b.journal||'').toLowerCase(); }
            else                         { va = (a.title||'').toLowerCase();   vb = (b.title||'').toLowerCase(); }
            return dir === 'asc' ? (va < vb ? -1 : va > vb ? 1 : 0)
                                 : (va > vb ? -1 : va < vb ? 1 : 0);
        });

        const esc = s => String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        tbody.innerHTML = sorted.map(p => {
            const excluded = this._authorExcluded.has(String(p.pmid));
            const title = p.title ? (p.title.length > 90 ? p.title.slice(0, 90) + '…' : p.title) : '—';
            return `<tr data-pmid="${p.pmid}"${excluded ? ' class="row-excluded"' : ''}>` +
                `<td class="cb-cell"><input type="checkbox" class="paper-cb" data-pmid="${p.pmid}"${excluded ? '' : ' checked'}></td>` +
                `<td class="papers-row-link" title="Open in PubMed">${esc(title)}</td>` +
                `<td>${esc(p.journal || '—')}</td>` +
                `<td>${p.year || '—'}</td>` +
                `<td>${(p.citation_count || 0).toLocaleString()}</td></tr>`;
        }).join('');

        // Update sort indicators on headers
        document.querySelectorAll('.sort-header').forEach(th => {
            const labels = { title: 'Title', journal: 'Journal', year: 'Year', citations: 'Citations' };
            const arrow = th.dataset.col === col ? (dir === 'asc' ? ' ↑' : ' ↓') : '';
            th.textContent = labels[th.dataset.col] + arrow;
        });
    }

    // ---- Geography ----

    // US state abbreviations to normalize to "USA"
    static get US_STATES() {
        return new Set([
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
            'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
            'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
            'TX','UT','VT','VA','WA','WV','WI','WY','DC',
        ]);
    }

    _normalizeCountry(c) {
        if (!c) return null;
        const t = c.trim();
        if (IMPACTApp.US_STATES.has(t.toUpperCase())) return 'USA';
        // Strip trailing email artifacts
        const clean = t.replace(/\.\s*(electronic address|email):?.*/i, '').trim();
        const lo = clean.toLowerCase();
        if (lo === 'usa' || lo === 'us' || lo === 'united states' || lo === 'united states of america') return 'USA';
        if (lo === 'uk' || lo === 'england' || lo === 'scotland' || lo === 'wales' || lo === 'great britain') return 'United Kingdom';
        if (/^(people.?s republic of china|pr china|p\.?\s*r\.?\s*china)$/i.test(clean)) return 'China';
        if (lo === 'republic of korea' || lo === 'south korea') return 'South Korea';
        if (lo === 'democratic people\'s republic of korea' || lo === 'north korea') return 'North Korea';
        if (lo === 'islamic republic of iran' || lo === 'iran, islamic republic') return 'Iran';
        if (lo === 'russian federation' || lo === 'russia') return 'Russia';
        if (lo === 'czech republic' || lo === 'czechia') return 'Czech Republic';
        if (lo === 'the netherlands' || lo === 'netherlands') return 'Netherlands';
        if (lo === 'taiwan' || lo === 'republic of china') return 'Taiwan';
        return clean || null;
    }

    setupGeography() {
        this.geoPicker = new SingleJournalPicker(
            'geo-journal-picker', this.journals,
            (slug) => {
                if (slug) {
                    this.loadGeographyData(slug);
                } else {
                    document.getElementById('geo-content').style.display = 'none';
                    document.getElementById('geo-hint').textContent = 'Select a journal above to see its geographic breakdown.';
                    document.getElementById('geo-hint').style.display = '';
                }
            }
        );
    }

    async loadGeographyData(slug) {
        const hint = document.getElementById('geo-hint');
        const content = document.getElementById('geo-content');
        hint.textContent = 'Loading…';
        hint.style.display = '';
        content.style.display = 'none';

        try {
            if (!this.papersDataCache[slug]) {
                const resp = await fetch(`data/papers/${slug}.json`);
                if (!resp.ok) throw new Error('No data');
                const d = await resp.json();
                this.papersDataCache[slug] = d.papers || [];
                this.papersDataCache[`${slug}__geo`] = d.geo || null;
            }

            const geo = this.papersDataCache[`${slug}__geo`];
            if (!geo || Object.keys(geo).length === 0) {
                hint.textContent = 'Geographic data not yet available for this journal.';
                hint.style.display = '';
                return;
            }

            // Normalize countries (state abbrevs → USA)
            const normalizedGeo = {};
            Object.entries(geo).forEach(([yr, countries]) => {
                normalizedGeo[yr] = {};
                Object.entries(countries).forEach(([c, n]) => {
                    const norm = this._normalizeCountry(c);
                    if (norm) normalizedGeo[yr][norm] = (normalizedGeo[yr][norm] || 0) + n;
                });
            });

            const years = Object.keys(normalizedGeo).sort();

            // Overall totals per country
            const totals = {};
            Object.values(normalizedGeo).forEach(byCountry => {
                Object.entries(byCountry).forEach(([c, n]) => {
                    totals[c] = (totals[c] || 0) + n;
                });
            });

            const top10 = Object.entries(totals).sort((a, b) => b[1] - a[1]).slice(0, 10).map(x => x[0]);

            // Metrics
            const totalPapers = Object.values(totals).reduce((a, b) => a + b, 0);
            const nCountries = Object.keys(totals).length;
            const recentYears = years.slice(-3);
            const recentTotals = {};
            recentYears.forEach(yr => {
                Object.entries(normalizedGeo[yr] || {}).forEach(([c, n]) => {
                    recentTotals[c] = (recentTotals[c] || 0) + n;
                });
            });
            const topRecent = Object.entries(recentTotals).sort((a, b) => b[1] - a[1]).slice(0, 12);

            document.getElementById('geo-metrics-row').innerHTML = [
                [totalPapers.toLocaleString(), 'Papers with Location Data'],
                [nCountries, 'Countries'],
                [top10[0] || '—', 'Top Country Overall'],
                [topRecent[0] ? topRecent[0][0] : '—', `Top Country (${recentYears[0]}–${recentYears[recentYears.length-1]})`],
            ].map(([val, label]) =>
                `<div class="metric-card"><span class="metric-value">${val}</span><span class="metric-label">${label}</span></div>`
            ).join('');

            hint.style.display = 'none';
            content.style.display = '';

            // Trend chart: stacked bars by year, top 10 countries
            const trendDatasets = top10.map((country, i) => ({
                label: country,
                data: years.map(yr => normalizedGeo[yr][country] || 0),
                backgroundColor: chartManager.palette[i % chartManager.palette.length] + 'cc',
                borderColor: chartManager.palette[i % chartManager.palette.length],
                borderWidth: 1,
            }));
            chartManager.createStackedBarChart('geo-trend-chart', years, trendDatasets, 'Papers');

            // Top countries overall (horizontal bar)
            const topOverall = Object.entries(totals).sort((a, b) => b[1] - a[1]).slice(0, 15);
            chartManager.createBarChart('geo-top-chart',
                topOverall.map(x => x[0]), topOverall.map(x => x[1]), 'Papers');

            // Recent years (horizontal bar)
            chartManager.createBarChart('geo-recent-chart',
                topRecent.map(x => x[0]), topRecent.map(x => x[1]), 'Papers');

        } catch (e) {
            hint.textContent = 'Geographic data not yet available for this journal.';
            hint.style.display = '';
            console.error('Geo error:', e);
        }
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
            'autophagy': 'autophagy',
            'redox biol': 'redox-biology',
            'redox biology': 'redox-biology',
            'sci immunol': 'science-immunology',
            'science immunology': 'science-immunology',
            'science': 'science',
            'nature': 'nature',
            'nat commun': 'nature-communications',
            'nature communications': 'nature-communications',
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

        // Collect all PMIDs that need PubMed date lookups (target papers + all their citing papers)
        const allDatePmids = new Set(pmids.map(String));
        for (const pmid of pmids) {
            const p = iciteMap[String(pmid)];
            if (p) for (const c of (p.cited_by || [])) allDatePmids.add(String(c));
        }

        // Step 2: fetch exact publication dates from PubMed ESummary
        tableContainer.innerHTML = `<p class="loading-text">Fetching publication dates from PubMed (${allDatePmids.size} papers)…</p>`;
        const pubmedDates = await this._fetchPubMedDates([...allDatePmids]);

        // Step 3: resolve author details — DB-cached first, live EFetch fallback
        tableContainer.innerHTML = `<p class="loading-text">Fetching author details…</p>`;
        const authorDetails = await this._resolveAuthors(pmids, iciteMap);

        // Step 4: for each PMID, compute exact 24-month citations
        const paperResults = [];

        for (const pmid of pmids) {
            const paper = iciteMap[String(pmid)];
            if (!paper) {
                paperResults.push({ pmid, found: false });
                continue;
            }

            const mainDate = pubmedDates[String(pmid)];
            const mainPubYear = mainDate ? mainDate.year : paper.year;
            const mainPubMonth = mainDate ? mainDate.month : null;
            let cit24mo = 0;
            const approx = false;

            if (mainPubYear && mainPubMonth) {
                // Exact 24-month window: pub month through pub month + 23 months (inclusive)
                const startTotMo = mainPubYear * 12 + (mainPubMonth - 1);
                const endTotMo = startTotMo + 23;
                for (const citPmid of (paper.cited_by || [])) {
                    const d = pubmedDates[String(citPmid)];
                    if (!d) continue;
                    if (d.month !== null) {
                        const citTotMo = d.year * 12 + (d.month - 1);
                        if (citTotMo >= startTotMo && citTotMo <= endTotMo) cit24mo++;
                    } else {
                        // No month: include only if year is strictly interior to the window years
                        const winStartYear = Math.floor(startTotMo / 12);
                        const winEndYear = Math.floor(endTotMo / 12);
                        if (d.year > winStartYear && d.year < winEndYear) cit24mo++;
                    }
                }
            } else if (mainPubYear) {
                // PubMed month unavailable — fall back to year-level approximation
                cit24mo = (paper.citedByPmidsByYear || []).filter(obj => {
                    const yr = Object.values(obj)[0];
                    return yr >= mainPubYear && yr <= mainPubYear + 2;
                }).length;
            }

            // Get time-matched journal benchmark (journal rate ~24 months after publication)
            const journalMatch = this._matchJournal(paper.journal);
            let journalRate = null;
            let journalRateMonth = null;
            if (journalMatch && mainPubYear) {
                const targetMonth = `${mainPubYear + 2}-01`;
                const rateInfo = await this._getJournalRateForPeriod(journalMatch.slug, targetMonth);
                if (rateInfo) {
                    journalRate = rateInfo.rate;
                    journalRateMonth = rateInfo.month;
                } else {
                    journalRate = journalMatch.latest_if;
                }
            }

            const authors = authorDetails[String(pmid)] || {};
            paperResults.push({
                pmid,
                found: true,
                title: paper.title || '',
                journal: paper.journal || '—',
                year: mainPubYear || '—',
                total_citations: paper.citation_count || 0,
                citations_24mo: cit24mo,
                approx,
                journal_name: journalMatch ? journalMatch.name : null,
                journal_rate: journalRate,
                journal_rate_month: journalRateMonth,
                first_author: authors.first || null,
                last_author: authors.last || null,
            });
        }

        // Summary metrics
        const found = paperResults.filter(p => p.found);
        const totalCites = found.reduce((s, p) => s + p.total_citations, 0);
        const avgCitPerYr = found.length > 0
            ? found.reduce((s, p) => s + p.citations_24mo, 0) / found.length / 2
            : 0;

        document.getElementById('author-papers').textContent = `${found.length} / ${pmids.length}`;
        document.getElementById('author-cites').textContent = UIHelpers.formatInt(totalCites);
        document.getElementById('author-avg').textContent = found.length > 0 ? UIHelpers.formatIF(totalCites / found.length) : '—';
        document.getElementById('author-weighted-if').textContent = found.length > 0 ? UIHelpers.formatIF(avgCitPerYr) : '—';

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

    async _fetchPubMedDates(pmids) {
        // Fetch exact publication dates from PubMed ESummary.
        // Returns {pmid_str: {year, month}} where month is 1-12 or null.
        const out = {};
        if (!pmids || pmids.length === 0) return out;

        const MONTH_MAP = { Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12 };
        const parseDate = s => {
            if (!s) return null;
            const parts = s.trim().split(/\s+/);
            const year = parseInt(parts[0]);
            if (!year || year < 1900) return null;
            let month = null;
            if (parts.length >= 2) {
                const mp = parts[1].split('-')[0];
                month = MONTH_MAP[mp] ?? (parseInt(mp) || null);
            }
            return { year, month };
        };
        // Prefer pubdate (print); use epubdate only if pubdate lacks a month
        const bestDate = (pd, epd) => {
            if (pd && pd.month) return pd;
            if (epd && epd.month) return epd;
            return pd || epd;
        };

        const batchSize = 200;
        for (let i = 0; i < pmids.length; i += batchSize) {
            const batch = pmids.slice(i, i + batchSize);
            try {
                const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${batch.join(',')}&retmode=json&tool=IMPACT&email=impact-tool@umich.edu`;
                const resp = await fetch(url);
                if (!resp.ok) continue;
                const data = await resp.json();
                const result = data.result || {};
                for (const pid of (result.uids || [])) {
                    const art = result[pid];
                    if (!art || art.error) continue;
                    const date = bestDate(parseDate(art.pubdate), parseDate(art.epubdate));
                    if (date) out[String(pid)] = date;
                }
            } catch (e) {
                console.error('PubMed ESummary fetch error:', e);
            }
        }
        return out;
    }

    async _loadAuthorData(slug) {
        // Returns {pmid_str: {f,fa,l,la}} or null if not available.
        // Results are cached so each journal file is fetched at most once.
        if (Object.prototype.hasOwnProperty.call(this.authorDataCache, slug)) {
            return this.authorDataCache[slug];
        }
        try {
            const resp = await fetch(`data/authors/${slug}.json`);
            if (!resp.ok) { this.authorDataCache[slug] = null; return null; }
            const d = await resp.json();
            this.authorDataCache[slug] = d.authors || null;
            return this.authorDataCache[slug];
        } catch (e) {
            this.authorDataCache[slug] = null;
            return null;
        }
    }

    async _resolveAuthors(pmids, iciteMap) {
        // Returns {pmid_str: {first: {name, affiliation}, last: {name, affiliation}}}.
        // Checks pre-fetched per-journal author JSON first; falls back to live EFetch
        // for PMIDs from untracked journals or those not found in the JSON.
        const out = {};
        const needLive = [];

        for (const pmid of pmids) {
            const paper = iciteMap[String(pmid)];
            const slug = paper ? this._matchJournal(paper.journal)?.slug : null;
            let found = false;

            if (slug) {
                const authorData = await this._loadAuthorData(slug);
                if (authorData && authorData[String(pmid)]) {
                    const d = authorData[String(pmid)];
                    out[String(pmid)] = {
                        first: { name: d.f || '', affiliation: d.fa || '' },
                        last:  { name: d.l || '', affiliation: d.la || '' },
                    };
                    found = true;
                }
            }

            if (!found) needLive.push(pmid);
        }

        if (needLive.length > 0) {
            const live = await this._fetchPubMedAuthors(needLive);
            Object.assign(out, live);
        }

        return out;
    }

    async _fetchPubMedAuthors(pmids) {
        // Fetch first/last author name + affiliation via EFetch XML.
        // Returns {pmid_str: {first: {name, affiliation}, last: {name, affiliation}}}.
        const out = {};
        if (!pmids || pmids.length === 0) return out;

        const batchSize = 50; // XML responses are larger; use smaller batches
        for (let i = 0; i < pmids.length; i += batchSize) {
            const batch = pmids.slice(i, i + batchSize);
            try {
                const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${batch.join(',')}&retmode=xml&rettype=abstract&tool=IMPACT&email=impact-tool@umich.edu`;
                const resp = await fetch(url);
                if (!resp.ok) continue;
                const text = await resp.text();
                const xml = new DOMParser().parseFromString(text, 'application/xml');
                for (const article of xml.querySelectorAll('PubmedArticle')) {
                    const pmid = article.querySelector('PMID')?.textContent?.trim();
                    if (!pmid) continue;
                    const authorEls = [...article.querySelectorAll('AuthorList > Author')];
                    if (!authorEls.length) continue;
                    const parseAuthor = el => {
                        const last = el.querySelector('LastName')?.textContent || '';
                        const initials = el.querySelector('Initials')?.textContent || '';
                        const aff = el.querySelector('AffiliationInfo > Affiliation')?.textContent || '';
                        return {
                            name: [last, initials].filter(Boolean).join(' '),
                            affiliation: this._cleanAffiliation(aff),
                        };
                    };
                    out[pmid] = {
                        first: parseAuthor(authorEls[0]),
                        last: parseAuthor(authorEls[authorEls.length - 1]),
                    };
                }
            } catch (e) {
                console.error('PubMed EFetch error:', e);
            }
        }
        return out;
    }

    _cleanAffiliation(aff) {
        if (!aff) return '';
        // Remove trailing email address
        let s = aff.replace(/\s*[\w.+-]+@[\w.-]+\.\w+\.?\s*$/, '').trim().replace(/[.;]+$/, '').trim();
        // Skip leading department/division/lab components so we get institution + location
        const deptPattern = /^(dept\.?|department|division|div\.?|laboratory|lab\.?|center|centre|school|college|graduate|program|unit|section|group|institute)\b/i;
        const parts = s.split(',').map(p => p.trim()).filter(Boolean);
        let start = 0;
        while (start < parts.length - 1 && deptPattern.test(parts[start])) start++;
        return parts.slice(start).join(', ');
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
            cit_per_yr: p.found ? p.citations_24mo / 2 : null,
            total: p.found ? p.total_citations : null,
            journal_rate: p.found ? p.journal_rate : null,
            benchmark_month: p.found ? (p.journal_rate_month || (p.journal_rate != null ? 'latest' : '—')) : '—',
            first_author: p.found && p.first_author ? `${p.first_author.name}${p.first_author.affiliation ? ' — ' + p.first_author.affiliation : ''}` : '—',
            last_author: p.found && p.last_author ? `${p.last_author.name}${p.last_author.affiliation ? ' — ' + p.last_author.affiliation : ''}` : '—',
        }));

        container.appendChild(UIHelpers.createTable(rows, [
            { key: 'pmid', label: 'PMID' },
            { key: 'title', label: 'Title' },
            { key: 'journal', label: 'Journal' },
            { key: 'year', label: 'Published' },
            { key: 'cit24mo', label: '24-mo Citations' },
            { key: 'cit_per_yr', label: 'Cit/yr (2-yr)', format: UIHelpers.formatIF },
            { key: 'total', label: 'Total Citations', format: UIHelpers.formatInt },
            { key: 'journal_rate', label: 'Journal Rate (benchmark)', format: UIHelpers.formatIF },
            { key: 'benchmark_month', label: 'Benchmark month' },
            { key: 'first_author', label: 'First Author' },
            { key: 'last_author', label: 'Last Author' },
        ]));

        const note = document.createElement('p');
        note.className = 'data-note';
        note.textContent = '24-mo Citations: total citations received within exactly 24 months of publication (PubMed dates). Cit/yr (2-yr): 24-mo citations ÷ 2 — the paper\'s annualized citation rate, directly comparable to the Journal Rate benchmark. Journal Rate (benchmark): the journal\'s rolling citation rate (citations per paper per year) at ~24 months after publication — computed the same way as a traditional 2-yr JIF.';
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
