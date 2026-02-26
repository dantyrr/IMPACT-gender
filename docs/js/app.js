/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
        this.authorDataCache = {};  // slug → {pmid: {f,fa,l,la}} or null if not available
        this.papersDataCache = {};  // slug → papers array
        this.papersCurrentData = null;
        this.papersSort = { col: 'c', dir: 'desc' };
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
            this.setupPapers();
            this.setupAuthorsSection();
            this.setupGeography();
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

        journalsData.forEach(jData => {
            const jIdx = this.journals.findIndex(j => j.slug === jData.slug);
            const color = chartManager.palette[jIdx % chartManager.palette.length];
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
                    dash: typeDashes[typeKey] || [],
                    months: ts.map(d => d.month),
                    values,
                });
            });
        });

        chartManager.createMultiSeriesChart('jc-chart', series);
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

    // ---- Papers Browser ----

    setupPapers() {
        this.papersPicker = new SingleJournalPicker(
            'papers-journal-picker', this.journals,
            (slug) => {
                if (slug) {
                    this.loadPapersForJournal(slug);
                } else {
                    document.getElementById('papers-content').style.display = 'none';
                    document.getElementById('papers-hint').textContent = 'Select a journal above to browse its papers.';
                    document.getElementById('papers-hint').style.display = '';
                    this.papersCurrentData = null;
                }
            }
        );

        document.getElementById('papers-search').addEventListener('input', () => this._renderPapersTable());
        document.getElementById('papers-type-filter').addEventListener('change', () => this._renderPapersTable());
        document.getElementById('papers-year-filter').addEventListener('change', () => this._renderPapersTable());

        document.querySelectorAll('#papers-table th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const col = th.dataset.col;
                if (this.papersSort.col === col) {
                    this.papersSort.dir = this.papersSort.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    this.papersSort = { col, dir: col === 'c' || col === 'y' ? 'desc' : 'asc' };
                }
                document.querySelectorAll('#papers-table th.sortable').forEach(h =>
                    h.classList.remove('sort-active', 'sort-asc', 'sort-desc')
                );
                th.classList.add('sort-active', this.papersSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
                this._renderPapersTable();
            });
        });
    }

    async loadPapersForJournal(slug) {
        const hint = document.getElementById('papers-hint');
        const content = document.getElementById('papers-content');
        hint.textContent = 'Loading…';
        hint.style.display = '';
        content.style.display = 'none';

        try {
            if (!this.papersDataCache[slug]) {
                const resp = await fetch(`data/papers/${slug}.json`);
                if (!resp.ok) throw new Error('Not found');
                const d = await resp.json();
                this.papersDataCache[slug] = d.papers || [];
                this.papersDataCache[`${slug}__geo`] = d.geo || null;
            }
            this.papersCurrentData = this.papersDataCache[slug];

            // Populate year filter
            const years = [...new Set(this.papersCurrentData.map(p => p.y).filter(Boolean))].sort((a, b) => b - a);
            const yearSel = document.getElementById('papers-year-filter');
            yearSel.innerHTML = '<option value="">All years</option>';
            years.forEach(y => {
                const opt = document.createElement('option');
                opt.value = y;
                opt.textContent = y;
                yearSel.appendChild(opt);
            });

            // Reset filters and sort
            document.getElementById('papers-search').value = '';
            document.getElementById('papers-type-filter').value = '';
            this.papersSort = { col: 'c', dir: 'desc' };
            document.querySelectorAll('#papers-table th.sortable').forEach(h =>
                h.classList.remove('sort-active', 'sort-asc', 'sort-desc')
            );
            const citCol = document.querySelector('#papers-table th[data-col="c"]');
            if (citCol) citCol.classList.add('sort-active', 'sort-desc');

            hint.style.display = 'none';
            content.style.display = '';
            this._renderPapersTable();
        } catch (e) {
            hint.textContent = 'Paper data not yet available for this journal.';
            hint.style.display = '';
        }
    }

    _renderPapersTable() {
        if (!this.papersCurrentData) return;

        const search = document.getElementById('papers-search').value.toLowerCase().trim();
        const typeFilter = document.getElementById('papers-type-filter').value.toLowerCase();
        const yearFilter = document.getElementById('papers-year-filter').value;

        let filtered = this.papersCurrentData.filter(p => {
            if (typeFilter && (p.pt || '').toLowerCase() !== typeFilter) return false;
            if (yearFilter && String(p.y) !== yearFilter) return false;
            if (search) {
                const inTitle = (p.t || '').toLowerCase().includes(search);
                const inAuthor = (p.fa || '').toLowerCase().includes(search) || (p.la || '').toLowerCase().includes(search);
                const inCountry = (p.fc || '').toLowerCase().includes(search) || (p.lc || '').toLowerCase().includes(search);
                if (!inTitle && !inAuthor && !inCountry) return false;
            }
            return true;
        });

        const { col, dir } = this.papersSort;
        filtered.sort((a, b) => {
            let av = a[col] ?? (col === 'c' || col === 'y' ? -1 : '');
            let bv = b[col] ?? (col === 'c' || col === 'y' ? -1 : '');
            if (typeof av === 'string') av = av.toLowerCase();
            if (typeof bv === 'string') bv = bv.toLowerCase();
            if (av < bv) return dir === 'asc' ? -1 : 1;
            if (av > bv) return dir === 'asc' ? 1 : -1;
            return 0;
        });

        document.getElementById('papers-count').textContent = `${filtered.length.toLocaleString()} papers`;

        const esc = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        const tbody = document.getElementById('papers-tbody');
        tbody.innerHTML = filtered.map(p => {
            const title = p.t ? (p.t.length > 90 ? p.t.slice(0, 90) + '…' : p.t) : '—';
            return `<tr class="papers-row-link" data-pmid="${p.pmid}" title="Open in PubMed">` +
                `<td>${esc(title)}</td>` +
                `<td>${p.y || '—'}</td>` +
                `<td>${esc(p.pt || '—')}</td>` +
                `<td>${esc(p.fa || '—')}</td>` +
                `<td>${esc(p.fc || '—')}</td>` +
                `<td>${p.c != null ? p.c.toLocaleString() : '—'}</td></tr>`;
        }).join('');

        // Event delegation — single listener handles all row clicks
        if (!tbody._papersClickBound) {
            tbody._papersClickBound = true;
            tbody.addEventListener('click', (e) => {
                const tr = e.target.closest('tr[data-pmid]');
                if (tr) window.open(`https://pubmed.ncbi.nlm.nih.gov/${tr.dataset.pmid}/`, '_blank');
            });
        }

        const total = this.papersCurrentData.length;
        const noteEl = document.getElementById('papers-note');
        noteEl.textContent = total >= 2000
            ? `Showing top 2,000 most-cited papers. Click any row to open in PubMed.`
            : `Showing all ${total.toLocaleString()} papers. Click any row to open in PubMed.`;
    }

    // ---- Authors Journal Breakdown ----

    setupAuthorsSection() {
        // Tab switching
        document.querySelectorAll('.authors-tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.authors-tab-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                const tabId = btn.dataset.tab;
                document.querySelectorAll('.authors-tab-panel').forEach(panel => {
                    panel.style.display = panel.id === tabId ? '' : 'none';
                });
            });
        });

        // Journal select for breakdown tab
        this.authorsPicker = new SingleJournalPicker(
            'authors-journal-picker', this.journals,
            (slug) => { if (slug) this.loadAuthorsBreakdown(slug); }
        );
    }

    async loadAuthorsBreakdown(slug) {
        const hint = document.getElementById('authors-hint');
        const content = document.getElementById('authors-content');
        hint.textContent = 'Loading…';
        hint.style.display = '';
        content.style.display = 'none';

        try {
            // Load papers data (country + author name)
            if (!this.papersDataCache[slug]) {
                const resp = await fetch(`data/papers/${slug}.json`);
                if (!resp.ok) throw new Error('No papers data');
                const d = await resp.json();
                this.papersDataCache[slug] = d.papers || [];
                this.papersDataCache[`${slug}__geo`] = d.geo || null;
            }
            const papers = this.papersDataCache[slug];

            // Load author data (institution via affiliation)
            const authorData = await this._loadAuthorData(slug);

            // Aggregate country counts (first author), normalizing variants
            const countryCounts = {};
            papers.forEach(p => {
                const c = this._normalizeCountry(p.fc);
                if (c) countryCounts[c] = (countryCounts[c] || 0) + 1;
            });

            // Aggregate institution counts (first author, from full affiliation string)
            const instCounts = {};
            if (authorData) {
                Object.values(authorData).forEach(d => {
                    if (d.fa) {
                        const inst = d.fa.split(',')[0].trim();
                        if (inst && inst.length > 2) instCounts[inst] = (instCounts[inst] || 0) + 1;
                    }
                });
            }

            // Aggregate top first authors by paper count
            const authorCounts = {};
            papers.forEach(p => {
                if (p.fa) authorCounts[p.fa] = (authorCounts[p.fa] || 0) + 1;
            });

            const topCountries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]).slice(0, 15);
            const topInst = Object.entries(instCounts).sort((a, b) => b[1] - a[1]).slice(0, 15);
            const topAuthors = Object.entries(authorCounts).sort((a, b) => b[1] - a[1]).slice(0, 20);

            // Update metrics row
            const metricsRow = document.getElementById('authors-metrics-row');
            const metrics = [
                [papers.length.toLocaleString(), 'Papers (top 2k)'],
                [Object.keys(countryCounts).length, 'Countries'],
            ];
            if (authorData) metrics.push([Object.keys(instCounts).length, 'Institutions']);
            metricsRow.innerHTML = metrics.map(([val, label]) =>
                `<div class="metric-card"><span class="metric-value">${val}</span><span class="metric-label">${label}</span></div>`
            ).join('');

            hint.style.display = 'none';
            content.style.display = '';

            chartManager.createBarChart('authors-country-chart',
                topCountries.map(x => x[0]), topCountries.map(x => x[1]), 'Papers');
            chartManager.createBarChart('authors-institution-chart',
                topInst.map(x => x[0]), topInst.map(x => x[1]), 'Papers');
            chartManager.createBarChart('authors-top-chart',
                topAuthors.map(x => x[0]), topAuthors.map(x => x[1]), 'Papers');

        } catch (e) {
            hint.textContent = 'Data not yet available for this journal.';
            hint.style.display = '';
            console.error('Error loading author breakdown:', e);
        }
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
