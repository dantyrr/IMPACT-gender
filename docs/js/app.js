/**
 * IMPACT App — Main application logic.
 */

class IMPACTApp {
    constructor() {
        this.journals = [];
        this.journalDataCache = {};
        this.authorDataCache = {};  // slug → {pmid: {f,fa,l,la}} or null if not available
        this._networkCenter = null;
        this._influenceJournalSlug = null;
        this._lastInfluenceRenderArgs = null;
        this._authorAllPapers = [];
        this._authorExcluded = new Set();
        this._authorTotalFound = 0;
        this._authorSort = { col: 'citations', dir: 'desc' };
        this._authorActiveTypes = null;  // null = all; Set<string> = specific types
        this.currentJournalSlug = null;
        this.currentWindow = 'timeseries';
        this._showCombined = true;
        this._showIndividual = true;
        this._compareShowCombined = true;
        this._compareShowIndividual = true;
        this._compareYZero = false;
        this._compareSeriesData = null;
        this.compareWindow = 'timeseries';
        this.jcWindow = 'timeseries';
        this.jcYZero = false;
        this._journalYZero = false;
        this._influenceYZero = false;
        this._influenceWindow = 'timeseries';
        this.papersDataCache = {};
        this._worldTopology = null;
        this._rcitsVersion = 0;
        // Range control state (null = auto)
        this.jcXMin = null; this.jcXMax = null;
        this.jcYMin = null; this.jcYMax = null;
        this.compareXMin = null; this.compareXMax = null;
        this.compareYMin = null; this.compareYMax = null;
        this._influenceXMin = null; this._influenceXMax = null;
        this._influenceYMin = null; this._influenceYMax = null;
        this.detailXMin = null; this.detailXMax = null;
        this.detailYMin = null; this.detailYMax = null;
        this._compCompXMin = null; this._compCompXMax = null;
        this._compCompYMin = null; this._compCompYMax = null;
        this._compCompShowCombined = true;
        this._compCompShowIndividual = true;
        this._geoTrendXMin = null; this._geoTrendXMax = null;
        this._geoTrendYMin = null; this._geoTrendYMax = null;
        this.init();
    }

    async init() {
        try {
            const index = await dataLoader.loadIndex();
            this.journals = index.journals || [];
            this.setupNavigation();
            this.renderJournalList(this.journals);
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
            document.getElementById('journal-list').innerHTML =
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

    }

    // ---- Journal List ----

    renderJournalList(journals) {
        const container = document.getElementById('journal-list');
        if (!container) return;
        container.innerHTML = '';

        const table = document.createElement('table');
        table.className = 'journal-list-table';
        const thead = document.createElement('thead');
        thead.innerHTML = '<tr><th>Journal</th><th>Citation Rate</th><th>Papers (24-mo)</th></tr>';
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        journals.forEach(journal => {
            const tr = document.createElement('tr');
            tr.dataset.slug = journal.slug;
            tr.className = 'journal-list-row';
            const rate = journal.latest_if != null ? Number(journal.latest_if).toFixed(2) : '—';
            const papers = journal.paper_count != null ? journal.paper_count.toLocaleString() : '—';
            tr.innerHTML = `<td class="jl-name">${UIHelpers.escapeHtml(journal.name)}</td><td class="jl-rate">${rate}</td><td class="jl-papers">${papers}</td>`;
            tr.addEventListener('click', () => this.showJournalDetail(journal.slug));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        container.appendChild(table);
    }

    setupSearch() {
        document.getElementById('journal-search').addEventListener('input', () => this._renderFilteredList());
    }

    _renderFilteredList() {
        const term = (document.getElementById('journal-search').value || '').toLowerCase().trim();
        const selected = this.jcPicker ? this.jcPicker.getSelected() : [];
        document.querySelectorAll('#journal-list [data-slug]').forEach(row => {
            const slug = row.dataset.slug;
            const j = this.journals.find(j => j.slug === slug);
            if (!j) { row.style.display = 'none'; return; }
            const matchesSel = selected.length === 0 || selected.includes(slug);
            const matchesSearch = !term || j.name.toLowerCase().includes(term) ||
                (j.abbreviation || '').toLowerCase().includes(term) || j.slug.includes(term);
            row.style.display = matchesSel && matchesSearch ? '' : 'none';
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

        this._setupToggleGroup('jc-y-toggle', (val) => {
            this.jcYZero = val === 'zero';
            this.updateJournalsTrendsChart();
        }, 'data-y');

        this._setupRangeControls('jc',
            { xMin: 'jcXMin', xMax: 'jcXMax', yMin: 'jcYMin', yMax: 'jcYMax' },
            () => this.updateJournalsTrendsChart()
        );

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
            document.getElementById('jc-range-controls').style.display = 'none';
            this._jcSeriesData = null;
            this._renderFilteredList();
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
        // Populate X selects from full month range (2005-01 onwards)
        const dataMonths = [...new Set(series.flatMap(s => s.months))].sort();
        const allMonths = this._generateFullMonthRange(dataMonths);
        this._populateXRangeSelects('jc', allMonths);
        const scaleOverrides = this._buildScaleOverrides(this.jcXMin, this.jcXMax, this.jcYMin, this.jcYMax);
        chartManager.createMultiSeriesChart('jc-chart', series, this.jcYZero, scaleOverrides);
        document.getElementById('jc-range-controls').style.display = '';
        document.getElementById('jc-download-bar').style.display = '';
        this._renderFilteredList();
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

            // Metrics: 24-mo averaged rates from by_type data
            const latest = data.latest || {};
            const ts24 = data.timeseries || [];
            const last24 = ts24.slice(-24);
            const lastEntry = ts24.length > 0 ? ts24[ts24.length - 1] : null;
            const bt = lastEntry && lastEntry.by_type ? lastEntry.by_type : {};

            // Average research rate over last 24 snapshots
            let resRateSum = 0, resRateN = 0;
            let revRateSum = 0, revRateN = 0;
            for (const entry of last24) {
                const ebt = entry.by_type || {};
                if (ebt.research && ebt.research.papers > 0) {
                    resRateSum += ebt.research.citations / ebt.research.papers;
                    resRateN++;
                }
                if (ebt.review && ebt.review.papers > 0) {
                    revRateSum += ebt.review.citations / ebt.review.papers;
                    revRateN++;
                }
            }
            const avgResRate = resRateN > 0 ? resRateSum / resRateN : null;
            const avgRevRate = revRateN > 0 ? revRateSum / revRateN : null;

            // Paper counts from latest snapshot's by_type
            const researchPapers = bt.research ? bt.research.papers : 0;
            const reviewPapers = bt.review ? bt.review.papers : 0;
            const totalPapers = researchPapers + reviewPapers
                + (bt.editorial ? bt.editorial.papers : 0)
                + (bt.letter ? bt.letter.papers : 0)
                + (bt.other ? bt.other.papers : 0);
            const reviewPct = totalPapers > 0 ? (reviewPapers / totalPapers * 100) : null;

            // Date range label
            const snapshotMonth = lastEntry ? lastEntry.month : latest.month;
            const dateRange = this._computeDateRange(snapshotMonth);
            const avgLabel = dateRange ? `24-mo avg · ${dateRange}` : '24-mo avg';

            document.getElementById('metric-research-rate').textContent = UIHelpers.formatIF(avgResRate);
            document.getElementById('metric-research-rate-label').textContent = `Research Rate (${avgLabel})`;
            document.getElementById('metric-review-rate').textContent = UIHelpers.formatIF(avgRevRate);
            document.getElementById('metric-review-rate-label').textContent = `Review Rate (${avgLabel})`;
            document.getElementById('metric-research-papers').textContent = UIHelpers.formatInt(researchPapers);
            document.getElementById('metric-research-papers-label').textContent = dateRange ? `Research Papers · ${dateRange}` : 'Research Papers';
            document.getElementById('metric-review-papers').textContent = UIHelpers.formatInt(reviewPapers);
            document.getElementById('metric-review-papers-label').textContent = dateRange ? `Review Papers · ${dateRange}` : 'Review Papers';
            document.getElementById('metric-review-pct').textContent = UIHelpers.formatPct(reviewPct);
            document.getElementById('metric-review-pct-label').textContent = 'Review %';

            // Reset window/type state for new journal
            this.currentWindow = 'timeseries';
            this._showCombined = true;
            this._showIndividual = true;
            this.detailXMin = null; this.detailXMax = null;
            this.detailYMin = null; this.detailYMax = null;
            // Reset range control inputs
            const dxMin = document.getElementById('detail-x-min');
            const dxMax = document.getElementById('detail-x-max');
            const dyMin = document.getElementById('detail-y-min');
            const dyMax = document.getElementById('detail-y-max');
            if (dxMin) dxMin.value = '';
            if (dxMax) dxMax.value = '';
            if (dyMin) dyMin.value = '';
            if (dyMax) dyMax.value = '';

            // Sync toggle button active states
            document.querySelectorAll('#window-toggle .toggle-btn').forEach(b =>
                b.classList.toggle('active', b.getAttribute('data-window') === 'timeseries')
            );
            // Reset type checkboxes: only "research" checked
            document.querySelectorAll('#type-checkboxes input').forEach(cb => {
                cb.checked = cb.value === 'research';
            });
            // Reset visibility toggles: both active
            const combBtn = document.getElementById('show-combined-btn');
            const indBtn = document.getElementById('show-individual-btn');
            if (combBtn) combBtn.classList.add('active');
            if (indBtn) indBtn.classList.add('active');

            // Setup toggle controls
            this.setupDetailToggles(data);

            // Initial charts
            const ts = data[this.currentWindow] || data.timeseries;
            const series = this._buildDetailSeries(data);
            const dataMonths = [...new Set(series.flatMap(s => s.months))].sort();
            const allMonths = this._generateFullMonthRange(dataMonths);
            this._populateXRangeSelects('detail', allMonths);
            const detailScaleOverrides = this._buildScaleOverrides(this.detailXMin, this.detailXMax, this.detailYMin, this.detailYMax);
            chartManager.createMultiSeriesChart('journal-chart', series, this._journalYZero, detailScaleOverrides);
            chartManager.createCompositionChart('composition-chart', ts, this._getCompositionVisibleTypes(), detailScaleOverrides);
            document.getElementById('detail-range-controls').style.display = '';

            // Show composition type checkboxes
            const compCheckboxes = document.getElementById('composition-type-checkboxes');
            if (compCheckboxes) {
                compCheckboxes.style.display = '';
                compCheckboxes.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    cb.checked = true;
                    cb.onchange = () => {
                        const rawTs = data[this.currentWindow] || data.timeseries;
                        const so = this._buildScaleOverrides(this.detailXMin, this.detailXMax, null, null);
                        chartManager.createCompositionChart('composition-chart', rawTs, this._getCompositionVisibleTypes(), so);
                    };
                });
            }

            // Show download bars and wire up buttons
            const detailCharts = ['jd-chart', 'composition-chart'];
            const chartCanvasIds = ['journal-chart', 'composition-chart'];
            detailCharts.forEach((id, idx) => {
                const bar = document.getElementById(`${id}-download-bar`);
                if (bar) bar.style.display = '';
                ['png', 'jpg', 'pdf'].forEach(fmt => {
                    const btn = document.getElementById(`${id}-dl-${fmt}`);
                    if (btn) btn.onclick = () => this._downloadDetailChart(chartCanvasIds[idx], slug, fmt);
                });
            });
            // CSV download for the rate chart
            const csvBtn = document.getElementById('jd-chart-dl-csv');
            if (csvBtn) csvBtn.onclick = () => this._downloadDetailCSV(data, slug);

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

    // ---- Journal Detail Downloads ----

    _downloadDetailChart(canvasId, slug, format) {
        const chart = chartManager.charts[canvasId];
        if (!chart) return;
        const filename = `${canvasId}-${slug}`;

        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            const journalName = (this.journals.find(j => j.slug === slug) || {}).name || slug;
            doc.setFontSize(11);
            doc.text(`IMPACT — ${journalName}`, 10, 10);
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

    _downloadDetailCSV(data, slug) {
        const series = this._buildDetailSeries(data);
        if (!series.length) return;
        const allMonths = [...new Set(series.flatMap(s => s.months))].sort();
        const header = ['Month', ...series.map(s => `"${s.label.replace(/"/g, '""')}"`)].join(',');
        const rows = allMonths.map(m => {
            const vals = series.map(s => {
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
        a.download = `citation-rate-${slug}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    }

    // ---- Detail series builder ----

    /**
     * Build series array for the journal detail chart based on checked types
     * and combined/individual visibility toggles.
     * All data comes exclusively from by_type — never uses rolling_if or rolling_if_no_reviews.
     */
    _buildDetailSeries(data) {
        const raw = data[this.currentWindow] || data.timeseries;
        const startIdx = raw.findIndex(d => d.papers > 0);
        const ts = startIdx >= 0 ? raw.slice(startIdx) : raw;
        const months = ts.map(d => d.month);

        const checkedTypes = Array.from(
            document.querySelectorAll('#type-checkboxes input:checked')
        ).map(cb => cb.value);

        if (checkedTypes.length === 0) return [];

        const typeLabels = {
            research: 'Research', review: 'Reviews',
            editorial: 'Editorials', letter: 'Letters', other: 'Other',
        };
        const typeDashes = {
            research: [8, 4], review: [4, 4],
            editorial: [2, 2], letter: [8, 4, 2, 4], other: [12, 3],
        };
        const typeColors = {
            research: chartManager.palette[1],
            review: chartManager.palette[2],
            editorial: chartManager.palette[3],
            letter: chartManager.palette[4],
            other: chartManager.palette[5],
        };

        // Helper: compute rate for a single type from by_type
        const typeRate = (entry, typeKey) => {
            const bt = entry.by_type && entry.by_type[typeKey];
            return (bt && bt.papers > 0) ? +(bt.citations / bt.papers).toFixed(3) : null;
        };

        const series = [];

        // Single type checked: just one solid line, no combined/individual distinction
        if (checkedTypes.length === 1) {
            const typeKey = checkedTypes[0];
            const values = ts.map(entry => typeRate(entry, typeKey));
            series.push({
                label: typeLabels[typeKey],
                color: typeColors[typeKey] || chartManager.palette[0],
                dash: [],
                months,
                values,
            });
            return series;
        }

        // Multiple types checked: combined and/or individual lines
        if (this._showCombined) {
            const values = ts.map(entry => {
                let totalCit = 0, totalPap = 0;
                checkedTypes.forEach(typeKey => {
                    const bt = entry.by_type && entry.by_type[typeKey];
                    if (bt) { totalCit += bt.citations || 0; totalPap += bt.papers || 0; }
                });
                return totalPap > 0 ? +(totalCit / totalPap).toFixed(3) : null;
            });
            const label = checkedTypes.map(t => typeLabels[t]).join(' + ');
            series.push({ label, color: chartManager.palette[0], dash: [], months, values });
        }

        if (this._showIndividual) {
            checkedTypes.forEach(typeKey => {
                const values = ts.map(entry => typeRate(entry, typeKey));
                series.push({
                    label: typeLabels[typeKey],
                    color: typeColors[typeKey] || chartManager.palette[0],
                    dash: typeDashes[typeKey] || [],
                    months,
                    values,
                });
            });
        }

        return series;
    }

    _computeDateRange(snapshotMonth) {
        if (!snapshotMonth) return null;
        const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const [yr, mo] = snapshotMonth.split('-').map(Number);
        // Papers window: 24 months ending one month before snapshot
        const endDate = new Date(yr, mo - 2, 1); // month before snapshot
        const startDate = new Date(endDate.getFullYear(), endDate.getMonth() - 23, 1);
        return `${MONTH_NAMES[startDate.getMonth()]} ${startDate.getFullYear()} – ${MONTH_NAMES[endDate.getMonth()]} ${endDate.getFullYear()}`;
    }

    setupDetailToggles(data) {
        const redrawRate = () => {
            const series = this._buildDetailSeries(data);
            const dataMonths = [...new Set(series.flatMap(s => s.months))].sort();
            const allMonths = this._generateFullMonthRange(dataMonths);
            this._populateXRangeSelects('detail', allMonths);
            const scaleOverrides = this._buildScaleOverrides(this.detailXMin, this.detailXMax, this.detailYMin, this.detailYMax);
            chartManager.createMultiSeriesChart('journal-chart', series, this._journalYZero, scaleOverrides);
        };

        const redrawSecondary = () => {
            const rawTs = data[this.currentWindow] || data.timeseries;
            const scaleOverrides = this._buildScaleOverrides(this.detailXMin, this.detailXMax, null, null);
            chartManager.createCompositionChart('composition-chart', rawTs, this._getCompositionVisibleTypes(), scaleOverrides);
        };

        // Window toggle — affects all charts
        this._setupToggleGroup('window-toggle', (windowKey) => {
            this.currentWindow = windowKey;
            redrawRate();
            redrawSecondary();
        }, 'data-window');

        // Article type checkboxes — only affects rate chart
        document.querySelectorAll('#type-checkboxes input').forEach(cb => {
            const newCb = cb.cloneNode(true);
            cb.parentNode.replaceChild(newCb, cb);
            newCb.addEventListener('change', () => redrawRate());
        });

        // Visibility toggles (Combined / Individual) — independent, not mutually exclusive
        const combBtn = document.getElementById('show-combined-btn');
        const indBtn = document.getElementById('show-individual-btn');
        if (combBtn && indBtn) {
            // Clone to remove old listeners
            const newComb = combBtn.cloneNode(true);
            combBtn.parentNode.replaceChild(newComb, combBtn);
            const newInd = indBtn.cloneNode(true);
            indBtn.parentNode.replaceChild(newInd, indBtn);

            newComb.addEventListener('click', () => {
                this._showCombined = !this._showCombined;
                // Guard: don't allow both off
                if (!this._showCombined && !this._showIndividual) {
                    this._showIndividual = true;
                    newInd.classList.add('active');
                }
                newComb.classList.toggle('active', this._showCombined);
                redrawRate();
            });
            newInd.addEventListener('click', () => {
                this._showIndividual = !this._showIndividual;
                // Guard: don't allow both off
                if (!this._showIndividual && !this._showCombined) {
                    this._showCombined = true;
                    newComb.classList.add('active');
                }
                newInd.classList.toggle('active', this._showIndividual);
                redrawRate();
            });
        }

        // Y-axis zero toggle
        this._setupToggleGroup('journal-y-toggle', (val) => {
            this._journalYZero = val === 'zero';
            redrawRate();
        }, 'data-y');

        // Range controls for detail charts
        this._setupRangeControls('detail',
            { xMin: 'detailXMin', xMax: 'detailXMax', yMin: 'detailYMin', yMax: 'detailYMax' },
            () => { redrawRate(); redrawSecondary(); }
        );
    }

    _getCompositionVisibleTypes() {
        const container = document.getElementById('composition-type-checkboxes');
        if (!container) return ['research', 'review', 'editorial', 'letter', 'other'];
        return Array.from(container.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
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

    // ---- Range Controls ----

    /**
     * Wire up range controls for a chart.
     * prefix: 'jc' | 'compare' | 'influence'
     * stateKeys: {xMin, xMax, yMin, yMax} — property names on `this`
     * redrawFn: () => void
     */
    _setupRangeControls(prefix, stateKeys, redrawFn) {
        const xMinSel = document.getElementById(`${prefix}-x-min`);
        const xMaxSel = document.getElementById(`${prefix}-x-max`);
        const yMinInp = document.getElementById(`${prefix}-y-min`);
        const yMaxInp = document.getElementById(`${prefix}-y-max`);
        const resetBtn = document.getElementById(`${prefix}-range-reset`);

        if (!xMinSel || !xMaxSel || !yMinInp || !yMaxInp || !resetBtn) return;

        let yDebounce = null;

        xMinSel.addEventListener('change', () => {
            this[stateKeys.xMin] = xMinSel.value || null;
            redrawFn();
        });
        xMaxSel.addEventListener('change', () => {
            this[stateKeys.xMax] = xMaxSel.value || null;
            redrawFn();
        });

        const onYInput = () => {
            clearTimeout(yDebounce);
            yDebounce = setTimeout(() => {
                this[stateKeys.yMin] = yMinInp.value !== '' ? parseFloat(yMinInp.value) : null;
                this[stateKeys.yMax] = yMaxInp.value !== '' ? parseFloat(yMaxInp.value) : null;
                redrawFn();
            }, 400);
        };
        yMinInp.addEventListener('input', onYInput);
        yMaxInp.addEventListener('input', onYInput);

        resetBtn.addEventListener('click', () => {
            this[stateKeys.xMin] = null; this[stateKeys.xMax] = null;
            this[stateKeys.yMin] = null; this[stateKeys.yMax] = null;
            xMinSel.value = '';
            xMaxSel.value = '';
            yMinInp.value = '';
            yMaxInp.value = '';
            redrawFn();
        });
    }

    /**
     * Generate a full month range from 2005-01 to the last month in the data.
     */
    _generateFullMonthRange(dataMonths) {
        if (!dataMonths.length) return dataMonths;
        const lastMonth = dataMonths[dataMonths.length - 1];
        const [endYr, endMo] = lastMonth.split('-').map(Number);
        const months = [];
        for (let yr = 2005; yr <= endYr; yr++) {
            const mEnd = yr === endYr ? endMo : 12;
            for (let mo = 1; mo <= mEnd; mo++) {
                months.push(`${yr}-${String(mo).padStart(2, '0')}`);
            }
        }
        return months;
    }

    /**
     * Populate X-range selects from a months array.
     * Preserves current selection if still valid.
     */
    _populateXRangeSelects(prefix, months) {
        const xMinSel = document.getElementById(`${prefix}-x-min`);
        const xMaxSel = document.getElementById(`${prefix}-x-max`);
        if (!xMinSel || !xMaxSel || !months.length) return;

        const prevMin = xMinSel.value;
        const prevMax = xMaxSel.value;

        // Group by year
        const byYear = {};
        months.forEach(m => {
            const yr = m.slice(0, 4);
            if (!byYear[yr]) byYear[yr] = [];
            byYear[yr].push(m);
        });

        const buildOptions = (sel, addBlank, blankText = '(start)') => {
            sel.innerHTML = '';
            if (addBlank) {
                const blank = document.createElement('option');
                blank.value = '';
                blank.textContent = blankText;
                sel.appendChild(blank);
            }
            Object.keys(byYear).sort().forEach(yr => {
                const grp = document.createElement('optgroup');
                grp.label = yr;
                byYear[yr].forEach(m => {
                    const opt = document.createElement('option');
                    opt.value = m;
                    opt.textContent = m;
                    grp.appendChild(opt);
                });
                sel.appendChild(grp);
            });
        };

        buildOptions(xMinSel, true, '(start)');
        buildOptions(xMaxSel, true, '(end)');

        // Restore previous selections if still valid
        if (prevMin && months.includes(prevMin)) xMinSel.value = prevMin;
        if (prevMax && months.includes(prevMax)) xMaxSel.value = prevMax;
    }

    /**
     * Build a scaleOverrides object from range state.
     */
    _buildScaleOverrides(xMin, xMax, yMin, yMax) {
        const overrides = {};
        if (xMin != null || xMax != null) {
            overrides.x = {};
            if (xMin != null) overrides.x.min = xMin;
            if (xMax != null) overrides.x.max = xMax;
        }
        if (yMin != null || yMax != null) {
            overrides.y = {};
            if (yMin != null) overrides.y.min = yMin;
            if (yMax != null) overrides.y.max = yMax;
        }
        return overrides;
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

        // Article type checkboxes
        document.querySelectorAll('#compare-type-checkboxes input').forEach(cb => {
            cb.addEventListener('change', () => this.updateComparison());
        });

        // Combined / Individual visibility toggles
        const combBtn = document.getElementById('compare-combined-btn');
        const indBtn = document.getElementById('compare-individual-btn');
        if (combBtn && indBtn) {
            combBtn.addEventListener('click', () => {
                this._compareShowCombined = !this._compareShowCombined;
                if (!this._compareShowCombined && !this._compareShowIndividual) {
                    this._compareShowIndividual = true;
                    indBtn.classList.add('active');
                }
                combBtn.classList.toggle('active', this._compareShowCombined);
                this.updateComparison();
            });
            indBtn.addEventListener('click', () => {
                this._compareShowIndividual = !this._compareShowIndividual;
                if (!this._compareShowIndividual && !this._compareShowCombined) {
                    this._compareShowCombined = true;
                    combBtn.classList.add('active');
                }
                indBtn.classList.toggle('active', this._compareShowIndividual);
                this.updateComparison();
            });
        }

        // Y-axis toggle
        this._setupToggleGroup('compare-y-toggle', (val) => {
            this._compareYZero = val === 'zero';
            this.updateComparison();
        }, 'data-y');

        // Range controls
        this._setupRangeControls('compare',
            { xMin: 'compareXMin', xMax: 'compareXMax', yMin: 'compareYMin', yMax: 'compareYMax' },
            () => this.updateComparison()
        );

        // Download buttons
        ['png', 'jpg', 'pdf'].forEach(fmt => {
            const btn = document.getElementById(`compare-dl-${fmt}`);
            if (btn) btn.addEventListener('click', () => this._downloadCompareChart(fmt));
        });
        const csvBtn = document.getElementById('compare-dl-csv');
        if (csvBtn) csvBtn.addEventListener('click', () => this._downloadCompareCSV());

        // Composition Combined / Individual toggles
        const compCombBtn = document.getElementById('compare-comp-combined-btn');
        const compIndBtn = document.getElementById('compare-comp-individual-btn');
        if (compCombBtn && compIndBtn) {
            compCombBtn.addEventListener('click', () => {
                this._compCompShowCombined = !this._compCompShowCombined;
                if (!this._compCompShowCombined && !this._compCompShowIndividual) {
                    this._compCompShowIndividual = true;
                    compIndBtn.classList.add('active');
                }
                compCombBtn.classList.toggle('active', this._compCompShowCombined);
                this.updateComparison();
            });
            compIndBtn.addEventListener('click', () => {
                this._compCompShowIndividual = !this._compCompShowIndividual;
                if (!this._compCompShowIndividual && !this._compCompShowCombined) {
                    this._compCompShowCombined = true;
                    compCombBtn.classList.add('active');
                }
                compIndBtn.classList.toggle('active', this._compCompShowIndividual);
                this.updateComparison();
            });
        }

        // Composition range controls
        this._setupRangeControls('compare-comp',
            { xMin: '_compCompXMin', xMax: '_compCompXMax', yMin: '_compCompYMin', yMax: '_compCompYMax' },
            () => this.updateComparison()
        );

        // Composition chart download buttons
        ['png', 'jpg', 'pdf'].forEach(fmt => {
            const btn = document.getElementById(`compare-comp-dl-${fmt}`);
            if (btn) btn.addEventListener('click', () => this._downloadCompareChart(fmt, 'compare-composition-chart', 'compare-composition'));
        });
        const compCsvBtn = document.getElementById('compare-comp-dl-csv');
        if (compCsvBtn) compCsvBtn.addEventListener('click', () => this._downloadCompareCompositionCSV());
    }

    async updateComparison() {
        const checked = this.comparePicker.getSelected();
        const tableContainer = document.getElementById('compare-table-container');
        const downloadBar = document.getElementById('compare-download-bar');

        if (checked.length === 0) {
            chartManager._destroy('compare-chart');
            chartManager._destroy('compare-composition-chart');
            document.getElementById('compare-range-controls').style.display = 'none';
            if (downloadBar) downloadBar.style.display = 'none';
            if (tableContainer) tableContainer.innerHTML = '';
            const metricsContainer = document.getElementById('compare-metrics-container');
            if (metricsContainer) metricsContainer.innerHTML = '';
            const compContainer = document.getElementById('compare-composition-container');
            if (compContainer) compContainer.style.display = 'none';
            this._compareSeriesData = null;
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

        // Build series from by_type data
        const checkedTypes = Array.from(
            document.querySelectorAll('#compare-type-checkboxes input:checked')
        ).map(cb => cb.value);

        if (checkedTypes.length === 0) {
            chartManager._destroy('compare-chart');
            chartManager._destroy('compare-composition-chart');
            if (downloadBar) downloadBar.style.display = 'none';
            const compContainer = document.getElementById('compare-composition-container');
            if (compContainer) compContainer.style.display = 'none';
            this._compareSeriesData = null;
            return;
        }

        const typeLabels = {
            research: 'Research', review: 'Reviews',
            editorial: 'Editorials', letter: 'Letters', other: 'Other',
        };
        const typeDashes = {
            research: [8, 4], review: [4, 4],
            editorial: [2, 2], letter: [8, 4, 2, 4], other: [12, 3],
        };

        const colorMap = this.comparePicker.getColorMap();
        const multiJournal = journalsData.length > 1;
        const multiType = checkedTypes.length > 1;
        const series = [];

        const typeRate = (entry, typeKey) => {
            const bt = entry.by_type && entry.by_type[typeKey];
            return (bt && bt.papers > 0) ? +(bt.citations / bt.papers).toFixed(3) : null;
        };

        journalsData.forEach((jData, jIdx) => {
            const color = colorMap[jData.slug] || chartManager.palette[jIdx % chartManager.palette.length];
            const raw = jData[this.compareWindow] || jData.timeseries;
            const startIdx = raw.findIndex(d => d.papers > 0);
            const ts = startIdx >= 0 ? raw.slice(startIdx) : raw;
            const months = ts.map(d => d.month);

            if (!multiType) {
                // Single type: one solid line per journal
                const typeKey = checkedTypes[0];
                const values = ts.map(entry => typeRate(entry, typeKey));
                const label = multiJournal ? jData.journal : typeLabels[typeKey];
                series.push({ label, color, dash: [], months, values });
            } else {
                // Multiple types: combined and/or individual
                if (this._compareShowCombined) {
                    const values = ts.map(entry => {
                        let totalCit = 0, totalPap = 0;
                        checkedTypes.forEach(typeKey => {
                            const bt = entry.by_type && entry.by_type[typeKey];
                            if (bt) { totalCit += bt.citations || 0; totalPap += bt.papers || 0; }
                        });
                        return totalPap > 0 ? +(totalCit / totalPap).toFixed(3) : null;
                    });
                    const label = multiJournal
                        ? `${jData.journal} — Combined`
                        : checkedTypes.map(t => typeLabels[t]).join(' + ');
                    series.push({ label, color, dash: [], months, values });
                }

                if (this._compareShowIndividual) {
                    checkedTypes.forEach(typeKey => {
                        const values = ts.map(entry => typeRate(entry, typeKey));
                        const label = multiJournal
                            ? `${jData.journal} — ${typeLabels[typeKey]}`
                            : typeLabels[typeKey];
                        series.push({
                            label, color,
                            dash: typeDashes[typeKey] || [],
                            months, values,
                        });
                    });
                }
            }
        });

        this._compareSeriesData = series;

        // Populate X selects from full month range (2005-01 onwards)
        const dataMonths = [...new Set(series.flatMap(s => s.months))].sort();
        const allMonths = this._generateFullMonthRange(dataMonths);
        this._populateXRangeSelects('compare', allMonths);

        const scaleOverrides = this._buildScaleOverrides(this.compareXMin, this.compareXMax, this.compareYMin, this.compareYMax);
        chartManager.createMultiSeriesChart('compare-chart', series, this._compareYZero, scaleOverrides);
        document.getElementById('compare-range-controls').style.display = '';
        if (downloadBar) downloadBar.style.display = '';

        this._renderCompareMetrics(journalsData);

        // Paper Composition chart
        const compContainer = document.getElementById('compare-composition-container');
        if (compContainer) {
            compContainer.style.display = '';
            this._populateXRangeSelects('compare-comp', allMonths);
            const compScaleOverrides = this._buildScaleOverrides(this._compCompXMin, this._compCompXMax, this._compCompYMin, this._compCompYMax);
            chartManager.createCompareCompositionChart(
                'compare-composition-chart', journalsData, colorMap, checkedTypes, this.compareWindow, compScaleOverrides,
                this._compCompShowCombined, this._compCompShowIndividual
            );
            document.getElementById('compare-comp-range-controls').style.display = '';
            const compBar = document.getElementById('compare-composition-download-bar');
            if (compBar) compBar.style.display = '';
        }

        if (tableContainer) {
            this.renderComparisonTable(tableContainer, journalsData);
        }
    }

    _renderCompareMetrics(journalsData) {
        const container = document.getElementById('compare-metrics-container');
        if (!container) return;
        if (!journalsData.length) { container.innerHTML = ''; return; }

        const colorMap = this.comparePicker.getColorMap();

        container.innerHTML = journalsData.map((j, jIdx) => {
            const ts24 = j.timeseries || [];
            const last24 = ts24.slice(-24);
            const lastEntry = ts24.length > 0 ? ts24[ts24.length - 1] : null;
            const bt = lastEntry && lastEntry.by_type ? lastEntry.by_type : {};

            let resRateSum = 0, resRateN = 0;
            let revRateSum = 0, revRateN = 0;
            for (const entry of last24) {
                const ebt = entry.by_type || {};
                if (ebt.research && ebt.research.papers > 0) {
                    resRateSum += ebt.research.citations / ebt.research.papers;
                    resRateN++;
                }
                if (ebt.review && ebt.review.papers > 0) {
                    revRateSum += ebt.review.citations / ebt.review.papers;
                    revRateN++;
                }
            }

            const researchPapers = bt.research ? bt.research.papers : 0;
            const reviewPapers = bt.review ? bt.review.papers : 0;
            const totalPapers = researchPapers + reviewPapers
                + (bt.editorial ? bt.editorial.papers : 0)
                + (bt.letter ? bt.letter.papers : 0)
                + (bt.other ? bt.other.papers : 0);
            const reviewPct = totalPapers > 0 ? (reviewPapers / totalPapers * 100) : null;

            const snapshotMonth = lastEntry ? lastEntry.month : null;
            const dateRange = this._computeDateRange(snapshotMonth);
            const avgLabel = dateRange ? `24-mo avg · ${dateRange}` : '24-mo avg';
            const color = colorMap[j.slug] || chartManager.palette[jIdx % chartManager.palette.length];

            const cards = [
                [UIHelpers.formatIF(resRateN > 0 ? resRateSum / resRateN : null), `Research Rate (${avgLabel})`],
                [UIHelpers.formatIF(revRateN > 0 ? revRateSum / revRateN : null), `Review Rate (${avgLabel})`],
                [UIHelpers.formatInt(researchPapers), dateRange ? `Research Papers · ${dateRange}` : 'Research Papers'],
                [UIHelpers.formatInt(reviewPapers), dateRange ? `Review Papers · ${dateRange}` : 'Review Papers'],
                [UIHelpers.formatPct(reviewPct), 'Review %'],
            ].map(([v, l]) =>
                `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
            ).join('');

            return `<div class="compare-metrics-journal">
                <div class="compare-metrics-header" style="border-left: 4px solid ${color}; padding-left: 0.5rem;">${j.journal}</div>
                <div class="metrics-row">${cards}</div>
            </div>`;
        }).join('');
    }

    renderComparisonTable(container, journalsData) {
        const rows = journalsData.map(j => {
            const ts24 = j.timeseries || [];
            const last24 = ts24.slice(-24);
            const lastEntry = ts24.length > 0 ? ts24[ts24.length - 1] : null;
            const bt = lastEntry && lastEntry.by_type ? lastEntry.by_type : {};

            let resRateSum = 0, resRateN = 0;
            let revRateSum = 0, revRateN = 0;
            for (const entry of last24) {
                const ebt = entry.by_type || {};
                if (ebt.research && ebt.research.papers > 0) {
                    resRateSum += ebt.research.citations / ebt.research.papers;
                    resRateN++;
                }
                if (ebt.review && ebt.review.papers > 0) {
                    revRateSum += ebt.review.citations / ebt.review.papers;
                    revRateN++;
                }
            }

            const researchPapers = bt.research ? bt.research.papers : 0;
            const reviewPapers = bt.review ? bt.review.papers : 0;
            const totalPapers = researchPapers + reviewPapers
                + (bt.editorial ? bt.editorial.papers : 0)
                + (bt.letter ? bt.letter.papers : 0)
                + (bt.other ? bt.other.papers : 0);

            return {
                journal: j.journal,
                research_rate: resRateN > 0 ? resRateSum / resRateN : null,
                review_rate: revRateN > 0 ? revRateSum / revRateN : null,
                research_papers: researchPapers,
                review_papers: reviewPapers,
                review_pct: totalPapers > 0 ? (reviewPapers / totalPapers * 100) : null,
            };
        });

        const columns = [
            { key: 'journal', label: 'Journal' },
            { key: 'research_rate', label: 'Research Rate', format: UIHelpers.formatIF },
            { key: 'review_rate', label: 'Review Rate', format: UIHelpers.formatIF },
            { key: 'research_papers', label: 'Research Papers', format: UIHelpers.formatInt },
            { key: 'review_papers', label: 'Review Papers', format: UIHelpers.formatInt },
            { key: 'review_pct', label: 'Review %', format: UIHelpers.formatPct },
        ];

        container.innerHTML = '';
        container.appendChild(UIHelpers.createTable(rows, columns));
    }

    // ---- Compare Downloads ----

    _downloadCompareChart(format, chartId = 'compare-chart', filename = 'compare-journals') {
        const chart = chartManager.charts[chartId];
        if (!chart) return;

        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            doc.setFontSize(11);
            doc.text('IMPACT — Journal Comparison', 10, 10);
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

    _downloadCompareCSV() {
        const series = this._compareSeriesData;
        if (!series || !series.length) return;
        const allMonths = [...new Set(series.flatMap(s => s.months))].sort();
        const header = ['Month', ...series.map(s => `"${s.label.replace(/"/g, '""')}"`)].join(',');
        const rows = allMonths.map(m => {
            const vals = series.map(s => {
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
        a.download = 'compare-journals.csv';
        a.click();
        URL.revokeObjectURL(url);
    }

    _downloadCompareCompositionCSV() {
        const chart = chartManager.charts['compare-composition-chart'];
        if (!chart) return;
        const labels = chart.data.labels;
        const datasets = chart.data.datasets;
        const header = ['Month', ...datasets.map(ds => `"${ds.label.replace(/"/g, '""')}"`)].join(',');
        const rows = labels.map((m, i) => {
            const vals = datasets.map(ds => {
                const v = ds.data[i];
                return v != null ? v : '';
            });
            return [m, ...vals].join(',');
        });
        const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'compare-composition.csv';
        a.click();
        URL.revokeObjectURL(url);
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
        document.getElementById('paper-analytics').style.display = 'none';

        // Reset state for new paper
        this._networkCenter = null;

        try {
            const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${pmid}`);
            if (!resp.ok) throw new Error('iCite API error');
            const json = await resp.json();
            const items = Array.isArray(json) ? json : (json.data || []);
            if (!items.length) { hint.textContent = 'Paper not found in iCite. Check the PMID.'; return; }

            const center = items[0];
            if (!center.citation_count) {
                hint.textContent = 'No citing papers found for this PMID yet.';
                return;
            }

            this._networkCenter = center;

            document.getElementById('network-metrics').innerHTML = [
                [center.citation_count.toLocaleString(), 'Total Citations'],
                [center.year || '—', 'Year Published'],
            ].map(([v, l]) =>
                `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
            ).join('');

            hint.style.display = 'none';
            results.style.display = '';

            await this._renderPaperAnalytics();

        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('Paper citation error:', e);
        }
    }

    // ---- Paper Analytics ----

    async _fetchPubMonth(pmid) {
        try {
            const resp = await fetch(
                `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${pmid}&retmode=json`,
                { signal: AbortSignal.timeout(6000) }
            );
            if (!resp.ok) return null;
            const json = await resp.json();
            const pubDate = (json?.result?.[String(pmid)]?.pubdate) || '';
            const MONTHS = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
            const m = pubDate.toLowerCase().match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/);
            return m ? (MONTHS[m[1]] || null) : null;
        } catch (e) { return null; }
    }

    async _renderPaperAnalytics() {
        const center = this._networkCenter;
        if (!center) return;

        // Show panel early with loading state
        const panel = document.getElementById('paper-analytics');
        panel.style.display = '';
        document.getElementById('paper-analytics-title').textContent = center.title || `PMID ${center.pmid}`;
        const authNames = Array.isArray(center.authors)
            ? center.authors.map(a => (typeof a === 'string') ? a : (a.fullName || a.lastName || ''))
            : [];
        const authStr = authNames.length
            ? authNames.slice(0, 2).join(', ') + (authNames.length > 2 ? ' et al.' : '') : '';
        document.getElementById('paper-analytics-meta').textContent =
            [authStr, center.journal, center.year].filter(Boolean).join(' · ');
        document.getElementById('paper-analytics-stats').innerHTML =
            `<div class="metric-card"><span class="metric-value">Loading…</span><span class="metric-label">Fetching citation data</span></div>`;

        // Build year histogram directly from citedByPmidsByYear (already in iCite response)
        const yearCounts = {};
        const citedByYear = center.citedByPmidsByYear || [];
        for (const entry of citedByYear) {
            const yr = Object.values(entry)[0];
            if (yr) yearCounts[yr] = (yearCounts[yr] || 0) + 1;
        }

        const pubMonth = await this._fetchPubMonth(center.pmid);

        const currentYear = new Date().getFullYear();
        if (center.year) {
            for (let y = center.year; y <= currentYear; y++) {
                if (!(y in yearCounts)) yearCounts[y] = 0;
            }
        }

        this._paperYearCounts = yearCounts;
        this._paperPubMonth = pubMonth;

        const totalCitations = center.citation_count || 0;
        const pubYear = center.year;
        // Use completed years only (exclude current partial year); base avg on
        // yearCounts so it stays consistent with the bar chart.
        const yearsActive = pubYear ? Math.max(1, currentYear - pubYear) : 1;
        const sampledTotal = Object.values(yearCounts).reduce((s, v) => s + v, 0);
        const lifetimeAvg = sampledTotal / yearsActive;

        // JIF 2-yr avg: (pub_year citations + pub_year+1 citations) / 2
        const jifAvg = pubYear
            ? ((yearCounts[pubYear] || 0) + (yearCounts[pubYear + 1] || 0)) / 2
            : null;

        // IMPACT 24-month avg: weighted by fraction of each calendar year in the 24-month window
        let twentyFourMonthAvg = null;
        let jifWindowMonths = null;
        const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        if (pubYear && pubMonth) {
            jifWindowMonths = 25 - pubMonth; // months of actual citation opportunity in JIF window
            const fracY  = (13 - pubMonth) / 12;  // fraction of pub year in 24-mo window
            const fracY2 = (pubMonth - 1) / 12;   // fraction of pub_year+2 in 24-mo window
            twentyFourMonthAvg = (
                (yearCounts[pubYear]     || 0) * fracY +
                (yearCounts[pubYear + 1] || 0) +
                (yearCounts[pubYear + 2] || 0) * fracY2
            ) / 2;
        } else if (pubYear) {
            twentyFourMonthAvg = jifAvg; // approximate (no pub month)
        }

        const jifTip = pubYear
            ? `Citations in ${pubYear} and ${pubYear+1} ÷ 2. ` +
              (pubMonth ? `Published in ${MONTH_NAMES[pubMonth-1]}, so the JIF window covers only ${jifWindowMonths} months — ` +
              `papers published earlier in the year get more citation time.` :
              'This is the citation contribution used in the traditional Journal Impact Factor.')
            : 'Traditional JIF calculation: first 2 calendar years ÷ 2.';
        const impactTip = pubYear
            ? `Citations in the 24 months from publication ÷ 2. ` +
              (pubMonth
                ? `For this paper (${MONTH_NAMES[pubMonth-1]} ${pubYear}), the window runs ` +
                  `${MONTH_NAMES[pubMonth-1]} ${pubYear} → ${MONTH_NAMES[(pubMonth+10)%12]} ${pubYear+2}. ` +
                  `Every paper gets an equal 24-month window regardless of publication month — ` +
                  `this eliminates the calendar bias that affects the traditional JIF.`
                : 'Every paper gets an equal 24-month window regardless of publication month.')
            : 'IMPACT 24-month rolling citation rate.';

        const pubmedCoverageTip = 'iCite counts only citations from PubMed-indexed papers. ' +
            'Google Scholar and Web of Science count more sources (preprints, conference papers, ' +
            'books, non-biomedical journals), so their totals will be higher — sometimes by ' +
            'a large factor for interdisciplinary papers.';

        // Stats
        document.getElementById('paper-analytics-stats').innerHTML = [
            [totalCitations.toLocaleString(), 'PubMed Citations ⓘ', '', pubmedCoverageTip],
            [lifetimeAvg.toFixed(1), 'Avg per Year (PubMed) ⓘ', '', pubmedCoverageTip],
            [jifAvg != null ? jifAvg.toFixed(1) : '—',
                pubMonth ? `JIF 2-Yr Avg (${jifWindowMonths}-mo window) ⓘ` : 'JIF 2-Yr Avg ⓘ',
                '#E69F00', jifTip],
            [twentyFourMonthAvg != null ? twentyFourMonthAvg.toFixed(1) : '—',
                'IMPACT 24-Mo Avg ⓘ',
                '#009E73', impactTip],
        ].map(([v, l, color, tip]) =>
            `<div class="metric-card"${tip ? ` title="${tip}"` : ''}${color ? ` style="border-left:3px solid ${color}; padding-left:0.75rem;"` : ''}>` +
            `<span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
        ).join('');

        // Wire toggles
        this._paperWindow = 1;
        this._paperJifWindow = false;
        this._paperPubYear = pubYear;

        document.querySelectorAll('#paper-window-toggle .toggle-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.win === '1');
            btn.onclick = () => {
                this._paperWindow = parseInt(btn.dataset.win);
                document.querySelectorAll('#paper-window-toggle .toggle-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._updatePaperCitationChart();
            };
        });

        document.querySelectorAll('#paper-jif-toggle .toggle-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.jif === 'off');
            btn.onclick = () => {
                this._paperJifWindow = btn.dataset.jif === 'on';
                document.querySelectorAll('#paper-jif-toggle .toggle-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._updatePaperCitationChart();
            };
        });

        // Download buttons
        document.getElementById('paper-cit-download-bar').style.display = '';
        document.getElementById('paper-cit-dl-png').onclick = () => this._downloadPaperChart('png');
        document.getElementById('paper-cit-dl-jpg').onclick = () => this._downloadPaperChart('jpg');
        document.getElementById('paper-cit-dl-pdf').onclick = () => this._downloadPaperChart('pdf');

        // Render chart
        this._updatePaperCitationChart();
    }

    _updatePaperCitationChart() {
        chartManager.createPaperCitationChart(
            'paper-citations-chart',
            this._paperYearCounts || {},
            this._paperWindow || 1,
            null,
            '',
            this._paperPubYear || null,
            this._paperPubMonth || null,
            this._paperJifWindow || false,
        );

        // Color legend note below chart
        const note = document.getElementById('paper-chart-note');
        if (note) {
            const win = this._paperWindow || 1;
            const py  = this._paperPubYear;
            const pm  = this._paperPubMonth;
            if (win === 1 && py && pm) {
                const sq = (color) =>
                    `<span style="display:inline-block;width:10px;height:10px;background:${color};border-radius:2px;vertical-align:middle;margin-right:3px;"></span>`;
                const sep = ' &nbsp;|&nbsp; ';
                if (this._paperJifWindow) {
                    const extNote = pm > 1
                        ? `${sep}${sq('#009E73')} 24-mo extension into ${py + 2} (${pm - 1} month${pm - 1 > 1 ? 's' : ''})`
                        : '';
                    note.innerHTML =
                        `${sq('#E69F00')} JIF window (${py}–${py + 1})` +
                        extNote +
                        `${sep}${sq('#0072B2')} outside 24-mo window`;
                } else {
                    const extNote = pm > 1
                        ? ` (includes ${pm - 1} month${pm - 1 > 1 ? 's' : ''} into ${py + 2})`
                        : '';
                    note.innerHTML =
                        `${sq('#009E73')} in 24-mo window${extNote}` +
                        `${sep}${sq('#0072B2')} outside 24-mo window`;
                }
                note.style.display = '';
            } else {
                note.style.display = 'none';
            }
        }
    }

    _downloadPaperChart(format) {
        const chart = chartManager.charts['paper-citations-chart'];
        if (!chart) return;
        const pmid = this._networkCenter?.pmid || 'unknown';
        const filename = `citations-pmid-${pmid}`;
        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            doc.setFontSize(11);
            doc.text(`IMPACT — Citations: PMID ${pmid}`, 10, 10);
            doc.addImage(chart.toBase64Image('image/png', 1), 'PNG', 10, 16, imgW, imgH);
            doc.save(`${filename}.pdf`);
            return;
        }
        const mime = format === 'jpg' ? 'image/jpeg' : 'image/png';
        const a = document.createElement('a');
        a.href = chart.toBase64Image(mime, 1);
        a.download = `${filename}.${format}`;
        a.click();
    }

    _fmtAuthors(authors) {
        if (!authors) return '';
        if (typeof authors === 'string') return authors;
        if (Array.isArray(authors)) {
            return authors.map(a => {
                if (typeof a === 'string') return a;
                return a.name || a.fullName || a.lastName || a.lastname || a.family || a.collective || '';
            }).filter(Boolean).join(', ');
        }
        return '';
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


    async _computeReceivedCitsByYear(active, canvasId = 'author-rcits-chart', loadingId = 'author-rcits-loading', dlPrefix = 'rcits') {
        const vKey = `_rcitsV_${canvasId}`;
        const version = (this[vKey] = (this[vKey] || 0) + 1);
        const loadingEl = document.getElementById(loadingId);
        if (loadingEl) { loadingEl.style.display = ''; loadingEl.textContent = 'Loading\u2026'; }

        const allCitedBy = [...new Set(active.flatMap(p => (p.cited_by || []).map(String)))];

        if (allCitedBy.length === 0) {
            if (this[vKey] !== version) return;
            if (loadingEl) loadingEl.style.display = 'none';
            chartManager.createBarChart(canvasId, [], [], 'Citations', { horizontal: false });
            return;
        }

        const MAX = 10000;
        const pmidsToFetch = allCitedBy.slice(0, MAX);
        const BATCH = 100, CONC = 5;
        const byYear = {};

        for (let i = 0; i < pmidsToFetch.length; i += BATCH * CONC) {
            if (this[vKey] !== version) return;
            const batches = [];
            for (let j = 0; j < CONC && (i + j * BATCH) < pmidsToFetch.length; j++) {
                batches.push(pmidsToFetch.slice(i + j * BATCH, i + (j + 1) * BATCH));
            }
            const results = await Promise.all(batches.map(async batch => {
                try {
                    const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${batch.join(',')}`);
                    if (!resp.ok) return [];
                    const json = await resp.json();
                    return Array.isArray(json) ? json : (json.data || []);
                } catch { return []; }
            }));
            results.flat().forEach(p => { if (p.year) byYear[p.year] = (byYear[p.year] || 0) + 1; });
        }

        if (this[vKey] !== version) return;
        if (loadingEl) loadingEl.style.display = 'none';

        const years = Object.keys(byYear).sort();
        chartManager.createBarChart(canvasId, years, years.map(y => byYear[y]), 'Citations', { horizontal: false });

        ['png', 'jpg', 'pdf'].forEach(fmt => {
            const btn = document.getElementById(`dl-${dlPrefix}-${fmt}`);
            if (btn) btn.onclick = () => this._downloadAuthorChart(canvasId, 'Citations per Year', fmt);
        });
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
        document.getElementById('influence-view-row').addEventListener('change', () => {
            if (this._lastInfluenceRenderArgs) {
                this._renderInfluenceChart(...this._lastInfluenceRenderArgs);
            }
        });

        this._setupToggleGroup('influence-window-toggle', (windowKey) => {
            this._influenceWindow = windowKey;
            if (this._lastInfluenceRenderArgs) {
                this._renderInfluenceChart(...this._lastInfluenceRenderArgs);
            }
        }, 'data-window');

        this._setupToggleGroup('influence-y-toggle', (val) => {
            this._influenceYZero = val === 'zero';
            if (this._lastInfluenceRenderArgs) {
                this._renderInfluenceChart(...this._lastInfluenceRenderArgs);
            }
        }, 'data-y');
        document.getElementById('inf-dl-png').onclick = () => this._downloadInfluence('png');
        document.getElementById('inf-dl-jpg').onclick = () => this._downloadInfluence('jpg');
        document.getElementById('inf-dl-pdf').onclick = () => this._downloadInfluence('pdf');
        document.getElementById('inf-dl-csv').onclick = () => this._downloadInfluenceCSV();

        this._setupRangeControls('influence',
            { xMin: '_influenceXMin', xMax: '_influenceXMax', yMin: '_influenceYMin', yMax: '_influenceYMax' },
            () => { if (this._lastInfluenceRenderArgs) this._renderInfluenceChart(...this._lastInfluenceRenderArgs); }
        );
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

            // Only papers actually published in the selected journal affect its IF.
            const seedsInJournal = seedPapers.filter(p => this._paperInJournal(p, journalData));

            // Load the papers file for this journal — it contains exact citations_by_year
            // (cy field) for the top 2000 papers, computed from our local DB.
            // This avoids any iCite API calls and covers all citations with no cap.
            let papersData = null;
            try {
                papersData = await dataLoader.loadPapers(slug);
            } catch (e) { /* papers file may not exist yet for this journal */ }

            const localCyMap = {};  // pmid_str → {year_str: count}
            if (papersData?.papers) {
                for (const p of papersData.papers) {
                    if (p.cy) localCyMap[String(p.pmid)] = p.cy;
                }
            }

            // For in-journal seeds without local data, fall back to iCite cited_by fetch
            const needsIcite = seedsInJournal.filter(p => !localCyMap[String(p.pmid)]);
            let citingPapers = [], totalCitedBy = 0;
            if (needsIcite.length) {
                const allCitedBy = [...new Set(needsIcite.flatMap(p => p.cited_by || []).map(String))];
                totalCitedBy = allCitedBy.length;
                if (totalCitedBy > 0) {
                    hint.textContent = `Fetching citing papers for ${needsIcite.length} PMID(s) not in local data…`;
                    citingPapers = await this._fetchICiteBatch(allCitedBy.slice(0, 2000));
                }
            }

            this._lastInfluenceRenderArgs = [journalData, seedPapers, seedsInJournal, localCyMap, citingPapers, totalCitedBy, papersData];
            this._renderInfluenceChart(...this._lastInfluenceRenderArgs);

            hint.style.display = 'none';
            results.style.display = '';
        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('Influence error:', e);
        }
    }

    // Fuzzy journal name match: normalise and check containment
    _paperInJournal(paper, journalData) {
        if (!paper?.journal) return false;
        const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
        const pj = norm(paper.journal);
        const jj = norm(journalData.journal || '');
        if (jj.length > 3 && (pj === jj || pj.includes(jj) || jj.includes(pj))) return true;
        // Also check against the NLM abbreviation (iCite returns abbreviated names)
        const idx = this.journals.find(j => j.slug === journalData.slug);
        if (idx?.abbreviation) {
            const aj = norm(idx.abbreviation);
            if (aj.length > 3 && (pj === aj || pj.includes(aj) || aj.includes(pj))) return true;
        }
        return false;
    }

    // Helper: compute the censored IF timeseries for a given set of in-journal seeds.
    // localSeeds: seeds with exact cy data; iciteSeeds: seeds using iCite cited_by fallback.
    // Each iCite seed contributes independently (additive, handles multi-seed double-counting).
    // windowKey: which timeseries variant is selected (determines paper window bounds).
    _computeAdjIf(ts, localSeeds, iciteSeeds, citingPapers, localCyMap, windowKey) {
        // Paper window parameters: (months, skip) matching backend compute_rolling_if
        const windowParams = {
            'timeseries':      { pw: 24, skip: 0 },
            'timeseries_12mo': { pw: 12, skip: 0 },
            'timeseries_5yr':  { pw: 60, skip: 12 },
        };
        const { pw, skip } = windowParams[windowKey] || windowParams['timeseries'];
        return ts.map(point => {
            const [y, m] = point.month.split('-').map(Number);
            const endOrd = y * 12 + m;
            const startOrd = endOrd - 11;
            // Paper window: pw months ending (1+skip) months before citation window start
            const paperWindowEnd = startOrd - 1 - skip;
            const paperWindowStart = paperWindowEnd - pw + 1;

            // Part 1: local DB seeds — exact year counts, proportional month allocation
            // Only subtract citations for seeds published in the paper window,
            // since only those papers' citations are included in the IF numerator.
            let localSeedCits = 0;
            for (const p of localSeeds) {
                const pOrd = (p.year || 0) * 12 + 6;
                if (pOrd < paperWindowStart || pOrd > paperWindowEnd) continue;
                const cy = localCyMap[String(p.pmid)];
                for (const [yearStr, cnt] of Object.entries(cy)) {
                    const k = parseInt(yearStr, 10);
                    const kStart = k * 12 + 1;
                    const kEnd   = k * 12 + 12;
                    const overlapMonths = Math.max(0,
                        Math.min(kEnd, endOrd) - Math.max(kStart, startOrd) + 1);
                    localSeedCits += cnt * overlapMonths / 12;
                }
            }

            // Part 2: iCite seeds — compute each seed's contribution independently,
            // then sum. Only for seeds published in the paper window.
            let iciteSeedCits = 0;
            for (const seed of iciteSeeds) {
                const sOrd = (seed.year || 0) * 12 + 6;
                if (sOrd < paperWindowStart || sOrd > paperWindowEnd) continue;
                const seedCitedBySet = new Set((seed.cited_by || []).map(String));
                const seedInSample = citingPapers.filter(p => seedCitedBySet.has(String(p.pmid)));
                const seedTotal = seedCitedBySet.size;
                const seedScale = (seedTotal > 0 && seedInSample.length > 0 && seedInSample.length < seedTotal)
                    ? seedTotal / seedInSample.length : 1;
                let count = 0;
                for (const p of seedInSample) {
                    const ord = (p.year || 0) * 12 + 6;
                    if (ord >= startOrd && ord <= endOrd) count++;
                }
                iciteSeedCits += count * seedScale;
            }

            // Denominator: subtract in-journal research seeds published in paper window
            const allSeeds = [...localSeeds, ...iciteSeeds];
            let seedResearchInWindow = 0;
            for (const s of allSeeds) {
                if (s.is_research_article === 'Yes' || s.is_research_article === true) {
                    const ord = (s.year || 0) * 12 + 6;
                    if (ord >= paperWindowStart && ord <= paperWindowEnd) seedResearchInWindow++;
                }
            }

            const citations = point.citations || 0;
            const research = point.research || point.papers || 1;
            return Math.max(0, citations - localSeedCits - iciteSeedCits) /
                   Math.max(1, research - seedResearchInWindow);
        });
    }

    // Helper: compute raw citation count attributed to a single seed per month.
    // Returns array of numbers (one per timeseries point).
    _computeSeedCitCount(ts, seed, citingPapers, localCyMap, windowKey) {
        const windowParams = {
            'timeseries':      { pw: 24, skip: 0 },
            'timeseries_12mo': { pw: 12, skip: 0 },
            'timeseries_5yr':  { pw: 60, skip: 12 },
        };
        const { pw, skip } = windowParams[windowKey] || windowParams['timeseries'];
        const pmidStr = String(seed.pmid);
        const cy = localCyMap[pmidStr];
        return ts.map(point => {
            const [y, m] = point.month.split('-').map(Number);
            const endOrd = y * 12 + m;
            const startOrd = endOrd - 11;
            const paperWindowEnd = startOrd - 1 - skip;
            const paperWindowStart = paperWindowEnd - pw + 1;
            const pOrd = (seed.year || 0) * 12 + 6;
            if (pOrd < paperWindowStart || pOrd > paperWindowEnd) return 0;

            if (cy) {
                // Local DB: proportional month allocation from year-level data
                let cits = 0;
                for (const [yearStr, cnt] of Object.entries(cy)) {
                    const k = parseInt(yearStr, 10);
                    const kStart = k * 12 + 1;
                    const kEnd = k * 12 + 12;
                    const overlapMonths = Math.max(0,
                        Math.min(kEnd, endOrd) - Math.max(kStart, startOrd) + 1);
                    cits += cnt * overlapMonths / 12;
                }
                return cits;
            } else {
                // iCite fallback
                const seedCitedBySet = new Set((seed.cited_by || []).map(String));
                const seedInSample = citingPapers.filter(p => seedCitedBySet.has(String(p.pmid)));
                const seedTotal = seedCitedBySet.size;
                const seedScale = (seedTotal > 0 && seedInSample.length > 0 && seedInSample.length < seedTotal)
                    ? seedTotal / seedInSample.length : 1;
                let count = 0;
                for (const p of seedInSample) {
                    const ord = (p.year || 0) * 12 + 6;
                    if (ord >= startOrd && ord <= endOrd) count++;
                }
                return count * seedScale;
            }
        });
    }

    _renderInfluenceChart(journalData, seedPapers, seedsInJournal, localCyMap, citingPapers, totalCitedBy, papersData) {
        // Paper info card
        const listEl = document.getElementById('inf-paper-list');
        listEl.innerHTML = seedPapers.map(p => {
            const inJ = seedsInJournal.some(s => s.pmid === p.pmid);
            const badge = inJ ? '' : ` <span class="inf-not-in-journal">not in ${journalData.journal}</span>`;
            return `
            <div class="inf-paper-row">
                <div class="inf-paper-title">${p.title || 'Unknown title'}${badge}</div>
                <div class="inf-paper-meta">${this._fmtAuthors(p.authors)} · ${p.journal || ''} · ${p.year || ''} · ${(p.citation_count || 0).toLocaleString()} citations ·
                    <a href="https://pubmed.ncbi.nlm.nih.gov/${p.pmid}/" class="pubmed-link" target="_blank" rel="noopener">PMID ${p.pmid}</a>
                </div>
            </div>`;
        }).join('<hr class="inf-paper-divider">');

        const windowKey = this._influenceWindow || 'timeseries';
        const ts = journalData[windowKey] || journalData.timeseries || [];
        if (!ts.length) return;

        const windowLabels = {
            'timeseries': '24-Month',
            'timeseries_12mo': '12-Month',
            'timeseries_5yr': '5-Year (yr 2–6)',
        };
        const windowLabel = windowLabels[windowKey] || '24-Month';

        const localSeeds = seedsInJournal.filter(p => localCyMap[String(p.pmid)]);
        const iciteSeeds = seedsInJournal.filter(p => !localCyMap[String(p.pmid)]);

        // Combined adjIf — all seeds removed together (used for metrics always)
        const adjIf = this._computeAdjIf(ts, localSeeds, iciteSeeds, citingPapers, localCyMap, windowKey);

        // Metric cards (always based on combined)
        const totalLocalCits = localSeeds.reduce((s, p) => {
            const cy = localCyMap[String(p.pmid)];
            return s + Object.values(cy).reduce((a, b) => a + b, 0);
        }, 0);
        const totalIciteCits = iciteSeeds.reduce((s, p) => s + (p.citation_count || 0), 0);
        const totalCitations = totalLocalCits + totalIciteCits;
        const contributions = ts.map((pt, i) => Math.max(0, (pt.rolling_if || 0) - adjIf[i]));
        const maxContrib = Math.max(...contributions);
        const peakIdx = contributions.indexOf(maxContrib);
        const peakMonth = ts[peakIdx]?.month || '—';
        const meanContrib = contributions.reduce((s, v) => s + v, 0) / contributions.length;
        const dataNote = localSeeds.length > 0
            ? ` (${localSeeds.length} exact${iciteSeeds.length > 0 ? `, ${iciteSeeds.length} via iCite` : ''})`
            : iciteSeeds.length > 0 ? ' (via iCite — rerun compute_snapshots for exact data)' : '';

        // Format rolling window date range for a snapshot month
        const winParams = { 'timeseries': { pw: 24, skip: 0 }, 'timeseries_12mo': { pw: 12, skip: 0 }, 'timeseries_5yr': { pw: 60, skip: 12 } };
        const { pw: fmtPw, skip: fmtSkip } = winParams[windowKey] || winParams['timeseries'];
        const fmtWindow = (monthStr) => {
            const [y, m] = monthStr.split('-').map(Number);
            // Citation window: 12 months ending at target
            const citStart = new Date(y, m - 13, 1);
            const end = new Date(y, m - 1, 1);
            // Paper window: pw months ending (1+skip) months before citation start
            const papEnd = new Date(y, m - 13 - fmtSkip, 1);
            const papStart = new Date(y, m - 13 - fmtSkip - fmtPw + 1, 1);
            const fmt = d => d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
            return { papers: `${fmt(papStart)}–${fmt(papEnd)}`, citations: `${fmt(citStart)}–${fmt(end)}` };
        };
        const peakWin = peakMonth !== '—' ? fmtWindow(peakMonth) : null;
        const firstMonth = ts[0]?.month || '—';
        const lastMonth = ts[ts.length - 1]?.month || '—';

        document.getElementById('influence-metrics').innerHTML = [
            [totalCitations.toLocaleString() + dataNote, 'Total citations (all years, in-journal only)'],
            [maxContrib.toFixed(3), `Peak citation rate boost` +
                (peakWin ? `<br><span class="metric-sublabel">Papers: ${peakWin.papers}<br>Citations: ${peakWin.citations}</span>` : '')],
            [meanContrib.toFixed(3), `Avg. citation rate contribution` +
                `<br><span class="metric-sublabel">${firstMonth} to ${lastMonth}</span>`],
        ].map(([v, l]) =>
            `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
        ).join('');

        // Build chart datasets based on view mode
        const viewMode = document.querySelector('input[name="inf-view"]:checked')?.value || 'combined';
        const isCensored = true; // Always show censored line
        const labels = ts.map(d => d.month);

        const originalDataset = {
            label: 'Original Rolling Citation Rate',
            data: ts.map(d => d.rolling_if),
            borderColor: chartManager.palette[0],
            backgroundColor: 'rgba(0, 114, 178, 0.08)',
            borderWidth: 2.5,
            tension: 0.3,
            fill: true,
            pointRadius: 0,
            pointHoverRadius: 5,
        };

        let datasets;
        if (viewMode === 'individual' && seedsInJournal.length >= 1) {
            // Stacked bands: each PMID's contribution layered from censored IF up toward original IF
            // Compute each seed's standalone contribution
            const contribs = seedsInJournal.map(seed => {
                const sl = localCyMap[String(seed.pmid)] ? [seed] : [];
                const si = localCyMap[String(seed.pmid)] ? [] : [seed];
                const singleAdjIf = this._computeAdjIf(ts, sl, si, citingPapers, localCyMap, windowKey);
                return ts.map((pt, i) => Math.max(0, (pt.rolling_if || 0) - singleAdjIf[i]));
            });

            // Build cumulative layers starting from the combined censored baseline
            const cumLayers = [];
            for (let s = 0; s < seedsInJournal.length; s++) {
                const prev = s === 0 ? adjIf : cumLayers[s - 1];
                cumLayers.push(prev.map((v, i) => v + contribs[s][i]));
            }

            // Individual view: Dataset 0 = Original IF, Dataset 1 = Censored baseline, Datasets 2+ = PMID layers
            // Include censored dataset with gray border so first PMID layer shows border
            const censoredDataset = {
                label: 'Individual PMID borders',
                data: adjIf,
                borderColor: chartManager.palette[7],
                backgroundColor: 'transparent',
                borderWidth: 1.5,
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 0,
                hidden: false,
            };

            const layerDatasets = seedsInJournal.map((seed, idx) => {
                const palIdx = (idx + 2) % chartManager.palette.length;
                const color = chartManager.palette[palIdx];
                // Convert hex to rgba for background with 0.25 alpha
                const r = parseInt(color.slice(1, 3), 16);
                const g = parseInt(color.slice(3, 5), 16);
                const b = parseInt(color.slice(5, 7), 16);
                const bgColor = `rgba(${r}, ${g}, ${b}, 0.25)`;
                // fill down to previous layer: dataset 1 (censored) for first, or idx+1 for subsequent
                const fillTarget = idx === 0 ? 1 : idx + 1;
                return {
                    label: `PMID ${seed.pmid}`,
                    data: cumLayers[idx],
                    borderColor: chartManager.palette[7],
                    backgroundColor: bgColor,
                    borderWidth: 1.5,
                    tension: 0.3,
                    fill: fillTarget,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                };
            });

            datasets = [originalDataset, censoredDataset, ...layerDatasets];
        } else {
            // Combined: one censored line = IF with all seeds removed together
            const censorLabel = seedsInJournal.length === 1
                ? `PMID ${seedsInJournal[0].pmid}` : `${seedsInJournal.length} PMIDs`;
            datasets = [
                originalDataset,
                {
                    label: `Censored Rolling Citation Rate (without ${censorLabel})`,
                    data: adjIf,
                    borderColor: chartManager.palette[1],
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    tension: 0.3,
                    fill: false,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    hidden: !isCensored,
                },
            ];
        }

        // Populate X selects and build scale overrides
        this._populateXRangeSelects('influence', labels);
        const infScaleOverrides = this._buildScaleOverrides(
            this._influenceXMin, this._influenceXMax, this._influenceYMin, this._influenceYMax
        );

        chartManager._destroy('influence-chart');
        const ctx = document.getElementById('influence-chart');
        chartManager.charts['influence-chart'] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: {
                        display: true,
                        text: `${journalData.journal} — Rolling ${windowLabel} Citation Rate`,
                        font: { size: 14 },
                    },
                    legend: { 
                        position: 'bottom',
                        onClick: null  // Disable legend clicks - users should control PMIDs via input
                    },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                if (viewMode === 'individual' && ctx.datasetIndex >= 2) {
                                    // Show per-PMID contribution (layer value minus previous layer)
                                    const prevDs = ctx.chart.data.datasets[ctx.datasetIndex - 1];
                                    const prevVal = prevDs ? prevDs.data[ctx.dataIndex] : 0;
                                    const contrib = ctx.parsed.y - prevVal;
                                    return `${ctx.dataset.label}: +${contrib.toFixed(3)}`;
                                }
                                return `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(3)}`;
                            },
                            afterBody: (items) => {
                                if (viewMode !== 'combined') return '';
                                const orig = items.find(i => i.datasetIndex === 0);
                                const cens = items.find(i => i.datasetIndex === 1);
                                if (orig && cens && !cens.dataset.hidden) {
                                    return `Contribution: +${(orig.parsed.y - cens.parsed.y).toFixed(3)}`;
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
                        ...(infScaleOverrides.x || {}),
                    },
                    y: {
                        title: { display: true, text: `Citation Rate (${windowLabel})` },
                        beginAtZero: this._influenceYZero,
                        ...(infScaleOverrides.y || {}),
                    },
                },
            },
        });
        document.getElementById('influence-range-controls').style.display = '';

        // --- Shared: compute yearly citation totals from papers data ---
        const countContainer = document.getElementById('influence-count-container');
        const monthlyContainer = document.getElementById('influence-monthly-container');
        const allPapers = papersData?.papers || [];

        // Build year-level totals for the whole journal and per-seed
        const yearTotals = {};  // year → total citations received that year (all papers)
        for (const p of allPapers) {
            if (!p.cy) continue;
            for (const [yr, cnt] of Object.entries(p.cy)) {
                yearTotals[yr] = (yearTotals[yr] || 0) + cnt;
            }
        }
        const seedYearTotals = seedsInJournal.map(seed => {
            const cy = localCyMap[String(seed.pmid)];
            return cy || {};
        });

        if (seedsInJournal.length > 0 && isCensored && allPapers.length > 0) {

            // --- Monthly Citation Count Chart (actual per-calendar-month counts) ---
            const journalMonthlyCits = papersData?.monthly_cits || {};
            const hasMonthlyData = Object.keys(journalMonthlyCits).length > 0;

            if (hasMonthlyData) {
                monthlyContainer.style.display = '';

                // Build per-seed cm maps
                const seedCmMaps = seedsInJournal.map(seed => {
                    const pmidStr = String(seed.pmid);
                    // Try cm from papers data first
                    for (const p of allPapers) {
                        if (p.pmid === seed.pmid || String(p.pmid) === pmidStr) {
                            return p.cm || {};
                        }
                    }
                    return {};
                });

                const monthlyTotal = labels.map(lbl => journalMonthlyCits[lbl] || 0);
                const monthlySeed = seedsInJournal.map((seed, s) =>
                    labels.map(lbl => seedCmMaps[s][lbl] || 0)
                );

                const monthlyDatasets = [{
                    label: 'Total Citations',
                    data: monthlyTotal,
                    borderColor: chartManager.palette[0],
                    backgroundColor: 'rgba(0, 114, 178, 0.08)',
                    borderWidth: 2.5,
                    tension: 0.3,
                    fill: true,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                }];

                const monthlyInvLayers = [];
                for (let s = 0; s < seedsInJournal.length; s++) {
                    const prev = s === 0 ? monthlyTotal : monthlyInvLayers[s - 1];
                    monthlyInvLayers.push(prev.map((v, i) => Math.max(0, v - monthlySeed[s][i])));
                }

                seedsInJournal.forEach((seed, idx) => {
                    const palIdx = (idx + 2) % chartManager.palette.length;
                    const color = chartManager.palette[palIdx];
                    const fillTarget = idx === 0 ? 0 : idx;
                    monthlyDatasets.push({
                        label: `PMID ${seed.pmid}`,
                        data: monthlyInvLayers[idx],
                        borderColor: color,
                        backgroundColor: color + '30',
                        borderWidth: 1.5,
                        tension: 0.3,
                        fill: fillTarget,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                    });
                });

                chartManager._destroy('influence-monthly-chart');
                const ctxM = document.getElementById('influence-monthly-chart');
                chartManager.charts['influence-monthly-chart'] = new Chart(ctxM, {
                    type: 'line',
                    data: { labels, datasets: monthlyDatasets },
                    options: {
                        responsive: true,
                        interaction: { intersect: false, mode: 'index' },
                        plugins: {
                            title: {
                                display: true,
                                text: `${journalData.journal} — Monthly Citation Count`,
                                font: { size: 14 },
                            },
                            legend: { position: 'bottom' },
                            tooltip: {
                                callbacks: {
                                    label: (ctx) => {
                                        if (ctx.datasetIndex === 0) {
                                            return `${ctx.dataset.label}: ${Math.round(ctx.parsed.y).toLocaleString()}`;
                                        }
                                        const fillTarget = ctx.datasetIndex === 1 ? 0 : ctx.datasetIndex - 1;
                                        const prevVal = ctx.chart.data.datasets[fillTarget].data[ctx.dataIndex];
                                        const cits = Math.round(prevVal - ctx.parsed.y);
                                        return `${ctx.dataset.label}: ${cits.toLocaleString()} citations`;
                                    },
                                },
                            },
                        },
                        scales: {
                            x: {
                                title: { display: true, text: 'Month' },
                                ticks: { maxTicksLimit: 12 },
                                ...(infScaleOverrides.x || {}),
                            },
                            y: {
                                title: { display: true, text: 'Citations per Month' },
                                beginAtZero: true,
                            },
                        },
                    },
                });
            } else {
                monthlyContainer.style.display = 'none';
                chartManager._destroy('influence-monthly-chart');
            }
        } else {
            monthlyContainer.style.display = 'none';
            chartManager._destroy('influence-monthly-chart');
        }
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

    _downloadInfluenceCSV() {
        const chart = chartManager.charts['influence-chart'];
        if (!chart) return;
        const slug = this._influenceJournalSlug || 'journal';
        const labels = chart.data.labels;
        const datasets = chart.data.datasets;
        const header = ['Month', ...datasets.map(ds => `"${ds.label.replace(/"/g, '""')}"`)].join(',');
        const rows = labels.map((m, i) => {
            const vals = datasets.map(ds => {
                const v = ds.data[i];
                return v != null ? v : '';
            });
            return [m, ...vals].join(',');
        });
        const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `influence-${slug}.csv`;
        a.click();
        URL.revokeObjectURL(url);
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
        document.getElementById('author-pmid-paste-btn').addEventListener('click', () => this.loadFromPastedPMIDs());

    }

    async loadFromNCBIUrl() {
        const val = document.getElementById('author-ncbi-input').value.trim();
        if (!val) return;

        const hint = document.getElementById('author-search-hint');
        const results = document.getElementById('author-search-results');
        hint.style.display = '';
        results.style.display = 'none';

        try {
            hint.textContent = 'Fetching NCBI bibliography…';

            // Strip trailing params to get clean base URL for pagination
            const baseUrl = val.replace(/[?#].*$/, '').replace(/\/+$/, '');

            const fetchProxy = async (url, json) => {
                const resp = await fetch(url, { signal: AbortSignal.timeout(15000) });
                if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
                const body = json ? (await resp.json()).contents : await resp.text();
                if (!body || !body.includes('pubmed')) throw new Error('no pubmed links');
                return body;
            };

            const fetchPage = (pageNum) => {
                const pageUrl = pageNum === 1 ? val : `${baseUrl}?page=${pageNum}`;
                const enc = encodeURIComponent(pageUrl);
                return Promise.any([
                    fetchProxy(`https://corsproxy.io/?url=${enc}`, false),
                    fetchProxy(`https://api.allorigins.win/get?url=${enc}`, true),
                    fetchProxy(`https://api.codetabs.com/v1/proxy?quest=${enc}`, false),
                ]);
            };

            const extractPmids = html =>
                [...html.matchAll(/\/pubmed\/(\d+)/g)].map(m => m[1]);

            const page1Html = await fetchPage(1).catch(() => {
                throw new Error('All CORS proxies failed — try again in a moment.');
            });

            const page1Pmids = extractPmids(page1Html);
            if (!page1Pmids.length) {
                hint.textContent = 'No PMIDs found. Make sure the bibliography is set to public.';
                return;
            }

            const allPmids = new Set(page1Pmids);

            // Try to detect the total publication count from page 1 HTML
            const totalMatch =
                page1Html.match(/\((\d[\d,]*)\s+publications?\)/i) ||
                page1Html.match(/(\d[\d,]*)\s+publications?/i) ||
                page1Html.match(/"count"\s*:\s*(\d+)/i) ||
                page1Html.match(/result_count[^>]*>\s*(\d[\d,]+)/i);
            const totalCount = totalMatch
                ? parseInt(totalMatch[1].replace(/,/g, ''), 10) : null;

            const PAGE_SIZE = page1Pmids.length || 50;
            const MAX_PAGES = 40;

            if (totalCount && totalCount > allPmids.size) {
                const pagesNeeded = Math.min(Math.ceil(totalCount / PAGE_SIZE), MAX_PAGES);
                hint.textContent = `${totalCount} papers found — loading ${pagesNeeded} pages…`;

                // Fetch remaining pages in batches of 3
                for (let p = 2; p <= pagesNeeded; p += 3) {
                    const batch = [p, p + 1, p + 2].filter(n => n <= pagesNeeded);
                    hint.textContent = `Loading pages ${batch[0]}–${batch[batch.length - 1]} of ${pagesNeeded}… (${allPmids.size} papers so far)`;
                    const batchHtml = await Promise.all(
                        batch.map(n => fetchPage(n).then(extractPmids).catch(() => []))
                    );
                    batchHtml.flat().forEach(id => allPmids.add(id));
                }
            } else if (!totalCount && page1Pmids.length >= PAGE_SIZE) {
                // Unknown total, but page is full — loop until empty
                hint.textContent = `${page1Pmids.length} papers on page 1, checking for more…`;
                for (let p = 2; p <= MAX_PAGES; p++) {
                    const pageHtml = await fetchPage(p).catch(() => null);
                    if (!pageHtml) break;
                    const pids = extractPmids(pageHtml);
                    if (!pids.length) break;
                    const before = allPmids.size;
                    pids.forEach(id => allPmids.add(id));
                    if (allPmids.size === before) break;
                    hint.textContent = `${allPmids.size} papers loaded (page ${p})…`;
                }
            }

            const pmids = [...allPmids];
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

    async loadFromPastedPMIDs() {
        const val = document.getElementById('author-pmid-paste').value.trim();
        if (!val) return;

        const hint = document.getElementById('author-search-hint');
        const results = document.getElementById('author-search-results');
        hint.style.display = '';
        results.style.display = 'none';

        const pmids = [...new Set(
            val.split(/[\s,;]+/).map(s => s.replace(/\D/g, '')).filter(Boolean)
        )];

        if (!pmids.length) {
            hint.textContent = 'No valid PMIDs found. Enter one PMID per line or comma-separated.';
            return;
        }

        try {
            hint.textContent = `Found ${pmids.length} PMIDs. Fetching citation data…`;
            const papers = await this._fetchICiteBatch(pmids);

            if (!papers.length) {
                hint.textContent = 'No iCite data found for these PMIDs.';
                return;
            }

            this._authorTotalFound = pmids.length;
            hint.style.display = 'none';
            results.style.display = '';
            this._renderAuthorSearchResults(papers, pmids.length);

        } catch (e) {
            hint.textContent = `Error: ${e.message}`;
            console.error('PMID paste load error:', e);
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
                const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(name)}[Author]&retmax=2000&retmode=json&tool=IMPACT&email=impact-tool@umich.edu`;
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
        this._authorActiveTypes = null;
        this._authorTotalFound = totalFound;
        this._setupTypeFilters(papers);
        this._renderAuthorPapersTable();
        this._refreshAuthorMetrics();
    }

    _setupTypeFilters(papers) {
        const typeSet = new Set();
        papers.forEach(p => (p.pub_types || []).forEach(t => typeSet.add(t)));
        const container = document.getElementById('author-type-filters');
        if (!typeSet.size) { container.style.display = 'none'; return; }

        const allTypes = [...typeSet].sort();
        const render = () => {
            container.innerHTML = '<span class="filter-label">Article type:</span> ' +
                ['All', ...allTypes].map(t => {
                    const isAll = t === 'All';
                    const active = isAll ? !this._authorActiveTypes : this._authorActiveTypes?.has(t);
                    return `<button class="type-pill${active ? ' active' : ''}" data-type="${t}">${t}</button>`;
                }).join('');
            container.querySelectorAll('.type-pill').forEach(btn => {
                btn.addEventListener('click', () => {
                    const t = btn.dataset.type;
                    if (t === 'All') {
                        this._authorActiveTypes = null;
                    } else {
                        if (!this._authorActiveTypes) this._authorActiveTypes = new Set(allTypes);
                        if (this._authorActiveTypes.has(t)) {
                            this._authorActiveTypes.delete(t);
                            if (!this._authorActiveTypes.size) this._authorActiveTypes = null;
                        } else {
                            this._authorActiveTypes.add(t);
                        }
                    }
                    render();
                    this._refreshAuthorMetrics();
                });
            });
        };
        render();
        container.style.display = '';
    }

    _refreshAuthorMetrics() {
        // Filter by excluded checkboxes AND by active article type toggles
        const active = this._authorAllPapers.filter(p => {
            if (this._authorExcluded.has(String(p.pmid))) return false;
            if (this._authorActiveTypes) {
                const types = p.pub_types || [];
                if (!types.some(t => this._authorActiveTypes.has(t))) return false;
            }
            return true;
        });
        const excluded = this._authorAllPapers.length - active.length;
        const totalCitations = active.reduce((s, p) => s + (p.citation_count || 0), 0);
        const hIndex = this._computeHIndex(active.map(p => p.citation_count || 0));

        const excTag = excluded
            ? ` <span style="font-size:.7em;color:#c0392b;font-weight:normal">(−${excluded} excluded)</span>` : '';
        const totalOnPubmed = this._authorTotalFound > this._authorAllPapers.length
            ? ` <span style="font-size:.72em;color:#888;font-weight:normal">of ${this._authorTotalFound.toLocaleString()} on PubMed</span>` : '';
        const tipText = 'Based only on PubMed-indexed citing articles. Google Scholar casts a wider net (preprints, books, non-indexed journals), so its h-index is typically higher.';
        document.getElementById('author-search-metrics').innerHTML = [
            [`${active.length.toLocaleString()}${excTag}${totalOnPubmed}`, 'Papers Included'],
            [totalCitations.toLocaleString(), 'Total Citations'],
            [hIndex, `h-index (est.) <span class="metric-info" data-tooltip="${tipText}">ⓘ</span>`],
        ].map(([v, l]) =>
            `<div class="metric-card"><span class="metric-value">${v}</span><span class="metric-label">${l}</span></div>`
        ).join('');

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

        // Wire journal bar click → pubs/year drill-down
        const jChart = chartManager.charts['author-journals-chart'];
        if (jChart) {
            jChart.options.onClick = (event, elements) => {
                if (!elements.length) return;
                this._showJournalDrill(topJ[elements[0].index][0]);
            };
            jChart.options.plugins.tooltip = jChart.options.plugins.tooltip || {};
            jChart.update('none');
        }

        // Wire download buttons
        const wireChart = (prefix, canvasId, title) => {
            ['png', 'jpg', 'pdf'].forEach(fmt => {
                const btn = document.getElementById(`dl-${prefix}-${fmt}`);
                if (btn) btn.onclick = () => this._downloadAuthorChart(canvasId, title, fmt);
            });
        };
        wireChart('pubs', 'author-pubs-chart', 'Publications per Year');
        wireChart('cits', 'author-cits-chart', 'Citations by Publication Year');
        wireChart('journals', 'author-journals-chart', 'Top Journals');

        this._computeReceivedCitsByYear(active);

        // Close drill-down
        const closeBtn = document.getElementById('author-journal-drill-close');
        if (closeBtn) closeBtn.onclick = () => {
            document.getElementById('author-journal-drill').style.display = 'none';
        };
    }

    _downloadAuthorChart(canvasId, title, format) {
        const chart = chartManager.charts[canvasId];
        if (!chart) return;
        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            doc.setFontSize(11);
            doc.text(`IMPACT — ${title}`, 10, 10);
            doc.addImage(chart.toBase64Image('image/png', 1), 'PNG', 10, 16, imgW, imgH);
            doc.save(`impact-${canvasId}.pdf`);
            return;
        }
        const mime = format === 'jpg' ? 'image/jpeg' : 'image/png';
        const a = document.createElement('a');
        a.href = chart.toBase64Image(mime, 1);
        a.download = `impact-${canvasId}.${format}`;
        a.click();
    }

    _showJournalDrill(journalName) {
        const active = this._authorAllPapers.filter(p => {
            if (this._authorExcluded.has(String(p.pmid))) return false;
            if (this._authorActiveTypes) {
                const types = p.pub_types || [];
                if (!types.some(t => this._authorActiveTypes.has(t))) return false;
            }
            return p.journal === journalName;
        });
        const pubsByYear = {};
        active.forEach(p => { if (p.year) pubsByYear[p.year] = (pubsByYear[p.year] || 0) + 1; });
        const sortedYears = Object.keys(pubsByYear).sort();

        const esc = s => String(s).replace(/</g, '&lt;');
        document.getElementById('author-journal-drill-title').textContent =
            `${journalName} — Publications per Year`;
        document.getElementById('author-journal-drill').style.display = '';
        chartManager.createBarChart('author-journal-drill-chart', sortedYears,
            sortedYears.map(y => pubsByYear[y]), 'Papers', { horizontal: false });
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

        // Reset range state
        this._geoTrendXMin = null; this._geoTrendXMax = null;
        this._geoTrendYMin = null; this._geoTrendYMax = null;
        const gxMin = document.getElementById('geo-trend-x-min');
        const gxMax = document.getElementById('geo-trend-x-max');
        const gyMin = document.getElementById('geo-trend-y-min');
        const gyMax = document.getElementById('geo-trend-y-max');
        if (gxMin) gxMin.value = '';
        if (gxMax) gxMax.value = '';
        if (gyMin) gyMin.value = '';
        if (gyMax) gyMax.value = '';

        try {
            if (!this.papersDataCache[`${slug}__geo`]) {
                const d = await dataLoader.loadPapers(slug);
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

            // World map
            this._renderGeoMap(totals);

            // Trend chart: stacked bars by year, top 10 countries
            this._geoTrendData = { normalizedGeo, years, top10 };
            this._renderGeoTrendChart();

            // Wire up range controls (only once per load)
            this._populateGeoYearSelects(years);
            document.getElementById('geo-trend-range-controls').style.display = '';
            this._setupRangeControls('geo-trend',
                { xMin: '_geoTrendXMin', xMax: '_geoTrendXMax', yMin: '_geoTrendYMin', yMax: '_geoTrendYMax' },
                () => this._renderGeoTrendChart()
            );

            // Top countries overall (horizontal bar)
            const topOverall = Object.entries(totals).sort((a, b) => b[1] - a[1]).slice(0, 15);
            chartManager.createBarChart('geo-top-chart',
                topOverall.map(x => x[0]), topOverall.map(x => x[1]), 'Papers');

            // Recent years (horizontal bar)
            chartManager.createBarChart('geo-recent-chart',
                topRecent.map(x => x[0]), topRecent.map(x => x[1]), 'Papers');

            // Show download bars and wire up buttons
            this._geoSlug = slug;
            const journalName = (this.journals.find(j => j.slug === slug) || {}).name || slug;
            this._geoJournalName = journalName;
            ['geo-map', 'geo-trend', 'geo-top', 'geo-recent'].forEach(id => {
                const bar = document.getElementById(`${id}-download-bar`);
                if (bar) bar.style.display = '';
                ['png', 'jpg', 'pdf'].forEach(fmt => {
                    const btn = document.getElementById(`${id}-dl-${fmt}`);
                    if (btn) btn.onclick = () => this._downloadGeo(id, fmt);
                });
            });

        } catch (e) {
            hint.textContent = 'Geographic data not yet available for this journal.';
            hint.style.display = '';
            console.error('Geo error:', e);
        }
    }

    async _renderGeoMap(geoTotals) {
        const container = document.getElementById('geo-map');
        if (!container || typeof d3 === 'undefined' || typeof topojson === 'undefined') return;
        container.innerHTML = '';

        // Country name → [longitude, latitude] centroid
        const CENTROIDS = {
            'USA': [-98.5, 38.5], 'Canada': [-96.5, 60.0], 'Mexico': [-102.5, 23.6],
            'Brazil': [-51.9, -14.2], 'Argentina': [-63.6, -38.4], 'Chile': [-71.5, -35.7],
            'Colombia': [-74.3, 4.6], 'Peru': [-75.0, -9.2], 'Venezuela': [-66.6, 6.4],
            'Ecuador': [-78.5, -1.8], 'Cuba': [-79.5, 21.5], 'Uruguay': [-55.8, -32.8],
            'Bolivia': [-64.7, -16.7], 'Paraguay': [-58.4, -23.4],
            'United Kingdom': [-1.5, 52.4], 'France': [2.2, 46.2], 'Germany': [10.5, 51.2],
            'Italy': [12.6, 42.5], 'Spain': [-3.7, 40.4], 'Netherlands': [5.3, 52.1],
            'Belgium': [4.5, 50.5], 'Switzerland': [8.2, 46.8], 'Austria': [14.6, 47.7],
            'Sweden': [18.6, 59.3], 'Norway': [8.5, 60.5], 'Denmark': [9.5, 56.3],
            'Finland': [25.7, 61.9], 'Portugal': [-8.2, 39.6], 'Ireland': [-8.2, 53.2],
            'Poland': [19.1, 52.0], 'Czech Republic': [15.5, 49.8], 'Hungary': [19.5, 47.2],
            'Romania': [24.9, 45.9], 'Bulgaria': [25.5, 42.7], 'Slovakia': [19.7, 48.7],
            'Croatia': [15.2, 45.1], 'Serbia': [21.0, 44.0], 'Slovenia': [14.8, 46.1],
            'Greece': [21.8, 39.1], 'Ukraine': [31.2, 49.0], 'Russia': [60.0, 60.0],
            'Turkey': [35.2, 39.0], 'Belarus': [28.0, 53.7], 'Luxembourg': [6.1, 49.8],
            'Estonia': [25.0, 58.6], 'Latvia': [24.6, 56.9], 'Lithuania': [23.9, 56.0],
            'Iceland': [-19.0, 64.9], 'Albania': [20.2, 41.2], 'Cyprus': [33.4, 35.1],
            'Bosnia and Herzegovina': [17.8, 44.0], 'North Macedonia': [21.7, 41.6],
            'Moldova': [28.4, 47.4], 'Kazakhstan': [66.9, 48.0], 'Georgia': [43.4, 42.3],
            'Azerbaijan': [47.6, 40.1], 'Armenia': [45.0, 40.1], 'Uzbekistan': [63.9, 41.4],
            'China': [104.2, 35.9], 'Japan': [138.3, 36.2], 'South Korea': [127.8, 36.5],
            'India': [78.9, 20.6], 'Taiwan': [120.9, 23.7], 'Singapore': [103.8, 1.4],
            'Hong Kong': [114.2, 22.3], 'Israel': [34.9, 31.5], 'Iran': [53.7, 32.4],
            'Saudi Arabia': [45.1, 23.9], 'Pakistan': [69.3, 30.4], 'Bangladesh': [90.4, 23.7],
            'Malaysia': [109.7, 4.2], 'Thailand': [100.9, 15.9], 'Indonesia': [113.9, -0.8],
            'Philippines': [122.9, 12.9], 'Vietnam': [108.3, 14.1], 'Sri Lanka': [80.8, 7.9],
            'Nepal': [84.1, 28.4], 'Myanmar': [96.5, 19.2], 'Mongolia': [103.8, 46.9],
            'United Arab Emirates': [53.8, 23.4], 'Jordan': [36.2, 31.2],
            'Lebanon': [35.5, 33.9], 'Kuwait': [47.5, 29.3], 'Qatar': [51.2, 25.4],
            'Bahrain': [50.6, 26.2], 'Iraq': [43.7, 33.2], 'Oman': [57.5, 21.5],
            'Egypt': [30.8, 26.8], 'Morocco': [-7.1, 31.8], 'Tunisia': [9.5, 34.0],
            'Algeria': [2.6, 28.0], 'Libya': [17.2, 26.3], 'Ethiopia': [40.5, 9.1],
            'South Africa': [25.1, -29.0], 'Nigeria': [8.7, 9.1], 'Kenya': [37.9, 0.0],
            'Ghana': [-1.0, 7.9], 'Tanzania': [35.0, -6.4], 'Uganda': [32.3, 1.4],
            'Cameroon': [12.4, 3.9], 'Sudan': [30.2, 15.6],
            'Australia': [133.8, -25.7], 'New Zealand': [172.5, -41.5],
            'North Korea': [127.5, 40.3], 'Cambodia': [104.9, 12.6],
        };

        // Load and cache world topology from CDN
        if (!this._worldTopology) {
            try {
                const r = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json');
                if (!r.ok) throw new Error('fetch failed');
                this._worldTopology = await r.json();
            } catch (e) {
                container.innerHTML = '<p class="data-note" style="padding:1rem;">Map unavailable (network error).</p>';
                return;
            }
        }

        const W = 960, H = 500;
        const projection = d3.geoMercator()
            .scale(W / (2 * Math.PI) * 0.92)
            .translate([W / 2, H / 1.58]);
        const path = d3.geoPath().projection(projection);

        const svg = d3.select(container)
            .append('svg')
            .attr('viewBox', `0 0 ${W} ${H}`)
            .attr('preserveAspectRatio', 'xMidYMid meet')
            .style('width', '100%')
            .style('height', 'auto')
            .style('display', 'block');

        svg.append('rect').attr('width', W).attr('height', H).attr('fill', '#daeaf5');

        const worldFeatures = topojson.feature(this._worldTopology, this._worldTopology.objects.countries);
        svg.append('g').selectAll('path')
            .data(worldFeatures.features)
            .join('path')
            .attr('d', path)
            .attr('fill', '#c8d8e2')
            .attr('stroke', '#a0b4be')
            .attr('stroke-width', 0.4);

        // Sort largest first so smaller circles render on top
        const entries = Object.entries(geoTotals)
            .filter(([c]) => CENTROIDS[c])
            .sort((a, b) => b[1] - a[1]);

        if (!entries.length) {
            container.innerHTML = '<p class="data-note" style="padding:1rem;">No mappable location data.</p>';
            return;
        }

        const maxN = entries[0][1];
        const rScale = d3.scaleSqrt().domain([0, maxN]).range([0, 38]);

        const tip = d3.select(container)
            .append('div')
            .style('position', 'absolute')
            .style('background', 'rgba(15,30,50,0.9)')
            .style('color', '#fff')
            .style('padding', '5px 10px')
            .style('border-radius', '4px')
            .style('font-size', '0.82rem')
            .style('pointer-events', 'none')
            .style('white-space', 'nowrap')
            .style('opacity', 0)
            .style('z-index', 10);

        svg.append('g').selectAll('circle')
            .data(entries)
            .join('circle')
            .attr('cx', ([c]) => { const pt = projection(CENTROIDS[c]); return pt ? pt[0] : -9999; })
            .attr('cy', ([c]) => { const pt = projection(CENTROIDS[c]); return pt ? pt[1] : -9999; })
            .attr('r', ([, n]) => Math.max(3, rScale(n)))
            .attr('fill', '#0072B2')
            .attr('fill-opacity', 0.55)
            .attr('stroke', '#004e80')
            .attr('stroke-width', 0.7)
            .style('cursor', 'default')
            .on('mouseover', function(event, [c, n]) {
                d3.select(this).attr('fill-opacity', 0.85);
                tip.html(`<strong>${c}</strong><br>${n.toLocaleString()} papers`)
                    .style('opacity', 1);
            })
            .on('mousemove', function(event) {
                const rect = container.getBoundingClientRect();
                tip.style('left', (event.clientX - rect.left + 12) + 'px')
                   .style('top',  (event.clientY - rect.top  - 36) + 'px');
            })
            .on('mouseout', function() {
                d3.select(this).attr('fill-opacity', 0.55);
                tip.style('opacity', 0);
            });
    }

    // ---- Geography Downloads ----

    _renderGeoTrendChart() {
        if (!this._geoTrendData) return;
        const { normalizedGeo, years, top10 } = this._geoTrendData;

        // Filter years by range
        const minYr = this._geoTrendXMin || years[0];
        const maxYr = this._geoTrendXMax || years[years.length - 1];

        // Build full year range from 2005 to max data year
        const lastYr = years[years.length - 1];
        const allYears = [];
        for (let y = 2005; y <= parseInt(lastYr); y++) allYears.push(String(y));

        const filtered = allYears.filter(yr => yr >= minYr && yr <= maxYr);

        const trendDatasets = top10.map((country, i) => ({
            label: country,
            data: filtered.map(yr => (normalizedGeo[yr] && normalizedGeo[yr][country]) || 0),
            backgroundColor: chartManager.palette[i % chartManager.palette.length] + 'cc',
            borderColor: chartManager.palette[i % chartManager.palette.length],
            borderWidth: 1,
        }));
        chartManager.createStackedBarChart('geo-trend-chart', filtered, trendDatasets, 'Papers');

        // Apply Y-axis overrides
        const chart = chartManager.charts['geo-trend-chart'];
        if (chart) {
            if (this._geoTrendYMin != null) chart.options.scales.y.min = this._geoTrendYMin;
            else delete chart.options.scales.y.min;
            if (this._geoTrendYMax != null) chart.options.scales.y.max = this._geoTrendYMax;
            else delete chart.options.scales.y.max;
            chart.update();
        }
    }

    _populateGeoYearSelects(dataYears) {
        const xMinSel = document.getElementById('geo-trend-x-min');
        const xMaxSel = document.getElementById('geo-trend-x-max');
        if (!xMinSel || !xMaxSel) return;

        const prevMin = xMinSel.value;
        const prevMax = xMaxSel.value;

        const lastYr = dataYears.length ? parseInt(dataYears[dataYears.length - 1]) : 2025;
        const allYears = [];
        for (let y = 2005; y <= lastYr; y++) allYears.push(String(y));

        [xMinSel, xMaxSel].forEach((sel, idx) => {
            sel.innerHTML = '';
            const blank = document.createElement('option');
            blank.value = '';
            blank.textContent = idx === 0 ? '(start)' : '(end)';
            sel.appendChild(blank);
            allYears.forEach(yr => {
                const opt = document.createElement('option');
                opt.value = yr;
                opt.textContent = yr;
                sel.appendChild(opt);
            });
        });

        if (prevMin && allYears.includes(prevMin)) xMinSel.value = prevMin;
        if (prevMax && allYears.includes(prevMax)) xMaxSel.value = prevMax;
    }

    _downloadGeo(chartId, format) {
        const slug = this._geoSlug || 'journal';
        const filename = `geo-${chartId.replace('geo-', '')}-${slug}`;

        if (chartId === 'geo-map') {
            this._downloadGeoMapImage(filename, format);
            return;
        }

        const canvasId = `${chartId}-chart`;
        const chart = chartManager.charts[canvasId];
        if (!chart) return;

        if (format === 'pdf') {
            if (!window.jspdf) return;
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
            const pw = doc.internal.pageSize.getWidth();
            const ph = doc.internal.pageSize.getHeight();
            const imgW = pw - 20;
            const imgH = Math.min(imgW * (chart.height / chart.width), ph - 28);
            doc.setFontSize(11);
            doc.text(`IMPACT — Geography: ${this._geoJournalName || slug}`, 10, 10);
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

    _downloadGeoMapImage(filename, format) {
        const container = document.getElementById('geo-map');
        const svgEl = container && container.querySelector('svg');
        if (!svgEl) return;

        const svgClone = svgEl.cloneNode(true);
        svgClone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
        const serializer = new XMLSerializer();
        const svgStr = serializer.serializeToString(svgClone);
        const svgBlob = new Blob([svgStr], { type: 'image/svg+xml;charset=utf-8' });
        const url = URL.createObjectURL(svgBlob);

        const img = new Image();
        const W = 1920, H = 1000;
        img.onload = () => {
            const canvas = document.createElement('canvas');
            canvas.width = W;
            canvas.height = H;
            const ctx = canvas.getContext('2d');
            if (format === 'jpg') {
                ctx.fillStyle = '#ffffff';
                ctx.fillRect(0, 0, W, H);
            }
            ctx.drawImage(img, 0, 0, W, H);
            URL.revokeObjectURL(url);

            if (format === 'pdf') {
                if (!window.jspdf) return;
                const { jsPDF } = window.jspdf;
                const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
                const pw = doc.internal.pageSize.getWidth();
                const ph = doc.internal.pageSize.getHeight();
                const imgW = pw - 20;
                const imgH = Math.min(imgW * (H / W), ph - 28);
                doc.setFontSize(11);
                doc.text(`IMPACT — Geography: ${this._geoJournalName || 'journal'}`, 10, 10);
                doc.addImage(canvas.toDataURL('image/png'), 'PNG', 10, 16, imgW, imgH);
                doc.save(`${filename}.pdf`);
                return;
            }
            const mime = format === 'jpg' ? 'image/jpeg' : 'image/png';
            const a = document.createElement('a');
            a.href = canvas.toDataURL(mime, 0.95);
            a.download = `${filename}.${format}`;
            a.click();
        };
        img.src = url;
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
            const d = await dataLoader.loadAuthor(slug);
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
            })),
            [
                { key: 'name', label: 'Journal' },
                { key: 'abbr', label: 'Abbreviation' },
                { key: 'issn', label: 'ISSN' },
                { key: 'latest_if', label: 'Citation Rate', format: UIHelpers.formatIF },
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
