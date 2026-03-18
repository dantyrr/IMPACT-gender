/**
 * Chart manager for gender analysis dashboard.
 * Creates and manages Chart.js instances.
 */
const PAIR_COLORS = {
    WW: '#882255',
    WM: '#CC6677',
    MW: '#44AA99',
    MM: '#332288',
};

const PAIR_LABELS = {
    WW: 'Woman FA + Woman LA',
    WM: 'Woman FA + Man LA',
    MW: 'Man FA + Woman LA',
    MM: 'Man FA + Man LA',
};

const PAIRS = ['WW', 'WM', 'MW', 'MM'];

const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            labels: { color: '#e6edf3', font: { size: 12 } },
        },
        tooltip: {
            backgroundColor: '#21262d',
            titleColor: '#e6edf3',
            bodyColor: '#e6edf3',
            borderColor: '#30363d',
            borderWidth: 1,
        },
    },
    scales: {
        x: {
            ticks: { color: '#8b949e' },
            grid: { color: '#21262d' },
        },
        y: {
            ticks: { color: '#8b949e' },
            grid: { color: '#21262d' },
        },
    },
};

const GenderChartManager = {
    _charts: {},

    _destroy(id) {
        if (this._charts[id]) {
            this._charts[id].destroy();
            delete this._charts[id];
        }
    },

    /**
     * Stacked area: gender pair composition over time (% of papers).
     */
    compositionChart(canvasId, compositionData) {
        this._destroy(canvasId);
        const years = Object.keys(compositionData).sort();
        if (years.length === 0) return;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: years.map(y => compositionData[y][pair]?.pct || 0),
            backgroundColor: PAIR_COLORS[pair] + 'CC',
            borderColor: PAIR_COLORS[pair],
            borderWidth: 1,
            fill: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: years, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}%`,
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        stacked: true,
                        max: 100,
                        title: { display: true, text: '% of papers', color: '#8b949e' },
                    },
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        stacked: true,
                    },
                },
            },
        });
    },

    /**
     * Grouped line chart: citation rate by gender pair over time.
     */
    citationRateChart(canvasId, rateData) {
        this._destroy(canvasId);
        const years = Object.keys(rateData).sort();
        // Skip last 2 years (incomplete citation data)
        const displayYears = years.slice(0, -2);
        if (displayYears.length === 0) return;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: displayYears.map(y => rateData[y]?.[pair]?.r || null),
            borderColor: PAIR_COLORS[pair],
            backgroundColor: PAIR_COLORS[pair] + '33',
            borderWidth: 2,
            pointRadius: 2,
            tension: 0.3,
            spanGaps: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: displayYears, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => {
                                const year = ctx.label;
                                const pair = PAIRS[ctx.datasetIndex];
                                const d = rateData[year]?.[pair];
                                if (!d) return '';
                                return `${ctx.dataset.label}: ${d.r.toFixed(2)} (${d.p} papers, ${d.c} cites)`;
                            },
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Citations per paper', color: '#8b949e' },
                    },
                },
            },
        });
    },

    /**
     * Rolling 24-month citation rate by gender pair (JIF-style).
     */
    rollingIfChart(canvasId, rollingData) {
        this._destroy(canvasId);
        const keys = Object.keys(rollingData).sort();
        if (keys.length === 0) return;

        // Monthly keys like "2008-01"; show only January labels on x-axis
        const isMonthly = keys[0].length > 4;
        const labels = isMonthly
            ? keys.map(k => k.endsWith('-01') ? k.slice(0, 4) : '')
            : keys;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: keys.map(k => rollingData[k]?.[pair]?.['if'] || null),
            borderColor: PAIR_COLORS[pair],
            backgroundColor: PAIR_COLORS[pair] + '33',
            borderWidth: 2,
            pointRadius: isMonthly ? 0 : 3,
            pointHoverRadius: 4,
            tension: 0.3,
            spanGaps: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            title: (items) => keys[items[0].dataIndex],
                            label: (ctx) => {
                                const key = keys[ctx.dataIndex];
                                const pair = PAIRS[ctx.datasetIndex];
                                const d = rollingData[key]?.[pair];
                                if (!d) return '';
                                return `${ctx.dataset.label}: ${d['if'].toFixed(2)} (${d.p.toLocaleString()} papers, ${d.c.toLocaleString()} cites)`;
                            },
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        ticks: {
                            ...CHART_DEFAULTS.scales.x.ticks,
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: isMonthly ? 20 : undefined,
                        },
                    },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Rolling citation rate', color: '#8b949e' },
                    },
                },
            },
        });
    },

    /**
     * Normalized rolling IF (MM = 1.0 baseline).
     */
    rollingIfNormChart(canvasId, rollingData) {
        this._destroy(canvasId);
        const keys = Object.keys(rollingData).sort();
        if (keys.length === 0) return;

        const isMonthly = keys[0].length > 4;
        const labels = isMonthly
            ? keys.map(k => k.endsWith('-01') ? k.slice(0, 4) : '')
            : keys;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: keys.map(k => rollingData[k]?.[pair]?.norm || null),
            borderColor: PAIR_COLORS[pair],
            borderWidth: 2,
            pointRadius: isMonthly ? 0 : 2,
            pointHoverRadius: 4,
            tension: 0.3,
            borderDash: pair === 'MM' ? [5, 5] : [],
            spanGaps: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            title: (items) => keys[items[0].dataIndex],
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(3) || 'N/A'}x`,
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        ticks: {
                            ...CHART_DEFAULTS.scales.x.ticks,
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: isMonthly ? 20 : undefined,
                        },
                    },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Relative to MM', color: '#8b949e' },
                    },
                },
            },
        });
    },

    /**
     * Normalized citation rate (MM = 1.0 baseline).
     */
    normalizedRateChart(canvasId, rateData) {
        this._destroy(canvasId);
        const years = Object.keys(rateData).sort().slice(0, -2);
        if (years.length === 0) return;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: years.map(y => rateData[y]?.[pair]?.norm || null),
            borderColor: PAIR_COLORS[pair],
            borderWidth: 2,
            pointRadius: 2,
            tension: 0.3,
            borderDash: pair === 'MM' ? [5, 5] : [],
            spanGaps: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: years, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(3) || 'N/A'}x`,
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Relative to MM', color: '#8b949e' },
                    },
                },
            },
        });
    },

    /**
     * Horizontal bar: citing gender analysis.
     */
    citingChart(canvasId, citingData) {
        this._destroy(canvasId);
        if (!citingData || Object.keys(citingData).length === 0) return;

        const labels = PAIRS.map(p => `Papers by ${p}`);
        const wPcts = PAIRS.map(p => citingData[p]?.pctW || 0);
        const mPcts = PAIRS.map(p => citingData[p]?.pctM || 0);

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Cited by woman FA',
                        data: wPcts,
                        backgroundColor: '#882255CC',
                        borderColor: '#882255',
                        borderWidth: 1,
                    },
                    {
                        label: 'Cited by man FA',
                        data: mPcts,
                        backgroundColor: '#332288CC',
                        borderColor: '#332288',
                        borderWidth: 1,
                    },
                ],
            },
            options: {
                ...CHART_DEFAULTS,
                indexAxis: 'y',
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.x.toFixed(1)}%`,
                        },
                    },
                },
                scales: {
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        stacked: true,
                        max: 100,
                        title: { display: true, text: '% of citing papers', color: '#8b949e' },
                    },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        stacked: true,
                    },
                },
            },
        });
    },

    /**
     * Horizontal bar: classification rate by country.
     */
    qualityCountryChart(canvasId, countryData) {
        this._destroy(canvasId);
        if (!countryData) return;

        // Sort by assignment rate
        const sorted = Object.entries(countryData)
            .sort((a, b) => b[1].pctAssigned - a[1].pctAssigned);

        const labels = sorted.map(([c]) => c);
        const assigned = sorted.map(([, d]) => d.pctAssigned);

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: '% names classified',
                    data: assigned,
                    backgroundColor: assigned.map(v =>
                        v >= 70 ? '#44AA99CC' : v >= 40 ? '#DDCC77CC' : '#CC6677CC'
                    ),
                    borderWidth: 0,
                }],
            },
            options: {
                ...CHART_DEFAULTS,
                indexAxis: 'y',
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    legend: { display: false },
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            afterLabel: (ctx) => {
                                const country = labels[ctx.dataIndex];
                                const d = countryData[country];
                                return `W: ${d.W.toLocaleString()} | M: ${d.M.toLocaleString()} | Unknown: ${d.U.toLocaleString()}`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        max: 100,
                        title: { display: true, text: '% classified (W + M)', color: '#8b949e' },
                    },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        ticks: { font: { size: 11 } },
                    },
                },
            },
        });
    },

    /**
     * Doughnut: overall classification breakdown.
     */
    qualityOverallChart(canvasId, overallData) {
        this._destroy(canvasId);
        if (!overallData) return;

        const w = overallData.W?.n || 0;
        const m = overallData.M?.n || 0;
        const u = overallData.U?.n || 0;
        const nullCount = overallData.NULL?.n || 0;

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Woman', 'Man', 'Unknown', 'No forename'],
                datasets: [{
                    data: [w, m, u, nullCount],
                    backgroundColor: ['#882255', '#332288', '#888888', '#444444'],
                    borderWidth: 0,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        labels: { color: '#e6edf3', font: { size: 12 } },
                    },
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => {
                                const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                                const pct = (ctx.parsed / total * 100).toFixed(1);
                                return `${ctx.label}: ${ctx.parsed.toLocaleString()} (${pct}%)`;
                            },
                        },
                    },
                },
            },
        });
    },

    /**
     * Journal-level composition chart (stacked bar by year).
     */
    journalCompositionChart(canvasId, yearlyData) {
        this._destroy(canvasId);
        const years = Object.keys(yearlyData).sort();
        if (years.length === 0) return;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: years.map(y => {
                const total = PAIRS.reduce((s, p) => s + (yearlyData[y]?.[p]?.p || 0), 0);
                const val = yearlyData[y]?.[pair]?.p || 0;
                return total > 0 ? (val / total * 100) : 0;
            }),
            backgroundColor: PAIR_COLORS[pair] + 'CC',
            borderColor: PAIR_COLORS[pair],
            borderWidth: 1,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: { labels: years, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => {
                                const year = ctx.label;
                                const pair = PAIRS[ctx.datasetIndex];
                                const d = yearlyData[year]?.[pair];
                                if (!d) return '';
                                return `${PAIR_LABELS[pair]}: ${d.p} papers (${ctx.parsed.y.toFixed(1)}%)`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ...CHART_DEFAULTS.scales.x, stacked: true },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        stacked: true,
                        max: 100,
                        title: { display: true, text: '% of papers', color: '#8b949e' },
                    },
                },
            },
        });
    },

    /**
     * Journal-level citation rate chart.
     */
    journalRateChart(canvasId, yearlyData) {
        this._destroy(canvasId);
        const years = Object.keys(yearlyData).sort().slice(0, -2);
        if (years.length === 0) return;

        const datasets = PAIRS.map(pair => ({
            label: PAIR_LABELS[pair],
            data: years.map(y => yearlyData[y]?.[pair]?.r || null),
            borderColor: PAIR_COLORS[pair],
            backgroundColor: PAIR_COLORS[pair] + '33',
            borderWidth: 2,
            pointRadius: 3,
            tension: 0.3,
            spanGaps: true,
        }));

        const ctx = document.getElementById(canvasId).getContext('2d');
        this._charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: years, datasets },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: (ctx) => {
                                const year = ctx.label;
                                const pair = PAIRS[ctx.datasetIndex];
                                const d = yearlyData[year]?.[pair];
                                if (!d) return '';
                                return `${PAIR_LABELS[pair]}: ${d.r.toFixed(2)} (${d.p} papers, ${d.c} cites)`;
                            },
                        },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Citations per paper', color: '#8b949e' },
                    },
                },
            },
        });
    },
};
