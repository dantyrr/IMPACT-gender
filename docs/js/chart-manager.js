/**
 * IMPACT Chart Manager
 * Wraps Chart.js for consistent chart creation and styling.
 */

class ChartManager {
    constructor() {
        this.charts = {};
        // Okabe-Ito colorblind-safe palette (Nature Methods recommended)
        // Yellow (#F0E442) and black replaced with purple and gray for web legibility
        this.palette = [
            '#0072B2', // 1 blue
            '#D55E00', // 2 vermilion
            '#009E73', // 3 bluish green
            '#56B4E9', // 4 sky blue
            '#E69F00', // 5 orange/amber
            '#CC79A7', // 6 reddish purple
            '#7B2D8B', // 7 purple (replaces yellow)
            '#7f7f7f', // 8 gray (replaces black)
            '#44AA99', // 9 teal
            '#AA4499', // 10 mauve
        ];
    }

    /**
     * Create a journal citation rate time series chart.
     * rateLabel: label shown on the legend (e.g. 'Citation Rate (Research)')
     */
    createJournalChart(canvasId, timeseries, officialJif, rateLabel = 'Citation Rate', beginAtZero = false) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Filter out months with zero data at the start
        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        const labels = data.map(d => d.month);

        const datasets = [
            {
                label: rateLabel,
                data: data.map(d => d.rolling_if),
                borderColor: this.palette[0],
                backgroundColor: 'rgba(26, 82, 118, 0.08)',
                borderWidth: 2.5,
                tension: 0.3,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 5,
            }
        ];

        // Official JIF reference line
        if (officialJif) {
            datasets.push({
                label: `Official JIF 2024 (${officialJif})`,
                data: Array(labels.length).fill(officialJif),
                borderColor: '#e74c3c',
                borderWidth: 1.5,
                borderDash: [10, 5],
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 0,
            });
        }

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Rolling Citation Rate', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(2)}`
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                    },
                    y: {
                        title: { display: true, text: 'Citation Rate' },
                        beginAtZero,
                    }
                }
            }
        });
    }

    /**
     * Create a citation trends chart.
     * mode: 'total' | 'per-paper'
     */
    createCitationChart(canvasId, timeseries, mode = 'total') {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        const labels = data.map(d => d.month);

        const datasets = [];

        if (mode === 'total') {
            datasets.push({
                label: 'Total Citations (12-mo window)',
                data: data.map(d => d.citations),
                borderColor: this.palette[0],
                backgroundColor: 'rgba(26, 82, 118, 0.12)',
                borderWidth: 2.5,
                tension: 0.3,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 5,
            });
        } else {
            // Citations per paper
            datasets.push({
                label: 'Citations / All Papers',
                data: data.map(d => d.papers > 0 ? (d.citations / d.papers) : 0),
                borderColor: this.palette[0],
                backgroundColor: 'rgba(26, 82, 118, 0.08)',
                borderWidth: 2.5,
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 5,
            });
            datasets.push({
                label: 'Citations / Research Papers',
                data: data.map(d => d.research > 0 ? (d.citations / d.research) : 0),
                borderColor: this.palette[1],
                borderWidth: 2,
                borderDash: [6, 3],
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 5,
            });
        }

        const yLabel = mode === 'total' ? 'Citations' : 'Citations per Paper';

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: mode === 'total' ? 'Citation Volume Over Time' : 'Citation Efficiency (per Paper)', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(mode === 'total' ? 0 : 2)}`
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                    },
                    y: {
                        title: { display: true, text: yLabel },
                        beginAtZero: true,
                    }
                }
            }
        });
    }

    /**
     * Create a stacked area chart showing paper composition over time.
     * @param {string} canvasId
     * @param {Array} timeseries
     * @param {string[]} visibleTypes - which types to show (default all 5)
     */
    createCompositionChart(canvasId, timeseries, visibleTypes, scaleOverrides = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        let labels = data.map(d => d.month);
        const monthMap = new Map(data.map((d, i) => [d.month, i]));

        // Expand labels to cover scaleOverrides date range
        labels = this._expandMonthRange(labels, scaleOverrides);

        const allTypes = ['research', 'review', 'editorial', 'letter', 'other'];
        const shown = visibleTypes || allTypes;

        const typeConfig = {
            research:  { label: 'Research Articles', color: this.palette[0], bg: 'rgba(0, 114, 178, 0.4)' },
            review:    { label: 'Reviews',           color: this.palette[6], bg: 'rgba(123, 45, 139, 0.4)' },
            editorial: { label: 'Editorials',        color: this.palette[4], bg: 'rgba(230, 159, 0, 0.4)' },
            letter:    { label: 'Letters',            color: this.palette[2], bg: 'rgba(0, 158, 115, 0.4)' },
            other:     { label: 'Other',              color: this.palette[7], bg: 'rgba(127, 127, 127, 0.4)' },
        };

        const getVal = (d, type) => {
            const bt = d.by_type;
            if (!bt) {
                if (type === 'research') return d.research || 0;
                if (type === 'review') return d.reviews || 0;
                return 0;
            }
            if (type === 'other') {
                return (bt.other?.papers || 0) + (bt.guideline?.papers || 0) + (bt.case_report?.papers || 0);
            }
            return bt[type]?.papers || 0;
        };

        const getData = (type) => labels.map(m => {
            const i = monthMap.get(m);
            return i !== undefined ? getVal(data[i], type) : 0;
        });

        const datasets = allTypes
            .filter(t => shown.includes(t))
            .map(type => ({
                label: typeConfig[type].label,
                data: getData(type),
                borderColor: typeConfig[type].color,
                backgroundColor: typeConfig[type].bg,
                borderWidth: 1.5,
                tension: 0.3,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 4,
            }));

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Paper Composition (24-mo Window)', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            afterBody: (items) => {
                                if (items.length >= 1) {
                                    const total = items.reduce((sum, i) => sum + i.parsed.y, 0);
                                    return `\nTotal: ${total}`;
                                }
                                return '';
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                        ...(scaleOverrides.x || {}),
                    },
                    y: {
                        title: { display: true, text: 'Papers' },
                        stacked: true,
                        beginAtZero: true,
                    }
                }
            }
        });
    }

    /**
     * Create a multi-journal paper composition chart.
     * Single journal: stacked area (same as detail). Multiple journals: line chart with journal colors + type dashes.
     * @param {string} canvasId
     * @param {Array} journalsData - array of {journal, slug, timeseries, ...}
     * @param {Object} colorMap - {slug: color}
     * @param {string[]} visibleTypes
     * @param {string} windowKey - timeseries key
     */
    createCompareCompositionChart(canvasId, journalsData, colorMap, visibleTypes, windowKey) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;
        this._destroy(canvasId);

        const allTypes = ['research', 'review', 'editorial', 'letter', 'other'];
        const shown = visibleTypes || allTypes;

        const typeLabels = {
            research: 'Research', review: 'Reviews',
            editorial: 'Editorials', letter: 'Letters', other: 'Other',
        };
        const typeDashes = {
            research: [8, 4], review: [4, 4],
            editorial: [2, 2], letter: [8, 4, 2, 4], other: [12, 3],
        };

        const getData = (ts, type) => ts.map(d => {
            const bt = d.by_type;
            if (!bt) return type === 'research' ? (d.research || 0) : type === 'review' ? (d.reviews || 0) : 0;
            if (type === 'other') return (bt.other?.papers || 0) + (bt.guideline?.papers || 0) + (bt.case_report?.papers || 0);
            return bt[type]?.papers || 0;
        });

        if (journalsData.length === 1) {
            // Single journal: use stacked area like the detail tab
            const jData = journalsData[0];
            const raw = jData[windowKey] || jData.timeseries;
            const startIdx = raw.findIndex(d => d.papers > 0);
            const ts = startIdx >= 0 ? raw.slice(startIdx) : raw;
            const labels = ts.map(d => d.month);

            const typeConfig = {
                research:  { label: 'Research Articles', color: this.palette[0], bg: 'rgba(0, 114, 178, 0.4)' },
                review:    { label: 'Reviews',           color: this.palette[6], bg: 'rgba(123, 45, 139, 0.4)' },
                editorial: { label: 'Editorials',        color: this.palette[4], bg: 'rgba(230, 159, 0, 0.4)' },
                letter:    { label: 'Letters',            color: this.palette[2], bg: 'rgba(0, 158, 115, 0.4)' },
                other:     { label: 'Other',              color: this.palette[7], bg: 'rgba(127, 127, 127, 0.4)' },
            };

            const datasets = allTypes.filter(t => shown.includes(t)).map(type => ({
                label: typeConfig[type].label,
                data: getData(ts, type),
                borderColor: typeConfig[type].color,
                backgroundColor: typeConfig[type].bg,
                borderWidth: 1.5,
                tension: 0.3,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 4,
            }));

            this.charts[canvasId] = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    interaction: { intersect: false, mode: 'index' },
                    plugins: {
                        title: { display: true, text: `Paper Composition — ${jData.journal}`, font: { size: 14 } },
                        legend: { position: 'bottom' },
                        tooltip: {
                            callbacks: {
                                afterBody: (items) => items.length >= 1 ? `\nTotal: ${items.reduce((s, i) => s + i.parsed.y, 0)}` : '',
                            }
                        }
                    },
                    scales: {
                        x: { title: { display: true, text: 'Month' }, ticks: { maxTicksLimit: 12 } },
                        y: { title: { display: true, text: 'Papers' }, stacked: true, beginAtZero: true },
                    }
                }
            });
            return;
        }

        // Multiple journals: line chart with journal colors + type dashes
        const allMonths = [...new Set(journalsData.flatMap(j => {
            const raw = j[windowKey] || j.timeseries;
            const si = raw.findIndex(d => d.papers > 0);
            return (si >= 0 ? raw.slice(si) : raw).map(d => d.month);
        }))].sort();

        const datasets = [];
        const multiType = shown.length > 1;

        journalsData.forEach((jData, jIdx) => {
            const color = colorMap[jData.slug] || this.palette[jIdx % this.palette.length];
            const raw = jData[windowKey] || jData.timeseries;
            const startIdx = raw.findIndex(d => d.papers > 0);
            const ts = startIdx >= 0 ? raw.slice(startIdx) : raw;
            const monthMap = new Map(ts.map((d, i) => [d.month, i]));

            if (!multiType) {
                // Single type selected
                const typeKey = shown[0];
                const typeData = getData(ts, typeKey);
                const data = allMonths.map(m => { const i = monthMap.get(m); return i !== undefined ? typeData[i] : null; });
                datasets.push({
                    label: jData.journal,
                    data, borderColor: color, backgroundColor: 'transparent',
                    borderWidth: 2.5, tension: 0.3, fill: false,
                    pointRadius: 0, pointHoverRadius: 5, spanGaps: true,
                });
            } else {
                // Total papers line (solid)
                // Pre-compute per-type arrays, then sum by index
                const typeArrays = shown.map(t => getData(ts, t));
                const totalData = ts.map((_, idx) => typeArrays.reduce((sum, arr) => sum + (arr[idx] || 0), 0));
                const totalMapped = allMonths.map(m => { const i = monthMap.get(m); return i !== undefined ? totalData[i] : null; });
                datasets.push({
                    label: `${jData.journal} — Total`,
                    data: totalMapped, borderColor: color, backgroundColor: 'transparent',
                    borderWidth: 2.5, tension: 0.3, fill: false,
                    pointRadius: 0, pointHoverRadius: 5, spanGaps: true,
                });

                // Per-type lines (dashed)
                shown.forEach(typeKey => {
                    const typeData = getData(ts, typeKey);
                    const data = allMonths.map(m => { const i = monthMap.get(m); return i !== undefined ? typeData[i] : null; });
                    datasets.push({
                        label: `${jData.journal} — ${typeLabels[typeKey]}`,
                        data, borderColor: color, backgroundColor: 'transparent',
                        borderWidth: 1.5, borderDash: typeDashes[typeKey] || [],
                        tension: 0.3, fill: false,
                        pointRadius: 0, pointHoverRadius: 4, spanGaps: true,
                    });
                });
            }
        });

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: allMonths, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Paper Composition Comparison', font: { size: 14 } },
                    legend: { position: 'bottom' },
                },
                scales: {
                    x: { title: { display: true, text: 'Month' }, ticks: { maxTicksLimit: 12 } },
                    y: { title: { display: true, text: 'Papers' }, beginAtZero: true },
                }
            }
        });
    }

    /**
     * Create a papers/citations bar chart.
     */
    createPapersChart(canvasId, timeseries) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        const labels = data.map(d => d.month);

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Papers (24-mo window)',
                        data: data.map(d => d.papers),
                        backgroundColor: 'rgba(26, 82, 118, 0.6)',
                        yAxisID: 'y',
                    },
                    {
                        label: 'Citations (12-mo window)',
                        data: data.map(d => d.citations),
                        backgroundColor: 'rgba(39, 174, 96, 0.5)',
                        yAxisID: 'y1',
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: { display: true, text: 'Papers and Citations — Raw Counts', font: { size: 14 } },
                    legend: { position: 'bottom' },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 12 } },
                    y: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'Papers' },
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        title: { display: true, text: 'Citations' },
                        grid: { drawOnChartArea: false },
                    }
                }
            }
        });
    }

    /**
     * Create comparison chart (multiple journals overlaid).
     * metric:    'rolling_if' | 'rolling_if_no_reviews' | 'citations' | 'papers'
     * windowKey: 'timeseries' | 'timeseries_12mo' | 'timeseries_5yr'
     */
    createComparisonChart(canvasId, journalsData, metric = 'rolling_if', windowKey = 'timeseries', scaleOverrides = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const metricLabels = {
            rolling_if: 'Citation Rate',
            rolling_if_no_reviews: 'Citation Rate (Research Only)',
            citations: 'Citations',
            papers: 'Papers'
        };

        const yLabel = metricLabels[metric] || metric;

        // Use the longest timeseries for labels, filtering out zero-data months
        let longestLabels = [];
        journalsData.forEach(j => {
            const ts = j[windowKey] || j.timeseries;
            const startIdx = ts.findIndex(d => d.papers > 0);
            const data = startIdx >= 0 ? ts.slice(startIdx) : ts;
            if (data.length > longestLabels.length) {
                longestLabels = data.map(d => d.month);
            }
        });

        const datasets = journalsData.map((j, i) => {
            const ts = j[windowKey] || j.timeseries;
            const startIdx = ts.findIndex(d => d.papers > 0);
            const data = startIdx >= 0 ? ts.slice(startIdx) : ts;
            return {
                label: j.journal,
                data: data.map(d => d[metric]),
                borderColor: this.palette[i % this.palette.length],
                borderWidth: 2.5,
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 5,
            };
        });

        const isInteger = (metric === 'citations' || metric === 'papers');

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: longestLabels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: `Journal Comparison — ${yLabel}`, font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const val = isInteger ? ctx.parsed.y.toLocaleString() : ctx.parsed.y.toFixed(2);
                                return `${ctx.dataset.label}: ${val}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                        ...(scaleOverrides.x || {}),
                    },
                    y: {
                        title: { display: true, text: yLabel },
                        beginAtZero: metric === 'citations' || metric === 'papers',
                        ...(scaleOverrides.y || {}),
                    }
                }
            }
        });
    }

    /**
     * Create a per-paper citation chart comparing 24-mo citations to journal benchmark.
     * papers: [{pmid, year, journal, citations_24mo, journal_rate, journal_name}]
     */
    createAuthorChart(canvasId, papers) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const labels = papers.map(p => `${p.pmid} (${p.year})`);
        const datasets = [
            {
                label: 'Paper Cit/yr (2-yr window)',
                data: papers.map(p => p.citations_24mo / 2),
                backgroundColor: 'rgba(26, 82, 118, 0.7)',
                borderColor: this.palette[0],
                borderWidth: 1,
            }
        ];

        if (papers.some(p => p.journal_rate != null)) {
            datasets.push({
                label: 'Journal Rate benchmark (cit/yr)',
                data: papers.map(p => p.journal_rate),
                backgroundColor: 'rgba(39, 174, 96, 0.5)',
                borderColor: this.palette[1],
                borderWidth: 1,
            });
        }

        const horizontal = papers.length > 5;

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                responsive: true,
                indexAxis: horizontal ? 'y' : 'x',
                plugins: {
                    title: { display: true, text: 'Citations/yr (2-yr window): Paper vs Journal Benchmark', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            afterLabel: (ctx) => {
                                const p = papers[ctx.dataIndex];
                                return p.journal_name
                                    ? `Journal: ${p.journal} (matched: ${p.journal_name})`
                                    : `Journal: ${p.journal}`;
                            }
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true },
                    y: { beginAtZero: true }
                }
            }
        });
    }

    /**
     * Create a multi-series chart with per-journal colors and per-type dash patterns.
     * series: [{label, color, dash, months, values}]
     */
    createMultiSeriesChart(canvasId, series, beginAtZero = false, scaleOverrides = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Build a unified sorted month axis across all series
        let allMonths = [...new Set(series.flatMap(s => s.months))].sort();
        allMonths = this._expandMonthRange(allMonths, scaleOverrides);

        const datasets = series.map(s => {
            const monthToIdx = new Map(s.months.map((m, i) => [m, i]));
            const data = allMonths.map(m => {
                const i = monthToIdx.get(m);
                return i !== undefined ? s.values[i] : null;
            });
            return {
                label: s.label,
                data,
                borderColor: s.color,
                borderWidth: 2.5,
                borderDash: s.dash || [],
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 5,
                spanGaps: true,
            };
        });

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: allMonths, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Citation Rate Trends', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const v = ctx.parsed.y;
                                return v != null ? `${ctx.dataset.label}: ${v.toFixed(2)}` : null;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Month' },
                        ticks: { maxTicksLimit: 12 },
                        ...(scaleOverrides.x || {}),
                    },
                    y: {
                        title: { display: true, text: 'Citation Rate' },
                        beginAtZero,
                        ...(scaleOverrides.y || {}),
                    }
                }
            }
        });
    }

    /**
     * Create a horizontal bar chart (for country/institution/author breakdowns).
     * labels: string[]  values: number[]
     */
    createBarChart(canvasId, labels, values, valueLabel = 'Papers', opts = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const horizontal = opts.horizontal !== false;  // default: horizontal
        const parsedKey = horizontal ? 'x' : 'y';

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: horizontal
                        ? labels.map((_, i) => this.palette[i % this.palette.length] + 'cc')
                        : this.palette[0] + 'cc',
                    borderColor: horizontal
                        ? labels.map((_, i) => this.palette[i % this.palette.length])
                        : this.palette[0],
                    borderWidth: 1,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: horizontal ? 'y' : 'x',
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => ` ${ctx.parsed[parsedKey].toLocaleString()} ${valueLabel}`
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, title: { display: !horizontal, text: horizontal ? undefined : valueLabel } },
                    y: {
                        beginAtZero: true,
                        ticks: { font: { size: 11 }, maxTicksLimit: 20 },
                        title: { display: horizontal, text: horizontal ? valueLabel : undefined },
                    }
                }
            }
        });
    }

    /**
     * Create a stacked bar chart (for country/geo trend visualization).
     * labels: string[] (x-axis — years)
     * datasets: [{label, data, backgroundColor, borderColor, borderWidth}]
     * valueLabel: string for y-axis
     */
    createStackedBarChart(canvasId, labels, datasets, valueLabel = 'Papers') {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;
        this._destroy(canvasId);

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { font: { size: 11 }, boxWidth: 12, padding: 8 }
                    },
                    tooltip: {
                        mode: 'index',
                        callbacks: {
                            label: (ctx) => ` ${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString()}`
                        }
                    }
                },
                scales: {
                    x: { stacked: true, title: { display: true, text: 'Year' } },
                    y: { stacked: true, beginAtZero: true, title: { display: true, text: valueLabel } }
                }
            }
        });
    }

    /**
     * Expand a sorted months array to cover scaleOverrides.x min/max range,
     * and remove x.min/x.max from overrides (category scale needs actual labels).
     * Returns the expanded months array. Mutates scaleOverrides.x in place.
     */
    _expandMonthRange(months, scaleOverrides) {
        const xOvr = scaleOverrides && scaleOverrides.x;
        if (!xOvr || (!xOvr.min && !xOvr.max) || !months.length) return months;

        const first = xOvr.min || months[0];
        const last = xOvr.max || months[months.length - 1];
        const [sYr, sMo] = first.split('-').map(Number);
        const [eYr, eMo] = last.split('-').map(Number);

        const full = [];
        for (let yr = sYr; yr <= eYr; yr++) {
            const m0 = yr === sYr ? sMo : 1;
            const m1 = yr === eYr ? eMo : 12;
            for (let mo = m0; mo <= m1; mo++) {
                full.push(`${yr}-${String(mo).padStart(2, '0')}`);
            }
        }

        // Remove min/max from x overrides — labels now cover the range
        delete xOvr.min;
        delete xOvr.max;
        if (Object.keys(xOvr).length === 0) delete scaleOverrides.x;

        return full;
    }

    _destroy(canvasId) {
        if (this.charts[canvasId]) {
            this.charts[canvasId].destroy();
            delete this.charts[canvasId];
        }
    }

    /**
     * Citation-by-year chart for a single paper.
     * yearCounts: {year: count, ...}
     * windowSize: 1 (annual) | 2 (2-yr rolling) | 5 (5-yr rolling)
     * journalTimeseries: array of {month: 'YYYY-MM', rolling_if: N} or null
     * journalName: string for legend label
     * pubYear: publication year (int) or null
     * pubMonth: publication month 1–12 or null
     * showJifWindow: bool — highlight JIF window (pub_year + pub_year+1) in amber
     */
    createPaperCitationChart(canvasId, yearCounts, windowSize, journalTimeseries, journalName, pubYear, pubMonth, showJifWindow) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;
        this._destroy(canvasId);

        const years = Object.keys(yearCounts).map(Number).sort((a, b) => a - b);
        const labels = years.map(String);
        const windowLabel = windowSize === 1 ? 'Citations per Year'
            : windowSize === 2 ? '2-Year Rolling Citations'
            : '5-Year Rolling Citations';

        const stacked = windowSize === 1 && pubYear && pubMonth;
        let datasets;

        if (stacked) {
            // Fraction of pub_year+2 inside the 24-month window
            const fracIn  = (pubMonth - 1) / 12;
            const fracOut = 1 - fracIn;

            if (showJifWindow) {
                // Three stacks: JIF window (amber) | 24-mo extension (teal) | outside (blue)
                datasets = [
                    {
                        type: 'bar', label: `JIF window (${pubYear}–${pubYear + 1})`,
                        data: years.map(y => (y === pubYear || y === pubYear + 1) ? (yearCounts[y] || 0) : 0),
                        backgroundColor: this.palette[4] + 'bb', borderColor: this.palette[4],
                        borderWidth: 1, stack: 'cit', yAxisID: 'y', order: 1,
                    },
                    {
                        type: 'bar', label: `24-mo extension (into ${pubYear + 2})`,
                        data: years.map(y => y === pubYear + 2 ? Math.round((yearCounts[y] || 0) * fracIn * 10) / 10 : 0),
                        backgroundColor: this.palette[2] + 'bb', borderColor: this.palette[2],
                        borderWidth: 1, stack: 'cit', yAxisID: 'y', order: 1,
                    },
                    {
                        type: 'bar', label: 'Outside 24-mo window',
                        data: years.map(y => {
                            if (y === pubYear || y === pubYear + 1) return 0;
                            if (y === pubYear + 2) return Math.round((yearCounts[y] || 0) * fracOut * 10) / 10;
                            return yearCounts[y] || 0;
                        }),
                        backgroundColor: this.palette[0] + 'aa', borderColor: this.palette[0],
                        borderWidth: 1, stack: 'cit', yAxisID: 'y', order: 1,
                    },
                ];
            } else {
                // Two stacks: in 24-mo window (green) | outside (blue)
                datasets = [
                    {
                        type: 'bar', label: 'In 24-mo window',
                        data: years.map(y => {
                            if (y === pubYear || y === pubYear + 1) return yearCounts[y] || 0;
                            if (y === pubYear + 2) return Math.round((yearCounts[y] || 0) * fracIn * 10) / 10;
                            return 0;
                        }),
                        backgroundColor: this.palette[2] + 'bb', borderColor: this.palette[2],
                        borderWidth: 1, stack: 'cit', yAxisID: 'y', order: 1,
                    },
                    {
                        type: 'bar', label: 'Outside 24-mo window',
                        data: years.map(y => {
                            if (y === pubYear || y === pubYear + 1) return 0;
                            if (y === pubYear + 2) return Math.round((yearCounts[y] || 0) * fracOut * 10) / 10;
                            return yearCounts[y] || 0;
                        }),
                        backgroundColor: this.palette[0] + 'aa', borderColor: this.palette[0],
                        borderWidth: 1, stack: 'cit', yAxisID: 'y', order: 1,
                    },
                ];
            }
        } else {
            // Non-annual or no pub date: single bar/line
            const values = years.map(y => {
                if (windowSize === 1) return yearCounts[y] || 0;
                if (windowSize === 2) return (yearCounts[y] || 0) + (yearCounts[y - 1] || 0);
                return [y, y-1, y-2, y-3, y-4].reduce((s, yr) => s + (yearCounts[yr] || 0), 0);
            });
            datasets = [{
                type: windowSize === 1 ? 'bar' : 'line',
                label: windowLabel,
                data: values,
                backgroundColor: this.palette[0] + 'aa',
                borderColor: this.palette[0],
                borderWidth: windowSize === 1 ? 1 : 2,
                fill: windowSize !== 1,
                tension: 0.3,
                pointRadius: windowSize === 1 ? 0 : 3,
                yAxisID: 'y',
                order: 1,
            }];
        }

        const scales = {
            x: {
                stacked,
                ticks: { maxRotation: 45 },
            },
            y: {
                stacked,
                beginAtZero: true,
                title: { display: true, text: windowLabel, font: { size: 11 } },
            },
        };

        if (journalTimeseries && journalTimeseries.length) {
            const jifByYear = {};
            journalTimeseries.forEach(pt => {
                const yr = parseInt((pt.month || '').split('-')[0]);
                if (yr) jifByYear[yr] = pt.rolling_if;
            });
            datasets.push({
                type: 'line',
                label: `${journalName || 'Journal'} IF (24-mo)`,
                data: labels.map(y => jifByYear[parseInt(y)] ?? null),
                borderColor: this.palette[1],
                backgroundColor: 'transparent',
                borderDash: [5, 4],
                borderWidth: 2,
                tension: 0.3,
                pointRadius: 2,
                yAxisID: 'y2',
                order: 0,
                spanGaps: true,
            });
            scales.y2 = {
                position: 'right',
                beginAtZero: true,
                grid: { drawOnChartArea: false },
                ticks: { color: this.palette[1] },
                title: { display: true, text: 'Journal IF', color: this.palette[1], font: { size: 11 } },
            };
        }

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.2,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        display: stacked || datasets.length > 1,
                        labels: { color: '#c8d8e8', boxWidth: 14 },
                    },
                    tooltip: {
                        callbacks: {
                            label: c => {
                                const v = c.parsed.y;
                                if (v == null || v === 0) return null;
                                const disp = Number.isInteger(v) ? v.toLocaleString() : v.toFixed(1);
                                return ` ${disp} — ${c.dataset.label}`;
                            },
                        },
                    },
                },
                scales,
            },
        });
    }
}

const chartManager = new ChartManager();
