/**
 * IMPACT Chart Manager
 * Wraps Chart.js for consistent chart creation and styling.
 */

class ChartManager {
    constructor() {
        this.charts = {};
        this.palette = [
            '#1a5276', '#27ae60', '#e74c3c', '#f39c12', '#8e44ad',
            '#2e86c1', '#16a085', '#c0392b', '#d35400', '#2c3e50'
        ];
    }

    /**
     * Create a journal citation rate time series chart.
     * rateLabel: label shown on the legend (e.g. 'Citation Rate (Research)')
     */
    createJournalChart(canvasId, timeseries, officialJif, rateLabel = 'Citation Rate') {
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
                        beginAtZero: false,
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
     */
    createCompositionChart(canvasId, timeseries) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        const labels = data.map(d => d.month);

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Research Articles',
                        data: data.map(d => d.research),
                        borderColor: this.palette[0],
                        backgroundColor: 'rgba(26, 82, 118, 0.4)',
                        borderWidth: 1.5,
                        tension: 0.3,
                        fill: true,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                    },
                    {
                        label: 'Reviews',
                        data: data.map(d => d.reviews),
                        borderColor: this.palette[4],
                        backgroundColor: 'rgba(142, 68, 173, 0.4)',
                        borderWidth: 1.5,
                        tension: 0.3,
                        fill: true,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                    }
                ]
            },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Paper Composition (24-mo Window)', font: { size: 14 } },
                    legend: { position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            afterBody: (items) => {
                                if (items.length >= 2) {
                                    const total = items.reduce((sum, i) => sum + i.parsed.y, 0);
                                    const reviewPct = total > 0 ? (items[1].parsed.y / total * 100).toFixed(1) : 0;
                                    return `\nTotal: ${total}  |  Review %: ${reviewPct}%`;
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
    createComparisonChart(canvasId, journalsData, metric = 'rolling_if', windowKey = 'timeseries') {
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
                    },
                    y: {
                        title: { display: true, text: yLabel },
                        beginAtZero: metric === 'citations' || metric === 'papers',
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
    createMultiSeriesChart(canvasId, series) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Build a unified sorted month axis across all series
        const allMonths = [...new Set(series.flatMap(s => s.months))].sort();

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
                    },
                    y: {
                        title: { display: true, text: 'Citation Rate' },
                        beginAtZero: false,
                    }
                }
            }
        });
    }

    /**
     * Create a horizontal bar chart (for country/institution/author breakdowns).
     * labels: string[]  values: number[]
     */
    createBarChart(canvasId, labels, values, valueLabel = 'Papers') {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: labels.map((_, i) => this.palette[i % this.palette.length] + 'cc'),
                    borderColor: labels.map((_, i) => this.palette[i % this.palette.length]),
                    borderWidth: 1,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => ` ${ctx.parsed.x.toLocaleString()} ${valueLabel}`
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, title: { display: true, text: valueLabel } },
                    y: { ticks: { font: { size: 11 }, maxTicksLimit: 20 } }
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

    _destroy(canvasId) {
        if (this.charts[canvasId]) {
            this.charts[canvasId].destroy();
            delete this.charts[canvasId];
        }
    }
}

const chartManager = new ChartManager();
