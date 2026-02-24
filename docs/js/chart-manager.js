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
     * Create a journal IF time series chart with mode switching.
     * mode: 'both' | 'all' | 'no-reviews'
     */
    createJournalChart(canvasId, timeseries, officialJif, mode = 'both') {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Filter out months with zero data at the start
        const startIdx = timeseries.findIndex(d => d.papers > 0);
        const data = startIdx >= 0 ? timeseries.slice(startIdx) : timeseries;
        const labels = data.map(d => d.month);

        const datasets = [];

        if (mode === 'both' || mode === 'all') {
            datasets.push({
                label: 'Citation Rate (all articles)',
                data: data.map(d => d.rolling_if),
                borderColor: this.palette[0],
                backgroundColor: 'rgba(26, 82, 118, 0.08)',
                borderWidth: 2.5,
                tension: 0.3,
                fill: mode === 'all',
                pointRadius: 0,
                pointHoverRadius: 5,
            });
        }

        if (mode === 'both' || mode === 'no-reviews') {
            datasets.push({
                label: 'Citation Rate (research only)',
                data: data.map(d => d.rolling_if_no_reviews),
                borderColor: this.palette[1],
                backgroundColor: 'rgba(39, 174, 96, 0.08)',
                borderWidth: mode === 'no-reviews' ? 2.5 : 2,
                borderDash: mode === 'both' ? [6, 3] : [],
                tension: 0.3,
                fill: mode === 'no-reviews',
                pointRadius: 0,
                pointHoverRadius: 5,
            });
        }

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
                    title: { display: true, text: 'Rolling 24-Month Citation Rate', font: { size: 14 } },
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
     * metric: 'rolling_if' | 'rolling_if_no_reviews' | 'citations' | 'papers'
     */
    createComparisonChart(canvasId, journalsData, metric = 'rolling_if') {
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
            const startIdx = j.timeseries.findIndex(d => d.papers > 0);
            const data = startIdx >= 0 ? j.timeseries.slice(startIdx) : j.timeseries;
            if (data.length > longestLabels.length) {
                longestLabels = data.map(d => d.month);
            }
        });

        const datasets = journalsData.map((j, i) => {
            const startIdx = j.timeseries.findIndex(d => d.papers > 0);
            const data = startIdx >= 0 ? j.timeseries.slice(startIdx) : j.timeseries;
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
     * Create an author/PMID-level bar chart.
     */
    createAuthorChart(canvasId, papers) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Sort by journal for grouping
        const sorted = [...papers].sort((a, b) => a.journal.localeCompare(b.journal));
        const labels = sorted.map(p => `PMID ${p.pmid}`);

        // Color by journal
        const journalColors = {};
        let colorIdx = 0;
        sorted.forEach(p => {
            if (!(p.journal in journalColors)) {
                journalColors[p.journal] = this.palette[colorIdx % this.palette.length];
                colorIdx++;
            }
        });

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: 'Journal Citation Rate at Pub Date',
                    data: sorted.map(p => p.journal_if),
                    backgroundColor: sorted.map(p => journalColors[p.journal] + '99'),
                    borderColor: sorted.map(p => journalColors[p.journal]),
                    borderWidth: 1,
                }]
            },
            options: {
                responsive: true,
                indexAxis: papers.length > 10 ? 'y' : 'x',
                plugins: {
                    title: { display: true, text: 'Journal Citation Rate at Publication Date', font: { size: 14 } },
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            afterLabel: (ctx) => {
                                const p = sorted[ctx.dataIndex];
                                return `Journal: ${p.journal}\nPublished: ${p.pub_date}`;
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

    _destroy(canvasId) {
        if (this.charts[canvasId]) {
            this.charts[canvasId].destroy();
            delete this.charts[canvasId];
        }
    }
}

const chartManager = new ChartManager();
