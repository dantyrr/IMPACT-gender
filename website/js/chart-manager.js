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
     * Create a journal time series chart (rolling IF over time).
     */
    createJournalChart(canvasId, timeseries, officialJif) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        const labels = timeseries.map(d => d.month);
        const datasets = [
            {
                label: 'Rolling IF (all)',
                data: timeseries.map(d => d.rolling_if),
                borderColor: this.palette[0],
                backgroundColor: 'rgba(26, 82, 118, 0.08)',
                borderWidth: 2.5,
                tension: 0.3,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 5,
            },
            {
                label: 'Rolling IF (no reviews)',
                data: timeseries.map(d => d.rolling_if_no_reviews),
                borderColor: this.palette[1],
                borderWidth: 2,
                borderDash: [6, 3],
                tension: 0.3,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 5,
            }
        ];

        // Add official JIF reference line
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
                    title: { display: true, text: 'Rolling 24-Month Impact Factor', font: { size: 14 } },
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
                        title: { display: true, text: 'Impact Factor' },
                        beginAtZero: false,
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

        const labels = timeseries.map(d => d.month);

        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Papers (24-mo window)',
                        data: timeseries.map(d => d.papers),
                        backgroundColor: 'rgba(26, 82, 118, 0.6)',
                        yAxisID: 'y',
                    },
                    {
                        label: 'Citations',
                        data: timeseries.map(d => d.citations),
                        backgroundColor: 'rgba(39, 174, 96, 0.5)',
                        yAxisID: 'y1',
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: { display: true, text: 'Papers and Citations', font: { size: 14 } },
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
     */
    createComparisonChart(canvasId, journalsData) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        this._destroy(canvasId);

        // Use the longest timeseries for labels
        let longestLabels = [];
        journalsData.forEach(j => {
            if (j.timeseries.length > longestLabels.length) {
                longestLabels = j.timeseries.map(d => d.month);
            }
        });

        const datasets = journalsData.map((j, i) => ({
            label: j.journal,
            data: j.timeseries.map(d => d.rolling_if),
            borderColor: this.palette[i % this.palette.length],
            borderWidth: 2.5,
            tension: 0.3,
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 5,
        }));

        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: { labels: longestLabels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    title: { display: true, text: 'Journal Comparison — Rolling IF', font: { size: 14 } },
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
                        title: { display: true, text: 'Impact Factor' },
                        beginAtZero: false,
                    }
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
