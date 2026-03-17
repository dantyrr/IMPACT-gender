/**
 * Gender Citation Analysis Dashboard — Main Controller
 */
document.addEventListener('DOMContentLoaded', () => {
    const app = new GenderApp();
    app.init();
});

class GenderApp {
    constructor() {
        this.currentSection = 'overview';
        this.journalIndex = [];
        this.aggregateData = null;
    }

    async init() {
        this._setupNavigation();
        this._setupJournalSearch();

        try {
            this.aggregateData = await GenderDataLoader.loadAggregate();
            this._renderOverview();
            this._renderCiting();
            this._renderQuality();
        } catch (e) {
            console.error('Failed to load aggregate data:', e);
            document.getElementById('overview-summary').innerHTML =
                '<p style="color:#CC6677">Failed to load data. Run the analysis pipeline first.</p>';
        }

        // Try to load journal index
        try {
            this.journalIndex = await GenderDataLoader.loadJournalIndex();
        } catch (e) {
            console.warn('Could not load journal index:', e);
        }
    }

    // ── Navigation ──

    _setupNavigation() {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const section = link.dataset.section;
                this._switchSection(section);
            });
        });
    }

    _switchSection(section) {
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));

        document.getElementById(section)?.classList.add('active');
        document.querySelector(`.nav-link[data-section="${section}"]`)?.classList.add('active');
        this.currentSection = section;
    }

    // ── Overview ──

    _renderOverview() {
        const d = this.aggregateData;
        if (!d) return;

        GenderChartManager.compositionChart('composition-chart', d.composition);
        GenderChartManager.citationRateChart('citation-rate-chart', d.citation_rates);
        GenderChartManager.normalizedRateChart('normalized-rate-chart', d.citation_rates);

        // Summary
        const years = Object.keys(d.composition).sort();
        const latest = d.composition[years[years.length - 1]];
        const earliest = d.composition[years[0]];

        if (latest && earliest) {
            const wwChange = (latest.WW?.pct || 0) - (earliest.WW?.pct || 0);
            const mmChange = (latest.MM?.pct || 0) - (earliest.MM?.pct || 0);

            const rateYears = Object.keys(d.citation_rates).sort();
            // Use 3rd-to-last year for stable rates (recent years have incomplete citation data)
            const rateYear = rateYears[rateYears.length - 3];
            const rates = d.citation_rates[rateYear];

            let rateSummary = '';
            if (rates) {
                const pairs = ['WW', 'WM', 'MW', 'MM'];
                const rateStrs = pairs.map(p =>
                    `${p}: <span class="stat-highlight">${rates[p]?.r?.toFixed(2) || 'N/A'}</span>`
                ).join(' | ');
                rateSummary = `
                    <h4>Citation rates (${rateYear})</h4>
                    <p>${rateStrs} citations per paper</p>
                `;
            }

            document.getElementById('overview-summary').innerHTML = `
                <h4>Key findings</h4>
                <p>From ${years[0]} to ${years[years.length - 1]}, the proportion of
                WW papers ${wwChange > 0 ? 'increased' : 'decreased'} by
                <span class="stat-highlight">${Math.abs(wwChange).toFixed(1)}pp</span>, while
                MM papers ${mmChange > 0 ? 'increased' : 'decreased'} by
                <span class="stat-highlight">${Math.abs(mmChange).toFixed(1)}pp</span>.</p>
                <p>Latest year (${years[years.length - 1]}):
                WW = <span class="stat-highlight">${latest.WW?.pct?.toFixed(1) || 0}%</span>,
                WM = <span class="stat-highlight">${latest.WM?.pct?.toFixed(1) || 0}%</span>,
                MW = <span class="stat-highlight">${latest.MW?.pct?.toFixed(1) || 0}%</span>,
                MM = <span class="stat-highlight">${latest.MM?.pct?.toFixed(1) || 0}%</span>
                of ${latest.total?.toLocaleString() || 0} classified papers.</p>
                ${rateSummary}
            `;
        }
    }

    // ── Citing ──

    _renderCiting() {
        const d = this.aggregateData;
        if (!d?.citing_gender) return;

        GenderChartManager.citingChart('citing-chart', d.citing_gender);

        // Summary
        const citing = d.citing_gender;
        let html = '<h4>Citing gender patterns</h4>';
        for (const pair of ['WW', 'WM', 'MW', 'MM']) {
            if (citing[pair]) {
                html += `<p>Papers by <span class="pair-badge pair-${pair.toLowerCase()}">${pair}</span>
                    are cited by woman FAs <span class="stat-highlight">${citing[pair].pctW?.toFixed(1) || 'N/A'}%</span>
                    of the time (${citing[pair].total?.toLocaleString() || 0} total citations with known gender).</p>`;
            }
        }
        document.getElementById('citing-summary').innerHTML = html;
    }

    // ── Quality ──

    _renderQuality() {
        const d = this.aggregateData;
        if (!d?.inference_quality) return;

        const q = d.inference_quality;
        GenderChartManager.qualityCountryChart('quality-country-chart', q.by_country);
        GenderChartManager.qualityOverallChart('quality-overall-chart', q.overall);

        // Summary
        const overall = q.overall;
        const total = overall.total || 0;
        const wPct = overall.W?.pct || 0;
        const mPct = overall.M?.pct || 0;
        const uPct = overall.U?.pct || 0;

        let countryNote = '';
        if (q.by_country) {
            const highCountries = Object.entries(q.by_country)
                .filter(([, d]) => d.pctAssigned >= 70)
                .map(([c]) => c)
                .slice(0, 5)
                .join(', ');
            const lowCountries = Object.entries(q.by_country)
                .filter(([, d]) => d.pctAssigned < 40)
                .map(([c]) => c)
                .slice(0, 5)
                .join(', ');
            if (highCountries) {
                countryNote += `<p>Highest classification rates: ${highCountries}</p>`;
            }
            if (lowCountries) {
                countryNote += `<p>Lowest classification rates: ${lowCountries}</p>`;
            }
        }

        document.getElementById('quality-summary').innerHTML = `
            <h4>Classification summary</h4>
            <p>Of ${total.toLocaleString()} papers with extractable first names:</p>
            <p>Woman: <span class="stat-highlight">${wPct}%</span> |
               Man: <span class="stat-highlight">${mPct}%</span> |
               Unknown: <span class="stat-highlight">${uPct}%</span></p>
            ${countryNote}
        `;
    }

    // ── Journal search ──

    _setupJournalSearch() {
        const input = document.getElementById('journal-search');
        const dropdown = document.getElementById('journal-dropdown');

        input.addEventListener('input', () => {
            const query = input.value.trim().toLowerCase();
            if (query.length < 2) {
                dropdown.classList.remove('open');
                return;
            }

            const matches = this.journalIndex
                .filter(j => (j.name || '').toLowerCase().includes(query))
                .slice(0, 20);

            if (matches.length === 0) {
                dropdown.classList.remove('open');
                return;
            }

            dropdown.innerHTML = matches.map(j =>
                `<div class="journal-option" data-slug="${j.slug}">${j.name}</div>`
            ).join('');
            dropdown.classList.add('open');

            dropdown.querySelectorAll('.journal-option').forEach(opt => {
                opt.addEventListener('click', () => {
                    input.value = opt.textContent;
                    dropdown.classList.remove('open');
                    this._loadJournal(opt.dataset.slug, opt.textContent);
                });
            });
        });

        document.addEventListener('click', (e) => {
            if (!e.target.closest('.journal-picker-container')) {
                dropdown.classList.remove('open');
            }
        });
    }

    async _loadJournal(slug, name) {
        const data = await GenderDataLoader.loadJournal(slug);
        const detail = document.getElementById('journal-detail');

        if (!data || !data.yearly || Object.keys(data.yearly).length === 0) {
            detail.style.display = 'block';
            document.getElementById('journal-name').textContent = name;
            document.getElementById('journal-summary').innerHTML =
                '<p style="color:#CC6677">No gender data available for this journal.</p>';
            return;
        }

        detail.style.display = 'block';
        document.getElementById('journal-name').textContent = data.name || name;

        GenderChartManager.journalCompositionChart('journal-composition-chart', data.yearly);
        GenderChartManager.journalRateChart('journal-rate-chart', data.yearly);

        // Summary
        const years = Object.keys(data.yearly).sort();
        const latestYear = years[years.length - 1];
        const latest = data.yearly[latestYear];
        const totalPapers = Object.keys(latest).reduce((s, p) => s + (latest[p]?.p || 0), 0);

        let html = `<h4>${data.name || name} (${latestYear})</h4>`;
        html += `<p>${totalPapers} classified research papers</p>`;

        for (const pair of ['WW', 'WM', 'MW', 'MM']) {
            const d = latest[pair];
            if (d && d.p > 0) {
                const pct = (d.p / totalPapers * 100).toFixed(1);
                html += `<p><span class="pair-badge pair-${pair.toLowerCase()}">${pair}</span>
                    ${d.p} papers (${pct}%), ${d.r.toFixed(2)} cites/paper</p>`;
            }
        }

        document.getElementById('journal-summary').innerHTML = html;
    }
}
