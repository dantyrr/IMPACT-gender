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
        this.genderNames = null;

        // Author tab state
        this._authorPapers = [];
        this._authorRefData = [];
        this._authorSearchName = '';
        this._authorExcluded = new Set();  // PMIDs to exclude (unchecked papers)
    }

    async init() {
        this._setupNavigation();
        this._setupJournalSearch();
        this._setupAuthorSearch();

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

        try {
            this.journalIndex = await GenderDataLoader.loadJournalIndex();
        } catch (e) {
            console.warn('Could not load journal index:', e);
        }

        try {
            this.genderNames = await GenderDataLoader.loadGenderNames();
        } catch (e) {
            console.warn('Could not load gender names:', e);
            this.genderNames = {};
        }
    }

    // ── Navigation ──

    _setupNavigation() {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                this._switchSection(link.dataset.section);
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

        if (d.rolling_if_24m) {
            GenderChartManager.rollingIfChart('rolling-if-chart', d.rolling_if_24m);
            GenderChartManager.rollingIfNormChart('rolling-if-norm-chart', d.rolling_if_24m);
        }

        GenderChartManager.citationRateChart('citation-rate-chart', d.citation_rates);
        GenderChartManager.normalizedRateChart('normalized-rate-chart', d.citation_rates);

        const years = Object.keys(d.composition).sort();
        const latest = d.composition[years[years.length - 1]];
        const earliest = d.composition[years[0]];

        if (latest && earliest) {
            const wwChange = (latest.WW?.pct || 0) - (earliest.WW?.pct || 0);
            const mmChange = (latest.MM?.pct || 0) - (earliest.MM?.pct || 0);

            const rateYears = Object.keys(d.citation_rates).sort();
            const rateYear = rateYears[rateYears.length - 3];
            const rates = d.citation_rates[rateYear];

            let rateSummary = '';
            if (rates) {
                const rateStrs = ['WW', 'WM', 'MW', 'MM'].map(p =>
                    `${p}: <span class="stat-highlight">${rates[p]?.r?.toFixed(2) || 'N/A'}</span>`
                ).join(' | ');
                rateSummary = `<h4>Citation rates (${rateYear})</h4><p>${rateStrs} citations per paper</p>`;
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
        GenderChartManager.citingByYearChart('citing-by-year-chart', d.citing_gender_by_year);
        GenderChartManager.citingGapChart('citing-gap-chart', d.citing_gender_by_year);

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

        const overall = q.overall;
        const total = overall.total || 0;

        let countryNote = '';
        if (q.by_country) {
            const highCountries = Object.entries(q.by_country)
                .filter(([, d]) => d.pctAssigned >= 70).map(([c]) => c).slice(0, 5).join(', ');
            const lowCountries = Object.entries(q.by_country)
                .filter(([, d]) => d.pctAssigned < 40).map(([c]) => c).slice(0, 5).join(', ');
            if (highCountries) countryNote += `<p>Highest classification rates: ${highCountries}</p>`;
            if (lowCountries) countryNote += `<p>Lowest classification rates: ${lowCountries}</p>`;
        }

        document.getElementById('quality-summary').innerHTML = `
            <h4>Classification summary</h4>
            <p>Of ${total.toLocaleString()} papers with extractable first names:</p>
            <p>Woman: <span class="stat-highlight">${overall.W?.pct || 0}%</span> |
               Man: <span class="stat-highlight">${overall.M?.pct || 0}%</span> |
               Unknown: <span class="stat-highlight">${overall.U?.pct || 0}%</span></p>
            ${countryNote}
        `;
    }

    // ── Journal search ──

    _setupJournalSearch() {
        const input = document.getElementById('journal-search');
        const dropdown = document.getElementById('journal-dropdown');

        input.addEventListener('input', () => {
            const query = input.value.trim().toLowerCase();
            if (query.length < 2) { dropdown.classList.remove('open'); return; }

            const matches = this.journalIndex
                .filter(j => (j.name || '').toLowerCase().includes(query))
                .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
                .slice(0, 20);

            if (matches.length === 0) { dropdown.classList.remove('open'); return; }

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
            if (!e.target.closest('.journal-picker-container')) dropdown.classList.remove('open');
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

        if (data.rolling_if) {
            GenderChartManager.rollingIfChart('journal-rolling-if-chart', data.rolling_if);
        }

        GenderChartManager.journalRateChart('journal-rate-chart', data.yearly);

        // Citing gender charts
        const citingCard = document.getElementById('journal-citing-card');
        const gapCard = document.getElementById('journal-gap-card');
        if (data.citing_gender_by_year && Object.keys(data.citing_gender_by_year).length > 0) {
            citingCard.style.display = '';
            gapCard.style.display = '';
            GenderChartManager.citingByYearChart('journal-citing-by-year-chart', data.citing_gender_by_year);
            GenderChartManager.citingGapChart('journal-citing-gap-chart', data.citing_gender_by_year);
        } else {
            citingCard.style.display = 'none';
            gapCard.style.display = 'none';
        }

        const years = Object.keys(data.yearly).sort();
        const latestYear = years[years.length - 1];
        const latest = data.yearly[latestYear];
        const totalPapers = Object.keys(latest).reduce((s, p) => s + (latest[p]?.p || 0), 0);

        let html = `<h4>${data.name || name} (${latestYear})</h4>`;
        html += `<p>${totalPapers.toLocaleString()} research papers with gender-classified authors</p>`;
        html += `<p class="chart-note" style="margin-top:0">Papers where both first and last author names could be classified.</p>`;

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

    // ── Author search ──

    _setupAuthorSearch() {
        document.getElementById('author-search-btn').addEventListener('click', () => this._searchAuthorByName());
        document.getElementById('author-name-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this._searchAuthorByName();
        });
        document.getElementById('author-ncbi-btn').addEventListener('click', () => this._loadFromNCBIUrl());
        document.getElementById('author-ncbi-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this._loadFromNCBIUrl();
        });
        document.getElementById('author-pmid-btn').addEventListener('click', () => this._loadAuthorPMIDs());
        document.getElementById('author-exclude-self').addEventListener('change', () => this._renderAuthorResults());
    }

    _showAuthorStatus(msg, isError = false) {
        const el = document.getElementById('author-status');
        el.style.display = 'block';
        el.className = `author-status ${isError ? 'author-status-error' : ''}`;
        el.textContent = msg;
    }

    _hideAuthorStatus() {
        document.getElementById('author-status').style.display = 'none';
    }

    async _searchAuthorByName() {
        const name = document.getElementById('author-name-input').value.trim();
        if (!name) return;

        this._authorSearchName = name;
        this._authorExcluded = new Set();
        document.getElementById('author-results').style.display = 'none';
        this._showAuthorStatus(`Searching PubMed for "${name}"...`);

        try {
            const { pmids, totalFound } = await GenderDataLoader.searchPubMed(name);
            if (pmids.length === 0) {
                this._showAuthorStatus(`No papers found for "${name}" on PubMed.`, true);
                return;
            }
            this._showAuthorStatus(`Found ${totalFound} papers. Fetching data from iCite...`);
            await this._processAuthorPMIDs(pmids);
        } catch (e) {
            console.error('Author search failed:', e);
            this._showAuthorStatus(`Search failed: ${e.message}`, true);
        }
    }

    async _loadFromNCBIUrl() {
        const val = document.getElementById('author-ncbi-input').value.trim();
        if (!val) return;

        this._authorSearchName = '';
        this._authorExcluded = new Set();
        document.getElementById('author-results').style.display = 'none';
        this._showAuthorStatus('Fetching NCBI bibliography...');

        try {
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

            const extractPmids = html => [...html.matchAll(/\/pubmed\/(\d+)/g)].map(m => m[1]);

            const page1Html = await fetchPage(1).catch(() => {
                throw new Error('All CORS proxies failed — try again in a moment.');
            });

            const page1Pmids = extractPmids(page1Html);
            if (!page1Pmids.length) {
                this._showAuthorStatus('No PMIDs found. Make sure the bibliography is set to public.', true);
                return;
            }

            const allPmids = new Set(page1Pmids);

            const totalMatch =
                page1Html.match(/\((\d[\d,]*)\s+publications?\)/i) ||
                page1Html.match(/(\d[\d,]*)\s+publications?/i) ||
                page1Html.match(/"count"\s*:\s*(\d+)/i);
            const totalCount = totalMatch ? parseInt(totalMatch[1].replace(/,/g, ''), 10) : null;

            const PAGE_SIZE = page1Pmids.length || 50;
            const MAX_PAGES = 40;

            if (totalCount && totalCount > allPmids.size) {
                const pagesNeeded = Math.min(Math.ceil(totalCount / PAGE_SIZE), MAX_PAGES);
                this._showAuthorStatus(`${totalCount} papers found — loading ${pagesNeeded} pages...`);

                for (let p = 2; p <= pagesNeeded; p += 3) {
                    const batch = [p, p + 1, p + 2].filter(n => n <= pagesNeeded);
                    this._showAuthorStatus(`Loading pages ${batch[0]}–${batch[batch.length - 1]} of ${pagesNeeded}... (${allPmids.size} papers so far)`);
                    const batchHtml = await Promise.all(
                        batch.map(n => fetchPage(n).then(extractPmids).catch(() => []))
                    );
                    batchHtml.flat().forEach(id => allPmids.add(id));
                }
            } else if (!totalCount && page1Pmids.length >= PAGE_SIZE) {
                for (let p = 2; p <= MAX_PAGES; p++) {
                    const pageHtml = await fetchPage(p).catch(() => null);
                    if (!pageHtml) break;
                    const pids = extractPmids(pageHtml);
                    if (!pids.length) break;
                    const before = allPmids.size;
                    pids.forEach(id => allPmids.add(id));
                    if (allPmids.size === before) break;
                    this._showAuthorStatus(`${allPmids.size} papers loaded (page ${p})...`);
                }
            }

            const pmids = [...allPmids];
            this._showAuthorStatus(`Found ${pmids.length} papers. Fetching data from iCite...`);
            await this._processAuthorPMIDs(pmids);
        } catch (e) {
            console.error('NCBI bibliography load error:', e);
            this._showAuthorStatus(`Error: ${e.message}`, true);
        }
    }

    async _loadAuthorPMIDs() {
        const text = document.getElementById('author-pmid-input').value.trim();
        if (!text) return;

        const pmids = [...new Set(text.split(/[\s,;]+/).map(s => s.replace(/\D/g, '')).filter(s => s.length >= 5))];
        if (pmids.length === 0) {
            this._showAuthorStatus('No valid PMIDs found.', true);
            return;
        }

        this._authorSearchName = '';
        this._authorExcluded = new Set();
        document.getElementById('author-results').style.display = 'none';
        this._showAuthorStatus(`Loading ${pmids.length} PMIDs from iCite...`);

        try {
            await this._processAuthorPMIDs(pmids);
        } catch (e) {
            console.error('PMID load failed:', e);
            this._showAuthorStatus(`Load failed: ${e.message}`, true);
        }
    }

    async _processAuthorPMIDs(pmids) {
        const papers = await GenderDataLoader.fetchICite(pmids);
        this._authorPapers = papers;

        if (papers.length === 0) {
            this._showAuthorStatus('No data returned from iCite for these papers.', true);
            return;
        }

        // Collect all referenced PMIDs
        const refPmidSet = new Set();
        for (const p of papers) {
            if (p.references && Array.isArray(p.references)) {
                for (const r of p.references) refPmidSet.add(String(r));
            }
        }

        if (refPmidSet.size === 0) {
            this._showAuthorStatus('No reference data available from iCite for these papers.', true);
            return;
        }

        this._showAuthorStatus(`Found ${papers.length} papers with ${refPmidSet.size} unique references. Fetching reference data...`);

        this._authorRefData = await GenderDataLoader.fetchICite([...refPmidSet]);

        this._showAuthorStatus(`Analyzing gender of ${this._authorRefData.length} referenced papers...`);
        this._renderAuthorResults();
        this._hideAuthorStatus();
        document.getElementById('author-results').style.display = '';
    }

    _extractFirstName(authorObj) {
        if (!authorObj || !authorObj.firstName) return null;
        const parts = authorObj.firstName.trim().split(/\s+/);
        for (const part of parts) {
            const clean = part.replace(/\.$/, '');
            if (clean.length > 1) return clean.toLowerCase();
        }
        return null;
    }

    _inferGender(firstName) {
        if (!firstName || !this.genderNames) return null;
        return this.genderNames[firstName.toLowerCase()] || null;
    }

    _isSelfCitation(refPaper) {
        if (!this._authorSearchName || !refPaper.authors) return false;
        const searchParts = this._authorSearchName.trim().split(/\s+/);
        const searchLast = searchParts[0].toLowerCase();
        if (Array.isArray(refPaper.authors)) {
            return refPaper.authors.some(a => a.lastName && a.lastName.toLowerCase() === searchLast);
        }
        return false;
    }

    _renderAuthorResults() {
        const papers = this._authorPapers;
        const refData = this._authorRefData;
        const includeSelf = document.getElementById('author-exclude-self').checked;

        const refMap = new Map();
        for (const r of refData) refMap.set(String(r.pmid), r);

        const counts = { WW: 0, WM: 0, MW: 0, MM: 0, unknown: 0 };
        let totalRefs = 0;
        let selfCitCount = 0;
        const paperRows = [];

        for (const paper of papers) {
            // Skip excluded papers
            if (this._authorExcluded.has(String(paper.pmid))) continue;
            if (!paper.references || !Array.isArray(paper.references)) continue;

            let paperWW = 0, paperWM = 0, paperMW = 0, paperMM = 0, paperUnk = 0, paperSelf = 0;

            for (const refPmid of paper.references) {
                const ref = refMap.get(String(refPmid));
                if (!ref || !ref.authors || !Array.isArray(ref.authors) || ref.authors.length === 0) {
                    paperUnk++;
                    totalRefs++;
                    continue;
                }

                const isSelf = this._isSelfCitation(ref);
                if (isSelf) paperSelf++;
                if (isSelf && !includeSelf) {
                    selfCitCount++;
                    continue;
                }

                totalRefs++;

                const fa = ref.authors[0];
                const la = ref.authors.length > 1 ? ref.authors[ref.authors.length - 1] : fa;
                const faGender = this._inferGender(this._extractFirstName(fa));
                const laGender = this._inferGender(this._extractFirstName(la));

                if (faGender && laGender) {
                    const pair = faGender + laGender;
                    if (pair === 'WW') { counts.WW++; paperWW++; }
                    else if (pair === 'WM') { counts.WM++; paperWM++; }
                    else if (pair === 'MW') { counts.MW++; paperMW++; }
                    else if (pair === 'MM') { counts.MM++; paperMM++; }
                    else { counts.unknown++; paperUnk++; }
                } else {
                    counts.unknown++;
                    paperUnk++;
                }
            }

            paperRows.push({
                pmid: paper.pmid,
                title: paper.title || 'Untitled',
                journal: paper.journal || '',
                year: paper.year || 0,
                refs: paper.references ? paper.references.length : 0,
                ww: paperWW, wm: paperWM, mw: paperMW, mm: paperMM, unk: paperUnk,
                selfCits: paperSelf,
            });
        }

        const classified = counts.WW + counts.WM + counts.MW + counts.MM;
        const pct = (v) => classified > 0 ? (v / classified * 100).toFixed(1) : '0.0';

        const noteEl = document.getElementById('author-breakdown-note');
        let noteText = `${classified.toLocaleString()} references with classifiable gender pairs out of ${totalRefs.toLocaleString()} total.`;
        if (!includeSelf && selfCitCount > 0) noteText += ` ${selfCitCount} self-citations excluded.`;
        noteEl.textContent = noteText;

        const metricsEl = document.getElementById('author-metrics');
        metricsEl.innerHTML = ['WW', 'WM', 'MW', 'MM'].map(pair =>
            `<div class="author-metric">
                <span class="pair-badge pair-${pair.toLowerCase()}">${pair}</span>
                <span class="author-metric-value">${counts[pair].toLocaleString()}</span>
                <span class="author-metric-pct">${pct(counts[pair])}%</span>
            </div>`
        ).join('') + `
            <div class="author-metric">
                <span class="pair-badge" style="background:#888">?</span>
                <span class="author-metric-value">${counts.unknown.toLocaleString()}</span>
                <span class="author-metric-pct">unknown</span>
            </div>`;

        GenderChartManager.authorGenderChart('author-gender-chart', counts);

        document.getElementById('author-paper-count').textContent =
            `${paperRows.length}` + (this._authorExcluded.size > 0 ? ` of ${this._authorPapers.length}` : '');
        this._renderAuthorPapersTable(paperRows);
    }

    _renderAuthorPapersTable(rows) {
        const container = document.getElementById('author-papers-table');
        if (rows.length === 0 && this._authorPapers.length === 0) {
            container.innerHTML = '<p style="color:var(--text-muted)">No papers with reference data.</p>';
            return;
        }

        // Build all rows (included + excluded) sorted by year desc
        const allRows = [];
        for (const paper of this._authorPapers) {
            const existing = rows.find(r => String(r.pmid) === String(paper.pmid));
            allRows.push({
                pmid: paper.pmid,
                title: paper.title || 'Untitled',
                journal: paper.journal || '',
                year: paper.year || 0,
                refs: paper.references ? paper.references.length : 0,
                ww: existing?.ww || 0, wm: existing?.wm || 0,
                mw: existing?.mw || 0, mm: existing?.mm || 0,
                unk: existing?.unk || 0,
                included: !this._authorExcluded.has(String(paper.pmid)),
            });
        }
        allRows.sort((a, b) => b.year - a.year);

        let html = `<table class="author-table">
            <thead>
                <tr>
                    <th class="cb-cell"></th>
                    <th>Title</th>
                    <th>Journal</th>
                    <th>Year</th>
                    <th>Refs</th>
                    <th><span class="pair-badge pair-ww">WW</span></th>
                    <th><span class="pair-badge pair-wm">WM</span></th>
                    <th><span class="pair-badge pair-mw">MW</span></th>
                    <th><span class="pair-badge pair-mm">MM</span></th>
                    <th>?</th>
                </tr>
            </thead>
            <tbody>`;

        for (const r of allRows) {
            const checked = r.included ? 'checked' : '';
            const dimClass = r.included ? '' : ' author-row-excluded';
            html += `<tr class="author-paper-row${dimClass}" data-pmid="${r.pmid}">
                <td class="cb-cell"><input type="checkbox" class="author-paper-cb" data-pmid="${r.pmid}" ${checked}></td>
                <td class="author-title-cell">${this._escapeHtml(r.title)}</td>
                <td>${this._escapeHtml(r.journal)}</td>
                <td>${r.year}</td>
                <td>${r.refs}</td>
                <td>${r.included ? (r.ww || '') : ''}</td>
                <td>${r.included ? (r.wm || '') : ''}</td>
                <td>${r.included ? (r.mw || '') : ''}</td>
                <td>${r.included ? (r.mm || '') : ''}</td>
                <td>${r.included ? (r.unk || '') : ''}</td>
            </tr>`;
        }

        html += '</tbody></table>';
        container.innerHTML = html;

        // Checkbox handlers
        container.querySelectorAll('.author-paper-cb').forEach(cb => {
            cb.addEventListener('change', (e) => {
                e.stopPropagation();
                const pmid = cb.dataset.pmid;
                if (cb.checked) {
                    this._authorExcluded.delete(pmid);
                } else {
                    this._authorExcluded.add(pmid);
                }
                this._renderAuthorResults();
            });
        });

        // Click row to open PubMed (but not on checkbox)
        container.querySelectorAll('.author-paper-row').forEach(row => {
            row.addEventListener('click', (e) => {
                if (e.target.tagName === 'INPUT') return;
                window.open(`https://pubmed.ncbi.nlm.nih.gov/${row.dataset.pmid}/`, '_blank');
            });
        });
    }

    _escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
}
