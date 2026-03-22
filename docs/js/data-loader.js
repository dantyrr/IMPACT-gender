/**
 * Data loader for gender analysis dashboard.
 * Loads JSON from local data/ directory.
 */
const GenderDataLoader = {
    _cache: {},

    async loadAggregate() {
        if (this._cache.aggregate) return this._cache.aggregate;
        const resp = await fetch('data/gender/aggregate.json');
        if (!resp.ok) throw new Error('Failed to load aggregate data');
        const data = await resp.json();
        this._cache.aggregate = data;
        return data;
    },

    async loadJournal(slug) {
        const key = `journal_${slug}`;
        if (this._cache[key]) return this._cache[key];
        const resp = await fetch(`data/gender/journals/${slug}.json`);
        if (!resp.ok) return null;
        const data = await resp.json();
        this._cache[key] = data;
        return data;
    },

    async loadJournalIndex() {
        if (this._cache.index) return this._cache.index;
        const resp = await fetch('data/index.json');
        if (!resp.ok) return [];
        const data = await resp.json();
        this._cache.index = data;
        return data;
    },

    async loadGenderNames() {
        if (this._cache.genderNames) return this._cache.genderNames;
        const resp = await fetch('data/gender_names.json');
        if (!resp.ok) return {};
        const data = await resp.json();
        this._cache.genderNames = data;
        return data;
    },

    /**
     * Fetch iCite data for an array of PMIDs.
     */
    async fetchICite(pmids, batchSize = 100) {
        const results = [];
        for (let i = 0; i < pmids.length; i += batchSize) {
            const batch = pmids.slice(i, i + batchSize);
            try {
                const resp = await fetch(`https://icite.od.nih.gov/api/pubs?pmids=${batch.join(',')}`);
                if (!resp.ok) continue;
                const json = await resp.json();
                results.push(...(Array.isArray(json) ? json : (json.data || [])));
            } catch (e) {
                console.warn('iCite batch failed:', e);
            }
        }
        return results;
    },

    /**
     * Search PubMed for an author name, return array of PMIDs.
     */
    async searchPubMed(authorName) {
        const term = encodeURIComponent(`${authorName}[Author]`);
        const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${term}&retmax=2000&retmode=json`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error('PubMed search failed');
        const json = await resp.json();
        const result = json.esearchresult || {};
        return {
            pmids: result.idlist || [],
            totalFound: parseInt(result.count || '0', 10),
        };
    },
};
