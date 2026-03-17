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
        // Load from main IMPACT index
        const resp = await fetch('../docs/data/index.json').catch(() => null);
        if (resp && resp.ok) {
            const data = await resp.json();
            this._cache.index = data;
            return data;
        }
        // Fallback: try local index
        const resp2 = await fetch('data/index.json');
        if (!resp2.ok) return [];
        const data = await resp2.json();
        this._cache.index = data;
        return data;
    },
};
