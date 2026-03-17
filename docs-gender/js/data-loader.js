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
};
