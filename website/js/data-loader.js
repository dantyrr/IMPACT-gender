/**
 * IMPACT Data Loader
 * Fetches and caches JSON data from the data/ directory.
 */

class DataLoader {
    constructor() {
        this.baseUrl = 'data';
        this.cache = {};
    }

    async loadIndex() {
        return this._fetch(`${this.baseUrl}/index.json`);
    }

    async loadJournal(slug) {
        return this._fetch(`${this.baseUrl}/journals/${slug}.json`);
    }

    async _fetch(url) {
        if (this.cache[url]) {
            return this.cache[url];
        }

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${url}`);
            }
            const data = await response.json();
            this.cache[url] = data;
            return data;
        } catch (error) {
            console.error('DataLoader error:', error);
            throw error;
        }
    }
}

// Global instance
const dataLoader = new DataLoader();
