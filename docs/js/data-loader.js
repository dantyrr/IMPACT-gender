/**
 * IMPACT Data Loader
 * Fetches JSON data from Cloudflare R2 (production) or local data/ (dev).
 *
 * Detection: if the page is served from localhost/file://, uses local data/.
 * Otherwise uses R2_BASE_URL.
 */

const R2_BASE_URL = 'https://pub-4368cf00a45748488f64d2b648550d4d.r2.dev';

class DataLoader {
    constructor() {
        const isLocal = location.hostname === 'localhost' ||
                        location.hostname === '127.0.0.1' ||
                        location.protocol === 'file:';
        this.baseUrl = isLocal ? 'data' : R2_BASE_URL;
        this.cache = {};
    }

    async loadIndex() {
        return this._fetch(`${this.baseUrl}/index.json`);
    }

    async loadJournal(slug) {
        return this._fetch(`${this.baseUrl}/journals/${slug}.json`);
    }

    async loadAuthor(slug) {
        return this._fetch(`${this.baseUrl}/authors/${slug}.json`);
    }

    async _fetch(url) {
        if (this.cache[url]) return this.cache[url];
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${url}`);
        const data = await response.json();
        this.cache[url] = data;
        return data;
    }
}

const dataLoader = new DataLoader();
