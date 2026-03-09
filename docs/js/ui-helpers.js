/**
 * IMPACT UI Helpers
 * Formatting, table building, and utility functions.
 */

const UIHelpers = {
    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    formatIF(value) {
        if (value == null || isNaN(value)) return '—';
        return Number(value).toFixed(2);
    },

    formatInt(value) {
        if (value == null) return '—';
        return Number(value).toLocaleString();
    },

    formatPct(value) {
        if (value == null || isNaN(value)) return '—';
        return Number(value).toFixed(1) + '%';
    },

    formatMonth(monthStr) {
        if (!monthStr) return '—';
        const [year, month] = monthStr.split('-');
        const names = [
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
        ];
        return `${names[parseInt(month) - 1]} ${year}`;
    },

    /**
     * Create an HTML table from data rows and column definitions.
     * columns: [{key, label, format?}]
     */
    createTable(rows, columns) {
        const table = document.createElement('table');
        table.className = 'data-table';

        // Header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col.label;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Body
        const tbody = document.createElement('tbody');
        rows.forEach(row => {
            const tr = document.createElement('tr');
            columns.forEach(col => {
                const td = document.createElement('td');
                const val = row[col.key];
                td.textContent = col.format ? col.format(val) : (val ?? '—');
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return table;
    },

    /**
     * Build a journal card element.
     */
    createJournalCard(journal) {
        const card = document.createElement('div');
        card.className = 'journal-card';
        card.dataset.slug = journal.slug;

        const reviewPct = journal.paper_count > 0
            ? ((journal.paper_count - (journal.paper_count - (journal.review_count || 0))) / journal.paper_count * 100)
            : 0;

        card.innerHTML = `
            <h4>${journal.name}</h4>
            <div class="card-if">
                ${this.formatIF(journal.latest_if)}
                <small>24-mo citation rate</small>
            </div>
            <div class="card-stats">
                <span>${this.formatInt(journal.paper_count)} papers</span>
            </div>
        `;

        return card;
    }
};
