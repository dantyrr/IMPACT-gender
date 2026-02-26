/**
 * JournalPicker — searchable multi-select with chip tags.
 *
 * Usage:
 *   const picker = new JournalPicker('my-container-id', journals, palette, onChange);
 *   picker.getSelected(); // → ['slug-a', 'slug-b']
 */
class JournalPicker {
    constructor(containerId, journals, palette, onChange) {
        this.journals = journals;
        this.palette = palette;
        this.onChange = onChange;
        this.selected = new Map(); // slug → { journal, color }
        this.container = document.getElementById(containerId);
        this._build();
    }

    _build() {
        this.container.className = 'journal-picker';

        this.tagsEl = document.createElement('div');
        this.tagsEl.className = 'picker-tags';

        const inputWrap = document.createElement('div');
        inputWrap.className = 'picker-input-wrap';

        this.input = document.createElement('input');
        this.input.type = 'text';
        this.input.className = 'picker-input';
        this.input.placeholder = 'Search journals to add…';
        inputWrap.appendChild(this.input);

        this.dropdown = document.createElement('div');
        this.dropdown.className = 'picker-dropdown';
        inputWrap.appendChild(this.dropdown);

        this.container.appendChild(this.tagsEl);
        this.container.appendChild(inputWrap);

        this.input.addEventListener('input', () => this._updateDropdown());
        this.input.addEventListener('focus', () => this._updateDropdown());
        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) this._closeDropdown();
        });
    }

    _colorFor(slug) {
        const idx = this.journals.findIndex(j => j.slug === slug);
        return this.palette[idx % this.palette.length];
    }

    _updateDropdown() {
        const term = this.input.value.toLowerCase().trim();
        const matches = this.journals
            .filter(j => !this.selected.has(j.slug) && (
                j.name.toLowerCase().includes(term) ||
                (j.abbreviation || '').toLowerCase().includes(term)
            ))
            .slice(0, 10);

        if (matches.length === 0) {
            this.dropdown.style.display = 'none';
            return;
        }

        this.dropdown.innerHTML = '';
        matches.forEach(journal => {
            const color = this._colorFor(journal.slug);
            const opt = document.createElement('div');
            opt.className = 'picker-option';

            const dot = document.createElement('span');
            dot.className = 'color-dot';
            dot.style.background = color;
            opt.appendChild(dot);
            opt.appendChild(document.createTextNode(journal.name));

            opt.addEventListener('mousedown', (e) => {
                e.preventDefault();
                this._select(journal, color);
            });
            this.dropdown.appendChild(opt);
        });

        this.dropdown.style.display = 'block';
    }

    _select(journal, color) {
        this.selected.set(journal.slug, { journal, color });
        this.input.value = '';
        this._closeDropdown();
        this._renderTags();
        this.onChange(this.getSelected());
    }

    _deselect(slug) {
        this.selected.delete(slug);
        this._renderTags();
        this.onChange(this.getSelected());
    }

    _renderTags() {
        this.tagsEl.innerHTML = '';
        this.selected.forEach(({ journal, color }, slug) => {
            const chip = document.createElement('span');
            chip.className = 'picker-chip';

            const dot = document.createElement('span');
            dot.className = 'color-dot';
            dot.style.background = color;
            chip.appendChild(dot);
            chip.appendChild(document.createTextNode(journal.name));

            const x = document.createElement('button');
            x.className = 'picker-chip-remove';
            x.textContent = '×';
            x.title = `Remove ${journal.name}`;
            x.addEventListener('click', () => this._deselect(slug));
            chip.appendChild(x);

            this.tagsEl.appendChild(chip);
        });
    }

    _closeDropdown() {
        this.dropdown.style.display = 'none';
    }

    getSelected() {
        return [...this.selected.keys()];
    }
}

/**
 * SingleJournalPicker — searchable single-select for a journal.
 *
 * Usage:
 *   const picker = new SingleJournalPicker('my-container-id', journals, (slug) => { ... });
 *   picker.getSelected(); // → 'slug' or null
 */
class SingleJournalPicker {
    constructor(containerId, journals, onChange) {
        this.journals = journals;
        this.onChange = onChange;
        this.currentSlug = null;
        this.container = document.getElementById(containerId);
        this._build();
    }

    _build() {
        this.container.className = 'single-journal-picker';

        const inputWrap = document.createElement('div');
        inputWrap.className = 'picker-input-wrap';

        this.input = document.createElement('input');
        this.input.type = 'text';
        this.input.className = 'picker-input';
        this.input.placeholder = 'Search journals…';

        this.clearBtn = document.createElement('button');
        this.clearBtn.className = 'single-picker-clear';
        this.clearBtn.textContent = '×';
        this.clearBtn.title = 'Clear selection';
        this.clearBtn.style.display = 'none';
        this.clearBtn.addEventListener('mousedown', (e) => {
            e.preventDefault();
            this._clear();
        });

        inputWrap.appendChild(this.input);
        inputWrap.appendChild(this.clearBtn);

        this.dropdown = document.createElement('div');
        this.dropdown.className = 'picker-dropdown';
        inputWrap.appendChild(this.dropdown);

        this.container.appendChild(inputWrap);

        this.input.addEventListener('input', () => this._updateDropdown());
        this.input.addEventListener('focus', () => this._updateDropdown());
        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) this._closeDropdown();
        });
    }

    _updateDropdown() {
        // If a journal is already locked in and user hasn't changed the text, skip
        const term = this.input.value.toLowerCase().trim();
        const matches = this.journals
            .filter(j =>
                j.name.toLowerCase().includes(term) ||
                (j.abbreviation || '').toLowerCase().includes(term)
            )
            .slice(0, 12);

        if (matches.length === 0) {
            this.dropdown.style.display = 'none';
            return;
        }

        this.dropdown.innerHTML = '';
        matches.forEach(journal => {
            const opt = document.createElement('div');
            opt.className = 'picker-option' + (journal.slug === this.currentSlug ? ' picker-option-selected' : '');
            opt.textContent = journal.name;
            opt.addEventListener('mousedown', (e) => {
                e.preventDefault();
                this._select(journal);
            });
            this.dropdown.appendChild(opt);
        });
        this.dropdown.style.display = 'block';
    }

    _select(journal) {
        this.currentSlug = journal.slug;
        this.input.value = journal.name;
        this.clearBtn.style.display = '';
        this._closeDropdown();
        this.onChange(journal.slug);
    }

    _clear() {
        this.currentSlug = null;
        this.input.value = '';
        this.clearBtn.style.display = 'none';
        this.onChange(null);
    }

    _closeDropdown() {
        this.dropdown.style.display = 'none';
    }

    getSelected() {
        return this.currentSlug;
    }
}
