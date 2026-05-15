/**
 * NL Grid — Обёртка над Tabulator для таблиц проекта NL Table
 * Переиспользуемый компонент: drag колонок, сортировка, sticky заголовки
 */

const NLGrid = {
    /**
     * Создать экземпляр Tabulator
     * @param {string} el - CSS-селектор или DOM-элемент
     * @param {object} opts - опции (columns, data, и т.д.)
     * @returns {Tabulator}
     */
    create(el, opts = {}) {
        const defaults = {
            layout: 'fitDataFill',
            movableColumns: true,
            sortable: true,
            headerSort: true,
            height: opts.height || '70vh',
            pagination: opts.pagination || false,
            paginationSize: opts.paginationSize || 50,
            placeholder: 'Нет данных',
            locale: 'ru-RU',
            columnHeaderSortMulti: true,
            clipboard: true,
            downloadRange: 'all',
            // Sticky header через virtualDom
            virtualDom: true,
            virtualDomBuffer: 50,
        };

        const config = { ...defaults, ...opts };

        // Размечаем группы колонок как неразделимые
        if (config.columns) {
            config.columns = config.columns.map(col => {
                // Группы колонок (с вложенными columns) — делаем неделимыми
                if (col.columns && col.columns.length > 0) {
                    return { ...col, movable: false }; // Группа двигается целиком через родителя
                }
                return col;
            });
        }

        return new Tabulator(el, config);
    },

    /**
     * Типовые форматтеры
     */
    formatters: {
        money: function(cell) {
            const val = parseFloat(cell.getValue());
            if (isNaN(val) || val === 0) return '<span style="color:#999">—</span>';
            return val.toLocaleString('ru-RU') + ' ₽';
        },
        pct: function(cell) {
            const val = parseFloat(cell.getValue());
            if (isNaN(val) || val === 0) return '<span style="color:#999">—</span>';
            return val.toFixed(1) + '%';
        },
        photo: function(cell) {
            const url = cell.getValue();
            if (!url) return '';
            const thumb = url.replace('/hq/', '/c246x328/').replace('/big/', '/c246x328/').replace('/tm/', '/c246x328/');
            return '<img src="' + thumb + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover">';
        },
    },

    /**
     * Сохранить порядок колонок в localStorage
     */
    saveColumnOrder(tabulator, key) {
        const cols = tabulator.getColumnDefinitions();
        const order = cols.map(c => c.field || c.title);
        localStorage.setItem('nl-grid-cols-' + key, JSON.stringify(order));
    },

    /**
     * Восстановить порядок колонок из localStorage
     */
    loadColumnOrder(key) {
        try {
            return JSON.parse(localStorage.getItem('nl-grid-cols-' + key));
        } catch { return null; }
    }
};
