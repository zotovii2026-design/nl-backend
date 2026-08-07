/**
 * nl-page-prefs.js — универсальный модуль сохранения/восстановления
 * фильтров и порядка колонок для разделов NL Table.
 *
 * Каждый раздел (pageKey) получает свой ключ в localStorage,
 * изолированный по организации.
 *
 * API:
 *   NLPagePrefs.read(pageKey)                    → {} prefs
 *   NLPagePrefs.write(pageKey, patch)            → void
 *   NLPagePrefs.saveFilters(pageKey, state)      → void
 *   NLPagePrefs.loadFilters(pageKey)             → {} filters
 *   NLPagePrefs.saveColumns(pageKey, tabulator)  → void
 *   NLPagePrefs.loadColumnOrder(pageKey)         → { groupOrder, childOrder } | null
 *   NLPagePrefs.applyColumnOrder(pageKey, cols)  → cols (переупорядоченные)
 *   NLPagePrefs.resetColumns(pageKey)            → void
 *   NLPagePrefs.resetAll(pageKey)                → void
 *   NLPagePrefs.makeKey(pageKey)                 → string
 */

var NLPagePrefs = (function() {

    var VERSION = 1;

    function _orgId() {
        if (typeof getCurrentOrgId === 'function') {
            return getCurrentOrgId() || 'default';
        }
        if (typeof ORG_ID !== 'undefined') {
            return ORG_ID || 'default';
        }
        return 'default';
    }

    function makeKey(pageKey) {
        return 'nl:' + pageKey + ':ui-prefs:v' + VERSION + ':' + _orgId();
    }

    function read(pageKey) {
        try {
            var raw = localStorage.getItem(makeKey(pageKey));
            var prefs = raw ? JSON.parse(raw) : {};
            return prefs && typeof prefs === 'object' ? prefs : {};
        } catch(e) {
            console.warn('NLPagePrefs.read', e);
            return {};
        }
    }

    function write(pageKey, patch) {
        try {
            var prefs = Object.assign({}, read(pageKey), patch || {});
            localStorage.setItem(makeKey(pageKey), JSON.stringify(prefs));
        } catch(e) {
            console.warn('NLPagePrefs.write', e);
        }
    }

    /* ── Фильтры ── */

    function saveFilters(pageKey, state) {
        write(pageKey, { filters: state });
    }

    function loadFilters(pageKey) {
        return read(pageKey).filters || {};
    }

    /* ── Колонки ── */

    function _columnKey(def) {
        return def.groupKey || def.field || def.title || '';
    }

    function saveColumns(pageKey, tabulator) {
        if (!tabulator) return;
        var defs = tabulator.getColumnDefinitions();
        var columns = {
            groupOrder: [],
            childOrder: {}
        };
        defs.forEach(function(def) {
            var key = _columnKey(def);
            if (!key) return;
            columns.groupOrder.push(key);
            if (def.columns && def.columns.length) {
                columns.childOrder[key] = def.columns.map(_columnKey).filter(Boolean);
            }
        });
        write(pageKey, { columns: columns });
    }

    function loadColumnOrder(pageKey) {
        return read(pageKey).columns || null;
    }

    function applyColumnOrder(pageKey, columns) {
        var saved = loadColumnOrder(pageKey);
        if (!saved) return columns;

        var groupOrder = Array.isArray(saved.groupOrder) ? saved.groupOrder : [];
        var childOrder = saved.childOrder && typeof saved.childOrder === 'object' ? saved.childOrder : {};
        var byKey = {};

        columns.forEach(function(col) {
            var key = _columnKey(col);
            if (key) byKey[key] = col;
            if (col.columns && childOrder[key]) {
                var childByKey = {};
                col.columns.forEach(function(child) {
                    var childKey = _columnKey(child);
                    if (childKey) childByKey[childKey] = child;
                });
                var orderedChildren = [];
                childOrder[key].forEach(function(childKey) {
                    if (childByKey[childKey]) orderedChildren.push(childByKey[childKey]);
                });
                col.columns.forEach(function(child) {
                    if (orderedChildren.indexOf(child) === -1) orderedChildren.push(child);
                });
                col.columns = orderedChildren;
            }
        });

        var ordered = [];
        groupOrder.forEach(function(key) {
            if (byKey[key]) ordered.push(byKey[key]);
        });
        columns.forEach(function(col) {
            if (ordered.indexOf(col) === -1) ordered.push(col);
        });
        return ordered;
    }

    function resetColumns(pageKey) {
        try {
            var prefs = read(pageKey);
            delete prefs.columns;
            localStorage.setItem(makeKey(pageKey), JSON.stringify(prefs));
        } catch(e) {
            console.warn('NLPagePrefs.resetColumns', e);
        }
    }

    function resetAll(pageKey) {
        try {
            localStorage.removeItem(makeKey(pageKey));
        } catch(e) {
            console.warn('NLPagePrefs.resetAll', e);
        }
    }

    return {
        makeKey: makeKey,
        read: read,
        write: write,
        saveFilters: saveFilters,
        loadFilters: loadFilters,
        saveColumns: saveColumns,
        loadColumnOrder: loadColumnOrder,
        applyColumnOrder: applyColumnOrder,
        resetColumns: resetColumns,
        resetAll: resetAll,
        _columnKey: _columnKey
    };
})();
