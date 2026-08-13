// Единый стиль табуляторов — эталон Справочника (8px заголовки, 11px ячейки)
(function(){
    var s = document.createElement('style');
    s.id = 'stats-grid-style';
    s.textContent = [
        '#page-stats .stats-section-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:6px 0 6px}',
        '#page-stats .stats-section-title{font-size:1.02em;font-weight:700;color:#333}',
        '#page-stats .stats-section-note{font-size:.82em;color:#888}',
        '#page-stats #stats-alerts:empty{display:none}',
        '#page-stats #stats-alerts{margin:0 0 8px}',
        '#page-stats .stats-table{margin-bottom:12px}',
        '#page-stats .stats-table:last-child{margin-bottom:0}',
        '#page-stats .tabulator{border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;background:#fff;font-size:.82em}',
        '#page-stats .tabulator .tabulator-header{background:#f8f9fa;border-bottom:2px solid #e0e0e0;color:#333;font-size:.7em;text-transform:none;letter-spacing:0}',
        '#page-stats .tabulator .tabulator-header .tabulator-col{border-right:1px solid #e0e0e0}',
        '#page-stats .tabulator .tabulator-header .tabulator-col .tabulator-col-content{padding:2px 4px!important}',
        '#page-stats .tabulator .tabulator-col-title{font-size:8px!important;line-height:1.1!important;padding:2px 4px!important}',
        '#page-stats .tabulator .tabulator-cell{font-size:11px!important;padding:4px 6px;border-right:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0}',
        '#page-stats .tabulator .tabulator-row:hover{background:#f8f9ff}',
        '#page-stats .tabulator .tabulator-footer{background:#f8f9fa;border-top:1px solid #e0e0e0;padding:6px 12px}',
        '#page-stats .tabulator .tabulator-placeholder{color:#999;font-size:12px}',
        '#page-stats .tabulator .tabulator-data-tree-control{border-color:#d8dbe3;background:#fff}',
        '#stats-daily-tabulator .tabulator-tableholder{overflow-x:hidden!important}',
        '#page-stats .stats-article-row{background:#f8f9ff!important;font-weight:600}',
        '#page-stats .stats-barcode-list{display:block;white-space:normal;overflow-wrap:anywhere;line-height:1.25}',
        '#page-stats .stats-grid-pending{pointer-events:none}'
    ].join('');
    document.head.appendChild(s);
})();

var STATS_PAGE_KEY = 'stats';
var statsSummaryTabulator = null;
var statsDailyTabulator = null;
var statsProductsTabulator = null;
var statsAllProducts = [];
var statsVisibleProducts = [];
var statsTreeOpen = false;
var statsRestoringFilters = false;

function statsIsoDate(value) {
    return value.toISOString().slice(0, 10);
}

function statsMoney(value) {
    var num = Number(value || 0);
    if (!num) return '—';
    return num.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽';
}

function statsNumber(value) {
    var num = Number(value || 0);
    return num ? num.toLocaleString('ru-RU', {maximumFractionDigits: 2}) : '—';
}

function statsPercent(value) {
    var num = Number(value || 0);
    return num ? num.toFixed(1) + '%' : '—';
}

function statsEscape(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function(ch) {
        return ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;',
        })[ch];
    });
}

function statsBarcodeText(value) {
    if (Array.isArray(value)) return value.filter(Boolean).join(', ');
    return String(value || '');
}

function statsBarcodeSearchText(row) {
    if (row && row.barcodes_search) return String(row.barcodes_search).toLowerCase();
    var value = statsBarcodeText(row.barcodes_display || row.barcodes || row.barcode || '');
    var compact = value.replace(/\D/g, '');
    return (value + ' ' + compact).toLowerCase();
}

function statsBarcodeMatches(query, row) {
    var q = String(query || '').trim().toLowerCase();
    if (!q) return true;
    var compactQ = q.replace(/\D/g, '');
    var search = statsBarcodeSearchText(row || {});
    return search.includes(q) || (!!compactQ && search.includes(compactQ));
}

function statsYesterday() {
    var base = new Date();
    base.setHours(12, 0, 0, 0);
    base.setDate(base.getDate() - 1);
    return base;
}

function statsFillCustomDates(dateFrom, dateTo) {
    var fromEl = document.getElementById('stats-date-from');
    var toEl = document.getElementById('stats-date-to');
    if (fromEl && !fromEl.value) fromEl.value = dateFrom;
    if (toEl && !toEl.value) toEl.value = dateTo;
}

function getStatsDateRange() {
    if (typeof nlGetDateRange === 'function') return nlGetDateRange();
    var period = 'yesterday';
    var custom = document.getElementById('stats-custom-period');
    var base = statsYesterday();
    var start = new Date(base);
    var end = new Date(base);

    if (period === 'yesterday') {
        // base is already yesterday
    } else if (period === 'last7') {
        start.setDate(base.getDate() - 6);
    } else if (period === 'week') {
        var mondayOffset = (base.getDay() + 6) % 7;
        start.setDate(base.getDate() - mondayOffset);
    } else if (period === 'custom') {
        if (custom) custom.style.display = 'flex';
        var dateFrom = document.getElementById('stats-date-from')?.value;
        var dateTo = document.getElementById('stats-date-to')?.value;
        if (!dateFrom || !dateTo) {
            start = new Date(base);
            dateFrom = statsIsoDate(start);
            dateTo = statsIsoDate(base);
            statsFillCustomDates(dateFrom, dateTo);
        }
        return {dateFrom: dateFrom, dateTo: dateTo};
    } else {
        start = new Date(base.getFullYear(), base.getMonth(), 1, 12);
    }

    if (custom) custom.style.display = 'none';
    return {dateFrom: statsIsoDate(start), dateTo: statsIsoDate(end)};
}

function statsPhotoFormatter(cell) {
    var url = cell.getValue();
    if (!url) return '';
    var thumb = String(url).replace('/hq/', '/c246x328/').replace('/big/', '/c246x328/').replace('/tm/', '/c246x328/');
    return '<img src="' + thumb + '" style="width:34px;height:34px;border-radius:4px;object-fit:cover" loading="lazy">';
}

function statsMoneyFormatter(cell) {
    return statsMoney(cell.getValue());
}

function statsNumberFormatter(cell) {
    return statsNumber(cell.getValue());
}

function statsPercentFormatter(cell) {
    return statsPercent(cell.getValue());
}

function statsTextHas(value, query) {
    return String(value || '').toLowerCase().includes(query);
}

function statsWbArticleFormatter(cell) {
    var row = cell.getRow().getData();
    var nmId = row.nm_id_display || cell.getValue();
    if (!nmId || (typeof nmId === 'string' && nmId.startsWith('_solo_'))) return '';
    var safeNmId = statsEscape(nmId);
    var url = 'https://www.wildberries.ru/catalog/' + encodeURIComponent(nmId) + '/detail.aspx';
    return '<a href="' + url + '" target="_blank" rel="noopener" style="color:#5b4a9e;text-decoration:none;font-weight:700" title="Открыть на Wildberries">' + safeNmId + '</a>';
}

function statsBarcodeFormatter(cell) {
    var row = cell.getRow().getData();
    var value = statsBarcodeText(row.barcodes_display || row.barcodes || row.barcode || '');
    return value ? '<span class="stats-barcode-list" title="' + statsEscape(value) + '">' + statsEscape(value) + '</span>' : '';
}

function statsBarcodeHeaderFilter(headerValue, rowValue, rowData) {
    return statsBarcodeMatches(headerValue, rowData);
}

function statsStockFormatter(cell) {
    var row = cell.getData();
    var fbo = Number(row.stock_fbo_qty || 0);
    var fbs = Number(row.stock_qty || 0);
    var total = fbo + fbs;
    var color = total <= 0 ? '#e74c3c' : total <= 5 ? '#e17055' : '#2d3436';
    return '<div style="font-weight:600;color:' + color + '">' + total.toLocaleString('ru-RU') + '</div>'
        + '<div style="font-size:.75em;color:#0984e3">FBO ' + fbo.toLocaleString('ru-RU') + '</div>'
        + '<div style="font-size:.75em;color:#6c5ce7">FBS ' + fbs.toLocaleString('ru-RU') + '</div>';
}

function initStatsSummaryGrid() {
    if (statsSummaryTabulator || !document.getElementById('stats-summary-tabulator')) return;
    statsSummaryTabulator = new Tabulator('#stats-summary-tabulator', {
        data: [],
        layout: 'fitDataFill',
        height: 112,
        placeholder: 'Нет данных',
        columns: [
            {title: 'Период', field: 'period', width: 190, headerSort: false},
            {title: 'Заказы ₽', field: 'orders_revenue', width: 120, hozAlign: 'right', formatter: statsMoneyFormatter},
            {title: 'Заказы', field: 'orders', width: 90, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: 'Выкупы ₽', field: 'buyouts_revenue', width: 120, hozAlign: 'right', formatter: statsMoneyFormatter},
            {title: 'Выкупы', field: 'buyouts', width: 90, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: '% выкупа', field: 'buyout_pct', width: 90, hozAlign: 'right', formatter: statsPercentFormatter},
            {title: 'Возвраты', field: 'returns', width: 90, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: 'Реклама', field: 'ad_cost', width: 110, hozAlign: 'right', formatter: statsMoneyFormatter},
            {title: 'Прибыль*', field: 'profit', width: 110, hozAlign: 'right', formatter: statsMoneyFormatter},
            {title: 'Остатки', field: 'stock_total', width: 100, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: 'FBO', field: 'stock_fbo', width: 85, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: 'FBS', field: 'stock_fbs', width: 85, hozAlign: 'right', formatter: statsNumberFormatter},
            {title: 'CTR', field: 'ctr', width: 80, hozAlign: 'right', formatter: statsPercentFormatter},
            {title: 'Рейтинг', field: 'rating', width: 85, hozAlign: 'right', formatter: statsNumberFormatter},
        ],
    });
}

function initStatsDailyGrid() {
    if (statsDailyTabulator || !document.getElementById('stats-daily-tabulator')) return;
    var columns = [
        {title: 'Дата', field: 'date', minWidth: 86, responsive: 0, headerSort: true},
        {title: 'Заказы ₽', field: 'orders_revenue', minWidth: 96, responsive: 0, hozAlign: 'right', formatter: statsMoneyFormatter, bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return statsMoney(cell.getValue()); }},
        {title: 'Заказы', field: 'orders_count', minWidth: 72, responsive: 0, hozAlign: 'right', formatter: statsNumberFormatter, bottomCalc: 'sum'},
        {title: 'Выкупы ₽', field: 'buyouts_revenue', minWidth: 96, responsive: 0, hozAlign: 'right', formatter: statsMoneyFormatter, bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return statsMoney(cell.getValue()); }},
        {title: 'Выкупы', field: 'buyouts_count', minWidth: 72, responsive: 0, hozAlign: 'right', formatter: statsNumberFormatter, bottomCalc: 'sum'},
        {title: 'Возвраты', field: 'returns_count', minWidth: 72, responsive: 2, hozAlign: 'right', formatter: statsNumberFormatter, bottomCalc: 'sum'},
        {title: '% выкупа', field: 'buyout_pct', minWidth: 72, responsive: 2, hozAlign: 'right', formatter: statsPercentFormatter},
        {title: 'Реклама', field: 'ad_cost', minWidth: 88, responsive: 3, hozAlign: 'right', formatter: statsMoneyFormatter, bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return statsMoney(cell.getValue()); }},
        {title: 'Показы', field: 'impressions', minWidth: 82, responsive: 5, hozAlign: 'right', formatter: statsNumberFormatter, bottomCalc: 'sum'},
        {title: 'Клики', field: 'clicks', minWidth: 68, responsive: 5, hozAlign: 'right', formatter: statsNumberFormatter, bottomCalc: 'sum'},
        {title: 'CTR', field: 'ctr', minWidth: 60, responsive: 4, hozAlign: 'right', formatter: statsPercentFormatter},
        {title: 'Остаток', field: 'stock_total', minWidth: 72, responsive: 4, hozAlign: 'right', formatter: statsNumberFormatter},
        {title: 'Рейтинг', field: 'avg_rating', minWidth: 68, responsive: 6, hozAlign: 'right', formatter: statsNumberFormatter},
    ];
    if (typeof NLPagePrefs !== 'undefined') columns = NLPagePrefs.applyColumnOrder(STATS_PAGE_KEY + '-daily', columns);
    statsDailyTabulator = new Tabulator('#stats-daily-tabulator', {
        data: [],
        layout: 'fitColumns',
        responsiveLayout: 'hide',
        height: 280,
        placeholder: 'Нет данных',
        movableColumns: true,
        headerSortClickElement: 'header',
        columns: columns,
    });
    statsDailyTabulator.on('columnMoved', function() {
        if (typeof NLPagePrefs !== 'undefined') NLPagePrefs.saveColumns(STATS_PAGE_KEY + '-daily', statsDailyTabulator);
    });
}

function getStatsProductColumns() {
    var columns = [
        {title: 'Фото', field: 'photo_main', width: 54, headerSort: false, formatter: statsPhotoFormatter},
        {title: 'Арт WB', field: 'nm_id_display', width: 105, headerFilter: 'input', formatter: statsWbArticleFormatter},
        {title: 'Название', field: 'product_name', minWidth: 220, headerFilter: 'input', formatter: function(cell) {
            var value = cell.getValue() || '';
            return '<span title="' + statsEscape(value) + '">' + statsEscape(value) + '</span>';
        }},
        {title: 'Размер', field: 'size_name', width: 90, headerFilter: 'input'},
        {title: 'ШК', field: 'barcodes_display', width: 240, headerFilter: 'input', headerFilterFunc: statsBarcodeHeaderFilter, formatter: statsBarcodeFormatter, variableHeight: true},
        {title: 'Остаток', field: 'stock_total', width: 105, hozAlign: 'right', formatter: statsStockFormatter},
        {title: 'Заказы разм.', field: 'orders_count', width: 105, hozAlign: 'right', sorter: 'number'},
        {title: 'Заказы разм. ₽', field: 'total_orders_revenue', width: 120, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: 'Выкупы разм.', field: 'buyouts_count', width: 105, hozAlign: 'right', sorter: 'number'},
        {title: 'Выкупы разм. ₽', field: 'total_buyouts_revenue', width: 120, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: 'Возвраты разм.', field: 'returns_count', width: 110, hozAlign: 'right', sorter: 'number'},
        {title: 'Заказы карт.', field: 'card_orders_count', width: 105, hozAlign: 'right', sorter: 'number', formatter: statsNumberFormatter},
        {title: 'Заказы карт. ₽', field: 'card_orders_revenue', width: 120, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: 'Выкупы карт.', field: 'card_buyouts_count', width: 105, hozAlign: 'right', sorter: 'number', formatter: statsNumberFormatter},
        {title: 'Выкупы карт. ₽', field: 'card_buyouts_revenue', width: 120, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: '% выкупа', field: 'buyout_pct', width: 90, hozAlign: 'right', sorter: 'number', formatter: statsPercentFormatter},
        {title: 'Рейтинг', field: 'rating', width: 85, hozAlign: 'right', sorter: 'number', formatter: statsNumberFormatter},
        {title: 'Показы карт.', field: 'impressions', width: 105, hozAlign: 'right', sorter: 'number'},
        {title: 'Клики карт.', field: 'clicks', width: 95, hozAlign: 'right', sorter: 'number'},
        {title: 'CTR карт.', field: 'ctr', width: 90, hozAlign: 'right', sorter: 'number', formatter: statsPercentFormatter},
        {title: 'Реклама', field: 'ad_cost', width: 105, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: 'Цена', field: 'price_display', width: 100, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
    ];
    if (typeof NLPagePrefs !== 'undefined') columns = NLPagePrefs.applyColumnOrder(STATS_PAGE_KEY + '-products', columns);
    return columns;
}

function initStatsProductsGrid() {
    if (statsProductsTabulator || !document.getElementById('stats-products-tabulator')) return;
    statsProductsTabulator = new Tabulator('#stats-products-tabulator', {
        data: [],
        columns: getStatsProductColumns(),
        height: '66vh',
        layout: 'fitDataFill',
        renderHorizontal: 'virtual',
        placeholder: 'Нет данных',
        movableColumns: true,
        headerSortClickElement: 'header',
        dataTree: true,
        dataTreeChildField: '_children',
        dataTreeStartExpanded: false,
        dataTreeElementColumn: 'nm_id_display',
        rowFormatter: function(row) {
            var data = row.getData();
            if (data && data._isArticleRow) row.getElement().classList.add('stats-article-row');
        },
    });
    statsProductsTabulator.on('dataFiltered', updateStatsProductsCount);
    statsProductsTabulator.on('columnMoved', function() {
        if (typeof NLPagePrefs !== 'undefined') NLPagePrefs.saveColumns(STATS_PAGE_KEY + '-products', statsProductsTabulator);
    });
}

function prepareStatsProducts(products) {
    var nmCounts = {};
    (products || []).forEach(function(p) {
        nmCounts[p.nm_id] = (nmCounts[p.nm_id] || 0) + 1;
    });

    var rows = (products || []).map(function(p) {
        var orders = Number(p.orders_count || 0);
        var buyouts = Number(p.buyouts_count || 0);
        var impressions = Number(p.impressions || 0);
        var clicks = Number(p.clicks || 0);
        var sizeName = p.size_name || '';
        var isSizeless = nmCounts[p.nm_id] === 1 && (!sizeName || sizeName === '0' || sizeName === 'ONE SIZE');
        var barcodes = statsBarcodeText(p.barcodes || p.barcode || '');
        return Object.assign({}, p, {
            _id: p.entity_id || ('stats_' + p.nm_id + '_' + (p.size_name || '0')),
            nm_id_display: p.nm_id,
            _noGroup: isSizeless,
            _isArticleRow: false,
            barcodes_display: barcodes,
            barcodes_search: statsBarcodeSearchText({barcodes_display: barcodes}),
            stock_total: Number(p.stock_qty || 0) + Number(p.stock_fbo_qty || 0),
            buyout_pct: orders ? buyouts / orders * 100 : 0,
            ctr: impressions ? clicks / impressions * 100 : 0,
            price_display: p.price_discount || p.price || p.wb_price_fact || 0,
        });
    });

    var grouped = {};
    var order = [];
    rows.forEach(function(row) {
        var nmId = String(row.nm_id_display || row.nm_id || '');
        if (!grouped[nmId]) {
            grouped[nmId] = [];
            order.push(nmId);
        }
        grouped[nmId].push(row);
    });

    return order.map(function(nmId) {
        var children = grouped[nmId] || [];
        if (children.length <= 1 || children[0]._noGroup) return children[0];
        var first = Object.assign({}, children[0]);
        var parent = Object.assign({}, first, {
            _id: 'article_' + nmId,
            _isArticleRow: true,
            _hasSizes: true,
            entity_id: '',
            nm_id: nmId,
            nm_id_display: nmId,
            size_name: children.length + (children.length > 1 && children.length < 5 ? ' размера' : ' размеров'),
            barcodes_display: children.map(function(child) { return child.barcodes_display; }).filter(Boolean).join(', '),
            barcodes_search: children.map(function(child) { return child.barcodes_search; }).filter(Boolean).join(' '),
            stock_qty: 0,
            stock_fbo_qty: 0,
            stock_total: 0,
            orders_count: 0,
            buyouts_count: 0,
            returns_count: 0,
            total_orders_revenue: 0,
            total_buyouts_revenue: 0,
            ad_cost: 0,
            _children: [],
        });
        children.forEach(function(child) {
            parent.stock_qty += Number(child.stock_qty || 0);
            parent.stock_fbo_qty += Number(child.stock_fbo_qty || 0);
            parent.stock_total += Number(child.stock_total || 0);
            parent.orders_count += Number(child.orders_count || 0);
            parent.buyouts_count += Number(child.buyouts_count || 0);
            parent.returns_count += Number(child.returns_count || 0);
            parent.total_orders_revenue += Number(child.total_orders_revenue || 0);
            parent.total_buyouts_revenue += Number(child.total_buyouts_revenue || 0);
            parent.ad_cost += Number(child.ad_cost || 0);
            var copy = Object.assign({}, child, {
                _hasSizes: true,
                _parent_nm_id: nmId,
                impressions: 0,
                clicks: 0,
                ctr: 0,
                card_orders_count: 0,
                card_buyouts_count: 0,
                card_returns_count: 0,
                card_orders_revenue: 0,
                card_buyouts_revenue: 0,
                card_cancel_revenue: 0,
            });
            parent._children.push(copy);
        });
        parent.impressions = Number(first.card_impressions || first.impressions || 0);
        parent.clicks = Number(first.card_clicks || first.clicks || 0);
        parent.ctr = parent.impressions ? parent.clicks / parent.impressions * 100 : 0;
        parent.card_orders_count = Number(first.card_orders_count || 0);
        parent.card_buyouts_count = Number(first.card_buyouts_count || 0);
        parent.card_returns_count = Number(first.card_returns_count || 0);
        parent.card_orders_revenue = Number(first.card_orders_revenue || 0);
        parent.card_buyouts_revenue = Number(first.card_buyouts_revenue || 0);
        parent.card_cancel_revenue = Number(first.card_cancel_revenue || 0);
        parent.buyout_pct = parent.orders_count ? parent.buyouts_count / parent.orders_count * 100 : 0;
        return parent;
    });
}

function applyStatsTopSearch() {
    applyStatsFilters();
}

function statsMatchSelected(value, selected) {
    if (!selected || !selected.length) return true;
    return selected.indexOf(String(value || '')) !== -1;
}

function statsMatchTags(value, selected) {
    if (!selected || !selected.length) return true;
    var tags = String(value || '').replace(/;/g, ',').replace(/\|/g, ',').split(',').map(function(v) { return v.trim(); });
    return tags.some(function(tag) { return selected.indexOf(tag) !== -1; });
}

function getStatsFilterState() {
    return {
        fulfillmentModel: document.getElementById('stats-flt-ff')?.value || '',
        status: typeof NLFilters !== 'undefined' ? NLFilters.getValues(document.getElementById('stats-flt-status')) : [],
        productClass: typeof NLFilters !== 'undefined' ? NLFilters.getValues(document.getElementById('stats-flt-class')) : [],
        brand: typeof NLFilters !== 'undefined' ? NLFilters.getValues(document.getElementById('stats-flt-brand')) : [],
        tags: typeof NLFilters !== 'undefined' ? NLFilters.getValues(document.getElementById('stats-flt-tags')) : [],
        search: document.getElementById('stats-flt-search')?.value || '',
        topSearch: document.getElementById('filter-article')?.value || '',
        treeOpen: statsTreeOpen,
    };
}

function saveStatsFilterPreferences() {
    if (statsRestoringFilters || typeof NLPagePrefs === 'undefined') return;
    NLPagePrefs.saveFilters(STATS_PAGE_KEY, getStatsFilterState());
}

function setStatsMultiFilterValues(id, values) {
    if (typeof NLFilters === 'undefined') return;
    var sel = document.getElementById(id);
    if (!sel) return;
    var available = new Set(Array.from(sel.options || []).map(function(o) { return o.value; }));
    NLFilters.setValues(sel, (values || []).filter(function(v) { return available.has(v); }));
    NLFilters.renderMenu(id);
}

function restoreStatsFilterPreferences() {
    if (typeof NLPagePrefs === 'undefined') return;
    var filters = NLPagePrefs.loadFilters(STATS_PAGE_KEY);
    if (!filters || !Object.keys(filters).length) return;
    statsRestoringFilters = true;
    var ff = document.getElementById('stats-flt-ff');
    var search = document.getElementById('stats-flt-search');
    if (ff) ff.value = filters.fulfillmentModel || '';
    if (search) search.value = filters.search || '';
    setStatsMultiFilterValues('stats-flt-status', filters.status || []);
    setStatsMultiFilterValues('stats-flt-class', filters.productClass || []);
    setStatsMultiFilterValues('stats-flt-brand', filters.brand || []);
    setStatsMultiFilterValues('stats-flt-tags', filters.tags || []);
    statsTreeOpen = !!filters.treeOpen;
    statsRestoringFilters = false;
}

function fillStatsMultiSelect(id, label, values) {
    var sel = document.getElementById(id);
    if (!sel || typeof NLFilters === 'undefined') return;
    sel.innerHTML = '';
    (values || []).forEach(function(v) {
        if (v == null || v === '') return;
        var opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        opt.title = v;
        sel.appendChild(opt);
    });
    NLFilters.selectAll(sel);
    NLFilters.renderMenu(id, label);
}

function renderStatsFilterOptions(options) {
    options = options || {};
    fillStatsMultiSelect('stats-flt-status', 'Статус', options.product_status || []);
    fillStatsMultiSelect('stats-flt-class', 'Класс', options.product_class || []);
    fillStatsMultiSelect('stats-flt-brand', 'Бренд', options.brand || []);
    fillStatsMultiSelect('stats-flt-tags', 'Теги', options.tags || []);
    restoreStatsFilterPreferences();
}

function applyStatsFilters() {
    if (!statsProductsTabulator) return;
    var state = getStatsFilterState();
    var q = String((state.search || '') + ' ' + (state.topSearch || '')).trim().toLowerCase();
    statsVisibleProducts = (statsAllProducts || []).filter(function(row) {
        if (state.fulfillmentModel && String(row.fulfillment_model || '').toLowerCase() !== state.fulfillmentModel.toLowerCase()) return false;
        if (!statsMatchSelected(row.product_status, state.status)) return false;
        if (!statsMatchSelected(row.product_class, state.productClass)) return false;
        if (!statsMatchSelected(row.brand, state.brand)) return false;
        if (!statsMatchTags(row.tags, state.tags)) return false;
        if (q) {
            return statsTextHas(row.nm_id_display || row.nm_id, q)
                || statsTextHas(row.vendor_code, q)
                || statsTextHas(row.product_name, q)
                || statsBarcodeMatches(q, row);
        }
        return true;
    });
    statsProductsTabulator.setData(prepareStatsProducts(statsVisibleProducts)).then(function() {
        setStatsTreeOpen(statsTreeOpen, true);
        updateStatsProductsCount();
    });
    saveStatsFilterPreferences();
}

function resetStatsFilters() {
    ['stats-flt-status', 'stats-flt-class', 'stats-flt-brand', 'stats-flt-tags'].forEach(function(id) {
        var sel = document.getElementById(id);
        if (sel && typeof NLFilters !== 'undefined') NLFilters.selectAll(sel);
    });
    var ff = document.getElementById('stats-flt-ff');
    var search = document.getElementById('stats-flt-search');
    if (ff) ff.value = '';
    if (search) search.value = '';
    applyStatsFilters();
}

function resetStatsColumnPreferences() {
    if (typeof NLPagePrefs !== 'undefined') {
        NLPagePrefs.resetColumns(STATS_PAGE_KEY + '-daily');
        NLPagePrefs.resetColumns(STATS_PAGE_KEY + '-products');
    }
    if (statsDailyTabulator) { statsDailyTabulator.destroy(); statsDailyTabulator = null; }
    if (statsProductsTabulator) { statsProductsTabulator.destroy(); statsProductsTabulator = null; }
    initStatsDailyGrid();
    initStatsProductsGrid();
    applyStatsFilters();
    if (typeof showToast === 'function') showToast('Порядок колонок восстановлен');
}

function resetStatsViewPreferences() {
    if (typeof NLPagePrefs !== 'undefined') {
        NLPagePrefs.resetAll(STATS_PAGE_KEY);
        NLPagePrefs.resetAll(STATS_PAGE_KEY + '-daily');
        NLPagePrefs.resetAll(STATS_PAGE_KEY + '-products');
    }
    resetStatsFilters();
    resetStatsColumnPreferences();
}

function toggleStatsTreeOpen() {
    setStatsTreeOpen(!statsTreeOpen);
}

function setStatsTreeOpen(open, silent) {
    if (!statsProductsTabulator) return;
    var rows = statsProductsTabulator.getRows ? statsProductsTabulator.getRows() : [];
    rows.forEach(function(row) {
        var data = row.getData();
        if (!data || !data._isArticleRow) return;
        if (open && typeof row.treeExpand === 'function') row.treeExpand();
        if (!open && typeof row.treeCollapse === 'function') row.treeCollapse();
    });
    statsTreeOpen = !!open;
    var btn = document.getElementById('stats-tree-toggle-btn');
    if (btn) btn.textContent = statsTreeOpen ? 'Свернуть размеры' : 'Раскрыть размеры';
    if (!silent && statsProductsTabulator.redraw) statsProductsTabulator.redraw(false);
    saveStatsFilterPreferences();
}

function updateStatsProductsCount() {
    var count = document.getElementById('stats-products-count');
    if (!count || !statsProductsTabulator) return;
    var total = statsAllProducts.length;
    var active = Array.isArray(statsVisibleProducts) ? statsVisibleProducts.length : total;
    count.textContent = active === total
        ? total.toLocaleString('ru-RU') + ' строк'
        : active.toLocaleString('ru-RU') + ' из ' + total.toLocaleString('ru-RU') + ' строк';
}

function setStatsGridPending(isPending) {
    ['stats-summary-tabulator', 'stats-daily-tabulator', 'stats-products-tabulator'].forEach(function(id) {
        var el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('stats-grid-pending', !!isPending);
    });
}

function renderStatsAlerts(summary) {
    var alerts = [];
    if ((summary.zero_stock_count || 0) > 0) alerts.push('<div class="alert-card red">Нет в наличии: ' + summary.zero_stock_count + ' товаров</div>');
    if ((summary.low_stock_count || 0) > 0) alerts.push('<div class="alert-card yellow">Низкий остаток (<=5): ' + summary.low_stock_count + ' товаров</div>');
    var el = document.getElementById('stats-alerts');
    if (el) el.innerHTML = alerts.join('');
}

async function loadStatsGrid() {
    if (!ORG_ID) return;
    setStatsGridPending(true);
    initStatsSummaryGrid();
    initStatsDailyGrid();
    initStatsProductsGrid();

    var range = getStatsDateRange();
    var label = document.getElementById('stats-period-label');
    if (label) label.textContent = range.dateFrom + ' - ' + range.dateTo;
    var count = document.getElementById('stats-products-count');
    if (count) count.textContent = 'Загрузка...';

    try {
        var url = '/api/v1/nl/control?org_id=' + encodeURIComponent(ORG_ID)
            + '&date_from=' + encodeURIComponent(range.dateFrom)
            + '&date_to=' + encodeURIComponent(range.dateTo);
        var res = await fetch(url, {headers: {'Authorization': 'Bearer ' + TOKEN}});
        if (!res.ok) throw new Error('Не удалось загрузить основные показатели');
        var data = await res.json();
        var s = data.summary || {};
        var ordersRevenue = Number(s.total_orders_revenue || 0);
        var buyoutsRevenue = Number(s.total_buyouts_revenue || s.total_revenue || 0);
        var orders = Number(s.total_orders || 0);
        var buyouts = Number(s.total_buyouts || 0);
        var adCost = Number(s.total_ad_cost || 0);

        renderStatsAlerts(s);
        await statsSummaryTabulator.setData([{
            period: (data.date_from || range.dateFrom) + ' - ' + (data.date_to || range.dateTo),
            orders_revenue: ordersRevenue,
            orders: orders,
            buyouts_revenue: buyoutsRevenue,
            buyouts: buyouts,
            buyout_pct: orders ? buyouts / orders * 100 : 0,
            returns: Number(s.total_returns || 0),
            ad_cost: adCost,
            profit: buyoutsRevenue - adCost,
            stock_total: Number(s.total_stock || 0),
            stock_fbo: Number(s.total_stock_fbo || 0),
            stock_fbs: Number(s.total_stock_fbs || 0),
            ctr: Number(s.ctr || 0),
            rating: Number(s.avg_rating || 0),
        }]);

        var daily = (data.daily || []).map(function(row) {
            var orders = Number(row.orders_count || 0);
            var buyouts = Number(row.buyouts_count || 0);
            return Object.assign({}, row, {
                buyout_pct: orders ? buyouts / orders * 100 : 0,
            });
        });
        var dailyCount = document.getElementById('stats-daily-count');
        if (dailyCount) dailyCount.textContent = daily.length.toLocaleString('ru-RU') + ' дней';
        renderStatsFilterOptions(data.filter_options || {});
        await statsDailyTabulator.setData(daily);

        statsAllProducts = (data.products || []).map(function(row) {
            var barcodes = statsBarcodeText(row.barcodes || row.barcode || '');
            return Object.assign({}, row, {
                nm_id_display: row.nm_id,
                barcodes_display: barcodes,
                barcodes_search: statsBarcodeSearchText({barcodes_display: barcodes}),
            });
        });
        statsVisibleProducts = statsAllProducts.slice();
        await statsProductsTabulator.setData(prepareStatsProducts(statsAllProducts));
        statsSummaryTabulator.redraw(true);
        statsDailyTabulator.redraw(true);
        statsProductsTabulator.redraw(true);
        setStatsGridPending(false);
        applyStatsFilters();
        updateStatsProductsCount();
    } catch (e) {
        console.error('loadStatsGrid error:', e);
        setStatsGridPending(false);
        if (count) count.textContent = 'Ошибка загрузки';
        if (statsSummaryTabulator) statsSummaryTabulator.setData([]);
        if (statsDailyTabulator) statsDailyTabulator.setData([]);
        if (statsProductsTabulator) statsProductsTabulator.setData([]);
    }
}

document.addEventListener('input', function(e) {
    if (e.target && e.target.id === 'filter-article' && _currentSection === 'stats') {
        applyStatsTopSearch();
    }
    if (e.target && e.target.id === 'stats-flt-search' && _currentSection === 'stats') {
        applyStatsFilters();
    }
});
