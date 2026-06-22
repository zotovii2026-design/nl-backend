var statsSummaryTabulator = null;
var statsProductsTabulator = null;
var statsAllProducts = [];

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
    var period = document.getElementById('filter-period')?.value || 'yesterday';
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
            {title: 'Выручка', field: 'revenue', width: 120, hozAlign: 'right', formatter: statsMoneyFormatter},
            {title: 'Заказы', field: 'orders', width: 90, hozAlign: 'right', formatter: statsNumberFormatter},
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

function getStatsProductColumns() {
    return [
        {title: 'Фото', field: 'photo_main', width: 54, headerSort: false, formatter: statsPhotoFormatter},
        {title: 'Арт WB', field: 'nm_id', width: 105, headerFilter: 'input'},
        {title: 'Название', field: 'product_name', minWidth: 220, headerFilter: 'input', formatter: function(cell) {
            var value = cell.getValue() || '';
            return '<span title="' + esc(value) + '">' + esc(value) + '</span>';
        }},
        {title: 'Размер', field: 'size_name', width: 90, headerFilter: 'input'},
        {title: 'ШК', field: 'barcode', width: 130, headerFilter: 'input'},
        {title: 'Остаток', field: 'stock_total', width: 105, hozAlign: 'right', formatter: statsStockFormatter},
        {title: 'Заказы', field: 'orders_count', width: 90, hozAlign: 'right', sorter: 'number'},
        {title: 'Выкупы', field: 'buyouts_count', width: 90, hozAlign: 'right', sorter: 'number'},
        {title: 'Возвраты', field: 'returns_count', width: 90, hozAlign: 'right', sorter: 'number'},
        {title: '% выкупа', field: 'buyout_pct', width: 90, hozAlign: 'right', sorter: 'number', formatter: statsPercentFormatter},
        {title: 'Рейтинг', field: 'rating', width: 85, hozAlign: 'right', sorter: 'number', formatter: statsNumberFormatter},
        {title: 'Показы', field: 'impressions', width: 95, hozAlign: 'right', sorter: 'number'},
        {title: 'Клики', field: 'clicks', width: 85, hozAlign: 'right', sorter: 'number'},
        {title: 'CTR', field: 'ctr', width: 80, hozAlign: 'right', sorter: 'number', formatter: statsPercentFormatter},
        {title: 'Реклама', field: 'ad_cost', width: 105, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
        {title: 'Цена', field: 'price_display', width: 100, hozAlign: 'right', sorter: 'number', formatter: statsMoneyFormatter},
    ];
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
        groupBy: 'nm_id',
        groupHeader: function(value, count, data) {
            var first = data && data[0] ? data[0] : {};
            var title = first.product_name ? ' - ' + first.product_name : '';
            return 'Арт WB ' + value + title + ' (' + count + ')';
        },
    });
    statsProductsTabulator.on('dataFiltered', updateStatsProductsCount);
}

function prepareStatsProducts(products) {
    return (products || []).map(function(p) {
        var orders = Number(p.orders_count || 0);
        var buyouts = Number(p.buyouts_count || 0);
        var impressions = Number(p.impressions || 0);
        var clicks = Number(p.clicks || 0);
        return Object.assign({}, p, {
            stock_total: Number(p.stock_qty || 0) + Number(p.stock_fbo_qty || 0),
            buyout_pct: orders ? buyouts / orders * 100 : 0,
            ctr: impressions ? clicks / impressions * 100 : 0,
            price_display: p.wb_price_fact || p.price_discount || p.price || 0,
        });
    });
}

function applyStatsTopSearch() {
    if (!statsProductsTabulator) return;
    var q = (document.getElementById('filter-article')?.value || '').trim().toLowerCase();
    if (!q) {
        statsProductsTabulator.clearFilter(true);
        updateStatsProductsCount();
        return;
    }
    statsProductsTabulator.setFilter(function(row) {
        return String(row.nm_id || '').toLowerCase().includes(q)
            || String(row.vendor_code || '').toLowerCase().includes(q)
            || String(row.product_name || '').toLowerCase().includes(q)
            || String(row.barcode || '').toLowerCase().includes(q);
    });
    updateStatsProductsCount();
}

function updateStatsProductsCount() {
    var count = document.getElementById('stats-products-count');
    if (!count || !statsProductsTabulator) return;
    var total = statsAllProducts.length;
    var active = total;
    try {
        active = statsProductsTabulator.getData('active').length;
    } catch (e) {
        active = total;
    }
    count.textContent = active === total
        ? total.toLocaleString('ru-RU') + ' строк'
        : active.toLocaleString('ru-RU') + ' из ' + total.toLocaleString('ru-RU') + ' строк';
}

function setStatsGridPending(isPending) {
    ['stats-summary-tabulator', 'stats-products-tabulator'].forEach(function(id) {
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
        var revenue = Number(s.total_revenue || 0);
        var orders = Number(s.total_orders || 0);
        var buyouts = Number(s.total_buyouts || 0);
        var adCost = Number(s.total_ad_cost || 0);

        renderStatsAlerts(s);
        await statsSummaryTabulator.setData([{
            period: (data.date_from || range.dateFrom) + ' - ' + (data.date_to || range.dateTo),
            revenue: revenue,
            orders: orders,
            buyouts: buyouts,
            buyout_pct: orders ? buyouts / orders * 100 : 0,
            returns: Number(s.total_returns || 0),
            ad_cost: adCost,
            profit: revenue - adCost,
            stock_total: Number(s.total_stock || 0),
            stock_fbo: Number(s.total_stock_fbo || 0),
            stock_fbs: Number(s.total_stock_fbs || 0),
            ctr: Number(s.ctr || 0),
            rating: Number(s.avg_rating || 0),
        }]);

        statsAllProducts = prepareStatsProducts(data.products || []);
        await statsProductsTabulator.setData(statsAllProducts);
        statsSummaryTabulator.redraw(true);
        statsProductsTabulator.redraw(true);
        setStatsGridPending(false);
        applyStatsTopSearch();
        updateStatsProductsCount();
    } catch (e) {
        console.error('loadStatsGrid error:', e);
        setStatsGridPending(false);
        if (count) count.textContent = 'Ошибка загрузки';
        if (statsSummaryTabulator) statsSummaryTabulator.setData([]);
        if (statsProductsTabulator) statsProductsTabulator.setData([]);
    }
}

document.addEventListener('input', function(e) {
    if (e.target && e.target.id === 'filter-article' && _currentSection === 'stats') {
        applyStatsTopSearch();
    }
});
