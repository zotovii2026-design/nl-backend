// ===== ADS VIEW SWITCHER =====
var _adsCurrentView = 'rk';
var adsArtsTabulator = null;

function switchAdsView(view) {
    _adsCurrentView = view;
    // Update buttons
    document.querySelectorAll('.ads-view-btn').forEach(function(btn) { btn.classList.remove('active'); });
    var activeBtn = document.getElementById('ads-view-' + view);
    if (activeBtn) {
        activeBtn.classList.add('active');
        activeBtn.style.background = '#6c5ce7';
        activeBtn.style.color = '#fff';
        activeBtn.style.borderColor = '#6c5ce7';
    }
    var inactiveBtn = document.getElementById('ads-view-' + (view === 'rk' ? 'art' : 'rk'));
    if (inactiveBtn) {
        inactiveBtn.style.background = '#fff';
        inactiveBtn.style.color = '#333';
        inactiveBtn.style.borderColor = '#ddd';
    }
    // Toggle containers
    var rkContainer = document.getElementById('ads-rk-container');
    var artContainer = document.getElementById('ads-arts-container');
    var artTotals = document.getElementById('ads-arts-totals');
    if (view === 'rk') {
        if (rkContainer) rkContainer.style.display = '';
        if (artContainer) artContainer.style.display = 'none';
        if (artTotals) artTotals.style.display = 'none';
    } else {
        if (rkContainer) rkContainer.style.display = 'none';
        if (artContainer) artContainer.style.display = '';
        if (artTotals) artTotals.style.display = '';
        loadAdsArts();
    }
}

function loadAdsArts() {
    var periodVal = document.getElementById('ads-period').value;
    var url;
    if (periodVal === 'calendar') {
        var from = document.getElementById('ads-date-from').value;
        var to = document.getElementById('ads-date-to').value;
        if (!from || !to) { alert('Укажите обе даты'); return; }
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + ORG_ID + '&date_from=' + from + '&date_to=' + to;
    } else {
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + ORG_ID + '&days=' + periodVal;
    }
    fetch(url, {headers:{'Authorization':'Bearer '+TOKEN}})
        .then(function(r) { return r.json(); })
        .then(function(d) {
            renderAdsArtsCards(d.totals || {});
            renderAdsArtsTable(d.items || []);
        })
        .catch(function(e) { console.error('loadAdsArts error:', e); });
}

function renderAdsArtsCards(t) {
    var el = document.getElementById('ads-arts-cards');
    if (!el) return;
    var fmt = function(v, suffix) {
        if (v == null) return '—';
        suffix = suffix || '';
        if (v >= 1000) return v.toLocaleString('ru-RU', {maximumFractionDigits:0}) + suffix;
        return (typeof v === 'number' ? v.toFixed(2) : v) + suffix;
    };
    el.innerHTML = [
        card('Расход', fmt(t.spent, ' ₽'), '#e17055'),
        card('Показы', fmt(t.views), '#6c5ce7'),
        card('Клики', fmt(t.clicks), '#0984e3'),
        card('CTR', (t.ctr || 0) + '%', '#00b894'),
        card('CPC', fmt(t.cpc, ' ₽'), '#fdcb6e'),
        card('Заказы', fmt(t.orders), '#00cec9'),
        card('CR', (t.cr || 0) + '%', '#e84393'),
        card('Артикулов', t.items_count || 0, '#636e72'),
    ].join('');
}

function card(title, value, color) {
    return '<div style="background:#fff;border-radius:8px;padding:8px 14px;text-align:center;min-width:90px;border:1px solid #eee">' +
        '<div style="font-size:.72em;color:#999;margin-bottom:2px">' + title + '</div>' +
        '<div style="font-size:1em;font-weight:700;color:' + color + '">' + value + '</div></div>';
}

function renderAdsArtsTable(items) {
    var container = document.getElementById('ads-arts-tabulator');
    if (!container) return;

    if (adsArtsTabulator) {
        adsArtsTabulator.setData(items);
        return;
    }

    adsArtsTabulator = new Tabulator(container, {
        data: items,
        columns: [
            {
                title: 'Фото', field: 'photo', width: 50, headerSort: false,
                formatter: function(cell) {
                    var url = cell.getValue();
                    if (!url) return '<div style="width:32px;height:32px;background:#f0f0f0;border-radius:4px"></div>';
                    var thumb = url.replace('/hq/', '/c246x328/').replace('/big/', '/c246x328/');
                    return '<img src="' + thumb + '" style="width:32px;height:32px;object-fit:cover;border-radius:4px" loading="lazy">';
                }
            },
            {
                title: 'Арт WB', field: 'nm_id', width: 100, headerSort: true,
                formatter: function(cell) {
                    var v = cell.getValue();
                    return '<a href="https://www.wildberries.ru/catalog/' + v + '/detail.aspx" target="_blank" style="color:#6c5ce7;text-decoration:none;font-weight:600">' + v + '</a>';
                }
            },
            { title: 'Арт продавца', field: 'vendor_code', width: 100, headerSort: true, tooltip: true },
            { title: 'Товар', field: 'name', width: 200, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
            {
                title: 'Расход ₽', field: 'spent', width: 100, headerSort: true, hozAlign: 'right',
                formatter: function(cell) {
                    var v = parseFloat(cell.getValue()) || 0;
                    return '<b style="color:#e17055">' + v.toLocaleString('ru-RU', {maximumFractionDigits:0}) + '</b>';
                },
                bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                    var v = parseFloat(cell.getValue()) || 0;
                    return '<b style="color:#e17055">' + v.toLocaleString('ru-RU', {maximumFractionDigits:0}) + ' ₽</b>';
                }
            },
            {
                title: 'Показы', field: 'views', width: 80, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { return (cell.getValue()||0).toLocaleString('ru-RU'); },
                bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0).toLocaleString('ru-RU') + '</b>'; }
            },
            {
                title: 'Клики', field: 'clicks', width: 70, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { return (cell.getValue()||0).toLocaleString('ru-RU'); },
                bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0).toLocaleString('ru-RU') + '</b>'; }
            },
            {
                title: 'CTR %', field: 'ctr', width: 65, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(2) + '%' : '—'; }
            },
            {
                title: 'CPC ₽', field: 'cpc', width: 65, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(2) : '—'; }
            },
            {
                title: 'Заказы', field: 'orders', width: 70, headerSort: true, hozAlign: 'right',
                bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0) + '</b>'; }
            },
            {
                title: 'CR %', field: 'cr', width: 60, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(1) + '%' : '—'; }
            },
            {
                title: 'Дней', field: 'active_days', width: 55, headerSort: true, hozAlign: 'right',
                formatter: function(cell) { return cell.getValue() || 0; }
            },
        ],
        height: '55vh',
        layout: 'fitColumns',
        placeholder: '<div style="padding:20px;text-align:center;color:#999">📭 Нет данных за период</div>',
        headerSortClickElement: 'header',
        sortable: true,
        pagination: false,
        movableColumns: true,
        persistence: { columns: true, sort: true },
        persistenceID: 'ads-arts-grid-state',
        persistenceMode: 'local',
    });
}
