// ===== ADS VIEW SWITCHER =====
var _adsCurrentView = 'rk';
var adsArtsTabulator = null;
var _adsStatusFilters = ['7', '9']; // default: active + paused
var _adsExpandedRow = null; // текущая раскрытая строка

function switchAdsView(view) {
    _adsCurrentView = view;
    document.querySelectorAll('.ads-view-btn').forEach(function(btn) { btn.classList.remove('active'); });
    var activeBtn = document.getElementById('ads-view-' + view);
    if (activeBtn) {
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
    var rkContainer = document.getElementById('ads-rk-container');
    var artContainer = document.getElementById('ads-arts-container');
    if (view === 'rk') {
        if (rkContainer) rkContainer.style.display = '';
        if (artContainer) artContainer.style.display = 'none';
        applyAdsFilters();
    } else {
        if (rkContainer) rkContainer.style.display = 'none';
        if (artContainer) artContainer.style.display = '';
        loadAdsArts();
    }
}

// ===== STATUS FILTER TOGGLE =====
// Статусы WB API: 7=Активна, 9=Приостановлена, 11=Завершена
var _statusColors = {
    '7':  { on: '#00b894', off: '#fff', offText: '#636e72' },
    '9':  { on: '#fdcb6e', off: '#fff', offText: '#636e72' },
    '11': { on: '#dfe6e9', off: '#fff', offText: '#636e72' }
};

function toggleAdsStatusFilter(btn) {
    var status = btn.dataset.status;
    var idx = _adsStatusFilters.indexOf(status);
    if (idx >= 0) {
        _adsStatusFilters.splice(idx, 1);
        var sc = _statusColors[status] || { off: '#fff', offText: '#636e72' };
        btn.style.background = sc.off;
        btn.style.color = sc.offText;
    } else {
        _adsStatusFilters.push(status);
        var sc = _statusColors[status] || { on: '#b2bec3' };
        btn.style.background = sc.on;
        btn.style.color = '#fff';
    }
    // Re-apply filters to current view
    if (_adsCurrentView === 'rk') {
        applyAdsFilters();
    } else {
        loadAdsArts();
    }
}

function loadAdsArts() {
    var periodVal = document.getElementById('ads-period').value;
    var statuses = _adsStatusFilters.join(',');
    var url;
    if (periodVal === 'calendar') {
        var from = document.getElementById('ads-date-from').value;
        var to = document.getElementById('ads-date-to').value;
        if (!from || !to) { alert('Укажите обе даты'); return; }
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + ORG_ID + '&date_from=' + from + '&date_to=' + to + '&statuses=' + statuses;
    } else {
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + ORG_ID + '&days=' + periodVal + '&statuses=' + statuses;
    }
    fetch(url, {headers:{'Authorization':'Bearer '+TOKEN}})
        .then(function(r) { return r.json(); })
        .then(function(d) {
            // Обновим карточки метрик
            updateAdsArtsMetrics(d.totals || {});
            renderAdsArtsTable(d.items || []);
        })
        .catch(function(e) { console.error('loadAdsArts error:', e); });
}

function updateAdsArtsMetrics(totals) {
    var el;
    el = document.getElementById('ad-spent'); if (el) el.textContent = (totals.spent || 0).toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽';
    el = document.getElementById('ad-views'); if (el) el.textContent = (totals.views || 0).toLocaleString('ru-RU');
    el = document.getElementById('ad-clicks'); if (el) el.textContent = (totals.clicks || 0).toLocaleString('ru-RU');
    el = document.getElementById('ad-ctr'); if (el) el.textContent = (totals.ctr || 0).toFixed(2) + '%';
    el = document.getElementById('ad-cpc'); if (el) el.textContent = (totals.cpc || 0).toFixed(2) + ' ₽';
    el = document.getElementById('ad-orders'); if (el) el.textContent = totals.orders || 0;
    el = document.getElementById('ad-cr'); if (el) el.textContent = (totals.cr || 0).toFixed(1) + '%';
    el = document.getElementById('ad-arts-count'); if (el) el.textContent = totals.items_count || 0;
}

function renderAdsArtsTable(items) {
    var container = document.getElementById('ads-arts-tabulator');
    if (!container) return;

    if (adsArtsTabulator) {
        // Закрываем раскрытую строку при обновлении данных
        closeArtCampaigns();
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
                title: 'РК', field: 'campaigns_count', width: 50, headerSort: true, hozAlign: 'center',
                formatter: function(cell) {
                    var v = cell.getValue() || 0;
                    if (v > 0) return '<span style="background:#6c5ce7;color:#fff;padding:2px 6px;border-radius:8px;font-size:.78em;font-weight:600">' + v + '</span>';
                    return '<span style="color:#999;font-size:.82em">0</span>';
                }
            },
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

    setTimeout(function() { initArtsRowExpand(); }, 500);
}

// Refresh wrapper
function refreshAds() {
    loadAds();
    if (_adsCurrentView === 'art') {
        loadAdsArts();
    }
}

// ===== Раскрытие строки — список РК для этого артикула =====
function initArtsRowExpand() {
    if (!adsArtsTabulator) return;
    adsArtsTabulator.on('rowClick', function(e, row) {
        if (e.target.closest('a') || e.target.closest('.art-campaigns-row')) return;
        toggleArtCampaigns(row);
    });
}

// Закрыть раскрытую строку
function closeArtCampaigns() {
    if (_adsExpandedRow) {
        var el = _adsExpandedRow.getElement();
        var next = el.nextElementSibling;
        if (next && next.classList.contains('art-campaigns-row')) {
            next.remove();
        }
        _adsExpandedRow = null;
    }
}

function toggleArtCampaigns(row) {
    var el = row.getElement();
    var existing = el.nextElementSibling;

    // Если кликнули на уже раскрытую — закрыть
    if (_adsExpandedRow === row) {
        closeArtCampaigns();
        return;
    }

    // Сначала закрыть предыдущую
    closeArtCampaigns();

    // Раскрыть текущую
    _adsExpandedRow = row;

    var data = row.getData();
    var nmId = data.nm_id;
    var camps = data.campaigns || [];

    var tr = document.createElement('tr');
    tr.className = 'art-campaigns-row';
    var td = document.createElement('td');
    td.colSpan = 12;
    td.style.cssText = 'padding:12px 16px;background:#f8f9fa;border-bottom:2px solid #6c5ce7';

    if (!camps.length) {
        td.innerHTML = '<div style="padding:8px;color:#999;font-size:.85em">📭 Нет РК с расходом за период</div>';
        tr.appendChild(td);
        el.parentNode.insertBefore(tr, el.nextSibling);
        return;
    }

    var statusIcons = {'4':'⏳','7':'🟢','8':'❌','9':'⏸','11':'☑'};
    var typeNames = {'4':'Авто','5':'Поиск','6':'Каталог','7':'Таргет','8':'Рек.','9':'Аукцион'};

    var html = '<div style="margin-bottom:8px;display:flex;align-items:center;gap:8px">';
    html += '<span style="font-weight:600;color:#6c5ce7;font-size:.9em">📢 РК для ' + nmId + ' (' + camps.length + ')</span>';
    html += '<span style="font-size:.75em;color:#999;background:#e8f8f5;padding:2px 6px;border-radius:3px">Данные из WB по nm_id</span>';
    html += '</div>';

    html += '<table style="width:100%;border-collapse:collapse;font-size:.82em">';
    html += '<tr style="background:#e0e0e0">';
    html += '<th style="padding:4px 8px;text-align:left">РК</th>';
    html += '<th style="padding:4px 8px">Статус</th>';
    html += '<th style="padding:4px 8px">Тип</th>';
    html += '<th style="padding:4px 8px;text-align:right">Расход</th>';
    html += '<th style="padding:4px 8px;text-align:right">Показы</th>';
    html += '<th style="padding:4px 8px;text-align:right">Клики</th>';
    html += '<th style="padding:4px 8px;text-align:right">CTR</th>';
    html += '<th style="padding:4px 8px;text-align:right">CPC</th>';
    html += '<th style="padding:4px 8px;text-align:right">Заказы</th>';
    html += '<th style="padding:4px 8px;text-align:right">В корзину</th>';
    html += '</tr>';

    camps.forEach(function(c, i) {
        var bg = i % 2 === 0 ? '#fff' : '#f8f9fa';
        var cpc = c.clicks > 0 ? (c.spent_share / c.clicks).toFixed(2) : '—';
        var ctr = c.clicks > 0 ? (c.clicks / c.views * 100).toFixed(2) + '%' : '—';
        html += '<tr style="background:' + bg + '">';
        html += '<td style="padding:4px 8px;font-weight:600">' + c.name + '<br><span style="color:#999;font-size:.8em">ID: ' + c.campaign_id + '</span></td>';
        html += '<td style="padding:4px 8px;text-align:center">' + (statusIcons[c.status] || '') + '</td>';
        html += '<td style="padding:4px 8px;text-align:center">' + (typeNames[c.type] || c.type || '—') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right;font-weight:600;color:#e17055">' + c.spent_share.toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.views||0).toLocaleString('ru-RU') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.clicks||0).toLocaleString('ru-RU') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + ctr + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + cpc + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.orders||0) + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.atbs||0) + '</td>';
        html += '</tr>';
    });

    html += '</table>';
    td.innerHTML = html;
    tr.appendChild(td);
    el.parentNode.insertBefore(tr, el.nextSibling);
}
