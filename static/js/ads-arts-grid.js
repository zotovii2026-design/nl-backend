// ===== ADS VIEW SWITCHER =====
var _adsCurrentView = 'rk';
var adsArtsTabulator = null;
var _adsStatusFilters = ['7', '9']; // default: active + paused

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
function toggleAdsStatusFilter(btn) {
    var status = btn.dataset.status;
    var idx = _adsStatusFilters.indexOf(status);
    if (idx >= 0) {
        _adsStatusFilters.splice(idx, 1);
        btn.style.background = '#fff';
        btn.style.color = '#636e72';
    } else {
        _adsStatusFilters.push(status);
        if (status === '7') { btn.style.background = '#00b894'; btn.style.color = '#fff'; }
        else if (status === '9') { btn.style.background = '#fdcb6e'; btn.style.color = '#fff'; }
        else if (status === '11') { btn.style.background = '#dfe6e9'; btn.style.color = '#2d3436'; }
        else { btn.style.background = '#b2bec3'; btn.style.color = '#fff'; }
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
    var statuses = (typeof _adsStatusFilters !== 'undefined' ? _adsStatusFilters : ['7', '9']).join(',');
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
            renderAdsArtsTable(d.items || []);
        })
        .catch(function(e) { console.error('loadAdsArts error:', e); });
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

    setTimeout(function() { initArtsRowExpand(); }, 500);
}

// Refresh wrapper
function refreshAds() {
    loadAds();
    if (typeof _adsCurrentView !== 'undefined' && _adsCurrentView === 'art') {
        loadAdsArts();
    }
}
// ===== Expand row on click — show campaigns for this art =====
function initArtsRowExpand() {
    if (!adsArtsTabulator) return;
    adsArtsTabulator.on('rowClick', function(e, row) {
        if (e.target.closest('a') || e.target.closest('.art-campaigns-row')) return;
        toggleArtCampaigns(row);
    });
}

function toggleArtCampaigns(row) {
    var el = row.getElement();
    var existing = el.nextElementSibling;
    if (existing && existing.classList.contains('art-campaigns-row')) {
        existing.remove();
        return;
    }

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

    var statusMap = {'4':'⏳','7':'🟢','8':'❌','9':'⏸','11':'☑'};
    var typeMap = {'4':'Авто','5':'Поиск','6':'Каталог','7':'Таргет','8':'Рек.','9':'Аукцион'};

    var html = '<div style="margin-bottom:6px;display:flex;align-items:center;gap:8px">';
    html += '<span style="font-weight:600;color:#6c5ce7;font-size:.9em">📢 РК для ' + nmId + ' (' + camps.length + ')</span>';
    html += '<span style="font-size:.75em;color:#999;background:#fff3cd;padding:2px 6px;border-radius:3px">Распределение по кликам</span>';
    html += '</div>';

    html += '<table style="width:100%;border-collapse:collapse;font-size:.82em">';
    html += '<tr style="background:#e8e8e8">';
    html += '<th style="padding:4px 8px;text-align:left">РК</th>';
    html += '<th style="padding:4px 8px">Статус</th>';
    html += '<th style="padding:4px 8px">Тип</th>';
    html += '<th style="padding:4px 8px;text-align:right">Расход</th>';
    html += '<th style="padding:4px 8px;text-align:right">Показы</th>';
    html += '<th style="padding:4px 8px;text-align:right">Клики</th>';
    html += '<th style="padding:4px 8px;text-align:right">CTR</th>';
    html += '<th style="padding:4px 8px;text-align:right">Заказы</th>';
    html += '<th style="padding:4px 8px;text-align:right">В корзину</th>';
    html += '</tr>';

    camps.forEach(function(c, i) {
        var bg = i % 2 === 0 ? '#fff' : '#f8f9fa';
        html += '<tr style="background:' + bg + '">';
        html += '<td style="padding:4px 8px;font-weight:600">' + c.name + '<br><span style="color:#999;font-size:.8em">ID: ' + c.campaign_id + '</span></td>';
        html += '<td style="padding:4px 8px;text-align:center">' + (statusMap[c.status] || c.status) + '</td>';
        html += '<td style="padding:4px 8px;text-align:center">' + (typeMap[c.type] || c.type || '—') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right;font-weight:600;color:#e17055">' + c.spent_share.toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.views||0).toLocaleString('ru-RU') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.clicks||0).toLocaleString('ru-RU') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.ctr ? c.ctr.toFixed(2) + '%' : '—') + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.orders||0) + '</td>';
        html += '<td style="padding:4px 8px;text-align:right">' + (c.atbs||0) + '</td>';
        html += '</tr>';
    });

    html += '</table>';
    td.innerHTML = html;
    tr.appendChild(td);
    el.parentNode.insertBefore(tr, el.nextSibling);
}
