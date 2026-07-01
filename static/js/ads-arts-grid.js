// ===== ADS VIEW SWITCHER =====
var _adsCurrentView = 'rk';
var adsArtsTabulator = null;
var _adsStatusFilters = ['9', '11']; // default: active + paused
var _adsExpandedRow = null; // текущая раскрытая строка
var _adsAllArtsData = [];  // Полные данные до фильтрации
var _adsArtsLoadSeq = 0;
var _adsRefreshStatusSeq = 0;
var _adsArtCharts = [];

// Сброс кэша Tabulator при смене версии колонок
(function() {
    var VER = 'ads-arts-grid-v8';
    if (localStorage.getItem('ads-arts-grid-ver') !== VER) {
        localStorage.removeItem('ads-arts-grid-state-columns');
        localStorage.removeItem('ads-arts-grid-state-sort');
        localStorage.setItem('ads-arts-grid-ver', VER);
    }
})();

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
var _statusColors = {
    '9':  { on: '#00b894', off: '#fff', offText: '#636e72' },
    '11': { on: '#fdcb6e', off: '#fff', offText: '#636e72' },
    '7':  { on: '#dfe6e9', off: '#fff', offText: '#636e72' }
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
    loadAds();
    if (_adsCurrentView === 'art') {
        loadAdsArts();
    }
}

function loadAdsArts() {
    var orgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId() : ((typeof getOrgId === 'function') ? getOrgId() : (typeof ORG_ID !== 'undefined' ? ORG_ID : ''));
    if (!orgId || orgId === 'null') return;
    var requestSeq = ++_adsArtsLoadSeq;
    var range = getAdsDateRange();
    var statuses = _adsStatusFilters.join(',');
    var url;
    if (range.days) {
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + encodeURIComponent(orgId) + '&days=' + range.days + '&statuses=' + statuses;
    } else if (range.date_from && range.date_to) {
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + encodeURIComponent(orgId) + '&date_from=' + range.date_from + '&date_to=' + range.date_to + '&statuses=' + statuses;
    } else {
        url = '/api/v1/nl/ad-stats/by-art?org_id=' + encodeURIComponent(orgId) + '&days=9&statuses=' + statuses;
    }
    if (typeof getAdsProductFilterQuery === 'function') url += getAdsProductFilterQuery();
    fetch(url, {headers:{'Authorization':'Bearer '+TOKEN}})
        .then(function(r) {
            if (r.status === 401) throw new Error('Нет доступа к выбранному магазину. Переключите магазин или обновите страницу.');
            if (!r.ok) throw new Error('Ошибка загрузки рекламы: ' + r.status);
            return r.json();
        })
        .then(function(d) {
            var currentOrgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId() : orgId;
            if (requestSeq !== _adsArtsLoadSeq || orgId !== currentOrgId) return;
            updateAdsArtsMetrics(d.totals || {});
            renderAdsArtsTable(d.items || []);
        })
        .catch(function(e) {
            console.error('loadAdsArts error:', e);
            if (typeof showAdsLoadError === 'function') showAdsLoadError(e.message);
        });
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
    el = document.getElementById('ad-atbs'); if (el) el.textContent = totals.atbs || 0;
    el = document.getElementById('ad-drr'); if (el) el.textContent = totals.drr ? totals.drr + '%' : '—';
    el = document.getElementById('ad-drr-total'); if (el) el.textContent = totals.drr_total ? totals.drr_total + '%' : '—';
}

// ===== Конфигурация колонок — по образцу ads-grid.js =====
function getAdsArtsColumns() {
    return [
        // === 📦 Товар ===
        {
            title: '📦 Товар',
            columns: [
                {
                    title: 'Фото', field: 'photo', width: 60, headerSort: false,
                    formatter: function(cell) {
                        var url = cell.getValue();
                        if (!url) return '<div style="width:32px;height:32px;background:#f0f0f0;border-radius:4px"></div>';
                        var thumb = url.replace('/hq/', '/c246x328/').replace('/big/', '/c246x328/');
                        return '<img src="' + thumb + '" style="width:32px;height:32px;object-fit:cover;border-radius:4px" loading="lazy">';
                    }
                },
                {
                    title: 'Арт WB', field: 'nm_id', width: 110, headerSort: true,
                    formatter: function(cell) {
                        var v = cell.getValue();
                        return '<a href="https://www.wildberries.ru/catalog/' + v + '/detail.aspx" target="_blank" style="color:#6c5ce7;text-decoration:none;font-weight:600">' + v + '</a>';
                    }
                },
                { title: 'Арт продавца', field: 'vendor_code', width: 120, headerSort: true, tooltip: true },
                {
                    title: 'Товар', field: 'name', width: 250, headerSort: true, tooltip: true,
                    formatter: function(cell) {
                        var v = cell.getValue() || '';
                        if (v.length > 40) v = v.substring(0, 40) + '…';
                        return '<div style="white-space:normal;line-height:1.3;font-size:.82em">' + v + '</div>';
                    }
                },
                { title: 'Класс', field: 'product_class', width: 70, headerSort: true, hozAlign: 'center' },
                { title: 'Статус', field: 'product_status', width: 110, headerSort: true, tooltip: true },
                {
                    title: 'РК', field: 'campaigns_count', width: 55, headerSort: true, hozAlign: 'center',
                    formatter: function(cell) {
                        var v = cell.getValue() || 0;
                        if (v > 0) return '<span style="background:#6c5ce7;color:#fff;padding:2px 6px;border-radius:8px;font-size:.78em;font-weight:600">' + v + '</span>';
                        return '<span style="color:#999;font-size:.82em">0</span>';
                    }
                },
            ]
        },
        // === 💰 Финансы ===
        {
            title: '💰 Финансы',
            columns: [
                {
                    title: 'Расход ₽', field: 'spent', width: 110, headerSort: true, hozAlign: 'right',
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
                    title: 'ДРР по РК %', field: 'drr', width: 105, headerSort: true, hozAlign: 'right',
                    tooltip: function(e, cell) {
                        var d = cell.getRow().getData();
                        var tip = 'ДРР по РК для артикула';
                        tip += '\nРасход: ' + (d.spent||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽';
                        tip += '\nЗаказы из рекламы: ' + (d.total_orders||0);
                        tip += '\nСумма заказов из рекламы: ' + (d.total_revenue||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽';
                        tip += '\nФормула: расход / сумма заказов из рекламы';
                        return tip;
                    },
                    formatter: function(cell) {
                        var v = parseFloat(cell.getValue())||0;
                        if (!v) return '—';
                        var color = v > 50 ? '#e74c3c' : v > 25 ? '#e17055' : '#00b894';
                        return '<span style="color:' + color + ';font-weight:600">' + v.toFixed(1) + '%</span>';
                    },
                },
                {
                    title: 'ДРР товара %', field: 'drr_product', width: 105, headerSort: true, hozAlign: 'right',
                    tooltip: function(e, cell) {
                        var d = cell.getRow().getData();
                        var tip = 'ДРР товара за период';
                        tip += '\nРасход: ' + (d.spent||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽';
                        tip += '\nВсе заказы товара: ' + (d.total_orders_product||0);
                        tip += '\nСумма всех заказов товара: ' + (d.total_revenue_product||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽';
                        tip += '\nФормула: расход / сумма всех заказов товара';
                        return tip;
                    },
                    formatter: function(cell) {
                        var v = parseFloat(cell.getValue())||0;
                        if (!v) return '—';
                        var color = v > 50 ? '#e74c3c' : v > 25 ? '#e17055' : '#00b894';
                        return '<span style="color:' + color + ';font-weight:600">' + v.toFixed(1) + '%</span>';
                    },
                },
            ]
        },
        // === 📊 Метрики ===
        {
            title: '📊 Метрики',
            columns: [
                {
                    title: 'Показы', field: 'views', width: 90, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) { return (cell.getValue()||0).toLocaleString('ru-RU'); },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0).toLocaleString('ru-RU') + '</b>'; }
                },
                {
                    title: 'Клики', field: 'clicks', width: 80, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) { return (cell.getValue()||0).toLocaleString('ru-RU'); },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0).toLocaleString('ru-RU') + '</b>'; }
                },
                {
                    title: 'CTR %', field: 'ctr', width: 75, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(2) + '%' : '—'; }
                },
                {
                    title: 'CPC ₽', field: 'cpc', width: 75, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(2) : '—'; }
                },
                {
                    title: 'Заказы', field: 'orders', width: 80, headerSort: true, hozAlign: 'right',
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) { return '<b>' + (cell.getValue()||0) + '</b>'; }
                },
                {
                    title: 'CR %', field: 'cr', width: 70, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) { var v = parseFloat(cell.getValue())||0; return v ? v.toFixed(1) + '%' : '—'; }
                },
            ]
        },
    ];
}

// Кастомный стиль заголовков — как в ads-grid.js
(function(){
    var s = document.createElement('style');
    s.textContent = '#ads-arts-tabulator .tabulator-col-title{font-size:8px!important;line-height:1.1!important;padding:2px 4px!important}#ads-arts-tabulator .tabulator-col .tabulator-col-content{padding:2px 4px!important}#ads-arts-tabulator .tabulator-cell{font-size:11px!important}';
    document.head.appendChild(s);
})();

function renderAdsArtsTable(items) {
    var container = document.getElementById('ads-arts-tabulator');
    if (!container) return;

    if (adsArtsTabulator) {
        closeArtCampaigns();
        _adsAllArtsData = items || [];
        if (typeof hasAdsProductFilters !== 'function' || !hasAdsProductFilters()) populateAdsFilterOptions();
        filterAdsArtsLocally();
        return;
    }
    _adsAllArtsData = items || [];
    if (typeof hasAdsProductFilters !== 'function' || !hasAdsProductFilters()) populateAdsFilterOptions();

    container.style.width = '100%';
    adsArtsTabulator = new Tabulator(container, {
        data: items,
        columns: getAdsArtsColumns(),
        height: '55vh',
        layout: 'fitDataFill',
        renderHorizontal: 'virtual',
        placeholder: '<div style="padding:20px;text-align:center;color:#999">📭 Нет данных за период</div>',
        headerSortClickElement: 'header',
        sortable: true,
        pagination: false,
        movableColumns: true,
        resizable: true,
        persistence: { columns: true, sort: true },
        persistenceID: 'ads-arts-grid-state-v3',
        persistenceMode: 'local',
    });

    setTimeout(function() { initArtsRowExpand(); }, 500);
}

// ===== COLUMN FILTERS =====
var _adsFilterReloadTimer = null;

function filterAdsArtsLocally() {
    var search = (document.getElementById('ads-flt-search')?.value || '').toLowerCase();
    var fltStatus = document.getElementById('ads-flt-status')?.value || '';
    var fltClass = document.getElementById('ads-flt-class')?.value || '';
    var fltBrand = document.getElementById('ads-flt-brand')?.value || '';

    var filtered = _adsAllArtsData;

    if (search) {
        filtered = filtered.filter(function(p) {
            return (p.name || '').toLowerCase().indexOf(search) >= 0 ||
                   String(p.nm_id || '').indexOf(search) >= 0 ||
                   (p.vendor_code || '').toLowerCase().indexOf(search) >= 0;
        });
    }
    if (fltStatus) filtered = filtered.filter(function(p) { return (p.product_status || '') === fltStatus; });
    if (fltClass) filtered = filtered.filter(function(p) { return (p.product_class || '') === fltClass; });
    if (fltBrand) filtered = filtered.filter(function(p) { return (p.brand || '') === fltBrand; });

    if (adsArtsTabulator) adsArtsTabulator.replaceData(filtered);
    var countEl = document.getElementById('ads-filter-count');
    if (countEl) countEl.textContent = filtered.length + ' из ' + _adsAllArtsData.length;
}

function applyAdsColumnFilters() {
    if (_adsCurrentView === 'rk') {
        if (typeof applyAdsFilters === 'function') applyAdsFilters();
    } else {
        filterAdsArtsLocally();
    }
    clearTimeout(_adsFilterReloadTimer);
    _adsFilterReloadTimer = setTimeout(function() {
        if (typeof loadAds === 'function') loadAds();
        if (_adsCurrentView === 'art') loadAdsArts();
    }, 250);
}

function resetAdsColumnFilters() {
    var el;
    el = document.getElementById('ads-flt-status'); if (el) el.value = '';
    el = document.getElementById('ads-flt-class'); if (el) el.value = '';
    el = document.getElementById('ads-flt-brand'); if (el) el.value = '';
    el = document.getElementById('ads-flt-search'); if (el) el.value = '';
    applyAdsColumnFilters();
}

function populateAdsFilterOptions() {
    if (!_adsAllArtsData.length) return;
    var brands = [];
    var statuses = [];
    var classes = [];
    var seen = {};
    var seenStatus = {};
    var seenClass = {};
    _adsAllArtsData.forEach(function(p) {
        if (p.brand && !seen[p.brand]) { seen[p.brand] = true; brands.push(p.brand); }
        if (p.product_status && !seenStatus[p.product_status]) { seenStatus[p.product_status] = true; statuses.push(p.product_status); }
        if (p.product_class && !seenClass[p.product_class]) { seenClass[p.product_class] = true; classes.push(p.product_class); }
    });
    brands.sort();
    statuses.sort();
    classes.sort();
    if (typeof fillAdsSelect === 'function') {
        fillAdsSelect('ads-flt-brand', 'Бренд: все', brands);
        fillAdsSelect('ads-flt-status', 'Статус: все', statuses);
        fillAdsSelect('ads-flt-class', 'Класс: все', classes);
    }
}

var _adsRefreshTimer = null;

function formatAdsRefreshSeconds(seconds) {
    seconds = Math.max(0, parseInt(seconds || 0, 10));
    var mins = Math.floor(seconds / 60);
    var secs = seconds % 60;
    if (mins >= 60) {
        var hours = Math.floor(mins / 60);
        var restMins = mins % 60;
        return hours + ' ч ' + (restMins < 10 ? '0' : '') + restMins + ' мин';
    }
    return mins + ':' + (secs < 10 ? '0' : '') + secs;
}

function formatAdsRefreshDate(value) {
    if (!value) return '';
    var d = new Date(value);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function renderAdsRefreshStatus(data, prefix) {
    var statusEl = document.getElementById('ads-updated');
    var btn = document.getElementById('ads-refresh-btn');
    if (!statusEl) return;

    if (_adsRefreshTimer) {
        clearInterval(_adsRefreshTimer);
        _adsRefreshTimer = null;
    }

    var remaining = parseInt((data && data.cooldown_remaining_seconds) || 0, 10);
    var lastSync = formatAdsRefreshDate(data && data.last_sync_at);
    var lastDate = data && data.last_stat_date ? data.last_stat_date : '';
    var base = prefix || (lastSync ? 'Последний сбор: ' + lastSync : 'Сбор еще не запускался');
    if (lastDate) base += ' · данные до ' + lastDate;

    function paint(sec) {
        if (sec > 0) {
            statusEl.textContent = base + ' · следующий запуск через ' + formatAdsRefreshSeconds(sec);
            if (btn) btn.disabled = true;
        } else {
            statusEl.textContent = base + ' · можно обновить';
            if (btn) btn.disabled = false;
        }
    }

    paint(remaining);
    if (remaining > 0) {
        _adsRefreshTimer = setInterval(function() {
            remaining -= 1;
            paint(remaining);
            if (remaining <= 0) {
                clearInterval(_adsRefreshTimer);
                _adsRefreshTimer = null;
            }
        }, 1000);
    }
}

function loadAdsRefreshStatus() {
    var orgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId() : ((typeof getOrgId === 'function') ? getOrgId() : (typeof ORG_ID !== 'undefined' ? ORG_ID : ''));
    if (!orgId || orgId === 'null') return;
    var requestSeq = ++_adsRefreshStatusSeq;
    fetch('/api/v1/nl/ad-stats/refresh-status?org_id=' + encodeURIComponent(orgId), {
        headers: {'Authorization': 'Bearer ' + TOKEN}
    })
        .then(function(r) { return r.json(); })
        .then(function(d) {
            var currentOrgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId() : orgId;
            if (requestSeq !== _adsRefreshStatusSeq || orgId !== currentOrgId) return;
            renderAdsRefreshStatus(d);
        })
        .catch(function(e) {
            console.warn('loadAdsRefreshStatus error:', e);
        });
}

function refreshAds() {
    var orgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId() : ((typeof getOrgId === 'function') ? getOrgId() : (typeof ORG_ID !== 'undefined' ? ORG_ID : ''));
    if (!orgId || orgId === 'null') return;
    var btn = document.getElementById('ads-refresh-btn');
    if (btn) btn.disabled = true;
    fetch('/api/v1/nl/ad-stats/refresh?org_id=' + encodeURIComponent(orgId), {
        method: 'POST',
        headers: {'Authorization': 'Bearer ' + TOKEN}
    })
        .then(function(r) {
            return r.json().then(function(d) {
                if (!r.ok) {
                    if (d && d.detail) renderAdsRefreshStatus(d.detail, 'WB ограничивает частоту запросов');
                    throw new Error((d && d.message) || (d && d.detail && d.detail.message) || 'Не удалось запустить сбор');
                }
                return d;
            });
        })
        .then(function(d) {
            renderAdsRefreshStatus(d, 'Сбор запущен за ' + (d.days_back || 9) + ' дней');
            loadAds();
            if (_adsCurrentView === 'art') {
                loadAdsArts();
            }
        })
        .catch(function(e) {
            console.warn('refreshAds error:', e);
            loadAdsRefreshStatus();
        });
}

// ===== Раскрытие строки — список РК для этого артикула =====
function initArtsRowExpand() {
    if (!adsArtsTabulator) return;
    adsArtsTabulator.on('rowClick', function(e, row) {
        if (e.target.closest('a') || e.target.closest('.art-campaigns-row')) return;
        toggleArtCampaigns(row);
    });
}

function closeArtCampaigns() {
    destroyAdsArtCharts();
    if (_adsExpandedRow) {
        var el = _adsExpandedRow.getElement();
        var next = el.nextElementSibling;
        if (next && next.classList.contains('art-campaigns-row')) {
            next.remove();
        }
        _adsExpandedRow = null;
    }
}

function destroyAdsArtCharts() {
    if (!_adsArtCharts || !_adsArtCharts.length) return;
    _adsArtCharts.forEach(function(chart) {
        if (chart && typeof chart.destroy === 'function') chart.destroy();
    });
    _adsArtCharts = [];
}

function toggleArtCampaigns(row) {
    var el = row.getElement();
    var existing = el.nextElementSibling;

    if (_adsExpandedRow === row) {
        closeArtCampaigns();
        return;
    }

    closeArtCampaigns();
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

    var statusIcons = {'-1':'🗑','4':'⏳','7':'☑','8':'❌','9':'🟢','11':'⏸'};
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
        html += '<td style="padding:4px 8px;text-align:center">' + (c.type_label || typeNames[c.type] || c.type || '—') + '</td>';
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
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-top:10px">';
    camps.forEach(function(c, i) {
        html += '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:8px;min-width:0">';
        html += '<div style="font-size:.78em;font-weight:700;color:#1a1a2e;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:6px">' + (c.name || ('РК #' + c.campaign_id)) + '</div>';
        html += '<div style="height:150px"><canvas id="ads-art-campaign-chart-' + nmId + '-' + i + '"></canvas></div>';
        html += '</div>';
    });
    html += '</div>';
    td.innerHTML = html;
    tr.appendChild(td);
    el.parentNode.insertBefore(tr, el.nextSibling);
    setTimeout(function() { renderAdsArtCampaignCharts(nmId, camps); }, 0);
}

function renderAdsArtCampaignCharts(nmId, camps) {
    if (typeof Chart === 'undefined') return;
    destroyAdsArtCharts();
    var palette = ['#6c5ce7', '#00b894', '#fdcb6e', '#e17055', '#0984e3', '#e84393'];
    (camps || []).forEach(function(c, i) {
        var canvas = document.getElementById('ads-art-campaign-chart-' + nmId + '-' + i);
        var daily = (c.daily || []).slice().sort(function(a, b) {
            return String(a.date || '') < String(b.date || '') ? -1 : 1;
        });
        if (!canvas || daily.length < 1) return;
        var labels = daily.map(function(p) { return String(p.date || '').slice(5); });
        var chart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {label: 'Показы', data: daily.map(function(p) { return Number(p.views || 0); }), borderColor: palette[0], backgroundColor: palette[0] + '22', yAxisID: 'qty', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4},
                    {label: 'Клики', data: daily.map(function(p) { return Number(p.clicks || 0); }), borderColor: palette[1], backgroundColor: palette[1] + '22', yAxisID: 'qty', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4},
                    {label: 'Заказы', data: daily.map(function(p) { return Number(p.orders || 0); }), borderColor: palette[2], backgroundColor: palette[2] + '22', yAxisID: 'qty', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4},
                    {label: 'Расход', data: daily.map(function(p) { return Number(p.spent || 0); }), borderColor: palette[3], backgroundColor: palette[3] + '22', yAxisID: 'rub', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4},
                    {label: 'CPC', data: daily.map(function(p) { return Number(p.cpc || 0); }), borderColor: palette[4], backgroundColor: palette[4] + '22', yAxisID: 'rub', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4},
                    {label: 'ДРР', data: daily.map(function(p) { return Number(p.drr || 0); }), borderColor: palette[5], backgroundColor: palette[5] + '22', yAxisID: 'pct', tension: 0.2, borderWidth: 2, pointRadius: 1, pointHoverRadius: 4, borderDash: [5, 5]},
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {mode: 'index', intersect: false},
                plugins: {legend: {position: 'top', labels: {boxWidth: 10, font: {size: 10}}}},
                scales: {
                    qty: {type: 'linear', position: 'left', grid: {color: 'rgba(0,0,0,.06)'}},
                    rub: {type: 'linear', position: 'right', grid: {drawOnChartArea: false}},
                    pct: {type: 'linear', position: 'right', display: false, grid: {drawOnChartArea: false}},
                },
            },
        });
        _adsArtCharts.push(chart);
    });
}
