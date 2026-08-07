/**
 * Ads Grid — Рекламные кампании на Tabulator
 * v20260807-filters1: NLFilters multiselect + NLPagePrefs persistence
 */

let adsTabulator = null;
let _adsAllData = [];
const ADS_PAGE_KEY = 'ads-rk';
let _adsRestoringFilters = false;

// Сброс кэша Tabulator при смене версии колонок
(function() {
    const VER = 'ads-grid-v15';
    if (localStorage.getItem('ads-grid-ver') !== VER) {
        localStorage.removeItem('tabulator-ads-grid-state-columns');
        localStorage.removeItem('tabulator-ads-grid-state-sort');
        localStorage.setItem('ads-grid-ver', VER);
    }
})();

function getOrgId() {
    if (typeof getCurrentOrgId === 'function') return getCurrentOrgId();
    var urlOrg = new URL(location.href).searchParams.get('org');
    return urlOrg || (typeof ORG_ID !== 'undefined' ? ORG_ID : localStorage.getItem('nl_org_id'));
}

/* ── Фильтры: построение query для API ── */

function getAdsProductFilterQuery() {
    var params = new URLSearchParams();
    var productStatus = NLFilters.getValues(document.getElementById('ads-flt-status'));
    var productClass = NLFilters.getValues(document.getElementById('ads-flt-class'));
    var brand = NLFilters.getValues(document.getElementById('ads-flt-brand'));
    var tags = NLFilters.getValues(document.getElementById('ads-flt-tags'));
    var search = (document.getElementById('ads-flt-search')?.value || '').trim();
    if (productStatus.length) params.set('product_status', productStatus[0]);
    if (productClass.length) params.set('product_class', productClass[0]);
    if (brand.length) params.set('brand', brand[0]);
    if (search) params.set('search', search);
    var qs = params.toString();
    return qs ? '&' + qs : '';
}

function hasAdsProductFilters() {
    return !!getAdsProductFilterQuery();
}

/* ── Маппинги ── */

const statusMap = {'-1':'🗑 Удалена','4':'⏳ Готова','7':'☑ Завершена','8':'❌ Отклонена','9':'🟢 Активна','11':'⏸ Пауза'};
const typeMap = {'4':'Автоматическая','5':'Поиск','6':'Каталог','7':'Таргет','8':'Рек. в рекомендациях','9':'Аукцион'};
const statusColors = {'-1':'background:#f1f3f5','4':'background:#fff3cd','7':'background:#e2e3e5','8':'background:#f8d7da','9':'background:#d4edda','11':'background:#fff3cd'};

function formatAdsRub(value) {
    return (parseFloat(value) || 0).toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽';
}

function getCampaignProductBreakdown(campaign) {
    var products = (campaign.products || []).slice(0, 3);
    if (!products.length) return '';
    var lines = products.map(function(p) {
        return [
            p.nm_id || '?',
            'расход ' + formatAdsRub(p.spent_share),
            'РК ' + formatAdsRub(p.sum_price),
            'товар ' + formatAdsRub(p.total_revenue_product)
        ].join(' · ');
    });
    if ((campaign.products || []).length > products.length) {
        lines.push('ещё ' + ((campaign.products || []).length - products.length) + ' товар(ов)');
    }
    return '\n\nТовары:\n' + lines.join('\n');
}

function campaignDrrTooltip(campaign, mode) {
    if (mode === 'product') {
        return 'ДРР товара за период'
            + '\nРасход РК: ' + formatAdsRub(campaign.spent)
            + '\nВсе заказы товаров: ' + (campaign.total_orders_product || 0)
            + '\nСумма всех заказов товаров: ' + formatAdsRub(campaign.total_revenue_product)
            + '\nФормула: расход РК / сумма всех заказов товаров РК'
            + getCampaignProductBreakdown(campaign);
    }
    return 'ДРР по РК'
        + '\nРасход РК: ' + formatAdsRub(campaign.spent)
        + '\nЗаказы из рекламы: ' + (campaign.total_orders || 0)
        + '\nСумма заказов из рекламы: ' + formatAdsRub(campaign.total_revenue)
        + '\nФормула: расход РК / сумма рекламных заказов товаров РК'
        + getCampaignProductBreakdown(campaign);
}

/* ── Конфигурация колонок ── */

function getAdsColumns() {
    return [
        {
            title: '📌 Основное',
            columns: [
                {
                    title: 'Статус', field: 'status', headerTooltip: 'Статус кампании', width: 100, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue() || '';
                        return '<span style="' + (statusColors[v]||'') + ';padding:2px 8px;border-radius:4px;font-size:.82em;white-space:nowrap">' + (statusMap[v] || v) + '</span>';
                    }
                },
                {
                    title: 'Кампания', field: 'name', headerTooltip: 'Название кампании', width: 180, headerSort: true, tooltip: true, cssClass: 'truncate-cell',
                    formatter: function(cell) {
                        const data = cell.getRow().getData();
                        const rawName = cell.getValue() || '';
                        const name = rawName || ('РК #' + data.campaign_id);
                        return '<div style="font-weight:600">' + name + '</div>' +
                               '<div style="color:#999;font-size:.75em">ID: ' + data.campaign_id + '</div>';
                    }
                },
                {
                    title: 'Тип', field: 'type', headerTooltip: 'Тип кампании', width: 100, headerSort: true,
                    formatter: function(cell) {
                        const data = cell.getRow().getData();
                        return data.type_label || typeMap[cell.getValue()] || cell.getValue() || '—';
                    }
                },
            ]
        },
        {
            title: '📦 Товары',
            columns: [
                {
                    title: 'Шт.', field: 'nm_count', headerTooltip: 'Количество товаров в РК', width: 50, headerSort: true, hozAlign: 'center',
                    formatter: function(cell) {
                        const v = cell.getValue() || 0;
                        if (v > 1) return '<span style="background:#6c5ce7;color:#fff;padding:2px 8px;border-radius:10px;font-size:.82em">' + v + '</span>';
                        return '<span style="font-size:.82em">' + v + '</span>';
                    }
                },
                {
                    title: 'Состав РК', field: 'products', headerTooltip: 'Товары в составе РК', width: 140, headerSort: false, tooltip: true,
                    formatter: function(cell) {
                        const products = cell.getValue();
                        if (!products || !products.length) return '<span style="color:#999;font-size:.8em">—</span>';
                        let html = '<div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap">';
                        const show = products.slice(0, 3);
                        show.forEach(function(p) {
                            if (p.photo) {
                                const thumb = p.photo.replace('/hq/','/c246x328/').replace('/big/','/c246x328/');
                                html += '<img src="' + thumb + '" style="width:28px;height:28px;border-radius:3px;object-fit:cover" loading="lazy">';
                            } else {
                                html += '<span style="background:#f0f0f0;padding:2px 6px;border-radius:3px;font-size:.7em">' + (p.nm_id || '?') + '</span>';
                            }
                        });
                        if (products.length > 3) {
                            html += '<span style="color:#6c5ce7;font-size:.75em;font-weight:600">+' + (products.length - 3) + '</span>';
                        }
                        html += '</div>';
                        return html;
                    }
                },
            ]
        },
        {
            title: '💰 Финансы',
            columns: [
                {
                    title: 'Расход ₽', field: 'spent', headerTooltip: 'Расход на кампанию', width: 110, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return '<b style="color:#e17055">' + v.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + '</b>';
                    },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return '<b style="color:#e17055">' + v.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽</b>';
                    }
                },
                {
                    title: 'ДРР по РК %', field: 'drr', headerTooltip: 'Расход / сумма рекламных заказов WB', width: 100, headerSort: true, hozAlign: 'right',
                    tooltip: function(e, cell) { return campaignDrrTooltip(cell.getRow().getData(), 'rk'); },
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        if (!v) return '—';
                        const color = v > 50 ? '#e74c3c' : v > 25 ? '#e17055' : '#00b894';
                        return '<span style="color:' + color + ';font-weight:600">' + v.toFixed(1) + '%</span>';
                    }
                },
                {
                    title: 'ДРР товара %', field: 'drr_product', headerTooltip: 'Расход РК / все заказы товаров РК за период', width: 105, headerSort: true, hozAlign: 'right',
                    tooltip: function(e, cell) { return campaignDrrTooltip(cell.getRow().getData(), 'product'); },
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        if (!v) return '—';
                        const color = v > 20 ? '#e74c3c' : v > 10 ? '#e17055' : '#00b894';
                        return '<span style="color:' + color + ';font-weight:600">' + v.toFixed(1) + '%</span>';
                    }
                },
                {
                    title: 'Σ заказов ₽', field: 'sum_price', headerTooltip: 'Сумма заказов из РК', width: 100, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return v ? v.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽' : '—';
                    },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return '<b>' + v.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽</b>';
                    }
                },
            ]
        },
        {
            title: '📊 Метрики',
            columns: [
                { title: 'Показы', field: 'views', headerTooltip: 'Количество показов', width: 90, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = cell.getValue() || 0; return v.toLocaleString('ru-RU'); },
                  bottomCalc: 'sum', bottomCalcFormatter: function(cell) { const v = cell.getValue() || 0; return '<b>' + v.toLocaleString('ru-RU') + '</b>'; } },
                { title: 'Клики', field: 'clicks', headerTooltip: 'Количество кликов', width: 80, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = cell.getValue() || 0; return v.toLocaleString('ru-RU'); },
                  bottomCalc: 'sum', bottomCalcFormatter: function(cell) { const v = cell.getValue() || 0; return '<b>' + v.toLocaleString('ru-RU') + '</b>'; } },
                { title: 'CTR %', field: 'ctr', headerTooltip: 'Click-through rate', width: 75, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = parseFloat(cell.getValue()) || 0; return v ? v.toFixed(2) + '%' : '—'; } },
                { title: 'CPC ₽', field: 'cpc', headerTooltip: 'Цена за клик', width: 75, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = parseFloat(cell.getValue()) || 0; return v ? v.toFixed(2) : '—'; } },
                { title: 'Заказы', field: 'orders', headerTooltip: 'Заказы из рекламы', width: 80, headerSort: true, hozAlign: 'right',
                  bottomCalc: 'sum', bottomCalcFormatter: function(cell) { const v = cell.getValue() || 0; return '<b>' + v + '</b>'; } },
                { title: 'клик / заказ', field: 'clicks_per_order', headerTooltip: 'Клики / заказы', width: 95, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = parseFloat(cell.getValue()) || 0; return v ? v.toFixed(1) : '—'; } },
                { title: 'В корз.', field: 'atbs', headerTooltip: 'Добавлений в корзину', width: 75, headerSort: true, hozAlign: 'right',
                  bottomCalc: 'sum', bottomCalcFormatter: function(cell) { const v = cell.getValue() || 0; return '<b>' + v + '</b>'; } },
                { title: 'CR %', field: 'cr', headerTooltip: 'Conversion rate', width: 70, headerSort: true, hozAlign: 'right',
                  formatter: function(cell) { const v = parseFloat(cell.getValue()) || 0; return v ? v.toFixed(1) + '%' : '—'; } },
            ]
        },
    ];
}

/* ── Инициализация Tabulator ── */

(function(){
    var s = document.createElement('style');
    s.textContent = '#ads-campaigns-tabulator .tabulator-col-title{font-size:8px!important;line-height:1.1!important;padding:2px 4px!important}#ads-campaigns-tabulator .tabulator-col .tabulator-col-content{padding:2px 4px!important}#ads-campaigns-tabulator .tabulator-cell{font-size:11px!important}';
    document.head.appendChild(s);
})();

function initAdsGrid() {
    if (adsTabulator) return;
    var container = document.getElementById('ads-campaigns-tabulator');
    if (!container) return;
    container.style.width = '100%';

    var cols = getAdsColumns();
    cols = NLPagePrefs.applyColumnOrder(ADS_PAGE_KEY, cols);

    adsTabulator = new Tabulator(container, {
        data: [],
        columns: cols,
        height: '60vh',
        layout: 'fitDataFill',
        renderHorizontal: 'virtual',
        placeholder: '<div style="padding:20px;text-align:center;color:#999"><div style="font-size:1.2em;margin-bottom:8px">📭 Нет данных</div><div style="font-size:.85em">Попробуйте снять фильтр «Скрыть пустые» или переключить вкладку</div></div>',
        headerSortClickElement: 'header',
        sortable: true,
        pagination: false,
        movableColumns: true,
        persistence: { columns: true, sort: true },
        persistenceID: 'ads-grid-state',
        persistenceMode: 'local',
        groupBy: false,
        rowFormatter: function(row) { row.getElement().style.cursor = 'pointer'; },
        rowClick: function(e, row) { showAdsCampaignDetail(row.getData()); },
        columnMoved: function() { NLPagePrefs.saveColumns(ADS_PAGE_KEY, adsTabulator); },
    });
}

function updateAdsTabulator(campaigns) {
    if (!adsTabulator) initAdsGrid();
    _adsAllData = campaigns || [];
    if (!hasAdsProductFilters()) populateAdsFilterOptionsForRK();
    applyAdsFilters();
}

/* ── Заполнение опций фильтров ── */

function populateAdsFilterOptionsForRK() {
    if (!_adsAllData.length) return;
    var brands = {}, statuses = {}, classes = {}, tags = {};
    _adsAllData.forEach(function(c) {
        (c.products || []).forEach(function(p) {
            if (p.brand) brands[p.brand] = true;
            if (p.product_status) statuses[p.product_status] = true;
            if (p.product_class) classes[p.product_class] = true;
            if (p.tags) {
                String(p.tags).split(',').forEach(function(t) {
                    t = t.trim();
                    if (t) tags[t] = true;
                });
            }
        });
    });
    fillAdsMultiSelect('ads-flt-brand', 'Бренд', Object.keys(brands).sort());
    fillAdsMultiSelect('ads-flt-status', 'Статус', Object.keys(statuses).sort());
    fillAdsMultiSelect('ads-flt-class', 'Класс', Object.keys(classes).sort());
    fillAdsMultiSelect('ads-flt-tags', 'Теги', Object.keys(tags).sort());
    restoreAdsFilterPreferences();
}

function fillAdsMultiSelect(id, label, values) {
    var sel = document.getElementById(id);
    if (!sel) return;
    sel.innerHTML = '';
    values.forEach(function(v) {
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

/* ── Сохранение/восстановление фильтров ── */

function getAdsFilterState() {
    return {
        status: NLFilters.getValues(document.getElementById('ads-flt-status')),
        productClass: NLFilters.getValues(document.getElementById('ads-flt-class')),
        brand: NLFilters.getValues(document.getElementById('ads-flt-brand')),
        tags: NLFilters.getValues(document.getElementById('ads-flt-tags')),
        search: document.getElementById('ads-flt-search')?.value || '',
    };
}

function saveAdsFilterPreferences() {
    if (_adsRestoringFilters) return;
    NLPagePrefs.saveFilters(ADS_PAGE_KEY, getAdsFilterState());
}

function setAdsMultiFilterValues(id, values) {
    var sel = document.getElementById(id);
    if (!sel) return;
    var available = new Set(Array.from(sel.options || []).map(function(o) { return o.value; }));
    NLFilters.setValues(sel, (values || []).filter(function(v) { return available.has(v); }));
    NLFilters.renderMenu(id);
}

function restoreAdsFilterPreferences() {
    if (typeof NLPagePrefs === 'undefined') return;
    var filters = NLPagePrefs.loadFilters(ADS_PAGE_KEY);
    if (!filters || !Object.keys(filters).length) return;
    _adsRestoringFilters = true;
    setAdsMultiFilterValues('ads-flt-status', filters.status || []);
    setAdsMultiFilterValues('ads-flt-class', filters.productClass || []);
    setAdsMultiFilterValues('ads-flt-brand', filters.brand || []);
    setAdsMultiFilterValues('ads-flt-tags', filters.tags || []);
    var search = document.getElementById('ads-flt-search');
    if (search) search.value = filters.search || '';
    _adsRestoringFilters = false;
}

function clearAdsFilters() {
    ['ads-flt-status', 'ads-flt-class', 'ads-flt-brand', 'ads-flt-tags'].forEach(function(id) {
        var sel = document.getElementById(id);
        if (sel) NLFilters.selectAll(sel);
    });
    var search = document.getElementById('ads-flt-search');
    if (search) search.value = '';
    saveAdsFilterPreferences();
    applyAdsFilters();
}

function resetAdsColumnPreferences() {
    NLPagePrefs.resetColumns(ADS_PAGE_KEY);
    if (adsTabulator) { adsTabulator.destroy(); adsTabulator = null; }
    if (typeof applyAdsFilters === 'function') applyAdsFilters();
    if (typeof showToast === 'function') showToast('✅ Порядок колонок восстановлен');
}

function resetAdsViewPreferences() {
    NLPagePrefs.resetAll(ADS_PAGE_KEY);
    if (typeof clearAdsFilters === 'function') clearAdsFilters();
    if (adsTabulator) { adsTabulator.destroy(); adsTabulator = null; }
    if (typeof applyAdsFilters === 'function') applyAdsFilters();
    if (typeof showToast === 'function') showToast('✅ Вид раздела сброшен');
}

/* ── Фильтрация ── */

function buildFilteredCampaign(campaign, products) {
    var spent = products.reduce(function(s, p) { return s + (parseFloat(p.spent_share) || 0); }, 0);
    var views = products.reduce(function(s, p) { return s + (parseInt(p.views || 0, 10) || 0); }, 0);
    var clicks = products.reduce(function(s, p) { return s + (parseInt(p.clicks || 0, 10) || 0); }, 0);
    var orders = products.reduce(function(s, p) { return s + (parseInt(p.orders || 0, 10) || 0); }, 0);
    var atbs = products.reduce(function(s, p) { return s + (parseInt(p.atbs || 0, 10) || 0); }, 0);
    var sumPrice = products.reduce(function(s, p) { return s + (parseFloat(p.sum_price) || 0); }, 0);
    var totalOrdersProduct = products.reduce(function(s, p) { return s + (parseInt(p.total_orders_product || 0, 10) || 0); }, 0);
    var totalRevenueProduct = products.reduce(function(s, p) { return s + (parseFloat(p.total_revenue_product) || 0); }, 0);
    return Object.assign({}, campaign, {
        spent: Math.round(spent * 100) / 100, views: views, clicks: clicks, orders: orders, atbs: atbs,
        sum_price: Math.round(sumPrice * 100) / 100,
        ctr: views ? Math.round((clicks / views * 100) * 100) / 100 : 0,
        cpc: clicks ? Math.round((spent / clicks) * 100) / 100 : 0,
        cr: clicks ? Math.round((orders / clicks * 100) * 100) / 100 : 0,
        clicks_per_order: orders ? Math.round((clicks / orders) * 10) / 10 : 0,
        drr: sumPrice ? Math.round((spent / sumPrice * 100) * 10) / 10 : 0,
        drr_product: totalRevenueProduct ? Math.round((spent / totalRevenueProduct * 100) * 10) / 10 : 0,
        drr_total: totalRevenueProduct ? Math.round((spent / totalRevenueProduct * 100) * 10) / 10 : 0,
        total_orders_product: totalOrdersProduct,
        total_revenue_product: Math.round(totalRevenueProduct * 100) / 100,
        nm_count: products.length, products: products,
    });
}

function productMatchesAdsFilters(product, search, brands, statuses, productClasses, tagsArr) {
    var matchSearch = !search ||
        (product.name || '').toLowerCase().indexOf(search) >= 0 ||
        String(product.nm_id || '').indexOf(search) >= 0 ||
        (product.vendor_code || '').toLowerCase().indexOf(search) >= 0;
    var matchBrand = !brands.length || brands.indexOf(product.brand || '') >= 0;
    var matchStatus = !statuses.length || statuses.indexOf(product.product_status || '') >= 0;
    var matchClass = !productClasses.length || productClasses.indexOf(product.product_class || '') >= 0;
    var matchTags = !tagsArr.length || (function() {
        var pTags = String(product.tags || '').split(',').map(function(t) { return t.trim(); }).filter(Boolean);
        return tagsArr.some(function(t) { return pTags.indexOf(t) >= 0; });
    })();
    return matchSearch && matchBrand && matchStatus && matchClass && matchTags;
}

function applyAdsFilters() {
    if (!adsTabulator) return;
    var activeStatuses = typeof _adsStatusFilters !== 'undefined' ? _adsStatusFilters : ['9', '11'];
    var filtered = _adsAllData.filter(function(c) {
        return activeStatuses.indexOf(c.status) >= 0;
    });

    var search = (document.getElementById('ads-flt-search')?.value || '').toLowerCase();
    var fltBrand = NLFilters.getValues(document.getElementById('ads-flt-brand'));
    var fltStatus = NLFilters.getValues(document.getElementById('ads-flt-status'));
    var fltClass = NLFilters.getValues(document.getElementById('ads-flt-class'));
    var fltTags = NLFilters.getValues(document.getElementById('ads-flt-tags'));

    if (search || fltBrand.length || fltStatus.length || fltClass.length || fltTags.length) {
        filtered = filtered.map(function(c) {
            var prods = c.products || [];
            var matchedProducts = prods.filter(function(p) {
                return productMatchesAdsFilters(p, search, fltBrand, fltStatus, fltClass, fltTags);
            });
            return matchedProducts.length ? buildFilteredCampaign(c, matchedProducts) : null;
        }).filter(Boolean);
    }

    adsTabulator.setData(filtered);
    var cnt = document.getElementById('ads-camp-count');
    if (cnt) cnt.textContent = filtered.length + ' из ' + _adsAllData.length;
    var fCnt = document.getElementById('ads-filter-count');
    if (fCnt) fCnt.textContent = filtered.length + ' из ' + _adsAllData.length;
    saveAdsFilterPreferences();
}

/* ── Детализация РК ── */

function showAdsCampaignDetail(campaign) {
    const modal = document.getElementById('ads-detail-modal');
    const content = document.getElementById('ads-detail-content');
    if (!modal || !content) return;

    let html = '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">';
    html += '<h3 style="color:#6c5ce7;margin:0">' + (campaign.name || ('РК #' + campaign.campaign_id)) + '</h3>';
    html += '<span style="font-size:.82em;color:#999">ID: ' + campaign.campaign_id + '</span>';
    html += '<span style="' + (statusColors[campaign.status]||'') + ';padding:2px 8px;border-radius:4px;font-size:.82em">' + (statusMap[campaign.status]||'') + '</span>';
    html += '</div>';

    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:16px">';
    html += '<div style="background:#fff4e6;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Расход</div><div style="font-weight:700;color:#e17055">' + (campaign.spent||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽</div></div>';
    html += '<div style="background:#e8f8f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">ДРР по РК</div><div style="font-weight:700">' + (campaign.drr||0).toFixed(1) + '%</div></div>';
    html += '<div style="background:#fff4e6;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">ДРР товара</div><div style="font-weight:700">' + (campaign.drr_product||campaign.drr_total||0).toFixed(1) + '%</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Показы</div><div style="font-weight:700">' + (campaign.views||0).toLocaleString('ru-RU') + '</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Клики</div><div style="font-weight:700">' + (campaign.clicks||0).toLocaleString('ru-RU') + '</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">CTR</div><div style="font-weight:700">' + (campaign.ctr||0).toFixed(2) + '%</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Заказы</div><div style="font-weight:700">' + (campaign.orders||0) + '</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center" title="Клики / заказы"><div style="font-size:.75em;color:#999">клик / заказ</div><div style="font-weight:700">' + (campaign.clicks_per_order ? campaign.clicks_per_order.toFixed(1) : '—') + '</div></div>';
    html += '</div>';

    html += '<div style="font-weight:600;margin-bottom:8px;color:#6c5ce7;font-size:.9em">📦 Состав РК (' + (campaign.nm_count||0) + ' товар' + (campaign.nm_count > 1 ? 'ов' : '') + ')</div>';
    if (campaign.products && campaign.products.length) {
        html += '<div style="display:flex;flex-wrap:wrap;gap:8px">';
        campaign.products.forEach(function(p) {
            html += '<div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:8px;display:flex;align-items:center;gap:8px;min-width:200px">';
            if (p.photo) {
                const thumb = p.photo.replace('/hq/','/c246x328/').replace('/big/','/c246x328/');
                html += '<img src="' + thumb + '" style="width:40px;height:40px;object-fit:cover;border-radius:4px">';
            }
            html += '<div>';
            html += '<div style="font-weight:600;font-size:.82em">' + (p.vendor_code || p.nm_id) + '</div>';
            html += '<div style="color:#999;font-size:.78em">' + (p.name || 'Арт. ' + p.nm_id) + '</div>';
            if (p.product_class || p.product_status) {
                html += '<div style="color:#636e72;font-size:.72em">' + (p.product_class || '') + (p.product_class && p.product_status ? ' · ' : '') + (p.product_status || '') + '</div>';
            }
            html += '<div style="color:#e17055;font-size:.78em;font-weight:600">' + (p.spent_share||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽</div>';
            html += '</div></div>';
        });
        html += '</div>';
    } else {
        html += '<div style="color:#999;font-size:.82em">Нет товаров в кампании</div>';
    }

    content.innerHTML = html;
    modal.style.display = 'flex';
}

function closeAdsDetailModal() {
    const modal = document.getElementById('ads-detail-modal');
    if (modal) modal.style.display = 'none';
}

/* ── Экспорт в CSV ── */

function exportAdsExcel() {
    if (!adsTabulator) return;
    var cols = adsTabulator.getColumnDefinitions();
    var flatCols = [];
    cols.forEach(function(c) {
        if (c.columns && c.columns.length) { c.columns.forEach(function(sub) { flatCols.push(sub); }); }
        else { flatCols.push(c); }
    });
    var headers = flatCols.map(function(c) { return c.title; });
    var rows = adsTabulator.getData();
    var lines = [];
    lines.push(headers.map(csvEscape).join(';'));
    rows.forEach(function(row) {
        var vals = flatCols.map(function(c) {
            var v = row[c.field];
            if (c.field === 'status') v = (statusMap[v]||{'-1':'Удалена','4':'Готова','7':'Завершена','8':'Отклонена','9':'Активна','11':'Пауза'})[v] || v;
            if (c.field === 'type') v = row.type_label || typeMap[v] || v;
            return v != null ? v : '';
        });
        lines.push(vals.map(csvEscape).join(';'));
    });
    var blob = new Blob(['\uFEFF' + lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url; a.download = 'ads-campaigns.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function csvEscape(v) {
    v = String(v == null ? '' : v);
    if (v.indexOf(';') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0) return '"' + v.replace(/"/g, '""') + '"';
    return v;
}
