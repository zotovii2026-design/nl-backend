/*
 * Common Reference — first read-only neutral identity slice.
 * Reuses the existing Reference UI below this neutral overview.
 */

var commonReferenceTabulator = null;

function commonReferenceState(product) {
    var marketplaces = new Set((product.listings || []).map(function(listing) {
        return String(listing.marketplace || '').toLowerCase();
    }));
    if (marketplaces.has('wildberries') && marketplaces.has('ozon')) return 'WB + Ozon';
    if (marketplaces.has('wildberries')) return 'WB only';
    if (marketplaces.has('ozon')) return 'Ozon only';
    return 'unmatched';
}

function commonReferenceFlatten(products) {
    return (products || []).map(function(product) {
        var listings = product.listings || [];
        var wb = listings.filter(function(item) { return String(item.marketplace).toLowerCase() === 'wildberries'; });
        var ozon = listings.filter(function(item) { return String(item.marketplace).toLowerCase() === 'ozon'; });
        return {
            id: product.id,
            name: product.name || 'Без названия',
            internal_sku: product.internal_sku || '',
            brand: product.brand || '',
            variants: (product.variants || []).length,
            wb_listings: wb.length,
            ozon_listings: ozon.length,
            match_state: commonReferenceState(product),
            status: product.status || '',
        };
    });
}

function commonReferenceBadge(value) {
    var colors = {
        'WB + Ozon': ['#dff5ec', '#167253'],
        'WB only': ['#f0e8ff', '#6941a5'],
        'Ozon only': ['#e1efff', '#185bbb'],
        'unmatched': ['#fff0cf', '#805c08'],
    };
    var palette = colors[value] || ['#f1f2f6', '#555'];
    return '<span style="display:inline-block;padding:3px 8px;border-radius:12px;font-size:10px;font-weight:700;background:' +
        palette[0] + ';color:' + palette[1] + '">' + value + '</span>';
}

function renderCommonReferenceSummary(rows) {
    var counts = {'WB + Ozon': 0, 'WB only': 0, 'Ozon only': 0, 'unmatched': 0};
    rows.forEach(function(row) { counts[row.match_state] = (counts[row.match_state] || 0) + 1; });
    var summary = document.getElementById('common-reference-summary');
    if (!summary) return;
    summary.innerHTML = Object.keys(counts).map(function(state) {
        return commonReferenceBadge(state) + '<b style="margin-right:12px">' + counts[state] + '</b>';
    }).join('');
}

function renderCommonReferenceTable(rows) {
    var target = document.getElementById('common-reference-tabulator');
    if (!target) return;
    if (typeof Tabulator === 'undefined') {
        target.innerHTML = '<div style="padding:12px;color:#a33">Tabulator не загружен</div>';
        return;
    }
    if (commonReferenceTabulator) commonReferenceTabulator.destroy();
    commonReferenceTabulator = new Tabulator(target, {
        data: rows,
        layout: 'fitColumns',
        height: 360,
        placeholder: 'Общие товары ещё не созданы',
        index: 'id',
        columns: [
            {title: 'Общий товар NL', field: 'name', minWidth: 220, headerFilter: 'input'},
            {title: 'Внутренний SKU', field: 'internal_sku', width: 130},
            {title: 'Бренд', field: 'brand', width: 120, headerFilter: 'input'},
            {title: 'Варианты', field: 'variants', width: 85, hozAlign: 'right'},
            {title: 'WB', field: 'wb_listings', width: 65, hozAlign: 'right'},
            {title: 'Ozon', field: 'ozon_listings', width: 70, hozAlign: 'right'},
            {title: 'Связь', field: 'match_state', width: 120, formatter: function(cell) {
                return commonReferenceBadge(cell.getValue());
            }},
            {title: 'Статус', field: 'status', width: 95},
        ],
    });
}

async function loadCommonReference() {
    var orgId = getCurrentOrgId();
    var status = document.getElementById('common-reference-status');
    if (!orgId) return;
    if (status) status.textContent = 'Загрузка общих товаров…';
    try {
        var response = await fetch('/api/v1/commerce/products?org_id=' + encodeURIComponent(orgId) + '&limit=200');
        var data = await response.json();
        if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
        var rows = commonReferenceFlatten(data.items || []);
        renderCommonReferenceSummary(rows);
        renderCommonReferenceTable(rows);
        if (status) status.textContent = 'Общих товаров: ' + rows.length + '. Ниже — проверенные настройки действующего Справочника.';
    } catch (error) {
        if (status) status.innerHTML = '<span style="color:#a56b00">Neutral-слой пока недоступен: ' +
            String(error.message || error) + '. Старый Справочник не затронут.</span>';
        renderCommonReferenceSummary([]);
        renderCommonReferenceTable([]);
    }
}

function destroyCommonReference() {
    if (commonReferenceTabulator) {
        commonReferenceTabulator.destroy();
        commonReferenceTabulator = null;
    }
}
