/**
 * Ads Grid — Рекламные кампании на Tabulator
 * Паттерн: ue-grid.js / cost-grid.js
 */

let adsTabulator = null;
let _adsAllData = [];  // Полные данные до фильтрации

// Сброс кэша Tabulator при смене версии колонок
(function() {
    const VER = 'ads-grid-v8';
    if (localStorage.getItem('ads-grid-ver') !== VER) {
        localStorage.removeItem('tabulator-ads-grid-state-columns');
        localStorage.removeItem('tabulator-ads-grid-state-sort');
        localStorage.setItem('ads-grid-ver', VER);
    }
})();

function getOrgId() {
    return typeof ORG_ID !== 'undefined' ? ORG_ID : localStorage.getItem('nl_org_id');
}

// Маппинги
const statusMap = {'4':'⏳ Ожидает','7':'🟢 Активна','8':'❌ Отклонена','9':'⏸ Приостановлена','11':'☑ Завершена'};
const typeMap = {'4':'Автоматическая','5':'Поиск','6':'Каталог','7':'Таргет','8':'Рек. в рекомендациях','9':'Аукцион'};
const statusColors = {'4':'background:#fff3cd','7':'background:#d4edda','8':'background:#f8d7da','9':'background:#fff3cd','11':'background:#e2e3e5'};

// Конфигурация колонок
function getAdsColumns() {
    return [
        // === 📌 Основное ===
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
                        const rawName = cell.getValue() || ''; const name = rawName || ('РК #' + data.campaign_id);
                        const data = cell.getRow().getData();
                        return '<div style="font-weight:600">' + name + '</div>' +
                               '<div style="color:#999;font-size:.75em">ID: ' + data.campaign_id + '</div>';
                    }
                },
                {
                    title: 'Тип', field: 'type', headerTooltip: 'Тип кампании', width: 100, headerSort: true,
                    formatter: function(cell) {
                        return typeMap[cell.getValue()] || cell.getValue() || '—';
                    }
                },
            ]
        },
        // === 📦 Товары ===
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
                        // Показываем до 3 фото + "+N"
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
        // === 💰 Финансы ===
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
                    title: 'ДРР %', field: 'drr', headerTooltip: 'Доля рекламных расходов', width: 85, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        if (!v) return '—';
                        const color = v > 50 ? '#e74c3c' : v > 25 ? '#e17055' : '#00b894';
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
        // === 📊 Метрики ===
        {
            title: '📊 Метрики',
            columns: [
                {
                    title: 'Показы', field: 'views', headerTooltip: 'Количество показов', width: 90, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return v.toLocaleString('ru-RU');
                    },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return '<b>' + v.toLocaleString('ru-RU') + '</b>';
                    }
                },
                {
                    title: 'Клики', field: 'clicks', headerTooltip: 'Количество кликов', width: 80, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return v.toLocaleString('ru-RU');
                    },
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return '<b>' + v.toLocaleString('ru-RU') + '</b>';
                    }
                },
                {
                    title: 'CTR %', field: 'ctr', headerTooltip: 'Click-through rate', width: 75, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return v ? v.toFixed(2) + '%' : '—';
                    }
                },
                {
                    title: 'CPC ₽', field: 'cpc', headerTooltip: 'Цена за клик', width: 75, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return v ? v.toFixed(2) : '—';
                    }
                },
                {
                    title: 'Заказы', field: 'orders', headerTooltip: 'Заказы из рекламы', width: 80, headerSort: true, hozAlign: 'right',
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return '<b>' + v + '</b>';
                    }
                },
                {
                    title: 'В корз.', field: 'atbs', headerTooltip: 'Добавлений в корзину', width: 75, headerSort: true, hozAlign: 'right',
                    bottomCalc: 'sum', bottomCalcFormatter: function(cell) {
                        const v = cell.getValue() || 0;
                        return '<b>' + v + '</b>';
                    }
                },
                {
                    title: 'CR %', field: 'cr', headerTooltip: 'Conversion rate', width: 70, headerSort: true, hozAlign: 'right',
                    formatter: function(cell) {
                        const v = parseFloat(cell.getValue()) || 0;
                        return v ? v.toFixed(1) + '%' : '—';
                    }
                },
            ]
        },
    ];
}

/**
 * Инициализация Tabulator
 */
// Force smaller header font for readability
(function(){
    var s = document.createElement('style');
    s.textContent = '#ads-campaigns-tabulator .tabulator-header .tabulator-col-title{font-size:11px;line-height:1.2}';
    document.head.appendChild(s);
})();

function initAdsGrid() {
    if (adsTabulator) return;

    const container = document.getElementById('ads-campaigns-tabulator');
    if (!container) return;

    container.style.width = '100%';
    adsTabulator = new Tabulator(container, {
        data: [],
        columns: getAdsColumns(),
        height: '60vh',
        layout: 'fitDataFill',
        renderHorizontal: 'virtual',
        placeholder: '<div style="padding:20px;text-align:center;color:#999"><div style="font-size:1.2em;margin-bottom:8px">📭 Нет данных</div><div style="font-size:.85em">Попробуйте снять фильтр «Скрыть пустые» или переключить вкладку</div></div>',
        headerSortClickElement: 'header',
        sortable: true,
        pagination: false,
        movableColumns: true,
        persistence: {
            columns: true,
            sort: true,
        },
        persistenceID: 'ads-grid-state',
        persistenceMode: 'local',
        groupBy: false,
        rowFormatter: function(row) {
            // Клик по строке — раскрыть состав РК
            row.getElement().style.cursor = 'pointer';
        },
        rowClick: function(e, row) {
            showAdsCampaignDetail(row.getData());
        },
    });
}

/**
 * Обновить данные в Tabulator
 */
function updateAdsTabulator(campaigns) {
    if (!adsTabulator) initAdsGrid();
    _adsAllData = campaigns || [];
    populateAdsFilterOptionsForRK();
    applyAdsFilters();
}

/**
 * Заполнить бренды для вида По РК (из товаров внутри кампаний)
 */
function populateAdsFilterOptionsForRK() {
    if (!_adsAllData.length) return;
    var brands = [];
    var seen = {};
    _adsAllData.forEach(function(c) {
        (c.products || []).forEach(function(p) {
            if (p.brand && !seen[p.brand]) { seen[p.brand] = true; brands.push(p.brand); }
        });
    });
    brands.sort();
    var brandSel = document.getElementById('ads-flt-brand');
    if (brandSel) {
        var current = brandSel.value;
        brandSel.innerHTML = '<option value="">Бренд: все</option>';
        brands.forEach(function(b) {
            var opt = document.createElement('option');
            opt.value = b;
            opt.textContent = b;
            brandSel.appendChild(opt);
        });
        brandSel.value = current;
    }
}

/**
 * Применить фильтры (табы статусов + колонки)
 */
function applyAdsFilters() {
    if (!adsTabulator) return;
    var activeStatuses = typeof _adsStatusFilters !== 'undefined' ? _adsStatusFilters : ['7', '9'];
    var filtered = _adsAllData.filter(function(c) {
        return activeStatuses.indexOf(c.status) >= 0;
    });

    // Дополнительные фильтры по колонкам (для кампаний — фильтруем по товарам внутри РК)
    var search = (document.getElementById('ads-flt-search')?.value || '').toLowerCase();
    var fltBrand = document.getElementById('ads-flt-brand')?.value || '';
    if (search || fltBrand) {
        filtered = filtered.filter(function(c) {
            var prods = c.products || [];
            // Если хоть один товар подходит — оставляем кампанию
            return prods.some(function(p) {
                var matchSearch = !search || 
                    (p.name || '').toLowerCase().indexOf(search) >= 0 ||
                    String(p.nm_id || '').indexOf(search) >= 0 ||
                    (p.vendor_code || '').toLowerCase().indexOf(search) >= 0;
                var matchBrand = !fltBrand || (p.brand || '') === fltBrand;
                return matchSearch && matchBrand;
            });
        });
    }

    adsTabulator.setData(filtered);
    var cnt = document.getElementById('ads-camp-count');
    if (cnt) cnt.textContent = filtered.length + ' из ' + _adsAllData.length;
    // Обновляем общий счётчик фильтров
    var fCnt = document.getElementById('ads-filter-count');
    if (fCnt) fCnt.textContent = filtered.length + ' из ' + _adsAllData.length;
}

/**
 * Показать детализацию состава РК (модальное или expandable)
 */
function showAdsCampaignDetail(campaign) {
    const modal = document.getElementById('ads-detail-modal');
    const content = document.getElementById('ads-detail-content');
    if (!modal || !content) return;

    let html = '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">';
    html += '<h3 style="color:#6c5ce7;margin:0">' + (campaign.name || ('РК #' + campaign.campaign_id)) + '</h3>';
    html += '<span style="font-size:.82em;color:#999">ID: ' + campaign.campaign_id + '</span>';
    html += '<span style="' + (statusColors[campaign.status]||'') + ';padding:2px 8px;border-radius:4px;font-size:.82em">' + (statusMap[campaign.status]||'') + '</span>';
    html += '</div>';

    // Метрики
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:16px">';
    html += '<div style="background:#fff4e6;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Расход</div><div style="font-weight:700;color:#e17055">' + (campaign.spent||0).toLocaleString('ru-RU',{maximumFractionDigits:0}) + ' ₽</div></div>';
    html += '<div style="background:#e8f8f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">ДРР</div><div style="font-weight:700">' + (campaign.drr||0).toFixed(1) + '%</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Показы</div><div style="font-weight:700">' + (campaign.views||0).toLocaleString('ru-RU') + '</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Клики</div><div style="font-weight:700">' + (campaign.clicks||0).toLocaleString('ru-RU') + '</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">CTR</div><div style="font-weight:700">' + (campaign.ctr||0).toFixed(2) + '%</div></div>';
    html += '<div style="background:#f0f1f5;border-radius:6px;padding:8px;text-align:center"><div style="font-size:.75em;color:#999">Заказы</div><div style="font-weight:700">' + (campaign.orders||0) + '</div></div>';
    html += '</div>';

    // Состав РК
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

/**
 * Экспорт в CSV (SheetJS не подключён — используем Blob)
 */
function exportAdsExcel() {
    if (!adsTabulator) return;
    var cols = adsTabulator.getColumnDefinitions();
    // Плоский список колонок (без групп)
    var flatCols = [];
    cols.forEach(function(c) {
        if (c.columns && c.columns.length) {
            c.columns.forEach(function(sub) { flatCols.push(sub); });
        } else {
            flatCols.push(c);
        }
    });
    var headers = flatCols.map(function(c) { return c.title; });
    var rows = adsTabulator.getData();
    var lines = [];
    lines.push(headers.map(csvEscape).join(';'));
    rows.forEach(function(row) {
        var vals = flatCols.map(function(c) {
            var v = row[c.field];
            if (c.field === 'status') v = ({'4':'Ожидает','7':'Активна','8':'Отклонена','9':'Приостановлена','11':'Завершена'})[v] || v;
            if (c.field === 'type') v = ({'4':'Авто','5':'Поиск','6':'Каталог','7':'Таргет','8':'Рек.рек.','9':'Аукцион'})[v] || v;
            return v != null ? v : '';
        });
        lines.push(vals.map(csvEscape).join(';'));
    });
    var blob = new Blob(['\uFEFF' + lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = 'ads-campaigns.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function csvEscape(v) {
    v = String(v == null ? '' : v);
    if (v.indexOf(';') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0) {
        return '"' + v.replace(/"/g, '""') + '"';
    }
    return v;
}
