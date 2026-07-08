/**
 * Promo Grid — Акции WB на Tabulator
 * Паттерн: ue-grid.js
 */

let promoTabulator = null;
let _promoAllData = [];
let _promoExpandedRow = null;

// Сброс кэша Tabulator при смене версии колонок
(function() {
    const VER = 'promo-grid-v5';
    if (localStorage.getItem('promo-grid-ver') !== VER) {
        localStorage.removeItem('tabulator-promo-grid-state-columns');
        localStorage.removeItem('tabulator-promo-grid-state-sort');
        localStorage.setItem('promo-grid-ver', VER);
    }
})();

function promoEscape(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function promoMoney(value) {
    if (value == null || value === '') return '—';
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    return n.toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽';
}

function promoDelta(value, suffix) {
    if (value == null || value === '') return '—';
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    const color = n >= 0 ? '#27ae60' : '#e74c3c';
    return '<span style="color:' + color + ';font-weight:600">' + n.toLocaleString('ru-RU', {maximumFractionDigits: 1}) + (suffix || '') + '</span>';
}

function promoDecisionLabel(value) {
    if (value === 'enter') return '<span style="color:#1e7e34;font-weight:700">✓ зайти</span>';
    if (value === 'exit') return '<span style="color:#b02a37;font-weight:700">✕ выйти</span>';
    return '—';
}

function promoDecisionButton(rowId, current, decision, label, title) {
    const active = current === decision;
    const isEnter = decision === 'enter';
    const color = isEnter ? '#1e7e34' : '#b02a37';
    const bg = active ? (isEnter ? '#d4edda' : '#f8d7da') : '#fff';
    const border = active ? color : '#d0d5dd';
    return '<button type="button" class="promo-decision-btn"'
        + ' data-id="' + promoEscape(rowId) + '"'
        + ' data-decision="' + promoEscape(decision) + '"'
        + ' title="' + promoEscape(title) + '"'
        + ' style="width:24px;height:24px;border:1px solid ' + border + ';background:' + bg + ';color:' + color + ';border-radius:4px;cursor:pointer;font-weight:700;line-height:1;margin-right:4px">'
        + label + '</button>';
}

function promoHasDecision(row, decision) {
    const selfMatch = decision
        ? row.decision === decision || (decision === 'enter' && row.plan)
        : row.decision || row.plan;
    if (selfMatch) return true;
    return (row.promotion_options || []).some(function(a) {
        return decision
            ? a.decision === decision || (decision === 'enter' && a.plan)
            : a.decision || a.plan;
    });
}

function promoSelectedDecisionCount() {
    let count = 0;
    _promoAllData.forEach(function(row) {
        if (row.decision || row.plan) count += 1;
        (row.promotion_options || []).forEach(function(action) {
            if (action.decision || action.plan) count += 1;
        });
    });
    return count;
}

function updatePromoUploadTemplatePanel() {
    const panel = document.getElementById('promo-upload-template-panel');
    const countEl = document.getElementById('promo-upload-template-count');
    if (!panel) return;
    const count = promoSelectedDecisionCount();
    panel.style.display = count > 0 ? 'flex' : 'none';
    if (countEl) {
        countEl.textContent = count + ' ' + (count === 1 ? 'строка' : count < 5 ? 'строки' : 'строк');
    }
}

function promoDateRange(start, end) {
    if (!start && !end) return 'Даты не указаны';
    if (start && end) return promoEscape(start) + ' → ' + promoEscape(end);
    return promoEscape(start || end);
}

function promoParseDate(value) {
    if (!value) return null;
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d;
}

function promoFormatShortDate(value) {
    const d = value instanceof Date ? value : promoParseDate(value);
    if (!d) return '—';
    return d.toLocaleDateString('ru-RU', {day: '2-digit', month: '2-digit'});
}

function promoOverlapsWindow(p, start, end) {
    const pStart = promoParseDate(p.start_date);
    const pEnd = promoParseDate(p.end_date);
    if (!pStart && !pEnd) return false;
    const from = pStart || pEnd;
    const to = pEnd || pStart;
    return from <= end && to >= start;
}

function getPromoColumns() {
    return [
        // === 📌 Основное ===
        {
            title: '📌 Основное',
            columns: [
                {
                    title: 'Фото', field: 'photo', width: 66, headerSort: false,
                    formatter: function(cell) {
                        const url = cell.getValue();
                        if (!url) return '';
                        const thumb = url.replace('/hq/','/c246x328/').replace('/big/','/c246x328/');
                        return '<img src="' + thumb + '" style="width:46px;height:46px;border-radius:4px;object-fit:cover">';
                    }
                },
                { title: 'Теги', field: 'tags', headerTooltip: 'Теги', width: 70, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Арт WB', field: 'nm_id',
                    headerTooltip: 'Артикул WB', width: 90, headerSort: true,
                    formatter: function(cell) {
                        const nmId = cell.getValue();
                        if (!nmId) return '—';
                        return '<a href="https://www.wildberries.ru/catalog/' + encodeURIComponent(nmId) + '/detail.aspx" target="_blank" rel="noopener" style="font-weight:700;color:#6c5ce7;text-decoration:none">' + promoEscape(nmId) + '</a>';
                    }
                },
                { title: 'Предмет', field: 'subject_name',
                    headerTooltip: 'Предмет/категория', width: 120, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Арт продавца', field: 'vendor_code',
                    headerTooltip: 'Артикул продавца', width: 80, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Бренд', field: 'brand',
                    headerTooltip: 'Бренд', width: 70, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Размер', field: 'size_name',
                    headerTooltip: 'Размер', width: 50, headerSort: true },
            ]
        },

        // === 📊 Показатели ===
        {
            title: '📊 Показатели',
            columns: [
                { title: 'Оборач.', field: 'turnover',
                    headerTooltip: 'Оборачиваемость', width: 70, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v != null ? v : '—'; }
                },
                { title: 'Остаток', field: 'stock_qty',
                    headerTooltip: 'Остаток товара', width: 70, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v != null ? v : '—'; }
                },
                { title: 'Цена до СПП', field: 'price_before_spp',
                    headerTooltip: 'Цена до СПП', width: 90, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Маржа %', field: 'margin_pct',
                    headerTooltip: 'Маржа актуальная', width: 70, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v != null ? v + '%' : '—'; }
                },
                { title: 'РРЦ', field: 'rrc_price',
                    headerTooltip: 'РРЦ из Справочника', width: 80, headerSort: true,
                    formatter: function(cell) { return promoMoney(cell.getValue()); }
                },
            ]
        },

        // === 🏷 Акция ===
        {
            title: '🏷 Акция',
            columns: [
                { title: 'Акция', field: 'promo_title',
                    headerTooltip: 'Название акции', width: 140, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Начало', field: 'promo_start',
                    headerTooltip: 'Дата начала акции', width: 90, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'Конец', field: 'promo_end',
                    headerTooltip: 'Дата окончания акции', width: 90, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'Важность', field: 'promo_importance',
                    headerTooltip: 'Важность акции', width: 75, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (!v) return '—';
                        const colors = {'high':'background:#f8d7da','medium':'background:#fff3cd','low':'background:#d4edda'};
                        return '<span style="' + (colors[v]||'') + ';padding:2px 6px;border-radius:3px;font-size:.85em">' + v + '</span>';
                    }
                },
                { title: 'Факт', field: 'in_any_promo',
                    headerTooltip: 'Сейчас в любой акции: regular из WB API или auto/public snapshot', width: 60, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue();
                        return v ? '<span style="background:#d4edda;padding:2px 6px;border-radius:3px">✓</span>' : '—';
                    }
                },
                { title: 'Источник', field: 'snapshot_in_promo',
                    headerTooltip: 'Источник факта участия', width: 74, headerSort: true,
                    formatter: function(cell) {
                        const row = cell.getRow().getData();
                        if (row.regular_in_promo) return '<span style="background:#e8f4ff;padding:2px 6px;border-radius:3px">WB</span>';
                        if (row.auto_in_promo) return '<span style="background:#fff3cd;padding:2px 6px;border-radius:3px">auto</span>';
                        return '—';
                    }
                },
                { title: 'План', field: 'plan',
                    headerTooltip: 'ЛПР отметил для участия', width: 60, headerSort: true,
                    editor: true, editorParams: { values: { true: '✓', false: '—' } },
                    formatter: function(cell) {
                        const v = cell.getValue();
                        return v ? '<span style="background:#cce5ff;padding:2px 6px;border-radius:3px;cursor:pointer">✓</span>' : '<span style="cursor:pointer">—</span>';
                    },
                    cellClick: function(e, cell) {
                        cell.setValue(!cell.getValue());
                    }
                },
                { title: 'Цена в акции', field: 'price_in_promo',
                    headerTooltip: 'Цена в акции', width: 90, headerSort: true,
                    editor: 'number', editorParams: { step: 1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Прибыль в акции', field: 'profit_in_promo',
                    headerTooltip: 'Прибыль в акции', width: 100, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (v == null) return '—';
                        const color = parseFloat(v) >= 0 ? '#27ae60' : '#e74c3c';
                        return '<span style="color:' + color + '">' + parseFloat(v).toLocaleString('ru-RU') + ' ₽</span>';
                    }
                },
                { title: 'Δ маржи', field: 'margin_delta',
                    headerTooltip: 'Разница маржи', width: 80, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (v == null) return '—';
                        const color = parseFloat(v) >= 0 ? '#27ae60' : '#e74c3c';
                        return '<span style="color:' + color + '">' + parseFloat(v).toLocaleString('ru-RU') + ' ₽</span>';
                    }
                },
                { title: 'Статус', field: 'status_text',
                    headerTooltip: 'Статус из шаблона WB', width: 100, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
            ]
        },
    ];
}

function initPromoGrid() {
    const container = document.getElementById('promo-tabulator');
    if (!container) {
        console.warn('[Promo Grid] Container #promo-tabulator not found');
        return;
    }

    // Стиль заголовков
    if (!document.getElementById('promo-header-style')) {
        const style = document.createElement('style');
        style.id = 'promo-header-style';
        style.textContent = '.tabulator-col-title { font-size: 8px !important; line-height: 1.1 !important; padding: 2px 4px !important; } .tabulator-col .tabulator-col-content { padding: 2px 4px !important; } .tabulator-cell { font-size: 11px !important; } .truncate-cell .tabulator-cell { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; }';
        document.head.appendChild(style);
    }

    promoTabulator = new Tabulator("#promo-tabulator", {
        columns: getPromoColumns(),
        data: [],
        layout: 'fitDataFill',
        index: '_promo_row_id',
        movableColumns: true,
        resizable: true,
        sortable: true,
        height: '70vh',
        virtualDom: true,
        virtualDomBuffer: 100,
        placeholder: 'Нажмите 🔄 Обновить для загрузки данных',
        stickyHeader: true,
        headerSortClickElement: 'header',
        columnHeaderSortMulti: true,
        persistence: {
            columns: true,
            sort: true,
        },
        persistenceID: 'promo-grid-state',

        groupBy: function(data) {
            if (data._noGroup) return '';
            return data.nm_id;
        },
        groupStartOpen: true,
        groupToggleElement: 'header',
        groupHeader: function(value, count, data, group) {
            if (!value) return '';
            const d = data[0] || {};
            const name = (d.product_name || '').substring(0, 40);
            const vc = d.vendor_code || '';
            const photo = d.photo ? d.photo.replace('/hq/','/c246x328/').replace('/big/','/c246x328/') : '';
            const img = photo ? '<img src="' + photo + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover;vertical-align:middle;margin-right:8px">' : '';
            return '<span style="font-size:6px;line-height:1">' + img + '<b>' + value + '</b> — ' + count + ' ' + (count === 1 ? 'размер' : count < 5 ? 'размера' : 'размеров') + ' &nbsp; <span style="color:#666">' + name + '</span> &nbsp; <span style="color:#999">[' + vc + ']</span></span>';
        },
    });

    promoTabulator.on('rowClick', function(e, row) {
        if (e.target.closest('a') || e.target.closest('.promo-actions-row')) return;
        togglePromoProductActions(row);
    });

    console.log('[Promo Grid] Tabulator initialized');
}

async function loadPromoData() {
    const promotionId = document.getElementById('promo-flt-action')?.value || '';
    let url = '/api/v1/nl/promotions/products?org_id=' + ORG_ID;
    if (promotionId) url += '&promotion_id=' + encodeURIComponent(promotionId);

    try {
        const res = await fetch(url, {headers:{'Authorization':'Bearer '+TOKEN}});
        const raw = await res.json();
        const data = Array.isArray(raw) ? raw : (raw.items || []);

        // Помечаем безразмерные и добавляем уникальный ID
        const nmCounts = {};
        data.forEach(p => { nmCounts[p.nm_id] = (nmCounts[p.nm_id] || 0) + 1; });
        data.forEach((p, i) => {
            p._noGroup = nmCounts[p.nm_id] === 1 && (!p.size_name || p.size_name === '0' || p.size_name === 'ONE SIZE');
            p._promo_row_id = p.id || (p.nm_id + '_' + p.wb_promotion_ext_id + '_' + i);
        });

        if (promoTabulator) {
            promoTabulator.replaceData(data);
        } else {
            initPromoGrid();
            promoTabulator.replaceData(data);
        }

        const countEl = document.getElementById('promo-count');
        if (countEl) countEl.textContent = data.length + ' товаров';

        _promoAllData = data;
        populatePromoFilterOptions();
        updatePromoUploadTemplatePanel();

        console.log('[Promo Grid] Loaded', data.length, 'rows');
        loadPromoSummary();
    } catch (e) {
        console.error('[Promo Grid] Load error:', e);
    }
}

function applyPromoFilters() {
    if (!_promoAllData.length) return;

    const search = (document.getElementById('promo-flt-search')?.value || '').toLowerCase();
    const fltBrand = document.getElementById('promo-flt-brand')?.value || '';
    const fltStatus = document.getElementById('promo-flt-status')?.value || '';
    const fltAction = document.getElementById('promo-flt-action')?.value || '';

    let filtered = _promoAllData;

    if (search) {
        filtered = filtered.filter(p =>
            (p.product_name || '').toLowerCase().includes(search) ||
            String(p.nm_id).includes(search) ||
            (p.vendor_code || '').toLowerCase().includes(search)
        );
    }
    if (fltBrand) filtered = filtered.filter(p => (p.brand || '') === fltBrand);
    if (fltStatus === 'in_action') filtered = filtered.filter(p => p.in_any_promo);
    if (fltStatus === 'plan') filtered = filtered.filter(p => promoHasDecision(p, 'enter'));
    if (fltStatus === 'not_in') filtered = filtered.filter(p => !p.in_any_promo && !promoHasDecision(p));
    if (fltAction) {
        filtered = filtered.filter(p => {
            if (String(p.wb_promotion_ext_id || '') === String(fltAction)) return true;
            return (p.promotion_options || []).some(opt => String(opt.promotion_id || '') === String(fltAction));
        });
    }

    closePromoProductActions();
    if (promoTabulator) promoTabulator.replaceData(filtered);
    const countEl = document.getElementById('promo-count');
    if (countEl) countEl.textContent = filtered.length + ' товаров';
}

function resetPromoFilters() {
    document.getElementById('promo-flt-action').value = '';
    document.getElementById('promo-flt-brand').value = '';
    document.getElementById('promo-flt-status').value = '';
    document.getElementById('promo-flt-search').value = '';
    applyPromoFilters();
}

function closePromoProductActions() {
    if (_promoExpandedRow) {
        const el = _promoExpandedRow.getElement();
        const next = el.nextElementSibling;
        if (next && next.classList.contains('promo-actions-row')) {
            next.remove();
        }
        _promoExpandedRow = null;
    }
}

function togglePromoProductActions(row) {
    const el = row.getElement();

    if (_promoExpandedRow === row) {
        closePromoProductActions();
        return;
    }

    closePromoProductActions();
    _promoExpandedRow = row;

    const data = row.getData();
    const actions = data.promotion_options || [];
    const nmId = data.nm_id;

    const tr = document.createElement('tr');
    tr.className = 'promo-actions-row';
    const td = document.createElement('td');
    td.colSpan = 20;
    td.style.cssText = 'padding:6px 10px;background:#f8f9fa;border-bottom:2px solid #6c5ce7';

    const priceBlockOffset = 686;
    const detailCss = 'margin-left:' + priceBlockOffset + 'px;max-width:calc(100% - ' + priceBlockOffset + 'px);overflow-x:auto';
    const tableCss = 'width:1066px;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;font-size:11px;table-layout:fixed';
    const thCss = 'padding:4px 6px;text-align:left;color:#777;background:#f3f4f6;border-bottom:1px solid #e5e7eb;font-weight:600;white-space:nowrap';
    const tdCss = 'padding:4px 6px;border-bottom:1px solid #eef0f3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis';

    let html = '<div style="' + detailCss + '"><table style="' + tableCss + '">';
    html += '<thead><tr>';
    html += '<th style="' + thCss + ';width:84px">Выбор</th>';
    html += '<th style="' + thCss + ';width:82px">Текущая</th>';
    html += '<th style="' + thCss + ';width:82px">Нужная</th>';
    html += '<th style="' + thCss + ';width:82px">В акции</th>';
    html += '<th style="' + thCss + ';width:82px">Прибыль</th>';
    html += '<th style="' + thCss + ';width:76px">Δ маржи</th>';
    html += '<th style="' + thCss + ';width:80px">РРЦ</th>';
    html += '<th style="' + thCss + ';width:72px">Тип</th>';
    html += '<th style="' + thCss + ';width:220px">Акция</th>';
    html += '<th style="' + thCss + ';width:116px">Даты</th>';
    html += '<th style="' + thCss + ';width:90px">Статус</th>';
    html += '</tr></thead><tbody>';

    html += '<tr>';
    html += '<td style="' + tdCss + '">—</td>';
    html += '<td style="' + tdCss + '">' + promoMoney(data.price_before_spp || data.price_basic) + '</td>';
    html += '<td style="' + tdCss + '">—</td>';
    html += '<td style="' + tdCss + '">' + promoMoney(data.price_product) + '</td>';
    html += '<td style="' + tdCss + '">—</td>';
    html += '<td style="' + tdCss + '">—</td>';
    html += '<td style="' + tdCss + '">' + promoMoney(data.rrc_price) + '</td>';
    html += '<td style="' + tdCss + '">auto</td>';
    html += '<td style="' + tdCss + ';font-weight:600" title="Автоакция WB">Автоакция WB</td>';
    html += '<td style="' + tdCss + '">сейчас</td>';
    html += '<td style="' + tdCss + '">' + (data.auto_in_promo ? '<span style="color:#1e7e34;font-weight:700">✓</span>' : '—') + '</td>';
    html += '</tr>';

    if (!actions.length) {
        html += '<tr><td style="' + tdCss + '" colspan="11">Нет доступных regular-акций по WB Calendar API</td></tr>';
    } else {
        actions.forEach(function(a) {
            const title = a.title || ('Акция ' + (a.promotion_id || ''));
            const status = a.in_action ? '<span style="color:#1e7e34;font-weight:700">✓ участвует</span>' : 'доступна';
            html += '<tr>';
            html += '<td style="' + tdCss + '">' + promoDecisionButton(a.id, a.decision, 'enter', '✓', 'Зайти в акцию') + promoDecisionButton(a.id, a.decision, 'exit', '✕', 'Выйти из акции') + '</td>';
            html += '<td style="' + tdCss + '">' + promoMoney(data.price_before_spp || a.current_price) + '</td>';
            html += '<td style="' + tdCss + '">' + promoMoney(a.required_price) + '</td>';
            html += '<td style="' + tdCss + '">' + promoMoney(a.price_in_promo) + '</td>';
            html += '<td style="' + tdCss + '">' + promoDelta(a.profit_in_promo, ' ₽') + '</td>';
            html += '<td style="' + tdCss + '">' + promoDelta(a.margin_delta, ' ₽') + '</td>';
            html += '<td style="' + tdCss + '">' + promoMoney(data.rrc_price) + '</td>';
            html += '<td style="' + tdCss + '">' + promoEscape(a.promo_type || 'regular') + '</td>';
            html += '<td style="' + tdCss + ';font-weight:600" title="' + promoEscape(title) + '">' + promoEscape(title) + '</td>';
            html += '<td style="' + tdCss + '">' + promoDateRange(a.start_date, a.end_date) + '</td>';
            html += '<td style="' + tdCss + '">' + status + '</td>';
            html += '</tr>';
        });
    }
    html += '</tbody></table></div>';

    td.innerHTML = html;
    td.querySelectorAll('.promo-decision-btn').forEach(function(btn) {
        btn.addEventListener('click', function(e) {
            setPromoActionDecision(e, row, btn.dataset.id, btn.dataset.decision);
        });
    });
    tr.appendChild(td);
    el.parentNode.insertBefore(tr, el.nextSibling);
}

async function setPromoActionDecision(e, row, rowId, decision) {
    e.preventDefault();
    e.stopPropagation();
    if (!rowId || !decision) return;

    const data = row.getData();
    const actions = data.promotion_options || [];
    const action = actions.find(a => String(a.id || '') === String(rowId));
    const nextDecision = action && action.decision === decision ? '' : decision;

    try {
        const res = await fetch('/api/v1/nl/promotions/products/save?org_id=' + ORG_ID, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer '+TOKEN },
            body: JSON.stringify({ items: [{ id: rowId, decision: nextDecision }] })
        });
        const result = await res.json();
        if (!res.ok || !result.ok) {
            throw new Error(result.error || ('HTTP ' + res.status));
        }

        actions.forEach(function(a) {
            if (String(a.id || '') === String(rowId)) {
                a.decision = nextDecision || null;
                a.plan = nextDecision === 'enter';
            }
        });
        if (String(data.id || '') === String(rowId)) {
            data.decision = nextDecision || null;
            data.plan = nextDecision === 'enter';
        }
        data.plan = data.decision === 'enter' || actions.some(a => a.decision === 'enter' || a.plan);
        row.update({
            promotion_options: actions,
            decision: data.decision,
            plan: data.plan,
        });
        _promoAllData.forEach(function(item) {
            if (String(item.id || '') === String(rowId)) {
                item.decision = nextDecision || null;
                item.plan = nextDecision === 'enter';
            }
            (item.promotion_options || []).forEach(function(a) {
                if (String(a.id || '') === String(rowId)) {
                    a.decision = nextDecision || null;
                    a.plan = nextDecision === 'enter';
                }
            });
        });
        closePromoProductActions();
        togglePromoProductActions(row);
        updatePromoUploadTemplatePanel();
    } catch (err) {
        alert('Ошибка сохранения решения: ' + err.message);
    }
}

function populatePromoFilterOptions() {
    if (!_promoAllData.length) return;

    const brands = [...new Set(_promoAllData.map(p => p.brand).filter(Boolean))].sort();
    const brandSel = document.getElementById('promo-flt-brand');
    if (brandSel) {
        const current = brandSel.value;
        brandSel.innerHTML = '<option value="">Бренд: все</option>';
        brands.forEach(b => {
            const opt = document.createElement('option');
            opt.value = b;
            opt.textContent = b;
            brandSel.appendChild(opt);
        });
        brandSel.value = current;
    }
}

async function loadPromoActions() {
    try {
        const res = await fetch('/api/v1/nl/promotions?org_id=' + ORG_ID, {headers:{'Authorization':'Bearer '+TOKEN}});
        const promos = await res.json();
        const sel = document.getElementById('promo-flt-action');
        if (!sel) return;
        const current = sel.value;
        sel.innerHTML = '<option value="">Все акции</option>';
        (Array.isArray(promos) ? promos : []).forEach(p => {
            const opt = document.createElement('option');
            opt.value = p.promotion_id;
            opt.textContent = (p.title || 'Акция ' + p.promotion_id) + ' (' + (p.promo_type || '?') + ')';
            sel.appendChild(opt);
        });
        sel.value = current;
    } catch (e) {
        console.error('[Promo Grid] Load actions error:', e);
    }
}

async function savePromoData() {
    if (!promoTabulator) return;
    const allData = promoTabulator.getData();
    if (!allData.length) { alert('Нет данных'); return; }

    const items = allData.map(r => ({
        id: r.id,
        plan: r.plan || false,
        price_in_promo: r.price_in_promo,
    }));

    try {
        const res = await fetch('/api/v1/nl/promotions/products/save?org_id=' + ORG_ID, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer '+TOKEN },
            body: JSON.stringify({ items })
        });
        const result = await res.json();
        if (result.ok) {
            alert('Сохранено: ' + items.length + ' строк');
        } else {
            alert('Ошибка: ' + (result.error || 'неизвестная'));
        }
    } catch (e) {
        alert('Ошибка сохранения: ' + e.message);
    }
}

function exportPromoExcel() {
    downloadPromoExcel();
}

function uploadPromoTemplate(input) { uploadPromoExcel(); input.value = ''; }

async function uploadPromoExcel() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.xlsx,.xls';
    input.onchange = async function() {
        const file = input.files[0];
        if (!file) return;
        const formData = new FormData();
        formData.append('file', file);
        try {
            const res = await fetch('/api/v1/nl/promotions/upload-excel?org_id=' + ORG_ID, {headers:{'Authorization':'Bearer '+TOKEN},
                method: 'POST',
                body: formData
            });
            const result = await res.json();
            if (result.ok) {
                alert('Загружено: ' + (result.count || 0) + ' строк');
                loadPromoData();
                loadPromoActions();
            } else {
                alert('Ошибка: ' + (result.error || 'неизвестная'));
            }
        } catch (e) {
            alert('Ошибка загрузки: ' + e.message);
        }
    };
    input.click();
}

// === Блок 1: Сводка по акциям ===

async function loadPromoSummary() {
    const container = document.getElementById('promo-summary-cards');
    if (!container) return;

    try {
        const res = await fetch('/api/v1/nl/promotions/summary?org_id=' + ORG_ID, {
            headers: {'Authorization': 'Bearer ' + TOKEN}
        });
        const data = await res.json();

        // Основная статистика
        const totalEl = document.getElementById('promo-total-products');
        const inPromoEl = document.getElementById('promo-in-promo');
        const pctEl = document.getElementById('promo-in-promo-pct');

        if (totalEl) totalEl.textContent = data.total_products || 0;
        if (inPromoEl) inPromoEl.textContent = data.in_promotion || 0;
        if (pctEl) {
            const pct = data.in_promotion_pct || 0;
            pctEl.textContent = pct + '%';
            pctEl.style.color = pct > 50 ? '#27ae60' : pct > 20 ? '#f39c12' : '#e74c3c';
        }

        // Календарь акций: по умолчанию окно сегодня ± 3 дня
        const promos = data.by_promotion || [];
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const rangeStart = new Date(today);
        rangeStart.setDate(today.getDate() - 3);
        const rangeEnd = new Date(today);
        rangeEnd.setDate(today.getDate() + 3);
        const windowPromos = promos
            .filter(p => promoOverlapsWindow(p, rangeStart, rangeEnd))
            .sort((a, b) => (promoParseDate(a.start_date)?.getTime() || 0) - (promoParseDate(b.start_date)?.getTime() || 0));
        const showCount = Math.min(windowPromos.length, 10);
        let html = '';

        html += '<div style="flex:0 0 100%;font-size:.82em;color:#666;margin-bottom:2px">'
            + '<b>Календарь акций</b> · '
            + promoFormatShortDate(rangeStart) + ' — ' + promoFormatShortDate(rangeEnd)
            + ' · сегодня ± 3 дня'
            + '</div>';

        for (let i = 0; i < showCount; i++) {
            const p = windowPromos[i];
            const pctColor = p.pct > 50 ? '#27ae60' : p.pct > 20 ? '#f39c12' : '#95a5a6';
            html += '<div class="promo-card" style="'
                + 'background:#fff;border:1px solid #e0e0e0;border-radius:8px;'
                + 'padding:10px 14px;min-width:190px;flex:1;cursor:pointer"'
                + ' onclick="filterByPromo(\'' + (p.promotion_id || '') + '\')">'
                + '<div style="font-size:.75em;color:#888;margin-bottom:4px">' 
                + (p.title || 'Акция ' + p.promotion_id).substring(0, 28)
                + '</div>'
                + '<div style="font-size:.72em;color:#666;margin-bottom:6px">'
                + promoFormatShortDate(p.start_date) + ' → ' + promoFormatShortDate(p.end_date)
                + ' · ' + promoEscape(p.promo_type || 'regular')
                + '</div>'
                + '<div style="display:flex;align-items:baseline;gap:6px">'
                + '<span style="font-size:1.3em;font-weight:700;color:' + pctColor + '">' + p.pct + '%</span>'
                + '<span style="font-size:.75em;color:#999">' + p.count + ' тов.</span>'
                + '</div>'
                + '</div>';
        }

        if (windowPromos.length > 10) {
            html += '<div class="promo-card" style="'
                + 'background:#f8f9fa;border:1px dashed #ccc;border-radius:8px;'
                + 'padding:10px 14px;min-width:120px;display:flex;align-items:center;justify-content:center;color:#666;font-size:.85em">'
                + '+' + (windowPromos.length - 10) + ' ещё</div>';
        }

        if (!windowPromos.length) {
            html += '<div style="color:#999;font-size:.85em;padding:12px;background:#fff;border:1px solid #e5e7eb;border-radius:8px">В окне сегодня ± 3 дня акций нет</div>';
        }
        container.innerHTML = html;
    } catch (e) {
        console.error('[Promo] Summary load error:', e);
        container.innerHTML = '<div style="color:#e74c3c;font-size:.85em;padding:8px">Ошибка загрузки сводки</div>';
    }
}

function filterByPromo(promotionId) {
    const sel = document.getElementById('promo-flt-action');
    if (sel && promotionId) {
        sel.value = promotionId;
        applyPromoFilters();
    }
}

async function downloadPromoExcel() {
    const promotionId = document.getElementById('promo-flt-action')?.value || '';
    let url = '/api/v1/nl/promotions/download-excel?org_id=' + ORG_ID + '&selected_only=1';
    if (promotionId) url += '&promotion_id=' + encodeURIComponent(promotionId);
    try {
        const res = await fetch(url, {headers: {'Authorization': 'Bearer ' + TOKEN}});
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const blob = await res.blob();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'promotions_export.xlsx';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
    } catch (e) {
        alert('Ошибка экспорта: ' + e.message);
    }
}

async function downloadPromoWbTemplate() {
    const promotionId = document.getElementById('promo-flt-action')?.value || '';
    let url = '/api/v1/nl/promotions/download-wb-template?org_id=' + ORG_ID;
    if (promotionId) url += '&promotion_id=' + encodeURIComponent(promotionId);
    try {
        const res = await fetch(url, {headers: {'Authorization': 'Bearer ' + TOKEN}});
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const blob = await res.blob();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'wb_upload_template.xlsx';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
    } catch (e) {
        alert('Ошибка скачивания шаблона ВБ: ' + e.message);
    }
}

async function switchPromoStore() {
    const sel = document.getElementById("promo-store");
    if (!sel) return;
    const newOrgId = sel.value;
    if (newOrgId === ORG_ID) return;
    ORG_ID = newOrgId;
    localStorage.setItem("nl_org_id", ORG_ID);
    const sideSel = document.getElementById("org-select");
    if (sideSel) sideSel.value = ORG_ID;
    const ueSel = document.getElementById("ue-store");
    if (ueSel) ueSel.value = ORG_ID;
    history.replaceState(null, "", "/nl/v2?org=" + ORG_ID);
    loadPromoData();
    loadPromoActions();
    loadPromoSummary();
}
