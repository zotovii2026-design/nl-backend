/**
 * Cost Grid — Справочник на Tabulator
 * Заменяет HTML-конкатенацию applyCostFilters()
 */

let costTabulator = null;
let _costEditedIds = new Set();  // entity_id строк с реальными изменениями
let _costEditedFieldsById = new Map();  // entity_id -> Set(field)
let _costTopQueryEditedIds = new Set();  // entity_id строк, где меняли top_query_*
let _costSyncingTopQueries = false;
const COST_ARTICLE_ONLY_FIELDS = [
    'product_status', 'tags',
    'season_jan', 'season_feb', 'season_mar', 'season_apr',
    'season_may', 'season_jun', 'season_jul', 'season_aug',
    'season_sep', 'season_oct', 'season_nov', 'season_dec',
    'top_query_1', 'top_query_2', 'top_query_3'
];
const COST_PARENT_PROPAGATE_FIELDS = [
    'product_status', 'tags', 'product_class', 'brand',
    'fulfillment_model', 'fbs_warehouse',
    'cost_price', 'extra_costs', '_tax_rate_override', 'vat_rate',
    'plan_length', 'plan_width', 'plan_height', 'plan_volume', 'plan_weight',
    'season_jan', 'season_feb', 'season_mar', 'season_apr',
    'season_may', 'season_jun', 'season_jul', 'season_aug',
    'season_sep', 'season_oct', 'season_nov', 'season_dec',
    'top_query_1', 'top_query_2', 'top_query_3',
    'buyout_niche_pct', 'mp_correction_pct', 'ad_plan_rub',
    'supply_days', 'min_batch_fbo', 'transport_pack_qty', 'rrc_price', 'min_price'
];

function costEscapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function costTagList(value) {
    const seen = new Set();
    const result = [];
    String(value || '').split(/[,;\n]/).forEach(function(part) {
        const clean = part.trim();
        const key = clean.toLowerCase();
        if (clean && !seen.has(key)) {
            seen.add(key);
            result.push(clean);
        }
    });
    return result;
}

function costTagsToString(tags) {
    return costTagList(tags).join(', ');
}

function costTagsEditor(cell, onRendered, success, cancel) {
    const input = document.createElement('input');
    input.type = 'text';
    input.value = costTagsToString(cell.getValue());
    input.placeholder = 'тег1, тег2';
    input.style.cssText = 'width:100%;height:100%;box-sizing:border-box;border:1px solid #6c5ce7;padding:3px 5px;font-size:11px';
    onRendered(function() {
        input.focus();
        input.select();
    });
    input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') success(costTagsToString(input.value));
        if (e.key === 'Escape') cancel();
    });
    input.addEventListener('blur', function() {
        success(costTagsToString(input.value));
    });
    return input;
}

function costTodayIso() {
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, '0');
    const dd = String(today.getDate()).padStart(2, '0');
    return yyyy + '-' + mm + '-' + dd;
}

function resetCostEditTracking() {
    _costEditedIds.clear();
    _costEditedFieldsById.clear();
    _costTopQueryEditedIds.clear();
}

function costNmIdKey(data) {
    return String(data.nm_id_display || data.nm_id || '').replace('_solo_', '');
}

function syncCostTopQueriesForNmId(sourceRow) {
    if (!costTabulator || !sourceRow || _costSyncingTopQueries) return;
    const source = sourceRow.getData();
    const nmKey = costNmIdKey(source);
    if (!nmKey) return;
    const payload = {
        top_query_1: source.top_query_1 || '',
        top_query_2: source.top_query_2 || '',
        top_query_3: source.top_query_3 || '',
        change_date: costTodayIso(),
    };
    _costSyncingTopQueries = true;
    const updates = [];
    costTabulator.getRows().forEach(function(row) {
        if (row === sourceRow) return;
        const data = row.getData();
        if (costNmIdKey(data) !== nmKey) return;
        const editedId = data.entity_id || data._id;
        ['top_query_1', 'top_query_2', 'top_query_3'].forEach(function(field) {
            costMarkEdited(data, field, true);
        });
        updates.push(row.update(payload));
    });
    Promise.all(updates).finally(function() {
        _costSyncingTopQueries = false;
    });
}

function costBaseNmId(data) {
    return parseInt(data.nm_id_display) || parseInt(String(data.nm_id || '').replace('_solo_', '')) || null;
}

function costRowsForSave() {
    if (!costTabulator) return [];
    const result = [];
    costTabulator.getData().forEach(function(row) {
        if (row._isArticleRow) {
            (row._children || []).forEach(function(child) { result.push(child); });
        } else {
            result.push(row);
        }
    });
    return result;
}

function costFindChildRows(nmId, skipRow) {
    const rows = [];
    if (!costTabulator) return rows;
    costTabulator.getRows().forEach(function(row) {
        if (row === skipRow) return;
        const data = row.getData();
        if (data._isArticleRow) return;
        if (costBaseNmId(data) === Number(nmId)) rows.push(row);
    });
    return rows;
}

function costMarkEdited(data, field, topQueryEdited) {
    const editedId = data.entity_id || data._id;
    if (editedId) _costEditedIds.add(editedId);
    if (editedId && field) {
        if (!_costEditedFieldsById.has(editedId)) _costEditedFieldsById.set(editedId, new Set());
        _costEditedFieldsById.get(editedId).add(field);
    }
    if (topQueryEdited && editedId) _costTopQueryEditedIds.add(editedId);
}

function costRecalcDerived(data, field, value) {
    const updates = {};
    if (field === 'cost_price') {
        updates._total_cost = ((parseFloat(value)||0) + (parseFloat(data.extra_costs)||0)).toFixed(2);
    } else if (field === 'extra_costs') {
        updates._total_cost = ((parseFloat(data.cost_price)||0) + (parseFloat(value)||0)).toFixed(2);
    }
    if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') {
        const l = (field === 'plan_length') ? parseFloat(value)||0 : parseFloat(data.plan_length)||0;
        const w = (field === 'plan_width') ? parseFloat(value)||0 : parseFloat(data.plan_width)||0;
        const h = (field === 'plan_height') ? parseFloat(value)||0 : parseFloat(data.plan_height)||0;
        updates.plan_volume = (l > 0 && w > 0 && h > 0) ? ((l * w * h) / 1000) : null;
    }
    return updates;
}

function costPropagateField(row, field, value) {
    const data = row.getData();
    const nmId = costBaseNmId(data);
    if (!nmId) return;
    const shouldPropagate = data._isArticleRow || COST_ARTICLE_ONLY_FIELDS.indexOf(field) !== -1;
    if (!shouldPropagate || COST_PARENT_PROPAGATE_FIELDS.indexOf(field) === -1) return;
    const update = {};
    update[field] = value;
    (data._children || []).forEach(function(childData) {
        Object.assign(childData, update, costRecalcDerived(childData, field, value));
        costMarkEdited(childData, field, field === 'top_query_1' || field === 'top_query_2' || field === 'top_query_3');
        if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') costMarkEdited(childData, 'plan_volume', false);
    });
    costFindChildRows(nmId, row).forEach(function(childRow) {
        const childData = childRow.getData();
        const childUpdate = Object.assign({}, update, costRecalcDerived(childData, field, value));
        costMarkEdited(childData, field, field === 'top_query_1' || field === 'top_query_2' || field === 'top_query_3');
        if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') costMarkEdited(childData, 'plan_volume', false);
        childRow.update(childUpdate);
    });
}

function setCostTreeOpen(open) {
    if (!costTabulator) return;
    const rows = costTabulator.getRows().filter(function(row) {
        const data = row.getData();
        return data && data._isArticleRow;
    });
    const expandBtn = document.getElementById('cost-expand-all-btn');
    const collapseBtn = document.getElementById('cost-collapse-all-btn');
    [expandBtn, collapseBtn].forEach(function(btn) {
        if (btn) btn.disabled = true;
    });
    const batchSize = open ? 25 : 60;
    let index = 0;
    function finish() {
        [expandBtn, collapseBtn].forEach(function(btn) {
            if (btn) btn.disabled = false;
        });
        if (costTabulator && typeof costTabulator.redraw === 'function') costTabulator.redraw(false);
    }
    function runBatch() {
        const end = Math.min(index + batchSize, rows.length);
        if (costTabulator && typeof costTabulator.blockRedraw === 'function') costTabulator.blockRedraw();
        for (; index < end; index += 1) {
            const row = rows[index];
            if (open && typeof row.treeExpand === 'function') row.treeExpand();
            if (!open && typeof row.treeCollapse === 'function') row.treeCollapse();
        }
        if (costTabulator && typeof costTabulator.restoreRedraw === 'function') costTabulator.restoreRedraw();
        if (index < rows.length) {
            window.requestAnimationFrame(runBatch);
        } else {
            finish();
        }
    }
    window.requestAnimationFrame(runBatch);
}

// Конфигурация колонок справочника
function getCostColumns() {
    return [
        // === Чекбокс ===
        {
            title: '☑',
            field: '_selected',
            width: 40,
            headerSort: false,
            movable: false,
            cssClass: 'sticky-col',
            formatter: function(cell) {
                return '<input type="checkbox" class="cost-row-check" style="cursor:pointer"' + (cell.getValue() ? ' checked' : '') + '>';
            },
            cellClick: function(e, cell) {
                cell.setValue(!cell.getValue());
                updateBulkBar();
            },
            headerClick: function(e, column) {
                const all = costTabulator.getData();
                const anyChecked = all.some(r => r._selected);
                all.forEach(r => r._selected = !anyChecked);
                // Перерисовываем чекбоксы через replaceData
                costTabulator.replaceData(all);
                updateBulkBar();
            }
        },

        // === 📌 Основное ===
        {
            title: '📌 Основное',
            columns: [
                {
                    title: 'Статус товара', field: 'product_status',
                    headerTooltip: 'Статус товара', width: 120, headerSort: true,
                    editor: 'list',
                    editorParams: {
                        values: (typeof getProductStatusEditorValues === 'function')
                            ? getProductStatusEditorValues()
                            : {'':'-', 'Новинка':'Новинка', 'Выводим':'Выводим', 'ТОП (А)':'ТОП (А)', 'Двигаем (В)':'Двигаем (В)', 'Категория С':'Категория С', 'Планируется к запуску':'Планируется к запуску'},
                        clearable: true,
                    },
                    formatter: function(cell) {
                        const v = cell.getValue() || '';
                        const label = (typeof getProductStatusLabel === 'function') ? getProductStatusLabel(v) : v;
                        const style = (typeof getProductStatusChipStyle === 'function') ? getProductStatusChipStyle(v) : '';
                        return '<span style="' + style + ';padding:2px 6px;border-radius:3px;font-size:.85em">' + (label || '—') + '</span>';
                    },
                },
                { title: 'Теги', field: 'tags',
                    headerTooltip: 'Ручные теги через запятую. Enter сохраняет ввод в ячейке.', width: 150,
                    editor: costTagsEditor, headerSort: true, tooltip: true, cssClass: 'truncate-cell',
                    formatter: function(cell) {
                        const tags = costTagList(cell.getValue());
                        if (!tags.length) return '<span style="color:#aaa">+ тег</span>';
                        return tags.map(function(tag) {
                            const safe = costEscapeHtml(tag);
                            return '<span style="display:inline-flex;align-items:center;gap:3px;margin:1px 3px 1px 0;padding:1px 5px;background:#eef1f5;border-radius:3px">' +
                                safe + '<button type="button" data-tag-remove="' + safe + '" title="Удалить тег" style="border:0;background:transparent;color:#777;cursor:pointer;font-size:10px;line-height:1;padding:0">x</button></span>';
                        }).join('');
                    },
                    cellClick: function(e, cell) {
                        const tag = e.target && e.target.getAttribute && e.target.getAttribute('data-tag-remove');
                        if (!tag) return;
                        e.preventDefault();
                        e.stopPropagation();
                        const tags = costTagList(cell.getValue()).filter(function(item) { return item !== tag; });
                        cell.setValue(tags.join(', '), true);
                    }
                },
                { title: 'Класс товара', field: 'product_class',
                    headerTooltip: 'Класс товара', width: 60, editor: 'list', editorParams: { values: ['A', 'B', 'C'], clearable: true }, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Бренд', field: 'brand',
                    headerTooltip: 'Бренд', width: 70, editor: 'input', headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                {
                    title: 'Фото', field: 'photo_main', width: 66, headerSort: false,
                    formatter: function(cell) {
                        const url = cell.getValue();
                        if (!url) return '';
                        const thumb = url.replace('/hq/','/c246x328/').replace('/big/','/c246x328/').replace('/tm/','/c246x328/');
                        return '<img src="' + thumb + '" style="width:46px;height:46px;border-radius:4px;object-fit:cover">';
                    }
                },
                { title: 'Категория', field: 'subject_name',
                    headerTooltip: 'Категория (предмет)', width: 100, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Арт продавца', field: 'vendor_code',
                    headerTooltip: 'Артикул продавца', width: 80, headerSort: true, tooltip: function(cell) {
                    var d = cell.getRow().getData();
                    var vc = d.vendor_code || '';
                    var sz = d.size_name && d.size_name !== '0' ? d.size_name : '';
                    return vc + (sz ? ' (' + sz + ')' : '');
                }, cssClass: 'truncate-cell' },
                { title: 'Баркод', field: '_barcodes',
                    headerTooltip: 'Штрихкоды', width: 80, headerSort: false, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Размер', field: '_sizeList',
                    headerTooltip: 'Размер', width: 60, headerSort: true },
                { title: 'Арт WB', field: 'nm_id_display',
                    headerTooltip: 'Артикул WB (клик — открыть на WB)', width: 85, headerSort: true,
                    formatter: function(cell) {
                        var nmId = cell.getValue();
                        // Если nm_id_display содержит _solo_ (безразмерный товар), достать настоящий nm_id из данных строки
                        if (!nmId || (typeof nmId === 'string' && nmId.startsWith('_solo_'))) {
                            var rowData = cell.getRow().getData();
                            nmId = rowData.nm_id;
                            if (!nmId || (typeof nmId === 'string' && nmId.startsWith('_solo_'))) return '';
                        }
                        var url = 'https://www.wildberries.ru/catalog/' + nmId + '/detail.aspx';
                        return '<a href="' + url + '" target="_blank" style="color:#5b4a9e;text-decoration:none;font-weight:bold" title="Открыть на Wildberries">' + nmId + '</a>';
                    }
                },
                { title: 'Товар', field: 'product_name',
                    headerTooltip: 'Название товара', width: 120, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
            ]
        },

        // === 🚚 Логистика ===
        {
            title: '🚚 Логистика',
            columns: [
                {
                    title: 'Отгрузка', field: 'fulfillment_model',
                    headerTooltip: 'Отгрузка (ФБО/ФБС)', width: 65, headerSort: true,
                    editor: 'list',
                    editorParams: { values: {'fbo':'ФБО','fbs':'ФБС'}, clearable: true },
                    formatter: function(cell) {
                        const v = cell.getValue() === 'fbs' ? 'ФБС' : 'ФБО';
                        return v;
                    },
                },
                {
                    title: 'Склад FBS', field: 'fbs_warehouse',
                    headerTooltip: 'Склад отгрузки FBS', width: 100, headerSort: true,
                    editor: 'list',
                    editable: function(cell) {
                        var row = cell.getRow().getData();
                        return row.fulfillment_model === 'fbs';
                    },
                    editorParams: function(cell) {
                        var values = {'':'-'};
                        (FBS_WAREHOUSES||[]).forEach(function(w) {
                            values[w.name] = w.name;
                        });
                        return { values: values, clearable: true };
                    },
                    formatter: function(cell) {
                        var row = cell.getRow().getData();
                        var v = cell.getValue();
                        if (row.fulfillment_model !== 'fbs') return '<span style="color:#ccc">—</span>';
                        return v || '—';
                    }
                },
            ]
        },

        // === 💰 Себестоимость ===
        {
            title: '💰 Себестоимость',
            columns: [
                { title: 'Себестоимость ₽', field: 'cost_price',
                    headerTooltip: 'Себестоимость, ₽', width: 100, editor: 'number', headerSort: true,
                    editorParams: { step: 0.01 }, formatter: function(cell) { const v = parseFloat(cell.getValue()); return v ? '<b>' + v.toLocaleString('ru-RU') + '</b>' : '—'; } },
                { title: 'Доп расходы ₽', field: 'extra_costs',
                    headerTooltip: 'Дополнительные расходы, ₽', width: 90, editor: 'number', headerSort: true },
                { title: 'Итого ₽', field: '_total_cost',
                    headerTooltip: 'Себестоимость итого, ₽', width: 100, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? '<b>' + parseFloat(v).toLocaleString('ru-RU') + '</b>' : '—'; },
                    mutator: function(value, data) { return ((parseFloat(data.cost_price)||0) + (parseFloat(data.extra_costs)||0)).toFixed(2); }
                },
                {
                    title: 'Налог %', field: '_tax_rate_override',
                    headerTooltip: 'Налог, %', width: 55, headerSort: true,
                    cssClass: 'tax-cell',
                    editor: 'number',
                    editorParams: { step: 0.01, min: 0, max: 100 },
                    formatter: function(cell) {
                        const override = cell.getValue();
                        if (override !== null && override !== '' && override !== undefined) return '<b>' + parseFloat(override) + '%</b>';
                        const defaultRate = _taxSettings.tax_rate;
                        return defaultRate ? '<span style="color:#6c5ce7">' + defaultRate + '%</span>' : '—';
                    }
                },
                {
                    title: 'НДС от дохода', field: 'vat_rate',
                    headerTooltip: 'НДС от дохода', width: 55, headerSort: false,
                    editor: 'list',
                    editorParams: { values: {0:'нет',5:'5%',7:'7%'}, clearable: true },
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (!v || v === 0 || v === 'нет') return 'нет';
                        return v + '%';
                    },
                },
            ]
        },

        // === 📐 Габариты ПЛАН ===
        {
            title: '📐 Габариты ПЛАН',
            columns: [
                { title: 'Длина', field: 'plan_length',
                    headerTooltip: 'Длина (ПЛАН), см', width: 50, editor: 'number', editorParams: {step:0.1,min:0} },
                { title: 'Ширина', field: 'plan_width',
                    headerTooltip: 'Ширина (ПЛАН), см', width: 50, editor: 'number', editorParams: {step:0.1,min:0} },
                { title: 'Высота', field: 'plan_height',
                    headerTooltip: 'Высота (ПЛАН), см', width: 50, editor: 'number', editorParams: {step:0.1,min:0} },
                { title: 'Объём, л', field: 'plan_volume',
                    headerTooltip: 'Объём (ПЛАН), л', width: 55, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v) : '—'; }
                },
                { title: 'Вес, гр', field: 'plan_weight',
                    headerTooltip: 'Вес (ПЛАН), гр', width: 55, editor: 'number', editorParams: {step:1,min:0} },
            ]
        },

        // === 📐 Габариты ФАКТ ===
        {
            title: '📐 Габариты ФАКТ',
            columns: [
                { title: 'Д×Ш×В', field: '_fact_dims',
                    headerTooltip: 'Габариты ФАКТ (Д×Ш×В)', width: 70, tooltip: true, headerSort: false, formatter: 'plaintext' },
                { title: 'Объём, л', field: '_fact_volume',
                    headerTooltip: 'Объём ФАКТ, л', width: 55, headerSort: false },
                { title: 'Вес, кг', field: '_fact_weight',
                    headerTooltip: 'Вес ФАКТ (от ВБ), кг', width: 50, headerSort: false },
            ]
        },

        // === 📊 Сезонность (неделимый блок) ===
        {
            title: '📊 Коэффициент сезонности',
            columns: [
                { title: 'янв', field: 'season_jan',
                    headerTooltip: 'Коэфф. сезонности — Январь', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'фев', field: 'season_feb',
                    headerTooltip: 'Коэфф. сезонности — Февраль', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'мар', field: 'season_mar',
                    headerTooltip: 'Коэфф. сезонности — Март', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'апр', field: 'season_apr',
                    headerTooltip: 'Коэфф. сезонности — Апрель', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'май', field: 'season_may',
                    headerTooltip: 'Коэфф. сезонности — Май', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'июн', field: 'season_jun',
                    headerTooltip: 'Коэфф. сезонности — Июнь', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'июл', field: 'season_jul',
                    headerTooltip: 'Коэфф. сезонности — Июль', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'авг', field: 'season_aug',
                    headerTooltip: 'Коэфф. сезонности — Август', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'сен', field: 'season_sep',
                    headerTooltip: 'Коэфф. сезонности — Сентябрь', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'окт', field: 'season_oct',
                    headerTooltip: 'Коэфф. сезонности — Октябрь', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'ноя', field: 'season_nov',
                    headerTooltip: 'Коэфф. сезонности — Ноябрь', width: 40, editor: 'number', cssClass: 'season-cell' },
                { title: 'дек', field: 'season_dec',
                    headerTooltip: 'Коэфф. сезонности — Декабрь', width: 40, editor: 'number', cssClass: 'season-cell' },
            ]
        },

        // === 🔍 ТОП запросы ===
        {
            title: '🔍 ТОП запросы',
            columns: [
                { title: '1', field: 'top_query_1',
                    headerTooltip: 'ТОП запрос #1', width: 70, editor: 'input', tooltip: true, cssClass: 'topquery-cell truncate-cell' },
                { title: '2', field: 'top_query_2',
                    headerTooltip: 'ТОП запрос #2', width: 70, editor: 'input', tooltip: true, cssClass: 'topquery-cell truncate-cell' },
                { title: '3', field: 'top_query_3',
                    headerTooltip: 'ТОП запрос #3', width: 70, editor: 'input', tooltip: true, cssClass: 'topquery-cell truncate-cell' },
            ]
        },

        // === 🎯 Расчёты ===
        {
            title: '🎯 Расчёты',
            columns: [
                { title: '% выкупа по кат.', field: 'buyout_niche_pct',
                    headerTooltip: '% выкупа по категории', width: 65, editor: 'number', headerSort: true },
                { title: 'Корр. комиссии %', field: 'mp_correction_pct',
                    headerTooltip: 'Коррекция к комиссии МП, %', width: 70, editor: 'number', headerSort: true },
                { title: 'Рекл. расходы %', field: 'ad_plan_rub',
                    headerTooltip: 'Рекламные расходы, % (по умолчанию 5%)', width: 65, editor: 'number',
                    editorParams: {step:0.1, min:0, max:99},
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (v !== null && v !== '' && v !== undefined) return parseFloat(v) + '%';
                        return '<span style="color:#999">5%</span>';
                    }
                },
                { title: 'Скорость достав., дн', field: 'supply_days',
                    headerTooltip: 'Скорость доставки, дней', width: 60, editor: 'number', editorParams: {min:0} },
                { title: 'Мин партия', field: 'min_batch_fbo',
                    headerTooltip: 'Минимальная партия FBO', width: 60, editor: 'number', editorParams: {min:1} },
                { title: 'РРЦ', field: 'rrc_price',
                    headerTooltip: 'Рекомендованная розничная цена', width: 60, editor: 'number', headerSort: true },
                { title: 'Мин. цена', field: 'min_price',
                    headerTooltip: 'Минимальная цена', width: 65, editor: 'number', headerSort: true },
                { title: 'Кратность вложения', field: 'transport_pack_qty',
                    headerTooltip: 'количество в транспортной упаковке', width: 80, editor: 'number',
                    editorParams: {min:1, step:1}, headerSort: true },
                { title: 'Дата правок', field: 'change_date',
                    headerTooltip: 'Дата внесения правок (авто)', width: 80, tooltip: true, cssClass: 'truncate-cell', editable: false },
                { title: 'Дата начала', field: 'valid_from',
                    headerTooltip: 'Дата начала действия', width: 80, tooltip: true, cssClass: 'truncate-cell', editor: 'input' },
            ]
        },
    ];
}

/**
 * Подготовить данные для Tabulator из _costProducts + _costMap
 */
function prepareCostData(products) {
    // Count entities per nm_id to detect sizeless products
    const nmCounts = {};
    products.forEach(p => { nmCounts[p.nm_id] = (nmCounts[p.nm_id] || 0) + 1; });

    const rows = products.map(p => {
        const c = _costMap[p.entity_id] || {};
        const isSizeless = nmCounts[p.nm_id] === 1 && (!p.size_name || p.size_name === '0' || p.size_name === 'ONE SIZE');

        // Фактические габариты
        const factDims = (p.length || '') + '×' + (p.width || '') + '×' + (p.height || '') || '—';

        // Объём плана (авто)
        const planVol = (c.plan_volume) ? parseFloat(c.plan_volume) :
            ((parseFloat(c.plan_length)||0) > 0 && (parseFloat(c.plan_width)||0) > 0 && (parseFloat(c.plan_height)||0) > 0)
            ? ((parseFloat(c.plan_length) * parseFloat(c.plan_width) * parseFloat(c.plan_height)) / 1000) : null;

        return {
            _id: p.entity_id || (p.nm_id + '_' + (p.size_name || '0')), // уникальный ID = entity_id
            _selected: false,
            _hasSizes: false,
            _sizesData: [],
            _noGroup: isSizeless, // безразмерные — без группировки

            // Данные продукта (из API /control)
            entity_id: p.entity_id || '',
            nm_id_display: (typeof p.nm_id === "number" ? p.nm_id : parseInt(p.nm_id)) || p.nm_id,
            nm_id: isSizeless ? ('_solo_' + (p.entity_id || (p.nm_id + '_0'))) : p.nm_id,
            size_name: p.size_name || '',
            product_name: p.product_name || '',
            vendor_code: p.vendor_code || c.vendor_code || '',
            photo_main: p.photo_main || '',
            subject_name: c.subject_name || p.subject_name || '',
            _barcodes: c.barcodes || c.barcode || p.barcode || '',
            _sizeList: p.size_name && p.size_name !== '0' ? p.size_name : '—',
            _fact_dims: factDims,
            _fact_volume: p.volume || '—',
            _fact_weight: p.weight || '—',

            // Данные себестоимости (из /cost-prices)
            product_status: c.product_status || '',
            tags: c.tags || '',
            product_class: c.product_class || '',
            brand: c.brand || '',
            fulfillment_model: c.fulfillment_model || 'fbo',
            fbs_warehouse: (c.fbs_warehouse && c.fbs_warehouse !== '0') ? c.fbs_warehouse : '',
            cost_price: c.cost_price || '',
            extra_costs: c.extra_costs || '',
            _total_cost: ((parseFloat(c.cost_price)||0) + (parseFloat(c.extra_costs)||0)).toFixed(2),
            _tax_rate_override: c.tax_rate || '',
            vat_rate: c.vat_rate || 0,
            tax_system: c.tax_system || "",
            plan_length: c.plan_length || '',
            plan_width: c.plan_width || '',
            plan_height: c.plan_height || '',
            plan_volume: planVol,
            plan_weight: c.plan_weight || '',
            season_jan: c.season_jan || '', season_feb: c.season_feb || '', season_mar: c.season_mar || '',
            season_apr: c.season_apr || '', season_may: c.season_may || '', season_jun: c.season_jun || '',
            season_jul: c.season_jul || '', season_aug: c.season_aug || '', season_sep: c.season_sep || '',
            season_oct: c.season_oct || '', season_nov: c.season_nov || '', season_dec: c.season_dec || '',
            top_query_1: c.top_query_1 || '', top_query_2: c.top_query_2 || '', top_query_3: c.top_query_3 || '',
            buyout_niche_pct: c.buyout_niche_pct || '',
            mp_correction_pct: c.mp_correction_pct || '',
            ad_plan_rub: (c.ad_plan_rub !== null && c.ad_plan_rub !== '' && c.ad_plan_rub !== undefined) ? c.ad_plan_rub : '',
            supply_days: c.supply_days || '',
            min_batch_fbo: c.min_batch_fbo || '',
            transport_pack_qty: c.transport_pack_qty || 1,
            rrc_price: c.rrc_price || '',
            min_price: c.min_price || '',
            change_date: c.change_date || '',
            valid_from: c.valid_from || new Date().toISOString().split('T')[0],
        };
    });
    const grouped = {};
    const ordered = [];
    rows.forEach(function(row) {
        const nmId = costBaseNmId(row);
        if (!grouped[nmId]) {
            grouped[nmId] = [];
            ordered.push(nmId);
        }
        grouped[nmId].push(row);
    });
    const data = [];
    ordered.forEach(function(nmId) {
        const children = grouped[nmId];
        if (!children || children.length <= 1) {
            data.push(children[0]);
            return;
        }
        const first = Object.assign({}, children[0]);
        first._id = 'article_' + nmId;
        first._isArticleRow = true;
        first._selected = false;
        first.entity_id = '';
        first.nm_id = nmId;
        first.nm_id_display = nmId;
        first.size_name = '';
        first._sizeList = children.length + ' разм.';
        first._barcodes = children.map(function(child) { return child._barcodes; }).filter(Boolean).join(', ');
        first._children = children;
        data.push(first);
    });
    return data;
}

/**
 * Инициализировать Tabulator для справочника
 */
function initCostTabulator(data) {
    resetCostEditTracking();
    
    // Уничтожаем старый если есть
    if (costTabulator) {
        costTabulator.destroy();
        costTabulator = null;
    }

    // Скрываем старую таблицу
    const oldTable = document.getElementById('cost-table');
    if (oldTable) oldTable.style.display = 'none';

    // Создаём контейнер для Tabulator если нет
    let tabEl = document.getElementById('cost-tabulator');
    if (!tabEl) {
        tabEl = document.createElement('div');
        tabEl.id = 'cost-tabulator';
        tabEl.style.height = '70vh';
        // Вставляем после скрытого wrapper (НЕ внутрь)
        var target = document.getElementById('cost-tabulator-host') || document.querySelector('.main-content');
            if (target) { target.appendChild(tabEl); } else { document.body.appendChild(tabEl); }
    }

    // CSS: уменьшенный шрифт заголовков
    if (!document.getElementById('cost-header-style')) {
        const style = document.createElement('style');
        style.id = 'cost-header-style';
        style.textContent = '.tabulator-col-title { font-size: 8px !important; line-height: 1.1 !important; padding: 2px 4px !important; } .tabulator-col .tabulator-col-content { padding: 2px 4px !important; } .tabulator-cell { font-size: 11px !important; } .truncate-cell .tabulator-cell { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; } .truncate-cell { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; }';
        document.head.appendChild(style);
    }

    costTabulator = new Tabulator("#cost-tabulator", {

        data: data,
        columns: getCostColumns(),
        layout: "fitDataFill",
        index: "_id",
        movableColumns: true,
        resizable: true,
        sortable: true,
        height: '70vh',
        virtualDom: true,
        virtualDomBuffer: 100,
        placeholder: 'Нет данных',
        columnHeaderSortMulti: true,
        dataTree: true,
        dataTreeChildField: '_children',
        dataTreeStartExpanded: false,
        dataTreeElementColumn: 'nm_id_display',
        initialSort: [
            {column: 'nm_id_display', dir: 'asc'},
        ],

        // cellEdited handled via table.on event below (Tabulator 6.x)
        cellEdited: function(cell) {
            // Fallback for older Tabulator versions
            const field = cell.getField();
            const row = cell.getRow();
            const data = row.getData();

            // Очистка склада FBS при переключении на ФБО
            if (field === 'fulfillment_model') {
                if (data.fulfillment_model !== 'fbs') {
                    row.update({ 'fbs_warehouse': '' });
                } else {
                    // Автоподстановка склада Коледино при выборе ФБС
                    if (!data.fbs_warehouse || data.fbs_warehouse === '0' || data.fbs_warehouse === '-' || data.fbs_warehouse === '') {
                        var _kd = (FBS_WAREHOUSES||[]).find(function(w) { return w.name && w.name.indexOf('\u041a\u043e\u043b\u0435\u0434\u0438\u043d\u043e') !== -1; });
                        row.update({ 'fbs_warehouse': _kd ? _kd.name : '' });
                    }
                }
            }

            // Пересчёт итого при изменении себестоимости или доп расходов
            if (field === 'cost_price' || field === 'extra_costs') {
                const total = ((parseFloat(data.cost_price)||0) + (parseFloat(data.extra_costs)||0)).toFixed(2);
                row.update({ '_total_cost': total });
            }

            // Пересчёт объёма плана при изменении габаритов
            if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') {
                const l = parseFloat(data.plan_length) || 0;
                const w = parseFloat(data.plan_width) || 0;
                const h = parseFloat(data.plan_height) || 0;
                const vol = (l > 0 && w > 0 && h > 0) ? ((l * w * h) / 1000) : null;
                row.update({ 'plan_volume': vol });
            }

            // === Синхронизация полей по nm_id ===
            var syncFields = [
                'plan_length', 'plan_width', 'plan_height', 'plan_weight',
                'season_jan', 'season_feb', 'season_mar', 'season_apr',
                'season_may', 'season_jun', 'season_jul', 'season_aug',
                'season_sep', 'season_oct', 'season_nov', 'season_dec',
                'brand', 'product_status', 'product_class',
                'buyout_niche_pct', 'mp_correction_pct', 'ad_plan_rub',
                'fulfillment_model', 'rrc_price', 'min_price'
            ];

            if (syncFields.indexOf(field) !== -1 && data.nm_id && !data._noGroup && COST_ARTICLE_ONLY_FIELDS.indexOf(field) !== -1) {
                var newVal = cell.getValue();
                var nmId = data.nm_id;
                var allRows = costTabulator.getRows();
                var updates = {};
                updates[field] = newVal;

                allRows.forEach(function(r) {
                    var rd = r.getData();
                    if (rd.nm_id === nmId && rd.entity_id !== data.entity_id) {
                        var rowUpdates = Object.assign({}, updates);
                        if (field === 'cost_price') {
                            rowUpdates['_total_cost'] = ((parseFloat(newVal)||0) + (parseFloat(rd.extra_costs)||0)).toFixed(2);
                        } else if (field === 'extra_costs') {
                            rowUpdates['_total_cost'] = ((parseFloat(rd.cost_price)||0) + (parseFloat(newVal)||0)).toFixed(2);
                        }
                        if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') {
                            var sl = (field === 'plan_length') ? parseFloat(newVal)||0 : parseFloat(rd.plan_length)||0;
                            var sw = (field === 'plan_width') ? parseFloat(newVal)||0 : parseFloat(rd.plan_width)||0;
                            var sh = (field === 'plan_height') ? parseFloat(newVal)||0 : parseFloat(rd.plan_height)||0;
                            rowUpdates['plan_volume'] = (sl > 0 && sw > 0 && sh > 0) ? ((sl * sw * sh) / 1000) : null;
                        }
                        r.update(rowUpdates);
                    }
                });
            }
        },

        // Сохраняем порядок колонок при перемещении
        columnMoved: function(column, columns) {
            NLGrid.saveColumnOrder(costTabulator, 'costprice');
        },

        // Кастомные стили строк
        rowFormatter: function(row) {
            const data = row.getData();
            const el = row.getElement();
            if (data._isArticleRow) {
                el.style.background = '#f8f9ff';
                el.style.fontWeight = '600';
            }
        },
    });

    // Tabulator 6.x uses events, not config callbacks
    costTabulator.on('cellEditing', function(cell) {
        _costDirty = true;
        
    });
    var _autoUpdatingDate = false;
    costTabulator.on('cellEdited', function(cell) {
        if (_autoUpdatingDate) return;
        _costDirty = true;
        // Обработка смены ФБО/ФБС
        var field = cell.getField();
        var row = cell.getRow();
        var data = row.getData();
        costMarkEdited(data, field, field === 'top_query_1' || field === 'top_query_2' || field === 'top_query_3');
        costPropagateField(row, field, cell.getValue());
        if (field === 'fulfillment_model') {
            if (data.fulfillment_model === 'fbs') {
                if (!data.fbs_warehouse || data.fbs_warehouse === '0' || data.fbs_warehouse === '-' || data.fbs_warehouse === '') {
                    // Авто: найти Коледино в FBS_WAREHOUSES
                    var _kd = (FBS_WAREHOUSES||[]).find(function(w) { return w.name && w.name.indexOf('\u041a\u043e\u043b\u0435\u0434\u0438\u043d\u043e') !== -1; });
                    row.update({ 'fbs_warehouse': _kd ? _kd.name : '' });
                    costMarkEdited(data, 'fbs_warehouse', false);
                }
            } else {
                row.update({ 'fbs_warehouse': '' });
                costMarkEdited(data, 'fbs_warehouse', false);
            }
        }
        if (field === 'top_query_1' || field === 'top_query_2' || field === 'top_query_3') {
            syncCostTopQueriesForNmId(row);
        }
        if (field === 'plan_length' || field === 'plan_width' || field === 'plan_height') {
            costMarkEdited(data, 'plan_volume', false);
        }
        // Автопростановка даты правок при изменении любой ячейки (кроме самой change_date)
        if (cell.getField() !== 'change_date') {
            _autoUpdatingDate = true;
            cell.getRow().update({ change_date: costTodayIso() });
            _autoUpdatingDate = false;
        }
    });

    return costTabulator;
}

/**
 * Обновить данные в Tabulator (после фильтрации)
 */
function updateCostTabulator(filteredProducts) {
    const data = prepareCostData(filteredProducts);
    if (costTabulator) {
        costTabulator.replaceData(data);
    } else {
        initCostTabulator(data);
    }
    document.getElementById('cost-count').textContent = filteredProducts.length + ' товаров';
}

/**
 * Собрать данные для сохранения из Tabulator
 */
function getCostDataForSave() {
    if (!costTabulator) return [];
    const rows = costRowsForSave();
    const numberOrNull = function(value) {
        if (value === null || value === undefined) return null;
        if (typeof value === 'string' && value.trim() === '') return null;
        const n = parseFloat(String(value).replace(',', '.'));
        return Number.isFinite(n) ? n : null;
    };
    const intOrNull = function(value) {
        const n = numberOrNull(value);
        return n === null ? null : Math.trunc(n);
    };
    const textValue = function(value) {
        return value === null || value === undefined ? '' : String(value);
    };
    const edited = function(data) {
        return _costEditedIds.has(data.entity_id || data._id);
    };
    const topQueryEdited = function(data) {
        return _costTopQueryEditedIds.has(data.entity_id || data._id);
    };
    const editedFields = function(data) {
        return Array.from(_costEditedFieldsById.get(data.entity_id || data._id) || []);
    };
    return rows.filter(edited).map(data => ({
        entity_id: data.entity_id || null,
        size_name: data.size_name || '',
        nm_id: parseInt(data.nm_id_display) || parseInt(String(data.nm_id).replace('_solo_','')) || null,
        _fields: editedFields(data),
        barcode: null,
        vendor_code: null,
        purchase_cost: null, logistics_cost: null, packaging_cost: null, other_costs: null,
        extra_costs: numberOrNull(data.extra_costs),
        cost_price: numberOrNull(data.cost_price),
        min_price: numberOrNull(data.min_price),
        vat: null,
        mp_base_pct: null,
        mp_correction_pct: numberOrNull(data.mp_correction_pct),
        fulfillment_model: data.fulfillment_model || 'fbo',
        storage_pct: null,
        buyout_niche_pct: numberOrNull(data.buyout_niche_pct),
        price_before_spp_plan: null,
        price_before_spp_change: null,
        change_date: edited(data) ? (data.change_date || null) : null,
        wb_club_discount_pct: null,
        rrc_price: numberOrNull(data.rrc_price),
        ad_plan_rub: numberOrNull(data.ad_plan_rub),
        product_class: textValue(data.product_class),
        brand: textValue(data.brand),
        product_status: textValue(data.product_status),
        tags: textValue(data.tags),
        tax_system: null,
        tax_rate: (function(){
            var o = numberOrNull(data._tax_rate_override);
            if (o !== null) return o;
            return _taxSettings.tax_rate === null || _taxSettings.tax_rate === undefined ? null : numberOrNull(_taxSettings.tax_rate);
        })(),
        season_jan: numberOrNull(data.season_jan), season_feb: numberOrNull(data.season_feb),
        season_mar: numberOrNull(data.season_mar), season_apr: numberOrNull(data.season_apr),
        season_may: numberOrNull(data.season_may), season_jun: numberOrNull(data.season_jun),
        season_jul: numberOrNull(data.season_jul), season_aug: numberOrNull(data.season_aug),
        season_sep: numberOrNull(data.season_sep), season_oct: numberOrNull(data.season_oct),
        season_nov: numberOrNull(data.season_nov), season_dec: numberOrNull(data.season_dec),
        plan_length: numberOrNull(data.plan_length),
        plan_width: numberOrNull(data.plan_width),
        plan_height: numberOrNull(data.plan_height),
        plan_volume: numberOrNull(data.plan_volume),
        plan_weight: numberOrNull(data.plan_weight),
        delivery_days_to_seller: null, delivery_days_to_mp: null,
        top_query_1: textValue(data.top_query_1), top_query_2: textValue(data.top_query_2), top_query_3: textValue(data.top_query_3),
        _top_query_edited: topQueryEdited(data),
        shipment_method: null, fbs_warehouse: data.fulfillment_model === 'fbs' ? textValue(data.fbs_warehouse) : '',
        transport_pack_qty: Math.max(intOrNull(data.transport_pack_qty) || 1, 1),
        vat_rate: (function(){ var v = data.vat_rate; return (!v || v === 'нет') ? 0 : numberOrNull(v); })(),
        valid_from: data.valid_from || new Date().toISOString().split('T')[0],
        notes: null,
        source: 'manual'
    }));
}
