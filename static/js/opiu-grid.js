// Единый стиль табуляторов — эталон Справочника (8px заголовки, 11px ячейки)
(function(){
    var s = document.createElement('style');
    s.textContent = '#opiu-tabulator .tabulator-col-title{font-size:8px!important;line-height:1.1!important;padding:2px 4px!important}#opiu-tabulator .tabulator-col .tabulator-col-content{padding:2px 4px!important}#opiu-tabulator .tabulator-cell{font-size:11px!important}';
    document.head.appendChild(s);
})();

let opiuTabulator = null;
let opiuAllRows = [];
let opiuTotalRow = null;

function opiuMoney(cell) {
    const value = Number(cell.getValue() || 0);
    return value.toLocaleString('ru-RU', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function opiuPercent(cell) {
    const value = Number(cell.getValue() || 0);
    return value.toLocaleString('ru-RU', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    }) + '%';
}

function opiuQuantity(cell) {
    const value = Number(cell.getValue() || 0);
    return value.toLocaleString('ru-RU', {maximumFractionDigits: 3});
}

function opiuColumns() {
    const moneyColumn = {hozAlign: 'right', formatter: opiuMoney};
    const columns = [
        {title: 'Фото', field: 'photo_main', width: 58, headerSort: false, formatter: NLGrid.formatters.photo},
        {title: 'Артикул поставщика', field: 'vendor_code', width: 165},
        {title: 'Артикул WB', field: 'nm_id', width: 115, hozAlign: 'right'},
        {title: 'Название', field: 'product_name', width: 240, tooltip: true},
        {title: 'Кол-во реализовано, шт', field: 'sales_qty', width: 145, hozAlign: 'right', formatter: opiuQuantity},
        {title: 'Цена розничная с учетом согласованной скидки, руб', field: 'retail_sum', width: 230, ...moneyColumn},
        {title: 'Возвраты, сумма, руб', field: 'returns_retail_sum', width: 150, ...moneyColumn},
        {title: 'Цена розн. с учетом скидки за вычетом возвратов, руб', field: 'retail_net_sum', width: 250, ...moneyColumn},
        {title: 'Вайлдберриз реализовал Товар (Пр), руб', field: 'realized_sum', width: 220, ...moneyColumn},
        {title: 'Комиссия ВБ с учетом НДС, %', field: 'marketplace_commission_pct', width: 175, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Комиссия ВБ с учетом НДС, сумма, руб', field: 'marketplace_commission_sum', width: 215, ...moneyColumn},
        {title: 'Эквайринг, %', field: 'acquiring_pct', width: 120, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Эквайринг, сумма, руб', field: 'acquiring_sum', width: 155, ...moneyColumn},
        {title: 'Услуги по доставке товара покупателю, руб', field: 'delivery_total', width: 230, ...moneyColumn},
        {title: 'Общая сумма штрафов, руб', field: 'penalty', width: 160, ...moneyColumn},
        {title: 'Хранение, руб', field: 'storage', width: 120, ...moneyColumn},
        {title: 'Расхождение по хранению, руб', field: 'storage_difference_info', width: 185, headerTooltip: 'Информационная колонка: разница между детализацией и контрольной суммой, в строки не распределяется.', ...moneyColumn},
        {title: 'Внутренняя реклама WB, руб', field: 'advertising_api_spend', width: 180, ...moneyColumn},
        {title: 'Внешняя реклама, руб', field: 'external_ad_spend', width: 160, headerTooltip: 'Заглушка под внешний рекламный расход. Сейчас не участвует в расчете, пока источник не подключен.', ...moneyColumn},
        {title: 'Расхождение по рекламе, руб', field: 'advertising_difference_info', width: 180, headerTooltip: 'Информационная колонка: разница между источниками рекламы, в строки не распределяется.', ...moneyColumn},
        {title: 'ДРР, %', field: 'drr', width: 100, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Заказы, шт', field: 'orders_qty', width: 110, hozAlign: 'right', formatter: opiuQuantity},
        {title: 'Заказы, сумма, руб', field: 'orders_sum', width: 155, ...moneyColumn},
        {title: 'Стоимость участия в программе лояльности, руб', field: 'loyalty_participation', width: 235, ...moneyColumn},
        {title: 'Сумма баллов, удержанных по программе лояльности, руб', field: 'loyalty_points', width: 255, ...moneyColumn},
        {title: '% программы лояльности', field: 'loyalty_pct', width: 150, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Платная приемка, руб', field: 'acceptance', width: 150, ...moneyColumn},
        {title: 'Прочие затраты, руб', field: 'other_expenses', width: 170, headerTooltip: 'Иные удержания из детализации WB, которые не попали в отдельные колонки. Если WB не отдал артикул, сумма распределяется пропорционально количеству реализованных товаров минус возвраты.', ...moneyColumn},
        {title: 'К перечислению на р/с', field: 'net_for_pay', width: 170, ...moneyColumn},
        {title: 'Валовая прибыль, руб', field: 'gross_profit_after_ads', width: 165, ...moneyColumn},
        {title: 'Валовая рентабельность, %', field: 'gross_margin', width: 160, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Себестоимость, руб/ед', field: 'cost_unit', width: 155, ...moneyColumn},
        {title: 'Себестоимость, сумма руб', field: 'cost_total', width: 175, ...moneyColumn},
        {title: 'Налоговые удержания НДС, руб', field: 'vat_tax', width: 190, headerTooltip: 'НДС от дохода из строки справочника.', ...moneyColumn},
        {title: 'Налоговые удержания (выбранный режим), руб', field: 'selected_tax', width: 240, headerTooltip: 'Налог считается по системе и ставке из соответствующей строки справочника.', ...moneyColumn},
        {title: 'Чистая прибыль', field: 'net_profit', width: 145, ...moneyColumn},
        {title: 'Рентабельность, %', field: 'net_margin', width: 135, hozAlign: 'right', formatter: opiuPercent},
        {title: 'ROI, %', field: 'roi', width: 100, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Наценка, %', field: 'markup', width: 110, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Баркод', field: 'barcode', width: 150},
        {title: 'Размер', field: 'size_name', width: 90},
        {title: 'Бренд', field: 'brand', width: 150},
        {title: 'Категория', field: 'subject_name', width: 180},
    ];
    const savedOrder = NLGrid.loadColumnOrder('opiu-v3');
    if (!savedOrder || !savedOrder.length) return columns;
    const byField = new Map(columns.map(column => [column.field, column]));
    return savedOrder.map(field => byField.get(field)).filter(Boolean)
        .concat(columns.filter(column => !savedOrder.includes(column.field)));
}

function ensureOpiuDom() {
    const page = document.getElementById('page-opiu');
    if (!page) return false;

    const hasRequiredDom = document.getElementById('opiu-summary')
        && document.getElementById('opiu-tabulator')
        && document.getElementById('opiu-count');
    if (hasRequiredDom) return true;

    if (typeof _lazyInit === 'function') _lazyInit('opiu');
    return !!(
        document.getElementById('opiu-summary')
        && document.getElementById('opiu-tabulator')
        && document.getElementById('opiu-count')
    );
}

function isOpiuGridAttached() {
    const container = document.getElementById('opiu-tabulator');
    return !!(
        opiuTabulator
        && container
        && opiuTabulator.element === container
        && container.isConnected
    );
}

function initOpiuGrid() {
    if (!ensureOpiuDom()) return false;
    const container = document.getElementById('opiu-tabulator');
    if (!container || typeof Tabulator === 'undefined') return false;
    if (opiuTabulator) opiuTabulator.destroy();

    opiuTabulator = NLGrid.create(container, {
        data: [],
        columns: opiuColumns(),
        layout: 'fitDataFill',
        height: '520px',
        index: '_row_id',
        movableColumns: true,
        placeholder: 'Нет финансовых данных за выбранный период',
        rowFormatter: function(row) {
            const data = row.getData();
            if (data._is_total) {
                row.getElement().style.backgroundColor = '#d9ead3';
                row.getElement().style.fontWeight = '700';
            } else if (data.vendor_code === '(без артикула)') {
                row.getElement().style.backgroundColor = '#fff2cc';
            }
        },
    });
    opiuTabulator.on('columnMoved', function() {
        NLGrid.saveColumnOrder(opiuTabulator, 'opiu-v3');
    });
    return true;
}

function fillOpiuSelect(id, values, label) {
    const select = document.getElementById(id);
    if (!select) return;
    const current = select.value;
    select.innerHTML = '<option value="">' + label + ': все</option>';
    Array.from(values).filter(Boolean).sort((a, b) => a.localeCompare(b, 'ru')).forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
    });
    if (Array.from(select.options).some(option => option.value === current)) select.value = current;
}

function fillOpiuFilters(rows) {
    fillOpiuSelect('opiu-filter-class', new Set(rows.map(row => row.product_class)), 'Класс');
    fillOpiuSelect('opiu-filter-status', new Set(rows.map(row => row.product_status)), 'Статус');
    fillOpiuSelect('opiu-filter-brand', new Set(rows.map(row => row.brand)), 'Бренд');
    fillOpiuSelect('opiu-filter-category', new Set(rows.map(row => row.subject_name)), 'Категория');
}

function applyOpiuFilters() {
    if (!opiuTabulator) return;
    const query = (document.getElementById('opiu-search')?.value || '').trim().toLowerCase();
    const productClass = document.getElementById('opiu-filter-class')?.value || '';
    const status = document.getElementById('opiu-filter-status')?.value || '';
    const brand = document.getElementById('opiu-filter-brand')?.value || '';
    const category = document.getElementById('opiu-filter-category')?.value || '';

    const filtered = opiuAllRows.filter(row => {
        if (productClass && row.product_class !== productClass) return false;
        if (status && row.product_status !== status) return false;
        if (brand && row.brand !== brand) return false;
        if (category && row.subject_name !== category) return false;
        if (query) {
            const haystack = ((row.product_name || '') + ' ' + (row.brand || '')).toLowerCase();
            if (!haystack.includes(query)) return false;
        }
        return true;
    });

    const data = filtered.slice();
    if (opiuTotalRow) data.push({...opiuTotalRow, _is_total: true, _row_id: '__total__'});
    opiuTabulator.replaceData(data);
    const count = document.getElementById('opiu-count');
    if (count) count.textContent = filtered.length + ' из ' + opiuAllRows.length + ' строк';
}

function resetOpiuFilters() {
    ['opiu-filter-class', 'opiu-filter-status', 'opiu-filter-brand', 'opiu-filter-category', 'opiu-search']
        .forEach(id => {
            const element = document.getElementById(id);
            if (element) element.value = '';
        });
    applyOpiuFilters();
}

function opiuIsoDate(value) {
    return value.toISOString().slice(0, 10);
}

function opiuYesterday() {
    const base = new Date();
    base.setHours(12, 0, 0, 0);
    base.setDate(base.getDate() - 1);
    return base;
}

function getOpiuDateRange() {
    if (typeof nlGetDateRange === 'function') return nlGetDateRange();
    const period = 'yesterday';
    const base = opiuYesterday();
    let start = new Date(base);
    let end = new Date(base);

    if (period === 'yesterday') {
        // base is already yesterday
    } else if (period === 'last7') {
        start.setDate(base.getDate() - 6);
    } else if (period === 'week') {
        const mondayOffset = (base.getDay() + 6) % 7;
        start.setDate(base.getDate() - mondayOffset);
    } else if (period === 'custom') {
        const dateFrom = document.getElementById('opiu-date-from')?.value;
        const dateTo = document.getElementById('opiu-date-to')?.value;
        if (dateFrom && dateTo) return {dateFrom, dateTo};
    } else {
        start = new Date(base.getFullYear(), base.getMonth(), 1, 12);
    }
    return {dateFrom: opiuIsoDate(start), dateTo: opiuIsoDate(end)};
}

function toggleOpiuCustomPeriod() {
    const custom = document.getElementById('filter-period')?.value === 'custom';
    const range = document.getElementById('opiu-custom-period');
    if (range) range.style.display = custom ? 'flex' : 'none';
    return custom;
}

function onOpiuPeriodChange() {
    const custom = toggleOpiuCustomPeriod();
    if (!custom && _currentSection === 'opiu') loadOpiu();
}

function setDefaultOpiuDates() {
    const base = opiuYesterday();
    const start = new Date(base);
    const fromInput = document.getElementById('opiu-date-from');
    const toInput = document.getElementById('opiu-date-to');
    if (fromInput && !fromInput.value) fromInput.value = opiuIsoDate(start);
    if (toInput && !toInput.value) toInput.value = opiuIsoDate(base);
    toggleOpiuCustomPeriod();
}

function renderOpiuSyncInfo(sync) {
    const element = document.getElementById('opiu-sync-info');
    if (!element) return;
    if (!sync) {
        const hasRows = Array.isArray(opiuAllRows) && opiuAllRows.length > 0;
        element.textContent = hasRows
            ? 'Данные рассчитаны из загруженных фин. отчётов WB. Отдельный синк этого периода не найден.'
            : 'Нет данных за выбранный период. Запустите синхронизацию.';
        return;
    }
    const parts = ['Синк: ' + sync.status];
    if (sync.finished_at) parts.push(new Date(sync.finished_at).toLocaleString('ru-RU'));
    if (sync.difference != null) {
        parts.push('расхождение ' + Number(sync.difference).toLocaleString('ru-RU', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        }) + ' ₽');
    }
    element.textContent = parts.join(' · ');
}

function fmtMoney(value) {
    return Number(value || 0).toLocaleString('ru-RU', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function fmtPct(value) {
    return Number(value || 0).toLocaleString('ru-RU', {
        minimumFractionDigits: 1,
        maximumFractionDigits: 1,
    }) + '%';
}

function renderOpiuSummary(total, dateFrom, dateTo) {
    const container = document.getElementById('opiu-summary');
    if (!container || !total) return;

    const periodLabel = dateFrom && dateTo
        ? 'за ' + dateFrom + ' — ' + dateTo
        : '';

    const revenue = Number(total.realized_unit * total.sales_qty || 0);
    const netPay = Number(total.net_for_pay || 0);
    const delivery = Number(total.delivery_total || 0);
    const storage = Number(total.storage || 0);
    const penalty = Number(total.penalty || 0);
    const deduction = Number(total.deduction || 0);
    const acceptance = Number(total.acceptance || 0);
    const loyaltyPoints = Number(total.loyalty_points || 0);
    const loyaltyParticipation = Number(total.loyalty_participation || 0);
    const grossProfit = Number(total.gross_profit || 0);
    const returnsRub = Number(total.returns_rub || 0);
    const salesQty = Number(total.sales_qty || 0);
    const returnsQty = Number(total.returns_qty || 0);
    const margin = revenue ? (grossProfit / revenue * 100) : 0;

    const cards = [
        {label: 'Выручка', value: fmtMoney(revenue) + ' ₽', color: '#5B4B8A'},
        {label: 'Продажи', value: salesQty.toLocaleString('ru-RU') + ' шт', color: '#333'},
        {label: 'Возвраты', value: returnsQty.toLocaleString('ru-RU') + ' шт / ' + fmtMoney(returnsRub) + ' ₽', color: '#c0392b'},
        {label: 'К перечислению', value: fmtMoney(netPay) + ' ₽', color: '#27ae60'},
        {label: 'Комиссия МП', value: fmtMoney(total.marketplace_commission_unit * salesQty) + ' ₽', color: '#e67e22'},
        {label: 'Доставка', value: fmtMoney(delivery) + ' ₽', color: '#333'},
        {label: 'Хранение', value: fmtMoney(storage) + ' ₽', color: '#333'},
        {label: 'Штрафы', value: fmtMoney(penalty) + ' ₽', color: '#c0392b'},
        {label: 'Удержания', value: fmtMoney(deduction) + ' ₽', color: '#c0392b'},
        {label: 'Приёмка', value: fmtMoney(acceptance) + ' ₽', color: '#333'},
        {label: 'Лояльность', value: fmtMoney(loyaltyPoints + loyaltyParticipation) + ' ₽', color: '#8e44ad'},
        {label: 'Валовая прибыль', value: fmtMoney(grossProfit) + ' ₽', color: grossProfit >= 0 ? '#27ae60' : '#c0392b', bold: true},
        {label: 'Маржинальность', value: fmtPct(margin), color: margin >= 20 ? '#27ae60' : (margin >= 0 ? '#e67e22' : '#c0392b'), bold: true},
    ];

    const html = cards.map(card => {
        const style = 'padding:14px 16px;background:' + (card.bold ? '#f0ebe8' : '#f8f9fb') +
            ';border-radius:10px;border:1px solid #e8e8e8';
        return '<div style="' + style + '">' +
            '<div style="font-size:.78em;color:#888;margin-bottom:4px">' + card.label + '</div>' +
            '<div style="font-size:1.15em;font-weight:' + (card.bold ? '700' : '600') +
            ';color:' + (card.color || '#333') + '">' + card.value + '</div>' +
            '</div>';
    }).join('');

    container.innerHTML =
        '<div style="grid-column:1/-1;display:flex;align-items:center;gap:8px;margin-bottom:2px">' +
        '<span style="font-size:1.05em;font-weight:700;color:#333">Итоги по магазину</span>' +
        (periodLabel ? '<span style="font-size:.8em;color:#999">' + periodLabel + '</span>' : '') +
        '</div>' + html;
}

async function loadOpiu() {
    if (!ensureOpiuDom()) return;
    if (!ORG_ID) return;
    if (opiuTabulator && !isOpiuGridAttached()) {
        try { opiuTabulator.destroy(); } catch {}
        opiuTabulator = null;
    }
    if (!opiuTabulator) {
        const initialized = initOpiuGrid();
        if (!initialized || !opiuTabulator) {
            console.error('opiuTabulator init failed', {
                hasPage: !!document.getElementById('page-opiu'),
                hasContainer: !!document.getElementById('opiu-tabulator'),
                hasTabulator: typeof Tabulator !== 'undefined',
            });
            return;
        }
    }
    setDefaultOpiuDates();
    const range = getOpiuDateRange();
    const count = document.getElementById('opiu-count');
    if (count) count.textContent = 'Загрузка...';

    try {
        const url = '/api/v1/nl/opiu/report?org_id=' + encodeURIComponent(ORG_ID)
            + '&date_from=' + range.dateFrom + '&date_to=' + range.dateTo;
        const response = await fetch(url, {headers: {'Authorization': 'Bearer ' + TOKEN}});
        if (!response.ok) throw new Error('Не удалось загрузить ОПиУ');
        const data = await response.json();
        opiuAllRows = (data.items || []).filter(row => !row.is_unassigned).map((row, index) => ({
            ...row,
            _row_id: row.entity_id + '|' + row.barcode + '|' + row.size_name + '|' + index,
        }));
        opiuTotalRow = data.product_total || data.total || null;
        fillOpiuFilters(opiuAllRows);
        applyOpiuFilters();
        renderOpiuSyncInfo(data.sync);
        renderOpiuSummary(data.total, range.dateFrom, range.dateTo);
    } catch (error) {
        console.error('loadOpiu error:', error);
        opiuAllRows = [];
        opiuTotalRow = null;
        if (opiuTabulator) opiuTabulator.replaceData([]);
        const summary = document.getElementById('opiu-summary');
        if (summary) summary.innerHTML = '';
        if (count) count.textContent = 'Ошибка загрузки';
    }
}

async function syncOpiu() {
    if (!ORG_ID) return;
    const range = getOpiuDateRange();
    const button = document.getElementById('opiu-sync-btn');
    if (button) {
        button.disabled = true;
        button.textContent = 'Запускаю...';
    }
    try {
        const url = '/api/v1/nl/opiu/sync?org_id=' + encodeURIComponent(ORG_ID)
            + '&date_from=' + range.dateFrom + '&date_to=' + range.dateTo;
        const response = await fetch(url, {
            method: 'POST',
            headers: {'Authorization': 'Bearer ' + TOKEN},
        });
        if (!response.ok) throw new Error('Не удалось запустить синхронизацию');
        const data = await response.json();
        showToast('Синхронизация ОПиУ поставлена в очередь: ' + data.task_id);
        setTimeout(loadOpiu, 5000);
    } catch (error) {
        showToast('Ошибка синхронизации ОПиУ: ' + error.message, 'error');
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = 'Обновить из WB';
        }
    }
}

async function exportOpiuExcel() {
    if (!ORG_ID) return;
    const range = getOpiuDateRange();
    const url = '/api/v1/nl/opiu/export?org_id=' + encodeURIComponent(ORG_ID)
        + '&date_from=' + range.dateFrom + '&date_to=' + range.dateTo;
    try {
        const response = await fetch(url, {headers: {'Authorization': 'Bearer ' + TOKEN}});
        if (!response.ok) throw new Error('Не удалось сформировать Excel');
        const blob = await response.blob();
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = 'opiu_' + range.dateFrom + '_' + range.dateTo + '.xlsx';
        link.click();
        URL.revokeObjectURL(link.href);
    } catch (error) {
        showToast('Ошибка экспорта ОПиУ: ' + error.message, 'error');
    }
}
