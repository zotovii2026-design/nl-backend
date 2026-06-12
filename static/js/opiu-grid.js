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
        {title: 'Название', field: 'product_name', width: 240, tooltip: true},
        {title: 'Артикул', field: 'vendor_code', width: 165},
        {title: 'Артикул WB', field: 'nm_id', width: 115, hozAlign: 'right'},
        {title: 'Баркод', field: 'barcode', width: 150},
        {title: 'Размер', field: 'size_name', width: 90},
        {title: 'Класс', field: 'product_class', width: 110},
        {title: 'Статус', field: 'product_status', width: 140},
        {title: 'Бренд', field: 'brand', width: 150},
        {title: 'Категория', field: 'subject_name', width: 180},
        {title: 'Кол-во продаж', field: 'sales_qty', width: 125, hozAlign: 'right', formatter: opiuQuantity},
        {title: 'Цена розничная (ед.)', field: 'retail_unit', width: 155, ...moneyColumn},
        {title: 'ВБ реализовал товар (ед.)', field: 'realized_unit', width: 180, ...moneyColumn},
        {title: 'Платёжная комиссия (ед.)', field: 'acquiring_unit', width: 175, ...moneyColumn},
        {title: '% платёжной комиссии', field: 'acquiring_pct', width: 155, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Комиссия МП (ед.)', field: 'marketplace_commission_unit', width: 155, ...moneyColumn},
        {title: '% комиссии МП', field: 'marketplace_commission_pct', width: 135, hozAlign: 'right', formatter: opiuPercent},
        {title: 'Доставка (всего)', field: 'delivery_total', width: 140, ...moneyColumn},
        {title: 'Доставка (ед.)', field: 'delivery_unit', width: 130, ...moneyColumn},
        {title: 'Возвраты (шт)', field: 'returns_qty', width: 125, hozAlign: 'right', formatter: opiuQuantity},
        {title: 'Возвраты (руб)', field: 'returns_rub', width: 135, ...moneyColumn},
        {title: 'К перечислению за вычетом возвратов', field: 'net_for_pay', width: 220, ...moneyColumn},
        {title: 'Штрафы', field: 'penalty', width: 115, ...moneyColumn},
        {title: 'Хранение', field: 'storage', width: 120, ...moneyColumn},
        {title: 'Удержания', field: 'deduction', width: 120, ...moneyColumn},
        {title: 'Операции на приёмке', field: 'acceptance', width: 155, ...moneyColumn},
        {title: 'Компенсация скидки лояльности', field: 'loyalty_compensation', width: 210, ...moneyColumn},
        {title: 'Сумма баллов лояльности', field: 'loyalty_points', width: 180, ...moneyColumn},
        {title: 'Стоимость участия в лояльности', field: 'loyalty_participation', width: 200, ...moneyColumn},
        {title: 'Валовая прибыль', field: 'gross_profit', width: 155, ...moneyColumn},
    ];
    const savedOrder = NLGrid.loadColumnOrder('opiu-v2');
    if (!savedOrder || !savedOrder.length) return columns;
    const byField = new Map(columns.map(column => [column.field, column]));
    return savedOrder.map(field => byField.get(field)).filter(Boolean)
        .concat(columns.filter(column => !savedOrder.includes(column.field)));
}

function initOpiuGrid() {
    const container = document.getElementById('opiu-tabulator');
    if (!container || typeof Tabulator === 'undefined') return;
    if (opiuTabulator) opiuTabulator.destroy();

    opiuTabulator = NLGrid.create(container, {
        data: [],
        columns: opiuColumns(),
        layout: 'fitData',
        height: '68vh',
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
        NLGrid.saveColumnOrder(opiuTabulator, 'opiu-v2');
    });
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

function getOpiuDateRange() {
    const period = document.getElementById('filter-period')?.value || 'month';
    const today = new Date();
    today.setHours(12, 0, 0, 0);
    let start = new Date(today);
    let end = new Date(today);

    if (period === 'yesterday') {
        start.setDate(start.getDate() - 1);
        end = new Date(start);
    } else if (period === 'week') {
        start.setDate(start.getDate() - 6);
    } else if (period === 'custom') {
        const dateFrom = document.getElementById('opiu-date-from')?.value;
        const dateTo = document.getElementById('opiu-date-to')?.value;
        if (dateFrom && dateTo) return {dateFrom, dateTo};
    } else {
        start = new Date(today.getFullYear(), today.getMonth(), 1, 12);
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
    const today = new Date();
    today.setHours(12, 0, 0, 0);
    const start = new Date(today.getFullYear(), today.getMonth(), 1, 12);
    const fromInput = document.getElementById('opiu-date-from');
    const toInput = document.getElementById('opiu-date-to');
    if (fromInput && !fromInput.value) fromInput.value = opiuIsoDate(start);
    if (toInput && !toInput.value) toInput.value = opiuIsoDate(today);
    toggleOpiuCustomPeriod();
}

function renderOpiuSyncInfo(sync) {
    const element = document.getElementById('opiu-sync-info');
    if (!element) return;
    if (!sync) {
        element.textContent = 'Данные ещё не синхронизировались';
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

async function loadOpiu() {
    if (!ORG_ID) return;
    if (!opiuTabulator) initOpiuGrid();
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
        opiuAllRows = (data.items || []).map((row, index) => ({
            ...row,
            _row_id: row.entity_id + '|' + row.barcode + '|' + row.size_name + '|' + index,
        }));
        opiuTotalRow = data.total || null;
        fillOpiuFilters(opiuAllRows);
        applyOpiuFilters();
        renderOpiuSyncInfo(data.sync);
    } catch (error) {
        console.error('loadOpiu error:', error);
        opiuAllRows = [];
        opiuTotalRow = null;
        if (opiuTabulator) opiuTabulator.replaceData([]);
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
