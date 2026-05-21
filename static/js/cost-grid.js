/**
 * Cost Grid — Справочник на Tabulator
 * Заменяет HTML-конкатенацию applyCostFilters()
 */

let costTabulator = null;

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
                        values: {
                            '':'-',
                            'Новинка':'🟢 Новинка',
                            'Выводим':'🔴 Выводим',
                            'ТОП (А)':'🔵 ТОП (А)',
                            'Двигаем (В)':'🟡 Двигаем (В)',
                            'Категория С':'⚪ Категория С',
                            'Планируется к запуску':'🟣 Планируется к запуску',
                        },
                        clearable: true,
                    },
                    formatter: function(cell) {
                        const v = cell.getValue() || '';
                        const colors = {
                            'Новинка':'background:#d4edda','Выводим':'background:#f8d7da',
                            'ТОП (А)':'background:#cce5ff','Двигаем (В)':'background:#fff3cd',
                            'Категория С':'background:#e2e3e5','Планируется к запуску':'background:#e2d9f3'
                        };
                        const labels = {
                            'Новинка':'🟢 Новинка', 'Выводим':'🔴 Выводим',
                            'ТОП (А)':'🔵 ТОП (А)', 'Двигаем (В)':'🟡 Двигаем (В)',
                            'Категория С':'⚪ Категория С', 'Планируется к запуску':'🟣 Планируется к запуску'
                        };
                        return '<span style="' + (colors[v]||'') + ';padding:2px 6px;border-radius:3px;font-size:.85em">' + (v || '—') + '</span>';
                    },
                },
                { title: 'Класс товара', field: 'product_class',
                    headerTooltip: 'Класс товара', width: 60, editor: 'input', headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
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
                { title: 'Арт WB', field: 'nm_id',
                    headerTooltip: 'Артикул WB', width: 80, headerSort: true, formatter: function(cell) { return '<b>' + cell.getValue() + '</b>'; } },
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
                { title: 'Вес', field: '_fact_weight',
                    headerTooltip: 'Вес ФАКТ', width: 50, headerSort: false },
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
                { title: 'Дата правок', field: 'change_date',
                    headerTooltip: 'Дата внесения правок', width: 80, tooltip: true, cssClass: 'truncate-cell', editor: 'input' },
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

    return products.map(p => {
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
            nm_id: p.nm_id,
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
            product_class: c.product_class || '',
            brand: c.brand || '',
            fulfillment_model: c.fulfillment_model || 'fbo',
            fbs_warehouse: c.fbs_warehouse || '',
            cost_price: c.cost_price || '',
            extra_costs: c.extra_costs || '',
            _total_cost: ((parseFloat(c.cost_price)||0) + (parseFloat(c.extra_costs)||0)).toFixed(2),
            _tax_rate_override: c.tax_rate || '',
            vat_rate: c.vat_rate || 0,
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
            rrc_price: c.rrc_price || '',
            min_price: c.min_price || '',
            change_date: c.change_date || '',
            valid_from: c.valid_from || new Date().toISOString().split('T')[0],
        };
    });
}

/**
 * Инициализировать Tabulator для справочника
 */
function initCostTabulator(data) {
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
        var wrapper = document.getElementById('cost-table-wrapper');
            if (wrapper) { wrapper.parentNode.appendChild(tabEl); } else { oldTable.parentNode.appendChild(tabEl); }
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
        initialSort: [
            {column: '_sizeList', dir: 'asc'},
        ],
        groupBy: function(data) {
            // Безразмерные товары — без группы
            if (data._noGroup) return '';
            return data.nm_id;
        },
        groupStartOpen: true,
        groupToggleElement: 'header',
        groupHeader: function(value, count, data, group) {
            if (!value) return ''; // безразмерные — пустой заголовок
            const d = data[0] || {};
            const name = (d.product_name || '').substring(0, 40);
            const vc = d.vendor_code || '';
            const photo = d.photo_main ? d.photo_main.replace('/hq/','/c246x328/').replace('/big/','/c246x328/').replace('/tm/','/c246x328/') : '';
            const img = photo ? '<img src="' + photo + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover;vertical-align:middle;margin-right:8px">' : '';
            return '<span style="font-size:6px;line-height:1">' + img + '<b>' + value + '</b> — ' + count + ' ' + (count === 1 ? 'размер' : count < 5 ? 'размера' : 'размеров') + ' &nbsp; <span style="color:#666">' + name + '</span> &nbsp; <span style="color:#999">[' + vc + ']</span></span>';
        },

        // При редактировании ячейки — обновляем вычисляемые поля + синхронизация по nm_id
        cellEdited: function(cell) {
            _costDirty = true;
            const field = cell.getField();
            const row = cell.getRow();
            const data = row.getData();

            // Очистка склада FBS при переключении на ФБО
            if (field === 'fulfillment_model') {
                if (data.fulfillment_model !== 'fbs') {
                    row.update({ 'fbs_warehouse': '' });
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

            if (syncFields.indexOf(field) !== -1 && data.nm_id && !data._noGroup) {
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
            if (data._hasSizes) {
                el.style.background = '#f8f9ff';
                el.style.cursor = 'pointer';
            }
        },
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
    document.getElementById('cost-count').textContent = data.length + ' товаров';
}

/**
 * Собрать данные для сохранения из Tabulator
 */
function getCostDataForSave() {
    if (!costTabulator) return [];
    const rows = costTabulator.getData();
    return rows.map(data => ({
        entity_id: data.entity_id || null,
        size_name: data.size_name || '',
        nm_id: parseInt(data.nm_id),
        barcode: null,
        vendor_code: null,
        purchase_cost: 0, logistics_cost: 0, packaging_cost: 0, other_costs: 0,
        extra_costs: parseFloat(data.extra_costs) || 0,
        cost_price: parseFloat(data.cost_price) || 0,
        min_price: parseFloat(data.min_price) || null,
        vat: 0,
        mp_base_pct: 0,
        mp_correction_pct: parseFloat(data.mp_correction_pct) || 0,
        fulfillment_model: data.fulfillment_model || 'fbo',
        storage_pct: 0,
        buyout_niche_pct: parseFloat(data.buyout_niche_pct) || 0,
        price_before_spp_plan: 0,
        price_before_spp_change: 0,
        change_date: data.change_date || null,
        wb_club_discount_pct: 0,
        rrc_price: parseFloat(data.rrc_price) || null,
        ad_plan_rub: (data.ad_plan_rub !== null && data.ad_plan_rub !== '' && data.ad_plan_rub !== undefined) ? parseFloat(data.ad_plan_rub) : 5,
        product_class: data.product_class || '',
        brand: data.brand || '',
        product_status: data.product_status || '',
        tax_system: '',
        tax_rate: (function(){ var o = data._tax_rate_override; return (o !== null && o !== '' && o !== undefined) ? parseFloat(o) || 0 : (_taxSettings.tax_rate || 0); })(),
        season_jan: parseFloat(data.season_jan) || null, season_feb: parseFloat(data.season_feb) || null,
        season_mar: parseFloat(data.season_mar) || null, season_apr: parseFloat(data.season_apr) || null,
        season_may: parseFloat(data.season_may) || null, season_jun: parseFloat(data.season_jun) || null,
        season_jul: parseFloat(data.season_jul) || null, season_aug: parseFloat(data.season_aug) || null,
        season_sep: parseFloat(data.season_sep) || null, season_oct: parseFloat(data.season_oct) || null,
        season_nov: parseFloat(data.season_nov) || null, season_dec: parseFloat(data.season_dec) || null,
        plan_length: parseFloat(data.plan_length) || null,
        plan_width: parseFloat(data.plan_width) || null,
        plan_height: parseFloat(data.plan_height) || null,
        plan_volume: data.plan_volume || null,
        plan_weight: parseFloat(data.plan_weight) || null,
        delivery_days_to_seller: null, delivery_days_to_mp: null,
        top_query_1: data.top_query_1 || '', top_query_2: data.top_query_2 || '', top_query_3: data.top_query_3 || '',
        shipment_method: '', fbs_warehouse: data.fbs_warehouse || '',
        vat_rate: (function(){ var v = data.vat_rate; return (!v || v === 'нет' || v === 0) ? 0 : parseFloat(v) || 0; })(),
        valid_from: data.valid_from || new Date().toISOString().split('T')[0],
        notes: '',
        source: 'manual'
    }));
}
