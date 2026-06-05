/**
 * UE Grid — Юнит-экономика на Tabulator
 * Паттерн: cost-grid.js + nl-grid.js
 * Этап 1: Каркас + заглушки, данные из текущего API
 */

let ueTabulator = null;
let _ueEditedIds = new Set();
let _ueAllData = [];  // Полные данные до фильтрации

// Сброс кэша Tabulator при смене версии колонок
(function() {
    const VER = 'ue-grid-v6';
    if (localStorage.getItem('ue-grid-ver') !== VER) {
        localStorage.removeItem('tabulator-ue-grid-state-columns');
        localStorage.removeItem('tabulator-ue-grid-state-sort');
        localStorage.setItem('ue-grid-ver', VER);
    }
})();

// Конфигурация колонок Юнит-экономики
function getUEColumns() {
    return [
        // === 📌 Основное (закреплённые) ===
        {
            title: '📌 Основное',
            columns: [
                {
                    title: 'Статус товара', field: 'product_status',
                    headerTooltip: 'Статус товара (из справочника)', width: 110, headerSort: true,
                    formatter: function(cell) {
                        const v = cell.getValue() || '';
                        const colors = {
                            'Новинка':'background:#d4edda','Выводим':'background:#f8d7da',
                            'ТОП (А)':'background:#cce5ff','Двигаем (В)':'background:#fff3cd',
                            'Категория С':'background:#e2e3e5','Планируется к запуску':'background:#e2d9f3'
                        };
                        return '<span style="' + (colors[v]||'') + ';padding:2px 6px;border-radius:3px;font-size:.85em">' + (v || '—') + '</span>';
                    }
                },
                { title: 'ABC класс товара', field: 'product_class',
                    headerTooltip: 'ABC класс товара', width: 55, headerSort: true, tooltip: true },
                { title: 'Бренд', field: 'brand',
                    headerTooltip: 'Бренд', width: 70, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                {
                    title: 'Фото', field: 'photo', width: 66, headerSort: false,
                    formatter: function(cell) {
                        const url = cell.getValue();
                        if (!url) return '';
                        const thumb = url.replace('/hq/','/c246x328/').replace('/big/','/c246x328/');
                        return '<img src="' + thumb + '" style="width:46px;height:46px;border-radius:4px;object-fit:cover">';
                    }
                },
                { title: 'Арт продавца', field: 'vendor_code',
                    headerTooltip: 'Артикул продавца', width: 80, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Размер', field: 'size_name',
                    headerTooltip: 'Размер', width: 50, headerSort: true },
                { title: 'Баркод', field: 'barcode',
                    headerTooltip: 'Баркод (по API)', width: 90, headerSort: false, tooltip: true, cssClass: 'truncate-cell' },
                { title: 'Арт WB', field: 'nm_id_display',
                    headerTooltip: 'SKU / Артикул WB (клик → страница товара)', width: 80, headerSort: true,
                    formatter: function(cell) {
                        const nmId = cell.getValue();
                        if (!nmId) return '';
                        const url = 'https://www.wildberries.ru/catalog/' + nmId + '/detail.aspx';
                        return '<a href="' + url + '" target="_blank" style="color:#5b4a9e;text-decoration:none;font-weight:bold" title="Открыть на Wildberries">' + nmId + '</a>';
                    }
                },
                { title: 'Товар', field: 'product_name',
                    headerTooltip: 'Название товара', width: 120, headerSort: true, tooltip: true, cssClass: 'truncate-cell' },
            ]
        },

        // === 💰 Себестоимость ===
        {
            title: '💰 Себестоимость',
            columns: [
                { title: 'Себестоимость ₽', field: 'cost_price',
                    headerTooltip: 'Себестоимость (из справочника)', width: 95, headerSort: true,
                    formatter: function(cell) { const v = parseFloat(cell.getValue()); return v ? '<b>' + v.toLocaleString('ru-RU') + '</b>' : '—'; }
                },
                { title: 'Доп расходы ₽', field: 'extra_costs',
                    headerTooltip: 'Дополнительные расходы', width: 85, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v : '—'; }
                },
            ]
        },

        // === 💳 Комиссия МП ===
        {
            title: '💳 Комиссия МП',
            columns: [
                { title: 'Базовый % МП', field: 'mp_base_pct',
                    headerTooltip: 'Базовый % комиссии МП (по API)', width: 75, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Коррекция % МП', field: 'mp_correction_pct',
                    headerTooltip: 'Корректировка комиссии (из справочника)', width: 85, headerSort: false,
                    editor: 'number', editorParams: { step: 0.1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Итоговый % МП', field: 'mp_total_pct',
                    headerTooltip: 'Итоговый % МП', width: 75, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? '<b>' + v + '%</b>' : '—'; }
                },
            ]
        },

        // === 📊 Выкуп ===
        {
            title: '📊 Выкуп',
            columns: [
                { title: '% выкупа по кат.', field: 'buyout_niche_pct',
                    headerTooltip: '% выкупа по категории', width: 85, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: '% выкупа факт', field: 'buyout_fact_pct',
                    headerTooltip: '% выкупа факт (по API)', width: 85, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
            ]
        },

        // === 🚚 Логистика ===
        {
            title: '🚚 Логистика',
            columns: [
                { title: 'Лог. тариф', field: 'logistics_tariff',
                    headerTooltip: 'Стоимость логистики до клиента (расчёт по тарифам WB)', width: 85, headerSort: true,
                    tooltip: function(e, cell) {
                        var row = cell.getRow().getData();
                        return row.logistics_tooltip || 'По данным tariffs/box';
                    },
                    formatter: function(cell) { var v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + ' ' + String.fromCharCode(8381) : String.fromCharCode(8212); }
                },
                { title: 'Лог. факт', field: 'logistics_actual',
                    headerTooltip: 'Логистика факт', width: 80, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Обратная лог.', field: 'reverse_logistics',
                    headerTooltip: 'Обратная логистика на 1 шт (тариф × %невозврата)', width: 80, headerSort: false,
                    tooltip: function(e, cell) {
                        var row = cell.getRow().getData();
                        var v = row.volume_liters || 0;
                        return 'Объём: ' + parseFloat(v).toFixed(3) + ' л\nТариф без коэфф. склада\nИтого: ' + (cell.getValue() || 0).toFixed(2) + ' ₽';
                    },
                    formatter: function(cell) { var v = cell.getValue(); if (v) return parseFloat(v).toLocaleString('ru-RU', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + ' ₽'; return '—'; }
                },
                { title: 'Лог. с % выкупа', field: 'logistics_with_buyout',
                    headerTooltip: 'Логистика с учётом % выкупа', width: 90, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'ИЛ', field: 'localization_idx',
                    headerTooltip: 'Индекс локализации', width: 45, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'ИРП', field: 'sales_dist_idx',
                    headerTooltip: 'Индекс распределения продаж', width: 45, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'Лог. к учёту', field: 'logistics_accounted',
                    headerTooltip: 'Логистика к учёту (с ИЛ и ИРП)', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
                { title: 'Лог. финотчет', field: 'logistics_finreport',
                    headerTooltip: 'Логистика МП финотчет', width: 90, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v || '—'; }
                },
            ]
        },

        // === 🏢 Хранение ===
        {
            title: '🏢 Хранение',
            columns: [
                { title: 'Хран. расч.', field: 'storage_tariff',
                    headerTooltip: 'Хранение в день расчётное', width: 80, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Хран. финотчет', field: 'storage_actual',
                    headerTooltip: 'Хранение в день финотчет', width: 90, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 💳 Эквайринг / Приёмка ===
        {
            title: '💳 Эквайринг / Приёмка',
            columns: [
                { title: 'Эквайринг %', field: 'acquiring_pct',
                    headerTooltip: 'Эквайринг, %', width: 75, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Эквайринг ₽', field: 'acquiring_rub',
                    headerTooltip: 'Эквайринг, ₽', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Приёмка 1 шт', field: 'acceptance_avg',
                    headerTooltip: 'Приёмка 1 шт, ср. 90 дней', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 🧾 Налоги ===
        {
            title: '🧾 Налоги',
            columns: [
                { title: 'Налог %', field: 'tax_rate',
                    headerTooltip: 'Налог % (из справочника)', width: 55, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? '<b>' + v + '%</b>' : '—'; }
                },
                { title: 'НДС от дохода', field: 'vat_rate',
                    headerTooltip: 'НДС от дохода (из справочника)', width: 65, headerSort: false,
                    formatter: function(cell) {
                        const v = cell.getValue();
                        if (!v || v === 0) return 'нет';
                        return v + '%';
                    }
                },
                { title: 'Налог итого %', field: 'tax_total_pct',
                    headerTooltip: 'Налог итого, %', width: 75, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Налог итого ₽', field: 'tax_total',
                    headerTooltip: 'Налог итого, ₽', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 📢 Реклама ===
        {
            title: '📢 Реклама',
            columns: [
                { title: 'Рекл. факт ₽', field: 'ad_fact_rub',
                    headerTooltip: 'Рекламные расходы факт, ₽', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Рекл. план ₽', field: 'ad_plan_rub',
                    headerTooltip: 'Рекламные расходы план, ₽ (ручной ввод)', width: 85, headerSort: false,
                    editor: 'number', editorParams: { step: 1, min: 0 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === ⚠️ Прочие удержания ===
        {
            title: '⚠️ Прочие',
            columns: [
                { title: 'Прочие ₽', field: 'other_deductions',
                    headerTooltip: 'Прочие удержания (ручной ввод)', width: 85, headerSort: false,
                    editor: 'number', editorParams: { step: 1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Прочие финотчет ₽', field: 'other_deductions_finreport',
                    headerTooltip: 'Прочие удержания (фин. отчёт)', width: 100, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 💲 Цены ===
        {
            title: '💲 Цены',
            columns: [
                { title: 'СПП %', field: 'spp_pct',
                    headerTooltip: 'СПП, %', width: 55, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Цена с СПП', field: 'price_with_spp',
                    headerTooltip: 'Цена с СПП', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Цена до СПП факт', field: 'price_before_spp',
                    headerTooltip: 'Цена до СПП, факт', width: 100, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v ? '<b>' + parseFloat(v).toLocaleString('ru-RU') + ' ₽</b>' : '—'; }
                },
                { title: 'Цена до СПП план', field: 'price_before_spp_plan',
                    headerTooltip: 'Цена до СПП, план (ручной ввод)', width: 100, headerSort: false,
                    editor: 'number', editorParams: { step: 1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 📊 Без WB клуба (ФАКТ) ===
        {
            title: '📊 Без WB клуба',
            columns: [
                { title: 'Расходы', field: 'expenses_fact',
                    headerTooltip: 'Расходы (без WB клуба)', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Прибыль', field: 'profit_fact',
                    headerTooltip: 'Прибыль (без WB клуба)', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Маржа', field: 'margin_fact',
                    headerTooltip: 'Маржа % (без WB клуба)', width: 65, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'ROI', field: 'roi_fact',
                    headerTooltip: 'ROI % (без WB клуба)', width: 65, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'На Р/С', field: 'to_account_fact',
                    headerTooltip: 'На расчётный счёт', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 🏷 WB Клуб ===
        {
            title: '🏷 WB Клуб / Лояльность',
            columns: [
                { title: 'Скидка WB Клуб %', field: 'wb_club_discount_pct',
                    headerTooltip: 'Скидка WB Клуб, %', width: 95, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Скидка WB Клуб ₽', field: 'wb_club_discount_rub',
                    headerTooltip: 'Скидка WB Клуб, ₽', width: 95, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Кэшбэк %', field: 'loyalty_cashback_pct',
                    headerTooltip: 'Программы лояльности (кэшбэк), %', width: 75, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Кэшбэк ₽', field: 'loyalty_cashback_rub',
                    headerTooltip: 'Кэшбэк, ₽', width: 80, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 📊 С WB клубом (ПЛАН) ===
        {
            title: '📊 С WB клубом',
            columns: [
                { title: 'Расходы', field: 'expenses_plan',
                    headerTooltip: 'Расходы план (с WB клубом)', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Прибыль', field: 'profit_plan',
                    headerTooltip: 'Прибыль план (с WB клубом)', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Маржа', field: 'margin_plan',
                    headerTooltip: 'Маржа план %', width: 65, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'ROI', field: 'roi_plan',
                    headerTooltip: 'ROI план %', width: 65, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'На Р/С', field: 'to_account_plan',
                    headerTooltip: 'На Р/С план', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
            ]
        },

        // === 🏷 Акции / Правки ===
        {
            title: '🏷 Акции / Правки',
            columns: [
                { title: 'В акции', field: 'in_promo',
                    headerTooltip: 'Позиция в акции', width: 65, headerSort: true,
                    formatter: function(cell) { const v = cell.getValue(); return v === 'ДА' || v === true ? '<span style="background:#d4edda;padding:2px 6px;border-radius:3px">ДА</span>' : '—'; }
                },
                { title: 'Дата правок', field: 'change_date',
                    headerTooltip: 'Дата внесения правок', width: 95, headerSort: false,
                    editor: 'date',
                    formatter: function(cell) { return cell.getValue() || '—'; }
                },
                { title: 'Цена к изм. ₽', field: 'price_before_spp_change',
                    headerTooltip: 'Цена до СПП к изменению', width: 95, headerSort: false,
                    editor: 'number', editorParams: { step: 1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'Скидка WB к изм.', field: 'wb_club_discount_change',
                    headerTooltip: 'Скидка WB клуба к изменению', width: 100, headerSort: false,
                    editor: 'number', editorParams: { step: 0.1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
                { title: 'Кэшбэк к изм.', field: 'loyalty_cashback_change',
                    headerTooltip: 'Скидка программ лояльности к изменению', width: 95, headerSort: false,
                    editor: 'number', editorParams: { step: 0.1 },
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
            ]
        },

        // === 📊 После правок ===
        {
            title: '📊 После правок',
            columns: [
                { title: 'Прибыль', field: 'profit_change',
                    headerTooltip: 'Прибыль после правок', width: 85, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? parseFloat(v).toLocaleString('ru-RU') + ' ₽' : '—'; }
                },
                { title: 'ROI', field: 'roi_change',
                    headerTooltip: 'ROI % после правок', width: 65, headerSort: false,
                    formatter: function(cell) { const v = cell.getValue(); return v ? v + '%' : '—'; }
                },
            ]
        },
    ];
}

/**
 * Инициализация Tabulator для Юнит-экономики
 * Стили и настройки — как в cost-grid.js (Справочник)
 */
function initUEGrid() {
    const container = document.getElementById('ue-tabulator');
    if (!container) {
        console.warn('[UE Grid] Container #ue-tabulator not found');
        return;
    }

    // === 8px стиль заголовков (как в cost-grid.js) ===
    if (!document.getElementById('ue-header-style')) {
        const style = document.createElement('style');
        style.id = 'ue-header-style';
        style.textContent = '.tabulator-col-title { font-size: 8px !important; line-height: 1.1 !important; padding: 2px 4px !important; } .tabulator-col .tabulator-col-content { padding: 2px 4px !important; } .tabulator-cell { font-size: 11px !important; } .truncate-cell .tabulator-cell { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; } .truncate-cell { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; }';
        document.head.appendChild(style);
    }

    ueTabulator = new Tabulator("#ue-tabulator", {
        columns: getUEColumns(),
        data: [],
        layout: 'fitDataFill',
        index: 'entity_id',
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
        persistenceID: 'ue-grid-state',

        // === Группировка по nm_id (как в Справочнике) ===
        // Группировка по nm_id. Безразмерные товары получили уникальный _solo_ ключ
        groupBy: 'nm_id',
        groupStartOpen: true,
        groupToggleElement: 'header',
        groupHeader: function(value, count, data, group) {
            // Скрыть заголовок для соло-товаров (безразмерных)
            if (value && typeof value === 'string' && value.startsWith('_solo_')) {
                var el = group.getElement();
                if (el) el.style.display = 'none';
                return '';
            }
            const d = data[0] || {};
            const name = (d.product_name || '').substring(0, 40);
            const vc = d.vendor_code || '';
            const photo = d.photo ? d.photo.replace('/hq/','/c246x328/').replace('/big/','/c246x328/') : '';
            const img = photo ? '<img src="' + photo + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover;vertical-align:middle;margin-right:8px">' : '';
            return '<span style="font-size:6px;line-height:1">' + img + '<b>' + value + '</b> — ' + count + ' ' + (count === 1 ? 'размер' : count < 5 ? 'размера' : 'размеров') + ' &nbsp; <span style="color:#666">' + name + '</span> &nbsp; <span style="color:#999">[' + vc + ']</span></span>';
        },

        // Подсветка строк внутри групп (как в Справочнике)
        rowFormatter: function(row) {
            const data = row.getData();
            const el = row.getElement();
            if (data._hasSizes) {
                el.style.background = '#f8f9ff';
                el.style.cursor = 'pointer';
            }
        },
    });

    // Автопростановка даты правок при изменении ячейки
    var _ueAutoDate = false;
    ueTabulator.on('cellEdited', function(cell) {
        if (_ueAutoDate) return;
        _ueEditedIds.add(cell.getRow().getData().entity_id);
        if (cell.getField() !== 'change_date') {
            _ueAutoDate = true;
            const today = new Date();
            const yyyy = today.getFullYear();
            const mm = String(today.getMonth() + 1).padStart(2, '0');
            const dd = String(today.getDate()).padStart(2, '0');
            cell.getRow().update({ change_date: yyyy + '-' + mm + '-' + dd });
            _ueAutoDate = false;
        }
    });

    console.log('[UE Grid] Tabulator initialized');
}

/**
 * Загрузка данных из API
 */
async function loadUEData(loadAll) {
    const search = document.getElementById('ue-search')?.value || '';
    let url = '/api/v1/nl/unit-economics?org_id=' + ORG_ID;
    if (search) url += '&search=' + encodeURIComponent(search);
    // Пагинация: первые 50 быстро, потом по кнопке — все
    if (!loadAll && !search) url += '&limit=50';

    // Лоадер: показываем что данные загружаются
    const container = document.getElementById('ue-tabulator');
    const countEl = document.getElementById('ue-count');
    if (container) container.style.opacity = '0.4';
    if (countEl) countEl.textContent = '⏳ Загрузка...';

    try {
        const res = await fetch(url);
        const raw = await res.json();
        const data = Array.isArray(raw) ? raw : (raw.items || []);
        const totalFromApi = raw.total || data.length;

        // Сохраняем оригинальный nm_id для отображения и ссылок
        data.forEach(p => { p.nm_id_display = p.nm_id; });

        // Помечаем безразмерные товары ПЕРЕД replaceData (для группировки — как в cost-grid.js)
        const nmCounts = {};
        data.forEach(p => { nmCounts[p.nm_id] = (nmCounts[p.nm_id] || 0) + 1; });
        data.forEach(p => {
            p._noGroup = nmCounts[p.nm_id] === 1 && (!p.size_name || p.size_name === '0' || p.size_name === 'ONE SIZE');
            p._hasSizes = !p._noGroup;
            if (p._noGroup) p.nm_id = '_solo_' + p.entity_id;
        });

        if (ueTabulator) {
            ueTabulator.replaceData(data).then(() => {
                const groups = ueTabulator.getGroups();
                groups.forEach(g => g.show());
            });
        } else {
            // Если Tabulator не инициализирован — инициализируем
            initUEGrid();
            ueTabulator.replaceData(data).then(() => {
                const groups = ueTabulator.getGroups();
                groups.forEach(g => g.show());
            });
        }

        const countEl = document.getElementById('ue-count');
        const hasMore = data.length < totalFromApi;
        if (countEl) {
            if (hasMore) {
                countEl.innerHTML = data.length + ' из ' + totalFromApi + ' товаров <button id="ue-load-all-btn" style="margin-left:8px;padding:2px 10px;border-radius:4px;border:1px solid #4a90d9;background:#4a90d9;color:#fff;cursor:pointer;font-size:12px;">Загрузить все (' + totalFromApi + ')</button>';
                const btn = document.getElementById('ue-load-all-btn');
                if (btn) btn.onclick = function() { loadUEData(true); };
            } else {
                countEl.textContent = data.length + ' товаров';
            }
        }

        _ueAllData = data;  // Сохраняем полные данные
        populateUEFilterOptions();  // Заполняем фильтры

        console.log('[UE Grid] Loaded', data.length, '/', totalFromApi, 'rows');
        if (container) container.style.opacity = '1';
    } catch (e) {
        console.error('[UE Grid] Load error:', e);
        if (container) container.style.opacity = '1';
        if (countEl) countEl.textContent = '❌ Ошибка загрузки';
    }
}

/**
 * Фильтрация данных (как applyCostFilters в Справочнике)
 */
function applyUEFilters() {
    if (!_ueAllData.length) return;

    const search = (document.getElementById('ue-flt-search')?.value || '').toLowerCase();
    const fltStatus = document.getElementById('ue-flt-status')?.value || '';
    const fltClass = document.getElementById('ue-flt-class')?.value || '';
    const fltBrand = document.getElementById('ue-flt-brand')?.value || '';
    const fltFF = document.getElementById('ue-flt-ff')?.value || '';

    let filtered = _ueAllData;

    // Поиск
    if (search) {
        filtered = filtered.filter(p =>
            (p.product_name || '').toLowerCase().includes(search) ||
            String(p.nm_id).includes(search) ||
            (p.vendor_code || '').toLowerCase().includes(search) ||
            (p.barcode || '').includes(search)
        );
    }

    // Фильтры по полям
    if (fltStatus) filtered = filtered.filter(p => (p.product_status || '') === fltStatus);
    if (fltClass) filtered = filtered.filter(p => (p.product_class || '') === fltClass);
    if (fltBrand) filtered = filtered.filter(p => (p.brand || '') === fltBrand);
    if (fltFF) filtered = filtered.filter(p => (p.tariff_type || '') === fltFF);

    // Обновляем таблицу
    if (ueTabulator) ueTabulator.replaceData(filtered);
    const countEl = document.getElementById('ue-count');
    if (countEl) countEl.textContent = filtered.length + ' товаров';
}

/**
 * Сброс всех фильтров
 */
function resetUEFilters() {
    document.getElementById('ue-flt-status').value = '';
    document.getElementById('ue-flt-class').value = '';
    document.getElementById('ue-flt-brand').value = '';
    document.getElementById('ue-flt-ff').value = '';
    document.getElementById('ue-flt-search').value = '';
    applyUEFilters();
}

/**
 * Заполнить выпадающие списки фильтров из загруженных данных
 */
function populateUEFilterOptions() {
    if (!_ueAllData.length) return;

    // Бренды
    const brands = [...new Set(_ueAllData.map(p => p.brand).filter(Boolean))].sort();
    const brandSel = document.getElementById('ue-flt-brand');
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

/**
 * Сохранение изменений
 */
async function saveUEData() {
    if (!ueTabulator) return;
    const allData = ueTabulator.getData();
    const edited = allData.filter(r => r._edited);
    if (!edited.length) {
        alert('Нет изменений для сохранения');
        return;
    }

    try {
        const res = await fetch('/api/v1/nl/unit-economics?org_id=' + ORG_ID, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ items: edited })
        });
        const result = await res.json();
        if (result.ok) {
            alert('Сохранено: ' + edited.length + ' строк');
        } else {
            alert('Ошибка: ' + (result.error || 'неизвестная'));
        }
    } catch (e) {
        alert('Ошибка сохранения: ' + e.message);
    }
}

/**
 * Экспорт в Excel
 */
function exportUEExcel() {
    if (!ueTabulator) return;
    ueTabulator.download('xlsx', 'unit-economics.xlsx', { sheetName: 'Юнит-экономика' });
}

// ─── ОБНОВЛЕНИЕ ЦЕН ИЗ WB API ────────────────────────────

let _pricesCooldownTimer = null;

/**
 * Проверить статус кулдауна и обновить кнопку
 */
async function checkPricesCooldown() {
    const btn = document.getElementById('btn-refresh-prices');
    if (!btn) return;
    const orgId = getOrgId();
    if (!orgId) { btn.disabled = true; btn.textContent = '💱 Цены из WB (нет орг.)'; return; }
    
    try {
        const res = await fetch('/api/v1/nl/prices/last-refresh?org_id=' + orgId);
        const data = await res.json();
        
        if (data.can_refresh) {
            btn.disabled = false;
            btn.textContent = '💱 Цены из WB';
            btn.style.background = '#6c5ce7';
            btn.style.cursor = 'pointer';
        } else {
            btn.disabled = true;
            btn.style.background = '#b2bec3';
            btn.style.cursor = 'not-allowed';
            const mins = Math.ceil(data.cooldown_remaining_seconds / 60);
            btn.textContent = '💱 Цены из WB (' + mins + ' мин)';
            
            // Запускаем обратный отсчёт
            if (!_pricesCooldownTimer) {
                _pricesCooldownTimer = setInterval(() => {
                    checkPricesCooldown();
                }, 30000); // Каждые 30 сек
            }
        }
    } catch (e) {
        btn.disabled = false;
        btn.textContent = '💱 Цены из WB';
        btn.style.background = '#6c5ce7';
    }
}

/**
 * Обновить цены из WB API
 */
async function refreshPricesFromWB() {
    const btn = document.getElementById('btn-refresh-prices');
    const orgId = getOrgId();
    if (!orgId) { alert('Не выбрана организация'); return; }
    
    btn.disabled = true;
    btn.textContent = '💱 Загрузка цен...';
    btn.style.background = '#b2bec3';
    
    try {
        const res = await fetch('/api/v1/nl/prices/refresh?org_id=' + orgId, { method: 'POST' });
        const data = await res.json();
        
        if (res.ok) {
            const msg = 'Цены обновлены: ' + data.updated + ' записей из ' + data.total_with_prices + ' товаров';
            btn.textContent = '💱 Цены из WB ✓';
            setTimeout(() => {
                checkPricesCooldown();
                // Перезагружаем данные таблицы
                loadUEData();
            }, 2000);
        } else {
            if (res.status === 429) {
                alert('Подождите: ' + data.detail);
            } else {
                alert('Ошибка: ' + (data.detail || 'неизвестная'));
            }
            checkPricesCooldown();
        }
    } catch (e) {
        alert('Ошибка: ' + e.message);
        checkPricesCooldown();
    }
}

// Запускаем проверку кулдауна при загрузке
document.addEventListener('DOMContentLoaded', () => {
    setTimeout(checkPricesCooldown, 1000);
});
