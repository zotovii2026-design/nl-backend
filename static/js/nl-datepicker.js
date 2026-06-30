/**
 * NL Datepicker — Единый выбор периода для всех разделов NL Table
 *
 * Кнопки: Сегодня | Вчера | 7 дней | Месяц | Выбор даты
 * При "Выбор даты" — открывается popover с календарём (2 месяца)
 *
 * Глобальный state: NL_DATE — {preset, dateFrom, dateTo}
 * Сохраняется в localStorage, переживает перезагрузку
 */

// Внедряем CSS инлайн
(function(){
    var style = document.createElement('style');
    style.textContent = `
.nl-dp-wrap{display:flex;align-items:center;gap:8px}
.nl-dp-label{font-size:.78em;color:#666;white-space:nowrap}
.nl-dp-buttons{display:flex;align-items:center;gap:0;border:1px solid #d8d8e2;border-radius:6px;overflow:hidden;background:#fff}
.nl-dp-btn{padding:5px 12px;font-size:.82em;border:none;border-right:1px solid #e0e0e0;background:#fff;color:#333;cursor:pointer;transition:background .15s,color .15s;white-space:nowrap}
.nl-dp-btn:last-child{border-right:none}
.nl-dp-btn:hover{background:#f0f0f5}
.nl-dp-btn.nl-dp-active{background:#6c5ce7;color:#fff;font-weight:600}
.nl-dp-current{font-size:.78em;color:#444;white-space:nowrap;min-width:98px}
.nl-dp-popover{position:absolute;top:100%;right:0;margin-top:6px;background:#fff;border:1px solid #e0e0e0;border-radius:10px;box-shadow:0 8px 30px rgba(0,0,0,.12);z-index:1000;display:flex;padding:14px;gap:14px}
.nl-dp-cal{width:280px}
.nl-dp-cal-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;font-size:.9em;font-weight:600;color:#1a1a2e}
.nl-dp-nav{border:none;background:#f5f5fa;color:#333;width:28px;height:28px;border-radius:6px;cursor:pointer;font-size:1.1em;display:flex;align-items:center;justify-content:center}
.nl-dp-nav:hover{background:#e8e8f0}
.nl-dp-weekdays{display:grid;grid-template-columns:repeat(7,1fr);text-align:center;font-size:.72em;color:#999;margin-bottom:4px}
.nl-dp-weekdays span{padding:4px 0}
.nl-dp-days{display:grid;grid-template-columns:repeat(7,1fr);gap:2px}
.nl-dp-day,.nl-dp-day-empty{text-align:center;padding:6px 0;font-size:.82em;cursor:default;border-radius:6px;transition:background .1s;user-select:none}
.nl-dp-day{cursor:pointer}
.nl-dp-day:hover{background:#f0f0f5}
.nl-dp-day.nl-dp-today{font-weight:700;color:#6c5ce7}
.nl-dp-day.nl-dp-selected{background:#6c5ce7;color:#fff;font-weight:600;border-radius:6px}
.nl-dp-day.nl-dp-in-range{background:rgba(108,92,231,.15);border-radius:0}
.nl-dp-day.nl-dp-in-range.nl-dp-selected{border-radius:6px}
.nl-dp-side{width:180px;display:flex;flex-direction:column;gap:10px;padding-left:14px;border-left:1px solid #f0f0f0}
.nl-dp-range-label{font-size:.78em;color:#999;text-transform:uppercase;letter-spacing:.03em}
.nl-dp-range-dates{font-size:.9em;color:#1a1a2e;font-weight:600;flex-grow:1}
.nl-dp-apply{padding:8px 16px;background:#6c5ce7;color:#fff;border:none;border-radius:6px;font-size:.85em;cursor:pointer;font-weight:600;transition:background .15s}
.nl-dp-apply:hover{background:#5b4bd4}
.nl-dp-cancel{padding:8px 16px;background:#f5f5fa;color:#666;border:none;border-radius:6px;font-size:.85em;cursor:pointer;transition:background .15s}
.nl-dp-cancel:hover{background:#e8e8f0}
`;
    document.head.appendChild(style);
})();

var NL_DATE = {
    preset: 'yesterday',
    dateFrom: null,
    dateTo: null,
};

// Элементы внутри popover
var _nlCalStart = null; // выбранная начальная дата (Date)
var _nlCalEnd = null;   // выбранная конечная дата (Date)
var _nlCalMonth = null; // текущий просматриваемый месяц (Date)
var _nlCalSelecting = false; // идёт выделение диапазона

(function() {
    // Восстановить из localStorage
    try {
        var saved = JSON.parse(localStorage.getItem('nl_date'));
        if (saved && saved.preset) {
            NL_DATE = saved;
        }
    } catch(e) {}

    // При готовности DOM — рендер кнопок
    document.addEventListener('DOMContentLoaded', initNlDatepicker);
    // На случай если DOM уже готов (inline-script после DOM)
    if (document.readyState !== 'loading') initNlDatepicker();
})();

function initNlDatepicker() {
    var container = document.getElementById('nl-datepicker-container');
    if (!container || container.dataset.ready === '1') return;
    container.dataset.ready = '1';

    container.innerHTML = ''
        + '<div class="nl-dp-wrap">'
        + '<span class="nl-dp-label">Период:</span>'
        + '<div class="nl-dp-buttons">'
        + '  <button class="nl-dp-btn" data-preset="today" onclick="nlSetPreset(\'today\')">Сегодня</button>'
        + '  <button class="nl-dp-btn" data-preset="yesterday" onclick="nlSetPreset(\'yesterday\')">Вчера</button>'
        + '  <button class="nl-dp-btn" data-preset="last7" onclick="nlSetPreset(\'last7\')">7 дней</button>'
        + '  <button class="nl-dp-btn" data-preset="month" onclick="nlSetPreset(\'month\')">Месяц</button>'
        + '  <button class="nl-dp-btn" data-preset="custom" onclick="nlToggleCalendar()">Выбор периода</button>'
        + '</div>'
        + '<span class="nl-dp-current" id="nl-dp-current"></span>'
        + '</div>'
        + '<div class="nl-dp-popover" id="nl-dp-popover" style="display:none">'
        + '  <div class="nl-dp-cal">'
        + '    <div class="nl-dp-cal-header">'
        + '      <button onclick="nlCalShift(-1)" class="nl-dp-nav">‹</button>'
        + '      <span id="nl-dp-cal-title"></span>'
        + '      <button onclick="nlCalShift(1)" class="nl-dp-nav">›</button>'
        + '    </div>'
        + '    <div class="nl-dp-weekdays">'
        + '      <span>Пн</span><span>Вт</span><span>Ср</span><span>Чт</span><span>Пт</span><span>Сб</span><span>Вс</span>'
        + '    </div>'
        + '    <div class="nl-dp-days" id="nl-dp-cal-days"></div>'
        + '  </div>'
        + '  <div class="nl-dp-side">'
        + '    <div class="nl-dp-range-label" id="nl-dp-range-label">Выберите период</div>'
        + '    <div class="nl-dp-range-dates" id="nl-dp-range-dates">—</div>'
        + '    <button class="nl-dp-apply" onclick="nlApplyCustom()">Применить</button>'
        + '    <button class="nl-dp-cancel" onclick="nlCloseCalendar()">Отмена</button>'
        + '  </div>'
        + '</div>';

    nlHighlightPreset();
    nlUpdateCurrentLabel();
}

function nlIsoDate(d) {
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1).padStart(2, '0');
    var dd = String(d.getDate()).padStart(2, '0');
    return y + '-' + m + '-' + dd;
}

function nlYesterday() {
    var d = new Date();
    d.setHours(12, 0, 0, 0);
    d.setDate(d.getDate() - 1);
    return d;
}

function nlSetPreset(preset) {
    NL_DATE.preset = preset;
    var base = nlYesterday();
    var start = new Date(base);
    var end = new Date(base);

    if (preset === 'today') {
        var today = new Date();
        today.setHours(12, 0, 0, 0);
        start = new Date(today);
        end = new Date(today);
    } else if (preset === 'yesterday') {
        // start = end = yesterday (уже)
    } else if (preset === 'last7') {
        start.setDate(base.getDate() - 6);
    } else if (preset === 'month') {
        start = new Date(base.getFullYear(), base.getMonth(), 1, 12);
        end = new Date(base);
    }

    NL_DATE.dateFrom = nlIsoDate(start);
    NL_DATE.dateTo = nlIsoDate(end);
    nlSaveDate();
    nlCloseCalendar();
    nlHighlightPreset();
    nlUpdateCurrentLabel();
    nlNotifySection();
}

function nlToggleCalendar() {
    var pop = document.getElementById('nl-dp-popover');
    if (!pop) return;
    if (pop.style.display === 'none') {
        // Открыть
        NL_DATE.preset = 'custom';
        nlHighlightPreset();
        if (NL_DATE.dateFrom && NL_DATE.dateTo) {
            _nlCalStart = new Date(NL_DATE.dateFrom + 'T12:00:00');
            _nlCalEnd = new Date(NL_DATE.dateTo + 'T12:00:00');
        } else {
            var base = nlYesterday();
            _nlCalStart = new Date(base);
            _nlCalEnd = new Date(base);
            NL_DATE.dateFrom = nlIsoDate(base);
            NL_DATE.dateTo = nlIsoDate(base);
        }
        _nlCalMonth = new Date(_nlCalEnd);
        _nlCalSelecting = false;
        nlRenderCalendar();
        nlUpdateRangeLabel();
        pop.style.display = 'flex';
    } else {
        nlCloseCalendar();
    }
}

function nlCloseCalendar() {
    var pop = document.getElementById('nl-dp-popover');
    if (pop) pop.style.display = 'none';
}

function nlCalShift(months) {
    if (!_nlCalMonth) return;
    _nlCalMonth.setMonth(_nlCalMonth.getMonth() + months);
    nlRenderCalendar();
}

function nlRenderCalendar() {
    var titleEl = document.getElementById('nl-dp-cal-title');
    var daysEl = document.getElementById('nl-dp-cal-days');
    if (!titleEl || !daysEl) return;

    var months = ['Январь','Февраль','Март','Апрель','Май','Июнь','Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'];
    titleEl.textContent = months[_nlCalMonth.getMonth()] + ' ' + _nlCalMonth.getFullYear();

    var year = _nlCalMonth.getFullYear();
    var month = _nlCalMonth.getMonth();
    var firstDay = new Date(year, month, 1);
    var lastDay = new Date(year, month + 1, 0);

    // День недели первого числа (Пн=0)
    var startOffset = (firstDay.getDay() + 6) % 7;

    var html = '';
    // Пустые ячейки до первого числа
    for (var i = 0; i < startOffset; i++) {
        html += '<span class="nl-dp-day-empty"></span>';
    }
    // Дни месяца
    var today = new Date();
    today.setHours(0, 0, 0, 0);
    var todayIso = nlIsoDate(today);

    for (var d = 1; d <= lastDay.getDate(); d++) {
        var date = new Date(year, month, d, 12);
        var iso = nlIsoDate(date);
        var classes = 'nl-dp-day';
        var isToday = (iso === todayIso);
        var inRange = _nlCalStart && _nlCalEnd && date >= _nlCalStart && date <= _nlCalEnd;
        var isStart = _nlCalStart && iso === nlIsoDate(_nlCalStart);
        var isEnd = _nlCalEnd && iso === nlIsoDate(_nlCalEnd);

        if (isToday) classes += ' nl-dp-today';
        if (isStart || isEnd) classes += ' nl-dp-selected';
        if (inRange) classes += ' nl-dp-in-range';

        html += '<span class="' + classes + '" onclick="nlCalClick(\'' + iso + '\')" data-date="' + iso + '">' + d + '</span>';
    }
    daysEl.innerHTML = html;
}

function nlCalClick(iso) {
    var clicked = new Date(iso + 'T12:00:00');

    if (!_nlCalSelecting) {
        // Первый клик — начало диапазона
        _nlCalStart = clicked;
        _nlCalEnd = null;
        _nlCalSelecting = true;
    } else {
        // Второй клик — конец диапазона
        if (clicked < _nlCalStart) {
            _nlCalEnd = _nlCalStart;
            _nlCalStart = clicked;
        } else {
            _nlCalEnd = clicked;
        }
        _nlCalSelecting = false;
    }
    nlRenderCalendar();
    nlUpdateRangeLabel();
}

function nlUpdateRangeLabel() {
    var label = document.getElementById('nl-dp-range-label');
    var dates = document.getElementById('nl-dp-range-dates');
    if (!label || !dates) return;

    if (_nlCalStart && _nlCalEnd) {
        var fmtOpts = {day:'numeric',month:'short',year:'numeric'};
        var s = _nlCalStart.toLocaleDateString('ru-RU', fmtOpts);
        var e = _nlCalEnd.toLocaleDateString('ru-RU', fmtOpts);
        label.textContent = 'Выбран период:';
        dates.textContent = s + ' — ' + e;
    } else if (_nlCalStart) {
        label.textContent = 'Выберите конечную дату';
        dates.textContent = _nlCalStart.toLocaleDateString('ru-RU', {day:'numeric',month:'short'});
    }
}

function nlApplyCustom() {
    if (_nlCalStart && !_nlCalEnd) {
        _nlCalEnd = new Date(_nlCalStart);
    }
    if (_nlCalStart && _nlCalEnd) {
        NL_DATE.preset = 'custom';
        NL_DATE.dateFrom = nlIsoDate(_nlCalStart);
        NL_DATE.dateTo = nlIsoDate(_nlCalEnd);
        nlSaveDate();
        nlCloseCalendar();
        nlHighlightPreset();
        nlUpdateCurrentLabel();
        nlNotifySection();
    }
}

function nlSaveDate() {
    localStorage.setItem('nl_date', JSON.stringify(NL_DATE));
}

function nlHighlightPreset() {
    document.querySelectorAll('.nl-dp-btn').forEach(function(btn) {
        if (btn.dataset.preset === NL_DATE.preset) {
            btn.classList.add('nl-dp-active');
        } else {
            btn.classList.remove('nl-dp-active');
        }
    });
}

function nlShortDate(iso) {
    if (!iso || iso.length < 10) return '';
    return iso.slice(8, 10) + '.' + iso.slice(5, 7);
}

function nlUpdateCurrentLabel() {
    var el = document.getElementById('nl-dp-current');
    if (!el) return;
    if (!NL_DATE.dateFrom || !NL_DATE.dateTo) {
        nlSetPresetSilent(NL_DATE.preset || 'yesterday');
    }
    var from = nlShortDate(NL_DATE.dateFrom);
    var to = nlShortDate(NL_DATE.dateTo);
    el.textContent = from && to ? (from === to ? from : from + '-' + to) : '';
}

/**
 * Уведомить текущий раздел о смене периода
 */
function nlNotifySection() {
    if (typeof onTopPeriodChange === 'function') onTopPeriodChange();
}

/**
 * Получить текущий период в формате {dateFrom, dateTo}
 * Единая функция для всех разделов
 */
function nlGetDateRange() {
    // Убедиться что даты заполнены для preset
    if (NL_DATE.preset !== 'custom' && !NL_DATE.dateFrom) {
        nlSetPresetSilent(NL_DATE.preset);
    }
    return {
        dateFrom: NL_DATE.dateFrom,
        dateTo: NL_DATE.dateTo
    };
}

/**
 * Получить текущий период в формате API-параметров.
 * Старые участки фронта используют snake_case, поэтому держим конвертацию в одном месте.
 */
function nlGetDateRangeParams() {
    var range = nlGetDateRange();
    return {
        date_from: range.dateFrom,
        date_to: range.dateTo
    };
}

/**
 * Получить дату-срез для разделов, где нельзя суммировать период
 * (например, остатки на складах). Берём конец выбранного периода.
 */
function nlGetSnapshotDate() {
    return nlGetDateRange().dateTo;
}

/**
 * Бесшумно применить preset без уведомления разделов (для внутреннего использования)
 */
function nlSetPresetSilent(preset) {
    var base = nlYesterday();
    var start = new Date(base);
    var end = new Date(base);
    if (preset === 'today') {
        var today = new Date();
        today.setHours(12, 0, 0, 0);
        start = new Date(today);
        end = new Date(today);
    } else if (preset === 'yesterday') {
        // start = end = yesterday (уже)
    } else if (preset === 'last7') {
        start.setDate(base.getDate() - 6);
    } else if (preset === 'month') {
        start = new Date(base.getFullYear(), base.getMonth(), 1, 12);
        end = new Date(base);
    }
    NL_DATE.dateFrom = nlIsoDate(start);
    NL_DATE.dateTo = nlIsoDate(end);
    nlSaveDate();
    nlUpdateCurrentLabel();
}

/**
 * Показать/скрыть датапикер в зависимости от раздела
 * Справочник, Настройки и т.д. — скрывать
 */
function nlDatepickerVisibility(sectionName) {
    var container = document.getElementById('nl-datepicker-container');
    if (!container) return;
    var noDateSections = ['costprice','salesplan','opexpenses','connectors','subscription','settings','help'];
    if (noDateSections.indexOf(sectionName) !== -1) {
        container.style.display = 'none';
    } else {
        container.style.display = '';
    }
}

// Закрытие по клику вне popover
document.addEventListener('click', function(e) {
    var pop = document.getElementById('nl-dp-popover');
    var container = document.getElementById('nl-datepicker-container');
    if (!pop || pop.style.display === 'none' || !container) return;
    var path = typeof e.composedPath === 'function' ? e.composedPath() : [];
    if (container.contains(e.target) || path.indexOf(container) !== -1) return;
    nlCloseCalendar();
});
