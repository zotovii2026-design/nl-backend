/**
 * Strategy Milestones — справочник стратегий и вехи по артикулам.
 * v2: mass select, batch assign, row expansion for milestone history.
 */

let strategyMilestonesTabulator = null;
let _strategyCategories = [
    {key: 'price', label: 'Цена', color: '#0984e3'},
    {key: 'ads', label: 'Реклама', color: '#e17055'},
    {key: 'seo', label: 'SEO', color: '#00b894'},
    {key: 'infographic', label: 'Инфографика', color: '#6c5ce7'},
    {key: 'main_photo', label: 'Главная картинка', color: '#fdcb6e'},
    {key: 'slides', label: 'Доп. слайды', color: '#00cec9'},
    {key: 'content', label: 'Наполнение', color: '#636e72'},
];
let _strategyActiveCategory = 'price';
let _strategyList = [];
let _strategyMilestones = [];
let _strategyOptions = {brands: [], subjects: [], statuses: [], classes: []};
let _expandedRows = new Set();
let _strategySelectedNmIds = new Set();
let _strategyRestoringSelection = false;

function strategyApiHeaders() {
    return {
        'Authorization': 'Bearer ' + TOKEN,
        'Content-Type': 'application/json'
    };
}

function strategyOrgParam() {
    return 'org_id=' + encodeURIComponent(getCurrentOrgId ? getCurrentOrgId() : ORG_ID);
}

function strategyCat(key) {
    return _strategyCategories.find(function(c) { return c.key === key; }) || _strategyCategories[0];
}

function strategyToday() {
    return new Date().toISOString().slice(0, 10);
}

function strategyEsc(value) {
    if (typeof esc === 'function') return esc(value || '');
    return String(value || '').replace(/[&<>"']/g, function(ch) {
        return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[ch];
    });
}

function strategyNmKey(value) {
    if (value === null || value === undefined || value === '') return '';
    return String(value);
}

function initStrategyMilestonesPage() {
    renderStrategyTabs();
    setStrategyFormDateDefaults();
    loadStrategyOptions();
    loadStrategies();
    loadStrategyMilestones();
}

function setStrategyFormDateDefaults() {
    var dateInput = document.getElementById('strategy-ms-date');
    if (dateInput && !dateInput.value) dateInput.value = strategyToday();
}

/* ==================== UPPER: STRATEGY LIBRARY ==================== */

function renderStrategyTabs() {
    var host = document.getElementById('strategy-category-tabs');
    if (!host) return;
    host.innerHTML = _strategyCategories.map(function(cat) {
        var active = cat.key === _strategyActiveCategory;
        return '<button type="button" onclick="selectStrategyCategory(\'' + cat.key + '\')" ' +
            'style="border:1px solid ' + (active ? cat.color : '#ddd') + ';background:' + (active ? cat.color : '#fff') + ';color:' + (active ? '#fff' : '#333') + ';border-radius:6px;padding:7px 11px;font-size:.86em;cursor:pointer">' +
            strategyEsc(cat.label) + '</button>';
    }).join('');
    var title = document.getElementById('strategy-active-title');
    if (title) title.textContent = strategyCat(_strategyActiveCategory).label;
    var catInput = document.getElementById('strategy-ms-category');
    if (catInput) catInput.value = _strategyActiveCategory;
}

async function selectStrategyCategory(category) {
    _strategyActiveCategory = category;
    renderStrategyTabs();
    clearStrategyForm(false);
    await loadStrategies();
    populateMilestoneStrategySelect();
}

async function loadStrategyOptions() {
    try {
        var resp = await fetch('/api/v1/nl/strategy-milestones/options?' + strategyOrgParam(), {
            headers: {'Authorization': 'Bearer ' + TOKEN}
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        _strategyOptions = await resp.json();
        fillStrategyFilters();
    } catch(e) {
        console.warn('[strategies] options error', e);
    }
}

function fillSelectOptions(id, values, firstLabel) {
    var el = document.getElementById(id);
    if (!el) return;
    var current = el.value;
    el.innerHTML = '<option value="">' + strategyEsc(firstLabel) + '</option>' + (values || []).map(function(v) {
        return '<option value="' + strategyEsc(v) + '">' + strategyEsc(v) + '</option>';
    }).join('');
    el.value = current;
}

function fillStrategyFilters() {
    fillSelectOptions('strategy-flt-brand', _strategyOptions.brands, 'Бренд: все');
    fillSelectOptions('strategy-flt-subject', _strategyOptions.subjects, 'Категория: все');
    fillSelectOptions('strategy-flt-status', _strategyOptions.statuses, 'Статус: все');
    fillSelectOptions('strategy-flt-class', _strategyOptions.classes, 'Класс: все');
}

async function loadStrategies() {
    var list = document.getElementById('strategy-list');
    if (list) list.innerHTML = '<div class="empty" style="padding:12px">Загрузка...</div>';
    try {
        var resp = await fetch('/api/v1/nl/strategies?' + strategyOrgParam() + '&category=' + encodeURIComponent(_strategyActiveCategory), {
            headers: {'Authorization': 'Bearer ' + TOKEN}
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var data = await resp.json();
        _strategyList = data.strategies || [];
        renderStrategyList();
        populateMilestoneStrategySelect();
    } catch(e) {
        if (list) list.innerHTML = '<div class="empty" style="padding:12px;color:#d63031">Ошибка: ' + strategyEsc(e.message) + '</div>';
    }
}

function renderStrategyList() {
    var list = document.getElementById('strategy-list');
    if (!list) return;
    if (!_strategyList.length) {
        list.innerHTML = '<div class="empty" style="padding:12px">Стратегий в этом направлении пока нет</div>';
        showStrategyDetails(null);
        return;
    }
    list.innerHTML = _strategyList.map(function(s) {
        return '<div onclick="showStrategyDetailsById(\'' + s.id + '\')" ' +
            'style="display:grid;grid-template-columns:70px 1fr 145px 110px;gap:8px;align-items:start;padding:8px 10px;border-bottom:1px solid #edf0f3;cursor:pointer">' +
            '<b style="color:' + strategyCat(s.category).color + '">' + strategyEsc(s.code) + '</b>' +
            '<div><div style="font-weight:600;color:#333">' + strategyEsc(s.title) + '</div><div style="font-size:.78em;color:#888;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + strategyEsc(s.description) + '</div></div>' +
            '<div style="font-size:.82em;color:#555">' + strategyEsc(s.default_executor || '-') + '</div>' +
            '<div style="font-size:.82em;color:#777">' + strategyEsc(s.role || '-') + '</div>' +
            '</div>';
    }).join('');
    showStrategyDetails(_strategyList[0]);
}

function showStrategyDetailsById(id) {
    showStrategyDetails(_strategyList.find(function(s) { return s.id === id; }) || null);
}

function showStrategyDetails(strategy) {
    var box = document.getElementById('strategy-detail');
    if (!box) return;
    if (!strategy) {
        box.innerHTML = '<div style="font-size:.85em;color:#888">Выберите стратегию из списка</div>';
        return;
    }
    box.innerHTML =
        '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">' +
        '<span style="font-weight:800;color:' + strategyCat(strategy.category).color + '">' + strategyEsc(strategy.code) + '</span>' +
        '<span style="font-weight:700;color:#333">' + strategyEsc(strategy.title) + '</span>' +
        '</div>' +
        '<div style="font-size:.85em;color:#666;line-height:1.45;white-space:pre-wrap;margin-bottom:10px">' + strategyEsc(strategy.description || 'Описание не заполнено') + '</div>' +
        '<div style="display:grid;grid-template-columns:90px 1fr;gap:5px;font-size:.82em;color:#555">' +
        '<span style="color:#888">Исполнитель</span><span>' + strategyEsc(strategy.default_executor || '-') + '</span>' +
        '<span style="color:#888">Роль</span><span>' + strategyEsc(strategy.role || '-') + '</span>' +
        '<span style="color:#888">Статус</span><span>' + strategyEsc(strategy.status || 'active') + '</span>' +
        '</div>' +
        '<div style="margin-top:10px;display:flex;gap:8px">' +
        '<button onclick="editStrategy(\'' + strategy.id + '\')" style="border:1px solid #ddd;background:#fff;border-radius:6px;padding:5px 9px;cursor:pointer;font-size:.82em">Редактировать</button>' +
        '<button onclick="deleteStrategy(\'' + strategy.id + '\')" style="border:1px solid #ffd6d6;background:#fff;color:#d63031;border-radius:6px;padding:5px 9px;cursor:pointer;font-size:.82em">Удалить</button>' +
        '</div>';
}

function clearStrategyForm(resetCategory) {
    ['strategy-id', 'strategy-code', 'strategy-title', 'strategy-executor', 'strategy-role', 'strategy-description'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = '';
    });
    if (resetCategory) _strategyActiveCategory = 'price';
    renderStrategyTabs();
}

function editStrategy(id) {
    var s = _strategyList.find(function(item) { return item.id === id; });
    if (!s) return;
    document.getElementById('strategy-id').value = s.id;
    document.getElementById('strategy-code').value = s.code || '';
    document.getElementById('strategy-title').value = s.title || '';
    document.getElementById('strategy-executor').value = s.default_executor || '';
    document.getElementById('strategy-role').value = s.role || '';
    document.getElementById('strategy-description').value = s.description || '';
}

async function saveStrategyDefinition() {
    var payload = {
        id: document.getElementById('strategy-id').value || null,
        category: _strategyActiveCategory,
        code: document.getElementById('strategy-code').value.trim(),
        title: document.getElementById('strategy-title').value.trim(),
        default_executor: document.getElementById('strategy-executor').value.trim(),
        role: document.getElementById('strategy-role').value.trim(),
        description: document.getElementById('strategy-description').value.trim(),
        status: 'active'
    };
    if (!payload.code || !payload.title) {
        alert('Заполните номер и название стратегии');
        return;
    }
    var resp = await fetch('/api/v1/nl/strategies?' + strategyOrgParam(), {
        method: 'POST',
        headers: strategyApiHeaders(),
        body: JSON.stringify(payload)
    });
    if (!resp.ok) throw new Error(await resp.text());
    clearStrategyForm(false);
    await loadStrategies();
}

async function deleteStrategy(id) {
    if (!confirm('Удалить стратегию? Вехи останутся в истории с сохранённым номером.')) return;
    var resp = await fetch('/api/v1/nl/strategies/' + encodeURIComponent(id) + '?' + strategyOrgParam(), {
        method: 'DELETE',
        headers: {'Authorization': 'Bearer ' + TOKEN}
    });
    if (!resp.ok) throw new Error(await resp.text());
    await loadStrategies();
}

function populateMilestoneStrategySelect() {
    var el = document.getElementById('strategy-ms-strategy');
    if (!el) return;
    var current = el.value;
    el.innerHTML = '<option value="">Стратегия: без привязки</option>' + _strategyList.map(function(s) {
        return '<option value="' + strategyEsc(s.id) + '">' + strategyEsc(s.code + ' — ' + s.title) + '</option>';
    }).join('');
    el.value = current;
}

function onMilestoneStrategyChange() {
    var id = document.getElementById('strategy-ms-strategy').value;
    var s = _strategyList.find(function(item) { return item.id === id; });
    if (!s) return;
    var executor = document.getElementById('strategy-ms-executor');
    var role = document.getElementById('strategy-ms-role');
    if (executor && !executor.value) executor.value = s.default_executor || '';
    if (role && !role.value) role.value = s.role || '';
}

/* ==================== LOWER: MILESTONES TABLE ==================== */

function strategyRangeParams() {
    if (typeof nlGetDateRangeParams === 'function') return nlGetDateRangeParams();
    return {date_from: '', date_to: ''};
}

async function loadStrategyMilestones() {
    var params = new URLSearchParams();
    params.set('org_id', getCurrentOrgId ? getCurrentOrgId() : ORG_ID);
    var range = strategyRangeParams();
    if (range.date_from) params.set('date_from', range.date_from);
    if (range.date_to) params.set('date_to', range.date_to);
    var map = {
        'strategy-flt-category': 'category',
        'strategy-flt-brand': 'brand',
        'strategy-flt-subject': 'subject',
        'strategy-flt-status': 'product_status',
        'strategy-flt-class': 'product_class',
        'strategy-flt-executor': 'executor',
        'strategy-flt-role': 'role',
        'strategy-flt-search': 'search'
    };
    Object.keys(map).forEach(function(id) {
        var el = document.getElementById(id);
        if (el && el.value) params.set(map[id], el.value);
    });
    try {
        var resp = await fetch('/api/v1/nl/strategy-milestones?' + params.toString(), {
            headers: {'Authorization': 'Bearer ' + TOKEN}
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var data = await resp.json();
        _strategyMilestones = data.milestones || [];
        renderStrategyMilestonesGrid();
        updateStrategyMilestoneCount();
        fillExecutorRoleFilters();
    } catch(e) {
        console.error('[strategies] milestones error', e);
    }
}

function fillExecutorRoleFilters() {
    var executors = Array.from(new Set(_strategyMilestones.map(function(r) { return r.executor; }).filter(Boolean))).sort();
    var roles = Array.from(new Set(_strategyMilestones.map(function(r) { return r.role; }).filter(Boolean))).sort();
    fillSelectOptions('strategy-flt-executor', executors, 'Исполнитель: все');
    fillSelectOptions('strategy-flt-role', roles, 'Роль: все');
}

function categoryFormatter(cell) {
    var cat = strategyCat(cell.getValue());
    return '<span style="display:inline-flex;align-items:center;gap:5px"><span style="width:8px;height:8px;border-radius:50%;background:' + cat.color + ';display:inline-block"></span>' + strategyEsc(cat.label) + '</span>';
}

function strategyChipFormatter(cell) {
    var d = cell.getRow().getData();
    if (!d.category && !d.code) return '<span style="color:#aaa">—</span>';
    var cat = strategyCat(d.category || 'price');
    var code = d.code || d.strategy_code || '';
    var title = d.strategy_title || '';
    return '<span style="display:inline-block;background:' + cat.color + ';color:#fff;border-radius:6px;padding:3px 7px;font-weight:700;font-size:.86em">' + strategyEsc(code) + '</span>' +
        (title ? '<span style="margin-left:6px;color:#333">' + strategyEsc(title) + '</span>' : '');
}

function linksFormatter(cell) {
    var links = cell.getValue() || [];
    if (!Array.isArray(links)) links = [];
    if (!links.length) return '<span style="color:#aaa">—</span>';
    return links.slice(0, 3).map(function(url, idx) {
        return '<a href="' + strategyEsc(url) + '" target="_blank" rel="noopener" style="color:#6c5ce7">ссылка ' + (idx + 1) + '</a>';
    }).join('<br>');
}

/* Row expansion: + button toggles milestone history */
function expandToggleFormatter(cell) {
    var data = cell.getRow().getData();
    var nmId = data.nm_id;
    var expanded = _expandedRows.has(nmId);
    return '<button onclick="toggleMilestoneHistory(' + nmId + ')" style="border:1px solid #ddd;background:#fff;border-radius:4px;padding:2px 6px;cursor:pointer;font-size:.82em">' +
        (expanded ? '▼' : '▶') + '</button>';
}

function renderStrategyMilestonesGrid() {
    var el = document.getElementById('strategy-milestones-tabulator');
    if (!el || typeof Tabulator === 'undefined') return;

    var columns = [
        {title: '', field: '_expand', width: 40, frozen: true, hozAlign: 'center',
         formatter: expandToggleFormatter, headerSort: false, resizable: false},
        {title: 'Дата', field: 'event_date', width: 100, frozen: true,
         formatter: function(cell) { return cell.getValue() ? strategyEsc(cell.getValue()) : '<span style="color:#ccc">—</span>'; }},
        {title: 'Арт WB', field: 'nm_id', width: 105, frozen: true, formatter: function(cell) {
            var v = cell.getValue();
            return '<a href="https://www.wildberries.ru/catalog/' + v + '/detail.aspx" target="_blank" rel="noopener" style="font-weight:700;color:#0984e3">' + v + '</a>';
        }},
        {title: 'Фото', field: 'photo_main', width: 58, formatter: NLGrid.formatters.photo, hozAlign: 'center'},
        {title: 'Товар', field: 'product_name', width: 230},
        {title: 'Бренд', field: 'brand', width: 130},
        {title: 'Категория', field: 'subject_name', width: 150},
        {title: 'Направление', field: 'category', width: 145, formatter: categoryFormatter},
        {title: 'Стратегия', field: 'code', width: 230, formatter: strategyChipFormatter},
        {title: 'Исполнитель', field: 'executor', width: 135},
        {title: 'Роль', field: 'role', width: 115},
        {title: 'Источники', field: 'source_links', width: 130, formatter: linksFormatter},
        {title: 'Комментарий', field: 'comment', width: 260},
        {title: 'Результат', field: 'result_note', width: 220},
        {title: 'Статус', field: 'product_status', width: 110},
        {title: 'Класс', field: 'product_class', width: 80},
    ];

    if (strategyMilestonesTabulator) {
        try { strategyMilestonesTabulator.destroy(); } catch(e) {}
        strategyMilestonesTabulator = null;
    }

    strategyMilestonesTabulator = NLGrid.create(el, {
        data: _strategyMilestones,
        columns: columns,
        height: '64vh',
        layout: 'fitDataFill',
        selectable: true,
        initialSort: [{column: 'event_date', dir: 'desc'}],
        placeholder: 'Нет товаров',
        rowFormatter: rowExpansionFormatter,
        rowClick: function(e, row) {
            var target = e && e.target;
            if (target && target.closest && target.closest('button,a,input,select,textarea')) return;
            openStrategyMilestoneForRow(row.getData());
        },
        rowSelectionChanged: function(data) {
            if (_strategyRestoringSelection) return;
            var visibleIds = new Set(_strategyMilestones.map(function(r) { return strategyNmKey(r.nm_id); }).filter(Boolean));
            var selectedIds = new Set((data || []).map(function(r) { return strategyNmKey(r.nm_id); }).filter(Boolean));
            visibleIds.forEach(function(id) {
                if (!selectedIds.has(id)) _strategySelectedNmIds.delete(id);
            });
            selectedIds.forEach(function(id) { _strategySelectedNmIds.add(id); });
            updateBulkBar();
        },
        renderComplete: function() {
            restoreStrategySelection();
        },
    });
    restoreStrategySelection();
}

/* Row expansion: shows milestone history below the row */
function rowExpansionFormatter(row) {
    var data = row.getData();
    var nmId = data.nm_id;
    var expandEl = row.getElement().querySelector('.tabulator-row-expand');
    if (_expandedRows.has(nmId)) {
        // Create expansion container
        var existing = row.getElement().querySelector('.milestone-history-container');
        if (existing) return;
        var container = document.createElement('div');
        container.className = 'milestone-history-container';
        container.style.cssText = 'padding:8px 12px;background:#f8f9fa;border-top:1px solid #edf0f3;border-bottom:1px solid #edf0f3;';
        container.innerHTML = '<div style="color:#888;font-size:.82em">Загрузка истории...</div>';
        row.getElement().appendChild(container);
        loadMilestoneHistory(nmId, container, row);
    } else {
        var hist = row.getElement().querySelector('.milestone-history-container');
        if (hist) hist.remove();
    }
}

async function toggleMilestoneHistory(nmId) {
    if (_expandedRows.has(nmId)) {
        _expandedRows.delete(nmId);
    } else {
        _expandedRows.add(nmId);
    }
    // Re-render to show/hide expansion
    if (strategyMilestonesTabulator) {
        strategyMilestonesTabulator.redraw(true);
    }
}

async function loadMilestoneHistory(nmId, container, row) {
    try {
        var resp = await fetch(
            '/api/v1/nl/strategy-milestones/by-art/' + nmId + '?' + strategyOrgParam(),
            {headers: {'Authorization': 'Bearer ' + TOKEN}}
        );
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var data = await resp.json();
        var milestones = data.milestones || [];
        if (!milestones.length) {
            container.innerHTML = '<div style="color:#888;font-size:.82em;padding:4px 0">Вех по этому товару ещё нет. Используйте форму ниже или массовое назначение.</div>';
            return;
        }
        var html = '<div style="font-weight:600;font-size:.86em;margin-bottom:6px;color:#555">История вех (' + milestones.length + ')</div>';
        html += '<table style="width:100%;font-size:.8em;border-collapse:collapse">';
        html += '<thead><tr style="color:#888;text-align:left">' +
            '<th style="padding:3px 8px">Дата</th>' +
            '<th style="padding:3px 8px">Направление</th>' +
            '<th style="padding:3px 8px">Стратегия</th>' +
            '<th style="padding:3px 8px">Исполнитель</th>' +
            '<th style="padding:3px 8px">Комментарий</th>' +
            '<th style="padding:3px 8px">Результат</th>' +
            '<th style="padding:3px 8px"></th>' +
            '</tr></thead><tbody>';
        milestones.forEach(function(m) {
            var cat = strategyCat(m.category);
            html += '<tr style="border-top:1px solid #eee">' +
                '<td style="padding:4px 8px;white-space:nowrap">' + strategyEsc(m.event_date || '—') + '</td>' +
                '<td style="padding:4px 8px"><span style="display:inline-flex;align-items:center;gap:4px"><span style="width:7px;height:7px;border-radius:50%;background:' + cat.color + '"></span>' + strategyEsc(cat.label) + '</span></td>' +
                '<td style="padding:4px 8px"><b>' + strategyEsc(m.code || m.strategy_code || '—') + '</b>' + (m.strategy_title ? ' ' + strategyEsc(m.strategy_title) : '') + '</td>' +
                '<td style="padding:4px 8px">' + strategyEsc(m.executor || '—') + '</td>' +
                '<td style="padding:4px 8px">' + strategyEsc(m.comment || '—') + '</td>' +
                '<td style="padding:4px 8px">' + strategyEsc(m.result_note || '—') + '</td>' +
                '<td style="padding:4px 8px"><button onclick="deleteMilestoneById(\'' + m.id + '\',' + nmId + ')" style="border:1px solid #ffd6d6;background:#fff;color:#d63031;border-radius:4px;padding:2px 6px;cursor:pointer;font-size:.82em">✕</button></td>' +
                '</tr>';
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    } catch(e) {
        container.innerHTML = '<div style="color:#d63031;font-size:.82em">Ошибка: ' + strategyEsc(e.message) + '</div>';
    }
}

async function deleteMilestoneById(milestoneId, nmId) {
    if (!confirm('Удалить эту веху?')) return;
    var resp = await fetch('/api/v1/nl/strategy-milestones/' + encodeURIComponent(milestoneId) + '?' + strategyOrgParam(), {
        method: 'DELETE',
        headers: {'Authorization': 'Bearer ' + TOKEN}
    });
    if (!resp.ok) { alert('Ошибка удаления'); return; }
    _expandedRows.delete(nmId);
    await loadStrategyMilestones();
    _expandedRows.add(nmId);
    strategyMilestonesTabulator.redraw(true);
}

function updateStrategyMilestoneCount() {
    var el = document.getElementById('strategy-ms-count');
    if (el) el.textContent = _strategyMilestones.length + ' товаров';
}

function openStrategyMilestoneForRow(row) {
    if (!row) return;
    document.getElementById('strategy-ms-id').value = '';
    document.getElementById('strategy-ms-nm').value = row.nm_id || '';
    document.getElementById('strategy-ms-date').value = strategyToday();
    var category = document.getElementById('strategy-ms-category');
    if (category) category.value = row.category || _strategyActiveCategory;
    if (row.category && row.category !== _strategyActiveCategory) {
        _strategyActiveCategory = row.category;
        selectStrategyCategory(row.category).then(function() {
            var strategy = document.getElementById('strategy-ms-strategy');
            if (strategy) strategy.value = '';
        });
    } else {
        var strategy = document.getElementById('strategy-ms-strategy');
        if (strategy) strategy.value = '';
    }
    document.getElementById('strategy-ms-executor').value = row.executor || '';
    document.getElementById('strategy-ms-role').value = row.role || '';
    document.getElementById('strategy-ms-links').value = '';
    document.getElementById('strategy-ms-comment').value = '';
    document.getElementById('strategy-ms-result').value = '';

    var detail = document.getElementById('strategy-ms-detail');
    if (detail) {
        detail.innerHTML = '<b>Новая веха для артикула ' + strategyEsc(row.nm_id) + '</b>' +
            '<span style="color:#888"> — ' + strategyEsc(row.product_name || 'товар без названия') + '</span>' +
            '<div style="margin-top:4px;color:#777;font-size:.92em">Заполните направление, стратегию, комментарий и сохраните. История товара открывается кнопкой ▶ в строке.</div>';
    }
}

function resetStrategyMilestoneFilters() {
    ['strategy-flt-category', 'strategy-flt-brand', 'strategy-flt-subject', 'strategy-flt-status', 'strategy-flt-class', 'strategy-flt-executor', 'strategy-flt-role', 'strategy-flt-search'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = '';
    });
    loadStrategyMilestones();
}

/* ==================== MASS SELECT & BATCH ASSIGN ==================== */

function getSelectedNmIds() {
    return Array.from(_strategySelectedNmIds).map(function(id) { return parseInt(id, 10); }).filter(Boolean);
}

function updateBulkBar() {
    var bar = document.getElementById('strategy-bulk-bar');
    var countEl = document.getElementById('strategy-selected-count');
    if (!bar || !countEl) return;
    var count = getSelectedNmIds().length;
    countEl.textContent = count;
    bar.style.display = 'flex';
    bar.style.background = count > 0 ? '#f0f4ff' : '#f7f8fa';
}

function selectAllStrategyRows() {
    if (!strategyMilestonesTabulator) return;
    _strategyMilestones.forEach(function(r) {
        var id = strategyNmKey(r.nm_id);
        if (id) _strategySelectedNmIds.add(id);
    });
    strategyMilestonesTabulator.selectRow();
    updateBulkBar();
}

function clearSelectedStrategyRows() {
    _strategySelectedNmIds.clear();
    if (strategyMilestonesTabulator) strategyMilestonesTabulator.deselectRow();
    updateBulkBar();
}

function restoreStrategySelection() {
    if (!strategyMilestonesTabulator) {
        updateBulkBar();
        return;
    }
    _strategyRestoringSelection = true;
    try {
        strategyMilestonesTabulator.getRows().forEach(function(row) {
            var id = strategyNmKey(row.getData().nm_id);
            if (!id) return;
            if (_strategySelectedNmIds.has(id)) {
                row.select();
            } else if (row.isSelected && row.isSelected()) {
                row.deselect();
            }
        });
    } finally {
        _strategyRestoringSelection = false;
        updateBulkBar();
    }
}

function showBatchAssignModal() {
    var nmIds = getSelectedNmIds();
    if (!nmIds.length) { alert('Сначала выделите товары'); return; }

    // Build modal
    var overlay = document.getElementById('strategy-batch-modal') || document.createElement('div');
    overlay.id = 'strategy-batch-modal';
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.4);z-index:9999;display:flex;align-items:center;justify-content:center';

    // Strategy options HTML
    var strategyOpts = '<option value="">Без привязки</option>' + _strategyList.map(function(s) {
        return '<option value="' + strategyEsc(s.id) + '">' + strategyEsc(s.code + ' — ' + strategyEsc(s.title)) + '</option>';
    }).join('');

    // Category options
    var catOpts = _strategyCategories.map(function(c) {
        return '<option value="' + c.key + '">' + strategyEsc(c.label) + '</option>';
    }).join('');

    overlay.innerHTML =
        '<div style="background:#fff;border-radius:12px;padding:24px;width:480px;max-width:90vw;box-shadow:0 8px 32px rgba(0,0,0,.15)">' +
        '<div style="font-weight:700;font-size:1.05em;margin-bottom:4px">Назначить веху</div>' +
        '<div style="font-size:.85em;color:#888;margin-bottom:16px">Выбрано товаров: <b>' + nmIds.length + '</b></div>' +
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">' +
            '<div><label style="font-size:.82em;color:#555">Дата</label><input type="date" id="batch-ms-date" value="' + strategyToday() + '" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em"></div>' +
            '<div><label style="font-size:.82em;color:#555">Направление</label><select id="batch-ms-category" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em">' + catOpts + '</select></div>' +
        '</div>' +
        '<div style="margin-bottom:12px"><label style="font-size:.82em;color:#555">Стратегия</label><select id="batch-ms-strategy" onchange="onBatchStrategyChange()" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em">' + strategyOpts + '</select></div>' +
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">' +
            '<div><label style="font-size:.82em;color:#555">Исполнитель</label><input type="text" id="batch-ms-executor" placeholder="не указан" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em"></div>' +
            '<div><label style="font-size:.82em;color:#555">Роль</label><input type="text" id="batch-ms-role" placeholder="не указана" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em"></div>' +
        '</div>' +
        '<div style="margin-bottom:16px"><label style="font-size:.82em;color:#555">Комментарий</label><input type="text" id="batch-ms-comment" placeholder="комментарий к вехе" style="width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:.9em"></div>' +
        '<div style="display:flex;gap:8px;justify-content:flex-end">' +
            '<button onclick="closeBatchModal()" style="border:1px solid #ddd;background:#fff;border-radius:8px;padding:8px 16px;cursor:pointer;font-size:.9em">Отмена</button>' +
            '<button onclick="submitBatchAssign(' + nmIds.length + ')" style="border:none;background:#0984e3;color:#fff;border-radius:8px;padding:8px 16px;cursor:pointer;font-size:.9em;font-weight:600">Назначить</button>' +
        '</div>' +
        '</div>';

    if (!overlay.parentElement) document.body.appendChild(overlay);

    // Set initial category to active
    setTimeout(function() {
        var catSel = document.getElementById('batch-ms-category');
        if (catSel) catSel.value = _strategyActiveCategory;
    }, 0);
}

function onBatchStrategyChange() {
    var id = document.getElementById('batch-ms-strategy').value;
    var s = _strategyList.find(function(item) { return item.id === id; });
    if (!s) return;
    var executor = document.getElementById('batch-ms-executor');
    var role = document.getElementById('batch-ms-role');
    var cat = document.getElementById('batch-ms-category');
    if (executor && !executor.value) executor.value = s.default_executor || '';
    if (role && !role.value) role.value = s.role || '';
    if (cat) cat.value = s.category || _strategyActiveCategory;
}

function closeBatchModal() {
    var overlay = document.getElementById('strategy-batch-modal');
    if (overlay) overlay.remove();
}

async function submitBatchAssign(expectedCount) {
    var nmIds = getSelectedNmIds();
    if (!nmIds.length) return;

    var payload = {
        nm_ids: nmIds,
        event_date: document.getElementById('batch-ms-date').value || strategyToday(),
        category: document.getElementById('batch-ms-category').value || _strategyActiveCategory,
        strategy_id: document.getElementById('batch-ms-strategy').value || null,
        executor: document.getElementById('batch-ms-executor').value.trim(),
        role: document.getElementById('batch-ms-role').value.trim(),
        comment: document.getElementById('batch-ms-comment').value.trim()
    };

    closeBatchModal();

    try {
        var resp = await fetch('/api/v1/nl/strategy-milestones/batch?' + strategyOrgParam(), {
            method: 'POST',
            headers: strategyApiHeaders(),
            body: JSON.stringify(payload)
        });
        if (!resp.ok) throw new Error(await resp.text());
        var result = await resp.json();
        clearSelectedStrategyRows();
        await loadStrategyMilestones();
    } catch(e) {
        alert('Ошибка: ' + e.message);
    }
}

/* ==================== SINGLE MILESTONE FORM (existing) ==================== */

async function saveStrategyMilestone() {
    var links = (document.getElementById('strategy-ms-links').value || '').split(/\n+/).map(function(v) { return v.trim(); }).filter(Boolean);
    var selectedStrategy = document.getElementById('strategy-ms-strategy').value;
    var payload = {
        id: document.getElementById('strategy-ms-id').value || null,
        nm_id: document.getElementById('strategy-ms-nm').value,
        event_date: document.getElementById('strategy-ms-date').value,
        category: document.getElementById('strategy-ms-category').value || _strategyActiveCategory,
        strategy_id: selectedStrategy || null,
        executor: document.getElementById('strategy-ms-executor').value.trim(),
        role: document.getElementById('strategy-ms-role').value.trim(),
        source_links: links,
        comment: document.getElementById('strategy-ms-comment').value.trim(),
        result_note: document.getElementById('strategy-ms-result').value.trim()
    };
    if (!payload.nm_id) {
        alert('Укажите артикул WB');
        return;
    }
    var resp = await fetch('/api/v1/nl/strategy-milestones?' + strategyOrgParam(), {
        method: 'POST',
        headers: strategyApiHeaders(),
        body: JSON.stringify(payload)
    });
    if (!resp.ok) throw new Error(await resp.text());
    clearStrategyMilestoneForm();
    await loadStrategyMilestones();
}

function clearStrategyMilestoneForm() {
    ['strategy-ms-id', 'strategy-ms-nm', 'strategy-ms-executor', 'strategy-ms-role', 'strategy-ms-links', 'strategy-ms-comment', 'strategy-ms-result'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = '';
    });
    var strategy = document.getElementById('strategy-ms-strategy');
    if (strategy) strategy.value = '';
    var category = document.getElementById('strategy-ms-category');
    if (category) category.value = _strategyActiveCategory;
    setStrategyFormDateDefaults();
}

function editStrategyMilestone(id) {
    var row = _strategyMilestones.find(function(item) { return item.id === id; });
    if (!row) return;
    document.getElementById('strategy-ms-id').value = row.id || '';
    document.getElementById('strategy-ms-nm').value = row.nm_id || '';
    document.getElementById('strategy-ms-date').value = row.event_date || strategyToday();
    document.getElementById('strategy-ms-category').value = row.category || _strategyActiveCategory;
    if (row.category && row.category !== _strategyActiveCategory) {
        _strategyActiveCategory = row.category;
        selectStrategyCategory(row.category).then(function() {
            document.getElementById('strategy-ms-strategy').value = row.strategy_id || '';
        });
    } else {
        document.getElementById('strategy-ms-strategy').value = row.strategy_id || '';
    }
    document.getElementById('strategy-ms-executor').value = row.executor || '';
    document.getElementById('strategy-ms-role').value = row.role || '';
    document.getElementById('strategy-ms-links').value = Array.isArray(row.source_links) ? row.source_links.join('\n') : '';
    document.getElementById('strategy-ms-comment').value = row.comment || '';
    document.getElementById('strategy-ms-result').value = row.result_note || '';
}

function onStrategyTopStoreChange() {
    loadStrategyOptions();
    loadStrategies();
    loadStrategyMilestones();
}
