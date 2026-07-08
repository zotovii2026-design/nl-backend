/**
 * Strategy Milestones — справочник стратегий и вехи по артикулам.
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
    var cat = strategyCat(d.category);
    var code = d.code || d.strategy_code || '-';
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

function renderStrategyMilestonesGrid() {
    var el = document.getElementById('strategy-milestones-tabulator');
    if (!el || typeof Tabulator === 'undefined') return;
    var columns = [
        {title: 'Дата', field: 'event_date', width: 100, frozen: true, sorter: 'date'},
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
        {title: '', field: '_actions', width: 92, hozAlign: 'center', formatter: function(cell) {
            var id = cell.getRow().getData().id;
            return '<button onclick="editStrategyMilestone(\'' + id + '\')" style="border:1px solid #ddd;background:#fff;border-radius:5px;padding:3px 7px;cursor:pointer">Изм.</button>';
        }},
    ];
    if (strategyMilestonesTabulator) {
        strategyMilestonesTabulator.replaceData(_strategyMilestones);
        return;
    }
    strategyMilestonesTabulator = NLGrid.create(el, {
        data: _strategyMilestones,
        columns: columns,
        height: '64vh',
        layout: 'fitDataFill',
        initialSort: [{column: 'event_date', dir: 'desc'}],
        placeholder: 'Вехи ещё не добавлены',
    });
    strategyMilestonesTabulator.on('rowClick', function(e, row) {
        showMilestoneDetails(row.getData());
    });
}

function showMilestoneDetails(row) {
    var box = document.getElementById('strategy-ms-detail');
    if (!box || !row) return;
    var links = Array.isArray(row.source_links) ? row.source_links : [];
    box.innerHTML =
        '<div style="font-weight:700;margin-bottom:6px">' + strategyEsc(row.nm_id) + ' · ' + strategyEsc(row.event_date) + '</div>' +
        '<div style="font-size:.86em;color:#555;line-height:1.45">' +
        '<b>' + strategyEsc(strategyCat(row.category).label) + ':</b> ' + strategyEsc((row.code || row.strategy_code || '-') + ' ' + (row.strategy_title || '')) + '<br>' +
        '<b>Исполнитель:</b> ' + strategyEsc(row.executor || '-') + ' · <b>роль:</b> ' + strategyEsc(row.role || '-') + '<br>' +
        '<b>Комментарий:</b> ' + strategyEsc(row.comment || '-') + '<br>' +
        '<b>Результат:</b> ' + strategyEsc(row.result_note || '-') + '<br>' +
        (links.length ? '<b>Источники:</b><br>' + links.map(function(url) { return '<a href="' + strategyEsc(url) + '" target="_blank" rel="noopener">' + strategyEsc(url) + '</a>'; }).join('<br>') : '') +
        '</div>';
}

function updateStrategyMilestoneCount() {
    var el = document.getElementById('strategy-ms-count');
    if (el) el.textContent = _strategyMilestones.length + ' вех';
}

function resetStrategyMilestoneFilters() {
    ['strategy-flt-category', 'strategy-flt-brand', 'strategy-flt-subject', 'strategy-flt-status', 'strategy-flt-class', 'strategy-flt-executor', 'strategy-flt-role', 'strategy-flt-search'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = '';
    });
    loadStrategyMilestones();
}

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
