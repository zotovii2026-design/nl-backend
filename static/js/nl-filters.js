/**
 * nl-filters.js — общий модуль мультивыбора для фильтров NL Table.
 *
 * UI-механика: кнопка → выпадающая панель с чекбоксами.
 * Не содержит бизнес-логики конкретных разделов.
 *
 * Использование:
 *   <div class="nl-filter-menu" data-filter-id="my-filter">
 *     <select id="my-filter" class="nl-filter-select" multiple></select>
 *     <button type="button" class="nl-filter-button" onclick="NLFilters.toggleMenu('my-filter')">Метка: все</button>
 *     <div id="my-filter-menu" class="nl-filter-panel"></div>
 *   </div>
 *
 *   NLFilters.renderMenu('my-filter', 'Метка');
 *   NLFilters.getValues(el);
 *   NLFilters.setValues(el, values);
 *
 * CSS-классы (определены в nl_v2.html):
 *   .nl-filter-native, .nl-filter-menu, .nl-filter-select,
 *   .nl-filter-button, .nl-filter-panel, .nl-filter-option
 */

var NLFilters = (function() {

    /* ── значения select ── */

    function getValues(el) {
        if (!el) return [];
        return Array.from(el.selectedOptions || []).map(function(opt) {
            return opt.value;
        }).filter(Boolean);
    }

    function getRawValues(el) {
        if (!el) return [];
        return Array.from(el.selectedOptions || []).map(function(opt) {
            return opt.value;
        });
    }

    function setValues(el, values) {
        if (!el) return;
        var selected = new Set(values || []);
        var hasSelected = false;
        Array.from(el.options || []).forEach(function(opt) {
            opt.selected = selected.has(opt.value);
            if (opt.selected) hasSelected = true;
        });
        if (!hasSelected) selectAll(el);
        renderMenu(el.id);
    }

    function selectAll(el) {
        if (!el) return;
        var allOpt = Array.from(el.options || []).find(function(opt) { return opt.value === ''; });
        if (allOpt) {
            Array.from(el.options || []).forEach(function(opt) { opt.selected = false; });
            allOpt.selected = true;
        } else {
            el.selectedIndex = -1;
        }
        renderMenu(el.id);
    }

    /* ── рендер выпадающего меню ── */

    function renderMenu(id, label) {
        var select = document.getElementById(id);
        var panel = document.getElementById(id + '-menu');
        var wrap = document.querySelector('.nl-filter-menu[data-filter-id="' + id + '"]');
        var btn = wrap ? wrap.querySelector('.nl-filter-button') : null;
        if (!select || !panel || !btn) return;

        // Если label не передён — пробуем data-label на wrap
        if (!label) label = wrap ? (wrap.dataset.label || 'Фильтр') : 'Фильтр';

        var selected = getValues(select);
        var opts = Array.from(select.options || []);
        btn.textContent = selected.length ? label + ': ' + selected.length : label + ': все';
        panel.innerHTML = opts.map(function(opt) {
            var checked = opt.value === '' ? !selected.length : selected.indexOf(opt.value) !== -1;
            return '<label class="nl-filter-option" title="' + esc(opt.textContent || opt.value) + '">' +
                '<input type="checkbox" data-filter-option="' + esc(opt.value) + '"' + (checked ? ' checked' : '') + '>' +
                '<span>' + esc(opt.textContent || opt.value) + '</span>' +
                '</label>';
        }).join('');
    }

    function toggleMenu(id, label) {
        var wrap = document.querySelector('.nl-filter-menu[data-filter-id="' + id + '"]');
        if (!wrap) return;
        // Закрыть другие открытые меню
        document.querySelectorAll('.nl-filter-menu.open').forEach(function(item) {
            if (item !== wrap) item.classList.remove('open');
        });
        renderMenu(id, label);
        wrap.classList.toggle('open');
    }

    function setOption(id, value, checked, onChange) {
        var select = document.getElementById(id);
        if (!select) return;
        if (value === '') {
            selectAll(select);
        } else {
            var values = new Set(getValues(select));
            if (checked) values.add(value); else values.delete(value);
            setValues(select, Array.from(values));
        }
        renderMenu(id);
        if (typeof onChange === 'function') onChange();
    }

    /* ── множественный выбор с Ctrl/Shift ── */

    function rememberSelection(el, event) {
        if (!el) return;
        el.dataset.nlPrevSelected = JSON.stringify(getValues(el));
        el.dataset.nlMultiMode = event && (event.ctrlKey || event.metaKey || event.shiftKey) ? '1' : '0';
    }

    function handleMultiChange(el) {
        if (!el) return;
        var rawValues = getRawValues(el);
        if (!rawValues.length || rawValues.indexOf('') !== -1) {
            selectAll(el);
            return;
        }
        var values = rawValues.filter(Boolean);
        var prev = [];
        try { prev = JSON.parse(el.dataset.nlPrevSelected || '[]'); } catch(e) { prev = []; }
        var added = values.filter(function(value) { return prev.indexOf(value) === -1; });
        if (el.dataset.nlMultiMode !== '1' && values.length > 1) {
            setValues(el, added.length ? [added[added.length - 1]] : [values[values.length - 1]]);
        }
    }

    /* ── сброс ── */

    function clearValue(id) {
        var el = document.getElementById(id);
        if (!el) return;
        if (el.multiple) {
            selectAll(el);
        } else {
            el.value = '';
        }
    }

    /* ── глобальный click-handler для меню ── */
    // Вызывать один раз из основного шаблона:
    //   NLFilters.initGlobalClick(function(filterId) { /* onChange callback */ });
    //
    // Если callback не передан — закрывает меню при клике вне.

    var _onChangeCallback = null;

    function initGlobalClick(onChangeCallback) {
        _onChangeCallback = onChangeCallback || null;
        document.addEventListener('click', function(event) {
            // Клик по опции внутри меню
            var option = event.target && event.target.closest ? event.target.closest('[data-filter-option]') : null;
            if (option) {
                var wrap = option.closest('.nl-filter-menu');
                if (!wrap) return;
                var filterId = wrap.dataset.filterId;
                setOption(filterId, option.getAttribute('data-filter-option'), option.checked, _onChangeCallback);
                event.stopPropagation();
                return;
            }
            // Клик вне меню — закрыть все
            var menu = event.target && event.target.closest ? event.target.closest('.nl-filter-menu') : null;
            if (!menu) {
                document.querySelectorAll('.nl-filter-menu.open').forEach(function(item) {
                    item.classList.remove('open');
                });
            }
        });
    }

    /* ── вспомогательное ── */

    function esc(s) {
        return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    return {
        getValues: getValues,
        setValues: setValues,
        selectAll: selectAll,
        renderMenu: renderMenu,
        toggleMenu: toggleMenu,
        setOption: setOption,
        rememberSelection: rememberSelection,
        handleMultiChange: handleMultiChange,
        clearValue: clearValue,
        initGlobalClick: initGlobalClick,
        esc: esc
    };
})();
