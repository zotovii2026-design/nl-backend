(function () {
    'use strict';

    const CAPABILITY_LABELS = {
        read: 'Чтение данных',
        prices_write: 'Изменение цен',
        stocks_write: 'Изменение остатков',
        content_write: 'Изменение карточек',
        ads_write: 'Управление рекламой'
    };

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function root() { return document.getElementById('marketplace-connections-root'); }
    function orgId() { return typeof getCurrentOrgId === 'function' ? getCurrentOrgId() : window.ORG_ID; }

    function formHtml() {
        return '<div style="background:#fff;border:1px solid #e6e6ef;border-left:4px solid #6c5ce7;border-radius:10px;padding:18px;margin-bottom:18px">' +
            '<div style="font-weight:700;font-size:1.05em;margin-bottom:12px">Добавить профиль доступа</div>' +
            '<div style="display:grid;grid-template-columns:160px 1fr 1fr;gap:10px;margin-bottom:10px">' +
              '<select id="mpc-marketplace" onchange="marketplaceConnectionsMarketplaceChanged()" style="padding:9px;border:1px solid #ddd;border-radius:6px"><option value="wb">Wildberries</option><option value="ozon">Ozon</option></select>' +
              '<input id="mpc-name" placeholder="Название ключа" style="padding:9px;border:1px solid #ddd;border-radius:6px">' +
              '<input id="mpc-purpose" placeholder="Назначение, например: чтение" style="padding:9px;border:1px solid #ddd;border-radius:6px">' +
            '</div>' +
            '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">' +
              '<input id="mpc-client-id" placeholder="Ozon Client-Id" style="display:none;padding:9px;border:1px solid #ddd;border-radius:6px">' +
              '<input id="mpc-secret" type="password" autocomplete="new-password" placeholder="API-ключ (после сохранения не показывается)" style="padding:9px;border:1px solid #ddd;border-radius:6px">' +
            '</div>' +
            '<div style="font-size:.88em;color:#555;margin-bottom:8px">Разрешения NL для этого профиля:</div>' +
            '<div id="mpc-capabilities" style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:12px">' +
              Object.keys(CAPABILITY_LABELS).map(function (key) {
                  return '<label style="display:flex;align-items:center;gap:5px"><input type="checkbox" value="' + key + '"' + (key === 'read' ? ' checked' : '') + '> ' + CAPABILITY_LABELS[key] + '</label>';
              }).join('') +
            '</div>' +
            '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">' +
              '<button id="mpc-save" class="btn" onclick="saveMarketplaceCredential()" style="background:#6c5ce7;color:#fff">Сохранить профиль</button>' +
              '<span id="mpc-form-status" style="font-size:.88em;color:#666"></span>' +
            '</div>' +
            '<div style="margin-top:10px;font-size:.82em;color:#777">Новый ключ сохраняется выключенным и непроверенным. NL не использует его, пока отдельная проверка не подтвердит права.</div>' +
          '</div>';
    }

    function shellHtml() {
        return '<div style="max-width:1100px;margin:0 auto;padding:20px">' +
          '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:16px">' +
            '<div><h3 style="color:#6c5ce7;margin:0 0 5px">🔐 Подключения WB/Ozon</h3>' +
            '<div style="color:#666;font-size:.9em">Отдельный безопасный раздел. Старые подключения WB продолжают работать без изменений.</div></div>' +
            '<button class="btn btn-outline" onclick="loadMarketplaceCredentials()">Обновить</button>' +
          '</div>' + formHtml() +
          '<div id="mpc-list"><div style="color:#999">Загрузка...</div></div>' +
        '</div>';
    }

    function statusBadge(item) {
        if (item.status === 'valid' && item.is_enabled) return '<span style="color:#00875a;background:#e8f8f0;padding:3px 8px;border-radius:999px">Активен</span>';
        if (item.status === 'valid') return '<span style="color:#42526e;background:#eef2ff;padding:3px 8px;border-radius:999px">Выключен</span>';
        return '<span style="color:#ad6b00;background:#fff6df;padding:3px 8px;border-radius:999px">Не проверен</span>';
    }

    function card(item) {
        const caps = (item.capabilities || []).map(function (cap) {
            return '<span style="background:#f1f2f4;color:#445;padding:3px 7px;border-radius:5px;font-size:.8em">' + esc(CAPABILITY_LABELS[cap] || cap) + '</span>';
        }).join(' ');
        return '<div style="background:#fff;border:1px solid #e6e6ef;border-radius:9px;padding:15px;margin-bottom:10px">' +
          '<div style="display:flex;align-items:center;gap:9px;flex-wrap:wrap">' +
            '<strong>' + (item.marketplace === 'ozon' ? 'Ozon' : 'Wildberries') + ' · ' + esc(item.name) + '</strong>' + statusBadge(item) +
            '<span style="color:#777;font-size:.86em">' + esc(item.purpose) + '</span>' +
            '<span style="margin-left:auto;color:#999;font-size:.8em">Секрет скрыт' + (item.has_client_id ? ' · Client-Id сохранён' : '') + '</span>' +
          '</div><div style="margin-top:9px;display:flex;gap:6px;flex-wrap:wrap">' + caps + '</div>' +
          '<div style="margin-top:11px;display:flex;gap:8px">' +
            '<button class="btn btn-outline" onclick="verifyMarketplaceCredential(\'' + esc(item.id) + '\')">Проверить авторизацию</button>' +
            (item.status === 'valid' ? '<button class="btn btn-outline" onclick="toggleMarketplaceCredential(\'' + esc(item.id) + '\',' + (!item.is_enabled) + ')">' + (item.is_enabled ? 'Выключить' : 'Включить') + '</button>' : '') +
            '<button class="btn btn-outline" onclick="deleteMarketplaceCredential(\'' + esc(item.id) + '\')" style="color:#c9372c">Удалить</button>' +
          '</div></div>';
    }

    window.initMarketplaceConnections = function () {
        const el = root();
        if (!el) return;
        el.innerHTML = shellHtml();
        window.loadMarketplaceCredentials();
    };

    window.marketplaceConnectionsMarketplaceChanged = function () {
        const marketplace = document.getElementById('mpc-marketplace')?.value;
        const clientId = document.getElementById('mpc-client-id');
        if (clientId) clientId.style.display = marketplace === 'ozon' ? '' : 'none';
    };

    window.loadMarketplaceCredentials = async function () {
        const list = document.getElementById('mpc-list');
        const oid = orgId();
        if (!list) return;
        if (!oid) { list.innerHTML = '<div style="color:#999">Выберите кабинет юридического лица.</div>'; return; }
        try {
            const response = await fetch('/api/v1/nl/marketplace-credentials?org_id=' + encodeURIComponent(oid));
            const data = await response.json();
            if (!response.ok) throw new Error(data.detail || 'Не удалось загрузить подключения');
            list.innerHTML = data.length ? data.map(card).join('') : '<div style="background:#fff;border:1px dashed #ccc;border-radius:9px;padding:20px;color:#777">Профили WB/Ozon ещё не добавлены.</div>';
        } catch (error) {
            list.innerHTML = '<div style="color:#c9372c">' + esc(error.message) + '</div>';
        }
    };

    window.saveMarketplaceCredential = async function () {
        const status = document.getElementById('mpc-form-status');
        const button = document.getElementById('mpc-save');
        const capabilities = Array.from(document.querySelectorAll('#mpc-capabilities input:checked')).map(function (el) { return el.value; });
        const payload = {
            marketplace: document.getElementById('mpc-marketplace').value,
            name: document.getElementById('mpc-name').value.trim(),
            purpose: document.getElementById('mpc-purpose').value.trim(),
            secret: document.getElementById('mpc-secret').value.trim(),
            client_id: document.getElementById('mpc-client-id').value.trim() || null,
            capabilities: capabilities
        };
        if (!payload.name || !payload.purpose || !payload.secret || !capabilities.length) {
            status.textContent = 'Заполните название, назначение, ключ и права.'; status.style.color = '#c9372c'; return;
        }
        button.disabled = true; status.textContent = 'Сохраняем…'; status.style.color = '#666';
        try {
            const response = await fetch('/api/v1/nl/marketplace-credentials?org_id=' + encodeURIComponent(orgId()), {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Не удалось сохранить');
            document.getElementById('mpc-secret').value = '';
            document.getElementById('mpc-client-id').value = '';
            status.textContent = 'Профиль сохранён выключенным до проверки.'; status.style.color = '#00875a';
            await window.loadMarketplaceCredentials();
        } catch (error) {
            status.textContent = error.message; status.style.color = '#c9372c';
        } finally { button.disabled = false; }
    };

    window.deleteMarketplaceCredential = async function (id) {
        if (!confirm('Удалить этот профиль доступа? Старый раздел подключений WB не изменится.')) return;
        const response = await fetch('/api/v1/nl/marketplace-credentials/' + encodeURIComponent(id) + '?org_id=' + encodeURIComponent(orgId()), {method: 'DELETE'});
        if (!response.ok) { const data = await response.json(); alert(data.detail || 'Не удалось удалить'); return; }
        await window.loadMarketplaceCredentials();
    };

    window.verifyMarketplaceCredential = async function (id) {
        const response = await fetch('/api/v1/nl/marketplace-credentials/' + encodeURIComponent(id) + '/verify?org_id=' + encodeURIComponent(orgId()), {method: 'POST'});
        const data = await response.json();
        if (!response.ok) { alert(data.detail || 'Ключ не прошёл проверку'); await window.loadMarketplaceCredentials(); return; }
        alert('Авторизация подтверждена безопасным запросом только на чтение. Заявленные права записи проверятся при первой разрешённой операции.');
        await window.loadMarketplaceCredentials();
    };

    window.toggleMarketplaceCredential = async function (id, enabled) {
        const response = await fetch('/api/v1/nl/marketplace-credentials/' + encodeURIComponent(id) + '?org_id=' + encodeURIComponent(orgId()), {
            method: 'PATCH', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({is_enabled: enabled})
        });
        const data = await response.json();
        if (!response.ok) { alert(data.detail || 'Не удалось изменить состояние'); return; }
        await window.loadMarketplaceCredentials();
    };
})();
