"""Техническая таблица состояния (ТС) — страница диагностики"""
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import date, timedelta

from core.database import get_db
from models.raw_data import TechStatus, RawApiData

router = APIRouter(tags=["admin"])

ADMIN_TOKEN = "nl-tech-2026"  # Простой токен для доступа


def status_emoji(status: str) -> str:
    return {"closed": "🟢", "active": "🟡", "error": "🔴"}.get(status, "⚪")


def cell_color(status: str) -> str:
    return {"green": "#4caf50", "yellow": "#ff9800", "red": "#f44336"}.get(status, "#999")


@router.get("/admin/tech", response_class=HTMLResponse)
async def tech_status_page(
    request: Request,
    token: str = Query(""),
    days: int = Query(15),
    db: AsyncSession = Depends(get_db),
):
    # Простая проверка доступа
    if token != ADMIN_TOKEN:
        return HTMLResponse("<h2>🚫 Доступ запрещён. Используйте ?token=nl-tech-2026</h2>", status_code=403)

    today = date.today()
    date_from = today - timedelta(days=days)

    # Получаем строки ТС
    result = await db.execute(
        select(TechStatus)
        .where(TechStatus.target_date >= date_from)
        .order_by(TechStatus.target_date.desc(), TechStatus.nm_id.asc())
        .limit(500)
    )
    rows = result.scalars().all()

    # Получаем статусы API ключей (по последнему синку каждого метода)
    methods_result = await db.execute(
        select(
            RawApiData.api_method,
            RawApiData.status,
            RawApiData.target_date,
            RawApiData.fetched_at,
        )
        .where(RawApiData.target_date >= date_from)
        .order_by(RawApiData.fetched_at.desc())
    )
    methods_raw = methods_result.all()

    # Группируем статусы методов
    method_statuses = {}
    for m in methods_raw:
        key = m[0]
        if key not in method_statuses:
            method_statuses[key] = {"status": m[1], "date": m[2], "fetched": m[3]}
        elif m[3] and method_statuses[key].get("fetched") and m[3] > method_statuses[key]["fetched"]:
            method_statuses[key] = {"status": m[1], "date": m[2], "fetched": m[3]}

    # Строим HTML
    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ТС — Таблица состояния</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }}
        h1 {{ color: #58a6ff; margin-bottom: 10px; font-size: 1.5em; }}
        h2 {{ color: #8b949e; margin: 20px 0 10px; font-size: 1.1em; }}
        .info {{ color: #8b949e; margin-bottom: 20px; font-size: 0.9em; }}
        .methods {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 20px; }}
        .method-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 15px; }}
        .method-card .name {{ font-weight: bold; color: #58a6ff; font-size: 0.85em; }}
        .method-card .status {{ font-size: 1.2em; }}
        .method-card .date {{ color: #8b949e; font-size: 0.75em; }}
        .controls {{ margin-bottom: 15px; }}
        .controls select, .controls input {{ background: #161b22; color: #c9d1d9; border: 1px solid #30363d; padding: 6px 10px; border-radius: 6px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.8em; overflow-x: auto; display: block; }}
        th {{ background: #161b22; color: #58a6ff; padding: 8px 6px; text-align: left; position: sticky; top: 0; white-space: nowrap; border-bottom: 2px solid #30363d; }}
        td {{ padding: 6px; border-bottom: 1px solid #21262d; white-space: nowrap; }}
        tr:hover {{ background: #161b22; }}
        .status-cell {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; }}
        .row-closed {{ border-left: 3px solid #4caf50; }}
        .row-active {{ border-left: 3px solid #ff9800; }}
        .row-error {{ border-left: 3px solid #f44336; }}
        .legend {{ display: flex; gap: 15px; margin-bottom: 15px; font-size: 0.85em; }}
        .legend span {{ display: flex; align-items: center; gap: 5px; }}
        .count {{ text-align: right; }}
        a {{ color: #58a6ff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <h1>🔧 ТС — Таблица состояния</h1>
    <div class="info">Последнее обновление: {rows[0].last_sync_at.strftime('%Y-%m-%d %H:%M UTC') if rows and rows[0].last_sync_at else 'нет данных'} | Дней: {days}</div>

    <h2>API методы</h2>
    <div class="methods">
"""

    for method, info in sorted(method_statuses.items()):
        emoji = "🟢" if info["status"] == "ok" else "🔴"
        html += f"""<div class="method-card"><div class="name">{method}</div><div class="status">{emoji}</div><div class="date">{info['date']}</div></div>\n"""

    html += f"""
    </div>

    <div class="legend">
        <span><span class="status-cell" style="background:#4caf50"></span> Закрыто (15+ дней)</span>
        <span><span class="status-cell" style="background:#ff9800"></span> В окне (1-15 дней)</span>
        <span><span class="status-cell" style="background:#f44336"></span> Ошибка</span>
    </div>

    <div class="controls">
        <label>Показать за:
        <select onchange="window.location.href='/admin/tech?token={token}&days='+this.value">
            <option value="7" {'selected' if days==7 else ''}>7 дней</option>
            <option value="15" {'selected' if days==15 else ''}>15 дней</option>
            <option value="30" {'selected' if days==30 else ''}>30 дней</option>
        </select></label>
    </div>

    <table>
    <thead>
        <tr>
            <th></th>
            <th>Дата</th>
            <th>Арт WB</th>
            <th>Арт пост.</th>
            <th>ШК</th>
            <th>Название</th>
            <th>Всего</th>
            <th>В раб.</th>
            <th>Архив</th>
            <th>Фото</th>
            <th>Видео</th>
            <th>Опис.</th>
            <th>Рейт.</th>
            <th>Заказы</th>
            <th>Выкупы</th>
            <th>Возвр.</th>
            <th>Показы</th>
            <th>Клики</th>
            <th>Склад</th>
            <th>Остаток</th>
            <th>Тариф</th>
            <th>Цена</th>
            <th>Скидка</th>
            <th>СПП</th>
            <th>Реклама</th>
            <th>Синк</th>
        </tr>
    </thead>
    <tbody>
"""

    for row in rows:
        row_class = f"row-{row.row_status}"
        emoji = status_emoji(row.row_status)

        html += f"""<tr class="{row_class}">
            <td>{emoji}</td>
            <td>{row.target_date.strftime('%d.%m')}</td>
            <td>{row.nm_id or ''}</td>
            <td>{row.vendor_code or ''}</td>
            <td>{row.barcode or ''}</td>
            <td>{(row.product_name or '')[:30]}</td>
            <td class="count">{row.cards_total or ''}</td>
            <td class="count">{row.cards_active or ''}</td>
            <td class="count">{row.cards_archive or ''}</td>
            <td class="count">{row.photo_count or ''}</td>
            <td>{row.has_video or ''}</td>
            <td class="count">{row.description_chars or ''}</td>
            <td>{row.rating or ''}</td>
            <td class="count">{row.orders_count or ''}</td>
            <td class="count">{row.buyouts_count or ''}</td>
            <td class="count">{row.returns_count or ''}</td>
            <td class="count">{row.impressions or ''}</td>
            <td class="count">{row.clicks or ''}</td>
            <td>{row.warehouse_name or ''}</td>
            <td class="count">{row.stock_qty or ''}</td>
            <td class="count">{row.tariff or ''}</td>
            <td class="count">{row.price or ''}</td>
            <td class="count">{row.price_discount or ''}</td>
            <td class="count">{row.price_spp or ''}</td>
            <td class="count">{row.ad_cost or ''}</td>
            <td>{row.last_sync_at.strftime('%H:%M') if row.last_sync_at else ''}</td>
        </tr>\n"""

    if not rows:
        html += '<tr><td colspan="26" style="text-align:center;padding:40px;color:#8b949e;">Нет данных. Запустите синк.</td></tr>'

    html += """
    </tbody>
    </table>
</body>
</html>
"""
    return HTMLResponse(html)
