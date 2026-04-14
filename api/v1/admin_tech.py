"""Техническая таблица состояния (ТС) — страница диагностики"""
from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import date, timedelta

from core.database import get_db
from models.raw_data import TechStatus, RawApiData, RawBarcode

router = APIRouter(tags=["admin"])

ADMIN_TOKEN = "nl-tech-2026"


@router.get("/admin/tech", response_class=HTMLResponse)
async def tech_status_page(
    token: str = Query(""),
    days: int = Query(15),
    db: AsyncSession = Depends(get_db),
):
    if token != ADMIN_TOKEN:
        return HTMLResponse("<h2>🚫 Доступ запрещён. Используйте ?token=nl-tech-2026</h2>", status_code=403)

    today = date.today()
    date_from = today - timedelta(days=days)

    # Строки ТС за период (только уникальные карточки по дате — берём первый barcode)
    result = await db.execute(
        select(TechStatus)
        .where(TechStatus.target_date >= date_from)
        .order_by(TechStatus.target_date.desc(), TechStatus.nm_id.asc())
    )
    all_rows = result.scalars().all()

    # Группируем по (дата, nm_id) — карточка с её штрихкодами
    from collections import OrderedDict
    cards = OrderedDict()
    for row in all_rows:
        key = (row.target_date, row.nm_id)
        if key not in cards:
            cards[key] = {
                "row": row,
                "barcodes": []
            }
        cards[key]["barcodes"].append({
            "barcode": row.barcode or "",
            "nm_id": row.nm_id,
        })

    # Получаем все штрихкоды с размерами
    barcodes_result = await db.execute(select(RawBarcode))
    barcode_map = {}
    for bc in barcodes_result.scalars().all():
        barcode_map[bc.barcode] = bc.size_name or ""

    # Статусы API методов
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
    method_statuses = {}
    for m in methods_raw:
        key = m[0]
        if key not in method_statuses:
            method_statuses[key] = {"status": m[1], "date": m[2], "fetched": m[3]}

    last_sync = None
    for c in cards.values():
        r = c["row"]
        if r.last_sync_at and (last_sync is None or r.last_sync_at > last_sync):
            last_sync = r.last_sync_at

    # Собираем уникальные nm_id для запроса баркодов
    nm_ids = list(set(k[1] for k in cards.keys() if k[1]))

    # Получаем баркоды с размерами по nm_id
    bc_by_nm = {}
    if nm_ids:
        bc_result = await db.execute(
            select(RawBarcode).where(RawBarcode.nm_id.in_(nm_ids))
        )
        for bc in bc_result.scalars().all():
            if bc.nm_id not in bc_by_nm:
                bc_by_nm[bc.nm_id] = []
            bc_by_nm[bc.nm_id].append({
                "barcode": bc.barcode,
                "size": bc.size_name or "?",
            })

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ТС — Таблица состояния</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; padding: 16px; }}
h1 {{ color: #58a6ff; margin-bottom: 8px; font-size: 1.4em; }}
.info {{ color: #8b949e; margin-bottom: 16px; font-size: 0.85em; }}
.methods {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; }}
.mc {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 8px 12px; font-size: 0.8em; }}
.mc .n {{ font-weight: bold; color: #58a6ff; }}
.mc .d {{ color: #8b949e; font-size: 0.75em; }}
.legend {{ display: flex; gap: 14px; margin-bottom: 12px; font-size: 0.8em; }}
.legend span {{ display: flex; align-items: center; gap: 4px; }}
.dot {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; }}
.controls {{ margin-bottom: 12px; }}
.controls select {{ background: #161b22; color: #c9d1d9; border: 1px solid #30363d; padding: 5px 8px; border-radius: 5px; }}

table {{ width: 100%; border-collapse: collapse; font-size: 0.75em; }}
th {{ background: #161b22; color: #58a6ff; padding: 6px 4px; text-align: left; position: sticky; top: 0; white-space: nowrap; border-bottom: 2px solid #30363d; z-index: 10; }}
td {{ padding: 4px; border-bottom: 1px solid #21262d; white-space: nowrap; }}
tr:hover {{ background: #161b22; }}
.r {{ text-align: right; }}

/* Карточка */
.card-row {{ background: #111820; }}
.card-row td {{ padding: 6px 4px; border-bottom: 1px solid #30363d; font-weight: 500; }}
.card-img {{ width: 40px; height: 40px; border-radius: 4px; object-fit: cover; }}
.card-img-placeholder {{ width: 40px; height: 40px; border-radius: 4px; background: #21262d; display: flex; align-items: center; justify-content: center; font-size: 1.2em; color: #484f58; }}

/* Размеры (подстроки) */
.size-row td {{ padding: 3px 4px; border-bottom: 1px solid #161b22; color: #8b949e; font-size: 0.9em; }}
.size-row:hover {{ background: #141a22; }}

.status-closed {{ border-left: 3px solid #4caf50; }}
.status-active {{ border-left: 3px solid #ff9800; }}
.status-error {{ border-left: 3px solid #f44336; }}

.barcodes {{ display: flex; flex-wrap: wrap; gap: 4px; }}
.bc-tag {{ background: #1c2333; border: 1px solid #30363d; border-radius: 3px; padding: 1px 5px; font-size: 0.85em; }}
.bc-tag .sz {{ color: #58a6ff; font-weight: 600; }}
.bc-tag .bc {{ color: #8b949e; }}
</style>
</head>
<body>
<h1>🔧 ТС — Таблица состояния</h1>
<div class="info">Синхронизация: {last_sync.strftime('%Y-%m-%d %H:%M UTC') if last_sync else 'нет данных'} | Дней: {days} | Карточек: {len(cards)}</div>

<div class="methods">
"""

    for method, info in sorted(method_statuses.items()):
        emoji = "🟢" if info["status"] == "ok" else "🔴"
        html += f'<div class="mc"><span class="n">{method}</span> {emoji}<br><span class="d">{info["date"]}</span></div>\n'

    html += f"""
</div>

<div class="legend">
<span><span class="dot" style="background:#4caf50"></span> Закрыто</span>
<span><span class="dot" style="background:#ff9800"></span> В окне</span>
<span><span class="dot" style="background:#f44336"></span> Ошибка</span>
</div>

<div class="controls">
<select onchange="location.href='/admin/tech?token={token}&days='+this.value">
<option value="7" {'selected' if days==7 else ''}>7 дней</option>
<option value="15" {'selected' if days==15 else ''}>15 дней</option>
<option value="30" {'selected' if days==30 else ''}>30 дней</option>
</select>
</div>

<table>
<thead>
<tr>
<th></th>
<th>Фото</th>
<th>Дата</th>
<th>Арт WB</th>
<th>Арт пост.</th>
<th>Название</th>
<th>Размеры / ШК</th>
<th>Всего</th>
<th>В раб.</th>
<th>Фото</th>
<th>Видео</th>
<th>Опис.</th>
<th>Заказы</th>
<th>Выкупы</th>
<th>Возвр.</th>
<th>Склад</th>
<th>Остаток</th>
<th>Цена</th>
<th>Реклама</th>
<th>Синк</th>
</tr>
</thead>
<tbody>
"""

    for key, card_data in cards.items():
        row = card_data["row"]
        target_date, nm_id = key

        status_class = f"status-{row.row_status}"
        emoji = {"closed": "🟢", "active": "🟡", "error": "🔴"}.get(row.row_status, "⚪")

        # Фото — миниатюра (заменяем /big/ на /small/)
        photo_url = row.photo_main or ""
        thumb_url = photo_url.replace("/big/", "/small/") if photo_url else ""
        img_html = f'<img class="card-img" src="{thumb_url}" loading="lazy">' if thumb_url else '<div class="card-img-placeholder">📦</div>'

        # Штрихкоды с размерами
        nm_barcodes = bc_by_nm.get(nm_id, [])
        bc_tags = ""
        if nm_barcodes:
            parts = []
            for bc in nm_barcodes:
                parts.append(f'<span class="bc-tag"><span class="sz">{bc["size"]}</span> <span class="bc">{bc["barcode"][-4:]}</span></span>')
            bc_tags = '<div class="barcodes">' + "".join(parts) + '</div>'

        html += f"""<tr class="card-row {status_class}">
<td>{emoji}</td>
<td>{img_html}</td>
<td>{target_date.strftime('%d.%m')}</td>
<td><b>{nm_id or ''}</b></td>
<td>{row.vendor_code or ''}</td>
<td>{(row.product_name or '')[:35]}</td>
<td>{bc_tags}</td>
<td class="r">{row.cards_total or ''}</td>
<td class="r">{row.cards_active or ''}</td>
<td class="r">{row.photo_count or ''}</td>
<td>{row.has_video or ''}</td>
<td class="r">{row.description_chars or ''}</td>
<td class="r">{row.orders_count or ''}</td>
<td class="r">{row.buyouts_count or ''}</td>
<td class="r">{row.returns_count or ''}</td>
<td>{row.warehouse_name or ''}</td>
<td class="r">{row.stock_qty or ''}</td>
<td class="r">{row.price or ''}</td>
<td class="r">{row.ad_cost or ''}</td>
<td>{row.last_sync_at.strftime('%H:%M') if row.last_sync_at else ''}</td>
</tr>\n"""

    if not cards:
        html += '<tr><td colspan="20" style="text-align:center;padding:40px;color:#8b949e;">Нет данных. Запустите синк.</td></tr>'

    html += """
</tbody>
</table>
</body>
</html>
"""
    return HTMLResponse(html)
