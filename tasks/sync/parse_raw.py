"""
Парсинг raw_api_data → tech_status.
Извлечено из scheduled_sync.py без изменения логики.
"""

import logging
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from celery import shared_task
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tasks.ue_precompute import run_precompute
from tasks.sync.utils import _run, _get_all_keys, _save_raw, get_wb_key_org_filter
from models.raw_data import TechStatus
from services.entity_sync import find_entity_by_barcode, add_unmatched

logger = logging.getLogger(__name__)

def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

@shared_task(name="wb.sched.parse_raw")
def sched_parse_raw():
    """Парсинг raw_api_data → tech_status после всех сборов"""
    result = _run(_do_parse_raw)
    try:
        run_precompute()
    except Exception as e:
        logging.getLogger(__name__).warning(f"[parse_raw] ue_precompute skipped: {e}")
    return result


async def _do_parse_raw(sf):
    """
    Парсер raw → tech_status по entity_id (слот размера).
    
    Логика:
    - 9-дневное окно: обновляем сегодня + 8 предыдущих дней
    - WB дописывает данные задним числом → полная замена, не инкремент
    - Сток — моментальный срез за конкретный день, НЕ суммируется
    - Все товары на каждый день (entity gap-fill)
    - Все даты по МСК
    """

    from services.entity_sync import find_entity_by_barcode, add_unmatched

    msk = ZoneInfo("Europe/Moscow")
    today_msk = datetime.now(msk).date()
    # 9-дневное окно: сегодня + 8 дней назад
    window_dates = [today_msk - timedelta(days=i) for i in range(9)]
    window_dates_set = set(window_dates)

    # Получаем org_ids из raw_api_data. Initial sync can scope this to one org.
    org_filter = get_wb_key_org_filter()
    async with sf() as db:
        if org_filter:
            result = await db.execute(
                text("""
                    SELECT DISTINCT organization_id
                    FROM raw_api_data
                    WHERE status = 'ok' AND organization_id = :org
                """),
                {"org": org_filter},
            )
        else:
            result = await db.execute(
                text("SELECT DISTINCT organization_id FROM raw_api_data WHERE status = 'ok'")
            )
        org_ids = [str(r[0]) for r in result.all()]

    total = 0
    for org_id in org_ids:
        logger.info(f"[parse_raw] processing org={org_id[:8]}, window={window_dates[-1]}..{window_dates[0]}")

        # --- Маппинг entity_id по (nm_id, size_name/chrt_id) ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT id, nm_id, size_name, chrt_id FROM product_entities WHERE organization_id = :org
            """), {"org": org_id})
            entity_by_nm_size = {}
            entity_by_nm_chrt = {}
            nm_to_first_entity = {}
            for row in result.all():
                eid = str(row[0])
                nm = int(row[1])
                sz = str(row[2])
                chrt_id = row[3]
                entity_by_nm_size[(nm, sz)] = eid
                chrt_key = _safe_int(chrt_id)
                if chrt_key is not None:
                    entity_by_nm_chrt[(nm, chrt_key)] = eid
                if nm not in nm_to_first_entity:
                    nm_to_first_entity[nm] = eid

        # --- Загружаем маппинг entity_id по barcode ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT eb.barcode, eb.entity_id FROM entity_barcodes eb
                WHERE eb.organization_id = :org
            """), {"org": org_id})
            entity_by_barcode = {}
            for row in result.all():
                entity_by_barcode[str(row[0])] = str(row[1])

        # --- Products (карточки) — один раз ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'products' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            prod_row = result.first()

        product_map = {}
        if prod_row and prod_row[0]:
            cards = prod_row[0] if isinstance(prod_row[0], list) else (prod_row[0].get("cards", []) if isinstance(prod_row[0], dict) else [])
            logger.info(f"[parse_raw] org={org_id[:8]}: products raw type={type(prod_row[0]).__name__}, cards_count={len(cards)}")
            for c in cards:
                if not isinstance(c, dict):
                    continue
                nm = c.get("nmID")
                if not nm:
                    continue
                photos = c.get("photos") or []
                for sz in (c.get("sizes") or []):
                    size_name = sz.get("techSizeName") or sz.get("techSize") or "ONE SIZE"
                    entity_id = entity_by_nm_size.get((int(nm), size_name))
                    key = entity_id or int(nm)
                    if key not in product_map:
                        product_map[key] = {
                            "name": c.get("title", ""),
                            "brand": c.get("brand", ""),
                            "photo": photos[0].get("hq", photos[0].get("tm", photos[0].get("big", ""))) if photos else "",
                            "nm_id": int(nm),
                            "entity_id": entity_id,
                            "vendor_code": c.get("vendorCode", "") or "",
                            "barcodes": [bc for sz_inner in (c.get("sizes") or []) for bc in (sz_inner.get("skus") or [])],
                            "rating": float(c.get("reviewRating", 0) or 0),
                        }

        logger.info(f"[parse_raw] org={org_id[:8]}: product_map built with {len(product_map)} entries")

        # --- Fallback: подтянуть vendor_code из product_entities ---
        for k, v in product_map.items():
            if not v.get('vendor_code'):
                eid = v.get('entity_id')
                nm = v.get('nm_id')
                async with sf() as db:
                    if eid:
                        result = await db.execute(text(
                            'SELECT vendor_code FROM product_entities WHERE id = :val LIMIT 1'
                        ), {'val': eid})
                    elif nm:
                        result = await db.execute(text(
                            'SELECT vendor_code FROM product_entities WHERE nm_id = :val AND organization_id = :org LIMIT 1'
                        ), {'val': nm, 'org': org_id})
                    else:
                        continue
                    row = result.first()
                    if row and row[0]:
                        v['vendor_code'] = row[0]

        # --- Fallback: подтянуть barcode из entity_barcodes ---
        for k, v in product_map.items():
            if not v.get('barcodes'):
                eid = v.get('entity_id')
                if eid:
                    async with sf() as db:
                        result = await db.execute(text(
                            'SELECT barcode FROM entity_barcodes WHERE entity_id = :eid AND is_active = true LIMIT 1'
                        ), {'eid': eid})
                        row = result.first()
                        if row and row[0]:
                            v['barcodes'] = [row[0]]

        # --- Fallback: подтянуть фото из product_entities ---
        for k, v in product_map.items():
            if not v.get('photo'):
                eid = v.get('entity_id')
                nm = v.get('nm_id')
                async with sf() as db:
                    if eid:
                        result = await db.execute(text(
                            'SELECT photo_main FROM product_entities WHERE id = :val LIMIT 1'
                        ), {'val': eid})
                    elif nm:
                        result = await db.execute(text(
                            'SELECT photo_main FROM product_entities WHERE nm_id = :val AND organization_id = :org LIMIT 1'
                        ), {'val': nm, 'org': org_id})
                    else:
                        continue
                    row = result.first()
                    if row and row[0]:
                        product_map[k]['photo'] = row[0]

        # --- Orders — за все дни из окна ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'orders' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            orders_rows = result.all()

        orders_map = {}  # key = (date, entity_id)
        seen_srids = set()  # дедупликация по srid (WB дублирует заказы в разных raw)
        from datetime import datetime as _dt_parse
        for orow in orders_rows:
            td, resp = orow
            ords = resp if isinstance(resp, list) else []
            for o in ords:
                if not isinstance(o, dict):
                    continue
                nm = o.get("nmId") or o.get("nm_id")
                barcode = str(o.get("barcode", "") or "")
                tech_size = str(o.get("techSize", "") or "")
                if not nm:
                    continue
                nm = int(nm)
                entity_id = entity_by_barcode.get(barcode) if barcode else None
                if not entity_id and tech_size:
                    entity_id = entity_by_nm_size.get((nm, tech_size))
                if not entity_id:
                    entity_id = nm_to_first_entity.get(nm)

                # Дедупликация по srid (WB дублирует один заказ в разных raw ответах)
                srid = str(o.get("srid", "") or "")
                if srid and srid in seen_srids:
                    continue
                if srid:
                    seen_srids.add(srid)

                # Используем РЕАЛЬНУЮ дату заказа, а не target_date из raw_api_data
                order_date_str = o.get("date", "")[:10]  # "2026-05-25T09:18:58" -> "2026-05-25"
                try:
                    order_date = date.fromisoformat(order_date_str) if order_date_str else td
                except ValueError:
                    order_date = td
                # Только если дата в окне
                if order_date not in window_dates_set:
                    continue

                key = (order_date, entity_id or nm)
                if key not in orders_map:
                    orders_map[key] = {"count": 0, "revenue": 0, "vendor_code": "", "barcode": barcode, "entity_id": entity_id, "nm_id": nm, "price": 0, "price_discount": 0}
                orders_map[key]["count"] += 1
                orders_map[key]["revenue"] += float(o.get("priceWithDisc") or o.get("totalPrice") or o.get("price") or 0)
                tp = float(o.get("totalPrice") or 0)
                pd = float(o.get("priceWithDisc") or 0)
                if tp > 0:
                    orders_map[key]["price"] = tp
                if pd > 0:
                    orders_map[key]["price_discount"] = pd
                if not orders_map[key]["vendor_code"]:
                    orders_map[key]["vendor_code"] = str(o.get("supplierArticle", "") or "")
                if entity_id and not orders_map[key]["entity_id"]:
                    orders_map[key]["entity_id"] = entity_id

        # --- Sales — за все дни из окна ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'sales' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            sales_rows = result.all()

        sales_map = {}  # key = (date, entity_id)
        seen_sale_ids = set()  # дедупликация по (sale_date, saleID) — один saleID один раз за конкретный день
        for srow in sales_rows:
            td, resp = srow
            sls = resp if isinstance(resp, list) else []
            for s in sls:
                if not isinstance(s, dict):
                    continue
                nm = s.get("nmId") or s.get("nm_id")
                barcode = str(s.get("barcode", "") or "")
                tech_size = str(s.get("techSize", "") or "")  # баг: было o.get вместо s.get
                if not nm:
                    continue
                nm = int(nm)
                entity_id = entity_by_barcode.get(barcode) if barcode else None
                if not entity_id and tech_size:
                    entity_id = entity_by_nm_size.get((nm, tech_size))
                if not entity_id:
                    entity_id = nm_to_first_entity.get(nm)

                # Используем РЕАЛЬНУЮ дату продажи (до дедупликации!)
                sale_date_str = s.get("date", "")[:10]
                try:
                    sale_date = date.fromisoformat(sale_date_str) if sale_date_str else td
                except ValueError:
                    sale_date = td
                if sale_date not in window_dates_set:
                    continue

                # Дедупликация по (реальная дата, saleID) — один saleID = одна запись за конкретный день
                sale_id = str(s.get("saleID", "") or "")
                dedup_key = (sale_date, sale_id)
                if sale_id and dedup_key in seen_sale_ids:
                    continue
                if sale_id:
                    seen_sale_ids.add(dedup_key)

                key = (sale_date, entity_id or nm)
                if key not in sales_map:
                    sales_map[key] = {"buyouts": 0, "returns": 0, "revenue": 0, "entity_id": entity_id, "nm_id": nm, "price": 0, "price_discount": 0}
                price = float(s.get("forPay") or s.get("priceWithDisc") or s.get("totalPrice") or 0)
                tp = float(s.get("totalPrice") or 0)
                pd = float(s.get("priceWithDisc") or 0)
                if tp > 0:
                    sales_map[key]["price"] = tp
                if pd > 0:
                    sales_map[key]["price_discount"] = pd
                if "R" in sale_id and not sale_id.startswith("S"):
                    sales_map[key]["returns"] += 1
                    sales_map[key]["revenue"] -= price
                else:
                    sales_map[key]["buyouts"] += 1
                    sales_map[key]["revenue"] += price
                if entity_id and not sales_map[key]["entity_id"]:
                    sales_map[key]["entity_id"] = entity_id

        # --- Stocks FBS (склады продавца) — из stocks_fbo, фильтр по имени ---
        stocks_by_date = {}  # key = date -> {entity_id: {qty, warehouses}}
        # будет заполнен ниже из stocks_fbo (только "склад продавца")

        # --- FBO Stocks (остатки на складах WB) ---
        fbo_by_date = {}  # key = date -> {entity_id/nm_id: {qty, chrt_id}}
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'stocks_fbo' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
                ORDER BY target_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            fbo_rows = result.all()

        for frow in fbo_rows:
            ftd, fresp = frow
            fbo_map = {}
            fitems = fresp if isinstance(fresp, list) else (fresp.get("data", {}).get("items", []) if isinstance(fresp, dict) else [])
            fbs_map = {}  # FBS for this date (склад продавца)
            for fi in fitems:
                if not isinstance(fi, dict):
                    continue
                nm = fi.get("nmId") or fi.get("nm_id")
                if not nm:
                    continue
                nm = int(nm)
                chrt = fi.get("chrtId")
                chrt_key = _safe_int(chrt)
                entity_id = entity_by_nm_chrt.get((nm, chrt_key)) if chrt_key is not None else None
                qty = int(fi.get("quantity", 0) or 0)
                wh_name = fi.get("warehouseName", "")
                is_seller_wh = "склад продавца" in wh_name.lower()
                
                if is_seller_wh:
                    # FBS — склад продавца
                    key = entity_id or nm
                    if key not in fbs_map:
                        fbs_map[key] = {"qty": 0, "warehouses": set(), "entity_id": entity_id, "nm_id": nm}
                    fbs_map[key]["qty"] += qty
                    if wh_name:
                        fbs_map[key]["warehouses"].add(wh_name)
                else:
                    # FBO — склад WB
                    key = entity_id or nm
                    if key not in fbo_map:
                        fbo_map[key] = {"qty": 0, "chrt_ids": set(), "entity_id": entity_id, "nm_id": nm}
                    fbo_map[key]["qty"] += qty
                    if chrt:
                        fbo_map[key]["chrt_ids"].add(chrt)
            
            if fbo_map:
                fbo_by_date[ftd] = fbo_map
            if fbs_map:
                # re-key by entity_id
                fbs_stock_map = {}
                for skey, sval in fbs_map.items():
                    snm = sval.get("nm_id") or skey
                    eid = sval.get("entity_id") or nm_to_first_entity.get(snm) or snm
                    if eid not in fbs_stock_map:
                        fbs_stock_map[eid] = {"qty": 0, "warehouses": set(), "entity_id": eid, "nm_id": snm}
                    fbs_stock_map[eid]["qty"] += sval["qty"]
                    fbs_stock_map[eid]["warehouses"].update(sval.get("warehouses", set()))
                stocks_by_date[ftd] = fbs_stock_map

        # --- Цены из tariff_snapshot (fallback) ---
        async with sf() as db:
            price_result = await db.execute(text("""
                SELECT DISTINCT ON (nm_id) nm_id, price_retail, price_with_spp
                FROM wb_tariff_snapshot
                WHERE organization_id = :org
                ORDER BY nm_id, target_date DESC
            """), {"org": org_id})
            tariff_prices = {r[0]: {"price": float(r[1]) if r[1] else 0, "price_spp": float(r[2]) if r[2] else 0} for r in price_result.all()}

        # --- Цены из prices raw (последний синк, точнее tariff_snapshot) ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'prices' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            prices_row = result.first()
        
        prices_by_nm = {}
        prices_by_entity = {}
        if prices_row and prices_row[0]:
            items = prices_row[0] if isinstance(prices_row[0], list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                nm = item.get("nmID") or item.get("nmId") or item.get("nm_id")
                if not nm:
                    continue
                for sz in (item.get("sizes") or []):
                    nm_int = int(nm)
                    price_info = {
                        "price": float(sz.get("price", 0) or 0) / 100,  # копейки → рубли
                        "price_discount": float(sz.get("discountedPrice", 0) or 0) / 100,
                        "price_spp": float(sz.get("clubDiscountedPrice", 0) or 0) / 100,
                    }
                    chrt = sz.get("chrtID") or sz.get("chrtId")
                    chrt_key = _safe_int(chrt)
                    entity_id = entity_by_nm_chrt.get((nm_int, chrt_key)) if chrt_key is not None else None
                    if entity_id:
                        prices_by_entity[entity_id] = price_info
                    else:
                        prices_by_nm[nm_int] = price_info

        # --- Sales Funnel: показы/клики по nm_id за каждый день ---
        # Формат WB: [{product: {nmId, title, ...}, statistic: {selected: {openCount, cartCount, ...}}}]
        funnel_map = {}  # key = (date, nm_id) -> {impressions, clicks}
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data
                WHERE api_method = 'sales_funnel' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
                ORDER BY target_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            funnel_rows = result.all()

        for fdate, fraw in funnel_rows:
            if fdate not in window_dates_set:
                continue
            funnel_data = fraw if isinstance(fraw, list) else []
            for fp in funnel_data:
                if not isinstance(fp, dict):
                    continue
                prod = fp.get("product") or {}
                nm = prod.get("nmId") or prod.get("nmID") or prod.get("nm_id")
                if not nm:
                    continue
                stat = (fp.get("statistic") or {}).get("selected") or {}
                period = stat.get("period") or {}
                # Старые raw-строки были 9-дневными. Их нельзя считать дневными.
                if period.get("start") and period.get("end"):
                    if period.get("start") != str(fdate) or period.get("end") != str(fdate):
                        continue
                funnel_map[(fdate, int(nm))] = {
                    "impressions": int(stat.get("openCount", 0) or 0),
                    "clicks": int(stat.get("cartCount", 0) or 0),
                }
        logger.info(f"[parse_raw] org={org_id[:8]}: sales_funnel rows={len(funnel_rows)}, funnel_map_keys={len(funnel_map)}")

        # --- Ad Stats: расходы по кампаниям за каждый день ---
        # Распределяем расходы пропорционально заказам (из tech_status)
        ad_cost_by_date = {}  # date -> total spent
        async with sf() as db:
            result = await db.execute(text("""
                SELECT stat_date, SUM(spent) as total_spent
                FROM ad_stats
                WHERE organization_id = :org AND stat_date >= :start_date
                GROUP BY stat_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            for r in result.all():
                ad_cost_by_date[r[0]] = float(r[1]) if r[1] else 0

        logger.info(f"[parse_raw] org={org_id[:8]}: ad_cost_by_date={len(ad_cost_by_date)}")

        # ============================================================
        # --- Upsert: перебираем ВСЕ entity × ВСЕ даты из окна ---
        # ============================================================
        
        # Собираем все entity_id
        all_entities = set()
        for (nm, sz), eid in entity_by_nm_size.items():
            all_entities.add(eid)

        # Pre-pass: суммарные заказы за каждый день (для пропорционального распределения ad_cost)
        total_orders_by_date = {}
        for td in window_dates:
            day_total = 0
            for ek in all_entities:
                day_total += orders_map.get((td, ek), {}).get("count", 0)
            total_orders_by_date[td] = day_total

        logger.info(f"[parse_raw] org={org_id[:8]}: dates={len(window_dates)}, entities={len(all_entities)}, orders_keys={len(orders_map)}, sales_keys={len(sales_map)}, stocks_dates={len(stocks_by_date)}")

        for target_date in window_dates:
            date_stock = stocks_by_date.get(target_date, {})
            
            for entity_key in all_entities:
                pinfo = product_map.get(entity_key, {})
                nm_from_pinfo = pinfo.get("nm_id", None)
                entity_from_pinfo = pinfo.get("entity_id", None)

                e_id = entity_from_pinfo or entity_key
                n_id = nm_from_pinfo
                if not n_id:
                    for (nm, sz), eid in entity_by_nm_size.items():
                        if eid == entity_key:
                            n_id = nm
                            break

                # Данные за конкретную дату
                oinfo = orders_map.get((target_date, entity_key), {})
                sinfo = sales_map.get((target_date, entity_key), {})
                skinfo = date_stock.get(entity_key, {})
                # FBO stock: prefer exact entity/chrt_id, fallback to nm_id only when source lacks size key.
                _date_fbo = fbo_by_date.get(target_date, {})
                _fbo_info = _date_fbo.get(e_id) or _date_fbo.get(n_id, {})
                _fbo_qty = _fbo_info.get("qty", 0) if _fbo_info else 0

                # Цены: приоритет sales > orders > prices raw > tariff_snapshot
                _tp = tariff_prices.get(n_id, {}) if n_id else {}
                _wp = prices_by_entity.get(e_id) or (prices_by_nm.get(n_id, {}) if n_id else {})
                _price = sinfo.get("price", 0) or oinfo.get("price", 0) or _wp.get("price", 0) or _tp.get("price", 0)
                _price_discount = sinfo.get("price_discount", 0) or oinfo.get("price_discount", 0) or _wp.get("price_discount", 0) or _tp.get("price_spp", 0)
                _price_spp = _wp.get("price_spp", 0) or _tp.get("price_spp", 0)

                _funnel = funnel_map.get((target_date, n_id), {}) if n_id else {}
                _impressions = _funnel.get("impressions", 0)
                _clicks = _funnel.get("clicks", 0)

                # Расходы: пропорционально заказам товара относительно всех заказов за день
                _date_ad_total = ad_cost_by_date.get(target_date, 0)
                _day_total_orders = total_orders_by_date.get(target_date, 0)
                _entity_orders = oinfo.get("count", 0)
                if _date_ad_total > 0 and _day_total_orders > 0 and _entity_orders > 0:
                    _ad_cost = round(_date_ad_total * _entity_orders / _day_total_orders, 2)
                else:
                    _ad_cost = 0

                async with sf() as db:
                    ins = pg_insert(TechStatus)
                    stmt = ins.values(
                        id=uuid.uuid4(),
                        organization_id=org_id,
                        target_date=target_date,
                        nm_id=n_id,
                        entity_id=e_id,
                        product_name=pinfo.get("name", ""),
                        vendor_code=oinfo.get("vendor_code", "") or pinfo.get("vendor_code", ""),
                        barcode=oinfo.get("barcode", "") or (pinfo.get("barcodes", [""])[0] if pinfo.get("barcodes") else ""),
                        photo_main=pinfo.get("photo", ""),
                        orders_count=oinfo.get("count", 0),
                        buyouts_count=sinfo.get("buyouts", 0),
                        returns_count=sinfo.get("returns", 0),
                        stock_qty=skinfo.get("qty", 0),
                        stock_fbo_qty=_fbo_qty,
                        warehouse_name=", ".join(skinfo.get("warehouses", set())) if skinfo.get("warehouses") else None,
                        price=_price if _price else None,
                        price_discount=_price_discount if _price_discount else None,
                        price_spp=_price_spp if _price_spp else None,
                        rating=pinfo.get("rating", None),
                        impressions=_impressions if _impressions else None,
                        clicks=_clicks if _clicks else None,
                        ad_cost=_ad_cost if _ad_cost else None,
                        row_status="active",
                        is_final="no",
                        last_sync_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    ).on_conflict_do_update(
                        constraint="tech_status_org_date_entity_key",
                        set_={
                            "product_name": ins.excluded.product_name,
                            "vendor_code": ins.excluded.vendor_code,
                            "barcode": ins.excluded.barcode,
                            "photo_main": ins.excluded.photo_main,
                            "orders_count": ins.excluded.orders_count,
                            "buyouts_count": ins.excluded.buyouts_count,
                            "returns_count": ins.excluded.returns_count,
                            "stock_qty": ins.excluded.stock_qty,
                            "stock_fbo_qty": ins.excluded.stock_fbo_qty,
                            "warehouse_name": ins.excluded.warehouse_name,
                            "nm_id": ins.excluded.nm_id,
                            "price": ins.excluded.price,
                            "price_discount": ins.excluded.price_discount,
                            "price_spp": ins.excluded.price_spp,
                            "rating": ins.excluded.rating,
                            "impressions": ins.excluded.impressions,
                            "clicks": ins.excluded.clicks,
                            "ad_cost": ins.excluded.ad_cost,
                            "last_sync_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                        }
                    )
                    try:
                        await db.execute(stmt)
                        await db.commit()
                        total += 1
                    except Exception as exc:
                        await db.rollback()
                        logger.error(f"[parse_raw] upsert error entity={e_id}, nm={n_id}, date={target_date}: {exc}")

    logger.info(f"[sched] parse_raw: {total} records upserted across {len(window_dates)} dates for {len(org_ids)} orgs")
    return {"parsed": total}


# ─── ПОДТЯЖКА ФОТО ────────────────────────────────────────
