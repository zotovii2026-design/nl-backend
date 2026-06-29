"""Unit Economics API routes."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date, timedelta
import math

from core.database import get_db
from models.user import User
from core.role_deps import require_organization_role
from models.organization import Role
from core.dependencies import get_current_user
from core.tenant_auth import require_query_organization_access
from services.reference import resolve_org_id
from domain.unit_economics import (
    apply_financial_formulas,
    build_box_tariff_context,
    calculate_delivery,
    calculate_reverse_delivery,
)
from repositories.unit_economics import (
    get_latest_date as get_unit_economics_latest_date,
    get_products as get_unit_economics_products,
    get_supporting_rows as get_unit_economics_supporting_rows,
)

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


async def build_unit_economics(
    org_id: str,
    db: AsyncSession,
    search: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Юнит Экономика — сборка всех данных по SKU"""
    # ── Redis-кэш: отдаём готовый результат если есть ──
    import redis as _redis_lib, json as _json
    _redis = _redis_lib.from_url("redis://redis:6379/0")
    _cache_key = f"ue_cache:{org_id}"
    if not search:
        _cached = _redis.get(_cache_key)
        if _cached:
            try:
                _cached_data = _json.loads(_cached)
                if limit and limit > 0:
                    _cached_data["items"] = _cached_data["items"][:limit]
                return _cached_data
            except Exception:
                pass

    latest_date = await get_unit_economics_latest_date(db, org_id)
    if not latest_date:
        return {"items": [], "total": 0}
    products = await get_unit_economics_products(db, org_id, latest_date)
    rb_rows, tsnap_rows, box_rows = await get_unit_economics_supporting_rows(
        org_id,
        db=db,
    )

    # ── Обработка единого запроса reference_book ──────────
    # Колонки rb_rows:
    #  0: entity_id, 1: nm_id
    #  2-9: UE fields (mp_correction...fulfillment_model)
    # 10-12: wb_club_discount_pct, storage_pct, product_status
    # 13-17: mp_base_pct, wb_price_fact, wb_price_retail, wb_discount_pct, wb_prices_updated_at
    # 18-24: cost fields (cost_price...vat)
    # 25-28: product_class, brand, tax_system, tax_rate, vat_rate
    # 29: fbs_warehouse

    ue_by_entity = {}
    ue_by_nm_bc = {}
    cost_by_entity = {}
    cost_by_nm = {}
    ff_by_entity = {}
    ff_by_nm = {}

    for r in rb_rows:
        eid = str(r[0]) if r[0] else None
        nm = r[1]

        # ─ UE fields ─
        ue_fields = {
            "mp_correction_pct": r[2], "buyout_niche_pct": r[3],
            "extra_costs": r[4], "ad_plan_rub": r[5],
            "price_before_spp_plan": r[6], "price_before_spp_change": r[7],
            "change_date": r[8], "tariff_type": r[9],
            "wb_club_discount_pct": r[10],
            "product_status": r[12],
            "mp_base_pct": r[13],
            "wb_price_fact": r[14],
            "wb_price_retail": r[15],
            "wb_discount_pct": r[16],
            "wb_prices_updated_at": r[17],
            "fulfillment_model": r[9],
            "fbs_warehouse": r[29],
        }
        if eid:
            if eid not in ue_by_entity:
                ue_by_entity[eid] = ue_fields
        else:
            key = (nm, "")
            if key not in ue_by_nm_bc:
                ue_by_nm_bc[key] = ue_fields

        # ─ Cost fields ─
        cost_fields = {
            "cost_price": r[18], "purchase_cost": r[19], "logistics_cost": r[20],
            "packaging_cost": r[21], "other_costs": r[22], "vat": r[23],
            "product_class": r[24], "brand": r[25], "tax_system": r[26],
            "tax_rate": r[27], "vat_rate": r[28],
            "product_status": r[12],
        }
        if eid:
            if eid not in cost_by_entity:
                cost_by_entity[eid] = cost_fields
            else:
                for k, v in cost_fields.items():
                    if v is not None and v != 0:
                        cost_by_entity[eid][k] = v
        if nm:
            if nm not in cost_by_nm:
                cost_by_nm[nm] = cost_fields
            else:
                for k, v in cost_fields.items():
                    if v is not None and v != 0:
                        cost_by_nm[nm][k] = v

        # ─ FF fields ─
        fful = r[9]  # fulfillment_model
        fwh = r[29]  # fbs_warehouse
        if eid:
            if eid not in ff_by_entity:
                ff_by_entity[eid] = (fful, fwh)
        if nm:
            if nm not in ff_by_nm:
                ff_by_nm[nm] = (fful, fwh)

    # ── WB-данные из wb_tariff_snapshot ──────────
    snap_by_nm = {}
    for r in tsnap_rows:
        if r[0] not in snap_by_nm:
            snap_by_nm[r[0]] = {
                "logistics_tariff": float(r[1]) if r[1] else 0,
                "storage_tariff": float(r[2]) if r[2] else 0,
                "ad_cost_fact": float(r[3]) if r[3] else 0,
                "buyout_pct_fact": float(r[4]) if r[4] else 0,
                "commission_pct": float(r[5]) if r[5] else 0,
                "price_retail": float(r[6]) if r[6] else 0,
                "price_with_spp": float(r[7]) if r[7] else 0,
                "spp_pct": float(r[8]) if r[8] else 0,
                "commission_fbs_pct": float(r[9]) if r[9] else 0,
            }

    tariff_context = build_box_tariff_context(box_rows)
    box_tariffs = tariff_context["tariffs"]

    # 8) Собираем результат
    items = []
    search_q = search.lower() if search else ""
    
    # size_name и subject_name берутся напрямую из product_entities (индексы 10, 11)

    for p in products:
        entity_id = str(p[0]) if p[0] else None
        nm_id = p[1]
        vendor_code = p[2] or ""
        product_name = p[3] or ""
        photo = p[4] or ""
        main_barcode = p[5] or ""
        price = float(p[6]) if p[6] else 0
        price_discount = float(p[7]) if p[7] else 0
        # size_name и subject_name из product_entities (индексы 10, 11)
        _pe_size_name = p[10] or ""
        _pe_subject_name = p[11] or ""
        # Габариты из product_entities (индексы 12, 13, 14) — в см
        _pe_width = float(p[12]) if p[12] else 0
        _pe_height = float(p[13]) if p[13] else 0
        _pe_length = float(p[14]) if p[14] else 0
        # Объём в литрах (Д×Ш×В см / 1000)
        _volume_liters = round(_pe_width * _pe_height * _pe_length / 1000, 3) if (_pe_width and _pe_height and _pe_length) else 0

        # Фульфилмент модель и склад ФБС
        ff_info = ff_by_entity.get(entity_id, ff_by_nm.get(nm_id, (None, None)))
        _fulfillment_model = ff_info[0] or "fbo"
        _fbs_warehouse = ff_info[1] if ff_info[1] and ff_info[1] != "0" else None

        # Расчёт логистики до клиента
        _delivery_to_client, _delivery_debug = calculate_delivery(
            _volume_liters,
            _fulfillment_model,
            _fbs_warehouse,
            tariff_context,
        )
        _reverse_logistics, _reverse_debug = calculate_reverse_delivery(
            _volume_liters
        )

        # Tooltip расшифровка логистики (детальная WB-методика)
        _logistics_tooltip_parts = []
        if _pe_width and _pe_height and _pe_length:
            _logistics_tooltip_parts.append(f"Габариты: {_pe_length}x{_pe_width}x{_pe_height} см")
            _logistics_tooltip_parts.append(f"Объём: {_volume_liters:.3f} л (окр. {math.ceil(_volume_liters)})")

        _meth = _delivery_debug.get("method", "")
        # Модель отгрузки
        if _fulfillment_model == "fbs":
            _model_label = f"Модель: ФБС (склад: {_fbs_warehouse or 'не указан'})"
        else:
            _model_label = "Модель: ФБО"
        _logistics_tooltip_parts.append(_model_label)
        _logistics_tooltip_parts.append(f"Методика: {_meth}")

        if "сетка" in _meth:
            # <= 1 литр: показываем сетку WB
            _logistics_tooltip_parts.append("")
            _logistics_tooltip_parts.append("WB-сетка (<= 1 л):")
            _logistics_tooltip_parts.append("  0.001-0.200 л → 23 ₽/л")
            _logistics_tooltip_parts.append("  0.201-0.400 л → 26 ₽/л")
            _logistics_tooltip_parts.append("  0.401-0.600 л → 29 ₽/л")
            _logistics_tooltip_parts.append("  0.601-0.800 л → 30 ₽/л")
            _logistics_tooltip_parts.append("  0.801-1.000 л → 32 ₽/л")
            _tr = _delivery_debug.get("tier_rate", 0)
            _logistics_tooltip_parts.append(f"  → Товар {_volume_liters:.3f} л = {_tr:.0f} ₽/л")
            _logistics_tooltip_parts.append("")
            if "ФБО-среднее" in _meth:
                _kd = box_tariffs.get("Коледино", {})
                _kr = box_tariffs.get("Краснодар", {})
                _kz = box_tariffs.get("Казань", {})
                _logistics_tooltip_parts.append(f"Коледино: коэфф. {_kd.get('fbo_coef', 0):.0f}%")
                _logistics_tooltip_parts.append(f"Краснодар: коэфф. {_kr.get('fbo_coef', 0):.0f}%")
                _logistics_tooltip_parts.append(f"Казань: коэфф. {_kz.get('fbo_coef', 0):.0f}%")
                _ac = _delivery_debug.get("avg_coef", 0)
                _logistics_tooltip_parts.append(f"Средний коэфф.: {_ac:.2f}%")
                _logistics_tooltip_parts.append(f"Формула: {_tr:.0f} × {_ac:.2f}% = {_delivery_to_client:.2f} ₽")
            else:
                _wh = _delivery_debug.get("warehouse", "?")
                _coef = _delivery_debug.get("coef", 0)
                _logistics_tooltip_parts.append(f"Склад: {_wh}, коэфф. {_coef:.0f}%")
                _logistics_tooltip_parts.append(f"Формула: {_tr:.0f} × {_coef:.0f}% = {_delivery_to_client:.2f} ₽")
        elif ">1л" in _meth:
            # > 1 литр: показываем формулу
            _logistics_tooltip_parts.append("")
            _vc = _delivery_debug.get("vol_ceil", 0)
            if "ФБС" in _meth:
                # ФБС: конкретный склад
                _b = _delivery_debug.get("base", 0)
                _l = _delivery_debug.get("liter", 0)
                _c = _delivery_debug.get("coef", 0)
                _wh = _delivery_debug.get("warehouse", "?")
                _fw = _delivery_debug.get("warehouse_requested", "")
                if _fw:
                    _logistics_tooltip_parts.append(f"Склад: {_fw} → тариф по {_wh}")
                else:
                    _logistics_tooltip_parts.append(f"Склад: {_wh}")
                _logistics_tooltip_parts.append(f"Базовый тариф: {_b:.2f} ₽ + {_l:.2f} ₽/л, коэфф. {_c:.0f}%")
                _logistics_tooltip_parts.append(f"Формула: {_b:.2f} + ({_vc}-1) × {_l:.2f} = {_delivery_to_client:.2f} ₽")
            elif "ФБО" in _meth:
                _kd = box_tariffs.get("Коледино", {})
                _kr = box_tariffs.get("Краснодар", {})
                _kz = box_tariffs.get("Казань", {})
                _ab = round((_kd.get('fbo_base') or 0) + (_kr.get('fbo_base') or 0) + (_kz.get('fbo_base') or 0), 2)
                _al = round((_kd.get('fbo_liter') or 0) + (_kr.get('fbo_liter') or 0) + (_kz.get('fbo_liter') or 0), 2)
                _logistics_tooltip_parts.append(f"Коледино: {_kd.get('fbo_base', 0):.2f} + {_kd.get('fbo_liter', 0):.2f}/л (коэфф. {_kd.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Краснодар: {_kr.get('fbo_base', 0):.2f} + {_kr.get('fbo_liter', 0):.2f}/л (коэфф. {_kr.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Казань: {_kz.get('fbo_base', 0):.2f} + {_kz.get('fbo_liter', 0):.2f}/л (коэфф. {_kz.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Среднее: {_ab/3:.2f} + {_al/3:.2f}/л")

        _logistics_tooltip_parts.append("")
        _logistics_tooltip_parts.append(f"Итого логистика: {_delivery_to_client:.2f} ₽")
        _logistics_tooltip = chr(10).join(_logistics_tooltip_parts)

        # Фильтр поиска
        if search_q and search_q not in str(nm_id) and search_q not in product_name.lower() and search_q not in vendor_code.lower():
            continue

        cost = cost_by_nm.get(nm_id, cost_by_entity.get(entity_id, {}))
        ue = ue_by_entity.get(entity_id, ue_by_nm_bc.get((nm_id, main_barcode), ue_by_nm_bc.get((nm_id, ""), {})))

        item = {
            "entity_id": entity_id,
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "product_name": product_name,
            "photo": photo.replace("/hq/", "/c246x328/").replace("/big/", "/c246x328/").replace("/tm/", "/c246x328/") if photo else "",
            "barcode": main_barcode,
            "size_name": _pe_size_name,
            "subject_name": _pe_subject_name or cost.get("subject_name", ""),
            "sku": f"{vendor_code}_{main_barcode}" if vendor_code else str(nm_id),

            # Из справочника / себестоимости
            # Себестоимость в Юните = Итого из справочника (cost_price + extra_costs)
            "cost_price": (float(cost.get("cost_price") or 0)) + (float(ue.get("extra_costs") or 0)),
            "purchase_cost": float(cost.get("purchase_cost") or 0),
            "logistics_cost": float(cost.get("logistics_cost") or 0),
            "packaging_cost": float(cost.get("packaging_cost") or 0),
            "other_costs": float(cost.get("other_costs") or 0),
            "product_class": cost.get("product_class"),
            "brand": cost.get("brand"),
            "tax_system": cost.get("tax_system"),
            "tax_rate": float(cost.get("tax_rate") or 0),
            "vat_rate": float(cost.get("vat_rate") or 0),

            # Из wb_tariff_snapshot (автоподтяжка)
            "mp_base_pct": (lambda _s=snap_by_nm.get(nm_id,{}), _u=ue, _p=p: (float(_u.get("mp_base_pct") or 0) if _u.get("mp_base_pct") else ((_s.get("commission_fbs_pct") if (_u.get("tariff_type") or "fbo") == "fbs" else _s.get("commission_pct")) or float(_p[8] or 0))))(),
            "buyout_fact_pct": snap_by_nm.get(nm_id, {}).get("buyout_pct_fact", 0),
            "logistics_tariff": _delivery_to_client,
            "reverse_logistics": _reverse_logistics,
            "logistics_actual": 0,  # Будет из финотчётов
            "storage_tariff": snap_by_nm.get(nm_id, {}).get("storage_tariff", 0),
            "storage_actual": 0,  # Будет из финотчётов
            "acceptance_avg": 0,  # Будет из API приёмки
            "price_before_spp": float(ue.get("wb_price_fact")) if ue.get("wb_price_fact") else (snap_by_nm.get(nm_id, {}).get("price_retail") or price),
            "spp_pct": (lambda _s=snap_by_nm.get(nm_id, {}): (
                round((1 - float(_s.get("price_with_spp", 0)) / float(_s.get("price_retail", 0))) * 100, 2)
                if _s.get("price_retail") and float(_s.get("price_retail", 0)) > 0
                and _s.get("price_with_spp") and float(_s.get("price_with_spp", 0)) > 0
                else 0
            ))(),
            "price_with_spp": float(snap_by_nm.get(nm_id, {}).get("price_with_spp", 0)) if snap_by_nm.get(nm_id, {}).get("price_with_spp") else 0,
            "ad_fact_pct": 0,  # заглушка, позже из финотчёта
            "ad_fact_rub": 0,  # заглушка, позже из финотчёта
            "wb_club_discount_pct_api": 0,

            # Из справочника
            "product_status": ue.get("product_status") or cost.get("product_status", ""),

            # Ручные вводы
            "mp_correction_pct": float(ue.get("mp_correction_pct") or 0),
            "buyout_niche_pct": float(ue.get("buyout_niche_pct") or 0),
            "extra_costs": 0,  # Уже включена в cost_price (Итого из справочника)
            "ad_plan_pct": min(99, max(0, float(ue.get("ad_plan_rub")) if ue.get("ad_plan_rub") not in (None, "", 0) else 5)),
            "ad_plan_rub": 0,  # рассчитывается ниже по цене
            "price_before_spp_plan": float(ue.get("price_before_spp_plan") or 0),
            "price_before_spp_change": float(ue.get("price_before_spp_change") or 0),
            "change_date": str(ue.get("change_date")) if ue.get("change_date") else None,
            "tariff_type": ue.get("tariff_type") or "box",
            "wb_club_discount_pct": float(ue.get("wb_club_discount_pct") or 0),

            # Логистика до клиента
            "volume_liters": _volume_liters,
            "volume_rounded": math.ceil(_volume_liters) if _volume_liters else 0,
            "fulfillment_model": _fulfillment_model,
            "fbs_warehouse": _fbs_warehouse or "",
            "delivery_to_client": _delivery_to_client,
            "logistics_tooltip": _logistics_tooltip,
        }

        items.append(apply_financial_formulas(item))

    # Сохраняем в Redis-кэш на 30 минут (ПОЛНЫЙ набор)
    _result_full = {"items": items, "total": len(items)}
    if not search:
        try:
            _redis.setex(_cache_key, 1800, _json.dumps(_result_full, ensure_ascii=False, default=str))
        except Exception:
            pass

    # Пагинация: если limit указан — обрезаем items
    if limit and limit > 0:
        return {"items": items[:limit], "total": len(items)}
    return _result_full


@router.get("/api/v1/nl/unit-economics")
async def get_unit_economics(
    org_id: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Юнит Экономика — только для участников организации."""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    return await build_unit_economics(org_id, db, search=search, limit=limit)


class UnitEconSave(BaseModel):
    nm_id: int
    barcode: Optional[str] = None
    entity_id: Optional[str] = None
    mp_correction_pct: Optional[float] = None
    buyout_niche_pct: Optional[float] = None
    extra_costs: Optional[float] = None
    ad_plan_rub: Optional[float] = None
    price_before_spp_plan: Optional[float] = None
    price_before_spp_change: Optional[float] = None
    change_date: Optional[str] = None
    tariff_type: Optional[str] = None
    wb_club_discount_pct: Optional[float] = None

class UnitEconBatchItem(BaseModel):
    nm_id: int
    barcode: Optional[str] = None
    entity_id: Optional[str] = None
    mp_correction_pct: Optional[float] = None
    buyout_niche_pct: Optional[float] = None
    extra_costs: Optional[float] = None
    ad_plan_rub: Optional[float] = None
    price_before_spp_plan: Optional[float] = None
    price_before_spp_change: Optional[float] = None
    change_date: Optional[str] = None
    tariff_type: Optional[str] = None
    wb_club_discount_pct: Optional[float] = None


class UnitEconBatchSave(BaseModel):
    items: list[UnitEconBatchItem]



@router.post("/api/v1/nl/unit-economics")
async def save_unit_economics(
    data: UnitEconSave,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Сохранить ручные вводы Юнит Экономики"""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    from models.reference_book import ReferenceBook
    from datetime import datetime as dt_mod

    change_date = date.today()

    # Определяем entity_id
    entity_id_ue = data.entity_id if hasattr(data, "entity_id") and data.entity_id else None
    if not entity_id_ue:
        from sqlalchemy import text as sql_text_sync
        ent_q = await db.execute(sql_text_sync(
            "SELECT pe.id FROM product_entities pe "
            "WHERE pe.organization_id = :org AND pe.nm_id = :nm "
            "ORDER BY CASE WHEN pe.size_name = :sz THEN 0 ELSE 1 END LIMIT 1"
        ), {"org": org_id, "nm": data.nm_id, "sz": data.barcode or ""})
        ent_row = ent_q.first()
        entity_id_ue = ent_row[0] if ent_row else None
    ins = pg_insert(ReferenceBook).values(
        organization_id=org_id,
        nm_id=data.nm_id,
        barcode=data.barcode,
        entity_id=entity_id_ue,
        valid_from=date.today(),
        mp_correction_pct=data.mp_correction_pct,
        buyout_niche_pct=data.buyout_niche_pct,
        extra_costs=data.extra_costs,
        ad_plan_rub=data.ad_plan_rub,
        price_before_spp_plan=data.price_before_spp_plan,
        price_before_spp_change=data.price_before_spp_change,
        change_date=date.today(),
        fulfillment_model=data.tariff_type or "fbo",
        wb_club_discount_pct=data.wb_club_discount_pct,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_book_org_nm_eid_vf_key",
        set_={
            "mp_correction_pct": ins.excluded.mp_correction_pct,
            "buyout_niche_pct": ins.excluded.buyout_niche_pct,
            "extra_costs": ins.excluded.extra_costs,
            "ad_plan_rub": ins.excluded.ad_plan_rub,
            "price_before_spp_plan": ins.excluded.price_before_spp_plan,
            "price_before_spp_change": ins.excluded.price_before_spp_change,
            "change_date": date.today(),
            "fulfillment_model": ins.excluded.fulfillment_model,
            "wb_club_discount_pct": ins.excluded.wb_club_discount_pct,
        }
    )
    await db.execute(stmt)
    await db.commit()
    try:
        import redis as _redis_lib
        _redis_lib.from_url("redis://redis:6379/0").delete(f"ue_cache:{org_id}")
    except Exception:
        pass
    return {"ok": True}


@router.post("/api/v1/nl/unit-economics/batch")
async def save_unit_economics_batch(
    payload: UnitEconBatchSave,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Пакетное сохранение ручных вводов Юнит Экономики"""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    from models.reference_book import ReferenceBook

    saved = 0
    for data in payload.items:
        entity_id_ue = data.entity_id if data.entity_id else None
        if not entity_id_ue:
            from sqlalchemy import text as sql_text_sync
            ent_q = await db.execute(sql_text_sync(
                "SELECT pe.id FROM product_entities pe "
                "WHERE pe.organization_id = :org AND pe.nm_id = :nm "
                "ORDER BY CASE WHEN pe.size_name = :sz THEN 0 ELSE 1 END LIMIT 1"
            ), {"org": org_id, "nm": data.nm_id, "sz": data.barcode or ""})
            ent_row = ent_q.first()
            entity_id_ue = ent_row[0] if ent_row else None

        ins = pg_insert(ReferenceBook).values(
            organization_id=org_id,
            nm_id=data.nm_id,
            barcode=data.barcode,
            entity_id=entity_id_ue,
            valid_from=date.today(),
            mp_correction_pct=data.mp_correction_pct,
            buyout_niche_pct=data.buyout_niche_pct,
            extra_costs=data.extra_costs,
            ad_plan_rub=data.ad_plan_rub,
            price_before_spp_plan=data.price_before_spp_plan,
            price_before_spp_change=data.price_before_spp_change,
            change_date=date.today(),
            fulfillment_model=data.tariff_type or "fbo",
            wb_club_discount_pct=data.wb_club_discount_pct,
        )
        stmt = ins.on_conflict_do_update(
            constraint="reference_book_org_nm_eid_vf_key",
            set_={
                "mp_correction_pct": ins.excluded.mp_correction_pct,
                "buyout_niche_pct": ins.excluded.buyout_niche_pct,
                "extra_costs": ins.excluded.extra_costs,
                "ad_plan_rub": ins.excluded.ad_plan_rub,
                "price_before_spp_plan": ins.excluded.price_before_spp_plan,
                "price_before_spp_change": ins.excluded.price_before_spp_change,
                "change_date": date.today(),
                "fulfillment_model": ins.excluded.fulfillment_model,
                "wb_club_discount_pct": ins.excluded.wb_club_discount_pct,
            }
        )
        await db.execute(stmt)
        saved += 1

    await db.commit()
    try:
        import redis as _redis_lib
        _redis_lib.from_url("redis://redis:6379/0").delete(f"ue_cache:{org_id}")
    except Exception:
        pass
    return {"ok": True, "saved": saved}




