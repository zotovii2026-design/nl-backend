"""SQL-запросы для Справочника"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List, Dict, Any


# ============================================================================
# GET reference (короткий формат)
# ============================================================================
GET_REFERENCE_SQL = text(
    "SELECT nm_id, vendor_code, cost_price, purchase_cost as purchase_price, packaging_cost, "
    "logistics_cost, other_costs, notes, product_class, brand, tax_system, tax_rate, vat_rate, "
    "transport_pack_qty, valid_from FROM reference_book "
    "WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
)


async def fetch_reference(db: AsyncSession, org_id: str, target_date: Optional[str] = None) -> List[Dict]:
    sql = (
        "SELECT nm_id, vendor_code, cost_price, purchase_cost as purchase_price, packaging_cost, "
        "logistics_cost, other_costs, notes, product_class, brand, tax_system, tax_rate, vat_rate, "
        "transport_pack_qty, valid_from FROM reference_book "
        "WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
    )
    params: Dict[str, Any] = {"org": org_id}
    if target_date:
        sql += " AND valid_from <= :td"
        from datetime import datetime as dt
        params["td"] = dt.strptime(target_date, "%Y-%m-%d").date()
    sql += " ORDER BY nm_id, valid_from DESC"
    result = await db.execute(text(sql), params)
    return [{
        "nm_id": r[0],
        "vendor_code": r[1],
        "target_date": str(r[14]),
        "cost_price": float(r[2]) if r[2] else None,
        "purchase_price": float(r[3]) if r[3] else None,
        "packaging_cost": float(r[4]) if r[4] else None,
        "logistics_cost": float(r[5]) if r[5] else None,
        "other_costs": float(r[6]) if r[6] else None,
        "notes": r[7],
        "product_class": r[8],
        "brand": r[9],
        "tax_system": r[10],
        "tax_rate": float(r[11]) if r[11] else None,
        "vat_rate": float(r[12]) if r[12] else None,
        "transport_pack_qty": int(r[13]) if r[13] else 1,
    } for r in result.all()]


# ============================================================================
# GET cost-prices (полный справочник с entity)
# ============================================================================
COST_PRICES_SQL = text(
    "SELECT pe.id as entity_id, pe.nm_id, pe.size_name, pe.vendor_code, COALESCE(cp.brand, pe.brand) as brand, "
    "pe.subject_id, pe.subject_name, pe.length, pe.width, pe.height, pe.weight, "
    "(SELECT string_agg(eb.barcode, ', ') FROM entity_barcodes eb WHERE eb.entity_id = pe.id AND eb.is_active = true) as barcodes, "
    "cp.id as ref_id, cp.cost_price, cp.purchase_cost, cp.logistics_cost, cp.packaging_cost, "
    "cp.other_costs, cp.extra_costs, cp.vat, cp.min_price, "
    "cp.mp_base_pct, cp.mp_correction_pct, cp.fulfillment_model, cp.storage_pct, "
    "cp.buyout_niche_pct, cp.price_before_spp_plan, cp.price_before_spp_change, "
    "cp.change_date, cp.wb_club_discount_pct, cp.ad_plan_rub, cp.supply_days, "
    "cp.min_batch_fbo, cp.transport_pack_qty, cp.product_status, cp.valid_from, cp.notes, "
    "cp.product_class, cp.tax_system, cp.tax_rate, "
    "cp.season_jan, cp.season_feb, cp.season_mar, cp.season_apr, cp.season_may, cp.season_jun, "
    "cp.season_jul, cp.season_aug, cp.season_sep, cp.season_oct, cp.season_nov, cp.season_dec, "
    "cp.plan_length, cp.plan_width, cp.plan_height, cp.plan_volume, cp.plan_weight, "
    "cp.delivery_days_to_seller, cp.delivery_days_to_mp, "
    "cp.top_query_1, cp.top_query_2, cp.top_query_3, "
    "cp.shipment_method, cp.fbs_warehouse, cp.rrc_price, cp.vat_rate, "
    "ts.product_name "
    "FROM product_entities pe "
    "LEFT JOIN LATERAL (SELECT * FROM reference_book WHERE organization_id = :org AND nm_id = pe.nm_id AND entity_id = pe.id "
    "  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE) ORDER BY valid_from DESC LIMIT 1) cp ON true "
    "LEFT JOIN (SELECT DISTINCT nm_id, product_name FROM tech_status WHERE organization_id = :org) ts ON pe.nm_id = ts.nm_id "
    "WHERE pe.organization_id = :org "
    "ORDER BY pe.nm_id, pe.size_name"
)


async def fetch_cost_prices(db: AsyncSession, org_id: str) -> List[Dict]:
    result = await db.execute(COST_PRICES_SQL, {"org": org_id})
    def fval(v): return float(v) if v else None
    def ival(v): return int(v) if v else None
    def sval(v): return str(v) if v else None
    return [{
        "id": sval(r[12]) or str(r[0]),
        "entity_id": str(r[0]),
        "nm_id": r[1],
        "size_name": r[2] or "",
        "vendor_code": r[3] or "",
        "brand": r[4] or "",
        "subject_id": r[5], "subject_name": r[6] or "",
        "length": fval(r[7]), "width": fval(r[8]), "height": fval(r[9]),
        "weight": fval(r[10]),
        "barcode": r[11] or "",
        "barcodes": r[11] or "",
        "cost_price": fval(r[13]) or 0,
        "purchase_cost": fval(r[14]),
        "logistics_cost": fval(r[15]),
        "packaging_cost": fval(r[16]),
        "other_costs": fval(r[17]),
        "extra_costs": fval(r[18]),
        "vat": fval(r[19]) or 0,
        "min_price": fval(r[20]),
        "mp_base_pct": fval(r[21]),
        "mp_correction_pct": fval(r[22]),
        "fulfillment_model": r[23] or "fbo",
        "storage_pct": fval(r[24]),
        "buyout_niche_pct": fval(r[25]),
        "price_before_spp_plan": fval(r[26]),
        "price_before_spp_change": fval(r[27]),
        "change_date": sval(r[28]),
        "wb_club_discount_pct": fval(r[29]),
        "ad_plan_rub": fval(r[30]),
        "supply_days": r[31],
        "min_batch_fbo": r[32],
        "transport_pack_qty": ival(r[33]) or 1,
        "product_status": r[34] or "",
        "valid_from": sval(r[35]) or "",
        "notes": r[36] or "",
        "product_class": r[37] or "",
        "tax_system": r[38] or "",
        "tax_rate": fval(r[39]) or 0,
        "season_jan": fval(r[40]), "season_feb": fval(r[41]),
        "season_mar": fval(r[42]), "season_apr": fval(r[43]),
        "season_may": fval(r[44]), "season_jun": fval(r[45]),
        "season_jul": fval(r[46]), "season_aug": fval(r[47]),
        "season_sep": fval(r[48]), "season_oct": fval(r[49]),
        "season_nov": fval(r[50]), "season_dec": fval(r[51]),
        "plan_length": fval(r[52]), "plan_width": fval(r[53]),
        "plan_height": fval(r[54]), "plan_volume": fval(r[55]),
        "plan_weight": fval(r[56]),
        "delivery_days_to_seller": ival(r[57]), "delivery_days_to_mp": ival(r[58]),
        "top_query_1": r[59] or "", "top_query_2": r[60] or "", "top_query_3": r[61] or "",
        "shipment_method": r[62] or "", "fbs_warehouse": r[63] or "",
        "rrc_price": fval(r[64]), "vat_rate": fval(r[65]) or 0,
        "product_name": r[66] or "",
        "sizes": [],
    } for r in result.all()]
