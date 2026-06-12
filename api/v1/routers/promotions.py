import io
from typing import Optional

import openpyxl
from fastapi import APIRouter, Depends, File, UploadFile
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.promotion import WbPromotion, WbPromotionProduct


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


@router.get("/api/v1/nl/promotions")
async def get_promotions(
    org_id: str,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    """Return WB promotions for an organization."""
    query = select(WbPromotion).where(WbPromotion.organization_id == org_id)
    if is_active is not None:
        query = query.where(WbPromotion.is_active == is_active)
    query = query.order_by(WbPromotion.start_date.desc())
    result = await db.execute(query)
    promotions = result.scalars().all()
    return [
        {
            "id": str(promotion.id),
            "promotion_id": promotion.promotion_id,
            "title": promotion.title,
            "promo_type": promotion.promo_type,
            "start_date": (
                promotion.start_date.isoformat() if promotion.start_date else None
            ),
            "end_date": (
                promotion.end_date.isoformat() if promotion.end_date else None
            ),
            "max_price": (
                float(promotion.max_price) if promotion.max_price else None
            ),
            "min_discount": promotion.min_discount,
            "has_boost": promotion.has_boost,
            "boost_value": (
                float(promotion.boost_value) if promotion.boost_value else None
            ),
            "is_active": promotion.is_active,
            "importance": promotion.importance,
            "source": promotion.source,
        }
        for promotion in promotions
    ]


@router.get("/api/v1/nl/promotions/products")
async def get_promotion_products(
    org_id: str,
    promotion_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Build promotion product rows for the Tabulator frontend."""
    params = {"org": org_id}
    promo_filter = ""
    if promotion_id:
        promo_filter = " AND wp.promotion_id = :promo_id"
        params["promo_id"] = int(promotion_id)

    query = text(
        """
        SELECT
            pp.id,
            pp.wb_promotion_ext_id,
            pp.nm_id,
            pp.in_action,
            pp.auto_matched,
            pp.current_price,
            pp.required_price,
            pp.price_in_promo,
            pp.profit_in_promo,
            pp.margin_delta,
            pp.plan,
            pp.status_text,
            pp.entity_id,
            pe.vendor_code,
            pe.product_name,
            pe.photo_main as photo,
            pe.size_name,
            pe.brand,
            pe.subject_name,
            wp.title as promo_title,
            wp.promo_type,
            wp.start_date as promo_start,
            wp.end_date as promo_end,
            wp.importance as promo_importance,
            COALESCE(ts.price, pp.current_price) as price_before_spp,
            ts.stock_qty,
            rb.cost_price
        FROM wb_promotion_products pp
        LEFT JOIN product_entities pe ON pe.id = pp.entity_id
        LEFT JOIN wb_promotions wp ON wp.id = pp.promotion_id_col
        LEFT JOIN LATERAL (
            SELECT price, stock_qty
            FROM tech_status ts2
            WHERE ts2.organization_id = :org AND ts2.nm_id = pp.nm_id
            ORDER BY target_date DESC LIMIT 1
        ) ts ON true
        LEFT JOIN LATERAL (
            SELECT cost_price FROM reference_book rb2
            WHERE rb2.organization_id = :org AND rb2.entity_id = pp.entity_id
            ORDER BY valid_from DESC LIMIT 1
        ) rb ON true
        WHERE pp.organization_id = :org
        """
        + promo_filter
        + """
        ORDER BY pp.nm_id, pe.size_name
        """
    )
    result = await db.execute(query, params)

    items = []
    for row in result.all():
        price_before = (
            float(row.price_before_spp) if row.price_before_spp else None
        )
        cost = float(row.cost_price) if row.cost_price else None
        margin_pct = None
        if price_before and cost and price_before > 0:
            margin_pct = round((price_before - cost) / price_before * 100, 1)
        items.append(
            {
                "id": str(row.id),
                "wb_promotion_ext_id": row.wb_promotion_ext_id,
                "nm_id": row.nm_id,
                "in_action": row.in_action or False,
                "auto_matched": row.auto_matched or False,
                "current_price": (
                    float(row.current_price) if row.current_price else None
                ),
                "required_price": (
                    float(row.required_price) if row.required_price else None
                ),
                "price_in_promo": (
                    float(row.price_in_promo) if row.price_in_promo else None
                ),
                "profit_in_promo": (
                    float(row.profit_in_promo) if row.profit_in_promo else None
                ),
                "margin_delta": (
                    float(row.margin_delta) if row.margin_delta else None
                ),
                "plan": row.plan or False,
                "status_text": row.status_text,
                "entity_id": str(row.entity_id) if row.entity_id else None,
                "vendor_code": row.vendor_code,
                "product_name": row.product_name,
                "photo": row.photo,
                "size_name": row.size_name,
                "brand": row.brand,
                "subject_name": row.subject_name,
                "promo_title": row.promo_title,
                "promo_type": row.promo_type,
                "promo_start": (
                    row.promo_start.strftime("%d.%m.%Y")
                    if row.promo_start
                    else None
                ),
                "promo_end": (
                    row.promo_end.strftime("%d.%m.%Y") if row.promo_end else None
                ),
                "promo_importance": row.promo_importance,
                "price_before_spp": price_before,
                "stock_qty": row.stock_qty,
                "margin_pct": margin_pct,
            }
        )
    return {"items": items}


class PromoProductSave(BaseModel):
    items: list


@router.post("/api/v1/nl/promotions/products/save")
async def save_promotion_products(
    data: PromoProductSave,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Save manual promotion fields."""
    saved = 0
    for item in data.items:
        row_id = item.get("id")
        if not row_id:
            continue
        query = select(WbPromotionProduct).where(
            WbPromotionProduct.id == row_id,
            WbPromotionProduct.organization_id == org_id,
        )
        result = await db.execute(query)
        promotion_product = result.scalar_one_or_none()
        if not promotion_product:
            continue
        if "plan" in item:
            promotion_product.plan = item["plan"]
        if "price_in_promo" in item:
            promotion_product.price_in_promo = item["price_in_promo"]
        saved += 1
    await db.commit()
    return {"ok": True, "saved": saved}


@router.post("/api/v1/nl/promotions/upload-excel")
async def upload_promo_excel(
    org_id: str,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a WB promotion Excel template."""
    content = await file.read()
    workbook = openpyxl.load_workbook(io.BytesIO(content))
    worksheet = workbook.active

    col_map = {}
    header_row = 1
    for col_idx in range(1, worksheet.max_column + 1):
        value = worksheet.cell(row=header_row, column=col_idx).value
        if value:
            value_lower = str(value).strip().lower()
            if "уже участв" in value_lower:
                col_map["in_action"] = col_idx
            elif "бренд" in value_lower:
                col_map["brand"] = col_idx
            elif "предмет" in value_lower:
                col_map["subject"] = col_idx
            elif "наименован" in value_lower:
                col_map["name"] = col_idx
            elif "артикул постав" in value_lower:
                col_map["vendor_code"] = col_idx
            elif "артикул wb" in value_lower or (
                "артикул продавца" in value_lower and "wb" in value_lower
            ):
                col_map["nm_id"] = col_idx
            elif "баркод" in value_lower or "штрих" in value_lower:
                col_map["barcode"] = col_idx
            elif "оборач" in value_lower:
                col_map["turnover"] = col_idx
            elif "остаток" in value_lower:
                col_map["stock"] = col_idx
            elif "плановая цена" in value_lower or "план. цена" in value_lower:
                col_map["planned_price"] = col_idx
            elif "текущая цена" in value_lower:
                col_map["current_price"] = col_idx
            elif "минимальная цена" in value_lower or "мин. цена" in value_lower:
                col_map["min_price"] = col_idx
            elif "текущая скидка" in value_lower:
                col_map["current_discount"] = col_idx
            elif "загружаемая скидка" in value_lower or "загружаем" in value_lower:
                col_map["upload_discount"] = col_idx
            elif "статус" in value_lower:
                col_map["status"] = col_idx

    nm_col = col_map.get("nm_id")
    if not nm_col:
        for col_idx in range(1, worksheet.max_column + 1):
            value = worksheet.cell(row=header_row, column=col_idx).value
            if value and (
                "артикул" in str(value).lower() and "wb" in str(value).lower()
            ):
                nm_col = col_idx
                break

    filename = file.filename or "upload"
    promo_title = filename.replace(".xlsx", "").replace(".xls", "")
    new_promotion = WbPromotion(
        organization_id=org_id,
        promotion_id=abs(hash(promo_title)) % 1000000,
        title=promo_title,
        promo_type="excel",
        is_active=True,
        source="excel",
    )
    db.add(new_promotion)
    await db.flush()

    count = 0
    for row_idx in range(2, worksheet.max_row + 1):
        nm_value = (
            worksheet.cell(row=row_idx, column=nm_col).value if nm_col else None
        )
        if not nm_value:
            continue
        try:
            nm_id = int(nm_value)
        except (ValueError, TypeError):
            continue

        entity_result = await db.execute(
            text(
                "SELECT id FROM product_entities "
                "WHERE organization_id = :org AND nm_id = :nm LIMIT 1"
            ),
            {"org": org_id, "nm": nm_id},
        )
        entity_row = entity_result.first()
        entity_id = entity_row[0] if entity_row else None

        def cell_value(column_key):
            column = col_map.get(column_key)
            return (
                worksheet.cell(row=row_idx, column=column).value
                if column
                else None
            )

        in_action = cell_value("in_action")
        if isinstance(in_action, str):
            in_action = in_action.strip().upper() in ("ДА", "YES", "1", "+")
        elif isinstance(in_action, (int, float)):
            in_action = bool(in_action)

        existing = await db.execute(
            text(
                "SELECT id FROM wb_promotion_products "
                "WHERE organization_id = :org "
                "AND wb_promotion_ext_id = :ext_id AND nm_id = :nm"
            ),
            {
                "org": org_id,
                "ext_id": new_promotion.promotion_id,
                "nm": nm_id,
            },
        )
        existing_row = existing.first()

        if existing_row:
            await db.execute(
                text(
                    "UPDATE wb_promotion_products "
                    "SET in_action = :ia, status_text = :st, "
                    "current_price = :cp, entity_id = :eid, "
                    "promotion_id_col = :pid, updated_at = now() "
                    "WHERE id = :rid"
                ),
                {
                    "ia": in_action,
                    "st": str(cell_value("status") or "")[:200],
                    "cp": cell_value("current_price"),
                    "eid": entity_id,
                    "pid": str(new_promotion.id),
                    "rid": existing_row[0],
                },
            )
        else:
            await db.execute(
                text(
                    "INSERT INTO wb_promotion_products "
                    "(id, organization_id, promotion_id_col, "
                    "wb_promotion_ext_id, nm_id, entity_id, in_action, "
                    "current_price, status_text, created_at) "
                    "VALUES (gen_random_uuid(), :org, :pid, :ext_id, :nm, "
                    ":eid, :ia, :cp, :st, now())"
                ),
                {
                    "org": org_id,
                    "pid": str(new_promotion.id),
                    "ext_id": new_promotion.promotion_id,
                    "nm": nm_id,
                    "eid": entity_id,
                    "ia": in_action,
                    "cp": cell_value("current_price"),
                    "st": str(cell_value("status") or "")[:200],
                },
            )
        count += 1

    await db.commit()
    return {
        "ok": True,
        "count": count,
        "promotion_id": new_promotion.promotion_id,
    }


@router.post("/api/v1/nl/promotions/sync-api")
async def sync_promo_api(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Start the existing WB promotion synchronization task."""
    from tasks.promo_sync import do_promo_sync

    result = do_promo_sync.delay()
    return {"status": "started", "task_id": result.id}
