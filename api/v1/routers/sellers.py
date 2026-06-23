"""Sellers and SEO keywords API routes."""
from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

from core.tenant_auth import require_query_organization_access
from core.database import get_db

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)

@router.get("/api/v1/nl/sellers")
async def get_sellers(org_id: str, db: AsyncSession = Depends(get_db)):
    """Список продавцов"""
    from sqlalchemy import text
    result = await db.execute(text(
        "SELECT id, seller_id, seller_name, inn, seller_type, contact_name, "
        "contact_email, contact_phone, role, is_active, notes, created_at "
        "FROM sellers WHERE organization_id = :org ORDER BY created_at DESC"
    ), {"org": org_id})
    return [{"id": str(r[0]), "seller_id": r[1], "seller_name": r[2], "inn": r[3],
             "seller_type": r[4], "contact_name": r[5], "contact_email": r[6],
             "contact_phone": r[7], "role": r[8], "is_active": r[9],
             "notes": r[10], "created_at": str(r[11])} for r in result.all()]


@router.post("/api/v1/nl/sellers")
async def add_seller(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить продавца"""
    from sqlalchemy import text
    await db.execute(text(
        "INSERT INTO sellers (organization_id, seller_id, seller_name, inn, seller_type, "
        "contact_name, contact_email, contact_phone, role, notes) "
        "VALUES (:org, :sid, :name, :inn, :type, :cname, :email, :phone, :role, :notes)"
    ), {"org": org_id, "sid": data.get("seller_id"), "name": data.get("seller_name"),
        "inn": data.get("inn"), "type": data.get("seller_type", "fbo"),
        "cname": data.get("contact_name"), "email": data.get("contact_email"),
        "phone": data.get("contact_phone"), "role": data.get("role", "seller"),
        "notes": data.get("notes")})
    await db.commit()
    return {"ok": True}


@router.get("/api/v1/nl/seo-keywords")
async def get_seo_keywords(org_id: str, nm_id: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """SEO ключевые запросы"""
    from sqlalchemy import text
    q = "SELECT id, nm_id, vendor_code, keyword, position, frequency_monthly, frequency_weekly, "         "season_start, season_end, season_multiplier, trend, trend_value, competition, "         "target_date, source, notes FROM seo_keywords WHERE organization_id = :org"
    params = {"org": org_id}
    if nm_id:
        q += " AND nm_id = :nm"
        params["nm"] = int(nm_id)
    q += " ORDER BY target_date DESC NULLS LAST, frequency_monthly DESC NULLS LAST"
    result = await db.execute(text(q), params)
    return [{"id": str(r[0]), "nm_id": r[1], "vendor_code": r[2], "keyword": r[3],
             "position": r[4], "frequency_monthly": r[5], "frequency_weekly": r[6],
             "season_start": r[7], "season_end": r[8],
             "season_multiplier": float(r[9]) if r[9] else 1.0,
             "trend": r[10], "trend_value": float(r[11]) if r[11] else None,
             "competition": r[12], "target_date": str(r[13]) if r[13] else None,
             "source": r[14], "notes": r[15]} for r in result.all()]


@router.post("/api/v1/nl/seo-keywords")
async def add_seo_keyword(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить SEO запрос"""
    from sqlalchemy import text
    await db.execute(text(
        "INSERT INTO seo_keywords (organization_id, nm_id, vendor_code, keyword, position, "
        "frequency_monthly, frequency_weekly, season_start, season_end, season_multiplier, "
        "trend, trend_value, competition, target_date, source, notes) "
        "VALUES (:org, :nm, :vc, :kw, :pos, :fm, :fw, :ss, :se, :sm, :trend, :tv, :comp, :td, :src, :notes)"
    ), {"org": org_id, "nm": data.get("nm_id"), "vc": data.get("vendor_code"),
        "kw": data.get("keyword"), "pos": data.get("position"),
        "fm": data.get("frequency_monthly"), "fw": data.get("frequency_weekly"),
        "ss": data.get("season_start"), "se": data.get("season_end"),
        "sm": data.get("season_multiplier", 1.0), "trend": data.get("trend"),
        "tv": data.get("trend_value"), "comp": data.get("competition"),
        "td": data.get("target_date"), "src": data.get("source", "manual"),
        "notes": data.get("notes")})
    await db.commit()
    return {"ok": True}


@router.post("/api/v1/nl/seo-keywords/upload")
async def upload_seo_keywords(org_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    """Загрузка SEO запросов из Excel/CSV"""
    import io, csv
    from sqlalchemy import text
    body = await request.body()
    filename = request.headers.get("x-filename", "upload.csv")
    rows = []
    if filename.endswith(".csv"):
        rows = list(csv.DictReader(io.StringIO(body.decode("utf-8-sig")), delimiter=";"))
    updated = 0
    for row in rows:
        nm = row.get("Арт WB") or row.get("nm_id")
        kw = row.get("Запрос") or row.get("keyword")
        if nm and kw:
            await db.execute(text(
                "INSERT INTO seo_keywords (organization_id, nm_id, vendor_code, keyword, "
                "position, frequency_monthly, season_start, season_end, trend, competition, target_date, source) "
                "VALUES (:org, :nm, :vc, :kw, :pos, :fm, :ss, :se, :trend, :comp, CURRENT_DATE, 'excel')"
            ), {"org": org_id, "nm": int(nm), "vc": row.get("Арт продавца",""),
                "kw": kw, "pos": row.get("Позиция"), "fm": row.get("Частотность"),
                "ss": row.get("Сезон начало"), "se": row.get("Сезон конец"),
                "trend": row.get("Тренд"), "comp": row.get("Конкуренция")})
            updated += 1
    await db.commit()
    return {"updated": updated, "total": len(rows)}



