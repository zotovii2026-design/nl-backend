"""Стратегии и вехи по артикулам."""

import json
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.strategy import StrategyDefinition, StrategyMilestone
from services.reference import resolve_org_id


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


STRATEGY_CATEGORIES = [
    {"key": "price", "label": "Цена", "color": "#0984e3"},
    {"key": "ads", "label": "Реклама", "color": "#e17055"},
    {"key": "seo", "label": "SEO", "color": "#00b894"},
    {"key": "infographic", "label": "Инфографика", "color": "#6c5ce7"},
    {"key": "main_photo", "label": "Главная картинка", "color": "#fdcb6e"},
    {"key": "slides", "label": "Доп. слайды", "color": "#00cec9"},
    {"key": "content", "label": "Наполнение", "color": "#636e72"},
]
_CATEGORY_KEYS = {c["key"] for c in STRATEGY_CATEGORIES}


def _date_or_none(value):
    if not value:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _text_or_none(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _strategy_row(row):
    def val(key):
        if hasattr(row, "get"):
            return row.get(key)
        return getattr(row, key)

    created_at = val("created_at")
    updated_at = val("updated_at")
    return {
        "id": str(val("id")),
        "category": val("category"),
        "code": val("code"),
        "title": val("title"),
        "description": val("description") or "",
        "default_executor": val("default_executor") or "",
        "role": val("role") or "",
        "status": val("status") or "active",
        "sort_order": val("sort_order"),
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


@router.get("/api/v1/nl/strategies/categories")
async def get_strategy_categories():
    return {"categories": STRATEGY_CATEGORIES}


@router.get("/api/v1/nl/strategies")
async def get_strategies(
    org_id: str,
    category: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    org_id = await resolve_org_id(org_id, db)
    params = {"org": org_id}
    where = "organization_id = :org"
    if category:
        where += " AND category = :category"
        params["category"] = category
    rows = await db.execute(
        text(
            f"""
            SELECT *
            FROM strategy_definitions
            WHERE {where}
            ORDER BY category, sort_order NULLS LAST, code, created_at
            """
        ),
        params,
    )
    return {"strategies": [_strategy_row(row) for row in rows.mappings().all()]}


@router.post("/api/v1/nl/strategies")
async def save_strategy(
    request: Request,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    org_id = await resolve_org_id(org_id, db)
    data = await request.json()
    category = _text_or_none(data.get("category"))
    code = _text_or_none(data.get("code"))
    title = _text_or_none(data.get("title"))
    if category not in _CATEGORY_KEYS:
        raise HTTPException(400, "Некорректное направление стратегии")
    if not code:
        raise HTTPException(400, "Номер стратегии обязателен")
    if not title:
        raise HTTPException(400, "Название стратегии обязательно")

    strategy_id = _text_or_none(data.get("id"))
    values = {
        "org": org_id,
        "category": category,
        "code": code,
        "title": title,
        "description": _text_or_none(data.get("description")),
        "default_executor": _text_or_none(data.get("default_executor")),
        "role": _text_or_none(data.get("role")),
        "status": _text_or_none(data.get("status")) or "active",
        "sort_order": data.get("sort_order"),
    }
    if strategy_id:
        values["id"] = strategy_id
        result = await db.execute(
            text(
                """
                UPDATE strategy_definitions
                SET category = :category,
                    code = :code,
                    title = :title,
                    description = :description,
                    default_executor = :default_executor,
                    role = :role,
                    status = :status,
                    sort_order = :sort_order,
                    updated_at = now()
                WHERE id = :id AND organization_id = :org
                RETURNING *
                """
            ),
            values,
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(404, "Стратегия не найдена")
    else:
        values["id"] = str(uuid.uuid4())
        result = await db.execute(
            text(
                """
                INSERT INTO strategy_definitions (
                    id, organization_id, category, code, title, description,
                    default_executor, role, status, sort_order
                )
                VALUES (
                    :id, :org, :category, :code, :title, :description,
                    :default_executor, :role, :status, :sort_order
                )
                ON CONFLICT ON CONSTRAINT strategy_definitions_org_category_code_key
                DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    default_executor = EXCLUDED.default_executor,
                    role = EXCLUDED.role,
                    status = EXCLUDED.status,
                    sort_order = EXCLUDED.sort_order,
                    updated_at = now()
                RETURNING *
                """
            ),
            values,
        )
        row = result.mappings().first()
    await db.commit()
    return {"ok": True, "strategy": _strategy_row(row)}


@router.delete("/api/v1/nl/strategies/{strategy_id}")
async def delete_strategy(strategy_id: str, org_id: str, db: AsyncSession = Depends(get_db)):
    org_id = await resolve_org_id(org_id, db)
    await db.execute(
        text(
            """
            DELETE FROM strategy_definitions
            WHERE id = :id AND organization_id = :org
            """
        ),
        {"id": strategy_id, "org": org_id},
    )
    await db.commit()
    return {"ok": True}


@router.get("/api/v1/nl/strategy-milestones/options")
async def get_milestone_options(org_id: str, db: AsyncSession = Depends(get_db)):
    org_id = await resolve_org_id(org_id, db)
    result = await db.execute(
        text(
            """
            WITH rb AS (
                SELECT DISTINCT ON (nm_id)
                       nm_id, product_status, product_class, brand
                FROM reference_book
                WHERE organization_id = :org
                  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
                ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
            ),
            pe AS (
                SELECT DISTINCT ON (nm_id)
                       nm_id, brand, subject_name
                FROM product_entities
                WHERE organization_id = :org
                ORDER BY nm_id, created_at DESC
            )
            SELECT
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(COALESCE(rb.brand, pe.brand, ''), '')), NULL) AS brands,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(COALESCE(pe.subject_name, ''), '')), NULL) AS subjects,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(COALESCE(rb.product_status, ''), '')), NULL) AS statuses,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(COALESCE(rb.product_class, ''), '')), NULL) AS classes
            FROM pe
            LEFT JOIN rb ON rb.nm_id = pe.nm_id
            """
        ),
        {"org": org_id},
    )
    row = result.mappings().first() or {}
    return {
        "brands": sorted(row.get("brands") or []),
        "subjects": sorted(row.get("subjects") or []),
        "statuses": sorted(row.get("statuses") or []),
        "classes": sorted(row.get("classes") or []),
        "categories": STRATEGY_CATEGORIES,
    }


@router.get("/api/v1/nl/strategy-milestones")
async def get_strategy_milestones(
    org_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
    strategy_id: Optional[str] = None,
    executor: Optional[str] = None,
    role: Optional[str] = None,
    product_status: Optional[str] = None,
    product_class: Optional[str] = None,
    brand: Optional[str] = None,
    subject: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    org_id = await resolve_org_id(org_id, db)
    d_to = _date_or_none(date_to) or date.today()
    d_from = _date_or_none(date_from) or (d_to - timedelta(days=30))
    params = {"org": org_id, "date_from": d_from, "date_to": d_to}

    # Фильтры уровня товара — применяются в WHERE к списку товаров
    product_filters = []
    # Фильтры уровня вех — применяются в LATERAL к выбору вехи
    ms_filters = ["sm.organization_id = :org", "sm.event_date BETWEEN :date_from AND :date_to"]

    if category:
        params["category"] = category
        ms_filters.append("sm.category = :category")
    if strategy_id:
        params["strategy_id"] = strategy_id
        ms_filters.append("sm.strategy_id = :strategy_id")
    if executor:
        params["executor"] = executor
        ms_filters.append("COALESCE(sm.executor, '') = :executor")
    if role:
        params["role"] = role
        ms_filters.append("COALESCE(sm.role, '') = :role")
    if product_status:
        params["product_status"] = product_status
        product_filters.append("COALESCE(rb.product_status, '') = :product_status")
    if product_class:
        params["product_class"] = product_class
        product_filters.append("COALESCE(rb.product_class, '') = :product_class")
    if brand:
        params["brand"] = brand
        product_filters.append("COALESCE(NULLIF(rb.brand, ''), pe.brand, '') = :brand")
    if subject:
        params["subject"] = subject
        product_filters.append("COALESCE(pe.subject_name, '') = :subject")
    if search:
        params["search"] = f"%{search.strip()}%"
        product_filters.append(
            """(
                pe.nm_id::text ILIKE :search
                OR COALESCE(pe.product_name, '') ILIKE :search
                OR COALESCE(pe.vendor_code, rb.vendor_code, '') ILIKE :search
            )"""
        )

    product_where = (" AND " + " AND ".join(product_filters)) if product_filters else ""

    result = await db.execute(
        text(
            f"""
            WITH pe AS (
                SELECT DISTINCT ON (nm_id)
                       id, nm_id, vendor_code, product_name, photo_main, brand, subject_name
                FROM product_entities
                WHERE organization_id = :org
                ORDER BY nm_id, created_at DESC
            ),
            rb AS (
                SELECT DISTINCT ON (nm_id)
                       nm_id, vendor_code, product_status, product_class, brand
                FROM reference_book
                WHERE organization_id = :org
                  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
                ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
            )
            SELECT
                lms.id,
                pe.nm_id,
                lms.event_date,
                lms.date_to,
                COALESCE(lms.category, '') AS category,
                lms.strategy_id,
                COALESCE(lms.strategy_code, '') AS strategy_code,
                COALESCE(sd.code, lms.strategy_code, '') AS code,
                COALESCE(sd.title, '') AS strategy_title,
                COALESCE(sd.description, '') AS strategy_description,
                COALESCE(lms.executor, sd.default_executor, '') AS executor,
                COALESCE(lms.role, sd.role, '') AS role,
                lms.source_links,
                COALESCE(lms.comment, '') AS comment,
                COALESCE(lms.result_note, '') AS result_note,
                COALESCE(pe.vendor_code, rb.vendor_code, '') AS vendor_code,
                COALESCE(pe.product_name, '') AS product_name,
                COALESCE(pe.photo_main, '') AS photo_main,
                COALESCE(NULLIF(rb.brand, ''), pe.brand, '') AS brand,
                COALESCE(pe.subject_name, '') AS subject_name,
                COALESCE(rb.product_status, '') AS product_status,
                COALESCE(rb.product_class, '') AS product_class
            FROM pe
            LEFT JOIN rb ON rb.nm_id = pe.nm_id
            LEFT JOIN LATERAL (
                SELECT sm.id, sm.event_date, sm.date_to, sm.category,
                       sm.strategy_id, sm.strategy_code, sm.executor, sm.role,
                       sm.source_links, sm.comment, sm.result_note
                FROM strategy_milestones sm
                WHERE sm.nm_id = pe.nm_id
                  AND {" AND ".join(ms_filters)}
                ORDER BY sm.event_date DESC
                LIMIT 1
            ) lms ON true
            LEFT JOIN strategy_definitions sd ON sd.id = lms.strategy_id
            WHERE 1=1{product_where}
            ORDER BY lms.event_date DESC NULLS LAST, pe.nm_id
            LIMIT 2000
            """
        ),
        params,
    )
    milestones = []
    for row in result.mappings().all():
        item = dict(row)
        item["id"] = str(item["id"]) if item.get("id") else None
        item["strategy_id"] = str(item["strategy_id"]) if item.get("strategy_id") else None
        item["event_date"] = item["event_date"].isoformat() if item.get("event_date") else None
        item["date_to"] = item["date_to"].isoformat() if item.get("date_to") else None
        item["source_links"] = item.get("source_links") or []
        milestones.append(item)
    return {"milestones": milestones, "date_from": d_from.isoformat(), "date_to": d_to.isoformat()}


@router.post("/api/v1/nl/strategy-milestones")
async def save_strategy_milestone(
    request: Request,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    org_id = await resolve_org_id(org_id, db)
    data = await request.json()
    nm_id = data.get("nm_id")
    try:
        nm_id = int(nm_id)
    except Exception:
        raise HTTPException(400, "Артикул WB обязателен")
    event_date = _date_or_none(data.get("event_date")) or date.today()
    category = _text_or_none(data.get("category"))
    if category not in _CATEGORY_KEYS:
        raise HTTPException(400, "Некорректное направление вехи")

    strategy_id = _text_or_none(data.get("strategy_id"))
    strategy_code = _text_or_none(data.get("strategy_code"))
    if strategy_id:
        srow = await db.execute(
            text(
                """
                SELECT id, category, code, default_executor, role
                FROM strategy_definitions
                WHERE id = :strategy_id AND organization_id = :org
                """
            ),
            {"strategy_id": strategy_id, "org": org_id},
        )
        strategy = srow.mappings().first()
        if not strategy:
            raise HTTPException(404, "Стратегия не найдена")
        category = strategy["category"]
        strategy_code = strategy["code"]

    ent = await db.execute(
        text(
            """
            SELECT id
            FROM product_entities
            WHERE organization_id = :org AND nm_id = :nm_id
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"org": org_id, "nm_id": nm_id},
    )
    entity_id = ent.scalar_one_or_none()
    raw_links = data.get("source_links") or []
    if isinstance(raw_links, str):
        raw_links = [line.strip() for line in raw_links.splitlines() if line.strip()]

    values = {
        "id": _text_or_none(data.get("id")) or str(uuid.uuid4()),
        "org": org_id,
        "entity_id": entity_id,
        "strategy_id": strategy_id,
        "nm_id": nm_id,
        "event_date": event_date,
        "date_to": _date_or_none(data.get("date_to")),
        "category": category,
        "strategy_code": strategy_code,
        "executor": _text_or_none(data.get("executor")),
        "role": _text_or_none(data.get("role")),
        "source_links": json.dumps(raw_links, ensure_ascii=False),
        "comment": _text_or_none(data.get("comment")),
        "result_note": _text_or_none(data.get("result_note")),
        "meta": json.dumps(data.get("meta") or {}, ensure_ascii=False),
    }
    exists = bool(_text_or_none(data.get("id")))
    if exists:
        result = await db.execute(
            text(
                """
                UPDATE strategy_milestones
                SET entity_id = :entity_id,
                    strategy_id = :strategy_id,
                    nm_id = :nm_id,
                    event_date = :event_date,
                    date_to = :date_to,
                    category = :category,
                    strategy_code = :strategy_code,
                    executor = :executor,
                    role = :role,
                    source_links = CAST(:source_links AS jsonb),
                    comment = :comment,
                    result_note = :result_note,
                    meta = CAST(:meta AS jsonb),
                    updated_at = now()
                WHERE id = :id AND organization_id = :org
                RETURNING id
                """
            ),
            values,
        )
        if not result.scalar_one_or_none():
            raise HTTPException(404, "Веха не найдена")
    else:
        await db.execute(
            text(
                """
                INSERT INTO strategy_milestones (
                    id, organization_id, entity_id, strategy_id, nm_id,
                    event_date, date_to, category, strategy_code, executor,
                    role, source_links, comment, result_note, meta
                )
                VALUES (
                    :id, :org, :entity_id, :strategy_id, :nm_id,
                    :event_date, :date_to, :category, :strategy_code, :executor,
                    :role, CAST(:source_links AS jsonb), :comment, :result_note, CAST(:meta AS jsonb)
                )
                """
            ),
            values,
        )
    await db.commit()
    return {"ok": True, "id": values["id"]}


@router.post("/api/v1/nl/strategy-milestones/batch")
async def batch_save_strategy_milestones(
    request: Request,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Массовое создание вех для нескольких товаров."""
    org_id = await resolve_org_id(org_id, db)
    data = await request.json()
    nm_ids = data.get("nm_ids") or []
    if not nm_ids:
        raise HTTPException(400, "Список артикулов пуст")
    nm_ids = [int(n) for n in nm_ids]

    event_date = _date_or_none(data.get("event_date")) or date.today()
    category = _text_or_none(data.get("category"))
    if category not in _CATEGORY_KEYS:
        raise HTTPException(400, "Некорректное направление вехи")

    strategy_id = _text_or_none(data.get("strategy_id"))
    strategy_code = _text_or_none(data.get("strategy_code"))
    if strategy_id:
        srow = await db.execute(
            text(
                """
                SELECT id, category, code, default_executor, role
                FROM strategy_definitions
                WHERE id = :strategy_id AND organization_id = :org
                """
            ),
            {"strategy_id": strategy_id, "org": org_id},
        )
        strategy = srow.mappings().first()
        if not strategy:
            raise HTTPException(404, "Стратегия не найдена")
        category = strategy["category"]
        strategy_code = strategy["code"]

    executor = _text_or_none(data.get("executor"))
    role_val = _text_or_none(data.get("role"))
    comment = _text_or_none(data.get("comment"))
    raw_links = data.get("source_links") or []
    if isinstance(raw_links, str):
        raw_links = [line.strip() for line in raw_links.splitlines() if line.strip()]
    links_json = json.dumps(raw_links, ensure_ascii=False)

    # Получаем entity_id для всех nm_ids
    ent_rows = await db.execute(
        text(
            """
            SELECT DISTINCT ON (nm_id) id, nm_id
            FROM product_entities
            WHERE organization_id = :org AND nm_id = ANY(:nm_ids)
            ORDER BY nm_id, created_at DESC
            """
        ),
        {"org": org_id, "nm_ids": nm_ids},
    )
    entity_map = {r.nm_id: r.id for r in ent_rows}

    created = []
    for nm_id in nm_ids:
        milestone_id = str(uuid.uuid4())
        values = {
            "id": milestone_id,
            "org": org_id,
            "entity_id": entity_map.get(nm_id),
            "strategy_id": strategy_id,
            "nm_id": nm_id,
            "event_date": event_date,
            "date_to": _date_or_none(data.get("date_to")),
            "category": category,
            "strategy_code": strategy_code,
            "executor": executor,
            "role": role_val,
            "source_links": links_json,
            "comment": comment,
            "result_note": None,
            "meta": "{}",
        }
        await db.execute(
            text(
                """
                INSERT INTO strategy_milestones (
                    id, organization_id, entity_id, strategy_id, nm_id,
                    event_date, date_to, category, strategy_code, executor,
                    role, source_links, comment, result_note, meta
                )
                VALUES (
                    :id, :org, :entity_id, :strategy_id, :nm_id,
                    :event_date, :date_to, :category, :strategy_code, :executor,
                    :role, CAST(:source_links AS jsonb), :comment, :result_note, CAST(:meta AS jsonb)
                )
                """
            ),
            values,
        )
        created.append(milestone_id)

    await db.commit()
    return {"ok": True, "created": len(created), "ids": created}


@router.get("/api/v1/nl/strategy-milestones/by-art/{nm_id}")
async def get_milestones_by_art(
    nm_id: int,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """История всех вех по конкретному товару."""
    org_id = await resolve_org_id(org_id, db)
    result = await db.execute(
        text(
            """
            SELECT
                sm.id, sm.nm_id, sm.event_date, sm.date_to, sm.category,
                sm.strategy_id, sm.strategy_code,
                COALESCE(sd.code, sm.strategy_code, '') AS code,
                COALESCE(sd.title, '') AS strategy_title,
                COALESCE(sm.executor, sd.default_executor, '') AS executor,
                COALESCE(sm.role, sd.role, '') AS role,
                sm.source_links,
                COALESCE(sm.comment, '') AS comment,
                COALESCE(sm.result_note, '') AS result_note
            FROM strategy_milestones sm
            LEFT JOIN strategy_definitions sd ON sd.id = sm.strategy_id
            WHERE sm.organization_id = :org AND sm.nm_id = :nm_id
            ORDER BY sm.event_date DESC, sm.created_at DESC
            """
        ),
        {"org": org_id, "nm_id": nm_id},
    )
    milestones = []
    for row in result.mappings().all():
        item = dict(row)
        item["id"] = str(item["id"])
        item["strategy_id"] = str(item["strategy_id"]) if item.get("strategy_id") else None
        item["event_date"] = item["event_date"].isoformat() if item.get("event_date") else None
        item["date_to"] = item["date_to"].isoformat() if item.get("date_to") else None
        item["source_links"] = item.get("source_links") or []
        milestones.append(item)
    return {"nm_id": nm_id, "milestones": milestones}


@router.delete("/api/v1/nl/strategy-milestones/{milestone_id}")
async def delete_strategy_milestone(milestone_id: str, org_id: str, db: AsyncSession = Depends(get_db)):
    org_id = await resolve_org_id(org_id, db)
    await db.execute(
        text(
            """
            DELETE FROM strategy_milestones
            WHERE id = :id AND organization_id = :org
            """
        ),
        {"id": milestone_id, "org": org_id},
    )
    await db.commit()
    return {"ok": True}
