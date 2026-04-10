from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import List, Optional
from datetime import datetime, timedelta
from core.database import get_db
from models.wb_data import WbProduct, WbSale
from pydantic import BaseModel

router = APIRouter()


# Модели для ответа
class ProductBase(BaseModel):
    id: str
    nm_id: int
    vendor_code: str
    name: str
    brand: str
    subject: str
    price: float
    discount: Optional[int] = None
    stock: Optional[int] = None

class SaleBase(BaseModel):
    id: str
    sale_id: str
    date_from: datetime
    date_to: datetime
    income: float
    brand: str
    subject: str
    quantity: Optional[int] = None
    total_price: Optional[float] = None
    nm_id: Optional[int] = None
    region_name: Optional[str] = None

class SalesStats(BaseModel):
    total_sales: int
    total_income: float
    avg_order_value: float
    top_products: List[dict]


@router.get("/products", response_model=List[ProductBase])
async def get_products(
    organization_id: str = "cfbe3b49-734c-43bc-a8cf-27f40d13940b",
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """Получить список товаров"""
    result = await db.execute(
        select(WbProduct)
        .where(WbProduct.organization_id == organization_id)
        .limit(limit)
        .offset(offset)
    )
    products = result.scalars().all()
    return [
        ProductBase(
            id=str(p.id),
            nm_id=p.nm_id,
            vendor_code=p.vendor_code or "",
            name=p.name,
            brand=p.brand or "",
            subject=p.subject or "",
            price=float(p.price) if p.price else 0,
            discount=p.discount,
            stock=p.stock
        )
        for p in products
    ]


@router.get("/sales", response_model=List[SaleBase])
async def get_sales(
    organization_id: str = "cfbe3b49-734c-43bc-a8cf-27f40d13940b",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """Получить продажи за период"""
    query = select(WbSale).where(WbSale.organization_id == organization_id)
    
    if date_from:
        query = query.where(WbSale.date_to >= datetime.fromisoformat(date_from))
    if date_to:
        query = query.where(WbSale.date_to <= datetime.fromisoformat(date_to))
    
    query = query.order_by(WbSale.date_to.desc()).limit(limit)
    
    result = await db.execute(query)
    sales = result.scalars().all()
    
    return [
        SaleBase(
            id=str(s.id),
            sale_id=s.sale_id,
            date_from=s.date_from,
            date_to=s.date_to,
            income=float(s.income) if s.income else 0,
            brand=s.brand or "",
            subject=s.subject or "",
            quantity=s.quantity,
            total_price=float(s.total_price) if s.total_price else 0,
            nm_id=s.nm_id,
            region_name=s.region_name
        )
        for s in sales
    ]


@router.get("/stats", response_model=SalesStats)
async def get_sales_stats(
    organization_id: str = "cfbe3b49-734c-43bc-a8cf-27f40d13940b",
    days: int = 7,
    db: AsyncSession = Depends(get_db)
):
    """Получить статистику продаж за N дней"""
    date_threshold = datetime.now() - timedelta(days=days)
    
    # Общая статистика
    result = await db.execute(
        select(
            func.count(WbSale.id).label('total_sales'),
            func.sum(WbSale.income).label('total_income'),
            func.avg(WbSale.total_price).label('avg_order_value')
        )
        .where(
            and_(
                WbSale.organization_id == organization_id,
                WbSale.date_to >= date_threshold
            )
        )
    )
    stats = result.one()
    
    # Топ товаров по выручке
    result = await db.execute(
        select(
            WbSale.nm_id,
            func.count(WbSale.id).label('sales_count'),
            func.sum(WbSale.income).label('total_income')
        )
        .where(
            and_(
                WbSale.organization_id == organization_id,
                WbSale.date_to >= date_threshold
            )
        )
        .group_by(WbSale.nm_id)
        .order_by(func.sum(WbSale.income).desc())
        .limit(5)
    )
    top_products = []
    for row in result:
        product_result = await db.execute(
            select(WbProduct).where(WbProduct.nm_id == row.nm_id)
        )
        product = product_result.scalar_one_or_none()
        top_products.append({
            "nm_id": row.nm_id,
            "name": product.name if product else "Unknown",
            "sales_count": row.sales_count,
            "total_income": float(row.total_income) if row.total_income else 0
        })
    
    return SalesStats(
        total_sales=stats.total_sales or 0,
        total_income=float(stats.total_income) if stats.total_income else 0,
        avg_order_value=float(stats.avg_order_value) if stats.avg_order_value else 0,
        top_products=top_products
    )
