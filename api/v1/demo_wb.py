from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import httpx
from core.database import get_db
from pydantic import BaseModel

router = APIRouter()


# WB API Key (токен из файла)
WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzkxNTAwNjY2LCJpZCI6IjAxOWQ3MWVhLTJmZTgtNzExYS05ZDI5LTU5Zjc3NjIzZjczMSIsImlpZCI6MjczNDYyMTcsIm9pZCI6NDIzNDM2MCwicyI6MTA3Mzc1Nzk1MCwic2lkIjoiN2RmOTdhZTQtYjk5YS00Zjk5LTliZWUtYTQyMDhhNjllZGM0IiwidCI6ZmFsc2UsInVpZCI6MjczNDYyMTd9._kt1wno7M2w9aIRJ1HCuCcM6WjjjXAa9ufYH4Zgyxiq0pMupJbqnxWp7r1ismzd9918aSfnc5s75DXLZWwaofw"


# Models for response
class ProductWB(BaseModel):
    nm_id: int
    vendor_code: str
    title: str
    brand: str
    subject_name: str
    subject_id: int
    price: Optional[float] = None
    stock: Optional[int] = None
    photos: List[str]
    need_kiz: bool
    kiz_marked: bool
    created_at: str
    updated_at: str
    sizes: List[dict]


class SalesStats(BaseModel):
    total_sales: int
    total_income: float
    avg_order_value: float
    top_products: List[dict]


async def get_wb_cards(search: str = None, limit: int = 100) -> List[dict]:
    """Получить карточки из WB API с фильтром withPhoto: -1"""
    async with httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {WB_API_KEY}",
            "Content-Type": "application/json"
        },
        timeout=30.0
    ) as client:
        payload = {
            "settings": {
                "cursor": {"limit": limit},
                "filter": {"withPhoto": -1}
            }
        }
        
        if search:
            payload["settings"]["filter"]["textSearch"] = search
        
        response = await client.post(
            "https://content-api.wildberries.ru/content/v2/get/cards/list",
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        return result.get("cards", [])


@router.get("/products", response_model=List[ProductWB])
async def get_wb_products(
    search: Optional[str] = Query(None, description="Поиск по названию или артикулу"),
    limit: int = Query(100, description="Лимит карточек")
):
    """Получить список товаров из WB API"""
    try:
        cards = await get_wb_cards(search=search, limit=limit)
        
        products = []
        for card in cards:
            # Получаем главное фото (первое в списке)
            main_photo_url = ""
            if card.get("photos") and len(card["photos"]) > 0:
                # Используем mini размер для быстрой загрузки
                main_photo_url = card["photos"][0].get("tm", card["photos"][0].get("big", ""))
            
            # Получаем цену из размеров
            price = 0
            if card.get("sizes") and len(card["sizes"]) > 0:
                price = card["sizes"][0].get("price", 0) or 0
            
            products.append(ProductWB(
                nm_id=card.get("nmID", 0),
                vendor_code=card.get("vendorCode", ""),
                title=card.get("title", ""),
                brand=card.get("brand", ""),
                subject_name=card.get("subjectName", ""),
                subject_id=card.get("subjectID", 0),
                price=float(price),
                stock=1,  # Симулируем остаток = 1 для демо
                photos=[main_photo_url],
                need_kiz=card.get("needKiz", False),
                kiz_marked=card.get("kizMarked", False),
                created_at=card.get("createdAt", ""),
                updated_at=card.get("updatedAt", ""),
                sizes=card.get("sizes", [])
            ))
        
        return products
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching WB cards: {str(e)}")


@router.get("/sales", response_model=List[dict])
async def get_wb_sales(
    date_from: Optional[str] = Query(None, description="Дата начала (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Дата конца (YYYY-MM-DD)"),
    limit: int = Query(100, description="Лимит продаж")
):
    """Получить продажи из WB API (симуляция для демо)"""
    # Симуляция продаж для демо
    sales = [
        {
            "id": 1,
            "sale_id": "12345",
            "date_from": "2026-04-09T00:00:00",
            "date_to": "2026-04-09T23:59:59",
            "income": 2500.0,
            "brand": "MV clothes",
            "subject": "Куртки горнолыжные",
            "quantity": 1,
            "total_price": 2500.0,
            "nm_id": 264418015,
            "region_name": "Москва"
        }
    ]
    
    return sales


@router.get("/stats", response_model=SalesStats)
async def get_wb_stats(days: int = Query(7, description="Кол-во дней для статистики")):
    """Получить статистику продаж (симуляция для демо)"""
    return SalesStats(
        total_sales=5,
        total_income=12500.0,
        avg_order_value=2500.0,
        top_products=[
            {
                "nm_id": 264418015,
                "name": "Куртка горнолыжная мембрана",
                "sales_count": 1,
                "total_income": 2500.0
            }
        ]
    )
