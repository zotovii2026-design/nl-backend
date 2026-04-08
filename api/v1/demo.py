from fastapi import APIRouter, HTTPException, status
from services.wb_api.client import WBApiClient

router = APIRouter(prefix="/demo", tags=["Demo"])

# WB API ключ для тестирования (Personal token - acc=3)
DEMO_WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjMsImVudCI6MSwiZXhwIjoxNzkxNDQ3MDMzLCJmb3IiOiJzZWxmIiwiaWQiOiIwMTlkNmViNy1jZmQxLTc2N2ItOWJiMC0zYzFhZWY4NzgxODkiLCJpaWQiOjczNDYyMTcsIm9pZCI6NDIzNDM2MCwicyI6ODE2Miwic2lkIjoiN2RmOTdhZTQtYjk5YS00Zjk5LTliZWUtYTQyMDhhNjllZGM0IiwidCI6ZmFsc2UsInVpZCI6MjczNDYyMTd9.FMKZ7vXuZpKN5ArqTPQDAbBn5AVJcdzclwce_ra2Jo5INeMK_IQyrOg8TIoaus9eUg__9IX6T7IM7UjkBXhO0A"

# WB API URL (устаревший, для совместимости)
DEMO_WB_API_URL = "https://suppliers-api.wildberries.ru"

@router.get("/products")
async def get_demo_products():
    """Получить товары WB (demo endpoint)"""
    try:
        client = WBApiClient(api_key=DEMO_WB_API_KEY)
        result = await client.get_products()
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"WB API error: {str(e)}"
        )

@router.get("/sales")
async def get_demo_sales():
    """Получить продажи WB (demo endpoint)"""
    try:
        client = WBApiClient(api_key=DEMO_WB_API_KEY)
        result = await client.get_sales()
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"WB API error: {str(e)}"
        )

@router.get("/orders")
async def get_demo_orders():
    """Получить заказы WB (demo endpoint)"""
    try:
        client = WBApiClient(api_key=DEMO_WB_API_KEY)
        result = await client.get_orders()
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"WB API error: {str(e)}"
        )

@router.get("/info")
async def get_demo_info():
    """Информация о подключении (demo endpoint)"""
    return {
        "api_url": DEMO_WB_API_URL,
        "api_key_prefix": DEMO_WB_API_KEY[:50] + "...",
        "message": "This is a demo endpoint using a hardcoded WB API key"
    }
