from fastapi import APIRouter, HTTPException, status
from services.wb_api.client import WBApiClient

router = APIRouter(prefix="/demo", tags=["Demo"])

# WB API ключ для тестирования (Personal token - acc=3)
DEMO_WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjMsImVudCI6MSwiZXhwIjoxNzkxNDg4ODcyLCJmb3IiOiJzZWxmIiwiaWQiOiIwMTlkNzEzNi0zOWIxLTdjNDQtYWY2MS04NTYzZGQxOWI5YTYiLCJpaWQiOjczNDYyMTcsIm9pZCI6NDIzNDM2MCwicyI6MTA3MzU3OTUwLCJzaWQiOiI3ZGY5N2FlNC1iOTlhLTRmOTktOWJlZS1hNDIwOGE2OWVkYzQiLCJ0IjpmYWxzZSwidWlkIjoyNzM0NjIxN30.GMKX6eFgol-sccf2gsHzxL6KOcuDw-TstnS2_htPjWMV4YG4wEerW3oglay9NULuqN3n0M43lZcr5w4i7HDqHg"

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
        "api_url": "https://content-api.wildberries.ru",
        "api_key_prefix": DEMO_WB_API_KEY[:50] + "...",
        "message": "This is a demo endpoint using a hardcoded WB API key"
    }
