from fastapi import APIRouter, HTTPException, status
from services.wb_api.client import WBApiClient

router = APIRouter(prefix="/demo", tags=["Demo"])

# WB API ключ для тестирования (Service token - acc=4)
DEMO_WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjQsImVudCI6MSwiZXhwIjoxNzkxMjY3OTE4LCJmb3IiOiJhc2lkOmE2OTc1YjllLTdmNTYtNWFkNi04MjRkLWY0YTQzMmU4ZjJhZSIsImlkIjoiMDE5ZDY0MGEtYmM1Yy03Njg2LTk4MDctM2NkNTNhZTI1NDMzIiwiaWlkIjoyNzM0NjIxNywib2lkIjo0MjM0MzYwLCJzIjoxMDczNzQ1NjYyLCJzaWQiOiI3ZGY5N2FlNC1iOTlhLTRmOTktOWJlZS1hNDIwOGE2OWVkYzQiLCJ0IjpmYWxzZSwidWlkIjoyNzM0NjIxN30.mG-1I0MevZESAz-SbAKaN9TfsRfcwcm2NCe1GR_s2ogJ32jjKvmAFl-13CCp02MyWIhobvCgMmtCx5GkSjQ8hA"

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
