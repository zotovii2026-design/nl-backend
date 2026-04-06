from fastapi import APIRouter, HTTPException, status
from services.wb_api.client import WBApiClient

router = APIRouter(prefix="/demo", tags=["Demo"])

# WB API ключ для тестирования
DEMO_WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjMsImVudCI6MSwiZXhwIjoxNzkxMjYwMTY4LCJmb3IiOiJzZWxmIiwiaWQiOiIwMTlkNjM5NC03YThhLTdhYjctOTQ0MS0xYWRmYjEwMDE1NDAiLCJpaWQiOjI3MzQ2MjE3LCJvaWQiOjQyMzQzNjAsInMiOjEwNzM4MjM0ODYsInNpZCI6IjdkZjk3YWU0LWI5OWEtNGY5OS05YmVlLWE0MjA4YTY5ZWRjNCIsInQiOmZhbHNlLCJ1aWQiOjI3MzQ2MjE3fQ.zZIVczx-YTRw1XAE7Ya8SSx_TDgVQFtt1WH9FPt_BU9NtGZnPJkwFHHZhllolMtmISjbJQ9w3pwvkXIQ3cfqLw"

# WB API URL
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
