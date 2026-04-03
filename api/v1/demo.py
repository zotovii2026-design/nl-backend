from fastapi import APIRouter, HTTPException, status
from services.wb_api.client import WBApiClient

router = APIRouter(prefix="/demo", tags=["Demo"])

# WB API ключ для тестирования
DEMO_WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzkwOTc3MTA2LCJpZCI6MSwiZWMiOjAxOWQ1MmI1LTRkYTctNzY4ZC05ZmE2LWZmMDc4NzI1NTdjOCIsImlpZCI6NDIzNDM2MCwicyI6MTA3Mzc1Nzk1MCwic2lkIjoiN2RmOTdhZTQtYjk5YS00Zjk5LTliZWUtYTQyMDhhNjllZGM0IiwidCI6ZmFsc2UsInVpZCI6NDIzNDM2MTd9"

DEMO_WB_API_URL = "https://dev.wildberries.ru"


@router.get("/info")
async def get_demo_info():
    """Информация о демо-версии"""
    return {
        "title": "NL Table API - Демо-версия",
        "description": "Демо-версия с реальным WB API ключом",
        "wb_api_url": DEMO_WB_API_URL,
        "wb_api_key_status": "active",
        "endpoints": [
            "GET /api/v1/demo/products",
            "GET /api/v1/demo/sales",
            "GET /api/v1/demo/orders",
            "GET /api/v1/demo/info"
        ],
        "documentation": "https://dev.wildberries.ru/"
    }


@router.get("/products")
async def get_demo_products(limit: int = 10):
    """Получение товаров из WB API"""
    try:
        async with WBApiClient(DEMO_WB_API_KEY) as client:
            products = await client.get_products(limit=limit)
        
        return {
            "status": "success",
            "count": len(products),
            "data": products
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch products: {str(e)}"
        )


@router.get("/sales")
async def get_demo_sales(limit: int = 10):
    """Получение продаж из WB API"""
    try:
        async with WBApiClient(DEMO_WB_API_KEY) as client:
            sales = await client.get_sales(limit=limit)
        
        return {
            "status": "success",
            "count": len(sales),
            "data": sales
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch sales: {str(e)}"
        )


@router.get("/orders")
async def get_demo_orders(limit: int = 10):
    """Получение заказов из WB API"""
    try:
        async with WBApiClient(DEMO_WB_API_KEY) as client:
            orders = await client.get_orders(limit=limit)
        
        return {
            "status": "success",
            "count": len(orders),
            "data": orders
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch orders: {str(e)}"
        )


@router.get("/test-connection")
async def test_wb_connection():
    """Тест подключения к WB API"""
    try:
        async with WBApiClient(DEMO_WB_API_KEY) as client:
            is_connected = await client.test_connection()
        
        return {
            "status": "connected" if is_connected else "failed",
            "wb_api_url": DEMO_WB_API_URL,
            "wb_api_key_status": "active"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "wb_api_url": DEMO_WB_API_URL
        }


@router.get("/raw-response")
async def get_raw_wb_response():
    """Получение сырого ответа от WB API (для отладки)"""
    try:
        async with WBApiClient(DEMO_WB_API_KEY) as client:
            response = await client.client.get(f"{DEMO_WB_API_URL}/api/v2/info")
            
        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "content_type": response.headers.get("content-type"),
            "raw_content": response.text[:1000],  # первые 1000 символов
            "encoding": response.encoding
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
