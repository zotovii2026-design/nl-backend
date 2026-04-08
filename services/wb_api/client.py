import httpx
from typing import Optional, List, Dict, Any
from core.config import settings


class WBApiClient:
    """Клиент для WB API"""

    # Правильные WB API URL для разных категорий данных
    CONTENT_URL = "https://content-api.wildberries.ru"
    MARKETPLACE_URL = "https://marketplace-api.wildberries.ru"
    STATISTICS_URL = "https://statistics-api.wildberries.ru"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "User-Agent": "NL-Table/1.0"
            },
            timeout=30.0,
            default_encoding="utf-8"
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def get_products(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Получение списка товаров"""
        response = await self.client.get(
            f"{self.CONTENT_URL}/api/v2/goods",
            params={"limit": limit, "offset": offset}
        )
        response.raise_for_status()
        result = response.json()
        return result.get("data", {}).get("cards", result.get("cards", result))

    async def get_product_detail(self, nm_id: int) -> Dict[str, Any]:
        """Получение деталей товара по nm_id"""
        response = await self.client.get(
            f"{self.CONTENT_URL}/api/v2/goods/{nm_id}"
        )
        response.raise_for_status()
        result = response.json()
        return result.get("data", result)

    async def get_sales(self,
                     date_from: Optional[str] = None,
                     date_to: Optional[str] = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        """Получение статистики продаж"""
        params = {"limit": limit}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        response = await self.client.get(
            f"{self.STATISTICS_URL}/api/v1/sales",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        return result.get("data", {}).get("cards", result.get("cards", result))

    async def get_orders(self,
                     date_from: Optional[str] = None,
                     date_to: Optional[str] = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        """Получение списка заказов"""
        params = {"limit": limit}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        response = await self.client.get(
            f"{self.MARKETPLACE_URL}/api/v2/orders",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        return result.get("data", {}).get("orders", result.get("orders", result))

    async def get_reports(self, 
                       report_type: str = "sales",
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None) -> Dict[str, Any]:
        """Получение отчётов"""
        params = {"type": report_type}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        response = await self.client.get(
            f"{self.base_url}/api/v1/analytics/reports",
            params=params
        )
        response.raise_for_status()
        return response.json().get("data", {})

    async def test_connection(self) -> bool:
        """Проверка подключения к WB API"""
        try:
            response = await self.client.get(
                f"{self.CONTENT_URL}/api/v2/goods",
                params={"limit": 1}
            )
            return response.status_code == 200
        except Exception:
            return False


async def get_wb_client(api_key: str) -> WBApiClient:
    """Фабрика для создания клиента с расшифрованным ключом"""
    # TODO: Добавить логику расшифровки ключа
    return WBApiClient(api_key)
