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

    async def get_cards(
        self,
        limit: int = 100,
        search: str = None,
        cursor_updated_at: str = None,
        cursor_nm_id: int = None
    ) -> Dict[str, Any]:
        """
        Получение списка карточек товаров
        
        КЛЮЧЕВОЙ ПАРАМЕТР: withPhoto: -1 — показывает все карточки
        
        Использует правильный endpoint POST /content/v2/get/cards/list
        
        Args:
            limit: лимит карточек за запрос (макс 100)
            search: поиск по названию/артикулу
            cursor_updated_at: курсор updatedAt для пагинации
            cursor_nm_id: курсор nmID для пагинации
        
        Returns:
            Dict с карточками и курсором для пагинации
        """
        payload = {
            "settings": {
                "cursor": {"limit": limit},
                "filter": {"withPhoto": -1}
            }
        }
        
        if search:
            payload["settings"]["filter"]["textSearch"] = search
        
        # Добавляем курсор для пагинации
        if cursor_updated_at or cursor_nm_id:
            payload["settings"]["cursor"]["updatedAt"] = cursor_updated_at
            payload["settings"]["cursor"]["nmID"] = cursor_nm_id
        
        response = await self.client.post(
            f"{self.CONTENT_URL}/content/v2/get/cards/list",
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        return {
            "cards": result.get("cards", []),
            "cursor": result.get("cursor", {})
        }

    async def get_all_cards(self, limit: int = 100, search: str = None) -> List[Dict[str, Any]]:
        """
        Получение всех карточек с автоматической пагинацией
        
        Args:
            limit: лимит карточек за запрос
            search: фильтр поиска
        
        Returns:
            List всех карточек
        """
        all_cards = []
        cursor_updated_at = None
        cursor_nm_id = None
        
        while True:
            result = await self.get_cards(
                limit=limit,
                search=search,
                cursor_updated_at=cursor_updated_at,
                cursor_nm_id=cursor_nm_id
            )
            
            cards = result.get("cards", [])
            cursor = result.get("cursor", {})
            
            if not cards:
                break
            
            all_cards.extend(cards)
            
            # Обновляем курсор для следующей страницы
            cursor_updated_at = cursor.get("updatedAt")
            cursor_nm_id = cursor.get("nmID")
            
            # Если курсор говорит, что нет следующей страницы — выходим
            if not cursor.get("next", False):
                break
        
        return all_cards

    async def get_card_by_nm_id(self, nm_id: int) -> Optional[Dict[str, Any]]:
        """Получение карточки по nm_id"""
        cards = await self.get_all_cards()
        for card in cards:
            if card.get("nmID") == nm_id:
                return card
        return None

    async def get_card_by_vendor_code(self, vendor_code: str) -> Optional[Dict[str, Any]]:
        """Получение карточки по артикулу продавца"""
        cards = await self.get_all_cards(search=vendor_code)
        for card in cards:
            if card.get("vendorCode") == vendor_code:
                return card
        return None

    async def get_products(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Получение списка товаров (карточек) - устаревший метод, используйте get_cards"""
        result = await self.get_cards(limit=limit)
        return result.get("cards", [])

    async def get_product_detail(self, nm_id: int) -> Dict[str, Any]:
        """Получение деталей товара по nm_id"""
        card = await self.get_card_by_nm_id(nm_id)
        return card if card else {}

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
            f"{self.STATISTICS_URL}/api/v1/supplier/sales",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return result.get("items") or result.get("data") or []
        return []

    async def get_sales_funnel_products(self,
                     date_from: str,
                     date_to: str,
                     timezone: str = "Europe/Moscow") -> List[Dict[str, Any]]:
        """Получение агрегированной аналитики продаж по товарам"""
        payload = {
            "selectedPeriod": {"start": date_from, "end": date_to},
            "timezone": timezone,
            "brandNames": [],
            "objectIDs": [],
            "tagIDs": [],
            "nmIDs": [],
            "page": 1
        }
        response = await self.client.post(
            "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products",
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        return result.get("data", {}).get("products", [])

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
            f"{self.STATISTICS_URL}/api/v1/supplier/orders",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        return result if isinstance(result, list) else result.get("data", result)

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
            f"{self.base_url}api/v1/analytics/reports",
            params=params
        )
        response.raise_for_status()
        return response.json().get("data", {})

    async def test_connection(self) -> bool:
        """Проверка подключения к WB API"""
        try:
            response = await self.client.get(
                f"{self.CONTENT_URL}/content/v2/object/parent/all"
            )
            return response.status_code == 200
        except Exception:
            return False


    async def get_stocks_api(self, date_from: str = None) -> list:
        """Получение остатков со складов через statistics API"""
        params = {}
        if date_from:
            params["dateFrom"] = date_from
        response = await self.client.get(
            f"{self.STATISTICS_URL}/api/v1/supplier/stocks",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        return result if isinstance(result, list) else result.get("data", result)


    async def get_stocks_warehouses(self, is_archive: bool = False) -> list:
        """
        Получение остатков со складов WB через analytics API.
        Требует Personal token (не Standard).

        POST /api/analytics/v1/stocks-report/wb-warehouses
        """
        payload = {"isArchive": is_archive}
        response = await self.client.post(
            "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses",
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        # API возвращает list записей с полями вроде nmID, vendorCode, warehouseName, quantity и т.д.
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return result.get("items") or result.get("data") or []
        return []


async def get_wb_client(api_key: str) -> WBApiClient:
    """Фабрика для создания клиента с расшифрованным ключом"""
    # TODO: Добавить логику расшифровки ключа
    return WBApiClient(api_key)
