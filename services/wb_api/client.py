import asyncio
import httpx
from typing import Optional, List, Dict, Any
from core.config import settings


class WBApiClient:
    """Клиент для WB API"""

    # Правильные WB API URL для разных категорий данных
    CONTENT_URL = "https://content-api.wildberries.ru"
    MARKETPLACE_URL = "https://marketplace-api.wildberries.ru"
    PRICES_URL = "https://discounts-prices-api.wildberries.ru"
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
        
        WB API отдаёт максимум limit карточек за запрос.
        Пагинация через курсор: берём updatedAt и nmID последней карточки
        и передаём как cursor в следующем запросе.
        
        Если WB не отдаёт cursor.next — делаем повторный запрос
        с курсором из последней карточки (WB иногда не ставит next=true).
        """
        import logging
        _log = logging.getLogger(__name__)
        
        all_cards = []
        seen_nm = set()
        page = 0
        max_pages = 50  # Защита от бесконечного цикла (50 * 100 = 5000 карточек)
        
        cursor_updated_at = None
        cursor_nm_id = None
        
        while page < max_pages:
            result = await self.get_cards(
                limit=limit,
                search=search,
                cursor_updated_at=cursor_updated_at,
                cursor_nm_id=cursor_nm_id
            )
            
            cards = result.get("cards", [])
            cursor = result.get("cursor", {})
            page += 1
            
            if not cards:
                _log.info(f"[pagination] page {page}: empty response, stopping")
                break
            
            # Дедупликация по nmID
            new_cards = []
            for c in cards:
                nm = c.get("nmID")
                if nm and nm not in seen_nm:
                    seen_nm.add(nm)
                    new_cards.append(c)
            
            all_cards.extend(new_cards)
            _log.info(f"[pagination] page {page}: {len(cards)} cards ({len(new_cards)} new), total {len(all_cards)}")
            
            # Курсор из ответа API
            api_cursor_at = cursor.get("updatedAt")
            api_cursor_nm = cursor.get("nmID")
            
            # Если API отдал курсор — используем его
            if api_cursor_at or api_cursor_nm:
                cursor_updated_at = api_cursor_at
                cursor_nm_id = api_cursor_nm
            else:
                # Fallback: берём курсор из последней карточки
                last = cards[-1]
                cursor_updated_at = last.get("updatedAt")
                cursor_nm_id = last.get("nmID")
            
            # Если вернулось меньше чем limit — это последняя страница
            if len(cards) < limit:
                _log.info(f"[pagination] page {page}: got {len(cards)} < {limit}, last page")
                break
            
            # Если нет курсора вообще — не можем продолжать
            if not cursor_updated_at and not cursor_nm_id:
                _log.warning(f"[pagination] page {page}: no cursor available, stopping")
                break
        
        _log.info(f"[pagination] done: {len(all_cards)} unique cards in {page} pages")
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
            # WB возвращает {"data": {"items": [...]}}
            data = result.get("data", {})
            if isinstance(data, dict):
                return data.get("items", [])
            if isinstance(data, list):
                return data
            return result.get("items", [])
        return []


    async def get_all_sales(self,
                     date_from: Optional[str] = None,
                     date_to: Optional[str] = None,
                     page_size: int = 1000) -> List[Dict[str, Any]]:
        """Получение ВСЕХ продаж с пагинацией"""
        import logging
        _log = logging.getLogger(__name__)
        all_sales = []
        seen_ids = set()
        max_pages = 100
        params = {"limit": page_size}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        for page in range(max_pages):
            response = await self.client.get(
                f"{self.STATISTICS_URL}/api/v1/supplier/sales",
                params=params
            )
            response.raise_for_status()
            result = response.json()
            items = result if isinstance(result, list) else []
            if isinstance(result, dict):
                data = result.get("data", {})
                items = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

            new_items = [s for s in items if s.get("saleID") and s["saleID"] not in seen_ids]
            for s in new_items:
                seen_ids.add(s["saleID"])
            all_sales.extend(new_items)

            _log.info(f"[sales pagination] page {page+1}: {len(items)} items ({len(new_items)} new), total {len(all_sales)}")

            if len(items) < page_size:
                break

            last_key = None
            if isinstance(result, dict):
                last_key = result.get("lastKey")
            if last_key:
                params["lastKey"] = last_key
            else:
                params["offset"] = (page + 1) * page_size

            await asyncio.sleep(0.5)

        _log.info(f"[sales pagination] done: {len(all_sales)} unique sales")
        return all_sales

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


    async def get_all_orders(self,
                     date_from: Optional[str] = None,
                     date_to: Optional[str] = None,
                     page_size: int = 1000) -> List[Dict[str, Any]]:
        """Получение ВСЕХ заказов с пагинацией"""
        import logging
        _log = logging.getLogger(__name__)
        all_orders = []
        seen_ids = set()
        max_pages = 100
        params = {"limit": page_size}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        for page in range(max_pages):
            response = await self.client.get(
                f"{self.STATISTICS_URL}/api/v1/supplier/orders",
                params=params
            )
            response.raise_for_status()
            result = response.json()
            items = result if isinstance(result, list) else result.get("data", [])
            if not isinstance(items, list):
                items = []

            new_items = [o for o in items if (o.get("srid") or o.get("odid") or o.get("gNumber")) and (o.get("srid") or o.get("odid") or o.get("gNumber")) not in seen_ids]
            for o in new_items:
                seen_ids.add(o.get("srid") or o.get("odid") or o.get("gNumber"))
            all_orders.extend(new_items)

            _log.info(f"[orders pagination] page {page+1}: {len(items)} items ({len(new_items)} new), total {len(all_orders)}")

            if len(items) < page_size:
                break

            last_key = None
            if isinstance(result, dict):
                last_key = result.get("lastKey")
            if last_key:
                params["lastKey"] = last_key
            else:
                params["offset"] = (page + 1) * page_size

            await asyncio.sleep(0.5)

        _log.info(f"[orders pagination] done: {len(all_orders)} unique orders")
        return all_orders

    async def get_reports(self, 
                       report_type: str = "sales",
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None) -> Dict[str, Any]:
        """DEPRECATED: Не используется. endpoint удалён из WB API."""
        raise NotImplementedError("get_reports() deprecated — endpoint удалён из WB API")

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
        """DEPRECATED: GET /api/v1/supplier/stocks — отключается 23 июня 2026.
        Используйте get_stocks_warehouses() (seller-analytics-api).
        """
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
            # WB возвращает {"data": {"items": [...]}}
            data = result.get("data", {})
            if isinstance(data, dict):
                return data.get("items", [])
            if isinstance(data, list):
                return data
            return result.get("items", [])
        return []




    async def get_stocks_seller_warehouses(self, is_archive: bool = False) -> list:
        """DEPRECATED: GET /api/v1/supplier/stocks — отключается 23 июня 2026.
        Используйте get_stocks_warehouses() (seller-analytics-api).
        """
        from datetime import date
        response = await self.client.get(
            f"{self.STATISTICS_URL}/api/v1/supplier/stocks",
            params={"dateFrom": date.today().isoformat()}
        )
        if response.status_code == 204:
            return []
        response.raise_for_status()
        result = response.json()
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            data = result.get("data", {})
            if isinstance(data, dict):
                return data.get("items", [])
            if isinstance(data, list):
                return data
            return result.get("items", [])
        return []

    async def get_fbs_warehouses(self) -> list:
        """Получение списка складов для FBS отгрузки.
        GET /api/v3/passes/offices
        Требует Personal token.
        """
        response = await self.client.get(
            f"{self.MARKETPLACE_URL}/api/v3/passes/offices"
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("data", [])
    async def get_all_offices(self) -> list:
        """Полный список офисов (склады + СЦ/СГТ + КГТ+).
        GET /api/v3/offices
        cargoType: 1=склад, 2=СЦ(СГТ), 3=КГТ+
        """
        response = await self.client.get(
            f"{self.MARKETPLACE_URL}/api/v3/offices"
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def get_prices(self, limit: int = 1000, offset: int = 0) -> list:
        """
        Получение товаров с ценами через Marketplace API.
        GET /api/v2/list/goods/filter
        """
        params = {"limit": limit, "offset": offset}
        response = await self.client.get(
            f"{self.PRICES_URL}/api/v2/list/goods/filter",
            params=params
        )
        response.raise_for_status()
        result = response.json()
        if isinstance(result, dict):
            data = result.get("data", {})
            if isinstance(data, dict):
                items = data.get("listGoods", [])
                total = data.get("total", 0)
                return {"items": items, "total": total}
            if isinstance(data, list):
                return {"items": data, "total": len(data)}
        return {"items": result if isinstance(result, list) else [], "total": 0}

    async def get_all_prices(self) -> list:
        """Получение цен всех товаров с пагинацией"""
        import logging
        _log = logging.getLogger(__name__)

        all_items = []
        offset = 0
        limit = 1000
        max_pages = 20

        for page in range(max_pages):
            result = await self.get_prices(limit=limit, offset=offset)
            items = result.get("items", [])
            total = result.get("total", 0)

            if not items:
                break

            all_items.extend(items)
            _log.info(f"[prices] page {page+1}: {len(items)} items, total {total}")

            offset += limit
            if offset >= total:
                break

        _log.info(f"[prices] done: {len(all_items)} items with prices")
        return all_items
    # ─── Calendar / Promotions API ───────────────────────

    CALENDAR_URL = "https://dp-calendar-api.wildberries.ru"

    async def get_calendar_promotions(self, start_date=None, end_date=None, all_promo=False, limit=100, offset=0):
        """Получить список акций из календаря WB"""
        from datetime import datetime, timedelta, timezone
        if not start_date:
            start_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if not end_date:
            end_date = (datetime.now(timezone.utc) + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params = {
            "startDateTime": start_date,
            "endDateTime": end_date,
            "allPromo": str(all_promo).lower(),
            "limit": limit,
            "offset": offset,
        }
        resp = await self.client.get(
            f"{self.CALENDAR_URL}/api/v1/calendar/promotions",
            params=params
        )
        resp.raise_for_status()
        return resp.json()

    async def get_promotion_details(self, promotion_ids):
        """Получить детали акций по IDs"""
        if isinstance(promotion_ids, list):
            ids_str = ",".join(str(i) for i in promotion_ids)
        else:
            ids_str = str(promotion_ids)
        resp = await self.client.get(
            f"{self.CALENDAR_URL}/api/v1/calendar/promotions/details",
            params={"promotionIDs": ids_str}
        )
        resp.raise_for_status()
        return resp.json()

    async def get_promotion_nomenclatures(self, promotion_id, in_action=False, limit=1000, offset=0):
        """Получить товары для участия в акции (НЕ для автоакций)"""
        params = {
            "promotionID": promotion_id,
            "inAction": str(in_action).lower(),
            "limit": limit,
            "offset": offset,
        }
        resp = await self.client.get(
            f"{self.CALENDAR_URL}/api/v1/calendar/promotions/nomenclatures",
            params=params
        )
        resp.raise_for_status()
        return resp.json()



async def get_wb_client(api_key: str) -> WBApiClient:
    """Фабрика для создания клиента с расшифрованным ключом"""
    # TODO: Добавить логику расшифровки ключа
    return WBApiClient(api_key)
