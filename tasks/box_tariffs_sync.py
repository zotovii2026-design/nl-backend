from tasks.ue_precompute import run_precompute
"""
Celery-таск: синхронизация тарифов коробной логистики WB
GET https://common-api.wildberries.ru/api/v1/tariffs/box
"""

import json
import logging
from datetime import date
from zoneinfo import ZoneInfo

from celery import shared_task
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tasks.scheduled_sync import _run, _get_all_keys
from models.wb_box_tariff import WbBoxTariff

logger = logging.getLogger(__name__)

# Склады ФБО для усреднения
FBO_WAREHOUSES = ["Коледино", "Краснодар", "Казань"]


def _parse_float(val) -> float | None:
    """Парсим значение из WB API — может быть строкой с запятой как разделителем"""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ".").replace(" ", ""))
    except (ValueError, TypeError):
        return None


@shared_task(name="wb.sched.box_tariffs")
def sched_box_tariffs():
    """Синхронизация тарифов коробной логистики WB по складам"""
    result = _run(_do_box_tariffs)
    try:
        import asyncio
        from core.database import async_session as _sf2
        from tasks.scheduled_sync import _get_org_ids_for_precompute
        orgs = asyncio.run(_get_org_ids_for_precompute(_sf2))
        run_precompute(orgs)
    except Exception as e:
        logger.warning(f"[box_tariffs] ue_precompute skipped: {e}")
    return result


async def _do_box_tariffs(sf):
    import httpx

    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    results = {}

    for org_id, api_key in all_keys:
        today = date.today()
        org_result = {"fetched": 0, "saved": 0}

        try:
            # 1. Запрашиваем тарифы из WB API
            async with httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            ) as client:
                resp = await client.get(
                    "https://common-api.wildberries.ru/api/v1/tariffs/box",
                    params={"date": today.isoformat()},
                )
                resp.raise_for_status()
                data = resp.json()

            # 2. Парсим ответ
            warehouse_list = (
                data.get("response", {})
                .get("data", {})
                .get("warehouseList", [])
            )
            org_result["fetched"] = len(warehouse_list)

            if not warehouse_list:
                logger.warning(f"[box_tariffs] org={org_id[:8]}: empty warehouseList")
                results[org_id[:8]] = org_result
                continue

            # 3. Сохраняем тарифы по каждому складу
            async with sf() as db:
                # Удаляем старые записи за сегодня (полная замена)
                await db.execute(
                    text(
                        "DELETE FROM wb_box_tariffs "
                        "WHERE organization_id = :org AND snapshot_date = :dt"
                    ),
                    {"org": org_id, "dt": today},
                )
                await db.commit()

            for wh in warehouse_list:
                wh_name = wh.get("warehouseName", "")
                if not wh_name:
                    continue

                row = WbBoxTariff(
                    organization_id=org_id,
                    warehouse_name=wh_name,
                    geo_name=wh.get("geoName"),
                    box_delivery_base=_parse_float(wh.get("boxDeliveryBase")),
                    box_delivery_liter=_parse_float(wh.get("boxDeliveryLiter")),
                    box_delivery_coef=_parse_float(wh.get("boxDeliveryCoefExpr")),
                    box_delivery_marketplace_base=_parse_float(wh.get("boxDeliveryMarketplaceBase")),
                    box_delivery_marketplace_liter=_parse_float(wh.get("boxDeliveryMarketplaceLiter")),
                    box_delivery_marketplace_coef=_parse_float(wh.get("boxDeliveryMarketplaceCoefExpr")),
                    box_storage_base=_parse_float(wh.get("boxStorageBase")),
                    box_storage_liter=_parse_float(wh.get("boxStorageLiter")),
                    box_storage_coef=_parse_float(wh.get("boxStorageCoefExpr")),
                    snapshot_date=today,
                )

                async with sf() as db:
                    db.add(row)
                    await db.commit()

                org_result["saved"] += 1

            logger.info(
                f"[box_tariffs] org={org_id[:8]}: fetched={org_result['fetched']}, saved={org_result['saved']}"
            )
            results[org_id[:8]] = org_result

        except Exception as e:
            logger.error(f"[box_tariffs] error org={org_id[:8]}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
