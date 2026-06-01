"""
Общий модуль получения API-ключей WB для всех организаций.
Используется scheduled_sync, ad_sync, promo_sync и др.
"""

from typing import List, Tuple, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.security import decrypt_data
from models.organization import WbApiKey


async def get_all_wb_keys(sf: async_sessionmaker) -> List[Tuple[str, str]]:
    """
    Получить все org_id + рабочие API-ключи.
    Returns: [(org_id, decrypted_token), ...]
    """
    async with sf() as db:
        result = await db.execute(select(WbApiKey))
        key_recs = result.scalars().all()
        if not key_recs:
            return []
        keys = []
        for key_rec in key_recs:
            if key_rec.personal_token:
                decrypted = decrypt_data(key_rec.personal_token)
            elif key_rec.api_key:
                decrypted = decrypt_data(key_rec.api_key)
            else:
                continue
            if decrypted:
                keys.append((str(key_rec.organization_id), decrypted))
        return keys
