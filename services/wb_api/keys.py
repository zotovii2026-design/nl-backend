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


async def add_wb_api_key(db, organization_id: str, name: str, api_key: str):
    """Добавить WB API ключ для организации"""
    from core.security import encrypt_data
    from models.organization import WbApiKey
    import uuid
    
    encrypted = encrypt_data(api_key)
    wb_key = WbApiKey(
        id=uuid.uuid4(),
        organization_id=organization_id,
        name=name,
        api_key=encrypted,
    )
    db.add(wb_key)
    await db.commit()
    await db.refresh(wb_key)
    return wb_key


async def get_wb_api_keys(db, organization_id: str):
    """Получить список WB API ключей организации"""
    from sqlalchemy import select
    from models.organization import WbApiKey
    
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == organization_id)
    )
    return result.scalars().all()


async def delete_wb_api_key(db, key_id: str, organization_id: str) -> bool:
    """Удалить WB API ключ"""
    from sqlalchemy import select
    from models.organization import WbApiKey
    
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.id == key_id, WbApiKey.organization_id == organization_id)
    )
    key_rec = result.scalar_one_or_none()
    if not key_rec:
        return False
    await db.delete(key_rec)
    await db.commit()
    return True
