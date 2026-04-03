from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models.organization import WbApiKey, Organization
from core.security import encrypt_data, decrypt_data
from services.wb_api.client import WBApiClient


async def add_wb_api_key(
    db: AsyncSession,
    organization_id: str,
    name: str,
    api_key: str
) -> WbApiKey:
    """Добавление WB API ключа в организацию (с шифрованием)"""
    
    # Шифрование ключа
    encrypted_key = encrypt_data(api_key)
    
    wb_key = WbApiKey(
        organization_id=organization_id,
        name=name,
        api_key=encrypted_key
    )
    db.add(wb_key)
    await db.commit()
    await db.refresh(wb_key)
    
    return wb_key


async def get_wb_api_keys(
    db: AsyncSession,
    organization_id: str
) -> list[WbApiKey]:
    """Получение списка WB API ключей организации (без расшифровки)"""
    result = await db.execute(
        select(WbApiKey).where(
            WbApiKey.organization_id == organization_id
        )
    )
    return result.scalars().all()


async def get_wb_api_key_by_id(
    db: AsyncSession,
    key_id: str,
    organization_id: str
) -> WbApiKey | None:
    """Получение WB API ключа по ID"""
    result = await db.execute(
        select(WbApiKey).where(
            WbApiKey.id == key_id,
            WbApiKey.organization_id == organization_id
        )
    )
    return result.scalar_one_or_none()


async def get_decrypted_wb_api_key(
    db: AsyncSession,
    key_id: str,
    organization_id: str
) -> str | None:
    """Получение расшифрованного WB API ключа"""
    wb_key = await get_wb_api_key_by_id(db, key_id, organization_id)
    
    if not wb_key:
        return None
    
    return decrypt_data(wb_key.api_key)


async def delete_wb_api_key(
    db: AsyncSession,
    key_id: str,
    organization_id: str
) -> bool:
    """Удаление WB API ключа"""
    wb_key = await get_wb_api_key_by_id(db, key_id, organization_id)
    
    if not wb_key:
        return False
    
    await db.delete(wb_key)
    await db.commit()
    
    return True
