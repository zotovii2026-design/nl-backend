from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from models.user import User
from models.organization import Role
from schemas.organization import WbApiKeyCreate, WbApiKeyResponse
from services.wb_api.keys import (
    add_wb_api_key,
    get_wb_api_keys,
    delete_wb_api_key
)

router = APIRouter(prefix="/organizations/{org_id}/wb-keys", tags=["WB API Keys"])


@router.post("", response_model=WbApiKeyResponse)
async def create_wb_key(
    org_id: str,
    key_data: WbApiKeyCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Добавление WB API ключа (admin+)"""
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    wb_key = await add_wb_api_key(
        db=db,
        organization_id=org_id,
        name=key_data.name,
        api_key=key_data.api_key
    )
    
    return wb_key


@router.get("", response_model=list[WbApiKeyResponse])
async def list_wb_keys(
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Получение списка WB API ключей организации (viewer+)"""
    # Проверка прав
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    
    keys = await get_wb_api_keys(db, org_id)
    
    # Важно: не возвращаем расшифрованные ключи
    return keys


@router.delete("/{key_id}")
async def delete_wb_key(
    org_id: str,
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Удаление WB API ключа (admin+)"""
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    success = await delete_wb_api_key(db, key_id, org_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WB API key not found"
        )
    
    return {"message": "WB API key deleted"}


@router.post("/{key_id}/personal-token")
async def set_personal_token(
    org_id: str,
    key_id: str,
    token_data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Установка Personal Token для stocks analytics API"""
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    from sqlalchemy import select
    from models.organization import WbApiKey
    from core.security import encrypt_data

    result = await db.execute(select(WbApiKey).where(WbApiKey.id == key_id, WbApiKey.organization_id == org_id))
    key_rec = result.scalar_one_or_none()
    if not key_rec:
        raise HTTPException(status_code=404, detail="Key not found")
    key_rec.personal_token = encrypt_data(token_data["personal_token"])
    await db.commit()
    return {"status": "ok", "message": "Personal token saved"}
