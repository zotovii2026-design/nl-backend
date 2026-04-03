from fastapi.testclient import TestClient
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.user import User
from models.organization import WbApiKey, Organization
from services.wb_api.keys import (
    add_wb_api_key,
    get_wb_api_keys,
    get_decrypted_wb_api_key,
    delete_wb_api_key
)
from core.security import encrypt_data, decrypt_data
import secrets


@pytest.fixture
async def test_api_key():
    """Фикстура для тестового API ключа"""
    return "test_wb_api_key_" + secrets.token_hex(16)


@pytest.mark.asyncio
async def test_add_wb_api_key(db: AsyncSession, test_user: User, test_organization: Organization):
    """Тест добавления WB API ключа"""
    wb_key = await add_wb_api_key(
        db=db,
        organization_id=str(test_organization.id),
        name="Тестовый ключ",
        api_key="test_wb_key_12345"
    )
    
    assert wb_key is not None
    assert wb_key.name == "Тестовый ключ"
    assert wb_key.organization_id == test_organization.id
    # Ключ должен быть зашифрован
    assert wb_key.api_key != "test_wb_key_12345"


@pytest.mark.asyncio
async def test_get_wb_api_keys(db: AsyncSession, test_user: User, test_organization: Organization):
    """Тест получения списка WB API ключей"""
    # Добавляем несколько ключей
    await add_wb_api_key(
        db=db,
        organization_id=str(test_organization.id),
        name="Ключ 1",
        api_key="test_key_1"
    )
    await add_wb_api_key(
        db=db,
        organization_id=str(test_organization.id),
        name="Ключ 2",
        api_key="test_key_2"
    )
    
    keys = await get_wb_api_keys(db, str(test_organization.id))
    
    assert len(keys) == 2
    assert all(key.organization_id == test_organization.id for key in keys)


@pytest.mark.asyncio
async def test_encryption_decryption():
    """Тест шифрования и дешифрования"""
    original_key = "my_secret_wb_api_key_12345"
    
    # Шифрование
    encrypted = encrypt_data(original_key)
    assert encrypted != original_key
    
    # Дешифрование
    decrypted = decrypt_data(encrypted)
    assert decrypted == original_key


@pytest.mark.asyncio
async def test_delete_wb_api_key(db: AsyncSession, test_user: User, test_organization: Organization):
    """Тест удаления WB API ключа"""
    # Добавляем ключ
    wb_key = await add_wb_api_key(
        db=db,
        organization_id=str(test_organization.id),
        name="Удаляемый ключ",
        api_key="test_key_to_delete"
    )
    
    # Удаляем
    success = await delete_wb_api_key(
        db=db,
        key_id=str(wb_key.id),
        organization_id=str(test_organization.id)
    )
    
    assert success is True
    
    # Проверяем, что ключа нет
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.id == wb_key.id)
    )
    deleted_key = result.scalar_one_or_none()
    
    assert deleted_key is None


@pytest.mark.asyncio
async def test_wb_api_client():
    """Тест клиента WB API (без реального ключа)"""
    from services.wb_api.client import WBApiClient
    
    client = WBApiClient("fake_key")
    
    # Тест подключения (должен вернуть False с фейковым ключом)
    is_connected = await client.test_connection()
    
    # Просто проверяем, что метод работает (не падает с ошибкой)
    assert isinstance(is_connected, bool)
    
    await client.client.aclose()
