from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
import base64
from cryptography.fernet import Fernet
from core.config import settings


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверка пароля"""
    return bcrypt.checkpw(plain_password[:72].encode('utf-8'), hashed_password.encode('utf-8'))


def get_password_hash(password: str) -> str:
    """Хеширование пароля"""
    return bcrypt.hashpw(password[:72].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


# JWT токены
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Создание access токена"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def create_refresh_token(data: dict) -> str:
    """Создание refresh токена"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=30)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def decode_token(token: str) -> Optional[dict]:
    """Декодирование токена"""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        return payload
    except JWTError:
        return None


# Шифрование данных (для WB API ключей)
def get_encryption_key() -> bytes:
    """Получение ключа шифрования из settings или генерация нового"""
    if settings.ENCRYPTION_KEY:
        # Кодируем ключ в base64 (Fernet требует 32 байта, закодированных в base64)
        key_bytes = settings.ENCRYPTION_KEY.encode('utf-8')
        # Если ключ не 32 байта, дополняем или обрезаем
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b'0')
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]
        return base64.urlsafe_b64encode(key_bytes)
    # В проде должен быть в .env
    return Fernet.generate_key()


def encrypt_data(data: str) -> str:
    """Шифрование данных"""
    key = get_encryption_key()
    f = Fernet(key)
    return f.encrypt(data.encode()).decode()


def decrypt_data(encrypted_data: str) -> str:
    """Дешифрование данных"""
    key = get_encryption_key()
    f = Fernet(key)
    return f.decrypt(encrypted_data.encode()).decode()
