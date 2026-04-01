from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from cryptography.fernet import Fernet
from core.config import settings

# Хеширование паролей
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверка пароля"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Хеширование пароля"""
    return pwd_context.hash(password)


# JWT токены
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Создание access токена"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict) -> str:
    """Создание refresh токена"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """Декодирование токена"""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        return None


# Шифрование
def get_fernet() -> Fernet:
    """Получение Fernet для шифрования"""
    # Ключ должен быть 32 байта, base64 encoded
    key = settings.ENCRYPTION_KEY.encode()
    if len(key) != 32:
        # Если ключ неправильного размера, расширяем или обрезаем
        key = (key + b'0' * 32)[:32]
    return Fernet(Fernet.generate_key() if len(settings.ENCRYPTION_KEY) != 44 else settings.ENCRYPTION_KEY)


def encrypt_data(data: str) -> str:
    """Шифрование данных"""
    fernet = Fernet(settings.ENCRYPTION_KEY.encode())
    encrypted = fernet.encrypt(data.encode())
    return encrypted.decode()


def decrypt_data(encrypted_data: str) -> str:
    """Дешифрование данных"""
    fernet = Fernet(settings.ENCRYPTION_KEY.encode())
    decrypted = fernet.decrypt(encrypted_data.encode())
    return decrypted.decode()
