from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # App
    APP_NAME: str = "NL Table API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/nl_table"

    # JWT
    SECRET_KEY: str = "your-secret-key-here"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # Optional receiver for structured Celery failure alerts.
    CELERY_ALERT_WEBHOOK_URL: Optional[str] = None

    # Wildberries API
    WB_API_BASE_URL: str = "https://suppliers-api.wildberries.ru"

    # Шифрование
    ENCRYPTION_KEY: str = "your-32-byte-encryption-key-here-change-me"

settings = Settings()
