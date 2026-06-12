from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from typing import Optional
from urllib.parse import unquote, urlparse


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # App
    APP_NAME: str = "NL Table API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: Literal["development", "test", "production"] = "development"
    PUBLIC_BASE_URL: Optional[str] = None
    CORS_ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"

    @field_validator("CORS_ALLOWED_ORIGINS")
    @classmethod
    def validate_cors_origins(cls, value: str) -> str:
        origins = [origin.strip() for origin in value.split(",") if origin.strip()]
        if not origins:
            raise ValueError("CORS_ALLOWED_ORIGINS must contain at least one origin")
        if "*" in origins:
            raise ValueError("CORS_ALLOWED_ORIGINS cannot contain a wildcard")
        return ",".join(origins)

    @property
    def cors_allowed_origins(self) -> list[str]:
        return self.CORS_ALLOWED_ORIGINS.split(",")

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

    @model_validator(mode="after")
    def validate_production_secrets(self):
        if self.ENVIRONMENT != "production":
            return self

        weak_markers = ("change-me", "change-in-production", "your-secret")
        secret_values = {
            "SECRET_KEY": self.SECRET_KEY,
            "ENCRYPTION_KEY": self.ENCRYPTION_KEY,
        }
        errors = []
        for name, value in secret_values.items():
            normalized = value.lower()
            if len(value) < 32 or any(marker in normalized for marker in weak_markers):
                errors.append(f"{name} must be a strong value of at least 32 characters")

        database_password = unquote(urlparse(self.DATABASE_URL).password or "")
        if (
            len(database_password) < 16
            or database_password.lower() in {"postgres", "password", "change-me"}
            or any(marker in database_password.lower() for marker in weak_markers)
        ):
            errors.append("DATABASE_URL must contain a strong database password")

        if errors:
            raise ValueError("; ".join(errors))
        return self

settings = Settings()
