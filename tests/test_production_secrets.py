import pytest
from pydantic import ValidationError

from core.config import Settings


STRONG_VALUES = {
    "SECRET_KEY": "jwt-" + "a" * 48,
    "ENCRYPTION_KEY": "encryption-" + "b" * 48,
    "DATABASE_URL": (
        "postgresql+asyncpg://postgres:"
        "database-password-with-32-characters@postgres:5432/nl_table"
    ),
}


def test_production_accepts_strong_secrets():
    settings = Settings(ENVIRONMENT="production", **STRONG_VALUES)

    assert settings.ENVIRONMENT == "production"


def test_missing_environment_fails_closed(monkeypatch):
    for name in ("ENVIRONMENT", "SECRET_KEY", "ENCRYPTION_KEY", "DATABASE_URL"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError, match="SECRET_KEY"):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("SECRET_KEY", "short", "SECRET_KEY"),
        (
            "ENCRYPTION_KEY",
            "your-32-byte-encryption-key-here-change-me",
            "ENCRYPTION_KEY",
        ),
        (
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@postgres:5432/nl_table",
            "DATABASE_URL",
        ),
    ],
)
def test_production_rejects_weak_secrets(field, value, message):
    values = {**STRONG_VALUES, field: value}

    with pytest.raises(ValidationError, match=message):
        Settings(ENVIRONMENT="production", **values)


def test_development_mode_does_not_require_production_secrets():
    settings = Settings(ENVIRONMENT="development")

    assert settings.ENVIRONMENT == "development"
