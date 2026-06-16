import os

# Принудительно переопределяем для тестов (перекрывает .env из контейнера)
os.environ["ENVIRONMENT"] = "test"
os.environ.setdefault("SECRET_KEY", "test-jwt-secret-" + "a" * 32)
os.environ.setdefault("ENCRYPTION_KEY", "test-encryption-key-" + "b" * 32)
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:test-password@postgres:5432/nl_table_test",
)
os.environ["CORS_ALLOWED_ORIGINS"] = "http://localhost:3000,http://localhost:8000"
