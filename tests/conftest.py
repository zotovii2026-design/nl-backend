import os


os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("SECRET_KEY", "test-jwt-secret-" + "a" * 32)
os.environ.setdefault("ENCRYPTION_KEY", "test-encryption-key-" + "b" * 32)
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:test-password@postgres:5432/nl_table_test",
)
