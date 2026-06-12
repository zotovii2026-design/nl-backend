import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from core.config import Settings
from main import app


def test_cors_allows_configured_origin():
    client = TestClient(app)

    response = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert response.headers["access-control-allow-credentials"] == "true"


def test_cors_rejects_unconfigured_origin():
    client = TestClient(app)

    response = client.options(
        "/health",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers


def test_cors_configuration_rejects_wildcard():
    with pytest.raises(ValidationError, match="cannot contain a wildcard"):
        Settings(CORS_ALLOWED_ORIGINS="*")
