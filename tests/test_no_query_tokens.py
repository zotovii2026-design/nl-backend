from fastapi.testclient import TestClient

from main import app


LEGACY_ACCOUNT_PATHS = {
    "/api/v1/nl/me",
    "/api/v1/nl/organizations",
    "/api/v1/nl/connect-wb",
    "/api/v1/nl/profile",
    "/api/v1/nl/verify-wb-key",
    "/api/v1/nl/rename-org",
    "/api/v1/nl/invite",
}


def test_legacy_account_routes_do_not_accept_query_tokens():
    schema = app.openapi()

    for path in LEGACY_ACCOUNT_PATHS:
        operations = schema["paths"][path]
        for operation in operations.values():
            parameters = operation.get("parameters", [])
            assert all(parameter["name"] != "token" for parameter in parameters)


def test_query_token_does_not_authenticate_request():
    client = TestClient(app)

    response = client.get("/api/v1/nl/me?token=legacy-token")

    assert response.status_code == 401


def test_embedded_frontend_does_not_put_jwt_in_urls():
    source = open("api/v1/nl.py", encoding="utf-8").read()

    assert "?token=" not in source
