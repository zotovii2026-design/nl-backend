import os
import uuid
from urllib.parse import urlparse

import httpx
import pytest


def _test_base_url() -> str:
    base_url = os.getenv("NL_TEST_BASE_URL", "http://127.0.0.1:18000").rstrip("/")
    parsed = urlparse(base_url)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail(
            "Characterization tests only run against a loopback test environment"
        )
    return base_url


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=_test_base_url(), timeout=15.0) as test_client:
        yield test_client


def _register_user(client: httpx.Client, label: str) -> dict:
    email = f"characterization-{label}-{uuid.uuid4().hex}@example.test"
    password = f"Test-{uuid.uuid4().hex}!"
    response = client.post(
        "/api/v1/nl/register",
        json={
            "email": email,
            "password": password,
            "org_name": f"Characterization {label}",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["access_token"]
    assert payload["org_id"]
    return {
        "email": email,
        "password": password,
        "token": payload["access_token"],
        "org_id": payload["org_id"],
    }


def test_public_smoke_endpoints(client):
    for path in ("/health", "/nl/login", "/nl/register", "/docs"):
        response = client.get(path)
        assert response.status_code == 200, f"{path}: {response.text}"


def test_protected_legacy_endpoints_reject_missing_token(client):
    me_response = client.get("/api/v1/nl/me")
    organizations_response = client.get("/api/v1/nl/organizations")

    assert me_response.status_code == 401
    assert organizations_response.status_code == 401


def test_registration_login_profile_and_organization_contract(client):
    user = _register_user(client, "owner")

    login_response = client.post(
        "/api/v1/nl/login",
        json={"email": user["email"], "password": user["password"]},
    )
    assert login_response.status_code == 200, login_response.text
    login_payload = login_response.json()
    assert login_payload["access_token"]
    assert login_payload["org_id"] == user["org_id"]

    me_response = client.get(
        "/api/v1/nl/me",
        params={"token": login_payload["access_token"]},
    )
    assert me_response.status_code == 200, me_response.text
    assert me_response.json() == {
        "email": user["email"],
        "org_id": user["org_id"],
    }

    organizations_response = client.get(
        "/api/v1/nl/organizations",
        params={"token": login_payload["access_token"]},
    )
    assert organizations_response.status_code == 200, organizations_response.text
    organizations = organizations_response.json()
    assert len(organizations) == 1
    assert organizations[0]["id"] == user["org_id"]
    assert organizations[0]["role"] == "owner"
    assert organizations[0]["wb_keys_count"] == 0


def test_invalid_password_is_rejected(client):
    user = _register_user(client, "invalid-password")
    response = client.post(
        "/api/v1/nl/login",
        json={"email": user["email"], "password": "definitely-wrong"},
    )
    assert response.status_code == 401


def test_unit_economics_rejects_foreign_organization(client):
    first_user = _register_user(client, "first-org")
    second_user = _register_user(client, "second-org")

    response = client.get(
        "/api/v1/nl/unit-economics",
        params={"org_id": second_user["org_id"]},
        headers={"Authorization": f"Bearer {first_user['token']}"},
    )
    assert response.status_code == 403, response.text


@pytest.mark.parametrize(
    "method,path,json_body",
    [
        ("GET", "/api/v1/nl/reference", None),
        ("GET", "/api/v1/nl/external-ads", None),
        ("GET", "/api/v1/nl/promotions", None),
        (
            "POST",
            "/api/v1/nl/external-ads",
            {"nm_id": None, "source": "characterization", "ad_type": "ad"},
        ),
    ],
)
def test_organization_routes_require_membership(
    client, method, path, json_body
):
    owner = _register_user(client, f"route-owner-{uuid.uuid4().hex}")
    foreign = _register_user(client, f"route-foreign-{uuid.uuid4().hex}")

    missing = client.request(
        method,
        path,
        params={"org_id": owner["org_id"]},
        json=json_body,
    )
    assert missing.status_code == 401, missing.text

    forbidden = client.request(
        method,
        path,
        params={"org_id": owner["org_id"]},
        json=json_body,
        headers={"Authorization": f"Bearer {foreign['token']}"},
    )
    assert forbidden.status_code == 403, forbidden.text

    allowed = client.request(
        method,
        path,
        params={"org_id": owner["org_id"]},
        json=json_body,
        headers={"Authorization": f"Bearer {owner['token']}"},
    )
    assert allowed.status_code == 200, allowed.text


def test_organization_detail_requires_membership(client):
    owner = _register_user(client, "organization-detail-owner")
    foreign = _register_user(client, "organization-detail-foreign")
    path = f"/api/v1/organizations/{owner['org_id']}"

    missing = client.get(path)
    assert missing.status_code == 401, missing.text

    forbidden = client.get(
        path,
        headers={"Authorization": f"Bearer {foreign['token']}"},
    )
    assert forbidden.status_code == 403, forbidden.text

    allowed = client.get(
        path,
        headers={"Authorization": f"Bearer {owner['token']}"},
    )
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["id"] == owner["org_id"]
