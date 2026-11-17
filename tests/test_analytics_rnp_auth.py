from api.v1.routers.analytics import router as analytics_router
from api.v1.routers.rnp import router as rnp_router
from core.tenant_auth import require_query_organization_access


def _router_dependencies(router):
    return [dependency.dependency for dependency in router.dependencies]


def test_analytics_router_requires_tenant_access():
    assert require_query_organization_access in _router_dependencies(analytics_router)


def test_rnp_router_requires_tenant_access():
    assert require_query_organization_access in _router_dependencies(rnp_router)
