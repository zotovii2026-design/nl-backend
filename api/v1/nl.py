"""NL Table — legacy router shim.

All routes have been extracted to dedicated routers:
- api/v1/routers/dashboard.py — products, dates, control
- api/v1/routers/sellers.py — sellers, seo-keywords
- api/v1/routers/marketer.py — marketer products/detail
- api/v1/routers/unit_economics.py — unit economics
- api/v1/routers/prices.py — WB prices refresh
- api/v1/routers/pages.py — register/login/nl-v2 HTML pages

This file is kept for backward compatibility but contains no routes.
"""
from fastapi import APIRouter

router = APIRouter(tags=["nl"])
