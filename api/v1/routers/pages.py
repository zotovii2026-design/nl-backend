"""Frontend HTML pages — register, login, main NL page."""
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["nl"])

# ─── FRONTEND ──────────────────────────────────────────────




@router.get("/nl/register", response_class=HTMLResponse)
async def nl_register_page():
    with open("templates/auth_register.html", "r", encoding="utf-8") as f:
        html = f.read()
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp

@router.get("/nl/login", response_class=HTMLResponse)
async def nl_login_page():
    with open("templates/auth_login.html", "r", encoding="utf-8") as f:
        html = f.read()
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp





@router.get("/nl/v2", response_class=HTMLResponse)
async def nl_page():
    """НЛ — главная страница"""
    with open("templates/nl_v2.html", "r", encoding="utf-8") as f:
        html = f.read()
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response
