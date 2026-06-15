from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from core.database import get_db
from core.config import settings
from api.v1 import auth, organizations, wb_keys, sync, admin_tech, nl, external_ad
from api.v1.routers import opiu, promotions, ads

# Импортируем Celery для регистрации задач
from core.celery import celery_app

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="SaaS платформа аналитики Wildberries"
)

# Cache static files middleware
from starlette.middleware.base import BaseHTTPMiddleware
class StaticCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "public, max-age=86400"
        return response
app.add_middleware(StaticCacheMiddleware)

# CORS middleware
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутов
app.include_router(auth.router, prefix="/api/v1")
app.include_router(organizations.router, prefix="/api/v1")
app.include_router(wb_keys.router, prefix="/api/v1")
app.include_router(sync.router, prefix="/api/v1")
app.include_router(admin_tech.router)
app.include_router(nl.router)
app.include_router(external_ad.router)
app.include_router(promotions.router)
app.include_router(opiu.router)
app.include_router(ads.router)

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """Проверка здоровья приложения и БД"""
    try:
        await db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

# Static files для JS/CSS модулей
from fastapi.staticfiles import StaticFiles
import os
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
