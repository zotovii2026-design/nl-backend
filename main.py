from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from core.database import get_db
from core.config import settings
from api.v1 import auth, organizations, wb_keys, sync, demo, demo_wb

# Импортируем Celery для регистрации задач
from core.celery import celery_app

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="SaaS платформа аналитики Wildberries"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутов
app.include_router(auth.router, prefix="/api/v1")
app.include_router(organizations.router, prefix="/api/v1")
app.include_router(wb_keys.router, prefix="/api/v1")
app.include_router(sync.router, prefix="/api/v1")
app.include_router(demo.router, prefix="/api/v1/demo")
app.include_router(demo_wb.router, prefix="/api/v1/demo_wb")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Demo dashboard page"""
    with open("templates/demo.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/wb-demo", response_class=HTMLResponse)
async def wb_demo():
    """WB Demo dashboard page"""
    with open("templates/demo_wb.html", "r", encoding="utf-8") as f:
        return f.read()


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
