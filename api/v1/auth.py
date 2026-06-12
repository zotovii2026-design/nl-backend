from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from core.database import get_db
from core.security import (
    verify_password,
    get_password_hash,
    create_access_token,
    create_refresh_token,
    decode_token
)
from core.dependencies import get_current_user
from core.rate_limit import enforce_rate_limit
from models.user import User
from schemas.auth import UserRegister, UserLogin, TokenResponse, TokenRefresh, UserResponse

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserResponse)
async def register(
    request: Request,
    user_data: UserRegister,
    db: AsyncSession = Depends(get_db)
):
    """Регистрация нового пользователя"""
    await enforce_rate_limit(
        request, "auth-register", 3, 3600, user_data.email
    )
    # Проверка существования пользователя
    result = await db.execute(
        select(User).where(User.email == user_data.email)
    )
    existing_user = result.scalar_one_or_none()

    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Создание пользователя
    user = User(
        email=user_data.email,
        password_hash=get_password_hash(user_data.password),
        is_active=True
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    return user


@router.post("/login")
async def login(
    request: Request,
    user_data: UserLogin,
    db: AsyncSession = Depends(get_db)
):
    """Логин пользователя"""
    await enforce_rate_limit(request, "auth-login", 5, 60, user_data.email)
    # Поиск пользователя
    result = await db.execute(
        select(User).where(User.email == user_data.email)
    )
    user = result.scalar_one_or_none()

    if not user or not verify_password(user_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is inactive"
        )

    # Обновление last_login
    user.last_login = datetime.utcnow()
    await db.commit()

    # Создание токенов
    access_token = create_access_token(data={"sub": str(user.id)})
    refresh_token = create_refresh_token(data={"sub": str(user.id)})

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token
    )


@router.post("/refresh")
async def refresh_token(
    request: Request,
    token_data: TokenRefresh,
    db: AsyncSession = Depends(get_db)
):
    """Обновление access токена"""
    await enforce_rate_limit(
        request, "auth-refresh", 20, 60, token_data.refresh_token
    )
    payload = decode_token(token_data.refresh_token)

    if not payload or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user"
        )

    access_token = create_access_token(data={"sub": str(user.id)})

    return {"access_token": access_token}


@router.post("/logout")
async def logout():
    """Логаут (в реальном приложении - добавить токен в Redis blacklist)"""
    return {"message": "Successfully logged out"}


@router.get("/me", response_model=UserResponse)
async def get_me(
    current_user: User = Depends(get_current_user)
):
    """Получение текущего пользователя"""
    return current_user
