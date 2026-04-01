# NL Table API

SaaS платформа аналитики Wildberries — бэкенд на FastAPI

## Стек

- **FastAPI** — веб-фреймворк
- **PostgreSQL** — база данных
- **Redis** — кэширование и очереди
- **Celery** — фоновые задачи
- **Alembic** — миграции БД

## Структура проекта

```
nl-backend/
├── api/v1/          # API роуты
├── core/            # Конфигурация, БД, безопасность
├── models/          # SQLAlchemy модели
├── schemas/         # Pydantic схемы
├── services/        # WB API клиент + синхронизация
├── tasks/           # Celery задачи
├── alembic/         # Миграции БД
├── tests/           # Тесты
└── main.py          # FastAPI приложение
```

## Спринты

| Спринт | Статус | Описание |
|--------|--------|----------|
| Спринт 1 | ✅ | Инфраструктура и БД |
| Спринт 2 | 🚧 | Авторизация (JWT, регистрация, логин) |
| Спринт 3 | 🚧 | Организации и роли |
| Спринт 4 | 🚧 | WB API клиент + шифрование |
| Спринт 5 | 🚧 | Синхронизация данных |
| Спринт 6 | 🚧 | Дашборд |

## Запуск

### Локально

```bash
# Создание .env файла
cp .env.example .env

# Запуск всех сервисов
docker-compose up -d

# Применение миграций
docker-compose exec app alembic upgrade head

# Логи
docker-compose logs -f app

# Остановка
docker-compose down
```

### API endpoints

- **Health check:** `GET /`
- **Health + БД:** `GET /health`
- **Swagger docs:** `http://localhost:8000/docs`

## Настройка переменных окружения

См. `.env.example`

Основные переменные:
- `DATABASE_URL` — строка подключения к PostgreSQL
- `SECRET_KEY` — секрет для JWT токенов
- `REDIS_URL` — строка подключения к Redis
- `WB_API_BASE_URL` — базовый URL WB API
- `ENCRYPTION_KEY` — ключ для шифрования (32 байта)

## Лицензия

MIT
