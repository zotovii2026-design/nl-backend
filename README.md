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
| Спринт 2 | ✅ | Авторизация (JWT, регистрация, логин) |
| Спринт 3 | ✅ | Организации и роли |
| Спринт 4 | ✅ | WB API клиент + шифрование ключей |
| Спринт 5 | 🚧 | Синхронизация данных |
| Спринт 6 | 🚧 | Дашборд |

---

## Спринт 4 — WB API клиент и шифрование

### Что создано:

1. **WB API клиент** (`services/wb_api/client.py`)
   - HTTP клиент для WB API (httpx)
   - Методы: get_products, get_sales, get_orders, get_reports
   - Проверка подключения (test_connection)

2. **Управление WB ключами** (`api/v1/wb_keys.py`, `services/wb_api/keys.py`)
   - POST `/api/v1/organizations/{org_id}/wb-keys` — добавление (с шифрованием)
   - GET `/api/v1/organizations/{org_id}/wb-keys` — список (без значений)
   - DELETE `/api/v1/organizations/{org_id}/wb-keys/{key_id}` — удаление
   - Шифрование/дешифрование через Fernet

3. **Тестирование** (`tests/test_wb_api.py`)
   - Тесты шифрования/дешифрования
   - Тесты API endpoints
   - Тесты WB API клиента

4. **Миграция БД**
   - `002_add_wb_api_keys_table.py` — таблица wb_api_keys

### Пример запроса:

```bash
# Добавление WB API ключа
curl -X POST http://localhost:8000/api/v1/organizations/{org_id}/wb-keys \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"name":"Основной ключ","api_key":"wb_api_key_here"}'
```

### Запуск тестов:

```bash
pip install -r requirements-dev.txt
pytest tests/test_wb_api.py -v
```

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
