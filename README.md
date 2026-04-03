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
| Спринт 5 | ✅ | Синхронизация данных (Celery + Beat) |
| Спринт 6 | 🚧 | Дашборд |

---

## Спринт 5 — Синхронизация данных (Celery + Beat)

### Что создано:

1. **Celery задачи** (`tasks/celery_app_new.py`)
   - `sync_wb_products` — синхронизация товаров (каждые 30 минут)
   - `sync_wb_sales` — синхронизация продаж (каждый час)
   - `sync_wb_orders` — синхронизация заказов (каждые 2 часа)
   - `sync_organization_data` — полная синхронизация организации
   - `cleanup_old_sync_logs` — очистка старых логов

2. **Модель логов синхронизации** (`models/sync.py`)
   - SyncLog: task_name, status, synced_count, error_message
   - Время выполнения и длительность

3. **API для управления синхронизацией** (`api/v1/sync.py`)
   - `POST /api/v1/organizations/{org_id}/sync/products` — товары
   - `POST /api/v1/organizations/{org_id}/sync/sales` — продажи
   - `POST /api/v1/organizations/{org_id}/sync/orders` — заказы
   - `POST /api/v1/organizations/{org_id}/sync/full` — полная
   - `GET /api/v1/organizations/{org_id}/sync/status/{task_id}` — статус
   - `GET /api/v1/organizations/{org_id}/sync/logs` — логи

4. **Миграции БД**
   - `003_add_sync_logs_table.py` — таблица sync_logs
   - `004_add_sync_model.py` — регистрация модели

5. **Тестирование** (`tests/test_sync_tasks.py`)
   - Тесты всех Celery задач
   - Тесты API endpoints

### Расписание (Celery Beat):

| Задача | Расписание | Описание |
|--------|------------|-----------|
| sync_wb_products | Каждые 30 минут | Товары |
| sync_wb_sales | Каждый час | Продажи |
| sync_wb_orders | Каждые 2 часа | Заказы |

### Пример запроса:

```bash
# Запуск полной синхронизации
curl -X POST http://localhost:8000/api/v1/organizations/{org_id}/sync/full \
  -H "Authorization: Bearer <access_token>"

# Проверка статуса задачи
curl http://localhost:8000/api/v1/organizations/{org_id}/sync/status/{task_id} \
  -H "Authorization: Bearer <access_token>"
```

### Запуск тестов:

```bash
pip install -r requirements-dev.txt
pytest tests/test_sync_tasks.py -v
```

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
