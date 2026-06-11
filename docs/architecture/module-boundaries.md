# NL Table: границы модулей

## 1. Архитектурное решение

Целевая форма - модульный монолит. Все модули работают в одном приложении и
одной БД, но имеют явных владельцев маршрутов, бизнес-логики и записи таблиц.

Предлагаемая структура:

```text
api/v1/routers/             HTTP-контракты
services/                   сценарии приложения и бизнес-правила
repositories/               SQL и операции хранения
domain/                     чистые формулы и типы предметной области
tasks/                      запуск фоновых сценариев
integrations/wb/            клиент WB API и адаптеры ответов
templates/                  HTML
static/js/                  клиентские модули
core/                       конфигурация, БД, безопасность, Celery
```

Физическое перемещение файлов выполняется позже и постепенно. На этом этапе
структура фиксирует ответственность.

## 2. Матрица модулей

| Модуль | Ответственность | Маршруты текущего API | Владелец записи |
|---|---|---|---|
| `identity` | Пользователи, JWT, вход и регистрация | `/auth/*`, `/api/v1/nl/register`, `/login`, `/me`, `/profile` | `users` |
| `organizations` | Организации, членство, приглашения, налоги | `/organizations/*`, NL organizations, invite, rename, tax-settings | `organizations`, `memberships`, `invitations` |
| `wb_credentials` | Ключи WB и проверка токена | `/organizations/{id}/wb-keys/*`, NL wb-keys, connect-wb, verify-wb-key | `wb_api_keys` |
| `catalog` | Карточки, размеры, сущности и штрихкоды | `/products`, внутренние сервисы синхронизации | `wb_products`, `product_entities`, `entity_barcodes`, `unmatched_barcodes`, `raw_barcodes` |
| `sync` | Получение, журналирование и нормализация WB-данных | `/sync/*`, фоновые `wb.*` | `raw_api_data`, `warehouse_refs`, `tech_status`, `sync_logs`, WB-проекции |
| `analytics` | Основные показатели, товарная аналитика, склады, РНП, потребность FBO | `/dates`, `/control`, `/analytics`, `/warehouses`, `/rnp`, `/fbo-needs`, `/opiu` | Только свои будущие настройки; основные таблицы читает |
| `reference` | Справочник, себестоимость, габариты и настройки товара | `/reference`, `/cost-prices*`, `/commission-rate` | `reference_book`, временно `reference_sheet` |
| `unit_economics` | Финансовые формулы, факт/план и ручные значения | `/unit-economics`, `/prices/refresh`, `/prices/last-refresh` | `unit_economics_user` |
| `tariffs` | Тарифы ФБО/ФБС, комиссии и снимки | `/fbs-warehouses`, внутренние задачи тарифов | `wb_box_tariffs`, `wb_tariff_snapshot` |
| `advertising` | Кампании WB и рекламная статистика | `/ad-stats`, `/ad-stats/by-art`, `/marketer/*` | `ad_campaigns`, `ad_stats`, `ad_stats_nm` |
| `external_advertising` | Ручной учет внешней рекламы | `/external-ads*` | `external_ads` |
| `promotions` | Акции WB и товары в акциях | `/promotions*` | `wb_promotions`, `wb_promotion_products` |
| `sales_plans` | Планы продаж | `/sales-plans*` | `sales_plans` |
| `operations` | Операционные расходы | `/operating-expenses*` | Таблица операционных расходов после формализации модели |
| `content_tools` | Продавцы и SEO-ключи | `/sellers*`, `/seo-keywords*` | `sellers`, `seo_keywords` |
| `admin` | Техническая диагностика | `/admin/tech`, `/health` | Не владеет бизнес-таблицами |
| `web_ui` | Страницы и клиентская оркестрация | `/nl/register`, `/nl/login`, `/nl/v2` | Не пишет в БД напрямую |

## 3. Владение таблицами

Правило: одна таблица имеет одного владельца записи. Другие модули читают ее
через сервис владельца или стабильный read model.

| Таблица | Владелец | Разрешенные потребители |
|---|---|---|
| `users` | `identity` | `organizations`, `admin` |
| `organizations` | `organizations` | Все организационные модули, только чтение |
| `memberships`, `invitations` | `organizations` | `identity`, проверки доступа |
| `wb_api_keys` | `wb_credentials` | `sync`, только расшифрованный ключ на время вызова |
| `wb_products` | `catalog` | `analytics`, `reference`, `advertising` |
| `product_entities` | `catalog` | Все товарные модули, только чтение |
| `entity_barcodes`, `unmatched_barcodes`, `raw_barcodes` | `catalog` | `sync` через сервис сопоставления |
| `raw_api_data` | `sync` | Нормализаторы и диагностика |
| `warehouse_refs` | `sync` | `analytics`, `tariffs` |
| `tech_status` | `sync` | `analytics`, `reference`, `unit_economics` |
| `reference_book`, `reference_sheet` | `reference` | `unit_economics`, `analytics`, `sales_plans` |
| `unit_economics_user` | `unit_economics` | Только `unit_economics` |
| `wb_box_tariffs`, `wb_tariff_snapshot` | `tariffs` | `reference`, `unit_economics` |
| `ad_campaigns`, `ad_stats`, `ad_stats_nm` | `advertising` | `analytics`, `unit_economics` через публичные агрегаты |
| `external_ads` | `external_advertising` | `analytics` через агрегаты |
| `wb_promotions`, `wb_promotion_products` | `promotions` | `reference`, `analytics` только чтение |
| `sales_plans` | `sales_plans` | `analytics` |
| `sync_logs` | `sync` | `admin`, мониторинг |

## 4. Разрешенные зависимости

```text
web_ui -> HTTP API

API router -> application service
application service -> domain
application service -> repository
application service -> public service другого модуля
repository -> SQLAlchemy model / PostgreSQL
sync service -> WB integration
Celery task -> application service
```

Общие инфраструктурные зависимости:

- `core.config`;
- `core.database`;
- `core.security`;
- единая проверка пользователя и членства в организации;
- единое Celery-приложение.

## 5. Запрещенные зависимости

- Роутер содержит многошаговую бизнес-логику, формулу или большой SQL.
- Celery-задача дублирует бизнес-логику HTTP-сценария.
- Сервис импортирует роутер или код интерфейса.
- Модель SQLAlchemy импортирует сервис.
- Один модуль напрямую изменяет таблицу другого модуля.
- Бизнес-модуль расшифровывает WB-ключ самостоятельно.
- `org_id` принимается как доказательство доступа без проверки membership.
- Frontend зависит от внутреннего имени Python-функции или таблицы БД.

## 6. Текущие нарушения и технический долг

Зафиксировано на 2026-06-11:

1. `api/v1/nl.py`: 9693 строки и 62 маршрута; в одном файле находятся HTTP,
   SQL, формулы, авторизация, организации и HTML.
2. Авторизация и организации реализованы и в отдельных роутерах, и в `nl.py`.
3. В `nl.py` два обработчика `GET /api/v1/nl/opiu`.
4. В `models/sync.py` и `models/wb_data.py` объявлены классы `SyncLog` для
   таблицы `sync_logs`.
5. Существуют `tasks/celery_app.py` и фактически используемый
   `tasks/celery_app_new.py`.
6. `tasks/daily_sync.py`, `tasks/scheduled_sync.py`, `tasks/wb_sync.py` и
   `tasks/stocks_sync.py` частично перекрываются по ответственности.
7. `scheduled_sync.py` содержит получение WB-данных, retry, запись raw,
   нормализацию, SQL и запуск пересчета.
8. Часть маршрутов `nl.py` использует Bearer JWT, часть query token, часть
   только `org_id`; необходим отдельный аудит безопасности.
9. HTML и значительная клиентская оркестрация находятся в Python-строке.
10. В коде используются таблицы `ad_campaigns`, `ad_stats`, `ad_stats_nm`,
    `sellers`, `seo_keywords`, которые не представлены отдельными моделями
    SQLAlchemy в текущем каталоге `models`.

Эти пункты не исправляются в рамках карты системы.

## 7. Порядок безопасного выделения модулей

Каждый этап начинается с characterization-тестов текущего контракта и
заканчивается отдельным коммитом.

1. Единая функция получения пользователя и проверки членства организации.
2. Сопоставление и устранение дублей `identity`, `organizations`,
   `wb_credentials`.
3. Выделение `promotions`: компактная область с отдельными моделями и JS.
4. Выделение `sales_plans`.
5. Выделение `external_advertising`.
6. Выделение `advertising`.
7. Выделение `reference` и тарифного чтения.
8. Выделение `analytics`.
9. Выделение расчетного ядра `unit_economics`.
10. Упорядочивание `sync` и объединение Celery-приложения.
11. Вынос HTML из `nl.py` в шаблоны после стабилизации API.

## 8. Контроль переноса одного модуля

Для каждого модуля:

1. Записать список маршрутов, входов, ответов и кодов ошибок.
2. Добавить тесты текущего поведения и изоляции организаций.
3. Вынести чистые формулы и сервисы без изменения маршрута.
4. Вынести SQL в repository.
5. Подключить отдельный router с прежним URL.
6. Сравнить ответы до и после на тестовых данных.
7. Проверить frontend-сценарий.
8. Удалить старый код только после подтверждения эквивалентности.

## 9. Критерии завершения будущего рефакторинга

- У каждого маршрута есть один обработчик.
- У каждой таблицы есть один владелец записи.
- Все приватные маршруты проверяют пользователя и membership.
- Бизнес-формулы тестируются без FastAPI и БД.
- Celery и HTTP вызывают одни и те же application services.
- `nl.py` перестает быть владельцем бизнес-логики.
- Переезд на другой сервер или URL требует изменения конфигурации, а не кода.
