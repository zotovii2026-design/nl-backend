# NL Table: потоки данных

## 1. Общая схема

```text
Wildberries API
    |
    v
Celery beat -> Celery worker -> WB API client
                                  |
                                  v
                         raw_api_data / WB-таблицы
                                  |
                                  v
                    нормализация и entity matching
                                  |
                                  v
              product_entities / entity_barcodes / tech_status
                                  |
                                  v
                    API чтения и бизнес-расчеты
                                  |
                                  v
                       HTML + JS + Tabulator
```

`organization_id` является обязательной границей изоляции данных. Каждый
запрос пользователя и каждый фоновый сценарий должен выполняться в контексте
конкретной организации.

## 2. Карточки и товарные сущности

Источник:

- WB Content API, `POST /content/v2/get/cards/list`;
- задача `wb.sched.products`;
- клиент `services/wb_api/client.py`.

Путь:

```text
WB Content API
  -> scheduled_sync._do_products
  -> raw_api_data(api_method=products)
  -> entity_sync.sync_entities_from_raw
  -> product_entities
  -> entity_barcodes
  -> unmatched_barcodes, если сопоставление невозможно
```

Владелец записи `product_entities`, `entity_barcodes` и
`unmatched_barcodes` - модуль `catalog`.

Стабильная сущность товара определяется сочетанием организации, артикула WB и
размера. Штрихкод является изменяемой исторической привязкой к сущности.

## 3. Заказы, продажи, остатки и аналитический статус

Источники:

- WB Statistics API;
- WB Seller Analytics API;
- задачи `wb.sched.orders`, `wb.sched.sales`, `wb.sched.stocks_fbo`,
  `wb.sched.warehouses`.

Путь:

```text
WB API
  -> scheduled_sync
  -> raw_api_data
  -> wb.sched.parse_raw
  -> сопоставление barcode/nm_id с ProductEntity
  -> tech_status
  -> /control, /analytics, /warehouses, /rnp, /fbo-needs
  -> разделы интерфейса
```

Правила:

- WB может корректировать данные задним числом, поэтому используется окно
  повторной обработки.
- Данные дня заменяются целиком, а не суммируются с предыдущей загрузкой.
- Остаток является моментальным срезом.
- Ошибка одной организации не должна останавливать обработку остальных.

Владелец получения и нормализации - `sync`. Владелец чтения агрегатов -
`analytics`. Таблица `tech_status` является проекцией чтения и заполняется
только процессом нормализации.

## 4. Справочник и себестоимость

Путь:

```text
Пользователь
  -> cost-grid.js
  -> /cost-prices, /cost-prices/batch, /cost-prices/upload
  -> reference service
  -> reference_book
```

Связанные источники:

- `product_entities` - идентификатор товара и размера;
- `wb_box_tariffs` - логистика ФБО/ФБС;
- `wb_tariff_snapshot` - комиссии и тарифные снимки;
- `organizations` - налоговые настройки.

Владелец записи `reference_book` - `reference`. Остальные модули получают
справочные данные через публичный сервис чтения и не изменяют таблицу напрямую.

## 5. Юнит-экономика

Путь:

```text
reference_book + product_entities + tech_status
    + wb_tariff_snapshot + wb_box_tariffs
    + unit_economics_user + organizations
  -> unit_economics calculation service
  -> /unit-economics
  -> ue-grid.js
```

Ручные изменения пользователя:

```text
ue-grid.js
  -> POST /unit-economics
  -> unit_economics_user
```

Владелец формул и записи `unit_economics_user` - `unit_economics`. Модуль
может читать публичные представления `catalog`, `reference`, `analytics`,
`tariffs` и `organizations`, но не должен записывать в их таблицы.

## 6. Тарифы и цены

Путь тарифов:

```text
WB API
  -> wb.sched.tariffs / wb.sched.box_tariffs / wb.sched.tariff_snapshot
  -> wb_box_tariffs / wb_tariff_snapshot / raw_api_data
  -> reference и unit_economics
```

Путь цен:

```text
WB discounts-prices API
  -> wb.sched.prices или /prices/refresh
  -> tech_status и/или тарифная проекция
  -> unit_economics
```

Целевой владелец интеграции и записи - `sync.tariffs`. Публичное чтение
предоставляется модулям `reference` и `unit_economics`.

## 7. Реклама WB

Путь:

```text
WB Promotion API
  -> wb.sched.adverts
  -> ad_campaigns

WB fullstats API
  -> wb.sched.ad_stats
  -> ad_stats + ad_stats_nm
  -> /ad-stats + /ad-stats/by-art
  -> ads-grid.js + ads-arts-grid.js
```

Владелец таблиц рекламы и расчетов рекламных метрик - `advertising`.
Атрибуция и состав кампании являются разными понятиями и не должны смешиваться
без явно определенного сценария.

## 8. Внешняя реклама

```text
Пользователь
  -> интерфейс внешней рекламы
  -> /external-ads
  -> external_ads
```

Владелец таблицы и CRUD - `external_advertising`. Модуль читает каталог для
привязки к `product_entities`, но не изменяет товарные сущности.

## 9. Акции

```text
WB Promotions API
  -> wb.sched.promo_sync
  -> wb_promotions + wb_promotion_products
  -> /promotions + /promotions/products
  -> promo-grid.js

Excel пользователя
  -> /promotions/upload-excel
  -> wb_promotion_products
```

Владелец таблиц и правил сохранения - `promotions`.

## 10. План продаж

```text
Пользователь
  -> раздел "План продаж"
  -> /sales-plans
  -> sales_plans
```

Владелец записи `sales_plans` - `sales_plans`. Для выбора товара модуль читает
каталог, но не меняет его.

## 11. Авторизация и организация

Целевой путь:

```text
JWT -> get_current_user -> membership check -> organization context
     -> бизнес-сценарий конкретного модуля
```

Проверка одного лишь формата `org_id` недостаточна. Перед чтением или записью
организационных данных необходимо подтвердить членство пользователя либо
системный контекст фоновой задачи.

Сейчас в проекте одновременно существуют основные роутеры `auth.py`,
`organizations.py`, `wb_keys.py` и похожие маршруты внутри `nl.py`. До удаления
дублей требуется сравнить контракты и потребителей фронтенда.
