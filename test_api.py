import asyncio, httpx, json, sys
sys.path.insert(0, '.')

async def main():
    from core.config import settings
    from core.security import decrypt_data
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from models.organization import WbApiKey

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    sf = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with sf() as db:
        result = await db.execute(select(WbApiKey).limit(1))
        k = result.scalar_one()
        token = decrypt_data(k.personal_token) if k.personal_token else decrypt_data(k.api_key)

    async with httpx.AsyncClient(
        base_url='https://advert-api.wildberries.ru',
        headers={'Authorization': f'Bearer {token}'},
        timeout=30.0,
    ) as client:
        r = await client.get('/adv/v3/fullstats', params={
            'ids': '33167747',
            'beginDate': '2026-05-29',
            'endDate': '2026-05-29'
        })
        data = r.json()
        # Print the FULL structure of one item to understand all fields
        item = data[0] if isinstance(data, list) and data else {}
        day = (item.get('days') or [{}])[0]
        app = (day.get('apps') or [{}])[0]
        nm = (app.get('nms') or [{}])[0]
        print('=== Top level keys ===')
        print(json.dumps({k:v for k,v in item.items() if k != 'days'}, indent=2))
        print('\n=== Day keys ===')
        print(json.dumps({k:v for k,v in day.items() if k != 'apps'}, indent=2))
        print('\n=== App keys ===')
        print(json.dumps({k:v for k,v in app.items() if k != 'nms'}, indent=2))
        print('\n=== NM keys ===')
        print(json.dumps(nm, indent=2))

    await engine.dispose()

asyncio.run(main())
