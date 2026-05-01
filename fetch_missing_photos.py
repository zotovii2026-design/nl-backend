import asyncio
from services.photo_fetch import fetch_photos_batch
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text as sql_text

DB = 'postgresql+asyncpg://postgres:postgres@1f9b661a07cd_nl-backend-postgres:5432/nl_table'

async def run():
    engine = create_async_engine(DB)
    async with engine.connect() as conn:
        rows = await conn.execute(sql_text(
            "SELECT DISTINCT nm_id FROM tech_status WHERE photo_main IS NULL OR photo_main = ''"
        ))
        nm_ids = [r[0] for r in rows.fetchall()]
    print(f'Total without photo: {len(nm_ids)}')
    
    all_photos = {}
    for i in range(0, len(nm_ids), 10):
        batch = nm_ids[i:i+10]
        photos = await fetch_photos_batch(batch)
        all_photos.update(photos)
        print(f'Batch {i//10+1}: found {len(photos)}/{len(batch)}')
    print(f'Total found: {len(all_photos)}')
    
    if all_photos:
        updated = 0
        async with engine.connect() as conn:
            for nm_id, url in all_photos.items():
                r = await conn.execute(sql_text(
                    "UPDATE tech_status SET photo_main = :url WHERE nm_id = :nm AND (photo_main IS NULL OR photo_main = '')"
                ).bindparams(url=url, nm=nm_id))
                updated += r.rowcount
            await conn.commit()
        print(f'Updated {updated} rows in tech_status')
    else:
        print('No new photos found - these items do not exist on WB CDN')
    
    await engine.dispose()

asyncio.run(run())
