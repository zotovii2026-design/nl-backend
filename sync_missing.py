#!/usr/bin/env python3
"""Подтягивает недостающие товары через WB API и обновляет photo_main в tech_status"""
import asyncio
import httpx
import asyncpg
import json

ORG_ID = '273d9c9a-c3fb-4a8c-99d3-03604eacc364'
DB_URL = 'postgresql://postgres:postgres@localhost:5432/nl_table'
ENCRYPTION_KEY = None  # получим из env

# Все карточки WB через content/v2/get/cards/list
CARDS_URL = 'https://content-api.wildberries.ru/content/v2/get/cards/list'

async def main():
    # 1. Получаем зашифрованный ключ из БД
    conn = await asyncpg.connect(DB_URL)
    
    row = await conn.fetchrow('SELECT encrypted_key FROM wb_api_keys WHERE organization_id = $1', ORG_ID)
    if not row:
        print("Нет WB API ключей!")
        await conn.close()
        return
    
    # 2. Расшифровываем ключ (Fernet)
    import os
    from cryptography.fernet import Fernet
    
    # Читаем ENCRYPTION_KEY из env
    import subprocess
    enc_key = subprocess.check_output(
        'docker exec nl-backend-app env | grep ENCRYPTION_KEY | cut -d= -f2',
        shell=True
    ).decode().strip()
    f = Fernet(enc_key.encode())
    api_key = f.decrypt(row['encrypted_key'].encode()).decode()
    
    # 3. Получаем ВСЕ карточки из WB (с пагинацией)
    missing_nmids = await conn.fetch("""
        SELECT DISTINCT ts.nm_id FROM tech_status ts
        LEFT JOIN wb_products wp ON wp.nm_id = ts.nm_id
        WHERE wp.nm_id IS NULL
    """)
    missing_set = {r['nm_id'] for r in missing_nmids}
    print(f"Недостающих nm_id: {len(missing_set)}")
    
    headers = {'Authorization': api_key}
    all_cards = []
    offset = 0
    limit = 100
    
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            payload = {
                "settings": {
                    "cursor": {"limit": limit, "offset": offset},
                    "filter": [{"type": "objectID", "selected": list(missing_set)}]  
                }
            }
            # WB API использует другой формат фильтрации - попробуем без фильтра
            payload_simple = {
                "settings": {
                    "cursor": {"limit": limit, "offset": offset},
                    "filter": []
                }
            }
            
            try:
                resp = await client.post(CARDS_URL, headers=headers, json=payload_simple)
                if resp.status_code == 429:
                    print("Rate limited, ждём 60с...")
                    await asyncio.sleep(60)
                    continue
                    
                data = resp.json()
                cards = data.get('data', {}).get('cards', data.get('cards', []))
                print(f"Страница offset={offset}: получено {len(cards)} карточек")
                
                if not cards:
                    break
                    
                all_cards.extend(cards)
                offset += limit
                
                if len(cards) < limit:
                    break
                    
                await asyncio.sleep(2)  # пауза между запросами
                
            except Exception as e:
                print(f"Ошибка: {e}")
                break
    
    print(f"Всего карточек: {len(all_cards)}")
    
    # 4. Индексируем по nm_id
    cards_by_nm = {}
    for card in all_cards:
        nm_id = card.get('nm_id') or card.get('imt_id')
        if nm_id:
            cards_by_nm[nm_id] = card
    
    # 5. Обновляем
    updated_products = 0
    updated_photos = 0
    still_missing = []
    
    for nm_id in missing_set:
        card = cards_by_nm.get(nm_id)
        if not card:
            still_missing.append(nm_id)
            continue
        
        # Фото
        photo_url = ''
        photos = card.get('photos', [])
        if photos:
            photo_url = photos[0].get('big', '')
        
        # Проверяем/создаём wb_products
        exists = await conn.fetchrow('SELECT id FROM wb_products WHERE nm_id = $1', nm_id)
        
        vendor_code = card.get('vendorCode', '')
        name = card.get('title', card.get('name', ''))
        brand = card.get('brand', '')
        subject = card.get('subject', '')
        price = card.get('sizes', [{}])[0].get('price', 0) if card.get('sizes') else 0
        
        if not exists:
            await conn.execute("""
                INSERT INTO wb_products (organization_id, nm_id, vendor_code, name, brand, subject, photo_url)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, ORG_ID, nm_id, vendor_code, name, brand, subject, photo_url)
            updated_products += 1
            print(f"  + {nm_id}: {name[:50]} photo={'yes' if photo_url else 'no'}")
        
        # Обновляем photo_main в tech_status
        if photo_url:
            result = await conn.execute("""
                UPDATE tech_status SET photo_main = $1 WHERE nm_id = $2 AND (photo_main IS NULL OR photo_main = '')
            """, photo_url, nm_id)
            if 'UPDATE 1' in result or 'UPDATE 2' in result:
                updated_photos += 1
    
    print(f"\n=== ИТОГО ===")
    print(f"wb_products добавлено: {updated_products}")
    print(f"photo_main обновлено: {updated_photos}")
    print(f"Всё ещё без данных: {still_missing}")
    
    await conn.close()

asyncio.run(main())
