#!/usr/bin/env python3
"""
Тестирование API Bybit на доступность данных за 2025 год
"""

import requests
import json
from datetime import datetime

def test_bybit_api_2025():
    """Тестирует доступность данных за 2025 год"""
    
    base_url = 'https://api.bybit.com'
    
    # Тестовые параметры для 2025 года
    test_params = [
        {
            'market': 'spot',
            'symbol': 'SOLUSDT',
            'start_date': '2025-01-01',
            'end_date': '2025-01-02'
        },
        {
            'market': 'linear',
            'symbol': 'SOLUSDT', 
            'start_date': '2025-01-01',
            'end_date': '2025-01-02'
        },
        {
            'market': 'spot',
            'symbol': 'SOLUSDT',
            'start_date': '2025-09-01',
            'end_date': '2025-09-02'
        }
    ]
    
    for params in test_params:
        print(f"\n🔍 Тестирую {params['market']} {params['symbol']} с {params['start_date']} по {params['end_date']}")
        
        # Конвертируем даты в миллисекунды
        start_ts = int(datetime.strptime(params['start_date'], '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.strptime(params['end_date'], '%Y-%m-%d').timestamp() * 1000)
        
        url = f"{base_url}/v5/market/kline"
        
        api_params = {
            'category': params['market'],
            'symbol': params['symbol'],
            'interval': '1',  # 1 минута
            'start': str(start_ts),
            'end': str(end_ts),
            'limit': 10
        }
        
        print(f"📡 URL: {url}")
        print(f"📋 Параметры: {api_params}")
        
        try:
            response = requests.get(url, params=api_params)
            print(f"📊 Статус: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Ответ API:")
                print(f"   retCode: {data.get('retCode')}")
                print(f"   retMsg: {data.get('retMsg')}")
                
                if data.get('retCode') == 0:
                    result = data.get('result', {})
                    list_data = result.get('list', [])
                    print(f"   📈 Получено записей: {len(list_data)}")
                    
                    if list_data:
                        print(f"   📅 Первая запись: {list_data[0]}")
                        print(f"   📅 Последняя запись: {list_data[-1]}")
                    else:
                        print("   ⚠️ Нет данных в ответе")
                else:
                    print(f"   ❌ Ошибка API: {data.get('retMsg')}")
            else:
                print(f"❌ HTTP ошибка: {response.text}")
                
        except Exception as e:
            print(f"❌ Ошибка запроса: {e}")
        
        print("-" * 50)

if __name__ == "__main__":
    test_bybit_api_2025()
