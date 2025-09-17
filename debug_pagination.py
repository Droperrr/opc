#!/usr/bin/env python3
import requests
import json
from datetime import datetime

def datetime_to_timestamp(datetime_obj):
    """Converts a datetime object to a Unix timestamp in milliseconds."""
    return int(datetime_obj.timestamp() * 1000)

def timestamp_to_datetime(timestamp):
    """Converts a Unix timestamp in milliseconds to a datetime object."""
    return datetime.fromtimestamp(timestamp / 1000)

def get_raw_api_response(currency, kind, start_timestamp, end_timestamp, count=1000):
    """
    Получение сырого ответа от API Deribit
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_timestamp (int): Начальное время в миллисекундах
        end_timestamp (int): Конечное время в миллисекундах
        count (int): Количество записей на запрос (максимум 1000 для Deribit)
        
    Returns:
        dict: Сырой ответ от API
    """
    url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time'
    
    # Параметры запроса
    params = {
        "currency": currency,
        "kind": kind,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "count": min(count, 1000),  # Ограничиваем максимальное количество
        "include_old": True
    }
    
    print(f"Запрос к API: {url}")
    print(f"Параметры запроса: {params}")
    
    try:
        # Выполняем запрос к API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Проверяем на ошибки HTTP
        
        # Возвращаем JSON ответ
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    except Exception as e:
        print(f"Ошибка при обработке ответа: {e}")
        return None

def get_all_trades_with_pagination(currency, kind, start_timestamp, end_timestamp, count=1000):
    """
    Получение всех сделок с пагинацией
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_timestamp (int): Начальное время в миллисекундах
        end_timestamp (int): Конечное время в миллисекундах
        count (int): Количество записей на запрос (максимум 1000 для Deribit)
        
    Returns:
        list: Список всех сделок
    """
    all_trades = []
    current_start_timestamp = start_timestamp
    request_count = 0
    max_requests = 100  # Ограничение на количество запросов для предотвращения бесконечного цикла
    
    print(f"Начало сбора данных с пагинацией с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    
    while current_start_timestamp < end_timestamp and request_count < max_requests:
        print(f"\n--- Запрос #{request_count + 1} ---")
        print(f"Текущее время начала: {timestamp_to_datetime(current_start_timestamp)}")
        
        # Получаем ответ от API
        response = get_raw_api_response(currency, kind, current_start_timestamp, end_timestamp, count)
        
        if not response:
            print("Ошибка получения данных от API")
            break
            
        # Проверяем наличие данных
        if 'result' not in response or 'trades' not in response['result']:
            print("В ответе нет данных о сделках")
            break
            
        trades = response['result']['trades']
        has_more = response['result'].get('has_more', False)
        
        print(f"Получено сделок в этом запросе: {len(trades)}")
        print(f"Есть еще данные: {has_more}")
        
        if not trades:
            print("Нет сделок в ответе")
            break
            
        # Добавляем сделки в общий список
        all_trades.extend(trades)
        print(f"Общее количество собранных сделок: {len(all_trades)}")
        
        # Определяем новое время начала для следующего запроса
        # Используем timestamp последней сделки + 1, чтобы не пропустить сделки с одинаковым временем
        last_timestamp = trades[-1]['timestamp']
        current_start_timestamp = last_timestamp + 1
        
        print(f"Следующее время начала: {timestamp_to_datetime(current_start_timestamp)}")
        
        # Проверяем, достигли ли мы конца запрашиваемого периода
        if not has_more or current_start_timestamp > end_timestamp:
            print("Достигнут конец запрашиваемого периода или нет больше данных")
            break
            
        request_count += 1
        
        # Небольшая задержка между запросами
        import time
        time.sleep(0.1)
    
    if request_count >= max_requests:
        print(f"Достигнуто максимальное количество запросов ({max_requests})")
        
    return all_trades

def main():
    """Основная функция для отладки пагинации"""
    print("=== Отладка пагинации API Deribit ===")
    
    # Параметры запроса для тестового периода (15 января 2023 года с 10:00 до 16:00)
    currency = 'BTC'
    kind = 'option'
    start_date = datetime(2023, 1, 15, 10, 0, 0)
    end_date = datetime(2023, 1, 15, 16, 0, 0)
    start_timestamp = datetime_to_timestamp(start_date)
    end_timestamp = datetime_to_timestamp(end_date)
    count = 1000
    
    print(f"Запрашиваем данные за период: {start_date} - {end_date}")
    print(f"Timestamp: {start_timestamp} - {end_timestamp}")
    
    # Получаем все сделки с пагинацией
    all_trades = get_all_trades_with_pagination(currency, kind, start_timestamp, end_timestamp, count)
    
    print(f"\n=== Итоги ===")
    print(f"Общее количество собранных сделок: {len(all_trades)}")
    
    if all_trades:
        print(f"Первая сделка: {all_trades[0]['timestamp']} ({timestamp_to_datetime(all_trades[0]['timestamp'])})")
        print(f"Последняя сделка: {all_trades[-1]['timestamp']} ({timestamp_to_datetime(all_trades[-1]['timestamp'])})")
        
        # Сохраняем все сделки в файл
        with open('all_trades_debug.json', 'w', encoding='utf-8') as f:
            json.dump(all_trades, f, indent=2, ensure_ascii=False)
        print("\nВсе сделки сохранены в файл 'all_trades_debug.json'")
    else:
        print("Не удалось получить сделки")

if __name__ == "__main__":
    main()