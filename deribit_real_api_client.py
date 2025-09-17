#!/usr/bin/env python3
import requests
import sqlite3
from datetime import datetime, date
import time

# Функция для преобразования datetime в timestamp
def datetime_to_timestamp(datetime_obj):
    """Converts a datetime object to a Unix timestamp in milliseconds."""
    return int(datetime_obj.timestamp() * 1000)

# Функция для преобразования timestamp в datetime
def timestamp_to_datetime(timestamp):
    """Converts a Unix timestamp in milliseconds to a datetime object."""
    return datetime.fromtimestamp(timestamp / 1000)

# Функция для получения данных о сделках с реального API Deribit
def get_real_trades(currency, kind, start_timestamp, end_timestamp, count=1000):
    """
    Получение данных о сделках с реального API Deribit
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_timestamp (int): Начальное время в миллисекундах
        end_timestamp (int): Конечное время в миллисекундах
        count (int): Количество записей на запрос (максимум 1000 для Deribit)
        
    Returns:
        list: Список сделок
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
        
        # Парсим JSON ответ
        data = response.json()
        
        # Проверяем на ошибки API
        if 'error' in data and data['error'] is not None:
            print(f"Ошибка API: {data['error']}")
            return []
        
        # Извлекаем сделки из ответа
        trades = data.get('result', {}).get('trades', [])
        print(f"Получено {len(trades)} сделок")
        
        return trades
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return []
    except Exception as e:
        print(f"Ошибка при обработке ответа: {e}")
        return []

# Функция для постраничной загрузки всех сделок в заданном диапазоне времени
def get_all_trades_paginated(currency, kind, start_timestamp, end_timestamp, count=1000):
    """
    Постраничная загрузка всех сделок в заданном диапазоне времени
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_timestamp (int): Начальное время в миллисекундах
        end_timestamp (int): Конечное время в миллисекундах
        count (int): Количество записей на запрос
        
    Returns:
        list: Список всех сделок
    """
    all_trades = []
    current_start = start_timestamp
    
    print(f"Начало постраничной загрузки данных с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    
    while current_start < end_timestamp:
        print(f"Загрузка данных с {timestamp_to_datetime(current_start)}")
        
        # Получаем сделки для текущей страницы
        trades = get_real_trades(currency, kind, current_start, end_timestamp, count)
        
        if not trades:
            print("Больше сделок не найдено")
            break
        
        # Добавляем сделки в общий список
        all_trades.extend(trades)
        print(f"Скачано {len(trades)} сделок. Всего: {len(all_trades)}")
        
        # Определяем новое начальное время для следующей страницы
        # Это максимальный timestamp из полученных сделок + 1
        max_timestamp = max(trade['timestamp'] for trade in trades)
        current_start = max_timestamp + 1
        
        # Проверяем, достигли ли мы конца запрашиваемого периода
        if max_timestamp >= end_timestamp:
            print("Достигнут конец запрашиваемого периода")
            break
        
        # Добавляем небольшую задержку между запросами, чтобы не перегружать API
        time.sleep(0.1)
    
    print(f"Загрузка завершена. Всего получено {len(all_trades)} сделок")
    return all_trades

# Тестовая функция
def main():
    """Тестовая функция для проверки работы API-клиента"""
    # Тестовые параметры (один час в январе 2023 года)
    currency = 'BTC'
    kind = 'option'
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    end_date = datetime(2023, 1, 1, 1, 0, 0)
    
    start_timestamp = datetime_to_timestamp(start_date)
    end_timestamp = datetime_to_timestamp(end_date)
    
    print("Тестовый запуск на коротком интервале (1 час)")
    print(f"Период: с {start_date} по {end_date}")
    
    # Получаем сделки
    trades = get_all_trades_paginated(currency, kind, start_timestamp, end_timestamp, 1000)
    
    print(f"\nРезультаты теста:")
    print(f"Получено сделок: {len(trades)}")
    
    if trades:
        # Показываем первые несколько сделок
        print(f"\nПримеры сделок:")
        for i, trade in enumerate(trades[:5]):
            print(f"  {i+1}. {trade['trade_id']}: {trade['instrument_name']} - {trade['amount']} @ {trade['price']}")

if __name__ == "__main__":
    main()