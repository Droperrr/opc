#!/usr/bin/env python3
import requests
from datetime import datetime

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
        
        # Если есть сделки, показываем время первой и последней сделки
        if trades:
            first_trade_time = trades[0]['timestamp']
            last_trade_time = trades[-1]['timestamp']
            max_trade_time = max(trade['timestamp'] for trade in trades)
            
            print(f"Время первой сделки: {timestamp_to_datetime(first_trade_time)}")
            print(f"Время последней сделки: {timestamp_to_datetime(last_trade_time)}")
            print(f"Максимальное время сделки: {timestamp_to_datetime(max_trade_time)}")
            print(f"Запрошенное конечное время: {timestamp_to_datetime(end_timestamp)}")
            print(f"Разница между максимальным временем и конечным временем: {end_timestamp - max_trade_time} мс")
        
        return trades
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return []
    except Exception as e:
        print(f"Ошибка при обработке ответа: {e}")
        return []

# Тестовая функция
def main():
    """Тестовая функция для отладки работы API"""
    # Параметры для января 2023 года
    currency = 'BTC'
    kind = 'option'
    start_timestamp = 1672531200000  # 2023-01-01 00:00:00
    end_timestamp = 9999999999999    # Очень большое значение
    
    print("Отладка получения сделок за январь 2023 года")
    print(f"Период: с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    
    # Получаем сделки
    trades = get_real_trades(currency, kind, start_timestamp, end_timestamp, 1000)
    
    print(f"\nРезультаты:")
    print(f"Получено сделок: {len(trades)}")

if __name__ == "__main__":
    main()