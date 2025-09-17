#!/usr/bin/env python3
import requests
import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta
import time
import random
import json

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
        end_timestamp (int): Конечное время в миллисекундах (используется для фильтрации, не для запроса к API)
        count (int): Количество записей на запрос
        
    Returns:
        list: Список всех сделок
    """
    all_trades = []
    current_start = start_timestamp
    
    # Используем очень большое значение для end_timestamp в запросах к API
    # Это позволяет получить все доступные данные
    api_end_timestamp = 9999999999999  # Очень большое значение (примерно год 2286)
    
    print(f"Начало постраничной загрузки данных с {timestamp_to_datetime(start_timestamp)}")
    print(f"Конечное время для фильтрации: {timestamp_to_datetime(end_timestamp)}")
    
    while True:
        print(f"Загрузка данных с {timestamp_to_datetime(current_start)}")
        
        # Получаем сделки для текущей страницы
        trades = get_real_trades(currency, kind, current_start, api_end_timestamp, count)
        
        if not trades:
            print("Больше сделок не найдено")
            break
        
        # Добавляем сделки в общий список
        all_trades.extend(trades)
        print(f"Скачано {len(trades)} сделок. Всего: {len(all_trades)}")
        
        # Если получено меньше 1000 сделок, значит, это последняя страница
        if len(trades) < 1000:
            print("Получена последняя страница данных")
            break
        
        # Определяем новое начальное время для следующей страницы
        # Это максимальный timestamp из полученных сделок + 1
        max_timestamp = max(trade['timestamp'] for trade in trades)
        current_start = max_timestamp + 1
        
        # Проверяем, превышает ли максимальное время конечное время для фильтрации
        if max_timestamp > end_timestamp:
            print("Достигнуто конечное время для фильтрации")
            # Фильтруем сделки, оставляя только те, что до конечного времени
            all_trades = [trade for trade in all_trades if trade['timestamp'] <= end_timestamp]
            break
        
        # Добавляем небольшую задержку между запросами, чтобы не перегружать API
        time.sleep(0.1)
    
    print(f"Загрузка завершена. Всего получено {len(all_trades)} сделок")
    return all_trades

# Функция для получения данных о сделках
def get_trades(currency, kind, start_date, end_date, include_old=True, count=1000, db_path='server_opc.db'):
    """
    Получает данные о сделках с Deribit API
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_date (date): Начальная дата
        end_date (date): Конечная дата
        include_old (bool): Включать ли старые данные
        count (int): Количество записей на запрос
        db_path (str): Путь к базе данных
        
    Returns:
        list: Список сделок
    """
    # Проверка входных параметров
    assert isinstance(currency, str), "currency must be a string"
    assert isinstance(start_date, date), "start_date must be a date object"
    assert isinstance(end_date, date), "end_date must be a date object"
    assert start_date <= end_date, "start_date must be before or equal to end_date"
    
    # Подключение к базе данных для проверки существующих данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Поиск максимального timestamp в базе для данной валюты и типа инструмента
    cursor.execute('''
        SELECT MAX(timestamp) FROM deribit_option_trades
        WHERE instrument_name LIKE ?
    ''', (f'{currency}-%',))
    
    result = cursor.fetchone()
    max_timestamp_in_db = result[0] if result[0] is not None else None
    
    conn.close()
    
    # Определение начального timestamp
    end_timestamp = datetime_to_timestamp(datetime.combine(end_date, datetime.max.time()))
    
    if max_timestamp_in_db is not None:
        start_timestamp = max_timestamp_in_db + 1
        print(f"INFO: Обнаружены данные до {timestamp_to_datetime(max_timestamp_in_db)}. Продолжаю сбор с этой точки.")
        # Проверка, что start_timestamp не превышает end_timestamp
        if start_timestamp > end_timestamp:
            print(f"INFO: Данные уже собраны до {timestamp_to_datetime(max_timestamp_in_db)}. Нет необходимости в сборе данных.")
            return []  # Возвращаем пустой список, так как данные уже собраны
    else:
        start_timestamp = datetime_to_timestamp(datetime.combine(start_date, datetime.min.time()))
        print(f"INFO: Начинаю сбор данных с {start_date}")
    
    # Получаем все сделки с постраничной загрузкой
    trades = get_all_trades_paginated(currency, kind, start_timestamp, end_timestamp, count)
    
    return trades

# Функция для создания таблицы в базе данных
def create_trades_table(conn):
    """
    Создает таблицу для хранения сделок опционов в базе данных
    
    Args:
        conn: Подключение к SQLite базе данных
    """
    cursor = conn.cursor()
    
    # Создание таблицы сделок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deribit_option_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT UNIQUE,
            timestamp INTEGER,
            instrument_name TEXT,
            price REAL,
            amount REAL,
            direction TEXT,
            iv REAL,
            settlement_price REAL,
            underlying_price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Создание индексов для ускорения поиска
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp 
        ON deribit_option_trades(timestamp)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_trades_instrument 
        ON deribit_option_trades(instrument_name)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_trades_trade_id 
        ON deribit_option_trades(trade_id)
    ''')
    
    conn.commit()

# Функция для сохранения сделок в базу данных
def save_trades_to_db(conn, trades):
    """
    Сохраняет список сделок в базу данных
    
    Args:
        conn: Подключение к SQLite базе данных
        trades (list): Список сделок
        
    Returns:
        int: Количество успешно сохраненных сделок
    """
    cursor = conn.cursor()
    
    saved_count = 0
    for trade in trades:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO deribit_option_trades 
                (trade_id, timestamp, instrument_name, price, amount, direction, iv, settlement_price, underlying_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade.get('trade_id'),
                trade.get('timestamp'),
                trade.get('instrument_name'),
                trade.get('price'),
                trade.get('amount'),
                trade.get('direction'),
                trade.get('iv'),
                trade.get('settlement_price'),
                trade.get('underlying_price')
            ))
            saved_count += 1
        except Exception as e:
            print(f"Ошибка при сохранении сделки {trade.get('trade_id', 'unknown')}: {e}")
            continue
    
    conn.commit()
    return saved_count

# Основная функция для сбора данных
def collect_trades(currency, start_date, end_date, db_path='server_opc.db'):
    """
    Основная функция для сбора и сохранения сделок
    
    Args:
        currency (str): Валюта для сбора данных
        start_date (date): Начальная дата
        end_date (date): Конечная дата
        db_path (str): Путь к базе данных
    """
    print(f"Начало сбора данных для {currency} с {start_date} по {end_date}")
    
    # Подключение к базе данных
    conn = sqlite3.connect(db_path)
    
    try:
        # Создание таблицы, если она не существует
        create_trades_table(conn)
        print("Таблица в базе данных создана или уже существует")
        
        # Получение данных о сделках
        trades = get_trades(currency, 'option', start_date, end_date, db_path=db_path)
        
        if trades:
            print(f"Получено {len(trades)} сделок")
            
            # Сохранение данных в базу
            saved_count = save_trades_to_db(conn, trades)
            print(f"Успешно сохранено {saved_count} сделок в базу данных")
            # Добавляем информацию о количестве новых сделок
            if saved_count > 0:
                print(f"INFO: Скачано еще {saved_count} новых сделок.")
        else:
            print("Данные не получены")
            
    except Exception as e:
        print(f"Ошибка при сборе данных: {e}")
    finally:
        conn.close()
        print("Подключение к базе данных закрыто")

# Точка входа в программу
if __name__ == "__main__":
    import argparse
    
    # Создание парсера аргументов командной строки
    parser = argparse.ArgumentParser(description='Сбор данных о сделках опционов с Deribit')
    parser.add_argument('--currency', type=str, required=True, help='Валюта для сбора данных (BTC, SOL и т.д.)')
    parser.add_argument('--start', type=str, required=True, help='Начальная дата в формате YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, help='Конечная дата в формате YYYY-MM-DD')
    
    # Парсинг аргументов
    args = parser.parse_args()
    
    # Преобразование строковых дат в объекты date
    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    
    # Вызов основной функции сбора данных
    collect_trades(args.currency, start_date, end_date, db_path='server_opc.db')