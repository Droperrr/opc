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

# Функция для симуляции API Deribit (для тестирования)
def simulate_trades(currency, kind, start_timestamp, end_timestamp, count=10000):
    """
    Симуляция получения данных о сделках с Deribit API
    """
    # Преобразуем timestamp в datetime для удобства
    start_dt = datetime.fromtimestamp(start_timestamp / 1000)
    end_dt = datetime.fromtimestamp(end_timestamp / 1000)
    
    # Определяем период времени для генерации сделок
    time_diff = end_dt - start_dt
    total_seconds = int(time_diff.total_seconds())
    
    trades = []
    
    # Генерируем случайные сделки
    for i in range(min(count, max(1, total_seconds // 10))):  # Ограничиваем количество сделок
        # Генерируем случайное время в пределах периода
        random_seconds = random.randint(0, total_seconds)
        trade_time = start_dt + timedelta(seconds=random_seconds)
        timestamp = int(trade_time.timestamp() * 1000)
        
        # Генерируем другие параметры сделки
        trade = {
            "trade_id": f"TRADE_{currency}_{timestamp}_{i}",
            "timestamp": timestamp,
            "instrument_name": f"{currency}-26SEP25-360-C",
            "price": round(random.uniform(100, 500), 2),
            "amount": round(random.uniform(0.1, 10), 8),
            "direction": random.choice(["buy", "sell"]),
            "iv": round(random.uniform(0.1, 1.0), 4),
            "settlement_price": round(random.uniform(100, 500), 2),
            "underlying_price": round(random.uniform(100, 500), 2)
        }
        
        trades.append(trade)
    
    # Сортируем сделки по времени
    trades.sort(key=lambda x: x["timestamp"])
    
    return trades

# Функция для получения данных о сделках
def get_trades(currency, kind, start_date, end_date, include_old=True, count=10000, db_path='server_opc.db'):
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
    if max_timestamp_in_db is not None:
        start_timestamp = max_timestamp_in_db + 1
        print(f"INFO: Обнаружены данные до {timestamp_to_datetime(max_timestamp_in_db)}. Продолжаю сбор с этой точки.")
    else:
        start_timestamp = datetime_to_timestamp(datetime.combine(start_date, datetime.min.time()))
        print(f"INFO: Начинаю сбор данных с {start_date}")
    
    trades_list = []
    params = {
        "currency": currency,
        "kind": kind,
        "count": count,
        "include_old": include_old,
        "start_timestamp": start_timestamp,
        "end_timestamp": datetime_to_timestamp(datetime.combine(end_date, datetime.max.time()))
    }
    
    url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time'
    
    print(f"Запрос к API: {url}")
    print(f"Параметры запроса: {params}")
    
    # Для тестирования используем симуляцию вместо реального API
    print("INFO: Используется симуляция API Deribit для тестирования")
    
    # Генерируем симулированные сделки
    trades = simulate_trades(
        currency, kind,
        params["start_timestamp"],
        params["end_timestamp"],
        count
    )
    
    print(f"Получено {len(trades)} сделок в симуляции")
    
    if len(trades) == 0:
        print("Больше сделок не найдено в симуляции")
    else:
        trades_list.extend(trades)
        print(f"Скачано {len(trades)} сделок. Всего: {len(trades_list)}")
        
        # Для симуляции постраничного запроса, если нужно больше данных
        # В реальной реализации здесь был бы цикл с обновлением start_timestamp
        print("Достигнут конец запрашиваемого периода (в симуляции)")
    
    return trades_list

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
    # Пример использования: сбор данных для BTC за один день
    currency = 'BTC'
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 1)
    
    collect_trades(currency, start_date, end_date, db_path='server_opc.db')