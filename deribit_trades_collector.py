import requests
import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta
import time
import random
import json
import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Прогревочный запрос
def warmup_request():
    """
    Выполняет прогревочный запрос к API Deribit для инициализации соединения.
    """
    url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_instrument'
    params = {
        "instrument_name": "BTC-PERPETUAL",
        "count": 1,
        "include_old": True
    }
    
    logger.info("Выполняю прогревочный запрос...")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logger.info("Прогревочный запрос выполнен успешно.")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Ошибка при выполнении прогревочного запроса: {e}. Продолжаю выполнение.")

# Вызов прогревочного запроса при импорте модуля
warmup_request()

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
    
    # Проверка, что start_timestamp не превышает end_timestamp
    if start_timestamp > end_timestamp:
        print("INFO: Начальная дата больше конечной. Возвращаю пустой список сделок.")
        return []
    
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

# Функция для получения данных о сделках с реального API Deribit
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
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
    
    logger.info(f"Запрос к API: {url}")
    logger.info(f"Параметры запроса: {params}")
    
    try:
        # Выполняем запрос к API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Проверяем на ошибки HTTP
        
        # Парсим JSON ответ
        data = response.json()
        
        # Проверяем на ошибки API
        if 'error' in data and data['error'] is not None:
            logger.error(f"Ошибка API: {data['error']}")
            return []
        
        # Извлекаем сделки из ответа
        trades = data.get('result', {}).get('trades', [])
        logger.info(f"Получено {len(trades)} сделок")
        
        return trades
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")
        return []
    except Exception as e:
        logger.error(f"Ошибка при обработке ответа: {e}")
        return []

# Функция для постраничной загрузки всех сделок в заданном диапазоне времени
def get_all_trades_paginated(currency, kind, start_timestamp, end_timestamp, count=1000):
    """
    Постраничная загрузка всех сделок в заданном диапазоне времени с использованием курсора trade_seq
    
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
    request_count = 0
    max_requests = 1000  # Ограничение на количество запросов для предотвращения бесконечного цикла
    last_trade_seq = None  # Инициализируем курсор
    
    logger.info(f"Начало постраничной загрузки данных с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    
    while request_count < max_requests:
        # Получаем сделки для текущей страницы
        # Для получения поля has_more нужно напрямую вызвать API, а не через get_real_trades
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
        
        # Если у нас есть курсор, добавляем его в параметры
        if last_trade_seq is not None:
            params["start_seq"] = last_trade_seq + 1
            logger.info(f"Загрузка данных с курсора trade_seq: {last_trade_seq + 1}")
        else:
            logger.info(f"Загрузка данных с {timestamp_to_datetime(start_timestamp)}")
        
        logger.info(f"Запрос к API: {url}")
        logger.info(f"Параметры запроса: {params}")
        
        try:
            # Выполняем запрос к API
            response = requests.get(url, params=params)
            logger.debug(f"DEBUG: Статус-код запроса: {response.status_code}")
            logger.debug(f"DEBUG: Сырой ответ (первые 500 символов): {response.text[:500]}")
            response.raise_for_status()  # Проверяем на ошибки HTTP
            
            # Парсим JSON ответ
            data = response.json()
            
            # Проверяем на ошибки API
            if 'error' in data and data['error'] is not None:
                logger.error(f"Ошибка API: {data['error']}")
                break
            
            # Извлекаем сделки из ответа
            trades = data.get('result', {}).get('trades', [])
            has_more = data.get('result', {}).get('has_more', False)
            
            logger.info(f"Получено {len(trades)} сделок")
            logger.info(f"Есть еще данные: {has_more}")
            logger.debug(f"DEBUG: Количество сделок в ответе: {len(trades)}")
            
            if not trades:
                logger.info("Больше сделок не найдено")
                break
            
            # Добавляем сделки в общий список
            all_trades.extend(trades)
            logger.info(f"Скачано {len(trades)} сделок. Всего: {len(all_trades)}")
            
            # Находим максимальное значение trade_seq в текущем списке сделок
            if trades:
                last_trade_seq = max(trade.get('trade_seq', 0) for trade in trades)
                logger.info(f"Максимальный trade_seq в текущем ответе: {last_trade_seq}")
            
            # Проверяем, есть ли еще данные
            if not has_more:
                logger.info("Достигнут конец данных")
                break
            
            # Добавляем небольшую задержку между запросами, чтобы не перегружать API
            time.sleep(0.1)
            
            request_count += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            break
        except Exception as e:
            logger.error(f"Ошибка при обработке ответа: {e}")
            break
    
    if request_count >= max_requests:
        print(f"Достигнуто максимальное количество запросов ({max_requests})")
    
    print(f"Загрузка завершена. Всего получено {len(all_trades)} сделок")
    return all_trades

# Функция для постраничной загрузки всех сделок с использованием "Micro-Window Pagination"
def get_all_trades_micro_window_pagination(currency, kind, start_timestamp, end_timestamp, count=100, sorting='asc', limit=None):
    """
    Постраничная загрузка всех сделок в заданном диапазоне времени с использованием "Micro-Window Pagination"
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        kind (str): Тип инструмента (option, future и т.д.)
        start_timestamp (int): Начальное время в миллисекундах
        end_timestamp (int): Конечное время в миллисекундах
        count (int): Количество записей на запрос (максимум 100 для Deribit)
        sorting (str): Порядок сортировки сделок ('asc' или 'desc')
        limit (int): Максимальное количество сделок для сбора
        
    Returns:
        list: Список всех сделок
    """
    all_trades = []
    request_count = 0
    max_requests = 10000  # Ограничение на количество запросов для предотвращения бесконечного цикла
    
    logger.info(f"Начало загрузки данных с Micro-Window Pagination с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    logger.info(f"Порядок сортировки: {sorting}")
    if limit:
        logger.info(f"Лимит сделок: {limit}")
    
    # Внешний цикл: итерация по дням
    current_day = datetime.fromtimestamp(start_timestamp / 1000).date()
    end_day = datetime.fromtimestamp(end_timestamp / 1000).date()
    
    while current_day <= end_day and request_count < max_requests:
        logger.info(f"Обрабатываю день: {current_day}")
        
        # Определяем начало и конец текущего дня в миллисекундах
        day_start_dt = datetime.combine(current_day, datetime.min.time())
        day_end_dt = datetime.combine(current_day, datetime.max.time())
        
        # Корректируем начало и конец дня, если они выходят за рамки общего диапазона
        if day_start_dt.timestamp() * 1000 < start_timestamp:
            day_start_dt = datetime.fromtimestamp(start_timestamp / 1000)
        if day_end_dt.timestamp() * 1000 > end_timestamp:
            day_end_dt = datetime.fromtimestamp(end_timestamp / 1000)
            
        day_start_ts = int(day_start_dt.timestamp() * 1000)
        day_end_ts = int(day_end_dt.timestamp() * 1000)
        
        # Внутренний цикл: итерация по 15-минутным интервалам
        interval_start_dt = day_start_dt
        while interval_start_dt < day_end_dt and request_count < max_requests:
            # Определяем конец 15-минутного интервала
            interval_end_dt = interval_start_dt + timedelta(minutes=15)
            if interval_end_dt > day_end_dt:
                interval_end_dt = day_end_dt
                
            interval_start_ts = int(interval_start_dt.timestamp() * 1000)
            interval_end_ts = int(interval_end_dt.timestamp() * 1000)
            
            logger.info(f"  Обрабатываю 15-минутное окно: {interval_start_dt} - {interval_end_dt}")
            
            # Пагинация внутри 15-минутного интервала с использованием курсора trade_seq
            last_trade_seq = None
            has_more = True
            interval_request_count = 0
            max_interval_requests = 1000  # Ограничение на количество запросов в одном интервале
            
            while has_more and request_count < max_requests and interval_request_count < max_interval_requests:
                url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time'
                
                # Параметры запроса
                params = {
                    "currency": currency,
                    "kind": kind,
                    "start_timestamp": interval_start_ts,
                    "end_timestamp": interval_end_ts,
                    "count": min(count, 100),  # Ограничиваем максимальное количество
                    "include_old": True,
                    "sorting": sorting
                }
                
                # Если есть курсор trade_seq, добавляем его в параметры
                if last_trade_seq is not None:
                    params["start_seq"] = last_trade_seq + 1
                    logger.debug(f"    Запрос с курсором trade_seq: {last_trade_seq + 1}")
                
                logger.debug(f"    Запрос к API: {url}")
                logger.debug(f"    Параметры запроса: {params}")
                
                try:
                    # Выполняем запрос к API
                    response = requests.get(url, params=params)
                    response.raise_for_status()  # Проверяем на ошибки HTTP
                    
                    # Парсим JSON ответ
                    data = response.json()
                    
                    # Проверяем на ошибки API
                    if 'error' in data and data['error'] is not None:
                        logger.error(f"    Ошибка API: {data['error']}")
                        break
                    
                    # Извлекаем сделки из ответа
                    trades = data.get('result', {}).get('trades', [])
                    has_more = data.get('result', {}).get('has_more', False)
                    
                    logger.info(f"    Получено {len(trades)} сделок (has_more: {has_more})")
                    
                    if not trades:
                        logger.info("    Больше сделок не найдено в этом интервале")
                        break
                    
                    # Добавляем сделки в общий список
                    all_trades.extend(trades)
                    logger.debug(f"    Скачано {len(trades)} сделок. Всего: {len(all_trades)}")
                    
                    # Проверяем, достигнут ли лимит
                    if limit and len(all_trades) >= limit:
                        logger.info(f"    Достигнут лимит {limit} сделок. Прекращаю сбор данных.")
                        all_trades = all_trades[:limit] if sorting == 'asc' else all_trades[-limit:]
                        has_more = False
                        break
                    
                    # Находим максимальное значение trade_seq в текущем списке сделок
                    if trades:
                        last_trade_seq = max(trade.get('trade_seq', 0) for trade in trades)
                        logger.debug(f"    Максимальный trade_seq в текущем ответе: {last_trade_seq}")
                    
                    # Добавляем небольшую задержку между запросами, чтобы не перегружать API
                    time.sleep(0.1)
                    
                    request_count += 1
                    interval_request_count += 1
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"    Ошибка при выполнении запроса: {e}")
                    break
                except Exception as e:
                    logger.error(f"    Ошибка при обработке ответа: {e}")
                    break
                    
            # Если достигнут лимит, прерываем внутренний цикл
            if limit and len(all_trades) >= limit:
                break
                
            # Переходим к следующему 15-минутному интервалу
            interval_start_dt = interval_end_dt
        
        # Если достигнут лимит, прерываем внешний цикл
        if limit and len(all_trades) >= limit:
            break
            
        # Переходим к следующему дню
        current_day += timedelta(days=1)
    
    if request_count >= max_requests:
        logger.warning(f"Достигнуто максимальное количество запросов ({max_requests})")
    
    logger.info(f"Загрузка завершена. Всего получено {len(all_trades)} сделок")
    return all_trades


# Функция для получения данных о сделках
def get_trades(currency, kind, start_date, end_date, include_old=True, count=100, db_path='server_opc.db',
               start_time=None, end_time=None, sorting='asc', limit=None):
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
        start_time (str): Время начала в формате HH:MM
        end_time (str): Время окончания в формате HH:MM
        sorting (str): Порядок сортировки сделок ('asc' или 'desc')
        limit (int): Максимальное количество сделок для сбора
        
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
    
    # Определение начального и конечного timestamp с учетом времени
    if start_time:
        start_datetime = datetime.combine(start_date, datetime.strptime(start_time, '%H:%M').time())
    else:
        start_datetime = datetime.combine(start_date, datetime.min.time())
    start_timestamp = datetime_to_timestamp(start_datetime)
    
    if end_time:
        end_datetime = datetime.combine(end_date, datetime.strptime(end_time, '%H:%M').time())
    else:
        end_datetime = datetime.combine(end_date, datetime.max.time())
    end_timestamp = datetime_to_timestamp(end_datetime)
    
    # Проверка, что start_timestamp не превышает end_timestamp
    if start_timestamp > end_timestamp:
        logger.info("INFO: Начальное время больше конечного. Возвращаю пустой список сделок.")
        return []
    
    logger.info(f"INFO: Начинаю сбор данных с {timestamp_to_datetime(start_timestamp)} по {timestamp_to_datetime(end_timestamp)}")
    
    # Получаем все сделки с постраничной загрузкой (новая логика)
    trades = get_all_trades_micro_window_pagination(currency, kind, start_timestamp, end_timestamp, count, sorting, limit)
    
    # Если задан лимит, возвращаем только указанное количество сделок
    if limit and len(trades) > limit:
        if sorting == 'asc':
            trades = trades[:limit]
        else:
            trades = trades[-limit:]
        logger.info(f"INFO: Применен лимит {limit} сделок")
    
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
    
    # Удаляем дубликаты, оставляя только последние по времени сделки с одинаковым trade_id
    unique_trades = {}
    for trade in trades:
        trade_id = trade.get('trade_id')
        timestamp = trade.get('timestamp', 0)
        
        # Если trade_id уже есть в словаре, заменяем сделку на более новую
        if trade_id not in unique_trades or timestamp > unique_trades[trade_id].get('timestamp', 0):
            unique_trades[trade_id] = trade
    
    # Преобразуем словарь обратно в список
    unique_trades_list = list(unique_trades.values())
    
    saved_count = 0
    error_count = 0
    
    logger.info(f"Начинаю сохранение {len(unique_trades_list)} уникальных сделок в базу данных (из {len(trades)} полученных)")
    
    for i, trade in enumerate(unique_trades_list):
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
            
            # Проверяем, была ли вставлена новая запись
            if cursor.rowcount > 0:
                saved_count += 1
            
            # Логируем прогресс каждые 100 сделок
            if (i + 1) % 100 == 0:
                logger.info(f"Обработано {i + 1} уникальных сделок, сохранено {saved_count} записей")
                
        except Exception as e:
            error_count += 1
            if error_count <= 10:  # Выводим только первые 10 ошибок
                logger.error(f"Ошибка при сохранении сделки {trade.get('trade_id', 'unknown')}: {e}")
            continue
    
    # Коммитим изменения
    try:
        conn.commit()
        logger.info(f"Изменения успешно сохранены в базе данных")
    except Exception as e:
        logger.error(f"Ошибка при коммите транзакции: {e}")
    
    logger.info(f"Всего обработано сделок: {len(trades)}")
    logger.info(f"Уникальных сделок: {len(unique_trades_list)}")
    logger.info(f"Успешно сохранено записей: {saved_count}")
    logger.info(f"Ошибок: {error_count}")
    
    return saved_count

# Основная функция для сбора данных
def collect_trades(currency, start_date, end_date, db_path='server_opc.db',
                   start_time=None, end_time=None, sorting='asc', limit=None):
    """
    Основная функция для сбора и сохранения сделок
    
    Args:
        currency (str): Валюта для сбора данных
        start_date (date): Начальная дата
        end_date (date): Конечная дата
        db_path (str): Путь к базе данных
        start_time (str): Время начала в формате HH:MM
        end_time (str): Время окончания в формате HH:MM
        sorting (str): Порядок сортировки сделок ('asc' или 'desc')
        limit (int): Максимальное количество сделок для сбора
    """
    logger.info(f"Начало сбора данных для {currency} с {start_date} по {end_date}")
    if start_time or end_time:
        logger.info(f"Время: с {start_time or '00:00'} по {end_time or '23:59'}")
    if sorting != 'asc':
        logger.info(f"Порядок сортировки: {sorting}")
    if limit:
        logger.info(f"Лимит сделок: {limit}")
    
    # Подключение к базе данных
    conn = sqlite3.connect(db_path)
    
    try:
        # Создание таблицы, если она не существует
        create_trades_table(conn)
        logger.info("Таблица в базе данных создана или уже существует")
        
        # Получение данных о сделках за указанный период
        trades = get_trades(currency, 'option', start_date, end_date, db_path=db_path,
                           start_time=start_time, end_time=end_time, sorting=sorting, limit=limit)
        
        if trades:
            logger.info(f"Получено {len(trades)} сделок от API")
            
            # Сохранение данных в базу
            saved_count = save_trades_to_db(conn, trades)
            logger.info(f"Успешно сохранено {saved_count} сделок в базу данных")
            # Добавляем информацию о количестве новых сделок
            if saved_count > 0:
                logger.info(f"INFO: Скачано еще {saved_count} новых сделок.")
        else:
            logger.info("Данные не получены")
            
    except Exception as e:
        logger.error(f"Ошибка при сборе данных: {e}")
    finally:
        conn.close()
        logger.info("Подключение к базе данных закрыто")
    
    # Возвращаем количество сохраненных записей для верификации
    return saved_count if 'saved_count' in locals() else 0

# Точка входа в программу
if __name__ == "__main__":
    import argparse
    
    # Создание парсера аргументов командной строки
    parser = argparse.ArgumentParser(description='Сбор данных о сделках опционов с Deribit')
    parser.add_argument('--currency', type=str, required=True, help='Валюта для сбора данных (BTC, SOL и т.д.)')
    parser.add_argument('--start', type=str, required=True, help='Начальная дата в формате YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, help='Конечная дата в формате YYYY-MM-DD')
    parser.add_argument('--start_time', type=str, help='Время начала в формате HH:MM')
    parser.add_argument('--end_time', type=str, help='Время окончания в формате HH:MM')
    parser.add_argument('--sorting', type=str, choices=['asc', 'desc'], default='asc', help='Порядок сортировки сделок')
    parser.add_argument('--limit', type=int, help='Максимальное количество сделок для сбора')
    
    # Парсинг аргументов
    args = parser.parse_args()
    
    # Преобразование строковых дат в объекты date
    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    
    # Вызов основной функции сбора данных
    collect_trades(args.currency, start_date, end_date, db_path='server_opc.db',
                   start_time=args.start_time, end_time=args.end_time,
                   sorting=args.sorting, limit=args.limit)