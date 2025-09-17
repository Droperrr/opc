#!/usr/bin/env python3
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
from datetime import datetime, timedelta
import os
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def aggregate_trades_to_minute(currency, start_date, end_date, db_path='server_opc.db'):
    """
    Агрегирует сделки по минутам для указанной валюты и периода.
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        start_date (str): Начальная дата в формате YYYY-MM-DD
        end_date (str): Конечная дата в формате YYYY-MM-DD
        db_path (str): Путь к базе данных SQLite
        
    Returns:
        pandas.DataFrame: Агрегированные данные по минутам
    """
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    
    # Формируем SQL-запрос для получения данных
    query = """
    SELECT
        timestamp,
        instrument_name,
        price,
        amount,
        direction,
        iv
    FROM deribit_option_trades
    WHERE instrument_name LIKE ?
    AND timestamp >= ?
    AND timestamp < ?
    ORDER BY timestamp
    """
    
    # Параметры для запроса
    params = (
        f'{currency}-%',
        datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000,
        (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000
    )
    
    # Выполняем запрос и загружаем данные в DataFrame
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    # Если данных нет, возвращаем пустой DataFrame
    if df.empty:
        logger.info(f"No data found for {currency} from {start_date} to {end_date}")
        return df
    
    # Преобразуем timestamp в datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Создаем минутные интервалы
    df['minute'] = df['datetime'].dt.floor('1min')
    
    # Агрегируем данные по минутам
    aggregated = df.groupby(['minute', 'instrument_name']).agg({
        'price': ['first', 'max', 'min', 'last'],  # open, high, low, close
        'amount': 'sum',  # volume
        'iv': 'mean'  # average implied volatility
    }).reset_index()
    
    # Упрощаем названия колонок
    aggregated.columns = ['minute', 'instrument_name', 'open', 'high', 'low', 'close', 'volume', 'avg_iv']
    
    # Добавляем валюту как отдельный столбец
    aggregated['currency'] = currency
    
    # Сортируем по времени
    aggregated = aggregated.sort_values('minute')
    
    logger.info(f"Aggregated {len(df)} trades into {len(aggregated)} minute bars for {currency}")
    return aggregated

def save_to_parquet(df, filename):
    """
    Сохраняет DataFrame в Parquet-файл, добавляя данные к существующему файлу, если он есть.
    
    Args:
        df (pandas.DataFrame): Данные для сохранения
        filename (str): Имя Parquet-файла
    """
    if df.empty:
        logger.info(f"No data to save to {filename}")
        return
    
    # Создаем PyArrow таблицу из DataFrame
    table = pa.Table.from_pandas(df)
    
    # Если файл уже существует, читаем его и объединяем с новыми данными
    if os.path.exists(filename):
        existing_table = pq.read_table(filename)
        # Объединяем таблицы
        combined_table = pa.concat_tables([existing_table, table])
        # Сохраняем объединенную таблицу
        pq.write_table(combined_table, filename)
        logger.info(f"Appended data to existing {filename}")
    else:
        # Создаем новый файл
        pq.write_table(table, filename)
        logger.info(f"Created new {filename}")

def main(args=None):
    # Если аргументы не переданы, создаем парсер аргументов командной строки
    if args is None:
        parser = argparse.ArgumentParser(description='ETL-процессор для трансформации данных из SQLite в Parquet')
        parser.add_argument('--currency', type=str, required=True, help='Валюта для обработки (BTC, SOL и т.д.)')
        parser.add_argument('--start', type=str, required=True, help='Начальная дата в формате YYYY-MM-DD')
        parser.add_argument('--end', type=str, required=True, help='Конечная дата в формате YYYY-MM-DD')
        
        # Парсим аргументы
        args = parser.parse_args()
    
    # Агрегируем сделки по минутам
    aggregated_data = aggregate_trades_to_minute(args.currency, args.start, args.end)
    
    # Логируем количество записей для верификации
    logger.info(f"Processing {len(aggregated_data)} aggregated records for {args.currency}")
    
    # Определяем имя файла для сохранения
    filename = f"{args.currency.lower()}_options_1m_2023.parquet"
    
    # Сохраняем данные в Parquet-файл
    save_to_parquet(aggregated_data, filename)
    
    # Возвращаем количество обработанных записей для верификации
    return len(aggregated_data)

if __name__ == "__main__":
    main()