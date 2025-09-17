#!/usr/bin/env python3
import subprocess
import sqlite3
import os
import json
from datetime import datetime, timedelta
import time
import sys
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Создаем FileHandler для записи логов в файл
file_handler = logging.FileHandler('pipeline.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Получаем логгер и добавляем ему FileHandler
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

# Импортируем функцию проверки дискового пространства
from system_check import check_disk_space

def clear_database_and_progress():
    """
    Очищает таблицу deribit_option_trades и файл progress.json перед началом.
    """
    logger.info("Clearing database and progress file...")
    
    # Подключаемся к базе данных
    conn = sqlite3.connect('server_opc.db')
    cursor = conn.cursor()
    
    # Очищаем таблицу
    cursor.execute("DELETE FROM deribit_option_trades")
    conn.commit()
    logger.info(f"Deleted {cursor.rowcount} rows from deribit_option_trades")
    
    conn.close()
    
    # Удаляем файл прогресса, если он существует
    if os.path.exists('progress.json'):
        os.remove('progress.json')
        logger.info("Deleted progress.json")
    
    logger.info("Database and progress file cleared.")

def get_quarter_dates(year, quarter):
    """
    Возвращает начальную и конечную дату квартала.
    
    Args:
        year (int): Год
        quarter (int): Квартал (1-4)
        
    Returns:
        tuple: (start_date, end_date) в формате 'YYYY-MM-DD'
    """
    quarter_start = {
        1: f"{year}-01-01",
        2: f"{year}-04-01",
        3: f"{year}-07-01",
        4: f"{year}-10-01"
    }
    
    quarter_end = {
        1: f"{year}-03-31",
        2: f"{year}-06-30",
        3: f"{year}-09-30",
        4: f"{year}-12-31"
    }
    
    return quarter_start[quarter], quarter_end[quarter]


def run_command(command):
    """
    Выполняет команду и возвращает результат.
    
    Args:
        command (str): Команда для выполнения
        
    Returns:
        bool: True если команда выполнена успешно, False в противном случае
    """
    logger.info(f"Running command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"Command failed with return code {result.returncode}")
        logger.error(f"Error output: {result.stderr}")
        return False
    else:
        logger.info("Command completed successfully")
        return True

def delete_processed_data_from_db(currency, start_date, end_date):
    """
    Удаляет обработанные данные из базы данных.
    
    Args:
        currency (str): Валюта (BTC, SOL и т.д.)
        start_date (str): Начальная дата в формате YYYY-MM-DD
        end_date (str): Конечная дата в формате YYYY-MM-DD
    """
    logger.info(f"Deleting processed data for {currency} from {start_date} to {end_date}...")
    
    # Преобразуем строки дат в Unix timestamp в миллисекундах
    from datetime import datetime, timedelta
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
    start_ts_ms = int(start_dt.timestamp() * 1000)
    end_ts_ms = int(end_dt.timestamp() * 1000)
    
    # Подключаемся к базе данных
    conn = sqlite3.connect('server_opc.db')
    cursor = conn.cursor()
    
    # Удаляем данные за указанный период
    cursor.execute("""
        DELETE FROM deribit_option_trades
        WHERE instrument_name LIKE ?
        AND timestamp >= ?
        AND timestamp < ?
    """, (f'{currency}-%', start_ts_ms, end_ts_ms))
    
    conn.commit()
    deleted_rows = cursor.rowcount
    logger.info(f"Deleted {deleted_rows} rows for {currency}")
    conn.close()
    
    # Возвращаем количество удаленных строк для верификации
    return deleted_rows

def main(quarter=None):
    # Шаг 1: Проверка дискового пространства
    logger.info("Step 1: Checking disk space...")
    if not check_disk_space('.', required_gb=40):
        logger.error("Insufficient disk space. Halting execution.")
        sys.exit(1)
    
    # Шаг 2: Очистка таблицы и файла прогресса
    logger.info("\nStep 2: Clearing database and progress file...")
    clear_database_and_progress()
    
    # Шаг 3: Главный цикл по кварталам 2023 года
    logger.info("\nStep 3: Processing quarters...")
    year = 2023
    
    # Определяем диапазон кварталов для обработки
    if quarter is not None:
        quarters_to_process = [quarter]
    else:
        quarters_to_process = range(1, 5)
    
    for quarter in quarters_to_process:
        logger.info(f"\nProcessing Q{quarter} {year}...")
        
        # Определяем даты начала и конца квартала
        start_date, end_date = get_quarter_dates(year, quarter)
        logger.info(f"Quarter dates: {start_date} to {end_date}")
        
        # Обрабатываем BTC
        logger.info(f"\nProcessing BTC for Q{quarter}...")
        btc_command = f"python3 deribit_trades_collector.py --currency BTC --start {start_date} --end {end_date}"
        if not run_command(btc_command):
            logger.error(f"Failed to collect BTC data for Q{quarter}. Exiting.")
            sys.exit(1)
        
        # Обрабатываем SOL
        logger.info(f"\nProcessing SOL for Q{quarter}...")
        sol_command = f"python3 deribit_trades_collector.py --currency SOL --start {start_date} --end {end_date}"
        if not run_command(sol_command):
            logger.error(f"Failed to collect SOL data for Q{quarter}. Exiting.")
            sys.exit(1)
        
        # Выполняем ETL для BTC
        logger.info(f"\nRunning ETL for BTC Q{quarter}...")
        btc_etl_command = f"python3 etl_processor.py --currency BTC --start {start_date} --end {end_date}"
        if not run_command(btc_etl_command):
            logger.error(f"Failed to process BTC data for Q{quarter}. Exiting.")
            sys.exit(1)
        
        # Выполняем ETL для SOL
        logger.info(f"\nRunning ETL for SOL Q{quarter}...")
        sol_etl_command = f"python3 etl_processor.py --currency SOL --start {start_date} --end {end_date}"
        if not run_command(sol_etl_command):
            logger.error(f"Failed to process SOL data for Q{quarter}. Exiting.")
            sys.exit(1)
        
        # Очищаем обработанные данные из базы
        logger.info(f"\nCleaning database for Q{quarter}...")
        deleted_btc_records = delete_processed_data_from_db("BTC", start_date, end_date)
        deleted_sol_records = delete_processed_data_from_db("SOL", start_date, end_date)
        logger.info(f"Deleted {deleted_btc_records} BTC records and {deleted_sol_records} SOL records from database.")
        
        # Логируем завершение обработки квартала
        logger.info(f"\nCompleted processing Q{quarter} {year}")
    
    logger.info("\nAll quarters processed successfully!")
    
    # Финальная проверка чистоты базы данных
    logger.info("\nPerforming final database cleanup verification...")
    conn = sqlite3.connect('server_opc.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM deribit_option_trades")
    remaining_records = cursor.fetchone()[0]
    conn.close()
    
    if remaining_records != 0:
        logger.error(f"CRITICAL ERROR: Database is not clean after pipeline execution! Remaining records: {remaining_records}")
        sys.exit(1)
    else:
        logger.info("Final verification passed: Database is clean.")
        
        # Финальная проверка существования Parquet-файлов
        logger.info("\nPerforming final Parquet files verification...")
        btc_parquet_file = "btc_options_1m_2023.parquet"
        sol_parquet_file = "sol_options_1m_2023.parquet"
        
        if not os.path.exists(btc_parquet_file):
            logger.error(f"CRITICAL ERROR: Parquet file {btc_parquet_file} was not created!")
            sys.exit(1)
        else:
            logger.info(f"Parquet file {btc_parquet_file} exists.")
            
        if not os.path.exists(sol_parquet_file):
            logger.error(f"CRITICAL ERROR: Parquet file {sol_parquet_file} was not created!")
            sys.exit(1)
        else:
            logger.info(f"Parquet file {sol_parquet_file} exists.")
        
        logger.info("Final verification passed: All Parquet files exist.")
        logger.info("Orchestrator has finished its job. Exiting.")
        sys.exit(0)

if __name__ == "__main__":
    import argparse
    
    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Оркестратор конвейера данных')
    parser.add_argument('--quarter', type=int, choices=range(1, 5), help='Номер квартала для обработки (1-4)')
    
    # Парсим аргументы
    args = parser.parse_args()
    
    # Вызываем главную функцию с аргументом quarter
    main(quarter=args.quarter)