#!/usr/bin/env python3
"""
Скрипт миграции базы данных для добавления поддержки мульти-активности и тегов наборов данных.
Добавляет колонки symbol и dataset_tag во все таблицы с временными рядами.
"""

import sqlite3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table_if_not_exists(conn, table_name):
    """Создает таблицу, если она не существует."""
    cursor = conn.cursor()
    
    try:
        if table_name == 'iv_data':
            # Создаем таблицу iv_data если её нет
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS iv_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                dataset_tag TEXT NOT NULL,
                markIv REAL,
                bid1Iv REAL,
                ask1Iv REAL,
                markPrice REAL NOT NULL,
                underlyingPrice REAL NOT NULL,
                delta REAL NOT NULL,
                gamma REAL NOT NULL,
                vega REAL NOT NULL,
                theta REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            logger.info(f"Создана таблица {table_name}")
        elif table_name == 'iv_agg':
            # Создаем таблицу iv_agg если её нет
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS iv_agg (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                spot_price REAL,
                iv_30d REAL,
                skew_30d REAL,
                basis_rel REAL,
                oi_total REAL,
                symbol TEXT NOT NULL,
                dataset_tag TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            logger.info(f"Создана таблица {table_name}")
        elif table_name == 'trend_signals_15m':
            # Создаем таблицу trend_signals_15m если её нет
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS trend_signals_15m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT,
                confidence REAL,
                reason TEXT,
                iv_30d REAL,
                skew_30d REAL,
                iv_momentum REAL,
                symbol TEXT NOT NULL,
                dataset_tag TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            logger.info(f"Создана таблица {table_name}")
        elif table_name == 'error_history':
            # Создаем таблицу error_history если её нет
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS error_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                formula_id TEXT NOT NULL,
                predicted REAL NOT NULL,
                actual REAL NOT NULL,
                error REAL NOT NULL,
                normalized_error REAL NOT NULL,
                volatility REAL NOT NULL,
                confidence REAL NOT NULL,
                method TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            logger.info(f"Создана таблица {table_name}")
        else:
            logger.info(f"Таблица {table_name} не требует специального создания")
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании таблицы {table_name}: {e}")
        raise

def add_columns_to_table(conn, table_name):
    """Добавляет колонки symbol и dataset_tag в указанную таблицу."""
    cursor = conn.cursor()
    
    try:
        # Добавляем колонку symbol с значением по умолчанию
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN symbol TEXT NOT NULL DEFAULT 'UNKNOWN'")
        logger.info(f"Добавлена колонка 'symbol' в таблицу {table_name}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            logger.info(f"Колонка 'symbol' уже существует в таблице {table_name}")
            # Обновляем существующие записи, чтобы установить значение по умолчанию
            cursor.execute(f"UPDATE {table_name} SET symbol = 'UNKNOWN' WHERE symbol IS NULL")
            logger.info(f"Обновлены существующие записи в таблице {table_name} для колонки 'symbol'")
        else:
            logger.error(f"Ошибка при добавлении колонки 'symbol' в {table_name}: {e}")
            raise

    try:
        # Добавляем колонку dataset_tag с значением по умолчанию
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN dataset_tag TEXT NOT NULL DEFAULT 'UNTAGGED'")
        logger.info(f"Добавлена колонка 'dataset_tag' в таблицу {table_name}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            logger.info(f"Колонка 'dataset_tag' уже существует в таблице {table_name}")
            # Обновляем существующие записи, чтобы установить значение по умолчанию
            cursor.execute(f"UPDATE {table_name} SET dataset_tag = 'UNTAGGED' WHERE dataset_tag IS NULL")
            logger.info(f"Обновлены существующие записи в таблице {table_name} для колонки 'dataset_tag'")
        else:
            logger.error(f"Ошибка при добавлении колонки 'dataset_tag' в {table_name}: {e}")
            raise

    # Добавляем индексы для новых колонок для ускорения запросов
    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol ON {table_name}(symbol)")
        logger.info(f"Создан индекс для колонки 'symbol' в таблице {table_name}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании индекса для 'symbol' в {table_name}: {e}")

    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dataset_tag ON {table_name}(dataset_tag)")
        logger.info(f"Создан индекс для колонки 'dataset_tag' в таблице {table_name}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании индекса для 'dataset_tag' в {table_name}: {e}")

    conn.commit()

def main():
    """Основная функция миграции."""
    db_path = 'server_opc.db'
    
    # Список таблиц, которые нужно модифицировать
    tables_to_modify = [
        'spot_data',
        'futures_data',
        'iv_data',
        'iv_agg',
        'trend_signals_15m',
        'error_history'
    ]
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        logger.info(f"Подключено к базе данных {db_path}")
        
        # Создаем недостающие таблицы
        for table_name in tables_to_modify:
            logger.info(f"Создаем таблицу {table_name} если она не существует...")
            create_table_if_not_exists(conn, table_name)
        
        # Проверяем, какие таблицы существуют
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Существующие таблицы: {existing_tables}")
        
        # Модифицируем все таблицы (добавляем колонки symbol и dataset_tag)
        for table_name in tables_to_modify:
            logger.info(f"Модифицируем таблицу {table_name}...")
            add_columns_to_table(conn, table_name)
        
        # Закрываем соединение
        conn.close()
        logger.info("Миграция базы данных успешно завершена.")
        
    except Exception as e:
        logger.error(f"Ошибка во время миграции: {e}")
        raise

if __name__ == "__main__":
    main()