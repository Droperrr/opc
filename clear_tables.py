#!/usr/bin/env python3
"""
Скрипт для полной очистки таблиц в базе данных server_opc.db
"""

import sqlite3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_tables():
    """Очищает все таблицы в базе данных."""
    db_path = 'server_opc.db'
    
    # Список таблиц для очистки
    tables_to_clear = [
        'spot_data',
        'futures_data',
        'iv_data',
        'iv_agg',
        'trend_signals_15m'
    ]
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logger.info(f"Подключено к базе данных {db_path}")
        
        # Очищаем таблицы
        for table_name in tables_to_clear:
            cursor.execute(f"DELETE FROM {table_name}")
            logger.info(f"Очищена таблица {table_name}")
        
        # Коммитим изменения
        conn.commit()
        logger.info("Все таблицы успешно очищены.")
        
        # Закрываем соединение
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при очистке таблиц: {e}")
        raise

if __name__ == "__main__":
    clear_tables()