#!/usr/bin/env python3
"""
Скрипт для очистки всех таблиц с данными в базе данных
"""

import sqlite3
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clear_tables.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clear_all_tables(db_path='server_opc.db'):
    """
    Очищает все таблицы с данными в базе данных
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        logger.info(f"Найдено {len(tables)} таблиц для очистки")
        
        # Очищаем каждую таблицу
        for table in tables:
            table_name = table[0]
            if table_name not in ['sqlite_sequence']:  # Пропускаем служебные таблицы
                try:
                    cursor.execute(f"DELETE FROM {table_name}")
                    logger.info(f"Очищена таблица: {table_name}")
                except Exception as e:
                    logger.warning(f"Не удалось очистить таблицу {table_name}: {e}")
        
        # Сбрасываем автоинкремент
        try:
            cursor.execute("DELETE FROM sqlite_sequence")
            logger.info("Сброшен автоинкремент")
        except Exception as e:
            logger.warning(f"Не удалось сбросить автоинкремент: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info("Все таблицы успешно очищены")
        
    except Exception as e:
        logger.error(f"Ошибка при очистке таблиц: {e}")
        raise

if __name__ == "__main__":
    clear_all_tables()