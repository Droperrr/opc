#!/usr/bin/env python3
"""
Скрипт для очистки исторических данных перед полной перезагрузкой
"""

import sqlite3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_historical_data(db_path: str = 'data/sol_iv.db'):
    """Очищает таблицы с историческими данными"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Очищаем таблицы с историческими данными
        tables_to_clear = ['spot_data', 'futures_data']
        
        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")
            logger.info(f"✅ Очищена таблица {table}")
        
        # Сбрасываем автоинкремент
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('spot_data', 'futures_data')")
        
        conn.commit()
        conn.close()
        
        logger.info("🎉 Все исторические данные очищены")
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки данных: {e}")

if __name__ == "__main__":
    clear_historical_data()
