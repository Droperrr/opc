#!/usr/bin/env python3
"""
Скрипт для проверки структуры базы данных
"""

import sqlite3
import pandas as pd
from logger import get_logger

logger = get_logger()

def check_database_structure():
    """Проверяет структуру базы данных"""
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("📊 Таблицы в базе данных sol_iv.db:")
        for table in tables:
            print(f"  - {table[0]}")
            
            # Показываем структуру каждой таблицы
            cursor.execute(f"PRAGMA table_info({table[0]})")
            columns = cursor.fetchall()
            print(f"    Колонки:")
            for col in columns:
                print(f"      {col[1]} ({col[2]})")
            print()
        
        # Проверяем наличие данных
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"📈 {table[0]}: {count} записей")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке БД: {e}")

if __name__ == "__main__":
    check_database_structure()
