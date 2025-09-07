#!/usr/bin/env python3
"""
Проверка структуры базы данных
"""

import sqlite3

def check_tables():
    try:
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("📊 Таблицы в базе данных:")
        for table in tables:
            table_name = table[0]
            print(f"  - {table_name}")
            
            # Считаем записи
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"    Записей: {count}")
            
            # Показываем колонки
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"    Колонки:")
            for col in columns:
                print(f"      {col[1]} ({col[2]})")
            print()
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_tables()
