#!/usr/bin/env python3
"""
Проверка структуры базы данных
"""

import sqlite3

def check_database_structure():
    """Проверка структуры базы данных"""
    try:
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Получение списка таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("Таблицы в БД:")
        for table in tables:
            print(f"  {table[0]}")
        
        # Проверка структуры основных таблиц
        main_tables = ['spot_data', 'futures_data', 'iv_agg', 'basis_agg']
        
        for table_name in main_tables:
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                print(f"\nКолонки в {table_name}:")
                for col in columns:
                    print(f"  {col[1]} ({col[2]})")
                
                # Проверка количества записей
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  Записей: {count}")
                
            except sqlite3.OperationalError as e:
                print(f"\nТаблица {table_name} не существует: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка проверки БД: {e}")

if __name__ == "__main__":
    check_database_structure()
