#!/usr/bin/env python3
"""
Финальный верификационный тест для сборщика данных deribit_trades_collector.py
"""

import sqlite3
import subprocess
import sys
from datetime import datetime, date

def clear_trades_table():
    """Очищает таблицу deribit_option_trades в базе данных"""
    print("Очистка таблицы deribit_option_trades...")
    try:
        conn = sqlite3.connect('server_opc.db')
        cursor = conn.cursor()
        
        # Удаляем все записи из таблицы
        cursor.execute('DELETE FROM deribit_option_trades')
        conn.commit()
        
        # Сбрасываем автоинкремент
        cursor.execute('DELETE FROM sqlite_sequence WHERE name="deribit_option_trades"')
        conn.commit()
        
        print("Таблица deribit_option_trades успешно очищена")
        conn.close()
    except Exception as e:
        print(f"Ошибка при очистке таблицы: {e}")
        sys.exit(1)

def run_data_collection():
    """Запускает сбор данных за 1 января 2023 года с 00:00 до 01:00 для BTC"""
    print("\nЗапуск сбора данных за 1 января 2023 года с 00:00 до 01:00 для BTC...")
    
    # Формируем команду для запуска сборщика данных
    cmd = [
        'python3', 'deribit_trades_collector.py',
        '--currency', 'BTC',
        '--start', '2023-01-01',
        '--end', '2023-01-01',
        '--start_time', '00:00',
        '--end_time', '01:00'
    ]
    
    try:
        # Запускаем сборщик данных и выводим лог в реальном времени
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Читаем и выводим лог в реальном времени
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Дожидаемся завершения процесса
        process.wait()
        
        if process.returncode == 0:
            print("\nСбор данных успешно завершен")
        else:
            print(f"\nСбор данных завершен с ошибкой (код возврата: {process.returncode})")
            sys.exit(1)
            
    except Exception as e:
        print(f"Ошибка при запуске сборщика данных: {e}")
        sys.exit(1)

def check_results():
    """Проверяет результаты сбора данных"""
    print("\nПроверка результатов...")
    try:
        conn = sqlite3.connect('server_opc.db')
        cursor = conn.cursor()
        
        # Выполняем SQL-запрос для проверки количества записей и уникальных trade_id
        cursor.execute('SELECT COUNT(*), COUNT(DISTINCT trade_id) FROM deribit_option_trades')
        result = cursor.fetchone()
        
        total_count = result[0]
        unique_count = result[1]
        
        print(f"Общее количество записей: {total_count}")
        print(f"Количество уникальных trade_id: {unique_count}")
        
        if total_count == unique_count:
            print("✅ УСПЕХ: Количество записей совпадает с количеством уникальных trade_id (нет дубликатов)")
        else:
            print("❌ ОШИБКА: Количество записей не совпадает с количеством уникальных trade_id (есть дубликаты)")
        
        conn.close()
        
        return total_count == unique_count
    except Exception as e:
        print(f"Ошибка при проверке результатов: {e}")
        sys.exit(1)

def main():
    """Основная функция"""
    print("=" * 60)
    print("ФИНАЛЬНЫЙ ВЕРИФИКАЦИОННЫЙ ТЕСТ")
    print("=" * 60)
    
    # Очищаем таблицу
    clear_trades_table()
    
    # Запускаем сбор данных
    run_data_collection()
    
    # Проверяем результаты
    success = check_results()
    
    print("\n" + "=" * 60)
    if success:
        print("ТЕСТ ПРОЙДЕН УСПЕШНО")
    else:
        print("ТЕСТ НЕ ПРОЙДЕН")
    print("=" * 60)

if __name__ == "__main__":
    main()