#!/usr/bin/env python3
"""
Этап А: Первый запуск сборщика для сбора данных по BTC за один час
"""
from datetime import date, datetime, timedelta
from deribit_trades_collector import collect_trades

if __name__ == "__main__":
    # Сбор данных для BTC за один час (например, с 00:00:00 по 01:00:00)
    # Для тестирования используем дату 2023-01-01
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 1)
    
    print("=== ЭТАП А: Первый запуск сборщика ===")
    print(f"Сбор данных для BTC с {start_date} по {end_date}")
    
    collect_trades('BTC', start_date, end_date, db_path='server_opc.db')
    
    print("=== ЭТАП А: Завершен ===")