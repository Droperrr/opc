#!/usr/bin/env python3
"""
Скрипт для генерации тестовых данных для базы server_opc.db
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_iv_data():
    """Генерация тестовых данных для таблицы iv_agg"""
    print("Генерация тестовых данных для таблицы iv_agg...")
    
    # Создаем временные данные за последние 30 дней
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Генерируем данные с частотой 1 минута
    timestamps = pd.date_range(start=start_date, end=end_date, freq='1min')
    
    # Создаем данные для таймфрейма 1m
    data_1m = []
    for i, ts in enumerate(timestamps):
        # Генерируем реалистичные значения
        spot_price = 100 + np.random.normal(0, 0.5) + i * 0.001  # небольшой тренд
        iv_30d = 0.8 + np.random.normal(0, 0.1) + 0.2 * np.sin(i * 0.01)  # сезонные колебания
        skew_30d = np.random.normal(0, 0.1) + 0.05 * np.sin(i * 0.005)  # сезонные колебания
        basis_rel = np.random.normal(0, 0.01)  # небольшие колебания
        oi_total = 1000 + np.random.exponential(500) + i * 10  # рост открытых позиций
        
        data_1m.append({
            'time': ts,
            'timeframe': '1m',
            'spot_price': max(0, spot_price),
            'iv_30d': max(0, iv_30d),
            'skew_30d': skew_30d,
            'basis_rel': basis_rel,
            'oi_total': max(0, oi_total)
        })
    
    # Генерируем данные для таймфрейма 15m
    timestamps_15m = pd.date_range(start=start_date, end=end_date, freq='15min')
    data_15m = []
    for i, ts in enumerate(timestamps_15m):
        # Генерируем реалистичные значения
        spot_price = 100 + np.random.normal(0, 0.5) + i * 0.015  # небольшой тренд
        iv_30d = 0.8 + np.random.normal(0, 0.1) + 0.2 * np.sin(i * 0.1)  # сезонные колебания
        skew_30d = np.random.normal(0, 0.1) + 0.05 * np.sin(i * 0.05)  # сезонные колебания
        basis_rel = np.random.normal(0, 0.01)  # небольшие колебания
        oi_total = 1000 + np.random.exponential(500) + i * 150  # рост открытых позиций
        
        data_15m.append({
            'time': ts,
            'timeframe': '15m',
            'spot_price': max(0, spot_price),
            'iv_30d': max(0, iv_30d),
            'skew_30d': skew_30d,
            'basis_rel': basis_rel,
            'oi_total': max(0, oi_total)
        })
    
    # Объединяем данные
    all_data = data_1m + data_15m
    
    # Создаем DataFrame
    df = pd.DataFrame(all_data)
    
    # Сохраняем в базу данных
    conn = sqlite3.connect('server_opc.db')
    df.to_sql('iv_agg', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Сгенерировано {len(data_1m)} записей для таймфрейма 1m")
    print(f"Сгенерировано {len(data_15m)} записей для таймфрейма 15m")
    print(f"Всего записей: {len(all_data)}")

def generate_signal_data():
    """Генерация тестовых данных для таблицы signals"""
    print("Генерация тестовых данных для таблицы signals...")
    
    # Создаем временные данные за последние 30 дней
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Генерируем данные с частотой 15 минут
    timestamps = pd.date_range(start=start_date, end=end_date, freq='15min')
    
    data = []
    for i, ts in enumerate(timestamps):
        # Генерируем сигналы
        signal_type = np.random.choice(['BUY', 'SELL', 'VOLATILITY SPIKE', 'NEUTRAL'])
        confidence = np.random.uniform(0.3, 0.9)
        
        # Генерируем причины
        reasons = [
            f"рост IV ({np.random.uniform(-0.05, 0.05):.3f}) при росте цены ({np.random.uniform(-1, 1):.2f})",
            f"падение IV ({np.random.uniform(-0.05, 0.05):.3f}) при падении цены ({np.random.uniform(-1, 1):.2f})",
            f"высокий skew ({np.random.uniform(-0.5, 0.5):.3f})",
            f"низкая волатильность ({np.random.uniform(0.5, 1.5):.3f})"
        ]
        reason = np.random.choice(reasons)
        
        data.append({
            'time': ts,
            'timeframe': '15m',
            'signal': signal_type,
            'confidence': confidence,
            'reason': reason
        })
    
    # Создаем DataFrame
    df = pd.DataFrame(data)
    
    # Сохраняем в базу данных
    conn = sqlite3.connect('server_opc.db')
    df.to_sql('signals', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Сгенерировано {len(data)} записей для таблицы signals")

def main():
    """Основная функция"""
    print("Генерация тестовых данных для базы server_opc.db")
    
    # Генерируем данные
    generate_iv_data()
    generate_signal_data()
    
    print("Генерация тестовых данных завершена успешно!")

if __name__ == "__main__":
    main()