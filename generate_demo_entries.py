#!/usr/bin/env python3
"""
Скрипт для генерации демонстрационных точек входа
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_demo_entries():
    """Генерация демонстрационных точек входа"""
    print("Генерация демонстрационных точек входа...")
    
    # Создаем временные данные за последние 30 дней
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Генерируем данные с частотой 1 час
    timestamps = pd.date_range(start=start_date, end=end_date, freq='1H')
    
    data = []
    for i, ts in enumerate(timestamps):
        # Генерируем направления
        direction = np.random.choice(['BUY', 'SELL'])
        
        # Генерируем confidence
        confidence = np.random.uniform(0.5, 0.9)
        
        # Генерируем причины
        reasons = [
            f"IV spike +{np.random.uniform(1, 5):.1f}% + BUY trend",
            f"IV spike -{np.random.uniform(1, 5):.1f}% + SELL trend",
            f"positive skew {np.random.uniform(0.01, 0.1):.4f} + BUY trend",
            f"negative skew {np.random.uniform(-0.1, -0.01):.4f} + SELL trend"
        ]
        reason = np.random.choice(reasons)
        
        # Генерируем другие параметры
        underlying_price = 100 + np.random.normal(0, 5)
        iv_spike = np.random.uniform(-5, 5)
        skew = np.random.uniform(-0.2, 0.2)
        trend_direction = np.random.choice(['BUY', 'SELL'])
        trend_confidence = np.random.uniform(0.3, 0.8)
        
        data.append({
            'timestamp': ts.isoformat(),
            'direction': direction,
            'timeframe': '1h',
            'confidence': round(confidence, 2),
            'reason': reason,
            'underlying_price': round(underlying_price, 2),
            'iv_spike': round(iv_spike, 2),
            'skew': round(skew, 4),
            'trend_direction': trend_direction,
            'trend_confidence': round(trend_confidence, 2)
        })
    
    # Создаем DataFrame
    df = pd.DataFrame(data)
    
    # Сохраняем в CSV файл
    df.to_csv('entry_points.csv', index=False)
    
    print(f"Сгенерировано {len(data)} демонстрационных точек входа")
    print("Файл entry_points.csv успешно создан")
    
    # Показываем первые 5 строк
    print("\nПервые 5 строк файла entry_points.csv:")
    print(df.head().to_string(index=False))

def main():
    """Основная функция"""
    print("Генерация демонстрационных точек входа для entry_points.csv")
    
    # Генерируем данные
    generate_demo_entries()
    
    print("Генерация демонстрационных точек входа завершена успешно!")

if __name__ == "__main__":
    main()