#!/usr/bin/env python3
"""
Скрипт для генерации расширенного набора данных для демонстрации оптимизированной системы
Создает данные за 7 дней с различными торговыми сессиями и рыночными условиями
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from logger import get_logger

logger = get_logger()

def generate_extended_iv_data():
    """Генерирует расширенные данные IV за 7 дней"""
    logger.info("🔄 Генерация расширенных данных IV...")
    
    # Базовые параметры
    start_date = datetime(2025, 8, 25, 0, 0, 0)  # 7 дней назад
    end_date = datetime(2025, 9, 3, 23, 59, 59)
    
    # Создаем временные метки для каждого часа
    timestamps = []
    current = start_date
    while current <= end_date:
        timestamps.append(current)
        current += timedelta(hours=1)
    
    data = []
    base_price = 210.0
    
    for timestamp in timestamps:
        # Симулируем рыночные условия
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        
        # Базовые значения
        price_change = np.random.normal(0, 2)  # Случайное изменение цены
        current_price = base_price + price_change
        
        # IV зависит от времени и дня недели
        base_iv = 0.8
        if hour in [8, 9, 14, 15, 16]:  # Активные часы
            base_iv += 0.2
        if day_of_week in [0, 4]:  # Понедельник и пятница
            base_iv += 0.1
        
        # Добавляем случайность
        iv = base_iv + np.random.normal(0, 0.1)
        iv = max(0.5, min(1.5, iv))  # Ограничиваем IV
        
        # Skew зависит от рыночных условий
        skew = np.random.normal(0, 0.05)
        if current_price > base_price:
            skew = abs(skew)  # Положительный skew при росте
        else:
            skew = -abs(skew)  # Отрицательный skew при падении
        
        # OI ratio
        oi_ratio = 0.5 + np.random.normal(0, 0.1)
        oi_ratio = max(0.3, min(0.7, oi_ratio))
        
        # Создаем записи для разных таймфреймов
        for timeframe in ['1m', '15m', '1h']:
            if timeframe == '1m':
                # Для 1m создаем больше вариации
                iv_1m = iv + np.random.normal(0, 0.15)
                skew_1m = skew + np.random.normal(0, 0.02)
                samples = random.randint(3, 8)
            elif timeframe == '15m':
                iv_1m = iv + np.random.normal(0, 0.1)
                skew_1m = skew + np.random.normal(0, 0.015)
                samples = random.randint(15, 25)
            else:  # 1h
                iv_1m = iv + np.random.normal(0, 0.05)
                skew_1m = skew + np.random.normal(0, 0.01)
                samples = random.randint(25, 35)
            
            data.append({
                'timestamp': timestamp,
                'underlying_price': current_price,
                'samples': samples,
                'skew': skew_1m,
                'iv_mean_all': iv_1m,
                'timeframe': timeframe,
                'iv_call_mean': iv_1m + skew_1m/2,
                'iv_put_mean': iv_1m - skew_1m/2,
                'oi_call_total': 1000000 * oi_ratio,
                'oi_put_total': 1000000 * (1 - oi_ratio),
                'oi_ratio': oi_ratio,
                'skew_percentile': 0.5 + skew_1m * 10  # Нормализованный skew
            })
        
        # Обновляем базовую цену
        base_price = current_price
    
    df = pd.DataFrame(data)
    df.to_csv('iv_aggregates_extended.csv', index=False)
    logger.info(f"📊 Создано {len(df)} записей IV данных")
    return df

def generate_extended_trend_signals():
    """Генерирует расширенные трендовые сигналы"""
    logger.info("🔄 Генерация расширенных трендовых сигналов...")
    
    # Читаем расширенные данные IV
    df = pd.read_csv('iv_aggregates_extended.csv', parse_dates=['timestamp'])
    
    signals = []
    
    # Группируем по 15m и 1h
    for timeframe in ['15m', '1h']:
        timeframe_data = df[df['timeframe'] == timeframe].copy()
        timeframe_data = timeframe_data.sort_values('timestamp')
        
        for i in range(1, len(timeframe_data)):
            current = timeframe_data.iloc[i]
            previous = timeframe_data.iloc[i-1]
            
            # Простая логика тренда
            direction = None
            confidence = 0.0
            reason = ""
            
            # Анализируем изменения
            iv_change = current['iv_mean_all'] - previous['iv_mean_all']
            skew_change = current['skew'] - previous['skew']
            oi_ratio = current['oi_ratio']
            
            # Генерируем сигналы на основе изменений
            if iv_change > 0.02 and skew_change > 0.01 and oi_ratio > 0.52:
                direction = 'BUY'
                confidence = min(0.9, 0.5 + abs(iv_change) + abs(skew_change))
                reason = f"IV↑{iv_change:.3f} + Skew↑{skew_change:.3f} + OI call-heavy"
            elif iv_change > 0.02 and skew_change < -0.01 and oi_ratio < 0.48:
                direction = 'SELL'
                confidence = min(0.9, 0.5 + abs(iv_change) + abs(skew_change))
                reason = f"IV↑{iv_change:.3f} + Skew↓{skew_change:.3f} + OI put-heavy"
            elif abs(iv_change) < 0.01 and abs(skew_change) > 0.02:
                if skew_change > 0:
                    direction = 'BUY'
                    confidence = 0.6
                    reason = f"Strong skew↑{skew_change:.3f}"
                else:
                    direction = 'SELL'
                    confidence = 0.6
                    reason = f"Strong skew↓{skew_change:.3f}"
            
            if direction and confidence > 0.3:
                signals.append({
                    'timestamp': current['timestamp'],
                    'timeframe': timeframe,
                    'direction': direction,
                    'confidence': round(confidence, 3),
                    'reason': reason,
                    'iv_call_mean': current['iv_call_mean'],
                    'iv_put_mean': current['iv_put_mean'],
                    'skew': current['skew'],
                    'oi_ratio': current['oi_ratio'],
                    'iv_momentum': iv_change
                })
    
    df_signals = pd.DataFrame(signals)
    df_signals.to_csv('trend_signals_extended.csv', index=False)
    logger.info(f"📈 Создано {len(signals)} трендовых сигналов")
    return df_signals

def generate_extended_entry_points():
    """Генерирует расширенные точки входа"""
    logger.info("🔄 Генерация расширенных точек входа...")
    
    # Читаем расширенные данные IV
    df = pd.read_csv('iv_aggregates_extended.csv', parse_dates=['timestamp'])
    
    # Фильтруем только 1m данные
    df_1m = df[df['timeframe'] == '1m'].copy()
    df_1m = df_1m.sort_values('timestamp')
    
    entries = []
    
    for i in range(5, len(df_1m)):
        current = df_1m.iloc[i]
        
        # Вычисляем среднее IV за последние 5 минут
        recent_iv = df_1m.iloc[i-5:i]['iv_mean_all'].mean()
        iv_spike = (current['iv_mean_all'] - recent_iv) / recent_iv
        
        # Генерируем точки входа на основе IV spikes
        if abs(iv_spike) > 0.03:  # 3% spike
            direction = 'BUY' if iv_spike > 0 else 'SELL'
            confidence = min(0.9, 0.4 + abs(iv_spike) * 5)
            
            entries.append({
                'timestamp': current['timestamp'],
                'direction': direction,
                'timeframe': '1m',
                'confidence': round(confidence, 3),
                'reason': f"IV spike {iv_spike*100:.1f}%",
                'underlying_price': current['underlying_price']
            })
    
    df_entries = pd.DataFrame(entries)
    df_entries.to_csv('entry_points_extended.csv', index=False)
    logger.info(f"🎯 Создано {len(entries)} точек входа")
    return df_entries

def main():
    """Основная функция генерации расширенных данных"""
    logger.info("🚀 Запуск генерации расширенных данных для демонстрации...")
    
    try:
        # Генерируем расширенные данные
        generate_extended_iv_data()
        generate_extended_trend_signals()
        generate_extended_entry_points()
        
        logger.info("✅ Генерация расширенных данных завершена успешно!")
        logger.info("📁 Созданные файлы:")
        logger.info("   - iv_aggregates_extended.csv")
        logger.info("   - trend_signals_extended.csv")
        logger.info("   - entry_points_extended.csv")
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации данных: {e}")

if __name__ == "__main__":
    main()
