#!/usr/bin/env python3
"""
Генератор сигналов на основе "свечной" стратегии
Логика: LONG при зеленой свече, SHORT при красной свече
Сигналы на выход: CLOSE_LONG при красной свече после серии зеленых, CLOSE_SHORT при зеленой после серии красных
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from typing import Dict

def generate_candle_signals(data_file='basis_features_1m_btc.parquet', output_file='candle_signals_btc.csv'):
    """
    Генерирует торговые сигналы на основе "свечной" стратегии.
    
    Args:
        data_file (str): Путь к файлу с данными.
        output_file (str): Путь к выходному файлу с сигналами.
    """
    print(f"📈 Генерация \"свечных\" сигналов BTC из {data_file}...")
    
    # Загружаем данные
    if not os.path.exists(data_file):
        print(f"❌ Файл {data_file} не найден")
        return
    
    df = pd.read_parquet(data_file)
    print(f"📊 Загружено {len(df)} записей")
    
    # Проверяем наличие необходимых колонок
    required_columns = ['time', 'spot_price']
    for col in required_columns:
        if col not in df.columns:
            print(f"❌ Отсутствует колонка {col}")
            return
    
    # Сортируем данные по времени
    df = df.sort_values('time').reset_index(drop=True)
    
    # Рассчитываем цены открытия как цены закрытия предыдущей свечи
    df['open_price'] = df['spot_price'].shift(1)
    
    # Удаляем первую строку, так как для нее нет цены открытия
    df = df.dropna(subset=['open_price']).reset_index(drop=True)
    
    # Определяем цвет свечи
    df['candle_color'] = np.where(df['spot_price'] > df['open_price'], 'green', 
                                 np.where(df['spot_price'] < df['open_price'], 'red', 'doji'))
    
    # Генерируем сигналы
    signals = []
    current_position = None  # None, 'LONG', 'SHORT'
    entry_price = None
    
    for idx, row in df.iterrows():
        timestamp = row['time']
        open_price = row['open_price']
        close_price = row['spot_price']
        candle_color = row['candle_color']
        
        # Определяем сессию на основе времени
        hour = pd.to_datetime(timestamp).hour
        if 0 <= hour < 8:
            session = 'asian'
        elif 8 <= hour < 16:
            session = 'european'
        else:
            session = 'us'
        
        # Генерируем сигналы на вход
        if current_position is None:
            if candle_color == 'green':
                signal = 'LONG'
                current_position = 'LONG'
                entry_price = open_price
            elif candle_color == 'red':
                signal = 'SHORT'
                current_position = 'SHORT'
                entry_price = open_price
            else:
                signal = None
        # Генерируем сигналы на выход
        elif current_position == 'LONG' and candle_color == 'red':
            signal = 'CLOSE_LONG'
            current_position = None
            entry_price = None
        elif current_position == 'SHORT' and candle_color == 'green':
            signal = 'CLOSE_SHORT'
            current_position = None
            entry_price = None
        else:
            signal = None
        
        # Если сигнал есть, добавляем его в список
        if signal:
            signals.append({
                'timestamp': timestamp,
                'signal': signal,
                'open_price': open_price,
                'close_price': close_price,
                'candle_color': candle_color,
                'session': session,
                'timeframe': '1m',
                'underlying_price': open_price,  # Используем цену открытия как базовую цену для входа
                'iv': 0,  # Добавляем поле iv с нулевым значением
                'confidence': 0.8,  # Добавляем поле confidence с фиксированным значением
                'reason': 'candle_color'  # Добавляем поле reason с фиксированным значением
            })
    
    # Создаем DataFrame с сигналами
    signals_df = pd.DataFrame(signals)
    
    # Сохраняем в CSV
    signals_df.to_csv(output_file, index=False)
    print(f"💾 Сохранено {len(signals_df)} сигналов BTC в {output_file}")
    
    # Выводим статистику
    if not signals_df.empty:
        print(f"📊 Статистика сигналов:")
        print(f"   LONG сигналов: {len(signals_df[signals_df['signal'] == 'LONG'])}")
        print(f"   SHORT сигналов: {len(signals_df[signals_df['signal'] == 'SHORT'])}")
        print(f"   CLOSE_LONG сигналов: {len(signals_df[signals_df['signal'] == 'CLOSE_LONG'])}")
        print(f"   CLOSE_SHORT сигналов: {len(signals_df[signals_df['signal'] == 'CLOSE_SHORT'])}")

def main():
    """Основная функция"""
    generate_candle_signals()

if __name__ == "__main__":
    main()