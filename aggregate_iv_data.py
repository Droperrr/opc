#!/usr/bin/env python3
"""
Скрипт для агрегации данных IV из таблицы iv_data в таблицу iv_agg
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import argparse

def aggregate_iv_data(symbol='BTCUSDT', dataset_tag='training_2023'):
    """Агрегация данных IV по разным таймфреймам с фильтрацией по symbol и dataset_tag"""
    print(f"Агрегация данных IV из таблицы iv_data в таблицу iv_agg для {symbol} ({dataset_tag})...")
    
    # Подключаемся к базе данных
    conn = sqlite3.connect('server_opc.db')
    
    try:
        # Загружаем данные из таблицы iv_data с фильтрацией
        df = pd.read_sql_query("""
            SELECT 
                time, 
                markIv, 
                underlyingPrice,
                symbol,
                dataset_tag
            FROM iv_data 
            WHERE symbol = ? AND dataset_tag = ?
            ORDER BY time
        """, conn, params=(symbol, dataset_tag), parse_dates=['time'])
        
        if df.empty:
            print("⚠️ Нет данных для агрегации")
            return
        
        print(f"📊 Загружено {len(df)} записей из таблицы iv_data")
        
        # Преобразуем время в datetime
        df['time'] = pd.to_datetime(df['time'])
        
        # Создаем агрегированные данные для разных таймфреймов
        timeframes = ['1m', '15m', '1h']
        aggregated_data = []
        
        for timeframe in timeframes:
            print(f"🔄 Агрегация данных для таймфрейма {timeframe}...")
            
            # Определяем частоту для ресемплинга
            if timeframe == '1m':
                freq = '1min'
            elif timeframe == '15m':
                freq = '15min'
            elif timeframe == '1h':
                freq = '1h'
            else:
                continue
            
            # Ресемплинг данных
            df_resampled = df.set_index('time').resample(freq).agg({
                'markIv': 'mean',
                'underlyingPrice': 'last'
            }).dropna()
            
            # Добавляем агрегированные данные
            for timestamp, row in df_resampled.iterrows():
                aggregated_data.append({
                    'time': timestamp,
                    'timeframe': timeframe,
                    'spot_price': row['underlyingPrice'],
                    'iv_30d': row['markIv'],
                    'skew_30d': 0.0,  # Для упрощения, можно рассчитать позже
                    'basis_rel': 0.0,  # Для упрощения, можно рассчитать позже
                    'oi_total': 0.0,   # Для упрощения, можно рассчитать позже
                    'symbol': symbol,
                    'dataset_tag': dataset_tag
                })
        
        # Создаем DataFrame с агрегированными данными
        agg_df = pd.DataFrame(aggregated_data)
        
        if not agg_df.empty:
            # Сохраняем агрегированные данные в таблицу iv_agg
            agg_df.to_sql('iv_agg', conn, if_exists='replace', index=False)
            print(f"✅ Сохранено {len(aggregated_data)} агрегированных записей в таблицу iv_agg")
        else:
            print("⚠️ Нет агрегированных данных для сохранения")
            
    except Exception as e:
        print(f"❌ Ошибка агрегации данных: {e}")
    finally:
        conn.close()

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Агрегация данных IV для системы OPC')
    parser.add_argument('--symbol', default='BTCUSDT',
                       help='Символ актива (например, BTCUSDT, SOLUSDT)')
    parser.add_argument('--tag', default='training_2023',
                       help='Тег набора данных (например, training_2023, live_2025)')
    
    args = parser.parse_args()
    
    print(f"Агрегация данных IV для системы OPC - {args.symbol} ({args.tag})")
    
    # Выполняем агрегацию данных
    aggregate_iv_data(args.symbol, args.tag)
    
    print("Агрегация данных завершена!")

if __name__ == "__main__":
    main()