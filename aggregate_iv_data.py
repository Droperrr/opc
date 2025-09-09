#!/usr/bin/env python3
"""
Скрипт для агрегации данных IV из таблицы iv_data в таблицу iv_agg
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def aggregate_iv_data():
    """Агрегация данных IV по разным таймфреймам"""
    print("Агрегация данных IV из таблицы iv_data в таблицу iv_agg...")
    
    # Подключаемся к базе данных
    conn = sqlite3.connect('server_opc.db')
    
    try:
        # Загружаем данные из таблицы iv_data
        df = pd.read_sql_query("""
            SELECT 
                time, 
                markIv, 
                underlyingPrice,
                symbol
            FROM iv_data 
            ORDER BY time
        """, conn, parse_dates=['time'])
        
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
                    'oi_total': 0.0    # Для упрощения, можно рассчитать позже
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
    print("Агрегация данных IV для системы OPC")
    
    # Выполняем агрегацию данных
    aggregate_iv_data()
    
    print("Агрегация данных завершена!")

if __name__ == "__main__":
    main()