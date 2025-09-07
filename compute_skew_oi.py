#!/usr/bin/env python3
"""
Модуль вычисления агрегатов IV и OI по таймфреймам
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta
from logger import get_logger

logger = get_logger()

TIMEFRAMES = ['1m', '15m', '1h']

def resample_to_interval(df, interval):
    """Ресемплирует данные к указанному интервалу"""
    # df.time is datetime
    if interval == '1m':
        rule = 'T'
    elif interval == '15m':
        rule = '15T'
    elif interval == '1h':
        rule = 'H'
    else:
        raise ValueError(interval)
    
    df = df.set_index('timestamp').resample(rule).agg({
        'mark_iv': 'mean',
        'underlying_price': 'last',
        'symbol': 'count',  # count of samples
        'open_interest': 'sum',
        'skew': 'mean'
    }).rename(columns={'symbol': 'samples'})
    
    df = df.reset_index()
    return df

def compute_agg(db_path='data/options_enriched.db', timeframe='15m', out_table='iv_agg'):
    """Вычисляет агрегаты IV и OI для указанного таймфрейма"""
    logger.info(f"🚀 Вычисление агрегатов для таймфрейма {timeframe}")
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Проверяем существование таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='options_enriched'")
        if not cursor.fetchone():
            logger.error("❌ Таблица options_enriched не найдена")
            return None
        
        # Загружаем данные
        sql = "SELECT timestamp, symbol, mark_iv, underlying_price, open_interest, skew, option_type FROM options_enriched"
        df = pd.read_sql_query(sql, conn, parse_dates=['timestamp'])
        
        if df.empty:
            logger.warning("⚠️ Нет данных в таблице options_enriched")
            return None
        
        logger.info(f"📊 Загружено {len(df)} записей")
        
        # Убеждаемся, что данные числовые
        df['mark_iv'] = pd.to_numeric(df['mark_iv'], errors='coerce')
        df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0)
        df['skew'] = pd.to_numeric(df['skew'], errors='coerce').fillna(0)
        
        # Ресемплируем к указанному таймфрейму
        df_res = resample_to_interval(df, timeframe)
        
        # Добавляем дополнительные колонки
        df_res['iv_mean_all'] = df_res['mark_iv']
        df_res['timeframe'] = timeframe
        
        # Вычисляем средние IV по call/put отдельно
        call_mean = df[df['option_type'] == 'C'].groupby(df['timestamp'].dt.floor({
            '1m': 'T', '15m': '15T', '1h': 'H'
        }[timeframe]))['mark_iv'].mean().rename('iv_call_mean')
        
        put_mean = df[df['option_type'] == 'P'].groupby(df['timestamp'].dt.floor({
            '1m': 'T', '15m': '15T', '1h': 'H'
        }[timeframe]))['mark_iv'].mean().rename('iv_put_mean')
        
        # Объединяем с основными данными
        df_res = df_res.merge(call_mean, left_on='timestamp', right_index=True, how='left')
        df_res = df_res.merge(put_mean, left_on='timestamp', right_index=True, how='left')
        
        # Заполняем пропуски
        df_res['iv_call_mean'] = df_res['iv_call_mean'].fillna(df_res['iv_mean_all'])
        df_res['iv_put_mean'] = df_res['iv_put_mean'].fillna(df_res['iv_mean_all'])
        
        # Вычисляем skew
        df_res['skew'] = df_res['iv_put_mean'] - df_res['iv_call_mean']
        
        # Вычисляем OI по группам
        oi_call = df[df['option_type'] == 'C'].groupby(df['timestamp'].dt.floor({
            '1m': 'T', '15m': '15T', '1h': 'H'
        }[timeframe]))['open_interest'].sum().rename('oi_call_total')
        
        oi_put = df[df['option_type'] == 'P'].groupby(df['timestamp'].dt.floor({
            '1m': 'T', '15m': '15T', '1h': 'H'
        }[timeframe]))['open_interest'].sum().rename('oi_put_total')
        
        df_res = df_res.merge(oi_call, left_on='timestamp', right_index=True, how='left')
        df_res = df_res.merge(oi_put, left_on='timestamp', right_index=True, how='left')
        
        # Заполняем пропуски OI
        df_res['oi_call_total'] = df_res['oi_call_total'].fillna(0)
        df_res['oi_put_total'] = df_res['oi_put_total'].fillna(0)
        
        # Вычисляем OI ratio
        df_res['oi_ratio'] = df_res.apply(
            lambda r: r['oi_call_total'] / (r['oi_call_total'] + r['oi_put_total']) 
            if (r['oi_call_total'] + r['oi_put_total']) > 0 else 0.5, 
            axis=1
        )
        
        # Вычисляем skew percentile (по всей истории)
        df_res['skew_percentile'] = df_res['skew'].rank(pct=True)
        
        # Убираем лишние колонки
        df_res = df_res.drop(['mark_iv', 'open_interest'], axis=1, errors='ignore')
        
        # Сохраняем в БД
        df_res.to_sql(out_table, conn, if_exists='replace', index=False)
        
        logger.info(f"💾 Сохранено {len(df_res)} записей в таблицу {out_table}")
        
        # Показываем примеры
        logger.info(f"📋 Примеры агрегатов для {timeframe}:")
        print(df_res.head(5).to_string(index=False))
        
        conn.close()
        return df_res
        
    except Exception as e:
        logger.error(f"❌ Ошибка вычисления агрегатов: {e}")
        return None

def compute_all_timeframes():
    """Вычисляет агрегаты для всех таймфреймов"""
    logger.info("🚀 Вычисление агрегатов для всех таймфреймов")
    
    results = {}
    
    for timeframe in TIMEFRAMES:
        logger.info(f"📊 Обрабатываем таймфрейм: {timeframe}")
        result = compute_agg(timeframe=timeframe, out_table=f'iv_agg_{timeframe}')
        if result is not None:
            results[timeframe] = result
    
    return results

def export_aggregates_sample():
    """Экспортирует примеры агрегатов в CSV"""
    try:
        conn = sqlite3.connect('data/options_enriched.db')
        
        # Получаем данные из всех таблиц агрегатов
        all_data = []
        
        for timeframe in TIMEFRAMES:
            table_name = f'iv_agg_{timeframe}'
            
            # Проверяем существование таблицы
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            
            if cursor.fetchone():
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                df['timeframe'] = timeframe
                all_data.append(df)
        
        if all_data:
            # Объединяем все данные
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Сохраняем в CSV
            csv_filename = 'iv_aggregates_sample.csv'
            combined_df.to_csv(csv_filename, index=False)
            
            logger.info(f"💾 Экспортировано {len(combined_df)} записей в {csv_filename}")
            
            # Показываем примеры
            logger.info("📋 Примеры агрегатов:")
            print(combined_df.head(10).to_string(index=False))
            
            return combined_df
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта агрегатов: {e}")
        return None

def run_computation_demo():
    """Демонстрация вычисления агрегатов"""
    logger.info("🎯 Демонстрация вычисления агрегатов IV и OI")
    
    # Вычисляем агрегаты для всех таймфреймов
    results = compute_all_timeframes()
    
    if results:
        logger.info("✅ Агрегаты успешно вычислены")
        
        # Экспортируем примеры
        sample_data = export_aggregates_sample()
        
        if sample_data is not None:
            # Показываем статистику
            print(f"\n📊 Статистика агрегатов:")
            for timeframe, df in results.items():
                print(f"{timeframe}: {len(df)} записей")
                if len(df) > 0:
                    print(f"  Средняя IV: {df['iv_mean_all'].mean():.4f}")
                    print(f"  Средний Skew: {df['skew'].mean():.4f}")
                    print(f"  Средний OI ratio: {df['oi_ratio'].mean():.3f}")
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация вычисления агрегатов
    run_computation_demo()
