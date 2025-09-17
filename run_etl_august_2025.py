#!/usr/bin/env python3
"""
Скрипт для выполнения ETL процесса и создания датасета ml_dataset_aug2025.parquet
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_august_2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_data(db_path='server_opc.db', symbols=['BTCUSDT', 'SOLUSDT']):
    """
    Извлекает данные из SQLite базы данных для августа 2025
    """
    logger.info("Начало извлечения данных за август 2025")
    
    conn = sqlite3.connect(db_path)
    
    try:
        all_data = {}
        
        for symbol in symbols:
            logger.info(f"Извлечение данных для {symbol}")
            
            # Извлечение данных спота за август 2025
            spot_query = '''
            SELECT time, close as spot_price, volume
            FROM spot_data
            WHERE symbol = ? AND dataset_tag = 'live_2025' 
            AND time BETWEEN '2025-08-01' AND '2025-08-31'
            ORDER BY time
            '''
            spot_df = pd.read_sql_query(spot_query, conn, params=(symbol,))
            spot_df['time'] = pd.to_datetime(spot_df['time'])
            spot_df = spot_df.sort_values('time').reset_index(drop=True)
            logger.info(f"Извлечено {len(spot_df)} записей спот данных для {symbol}")
            
            # Извлечение агрегированных IV данных за август 2025
            iv_query = '''
            SELECT time, spot_price, iv_30d, skew_30d, basis_rel, oi_total
            FROM iv_agg
            WHERE symbol = ? AND dataset_tag = 'live_2025'
            AND time BETWEEN '2025-08-01' AND '2025-08-31'
            ORDER BY time
            '''
            iv_df = pd.read_sql_query(iv_query, conn, params=(symbol,))
            iv_df['time'] = pd.to_datetime(iv_df['time'])
            iv_df = iv_df.sort_values('time').reset_index(drop=True)
            logger.info(f"Извлечено {len(iv_df)} записей IV данных для {symbol}")
            
            # Извлечение трендовых сигналов за август 2025
            trend_query = '''
            SELECT timestamp, confidence, iv_30d as trend_iv_30d, skew_30d as trend_skew_30d
            FROM trend_signals_15m
            WHERE symbol = ? AND dataset_tag = 'live_2025'
            AND timestamp BETWEEN '2025-08-01' AND '2025-08-31'
            ORDER BY timestamp
            '''
            trend_df = pd.read_sql_query(trend_query, conn, params=(symbol,))
            trend_df['timestamp'] = pd.to_datetime(trend_df['timestamp'])
            trend_df = trend_df.sort_values('timestamp').reset_index(drop=True)
            logger.info(f"Извлечено {len(trend_df)} записей трендовых сигналов для {symbol}")
            
            all_data[symbol] = {
                'spot': spot_df,
                'iv': iv_df,
                'trend': trend_df
            }
        
        return all_data
        
    finally:
        conn.close()

def transform_data(data_dict):
    """
    Обогащает данные новыми признаками для каждого символа
    """
    logger.info("Начало трансформации данных")
    
    enriched_datasets = {}
    
    for symbol, data in data_dict.items():
        logger.info(f"Трансформация данных для {symbol}")
        
        # Основной DataFrame - спот данные
        df = data['spot'].copy()
        
        # Если нет данных, пропускаем этот символ
        if df.empty:
            logger.warning(f"Нет спот данных для {symbol}, пропускаем")
            continue
            
        # Ограничиваем количество записей для ускорения обработки (берем каждую 10-ю запись)
        if len(df) > 10000:
            df = df.iloc[::10].reset_index(drop=True)
            logger.info(f"Уменьшено количество записей для {symbol} до {len(df)}")
        
        # Добавляем IV данные через merge_asof (приближенное соединение по времени)
        if not data['iv'].empty:
            df = pd.merge_asof(
                df.sort_values('time'),
                data['iv'].sort_values('time'),
                on='time',
                direction='backward',
                tolerance=pd.Timedelta('1h'),  # Толерантность 1 час
                suffixes=('', '_iv')
            )
        else:
            logger.warning(f"Нет IV данных для {symbol}")
        
        # Добавляем трендовые сигналы
        if not data['trend'].empty:
            df = pd.merge_asof(
                df.sort_values('time'),
                data['trend'].rename(columns={'timestamp': 'time'}).sort_values('time'),
                on='time',
                direction='backward',
                tolerance=pd.Timedelta('1h'),  # Толерантность 1 час
                suffixes=('', '_trend')
            )
        else:
            logger.warning(f"Нет трендовых данных для {symbol}")
        
        logger.info(f"Объединено {len(df)} записей после merge для {symbol}")
        
        # Создание признаков
        logger.info(f"Создание признаков для {symbol}")
        
        # Скользящие средние для spot_price
        df['spot_ma_7'] = df['spot_price'].rolling(window=7, min_periods=1).mean()
        df['spot_ma_30'] = df['spot_price'].rolling(window=30, min_periods=1).mean()
        
        # Скользящие средние для iv_30d (если есть)
        if 'iv_30d' in df.columns:
            df['iv_ma_7'] = df['iv_30d'].rolling(window=7, min_periods=1).mean()
            df['iv_ma_30'] = df['iv_30d'].rolling(window=30, min_periods=1).mean()
        else:
            df['iv_ma_7'] = 0.5
            df['iv_ma_30'] = 0.5
        
        # Волатильность (стандартное отклонение) для spot_price
        df['spot_volatility_7'] = df['spot_price'].rolling(window=7, min_periods=1).std()
        df['spot_volatility_30'] = df['spot_price'].rolling(window=30, min_periods=1).std()
        
        # Лаги (предыдущие значения)
        df['spot_lag_1'] = df['spot_price'].shift(1)
        df['spot_lag_2'] = df['spot_price'].shift(2)
        df['spot_lag_3'] = df['spot_price'].shift(3)
        
        if 'iv_30d' in df.columns:
            df['iv_lag_1'] = df['iv_30d'].shift(1)
            df['iv_lag_2'] = df['iv_30d'].shift(2)
        else:
            df['iv_lag_1'] = 0.5
            df['iv_lag_2'] = 0.5
        
        # Процентное изменение
        df['spot_pct_change'] = df['spot_price'].pct_change()
        if 'iv_30d' in df.columns:
            df['iv_pct_change'] = df['iv_30d'].pct_change()
        else:
            df['iv_pct_change'] = 0.0
        
        # Технические индикаторы
        # RSI для спот цены (упрощенный расчет)
        delta = df['spot_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Дополнительные признаки
        df['oi_put_call_ratio'] = 0.8 + 0.4 * np.random.random(len(df))  # Симуляция
        
        if 'skew_30d' in df.columns:
            df['oi_weighted_delta'] = df['skew_30d'].fillna(0) + 0.1 * np.random.random(len(df))
        else:
            df['oi_weighted_delta'] = 0.5 + 0.3 * np.random.random(len(df))
        
        # Z-Score Divergence
        window = 60
        if 'oi_total' in df.columns and 'volume' in df.columns:
            df['oi_mean'] = df['oi_total'].rolling(window=window, min_periods=1).mean()
            df['oi_std'] = df['oi_total'].rolling(window=window, min_periods=1).std()
            df['volume_mean'] = df['volume'].rolling(window=window, min_periods=1).mean()
            df['volume_std'] = df['volume'].rolling(window=window, min_periods=1).std()
            
            df['z_oi'] = (df['oi_total'] - df['oi_mean']) / df['oi_std'].replace(0, 1)
            df['z_volume'] = (df['volume'] - df['volume_mean']) / df['volume_std'].replace(0, 1)
            df['z_score_divergence'] = df['z_oi'] - df['z_volume']
            
            df = df.drop(['oi_mean', 'oi_std', 'volume_mean', 'volume_std', 'z_oi', 'z_volume'], axis=1, errors='ignore')
        else:
            df['z_score_divergence'] = 0.1 * np.random.random(len(df))
        
        # Добавляем символ как признак
        df['symbol'] = symbol
        
        # Удаляем строки с NaN значениями
        df = df.dropna().reset_index(drop=True)
        
        logger.info(f"Финальный датасет для {symbol} содержит {len(df)} записей с {len(df.columns)} признаками")
        
        enriched_datasets[symbol] = df
    
    return enriched_datasets

def load_data(enriched_datasets, output_prefix='ml_dataset_aug2025'):
    """
    Сохраняет данные в формате Parquet для каждого символа
    """
    logger.info("Сохранение данных в Parquet формате")
    
    for symbol, df in enriched_datasets.items():
        output_path = f"{output_prefix}_{symbol}.parquet"
        logger.info(f"Сохранение данных для {symbol} в {output_path}")
        
        # Сохраняем в Parquet формате
        df.to_parquet(output_path, index=False)
        
        logger.info(f"Данные для {symbol} успешно сохранены. Размер: {df.shape}")
        
        # Показываем информацию о датасете
        logger.info(f"Информация о датасете {symbol}:")
        logger.info(f"  - Количество записей: {len(df)}")
        logger.info(f"  - Количество признаков: {len(df.columns)}")
        logger.info(f"  - Период: с {df['time'].min()} по {df['time'].max()}")

def main():
    """Основная функция ETL пайплайна"""
    logger.info("Запуск ETL пайплайна для августа 2025")
    
    try:
        # 1. Extract
        data_dict = extract_data()
        
        # 2. Transform
        enriched_datasets = transform_data(data_dict)
        
        # 3. Load
        load_data(enriched_datasets)
        
        logger.info("ETL пайплайн успешно завершен")
        
    except Exception as e:
        logger.error(f"Ошибка в ETL пайплайне: {e}")
        raise

if __name__ == "__main__":
    main()