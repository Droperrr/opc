#!/usr/bin/env python3
"""
Generate Realistic IV and Basis Data for 2025 Period
Creates realistic IV and basis data based on current patterns and spot price movements
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('realistic_data_generation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RealisticDataGenerator:
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def get_current_iv_patterns(self) -> Dict[str, float]:
        """Получает текущие паттерны IV из базы данных"""
        try:
            query = """
            SELECT 
                markIv,
                underlyingPrice,
                strike_price,
                option_type,
                expiry_date
            FROM iv_data 
            WHERE markIv IS NOT NULL AND markIv > 0
            ORDER BY time DESC
            LIMIT 100
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            if df.empty:
                logger.warning("⚠️ Нет данных IV в базе, используем значения по умолчанию")
                return {
                    'mean_iv': 0.8,
                    'std_iv': 0.2,
                    'min_iv': 0.4,
                    'max_iv': 1.5,
                    'iv_volatility': 0.3
                }
            
            # Анализируем паттерны IV
            patterns = {
                'mean_iv': df['markIv'].mean(),
                'std_iv': df['markIv'].std(),
                'min_iv': df['markIv'].min(),
                'max_iv': df['markIv'].max(),
                'iv_volatility': df['markIv'].std() / df['markIv'].mean()
            }
            
            logger.info(f"📊 Анализ IV паттернов: среднее={patterns['mean_iv']:.3f}, std={patterns['std_iv']:.3f}")
            return patterns
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения паттернов IV: {e}")
            return {
                'mean_iv': 0.8,
                'std_iv': 0.2,
                'min_iv': 0.4,
                'max_iv': 1.5,
                'iv_volatility': 0.3
            }
    
    def get_spot_price_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Получает спотовые данные за указанный период"""
        try:
            query = """
            SELECT 
                time,
                close as spot_price,
                volume
            FROM spot_data 
            WHERE time BETWEEN ? AND ? 
            AND timeframe = '1m'
            ORDER BY time
            """
            
            df = pd.read_sql_query(query, self.conn, params=[start_date, end_date])
            df['time'] = pd.to_datetime(df['time'])
            
            logger.info(f"📊 Загружено {len(df)} записей спотовых данных")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки спотовых данных: {e}")
            return pd.DataFrame()
    
    def generate_realistic_iv_data(self, spot_df: pd.DataFrame, iv_patterns: Dict[str, float]) -> pd.DataFrame:
        """Генерирует реалистичные IV данные на основе спотовых цен и паттернов"""
        try:
            logger.info("🚀 Генерация реалистичных IV данных")
            
            # Создаем копию DataFrame
            df = spot_df.copy()
            
            # Базовые параметры IV
            base_iv = iv_patterns['mean_iv']
            iv_std = iv_patterns['std_iv']
            
            # Генерируем IV на основе волатильности спотовых цен
            spot_returns = df['spot_price'].pct_change().fillna(0)
            spot_volatility = spot_returns.rolling(60).std().fillna(spot_returns.std())
            
            # IV коррелирует с волатильностью спота
            iv_correlation = 0.7
            iv_base = base_iv + iv_correlation * (spot_volatility - spot_volatility.mean()) / spot_volatility.std() * iv_std
            
            # Добавляем случайные колебания
            np.random.seed(42)  # Для воспроизводимости
            iv_noise = np.random.normal(0, iv_std * 0.3, len(df))
            
            # Генерируем IV с учетом трендов
            iv_trend = 0.1 * np.sin(np.arange(len(df)) * 0.001)  # Медленный тренд
            iv_cycle = 0.05 * np.sin(np.arange(len(df)) * 0.01)  # Быстрый цикл
            
            # Финальная IV
            df['iv_30d'] = iv_base + iv_noise + iv_trend + iv_cycle
            
            # Ограничиваем IV разумными пределами
            df['iv_30d'] = df['iv_30d'].clip(iv_patterns['min_iv'], iv_patterns['max_iv'])
            
            # Генерируем skew (разность между call и put IV)
            skew_base = 0.05
            skew_volatility = 0.02
            skew_noise = np.random.normal(0, skew_volatility, len(df))
            skew_trend = 0.02 * np.sin(np.arange(len(df)) * 0.002)
            
            df['skew_30d'] = skew_base + skew_noise + skew_trend
            df['skew_30d'] = df['skew_30d'].clip(-0.2, 0.2)
            
            logger.info(f"✅ Сгенерировано IV данных: среднее={df['iv_30d'].mean():.3f}, std={df['iv_30d'].std():.3f}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации IV данных: {e}")
            return spot_df
    
    def generate_realistic_basis_data(self, spot_df: pd.DataFrame) -> pd.DataFrame:
        """Генерирует реалистичные basis данные на основе спотовых цен"""
        try:
            logger.info("🚀 Генерация реалистичных basis данных")
            
            df = spot_df.copy()
            
            # Базовые параметры basis
            basis_mean = 0.001  # 0.1% в среднем
            basis_std = 0.0005  # 0.05% стандартное отклонение
            
            # Basis коррелирует с трендом спота
            spot_trend = df['spot_price'].rolling(240).mean() / df['spot_price'] - 1
            basis_correlation = 0.3
            basis_base = basis_mean + basis_correlation * spot_trend * basis_std * 10
            
            # Добавляем случайные колебания
            np.random.seed(42)  # Для воспроизводимости
            basis_noise = np.random.normal(0, basis_std, len(df))
            
            # Генерируем funding rate
            funding_base = 0.0001  # 0.01% в среднем
            funding_volatility = 0.00005
            funding_noise = np.random.normal(0, funding_volatility, len(df))
            funding_cycle = 0.00002 * np.sin(np.arange(len(df)) * 0.005)
            
            # Финальные значения
            df['basis_rel'] = basis_base + basis_noise
            df['basis_rel'] = df['basis_rel'].clip(-0.01, 0.01)  # Ограничиваем ±1%
            
            df['funding_rate'] = funding_base + funding_noise + funding_cycle
            df['funding_rate'] = df['funding_rate'].clip(-0.001, 0.001)  # Ограничиваем ±0.1%
            
            # Генерируем open interest (коррелирует с объемом)
            oi_base = 1000000
            oi_correlation = 0.5
            volume_normalized = df['volume'] / df['volume'].mean()
            df['oi_total'] = oi_base * (1 + oi_correlation * (volume_normalized - 1))
            df['oi_total'] = df['oi_total'].clip(500000, 5000000)
            
            logger.info(f"✅ Сгенерировано basis данных: среднее={df['basis_rel'].mean():.6f}, std={df['basis_rel'].std():.6f}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации basis данных: {e}")
            return spot_df
    
    def save_to_database(self, df: pd.DataFrame, table_name: str):
        """Сохраняет данные в базу данных"""
        try:
            logger.info(f"💾 Сохранение данных в таблицу {table_name}")
            
            cursor = self.conn.cursor()
            
            # Создаем таблицу если её нет
            if table_name == 'iv_agg_realistic':
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS iv_agg_realistic (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        time TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        spot_price REAL,
                        iv_30d REAL,
                        skew_30d REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(time, timeframe)
                    )
                ''')
            elif table_name == 'basis_agg_realistic':
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS basis_agg_realistic (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        time TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        basis_rel REAL,
                        funding_rate REAL,
                        oi_total REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(time, timeframe)
                    )
                ''')
            
            # Сохраняем данные
            for _, row in df.iterrows():
                time_str = row['time'].strftime('%Y-%m-%d %H:%M:%S')
                
                if table_name == 'iv_agg_realistic':
                    cursor.execute('''
                        INSERT OR REPLACE INTO iv_agg_realistic 
                        (time, timeframe, spot_price, iv_30d, skew_30d)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        time_str, '1m', row['spot_price'], row['iv_30d'], row['skew_30d']
                    ))
                elif table_name == 'basis_agg_realistic':
                    cursor.execute('''
                        INSERT OR REPLACE INTO basis_agg_realistic 
                        (time, timeframe, basis_rel, funding_rate, oi_total)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        time_str, '1m', row['basis_rel'], row['funding_rate'], row['oi_total']
                    ))
            
            self.conn.commit()
            logger.info(f"✅ Сохранено {len(df)} записей в {table_name}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в базу данных: {e}")
    
    def update_existing_tables(self, df: pd.DataFrame):
        """Обновляет существующие таблицы iv_agg и basis_agg"""
        try:
            logger.info("🔄 Обновление существующих таблиц iv_agg и basis_agg")
            
            cursor = self.conn.cursor()
            
            # Очищаем старые данные
            cursor.execute("DELETE FROM iv_agg WHERE timeframe = '1m'")
            cursor.execute("DELETE FROM basis_agg WHERE timeframe = '1m'")
            
            # Вставляем новые данные
            for _, row in df.iterrows():
                time_str = row['time'].strftime('%Y-%m-%d %H:%M:%S')
                
                # Обновляем iv_agg
                cursor.execute('''
                    INSERT INTO iv_agg 
                    (time, timeframe, spot_price, iv_30d, skew_30d, basis_rel, oi_total, funding_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    time_str, '1m', row['spot_price'], row['iv_30d'], row['skew_30d'],
                    row['basis_rel'], row['oi_total'], row['funding_rate']
                ))
                
                # Обновляем basis_agg
                cursor.execute('''
                    INSERT INTO basis_agg 
                    (time, timeframe, basis_rel, funding_rate)
                    VALUES (?, ?, ?, ?)
                ''', (
                    time_str, '1m', row['basis_rel'], row['funding_rate']
                ))
            
            self.conn.commit()
            logger.info("✅ Существующие таблицы обновлены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления таблиц: {e}")
    
    def generate_all_data(self, start_date: str, end_date: str):
        """Генерирует все реалистичные данные за указанный период"""
        try:
            logger.info(f"🚀 Генерация реалистичных данных за период {start_date} - {end_date}")
            
            # Получаем паттерны IV
            iv_patterns = self.get_current_iv_patterns()
            
            # Загружаем спотовые данные
            spot_df = self.get_spot_price_data(start_date, end_date)
            if spot_df.empty:
                logger.error("❌ Нет спотовых данных для генерации")
                return False
            
            # Генерируем IV данные
            df_with_iv = self.generate_realistic_iv_data(spot_df, iv_patterns)
            
            # Генерируем basis данные
            df_complete = self.generate_realistic_basis_data(df_with_iv)
            
            # Сохраняем в новые таблицы
            self.save_to_database(df_complete, 'iv_agg_realistic')
            self.save_to_database(df_complete, 'basis_agg_realistic')
            
            # Обновляем существующие таблицы
            self.update_existing_tables(df_complete)
            
            logger.info("🎉 Генерация данных завершена успешно")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации данных: {e}")
            return False
    
    def close(self):
        """Закрывает соединение с базой данных"""
        self.conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Генерация реалистичных IV и basis данных')
    parser.add_argument('--start', required=True,
                       help='Дата начала в формате YYYY-MM-DD')
    parser.add_argument('--end', required=True,
                       help='Дата окончания в формате YYYY-MM-DD')
    parser.add_argument('--db', default='data/sol_iv.db',
                       help='Путь к базе данных')
    
    args = parser.parse_args()
    
    # Создаем папку data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Инициализируем генератор
    generator = RealisticDataGenerator(args.db)
    
    try:
        # Генерируем данные
        success = generator.generate_all_data(args.start, args.end)
        
        if success:
            logger.info("✅ Генерация реалистичных данных завершена успешно")
        else:
            logger.error("❌ Генерация данных завершена с ошибками")
            
    finally:
        generator.close()

if __name__ == "__main__":
    main()
