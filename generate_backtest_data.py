#!/usr/bin/env python3
"""
Генерация синтетических данных для backtesting
Задача S-04: Создание 6-месячных данных для тестирования
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from logger import get_logger

logger = get_logger()

class BacktestDataGenerator:
    def __init__(self):
        self.db_path = 'data/sol_iv.db'
        
    def create_backtest_tables(self):
        """Создает таблицы для backtesting"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица iv_agg (агрегированные данные по опционам)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS iv_agg (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    spot_price REAL NOT NULL,
                    iv_30d REAL,
                    skew_30d REAL,
                    basis_rel REAL,
                    oi_total REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица basis_agg (агрегированные данные по basis)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS basis_agg (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    basis_rel REAL,
                    funding_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info("✅ Таблицы для backtesting созданы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
    
    def generate_synthetic_data(self, start_date='2024-03-01', end_date='2024-09-01'):
        """Генерирует синтетические данные за 6 месяцев"""
        try:
            # Создаем временной ряд (1 минута)
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Генерируем временные метки (каждую минуту)
            timestamps = pd.date_range(start=start_dt, end=end_dt, freq='1min')
            
            logger.info(f"📊 Генерирую данные для {len(timestamps)} временных точек")
            
            # Базовые параметры
            base_price = 100.0
            base_iv = 0.5
            base_skew = 0.0
            base_basis = 0.0
            
            # Генерируем данные
            data = []
            
            for i, ts in enumerate(timestamps):
                # Добавляем тренд и волатильность
                trend_factor = np.sin(i / (24 * 60 * 30)) * 0.1  # Месячный цикл
                volatility_factor = np.abs(np.sin(i / (24 * 60 * 7))) * 0.3  # Недельный цикл
                
                # Цена спота с трендом и шумом
                price_change = trend_factor + np.random.normal(0, 0.001)
                spot_price = base_price * (1 + price_change)
                
                # IV с волатильностью
                iv_30d = base_iv + volatility_factor + np.random.normal(0, 0.05)
                iv_30d = max(0.1, min(1.5, iv_30d))  # Ограничиваем IV
                
                # Skew с корреляцией с IV
                skew_30d = np.random.normal(0, 0.2) + (iv_30d - base_iv) * 0.5
                skew_30d = max(-1.0, min(1.0, skew_30d))
                
                # Basis с корреляцией с ценой
                basis_rel = np.random.normal(0, 0.01) + price_change * 0.1
                basis_rel = max(-0.05, min(0.05, basis_rel))
                
                # Open Interest
                oi_total = np.random.uniform(1000, 10000)
                
                # Funding Rate
                funding_rate = np.random.normal(0, 0.0001)
                
                data.append({
                    'time': ts.strftime('%Y-%m-%d %H:%M:%S'),
                    'timeframe': '1m',
                    'spot_price': spot_price,
                    'iv_30d': iv_30d,
                    'skew_30d': skew_30d,
                    'basis_rel': basis_rel,
                    'oi_total': oi_total,
                    'funding_rate': funding_rate
                })
                
                # Обновляем базовые значения для следующей итерации
                base_price = spot_price
                base_iv = iv_30d * 0.99 + 0.5 * 0.01  # Плавное изменение
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации данных: {e}")
            return pd.DataFrame()
    
    def save_to_database(self, df):
        """Сохраняет данные в базу данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Сохраняем в iv_agg
            iv_data = df[['time', 'timeframe', 'spot_price', 'iv_30d', 'skew_30d', 'basis_rel', 'oi_total']].copy()
            iv_data.to_sql('iv_agg', conn, if_exists='replace', index=False)
            
            # Сохраняем в basis_agg
            basis_data = df[['time', 'timeframe', 'basis_rel', 'funding_rate']].copy()
            basis_data.to_sql('basis_agg', conn, if_exists='replace', index=False)
            
            conn.close()
            
            logger.info(f"✅ Данные сохранены в базу: {len(df)} записей")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
    
    def create_sample_data(self):
        """Создает небольшой набор данных для быстрого тестирования"""
        try:
            # Создаем данные за 1 неделю для быстрого тестирования
            start_date = '2024-08-01'
            end_date = '2024-08-08'
            
            logger.info("🔧 Создаю тестовые данные за 1 неделю")
            
            # Создаем таблицы
            self.create_backtest_tables()
            
            # Генерируем данные
            df = self.generate_synthetic_data(start_date, end_date)
            
            if not df.empty:
                # Сохраняем в БД
                self.save_to_database(df)
                
                # Сохраняем CSV для проверки
                df.to_csv('data/backtests/sample_data.csv', index=False)
                
                logger.info(f"✅ Тестовые данные созданы: {len(df)} записей")
                return True
            else:
                logger.error("❌ Не удалось сгенерировать данные")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания тестовых данных: {e}")
            return False
    
    def create_full_data(self):
        """Создает полный набор данных за 6 месяцев"""
        try:
            logger.info("🔧 Создаю полные данные за 6 месяцев")
            
            # Создаем таблицы
            self.create_backtest_tables()
            
            # Генерируем данные
            df = self.generate_synthetic_data()
            
            if not df.empty:
                # Сохраняем в БД
                self.save_to_database(df)
                
                # Сохраняем CSV для проверки
                df.to_csv('data/backtests/full_data.csv', index=False)
                
                logger.info(f"✅ Полные данные созданы: {len(df)} записей")
                return True
            else:
                logger.error("❌ Не удалось сгенерировать данные")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания полных данных: {e}")
            return False

def main():
    """Основная функция"""
    generator = BacktestDataGenerator()
    
    # Создаем тестовые данные (1 неделя)
    success = generator.create_sample_data()
    
    if success:
        logger.info("✅ Тестовые данные готовы для backtesting")
    else:
        logger.error("❌ Не удалось создать тестовые данные")

if __name__ == "__main__":
    main()
