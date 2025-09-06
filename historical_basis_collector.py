#!/usr/bin/env python3
"""
Historical Basis Data Collector for SOL Futures
Collects historical basis data (futures-spot spread) for the 2025 period
"""

import requests
import sqlite3
import pandas as pd
import time
import logging
import argparse
from datetime import datetime, timedelta
import os
from typing import List, Dict, Optional
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('historical_basis_collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalBasisCollector:
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        self.db_path = db_path
        self.base_url = 'https://api.bybit.com'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Ограничения API
        self.RATE_LIMIT_DELAY = 0.2  # 200ms между запросами
        self.MAX_RETRIES = 3
        self.BACKOFF_FACTOR = 2
        
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных для исторических basis данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица для исторических basis данных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historical_basis_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    futures_price REAL,
                    spot_price REAL,
                    basis_abs REAL,
                    basis_rel REAL,
                    funding_rate REAL,
                    open_interest REAL,
                    volume_24h REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, symbol)
                )
            ''')
            
            # Таблица для агрегированных basis данных (по дням)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS basis_agg_historical (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    basis_rel REAL,
                    basis_abs REAL,
                    funding_rate REAL,
                    oi_total REAL,
                    volume_total REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, timeframe)
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_historical_basis_time ON historical_basis_data(time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_historical_basis_symbol ON historical_basis_data(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_basis_agg_historical_time ON basis_agg_historical(time)')
            
            conn.commit()
            conn.close()
            logger.info("✅ База данных для исторических basis данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    def get_futures_symbols(self, base_coin='SOL', return_all=False):
        """Получить список доступных символов фьючерсов для SOL."""
        logger.info(f"🔍 Поиск фьючерсов для {base_coin}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            params = {'category': 'linear', 'baseCoin': base_coin}
            
            logger.info(f"🌐 Отправка запроса к {self.base_url}/v5/market/instruments-info")
            response = self.session.get(f'{self.base_url}/v5/market/instruments-info', params=params, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"❌ HTTP {response.status_code}: {response.text}")
                return None
                
            response_data = response.json()
            if 'result' in response_data and 'list' in response_data['result']:
                symbols = [item['symbol'] for item in response_data['result']['list'] if item['status'] == 'Trading']
                logger.info(f"✅ Найдено {len(symbols)} активных фьючерсов для {base_coin}")
                if symbols:
                    logger.info(f"🎯 Первые 5 символов: {symbols[:5]}")
                return symbols if return_all else (symbols[0] if symbols else None)
            else:
                logger.error(f"❌ Ошибка в ответе API: {response_data}")
                return None
        except Exception as e:
            logger.error(f"❌ Исключение при получении символов: {e}")
            return None
    
    def get_historical_kline_data(self, symbol: str, interval: str, 
                                 start_time: str, end_time: str, limit: int = 1000) -> Optional[List]:
        """Получение исторических данных свечей для фьючерсов"""
        try:
            url = f"{self.base_url}/v5/market/kline"
            
            params = {
                'category': 'linear',
                'symbol': symbol,
                'interval': interval,
                'start': start_time,
                'end': end_time,
                'limit': limit
            }
            
            logger.info(f"📡 Запрос исторических данных: {symbol} {interval} {start_time} - {end_time}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data['retCode'] != 0:
                logger.error(f"❌ API ошибка: {data['retMsg']}")
                return None
            
            return data['result']['list']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            return None
    
    def get_funding_rate(self, symbol: str, start_time: str, end_time: str) -> Optional[List]:
        """Получение исторических данных funding rate"""
        try:
            url = f"{self.base_url}/v5/market/funding/history"
            
            params = {
                'category': 'linear',
                'symbol': symbol,
                'startTime': start_time,
                'endTime': end_time,
                'limit': 200
            }
            
            logger.info(f"📡 Запрос funding rate: {symbol} {start_time} - {end_time}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data['retCode'] != 0:
                logger.error(f"❌ API ошибка: {data['retMsg']}")
                return None
            
            return data['result']['list']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса funding rate: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка funding rate: {e}")
            return None
    
    def get_open_interest(self, symbol: str, start_time: str, end_time: str) -> Optional[List]:
        """Получение исторических данных open interest"""
        try:
            url = f"{self.base_url}/v5/market/open-interest"
            
            params = {
                'category': 'linear',
                'symbol': symbol,
                'interval': '1d',
                'startTime': start_time,
                'endTime': end_time,
                'limit': 200
            }
            
            logger.info(f"📡 Запрос open interest: {symbol} {start_time} - {end_time}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data['retCode'] != 0:
                logger.error(f"❌ API ошибка: {data['retMsg']}")
                return None
            
            return data['result']['list']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса open interest: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка open interest: {e}")
            return None
    
    def collect_historical_basis_data(self, start_date: str, end_date: str, max_symbols: int = 20):
        """Сбор исторических basis данных для фьючерсов SOL"""
        try:
            # Получаем список символов фьючерсов
            symbols = self.get_futures_symbols('SOL', return_all=True)
            if not symbols:
                logger.error("❌ Не удалось получить список символов фьючерсов")
                return False
            
            # Ограничиваем количество символов
            if max_symbols:
                symbols = symbols[:max_symbols]
                logger.info(f"🔢 Ограничиваем сбор до {max_symbols} символов")
            
            logger.info(f"🚀 Начинаю сбор исторических basis данных для {len(symbols)} символов")
            logger.info(f"📅 Период: {start_date} - {end_date}")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            total_records = 0
            
            for i, symbol in enumerate(symbols, 1):
                logger.info(f"📈 Обрабатываем {i}/{len(symbols)}: {symbol}")
                
                try:
                    # Конвертируем даты в timestamp
                    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
                    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
                    
                    current_ts = start_ts
                    
                    while current_ts < end_ts:
                        # Вычисляем следующий timestamp (1 день)
                        next_ts = current_ts + (1000 * 24 * 60 * 60 * 1000)  # +1 день
                        if next_ts > end_ts:
                            next_ts = end_ts
                        
                        # Получаем исторические данные фьючерсов
                        futures_kline_data = self.get_historical_kline_data(
                            symbol, 'D', str(current_ts), str(next_ts)
                        )
                        
                        if futures_kline_data:
                            for candle in futures_kline_data:
                                try:
                                    timestamp = int(candle[0])
                                    time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                                    futures_price = float(candle[4])  # close price
                                    volume_24h = float(candle[5])
                                    
                                    # Получаем спотовую цену для этого времени
                                    spot_query = """
                                    SELECT close FROM spot_data 
                                    WHERE time <= ? AND timeframe = '1m'
                                    ORDER BY time DESC LIMIT 1
                                    """
                                    cursor.execute(spot_query, [time_str])
                                    spot_result = cursor.fetchone()
                                    
                                    if not spot_result:
                                        continue
                                    
                                    spot_price = float(spot_result[0])
                                    
                                    # Вычисляем basis
                                    basis_abs = futures_price - spot_price
                                    basis_rel = basis_abs / spot_price if spot_price > 0 else 0
                                    
                                    # Получаем funding rate для этого времени
                                    funding_rate = 0.0  # По умолчанию
                                    try:
                                        funding_data = self.get_funding_rate(symbol, str(timestamp), str(timestamp + 86400000))
                                        if funding_data:
                                            funding_rate = float(funding_data[0]['fundingRate']) if funding_data[0]['fundingRate'] != '0' else 0.0
                                    except:
                                        pass
                                    
                                    # Получаем open interest для этого времени
                                    open_interest = 0.0  # По умолчанию
                                    try:
                                        oi_data = self.get_open_interest(symbol, str(timestamp), str(timestamp + 86400000))
                                        if oi_data:
                                            open_interest = float(oi_data[0]['openInterest']) if oi_data[0]['openInterest'] != '0' else 0.0
                                    except:
                                        pass
                                    
                                    # Сохраняем данные
                                    cursor.execute('''
                                        INSERT OR REPLACE INTO historical_basis_data 
                                        (time, symbol, futures_price, spot_price, basis_abs, basis_rel, 
                                         funding_rate, open_interest, volume_24h)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ''', (
                                        time_str, symbol, futures_price, spot_price, basis_abs, basis_rel,
                                        funding_rate, open_interest, volume_24h
                                    ))
                                    
                                    total_records += 1
                                    
                                except Exception as e:
                                    logger.warning(f"⚠️ Ошибка обработки свечи: {e}")
                                    continue
                            
                            conn.commit()
                            logger.info(f"✅ Сохранено {len(futures_kline_data)} записей для {symbol}")
                        
                        # Следующий период
                        current_ts = next_ts
                        
                        # Задержка для соблюдения rate limit
                        time.sleep(self.RATE_LIMIT_DELAY)
                
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки символа {symbol}: {e}")
                    continue
            
            conn.close()
            logger.info(f"🎉 Завершен сбор исторических basis данных: {total_records} записей")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сбора исторических basis данных: {e}")
            return False
    
    def aggregate_basis_data(self, start_date: str, end_date: str):
        """Агрегирует исторические basis данные в ежедневные метрики"""
        try:
            logger.info(f"📊 Агрегация basis данных за период {start_date} - {end_date}")
            
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем исторические basis данные
            query = """
            SELECT 
                time,
                basis_rel,
                basis_abs,
                funding_rate,
                open_interest,
                volume_24h
            FROM historical_basis_data 
            WHERE time BETWEEN ? AND ?
            ORDER BY time
            """
            
            df = pd.read_sql_query(query, conn, params=[start_date, end_date])
            
            if df.empty:
                logger.warning("⚠️ Нет исторических basis данных для агрегации")
                return False
            
            df['time'] = pd.to_datetime(df['time'])
            
            # Группируем по дням
            df['date'] = df['time'].dt.date
            
            # Агрегируем данные по дням
            daily_basis = df.groupby('date').agg({
                'basis_rel': ['mean', 'std', 'min', 'max'],
                'basis_abs': ['mean', 'std', 'min', 'max'],
                'funding_rate': 'mean',
                'open_interest': 'sum',
                'volume_24h': 'sum'
            }).reset_index()
            
            # Переименовываем колонки
            daily_basis.columns = ['date', 'basis_rel_mean', 'basis_rel_std', 'basis_rel_min', 'basis_rel_max',
                                 'basis_abs_mean', 'basis_abs_std', 'basis_abs_min', 'basis_abs_max',
                                 'funding_rate', 'oi_total', 'volume_total']
            
            # Сохраняем агрегированные данные
            cursor = conn.cursor()
            
            for _, row in daily_basis.iterrows():
                time_str = row['date'].strftime('%Y-%m-%d %H:%M:%S')
                
                cursor.execute('''
                    INSERT OR REPLACE INTO basis_agg_historical 
                    (time, timeframe, basis_rel, basis_abs, funding_rate, oi_total, volume_total)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    time_str, '1d', row['basis_rel_mean'], row['basis_abs_mean'], 
                    row['funding_rate'], row['oi_total'], row['volume_total']
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Агрегация завершена: {len(daily_basis)} дней")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка агрегации basis данных: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Сбор исторических basis данных SOL фьючерсов')
    parser.add_argument('--start', required=True,
                       help='Дата начала в формате YYYY-MM-DD')
    parser.add_argument('--end', required=True,
                       help='Дата окончания в формате YYYY-MM-DD')
    parser.add_argument('--max-symbols', type=int, default=20,
                       help='Максимальное количество символов для сбора')
    parser.add_argument('--db', default='data/sol_iv.db',
                       help='Путь к базе данных')
    parser.add_argument('--aggregate-only', action='store_true',
                       help='Только агрегация существующих данных')
    
    args = parser.parse_args()
    
    # Создаем папку data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Инициализируем сборщик
    collector = HistoricalBasisCollector(args.db)
    
    if args.aggregate_only:
        # Только агрегация
        success = collector.aggregate_basis_data(args.start, args.end)
    else:
        # Полный сбор данных
        success = collector.collect_historical_basis_data(args.start, args.end, args.max_symbols)
        if success:
            success = collector.aggregate_basis_data(args.start, args.end)
    
    if success:
        logger.info("✅ Сбор исторических basis данных завершен успешно")
    else:
        logger.error("❌ Сбор исторических basis данных завершен с ошибками")

if __name__ == "__main__":
    main()
