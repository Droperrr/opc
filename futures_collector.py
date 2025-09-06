#!/usr/bin/env python3
"""
Сборщик исторических данных для SOL/USDT
Поддерживает все таймфреймы: 1m, 5m, 15m, 1h, 4h, 1d
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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection_log.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        self.db_path = db_path
        self.base_url = 'https://api.bybit.com'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Поддерживаемые таймфреймы
        self.SUPPORTED_TIMEFRAMES = {
            '1': '1m',
            '5': '5m', 
            '15': '15m',
            '60': '1h',
            '240': '4h',
            '1440': '1d'
        }
        
        # Ограничения API
        self.RATE_LIMIT_DELAY = 0.1  # 100ms между запросами
        self.MAX_RETRIES = 3
        self.BACKOFF_FACTOR = 2
        
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных с таблицами для всех таймфреймов"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица для спотовых данных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS spot_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, timeframe)
                )
            ''')
            
            # Таблица для фьючерсных данных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS futures_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, timeframe)
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_spot_time ON spot_data(time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_spot_timeframe ON spot_data(timeframe)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_futures_time ON futures_data(time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_futures_timeframe ON futures_data(timeframe)')
            
            conn.commit()
            conn.close()
            logger.info("✅ База данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    def get_kline_data(self, market_type: str, symbol: str, interval: str, 
                      start_time: str, end_time: str, limit: int = 1000) -> Optional[List]:
        """
        Получение данных свечей от Bybit API
        """
        try:
            url = f"{self.base_url}/v5/market/kline"
            
            params = {
                'category': market_type,
                'symbol': symbol,
                'interval': interval,
                'start': start_time,
                'end': end_time,
                'limit': limit
            }
            
            logger.info(f"📡 Запрос: {market_type} {symbol} {interval} {start_time} - {end_time}")
            
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
    
    def save_progress(self, current_date: str, market_type: str, timeframe: str):
        """Сохранение прогресса сбора данных"""
        try:
            with open('data_collection_progress.txt', 'a') as f:
                f.write(f"{datetime.now().isoformat()}: {market_type} {timeframe} {current_date}\n")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось сохранить прогресс: {e}")
    
    def collect_data_for_timeframe(self, market_type: str, symbol: str, 
                                  timeframe: str, start_date: str, end_date: str) -> bool:
        """
        Сбор данных для конкретного таймфрейма
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Определяем таблицу
            table_name = 'spot_data' if market_type == 'spot' else 'futures_data'
            
            # Конвертируем даты в timestamp
            start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
            end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
            
            current_ts = start_ts
            total_records = 0
            
            logger.info(f"🚀 Начинаю сбор {market_type} {symbol} {timeframe} с {start_date} по {end_date}")
            
            while current_ts < end_ts:
                # Вычисляем следующий timestamp
                if timeframe == '1':
                    next_ts = current_ts + (1000 * 60 * 1000)  # +1000 минут
                elif timeframe == '5':
                    next_ts = current_ts + (1000 * 5 * 1000)   # +1000 * 5 минут
                elif timeframe == '15':
                    next_ts = current_ts + (1000 * 15 * 1000)  # +1000 * 15 минут
                elif timeframe == '60':
                    next_ts = current_ts + (1000 * 60 * 1000)  # +1000 часов
                elif timeframe == '240':
                    next_ts = current_ts + (1000 * 4 * 60 * 1000)  # +1000 * 4 часов
                elif timeframe == '1440':
                    next_ts = current_ts + (1000 * 24 * 60 * 1000)  # +1000 дней
                else:
                    next_ts = current_ts + (1000 * 60 * 1000)  # по умолчанию
                
                # Ограничиваем до end_ts
                if next_ts > end_ts:
                    next_ts = end_ts
                
                # Получаем данные
                kline_data = self.get_kline_data(
                    market_type, symbol, timeframe,
                    str(current_ts), str(next_ts)
                )
                
                if kline_data:
                    # Сохраняем данные
                    for candle in kline_data:
                        try:
                            # Bybit возвращает: [timestamp, open, high, low, close, volume, ...]
                            timestamp = int(candle[0])
                            time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                            
                            cursor.execute(f'''
                                INSERT OR REPLACE INTO {table_name} 
                                (time, timeframe, open, high, low, close, volume)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                time_str,
                                self.SUPPORTED_TIMEFRAMES[timeframe],
                                float(candle[1]),
                                float(candle[2]),
                                float(candle[3]),
                                float(candle[4]),
                                float(candle[5])
                            ))
                            
                            total_records += 1
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Ошибка сохранения свечи: {e}")
                            continue
                    
                    conn.commit()
                    
                    # Сохраняем прогресс
                    current_date_str = datetime.fromtimestamp(current_ts / 1000).strftime('%Y-%m-%d')
                    self.save_progress(current_date_str, market_type, timeframe)
                    
                    logger.info(f"✅ Сохранено {len(kline_data)} записей для {current_date_str}")
                
                # Следующий период
                current_ts = next_ts
                
                # Задержка для соблюдения rate limit
                time.sleep(self.RATE_LIMIT_DELAY)
            
            conn.close()
            logger.info(f"🎉 Завершен сбор {market_type} {timeframe}: {total_records} записей")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сбора данных для {timeframe}: {e}")
            return False
    
    def collect_all_timeframes(self, market_type: str, symbol: str, 
                             timeframes: List[str], start_date: str, end_date: str):
        """
        Сбор данных для всех указанных таймфреймов
        """
        logger.info(f"🚀 Начинаю сбор данных для {market_type} {symbol}")
        logger.info(f"📅 Период: {start_date} - {end_date}")
        logger.info(f"⏰ Таймфреймы: {timeframes}")
        
        results = {}
        
        for timeframe in timeframes:
            if timeframe not in self.SUPPORTED_TIMEFRAMES:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue
            
            success = self.collect_data_for_timeframe(
                market_type, symbol, timeframe, start_date, end_date
            )
            
            results[timeframe] = success
            
            if success:
                logger.info(f"✅ {timeframe} - успешно")
            else:
                logger.error(f"❌ {timeframe} - ошибка")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Сбор исторических данных SOL/USDT')
    parser.add_argument('--market', choices=['spot', 'linear'], required=True,
                       help='Тип рынка: spot или linear (фьючерсы)')
    parser.add_argument('--pair', required=True,
                       help='Торговая пара (например, SOL/USDT или SOLUSDT)')
    parser.add_argument('--tf', nargs='+', default=['1', '5', '15', '60', '240', '1440'],
                       help='Таймфреймы для сбора')
    parser.add_argument('--start', required=True,
                       help='Дата начала в формате YYYY-MM-DD')
    parser.add_argument('--end', required=True,
                       help='Дата окончания в формате YYYY-MM-DD')
    parser.add_argument('--db', default='data/sol_iv.db',
                       help='Путь к базе данных')
    
    args = parser.parse_args()
    
    # Создаем папку data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Инициализируем сборщик
    collector = DataCollector(args.db)
    
    # Нормализуем символ
    symbol = args.pair.replace('/', '') if '/' in args.pair else args.pair
    
    # Запускаем сбор
    results = collector.collect_all_timeframes(
        args.market, symbol, args.tf, args.start, args.end
    )
    
    # Выводим результаты
    logger.info("📊 Результаты сбора:")
    for tf, success in results.items():
        status = "✅" if success else "❌"
        logger.info(f"{status} {tf} ({collector.SUPPORTED_TIMEFRAMES[tf]})")

if __name__ == "__main__":
    main()
