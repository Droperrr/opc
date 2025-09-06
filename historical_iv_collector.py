#!/usr/bin/env python3
"""
Historical IV Data Collector for SOL Options
Collects historical implied volatility data for the 2025 period
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
from scipy.stats import norm
from scipy.optimize import minimize_scalar

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('historical_iv_collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalIVCollector:
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
        """Инициализация базы данных для исторических IV данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица для исторических IV данных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historical_iv_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    mark_iv REAL,
                    bid_iv REAL,
                    ask_iv REAL,
                    mark_price REAL,
                    underlying_price REAL,
                    strike_price REAL,
                    expiry_date TEXT,
                    option_type TEXT,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    theta REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, symbol)
                )
            ''')
            
            # Таблица для агрегированных IV данных (по дням)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS iv_agg_historical (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    spot_price REAL,
                    iv_30d REAL,
                    iv_60d REAL,
                    iv_90d REAL,
                    skew_30d REAL,
                    skew_60d REAL,
                    skew_90d REAL,
                    iv_term_structure REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(time, timeframe)
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_historical_iv_time ON historical_iv_data(time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_historical_iv_symbol ON historical_iv_data(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_iv_agg_historical_time ON iv_agg_historical(time)')
            
            conn.commit()
            conn.close()
            logger.info("✅ База данных для исторических IV данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    def get_option_symbols(self, base_coin='SOL', return_all=False):
        """Получить список доступных символов опционов для SOL."""
        logger.info(f"🔍 Поиск опционов для {base_coin}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            params = {'category': 'option', 'baseCoin': base_coin}
            
            logger.info(f"🌐 Отправка запроса к {self.base_url}/v5/market/instruments-info")
            response = self.session.get(f'{self.base_url}/v5/market/instruments-info', params=params, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"❌ HTTP {response.status_code}: {response.text}")
                return None
                
            response_data = response.json()
            if 'result' in response_data and 'list' in response_data['result']:
                symbols = [item['symbol'] for item in response_data['result']['list'] if item['status'] == 'Trading']
                logger.info(f"✅ Найдено {len(symbols)} активных символов для {base_coin}")
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
        """Получение исторических данных свечей для опционов"""
        try:
            url = f"{self.base_url}/v5/market/kline"
            
            params = {
                'category': 'option',
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
    
    def calculate_implied_volatility(self, option_price: float, spot_price: float, 
                                   strike_price: float, time_to_expiry: float, 
                                   risk_free_rate: float = 0.05, option_type: str = 'call') -> float:
        """Вычисляет подразумеваемую волатильность с помощью Newton-Raphson метода"""
        try:
            def black_scholes(S, K, T, r, sigma, option_type='call'):
                d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
                d2 = d1 - sigma*np.sqrt(T)
                
                if option_type == 'call':
                    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)
                else:
                    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)
            
            def objective(sigma):
                return black_scholes(spot_price, strike_price, time_to_expiry, risk_free_rate, sigma, option_type) - option_price
            
            # Начальное приближение
            sigma_guess = 0.3
            
            # Newton-Raphson итерации
            for _ in range(50):
                try:
                    f = objective(sigma_guess)
                    if abs(f) < 1e-6:
                        break
                    
                    # Производная (вега)
                    d1 = (np.log(spot_price/strike_price) + (risk_free_rate + 0.5*sigma_guess**2)*time_to_expiry) / (sigma_guess*np.sqrt(time_to_expiry))
                    vega = spot_price * np.sqrt(time_to_expiry) * norm.pdf(d1)
                    
                    if abs(vega) < 1e-10:
                        break
                    
                    sigma_guess = sigma_guess - f / vega
                    
                    if sigma_guess <= 0:
                        sigma_guess = 0.01
                    elif sigma_guess > 5:
                        sigma_guess = 5
                        
                except:
                    break
            
            return sigma_guess if sigma_guess > 0 else None
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка вычисления IV: {e}")
            return None
    
    def collect_historical_iv_data(self, start_date: str, end_date: str, max_symbols: int = 50):
        """Сбор исторических IV данных для опционов SOL"""
        try:
            # Получаем список символов опционов
            symbols = self.get_option_symbols('SOL', return_all=True)
            if not symbols:
                logger.error("❌ Не удалось получить список символов опционов")
                return False
            
            # Ограничиваем количество символов
            if max_symbols:
                symbols = symbols[:max_symbols]
                logger.info(f"🔢 Ограничиваем сбор до {max_symbols} символов")
            
            logger.info(f"🚀 Начинаю сбор исторических IV данных для {len(symbols)} символов")
            logger.info(f"📅 Период: {start_date} - {end_date}")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            total_records = 0
            
            for i, symbol in enumerate(symbols, 1):
                logger.info(f"📈 Обрабатываем {i}/{len(symbols)}: {symbol}")
                
                try:
                    # Парсим информацию о символе
                    # Формат: SOL-26SEP25-360-P-USDT
                    parts = symbol.split('-')
                    if len(parts) >= 4:
                        expiry_str = parts[1]  # 26SEP25
                        strike_str = parts[2]  # 360
                        option_type = parts[3]  # P или C
                        
                        # Парсим дату экспирации
                        try:
                            expiry_date = datetime.strptime(expiry_str, '%d%b%y')
                            strike_price = float(strike_str)
                        except:
                            logger.warning(f"⚠️ Не удалось распарсить символ {symbol}")
                            continue
                    else:
                        logger.warning(f"⚠️ Неверный формат символа {symbol}")
                        continue
                    
                    # Конвертируем даты в timestamp
                    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
                    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
                    
                    current_ts = start_ts
                    
                    while current_ts < end_ts:
                        # Вычисляем следующий timestamp (1 день)
                        next_ts = current_ts + (1000 * 24 * 60 * 60 * 1000)  # +1 день
                        if next_ts > end_ts:
                            next_ts = end_ts
                        
                        # Получаем исторические данные
                        kline_data = self.get_historical_kline_data(
                            symbol, 'D', str(current_ts), str(next_ts)
                        )
                        
                        if kline_data:
                            for candle in kline_data:
                                try:
                                    timestamp = int(candle[0])
                                    time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                                    close_price = float(candle[4])
                                    
                                    # Вычисляем время до экспирации
                                    current_date = datetime.fromtimestamp(timestamp / 1000)
                                    time_to_expiry = (expiry_date - current_date).days / 365.0
                                    
                                    if time_to_expiry <= 0:
                                        continue
                                    
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
                                    
                                    # Вычисляем IV
                                    iv = self.calculate_implied_volatility(
                                        close_price, spot_price, strike_price, 
                                        time_to_expiry, option_type=option_type
                                    )
                                    
                                    if iv is not None:
                                        # Сохраняем данные
                                        cursor.execute('''
                                            INSERT OR REPLACE INTO historical_iv_data 
                                            (time, symbol, mark_iv, mark_price, underlying_price, 
                                             strike_price, expiry_date, option_type)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                        ''', (
                                            time_str, symbol, iv, close_price, spot_price,
                                            strike_price, expiry_date.strftime('%Y-%m-%d'), option_type
                                        ))
                                        
                                        total_records += 1
                                        
                                except Exception as e:
                                    logger.warning(f"⚠️ Ошибка обработки свечи: {e}")
                                    continue
                            
                            conn.commit()
                            logger.info(f"✅ Сохранено {len(kline_data)} записей для {symbol}")
                        
                        # Следующий период
                        current_ts = next_ts
                        
                        # Задержка для соблюдения rate limit
                        time.sleep(self.RATE_LIMIT_DELAY)
                
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки символа {symbol}: {e}")
                    continue
            
            conn.close()
            logger.info(f"🎉 Завершен сбор исторических IV данных: {total_records} записей")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сбора исторических IV данных: {e}")
            return False
    
    def aggregate_iv_data(self, start_date: str, end_date: str):
        """Агрегирует исторические IV данные в ежедневные метрики"""
        try:
            logger.info(f"📊 Агрегация IV данных за период {start_date} - {end_date}")
            
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем исторические IV данные
            query = """
            SELECT 
                time,
                mark_iv,
                underlying_price,
                strike_price,
                expiry_date,
                option_type
            FROM historical_iv_data 
            WHERE time BETWEEN ? AND ?
            ORDER BY time
            """
            
            df = pd.read_sql_query(query, conn, params=[start_date, end_date])
            
            if df.empty:
                logger.warning("⚠️ Нет исторических IV данных для агрегации")
                return False
            
            df['time'] = pd.to_datetime(df['time'])
            df['expiry_date'] = pd.to_datetime(df['expiry_date'])
            
            # Вычисляем время до экспирации
            df['time_to_expiry'] = (df['expiry_date'] - df['time']).dt.days / 365.0
            
            # Группируем по дням
            df['date'] = df['time'].dt.date
            
            # Агрегируем данные по дням
            daily_iv = df.groupby('date').agg({
                'underlying_price': 'last',  # Последняя цена дня
                'mark_iv': ['mean', 'std', 'min', 'max'],
                'time_to_expiry': 'mean'
            }).reset_index()
            
            # Переименовываем колонки
            daily_iv.columns = ['date', 'spot_price', 'iv_mean', 'iv_std', 'iv_min', 'iv_max', 'avg_ttm']
            
            # Вычисляем дополнительные метрики
            daily_iv['iv_30d'] = daily_iv['iv_mean']  # Средняя IV
            daily_iv['skew_30d'] = daily_iv['iv_std'] / daily_iv['iv_mean']  # Коэффициент вариации как мера skew
            
            # Сохраняем агрегированные данные
            cursor = conn.cursor()
            
            for _, row in daily_iv.iterrows():
                time_str = row['date'].strftime('%Y-%m-%d %H:%M:%S')
                
                cursor.execute('''
                    INSERT OR REPLACE INTO iv_agg_historical 
                    (time, timeframe, spot_price, iv_30d, skew_30d)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    time_str, '1d', row['spot_price'], row['iv_30d'], row['skew_30d']
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Агрегация завершена: {len(daily_iv)} дней")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка агрегации IV данных: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Сбор исторических IV данных SOL опционов')
    parser.add_argument('--start', required=True,
                       help='Дата начала в формате YYYY-MM-DD')
    parser.add_argument('--end', required=True,
                       help='Дата окончания в формате YYYY-MM-DD')
    parser.add_argument('--max-symbols', type=int, default=50,
                       help='Максимальное количество символов для сбора')
    parser.add_argument('--db', default='data/sol_iv.db',
                       help='Путь к базе данных')
    parser.add_argument('--aggregate-only', action='store_true',
                       help='Только агрегация существующих данных')
    
    args = parser.parse_args()
    
    # Создаем папку data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Инициализируем сборщик
    collector = HistoricalIVCollector(args.db)
    
    if args.aggregate_only:
        # Только агрегация
        success = collector.aggregate_iv_data(args.start, args.end)
    else:
        # Полный сбор данных
        success = collector.collect_historical_iv_data(args.start, args.end, args.max_symbols)
        if success:
            success = collector.aggregate_iv_data(args.start, args.end)
    
    if success:
        logger.info("✅ Сбор исторических IV данных завершен успешно")
    else:
        logger.error("❌ Сбор исторических IV данных завершен с ошибками")

if __name__ == "__main__":
    main()
