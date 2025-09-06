#!/usr/bin/env python3
"""
Модуль обогащения данных опционов SOL (IV + Skew + OI)
"""

import sqlite3
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from logger import get_logger
from get_iv import get_option_symbols, get_historical_iv

logger = get_logger()

class OptionsEnricher:
    """Обогатитель данных опционов с IV, Skew и OI"""
    
    def __init__(self, db_path='data/options_enriched.db'):
        """Инициализация обогатителя"""
        self.db_path = db_path
        self.base_url = 'https://api.bybit.com'
        self.init_database()
        
    def init_database(self):
        """Инициализирует базу данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицу для обогащенных данных
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS options_enriched (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strike REAL,
                    expiry TEXT,
                    mark_iv REAL,
                    skew REAL,
                    open_interest REAL,
                    underlying_price REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    theta REAL,
                    option_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                cursor.execute(create_table_sql)
                
                # Создаем индексы для быстрого поиска
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON options_enriched(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON options_enriched(symbol)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_expiry ON options_enriched(expiry)")
                
                conn.commit()
                logger.info(f"🗄️ База данных инициализирована: {self.db_path}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            raise
    
    def parse_symbol_info(self, symbol):
        """Парсит информацию из символа опциона"""
        try:
            # Формат: SOL-26SEP25-360-C-USDT
            parts = symbol.split('-')
            if len(parts) >= 4:
                base_coin = parts[0]  # SOL
                expiry = parts[1]     # 26SEP25
                strike = float(parts[2])  # 360
                option_type = parts[3]    # C или P
                
                return {
                    'base_coin': base_coin,
                    'expiry': expiry,
                    'strike': strike,
                    'option_type': option_type
                }
        except Exception as e:
            logger.warning(f"⚠️ Не удалось распарсить символ {symbol}: {e}")
        
        return None
    
    def get_open_interest(self, symbol):
        """Получает Open Interest для символа"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            params = {
                'category': 'option',
                'symbol': symbol
            }
            
            response = requests.get(f'{self.base_url}/v5/market/open-interest', 
                                 params=params, headers=headers, timeout=10)
            
            if response.status_code == 200 and response.text.strip():
                data = response.json()
                
                if 'result' in data and 'list' in data['result'] and data['result']['list']:
                    oi_data = data['result']['list'][0]
                    return float(oi_data.get('openInterest', 0))
            
            logger.warning(f"⚠️ Не удалось получить OI для {symbol}")
            return 0.0
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения OI для {symbol}: {e}")
            return 0.0
    
    def calculate_skew(self, call_iv, put_iv):
        """Рассчитывает Skew между call и put IV"""
        if call_iv is None or put_iv is None or call_iv == 0 or put_iv == 0:
            return 0.0
        
        total_iv = call_iv + put_iv
        if total_iv == 0:
            return 0.0
        
        skew = (call_iv - put_iv) / total_iv
        return round(skew, 4)
    
    def collect_enriched_data(self, max_symbols=None):
        """Собирает обогащенные данные опционов"""
        logger.info("🚀 Начинаем сбор обогащенных данных опционов")
        
        # Получаем список всех символов
        all_symbols = get_option_symbols('SOL', return_all=True)
        if not all_symbols:
            logger.error("❌ Не удалось получить список символов")
            return None
        
        if max_symbols:
            all_symbols = all_symbols[:max_symbols]
            logger.info(f"🔢 Ограничиваем сбор до {max_symbols} символов")
        
        logger.info(f"📊 Будем обрабатывать {len(all_symbols)} символов")
        
        enriched_data = []
        successful_count = 0
        failed_count = 0
        
        for i, symbol in enumerate(all_symbols, 1):
            logger.info(f"📈 Обрабатываем {i}/{len(all_symbols)}: {symbol}")
            
            try:
                # Парсим информацию о символе
                symbol_info = self.parse_symbol_info(symbol)
                if not symbol_info:
                    failed_count += 1
                    continue
                
                # Получаем IV данные напрямую
                try:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json'
                    }
                    params = {
                        'category': 'option',
                        'symbol': symbol
                    }
                    
                    response = requests.get(f'{self.base_url}/v5/market/tickers', 
                                         params=params, headers=headers, timeout=10)
                    
                    if response.status_code == 200 and response.text.strip():
                        data = response.json()
                        
                        if 'result' in data and 'list' in data['result'] and data['result']['list']:
                            iv_data = data['result']['list'][0]
                        else:
                            failed_count += 1
                            continue
                    else:
                        failed_count += 1
                        continue
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка получения IV для {symbol}: {e}")
                    failed_count += 1
                    continue
                
                # Получаем OI данные
                open_interest = self.get_open_interest(symbol)
                
                # Создаем запись данных
                current_time = datetime.now()
                data_row = {
                    'timestamp': current_time.isoformat(),
                    'symbol': symbol,
                    'strike': symbol_info['strike'],
                    'expiry': symbol_info['expiry'],
                    'mark_iv': float(iv_data['markIv']) if iv_data['markIv'] != '0' else None,
                    'skew': 0.0,  # Будет рассчитан позже
                    'open_interest': open_interest,
                    'underlying_price': float(iv_data['underlyingPrice']),
                    'delta': float(iv_data['delta']),
                    'gamma': float(iv_data['gamma']),
                    'vega': float(iv_data['vega']),
                    'theta': float(iv_data['theta']),
                    'option_type': symbol_info['option_type']
                }
                
                enriched_data.append(data_row)
                successful_count += 1
                
                logger.info(f"✅ {symbol}: IV={data_row['mark_iv']:.4f}, OI={open_interest}")
                
                # Небольшая пауза между запросами
                time.sleep(0.1)
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ {symbol}: Ошибка - {e}")
        
        logger.info(f"📊 Статистика: успешно {successful_count}, неудачно {failed_count}")
        
        if enriched_data:
            # Рассчитываем Skew для пар call/put
            self.calculate_skew_for_pairs(enriched_data)
            
            # Сохраняем данные
            self.save_enriched_data(enriched_data)
            
            return enriched_data
        else:
            logger.error("❌ Не удалось собрать данные")
            return None
    
    def calculate_skew_for_pairs(self, data):
        """Рассчитывает Skew для пар call/put опционов"""
        logger.info("📊 Расчет Skew для пар опционов")
        
        # Группируем данные по страйку и экспирации
        df = pd.DataFrame(data)
        df['strike_expiry'] = df['strike'].astype(str) + '_' + df['expiry']
        
        # Находим пары call/put
        for group_key in df['strike_expiry'].unique():
            group_data = df[df['strike_expiry'] == group_key]
            
            if len(group_data) == 2:  # Должна быть пара call/put
                call_data = group_data[group_data['option_type'] == 'C']
                put_data = group_data[group_data['option_type'] == 'P']
                
                if len(call_data) == 1 and len(put_data) == 1:
                    call_iv = call_data.iloc[0]['mark_iv']
                    put_iv = put_data.iloc[0]['mark_iv']
                    
                    if call_iv is not None and put_iv is not None:
                        skew = self.calculate_skew(call_iv, put_iv)
                        
                        # Обновляем данные
                        call_idx = call_data.index[0]
                        put_idx = put_data.index[0]
                        
                        df.loc[call_idx, 'skew'] = skew
                        df.loc[put_idx, 'skew'] = skew
        
        # Обновляем исходные данные
        for i, row in df.iterrows():
            data[i]['skew'] = row['skew']
    
    def save_enriched_data(self, data):
        """Сохраняет обогащенные данные в БД и CSV"""
        if not data:
            logger.warning("⚠️ Нет данных для сохранения")
            return
        
        try:
            # Сохраняем в БД
            df = pd.DataFrame(data)
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('options_enriched', conn, if_exists='append', index=False)
            
            # Сохраняем в CSV
            csv_filename = 'options_enriched.csv'
            df.to_csv(csv_filename, index=False)
            
            logger.info(f"💾 Сохранено {len(data)} записей в БД и CSV")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения данных: {e}")
    
    def get_statistics(self):
        """Получает статистику по собранным данным"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Базовые запросы
                queries = {
                    'total_records': "SELECT COUNT(*) FROM options_enriched",
                    'unique_dates': "SELECT COUNT(DISTINCT DATE(timestamp)) FROM options_enriched",
                    'unique_strikes': "SELECT COUNT(DISTINCT strike) FROM options_enriched",
                    'unique_expiries': "SELECT COUNT(DISTINCT expiry) FROM options_enriched",
                    'avg_iv': "SELECT AVG(mark_iv) FROM options_enriched WHERE mark_iv IS NOT NULL",
                    'avg_skew': "SELECT AVG(skew) FROM options_enriched WHERE skew != 0",
                    'avg_oi': "SELECT AVG(open_interest) FROM options_enriched WHERE open_interest > 0"
                }
                
                stats = {}
                for name, query in queries.items():
                    cursor = conn.cursor()
                    cursor.execute(query)
                    result = cursor.fetchone()
                    stats[name] = result[0] if result else 0
                
                # Дополнительная статистика по типам опционов
                type_stats = pd.read_sql_query(
                    "SELECT option_type, COUNT(*), AVG(mark_iv) as avg_iv, AVG(skew) as avg_skew FROM options_enriched GROUP BY option_type",
                    conn
                )
                
                return stats, type_stats
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return None, None
    
    def export_sample_data(self, limit=10):
        """Экспортирует пример данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                SELECT timestamp, symbol, strike, expiry, mark_iv, skew, open_interest, 
                       underlying_price, option_type
                FROM options_enriched 
                ORDER BY timestamp DESC 
                LIMIT {limit}
                """
                
                df = pd.read_sql_query(query, conn)
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта примеров: {e}")
            return pd.DataFrame()

def run_enrichment_demo():
    """Демонстрация работы обогатителя"""
    logger.info("🎯 Демонстрация обогащения данных опционов")
    
    # Создаем обогатитель
    enricher = OptionsEnricher()
    
    # Собираем данные (ограничиваем для демонстрации)
    data = enricher.collect_enriched_data(max_symbols=10)
    
    if data:
        # Получаем статистику
        stats, type_stats = enricher.get_statistics()
        
        if stats:
            print(f"\n📊 Статистика обогащенных данных:")
            print(f"Всего записей: {stats['total_records']}")
            print(f"Уникальных дат: {stats['unique_dates']}")
            print(f"Уникальных страйков: {stats['unique_strikes']}")
            print(f"Уникальных экспираций: {stats['unique_expiries']}")
            print(f"Средняя IV: {stats['avg_iv']:.4f}")
            print(f"Средний Skew: {stats['avg_skew']:.4f}")
            print(f"Средний OI: {stats['avg_oi']:.2f}" if stats['avg_oi'] is not None else "Средний OI: N/A")
        
        if type_stats is not None and not type_stats.empty:
            print(f"\n📈 Статистика по типам опционов:")
            print(type_stats.to_string(index=False))
        
        # Показываем примеры данных
        sample_data = enricher.export_sample_data(10)
        if not sample_data.empty:
            print(f"\n📋 Примеры обогащенных данных:")
            print(sample_data.to_string(index=False))
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация работы обогатителя
    run_enrichment_demo()
