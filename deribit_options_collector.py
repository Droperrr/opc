#!/usr/bin/env python3
"""
Сборщик исторических данных опционов с публичного API Deribit
Поддерживает получение списка опционных контрактов и исторических свечей
"""

import requests
import sqlite3
import pandas as pd
import logging
import argparse
from datetime import datetime, timedelta
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deribit_data_collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DeribitOptionsCollector:
    def __init__(self, db_path: str = 'server_opc.db'):
        self.db_path = db_path
        self.base_url = 'https://www.deribit.com/api/v2'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json'
        })
        
        # Ограничения API
        self.RATE_LIMIT_DELAY = 0.1  # 100ms между запросами
        
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных с таблицей для данных Deribit"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица для исторических свечей опционов Deribit
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS deribit_options_kline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    instrument_name TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    cost REAL NOT NULL,
                    ticks INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, instrument_name)
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deribit_timestamp ON deribit_options_kline(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deribit_instrument ON deribit_options_kline(instrument_name)')
            
            conn.commit()
            conn.close()
            logger.info("✅ База данных для данных Deribit инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    def get_instruments(self, currency: str = 'BTC', kind: str = 'option'):
        """
        Получение списка инструментов с Deribit API
        
        Args:
            currency: Валюта (BTC, ETH, SOL и т.д.)
            kind: Тип инструмента (option, future, spot)
            
        Returns:
            list: Список инструментов
        """
        try:
            url = f"{self.base_url}/public/get_instruments"
            
            params = {
                'currency': currency,
                'kind': kind
            }
            
            logger.info(f"📡 Запрос списка инструментов: {currency} {kind}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'result' in data and data['result']:
                instruments = data['result']
                logger.info(f"✅ Получено {len(instruments)} инструментов")
                return instruments
            else:
                logger.error(f"❌ Ошибка получения инструментов: {data}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса инструментов: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении инструментов: {e}")
            return []
    
    def get_tradingview_chart_data(self, instrument_name: str, start_timestamp: int, 
                                 end_timestamp: int, resolution: str = '1D'):
        """
        Получение исторических свечей с Deribit API
        
        Args:
            instrument_name: Название инструмента
            start_timestamp: Начальное время (в миллисекундах)
            end_timestamp: Конечное время (в миллисекундах)
            resolution: Разрешение свечей (1, 3, 5, 10, 15, 30, 60, 120, 180, 360, 720, 1D)
            
        Returns:
            dict: Данные свечей
        """
        try:
            url = f"{self.base_url}/public/get_tradingview_chart_data"
            
            params = {
                'instrument_name': instrument_name,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp,
                'resolution': resolution
            }
            
            logger.info(f"📡 Запрос свечей: {instrument_name} {resolution} {start_timestamp} - {end_timestamp}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'result' in data and data['result']:
                result = data['result']
                if 'status' in result and result['status'] == 'ok':
                    logger.info(f"✅ Получено данные свечей для {instrument_name}")
                    return result
                else:
                    logger.error(f"❌ Ошибка в данных свечей: {result}")
                    return None
            else:
                logger.error(f"❌ Ошибка получения свечей: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка запроса свечей: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении свечей: {e}")
            return None
    
    def save_kline_data(self, instrument_name: str, chart_data: dict):
        """
        Сохранение данных свечей в базу данных
        
        Args:
            instrument_name: Название инструмента
            chart_data: Данные свечей
            
        Returns:
            int: Количество сохраненных записей
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Извлекаем данные свечей
            ticks = chart_data.get('ticks', [])
            opens = chart_data.get('open', [])
            highs = chart_data.get('high', [])
            lows = chart_data.get('low', [])
            closes = chart_data.get('close', [])
            volumes = chart_data.get('volume', [])
            costs = chart_data.get('cost', [])
            
            saved_count = 0
            
            # Сохраняем каждую свечу
            for i in range(len(ticks)):
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO deribit_options_kline 
                        (timestamp, instrument_name, open, high, low, close, volume, cost, ticks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ticks[i],
                        instrument_name,
                        opens[i] if i < len(opens) else 0,
                        highs[i] if i < len(highs) else 0,
                        lows[i] if i < len(lows) else 0,
                        closes[i] if i < len(closes) else 0,
                        volumes[i] if i < len(volumes) else 0,
                        costs[i] if i < len(costs) else 0,
                        ticks[i]
                    ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сохранения свечи {i}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Сохранено {saved_count} свечей для {instrument_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения свечей: {e}")
            return 0
    
    def collect_data_for_instrument(self, instrument_name: str, start_date: str, end_date: str):
        """
        Сбор данных для конкретного инструмента
        
        Args:
            instrument_name: Название инструмента
            start_date: Начальная дата в формате YYYY-MM-DD
            end_date: Конечная дата в формате YYYY-MM-DD
            
        Returns:
            bool: Успешность выполнения
        """
        try:
            # Конвертируем даты в timestamp
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            start_timestamp = int(start_dt.timestamp() * 1000)
            end_timestamp = int(end_dt.timestamp() * 1000)
            
            # Получаем свечи
            chart_data = self.get_tradingview_chart_data(
                instrument_name, start_timestamp, end_timestamp, '1D'
            )
            
            if chart_data:
                # Сохраняем данные
                saved_count = self.save_kline_data(instrument_name, chart_data)
                return saved_count > 0
            else:
                logger.error(f"❌ Не удалось получить данные свечей для {instrument_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка сбора данных для {instrument_name}: {e}")
            return False
    
    def collect_sample_data(self, currency: str = 'BTC', start_date: str = '2023-01-01',
                          end_date: str = '2023-01-01'):
        """
        Сбор тестовых данных для одного инструмента (опцион или фьючерс)
        
        Args:
            currency: Валюта (BTC, ETH и т.д.)
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            bool: Успешность выполнения
        """
        try:
            logger.info(f"🚀 Начинаю сбор тестовых данных для {currency}")
            logger.info(f"📅 Период: {start_date} - {end_date}")
            
            # Получаем список инструментов (сначала пробуем опционы, потом фьючерсы)
            instruments = self.get_instruments(currency, 'option')
            instrument_kind = 'option'
            
            # Проверяем, есть ли данные для опционов, если нет - пробуем фьючерсы
            if instruments:
                # Проверяем, есть ли данные для первого активного опциона
                active_instruments = [inst for inst in instruments if inst.get('is_active', False)]
                if active_instruments:
                    # Проверяем данные для первого опциона
                    test_instrument = active_instruments[0]
                    test_name = test_instrument.get('instrument_name')
                    # Пробуем получить данные для тестовой даты
                    test_start = int(datetime.strptime('2025-09-01', '%Y-%m-%d').timestamp() * 1000)
                    test_end = int(datetime.strptime('2025-09-01', '%Y-%m-%d').timestamp() * 1000)
                    test_data = self.get_tradingview_chart_data(test_name, test_start, test_end, '1D')
                    if not test_data or test_data.get('status') != 'ok':
                        logger.info(" Для опционов нет данных, пробуем фьючерсы")
                        instruments = self.get_instruments(currency, 'future')
                        instrument_kind = 'future'
                else:
                    logger.info(" Нет активных опционов, пробуем фьючерсы")
                    instruments = self.get_instruments(currency, 'future')
                    instrument_kind = 'future'
            else:
                logger.info(" опционов не найдено, пробуем фьючерсы")
                instruments = self.get_instruments(currency, 'future')
                instrument_kind = 'future'
            
            if not instruments:
                logger.error("❌ Не удалось получить список инструментов")
                return False
            
            # Выбираем активный инструмент с ближайшей датой экспирации
            selected_instrument = None
            active_instruments = [inst for inst in instruments if inst.get('is_active', False)]
            
            if active_instruments:
                # Для фьючерсов и опционов разная логика выбора
                if instrument_kind == 'future':
                    # Для фьючерсов ищем BTC-PERPETUAL в первую очередь
                    perpetual = next((inst for inst in active_instruments if inst.get('instrument_name') == f'{currency}-PERPETUAL'), None)
                    if perpetual:
                        selected_instrument = perpetual
                        logger.info(f"🎯 Выбран PERPETUAL фьючерс: {selected_instrument.get('instrument_name')}")
                    else:
                        # Если нет PERPETUAL, берем первый активный
                        selected_instrument = active_instruments[0]
                        logger.info(f"🎯 Выбран фьючерс: {selected_instrument.get('instrument_name')}")
                else:
                    # Для опционов сортируем по дате экспирации
                    try:
                        # Извлекаем дату из имени инструмента
                        def extract_expiry_date(instrument):
                            name = instrument.get('instrument_name', '')
                            # Ищем дату в формате DDMMMYY
                            import re
                            match = re.search(r'(\d{1,2}[A-Z]{3}\d{2})', name)
                            if match:
                                date_str = match.group(1)
                                # Преобразуем в datetime
                                try:
                                    # Пример: 12SEP25 -> 12 SEP 2025
                                    months = {
                                        'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                                        'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                                        'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
                                    }
                                    day = date_str[:2]
                                    month_abbr = date_str[2:5]
                                    year = '20' + date_str[5:]
                                    
                                    if month_abbr in months:
                                        month = months[month_abbr]
                                        date_formatted = f"{year}-{month}-{day}"
                                        return datetime.strptime(date_formatted, '%Y-%m-%d')
                                except Exception as e:
                                    logger.debug(f"Не удалось распарсить дату из {date_str}: {e}")
                                    pass
                            return datetime.max  # Если не удалось распарсить, ставим в конец
                        
                        # Сортируем по дате экспирации
                        active_instruments.sort(key=extract_expiry_date)
                        selected_instrument = active_instruments[0]
                        logger.info(f"🎯 Выбран опцион с ближайшей экспирацией: {selected_instrument.get('instrument_name')}")
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка сортировки опционов по дате: {e}")
                        # Если не удалось отсортировать, берем первый активный
                        selected_instrument = active_instruments[0]
            else:
                # Если нет активных инструментов, берем первый попавшийся
                if instruments:
                    selected_instrument = instruments[0]
                    logger.warning("⚠️ Нет активных инструментов, выбираем первый из списка")
            
            if not selected_instrument:
                logger.error("❌ Не найдено доступных инструментов")
                return False
            
            instrument_name = selected_instrument.get('instrument_name')
            logger.info(f"🎯 Выбран инструмент для теста: {instrument_name} ({instrument_kind})")
            
            # Собираем данные для выбранного инструмента
            success = self.collect_data_for_instrument(instrument_name, start_date, end_date)
            
            if success:
                logger.info(f"🎉 Сбор данных для {instrument_name} завершен успешно")
            else:
                logger.error(f"❌ Сбор данных для {instrument_name} завершен с ошибками")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Ошибка сбора тестовых данных: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Сбор исторических данных опционов с Deribit')
    parser.add_argument('--currency', default='BTC', help='Валюта (BTC, ETH и т.д.)')
    parser.add_argument('--start', default='2023-01-01', help='Дата начала в формате YYYY-MM-DD')
    parser.add_argument('--end', default='2023-01-01', help='Дата окончания в формате YYYY-MM-DD')
    parser.add_argument('--db', default='server_opc.db', help='Путь к базе данных')
    
    args = parser.parse_args()
    
    # Инициализируем сборщик
    collector = DeribitOptionsCollector(args.db)
    
    # Собираем тестовые данные
    success = collector.collect_sample_data(args.currency, args.start, args.end)
    
    if success:
        logger.info("✅ Сбор данных завершен успешно")
    else:
        logger.error("❌ Сбор данных завершен с ошибками")

if __name__ == "__main__":
    main()