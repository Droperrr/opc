import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from typing import Optional, List
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация API
BASE_URL = 'https://api.bybit.com'
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

def get_funding_rate(symbol: str, start_time: str, end_time: str) -> Optional[List]:
    """
    Получение исторических данных funding rate из API Bybit.
    
    Args:
        symbol (str): Символ фьючерса (например, SOLUSDT)
        start_time (str): Время начала в формате timestamp (мс)
        end_time (str): Время окончания в формате timestamp (мс)
        
    Returns:
        Optional[List]: Список данных о funding rate или None в случае ошибки
    """
    try:
        url = f"{BASE_URL}/v5/market/funding/history"
        
        params = {
            'category': 'linear',
            'symbol': symbol,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 200
        }
        
        logger.info(f"📡 Запрос funding rate: {symbol} {start_time} - {end_time}")
        
        response = SESSION.get(url, params=params, timeout=10)
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

def extract_data():
    """
    Извлекает данные из spot_data и futures_data за последние 6 месяцев.
    """
    # Подключение к базе данных
    conn = sqlite3.connect('server_opc.db')
    
    # Вычисляем дату 6 месяцев назад
    six_months_ago = datetime.now() - timedelta(days=6*30)  # Приблизительно 6 месяцев
    six_months_ago_str = six_months_ago.strftime('%Y-%m-%d %H:%M:%S')
    
    # SQL-запросы для извлечения данных
    spot_query = f"""
    SELECT time, close as spot_price
    FROM spot_data
    WHERE timeframe = '1m' AND time >= '{six_months_ago_str}'
    ORDER BY time
    """
    
    futures_query = f"""
    SELECT time, close as futures_price
    FROM futures_data
    WHERE timeframe = '1m' AND time >= '{six_months_ago_str}'
    ORDER BY time
    """
    
    # Извлечение данных в DataFrame
    spot_df = pd.read_sql_query(spot_query, conn)
    futures_df = pd.read_sql_query(futures_query, conn)
    
    conn.close()
    
    print("Spot Data Info:")
    print(spot_df.info())
    print("\nFutures Data Info:")
    print(futures_df.info())
    
    return spot_df, futures_df

def transform_data(spot_df, futures_df):
    """
    Объединяет данные, вычисляет basis_relative и добавляет funding_rate.
    """
    # Объединение DataFrame по временной метке
    merged_df = pd.merge(spot_df, futures_df, on='time', how='inner')
    
    # Расчет basis_relative
    merged_df['basis_relative'] = (merged_df['futures_price'] - merged_df['spot_price']) / merged_df['spot_price']
    
    # Получение funding_rate
    # Символ фьючерса (жестко задан для примера, в реальном случае может быть параметром)
    symbol = "BTCUSDT"
    
    # Преобразование времени в timestamp
    merged_df['timestamp'] = pd.to_datetime(merged_df['time']).astype(int) // 10**9 * 1000  # В миллисекундах
    
    # Получение уникальных дней для запроса funding rate
    merged_df['date'] = pd.to_datetime(merged_df['time']).dt.date
    unique_dates = merged_df['date'].unique()
    
    funding_rates = {}
    
    for date in unique_dates:
        # Определяем начало и конец дня
        start_dt = pd.Timestamp(date)
        end_dt = start_dt + pd.Timedelta(days=1)
        
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        # Получаем funding rate для дня
        funding_data = get_funding_rate(symbol, str(start_ts), str(end_ts))
        
        if funding_data:
            for item in funding_data:
                # Время в funding data обычно в секундах, преобразуем в миллисекунды для сопоставления
                funding_time = int(float(item['fundingRateTimestamp']) * 1000)
                funding_rate = float(item['fundingRate'])
                funding_rates[funding_time] = funding_rate
        else:
            logger.warning(f"⚠️ Нет данных funding rate для {date}")
        
        # Задержка для соблюдения rate limit
        time.sleep(0.2)
    
    # Сопоставление funding_rate с записями
    def get_funding_for_timestamp(ts):
        # Находим ближайший предыдущий funding rate
        # Так как funding rate обычно публикуются раз в 8 часов,
        # мы ищем последний известный funding rate перед данной минутой
        ts_int = int(ts)
        closest_time = max([t for t in funding_rates.keys() if t <= ts_int], default=None)
        if closest_time is not None:
            return funding_rates[closest_time]
        else:
            # Если нет предыдущего значения, ищем ближайшее следующее
            closest_time = min([t for t in funding_rates.keys() if t > ts_int], default=None)
            return funding_rates.get(closest_time, 0.0)
    
    merged_df['funding_rate'] = merged_df['timestamp'].apply(get_funding_for_timestamp)
    
    # Выбор необходимых колонок
    final_df = merged_df[['time', 'spot_price', 'futures_price', 'basis_relative', 'funding_rate']]
    
    print("\nTransformed Data Head:")
    print(final_df.head())
    
    return final_df

def load_data(df):
    """
    Сохраняет DataFrame в Parquet-файл.
    """
    df.to_parquet('basis_raw_data_1m_btc.parquet', index=False)
    print("\nData loaded to basis_raw_data_1m_btc.parquet")

def main():
    """
    Основная функция для выполнения ETL процесса.
    """
    # Шаг 1: Извлечение
    spot_df, futures_df = extract_data()
    
    # Шаг 2: Трансформация
    transformed_df = transform_data(spot_df, futures_df)
    
    # Шаг 3: Загрузка
    load_data(transformed_df)

if __name__ == "__main__":
    main()