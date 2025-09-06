import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import json
from logger import get_logger
from database import get_database

# Загрузка API-ключа и секрета
load_dotenv()
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')
BASE_URL = 'https://api.bybit.com'  # Основной API URL

# Инициализация логгера
logger = get_logger()

def get_option_symbols(base_coin='SOL', return_all=False):
    """Получить список доступных символов опционов для SOL."""
    logger.info(f"🔍 Поиск опционов для {base_coin}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        params = {'category': 'option', 'baseCoin': base_coin}
        if API_KEY:
            params['api_key'] = API_KEY
            logger.info("🔑 API ключ найден и будет использован")
        else:
            logger.info("⚠️ API ключ не найден, запрос без аутентификации")
        
        logger.info(f"🌐 Отправка запроса к {BASE_URL}/v5/market/instruments-info")
        response = requests.get(f'{BASE_URL}/v5/market/instruments-info', params=params, headers=headers, timeout=10)
        
        # Проверяем статус ответа
        if response.status_code != 200:
            logger.error(f"❌ HTTP {response.status_code}: {response.text}")
            return None
            
        # Проверяем, что ответ не пустой
        if not response.text.strip():
            logger.error("❌ Пустой ответ от API")
            return None
            
        response_data = response.json()
        if 'result' in response_data and 'list' in response_data['result']:
            symbols = [item['symbol'] for item in response_data['result']['list'] if item['status'] == 'Trading']
            logger.info(f"✅ Найдено {len(symbols)} активных символов для {base_coin}")
            if symbols:
                logger.info(f"🎯 Первые 5 символов: {symbols[:5]}")
            return symbols if return_all else (symbols[0] if symbols else None)
        else:
            logger.error(f"❌ Ошибка в ответе API: {json.dumps(response_data)}")
            return None
    except Exception as e:
        logger.error(f"❌ Исключение при получении символов: {e}")
        return None

def get_historical_iv(symbol='SOL-26SEP25-360-P-USDT', start_date='2024-01-01', end_date='2024-12-31'):
    """Собирает текущие данные IV для указанного символа опциона."""
    
    logger.info(f"🚀 Начинаем сбор данных IV для символа: {symbol}")
    
    # Проверяем доступный символ
    if not symbol:
        logger.info("🔍 Символ не указан, ищем доступные символы SOL")
        symbol = get_option_symbols('SOL')
        if not symbol:
            logger.error("❌ Не найдено валидных символов для SOL")
            return None
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        params = {
            'category': 'option',
            'symbol': symbol
        }
        if API_KEY:
            params['api_key'] = API_KEY
            logger.info("🔑 API ключ будет использован для запроса ticker данных")
        
        logger.info(f"🌐 Отправка запроса к {BASE_URL}/v5/market/tickers")
        response = requests.get(f'{BASE_URL}/v5/market/tickers', params=params, headers=headers, timeout=10)
        
        # Проверяем статус ответа
        if response.status_code != 200:
            logger.error(f"❌ HTTP {response.status_code}: {response.text}")
            return None
            
        # Проверяем, что ответ не пустой
        if not response.text.strip():
            logger.error("❌ Пустой ответ от API")
            return None
        
        response_data = response.json()
        
        if 'result' in response_data and 'list' in response_data['result'] and response_data['result']['list']:
            ticker_data = response_data['result']['list'][0]
            logger.info(f"✅ Получены ticker данные для {ticker_data['symbol']}")
            
            # Создаем DataFrame с текущими данными
            current_time = datetime.now()
            data = {
                'time': [current_time],
                'symbol': [ticker_data['symbol']],
                'markIv': [float(ticker_data['markIv']) if ticker_data['markIv'] != '0' else None],
                'bid1Iv': [float(ticker_data['bid1Iv']) if ticker_data['bid1Iv'] != '0' else None],
                'ask1Iv': [float(ticker_data['ask1Iv']) if ticker_data['ask1Iv'] != '0' else None],
                'markPrice': [float(ticker_data['markPrice'])],
                'underlyingPrice': [float(ticker_data['underlyingPrice'])],
                'delta': [float(ticker_data['delta'])],
                'gamma': [float(ticker_data['gamma'])],
                'vega': [float(ticker_data['vega'])],
                'theta': [float(ticker_data['theta'])]
            }
            
            df = pd.DataFrame(data)
            filename = 'sol_option_iv_current.csv'
            df.to_csv(filename, index=False)
            logger.info(f"💾 Данные сохранены в файл: {filename}")
            logger.info(f"📊 Mark IV: {data['markIv'][0]}, Underlying Price: {data['underlyingPrice'][0]}")
            
            return df
        else:
            logger.error(f"❌ Данные ticker не найдены: {json.dumps(response_data)}")
            return None
    
    except Exception as e:
        logger.error(f"❌ Исключение при получении IV данных: {e}")
        return None

def get_all_sol_options_data(base_coin='SOL', max_symbols=None):
    """Собирает данные IV для всех доступных опционов SOL."""
    
    logger.info(f"🚀 Начинаем сбор данных IV для всех опционов {base_coin}")
    
    # Получаем все символы
    all_symbols = get_option_symbols(base_coin, return_all=True)
    if not all_symbols:
        logger.error("❌ Не удалось получить список символов")
        return None
    
    # Ограничиваем количество символов для тестирования
    if max_symbols:
        all_symbols = all_symbols[:max_symbols]
        logger.info(f"🔢 Ограничиваем сбор до {max_symbols} символов для тестирования")
    
    logger.info(f"📊 Будем обрабатывать {len(all_symbols)} символов")
    
    all_data = []
    successful_count = 0
    failed_count = 0
    
    for i, symbol in enumerate(all_symbols, 1):
        logger.info(f"📈 Обрабатываем {i}/{len(all_symbols)}: {symbol}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            params = {
                'category': 'option',
                'symbol': symbol
            }
            if API_KEY:
                params['api_key'] = API_KEY
            
            response = requests.get(f'{BASE_URL}/v5/market/tickers', params=params, headers=headers, timeout=10)
            
            if response.status_code == 200 and response.text.strip():
                response_data = response.json()
                
                if 'result' in response_data and 'list' in response_data['result'] and response_data['result']['list']:
                    ticker_data = response_data['result']['list'][0]
                    
                    # Создаем запись данных
                    current_time = datetime.now()
                    data_row = {
                        'time': current_time,
                        'symbol': ticker_data['symbol'],
                        'markIv': float(ticker_data['markIv']) if ticker_data['markIv'] != '0' else None,
                        'bid1Iv': float(ticker_data['bid1Iv']) if ticker_data['bid1Iv'] != '0' else None,
                        'ask1Iv': float(ticker_data['ask1Iv']) if ticker_data['ask1Iv'] != '0' else None,
                        'markPrice': float(ticker_data['markPrice']),
                        'underlyingPrice': float(ticker_data['underlyingPrice']),
                        'delta': float(ticker_data['delta']),
                        'gamma': float(ticker_data['gamma']),
                        'vega': float(ticker_data['vega']),
                        'theta': float(ticker_data['theta'])
                    }
                    
                    all_data.append(data_row)
                    successful_count += 1
                    logger.info(f"✅ {symbol}: Mark IV = {data_row['markIv']:.4f}")
                else:
                    failed_count += 1
                    logger.warning(f"⚠️ {symbol}: Нет данных ticker")
            else:
                failed_count += 1
                logger.warning(f"⚠️ {symbol}: HTTP {response.status_code}")
                
        except Exception as e:
            failed_count += 1
            logger.error(f"❌ {symbol}: Ошибка - {e}")
        
        # Небольшая пауза между запросами
        time.sleep(0.1)
    
    # Создаем DataFrame из всех собранных данных
    if all_data:
        df = pd.DataFrame(all_data)
        filename = 'sol_all_options_iv.csv'
        
        # Проверяем, существует ли файл, чтобы добавить данные, а не перезаписать
        try:
            existing_df = pd.read_csv(filename)
            df = pd.concat([existing_df, df], ignore_index=True)
            logger.info(f"📝 Добавляем {len(all_data)} новых записей к существующим {len(existing_df)}")
        except FileNotFoundError:
            logger.info(f"📝 Создаем новый файл с {len(all_data)} записями")
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Все данные сохранены в файл: {filename}")
        
        # Сохраняем данные в базу данных
        try:
            db = get_database()
            db.insert_iv_data(all_data)
            logger.info(f"🗄️ Данные также сохранены в базу данных")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в базу данных: {e}")
        
        logger.info(f"📊 Статистика: успешно {successful_count}, неудачно {failed_count}")
        
        return df
    else:
        logger.error("❌ Не удалось собрать данные ни для одного символа")
        return None

def export_iv_data_to_csv(date):
    """
    Экспортирует данные IV за указанную дату в CSV файл
    
    Args:
        date (str): Дата в формате YYYY-MM-DD
        
    Returns:
        str: Путь к созданному файлу или None при ошибке
    """
    from database import export_to_csv
    
    logger.info(f"📤 Экспорт данных IV за {date}")
    
    try:
        export_file = export_to_csv(date)
        if export_file:
            logger.info(f"✅ Экспорт успешен: {export_file}")
            return export_file
        else:
            logger.error(f"❌ Экспорт не удался для даты {date}")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта: {e}")
        return None

# Тест
if __name__ == "__main__":
    logger.info("🎯 Запуск скрипта get_iv.py")
    
    # Тестируем сбор данных для всех опционов (ограничиваем до 5 для демонстрации)
    iv_df = get_all_sol_options_data(max_symbols=5)
    if iv_df is not None:
        logger.info("✅ Скрипт выполнен успешно")
        print("\n📊 Результат (первые 5 строк):")
        print(iv_df.head())
        print(f"\n📈 Всего записей: {len(iv_df)}")
        
        # Показываем статистику базы данных
        db = get_database()
        total_records = db.get_total_records()
        unique_symbols = db.get_unique_symbols()
        print(f"\n🗄️ Статистика базы данных:")
        print(f"📊 Всего записей в БД: {total_records}")
        print(f"🎯 Уникальных символов: {len(unique_symbols)}")
        
        # Тестируем экспорт данных за сегодня
        today = datetime.now().strftime('%Y-%m-%d')
        export_file = export_iv_data_to_csv(today)
        if export_file:
            print(f"📤 Экспорт за {today}: {export_file}")
        
    else:
        logger.error("❌ Скрипт завершился с ошибкой")
        print("❌ Данные не собраны, проверьте логи выше")