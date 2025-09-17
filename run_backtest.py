import argparse
import sqlite3
import sys
from datetime import datetime
import pandas as pd
import numpy as np
import time

# Импорты из проекта
from basis_analyzer import BasisAnalyzer
from historical_analyzer import HistoricalAnalyzer
from prediction_layer import PredictionLayer
from error_monitor import ErrorMonitor
from block_detector import BlockDetector
from block_analyzer import BlockAnalyzer
from formula_engine_blocks import FormulaEngineBlocks

# Импорт конфигурации
from config_loader import get_config

def prepare_ml_features(data_df, index, window_size=10):
    """
    Подготовка признаков для ML-модели
    
    Args:
        data_df: DataFrame с данными
        index: Текущий индекс
        window_size: Размер окна для анализа
        
    Returns:
        dict: Словарь с признаками
    """
    if index < window_size:
        # Если недостаточно данных, используем доступные
        start_idx = 0
    else:
        start_idx = index - window_size + 1
    
    # Получаем окно данных
    window_data = data_df.iloc[start_idx:index+1]
    
    # Извлекаем признаки
    features = {
        'spot_price': data_df.iloc[index]['spot_price'] if index >= 0 and 'spot_price' in data_df.columns else 0,
        'volume': 1000.0,  # Значение по умолчанию, так как volume отсутствует в данных
        'spot_price_iv': data_df.iloc[index]['spot_price'] if index >= 0 and 'spot_price' in data_df.columns else 0,  # Используем spot_price как замену
        'skew_30d': data_df.iloc[index]['skew'] if index >= 0 and 'skew' in data_df.columns and not pd.isna(data_df.iloc[index]['skew']) else 0,
        'basis_rel': data_df.iloc[index]['basis_rel'] if index >= 0 and 'basis_rel' in data_df.columns and not pd.isna(data_df.iloc[index]['basis_rel']) else 0,
        'confidence': data_df.iloc[index]['confidence'] if index >= 0 and 'confidence' in data_df.columns and not pd.isna(data_df.iloc[index]['confidence']) else 0.5,
        'trend_iv_30d': data_df.iloc[index]['iv_mean_all'] if index >= 0 and 'iv_mean_all' in data_df.columns and not pd.isna(data_df.iloc[index]['iv_mean_all']) else 0,
        'trend_skew_30d': data_df.iloc[index]['skew'] if index >= 0 and 'skew' in data_df.columns and not pd.isna(data_df.iloc[index]['skew']) else 0,
        'spot_ma_7': window_data['spot_price'].rolling(7, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'spot_price' in window_data.columns else 0,
        'spot_ma_30': window_data['spot_price'].rolling(30, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'spot_price' in window_data.columns else 0,
        'iv_ma_7': window_data['iv_mean_all'].rolling(7, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'iv_mean_all' in window_data.columns else 0,
        'iv_ma_30': window_data['iv_mean_all'].rolling(30, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'iv_mean_all' in window_data.columns else 0,
        'spot_volatility_7': window_data['spot_price'].diff().abs().rolling(7, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'spot_price' in window_data.columns else 0,
        'spot_volatility_30': window_data['spot_price'].diff().abs().rolling(30, min_periods=1).mean().iloc[-1] if len(window_data) > 0 and 'spot_price' in window_data.columns else 0,
        'spot_lag_1': data_df.iloc[index-1]['spot_price'] if index >= 1 and 'spot_price' in data_df.columns and not pd.isna(data_df.iloc[index-1]['spot_price']) else 0,
        'spot_lag_2': data_df.iloc[index-2]['spot_price'] if index >= 2 and 'spot_price' in data_df.columns and not pd.isna(data_df.iloc[index-2]['spot_price']) else 0,
        'spot_lag_3': data_df.iloc[index-3]['spot_price'] if index >= 3 and 'spot_price' in data_df.columns and not pd.isna(data_df.iloc[index-3]['spot_price']) else 0,
        'iv_lag_1': data_df.iloc[index-1]['iv_mean_all'] if index >= 1 and 'iv_mean_all' in data_df.columns and not pd.isna(data_df.iloc[index-1]['iv_mean_all']) else 0,
        'iv_lag_2': data_df.iloc[index-2]['iv_mean_all'] if index >= 2 and 'iv_mean_all' in data_df.columns and not pd.isna(data_df.iloc[index-2]['iv_mean_all']) else 0,
        'spot_pct_change': (data_df.iloc[index]['spot_price'] / data_df.iloc[index-1]['spot_price'] - 1) if index >= 1 and 'spot_price' in data_df.columns and data_df.iloc[index-1]['spot_price'] != 0 else 0,
        'iv_pct_change': (data_df.iloc[index]['iv_mean_all'] / data_df.iloc[index-1]['iv_mean_all'] - 1) if index >= 1 and 'iv_mean_all' in data_df.columns and data_df.iloc[index-1]['iv_mean_all'] != 0 and not pd.isna(data_df.iloc[index-1]['iv_mean_all']) and not pd.isna(data_df.iloc[index]['iv_mean_all']) else 0,
        'rsi': 50.0  # Значение по умолчанию, в реальной реализации можно рассчитать
    }
    
    return features

def load_data(symbol, tag, start_date, end_date):
    """
    Загружает исторические данные из базы данных для заданного символа и тега.
    """
    # Загрузка конфигурации
    config = get_config()
    db_path = config.get('database', {}).get('path', 'server_opc.db')
    
    conn = sqlite3.connect(db_path)
    
    # Загрузка данных спота
    spot_query = '''
    SELECT time, close as spot_price FROM spot_data 
    WHERE symbol = ? AND dataset_tag = ? AND time BETWEEN ? AND ?
    ORDER BY time
    '''
    spot_df = pd.read_sql_query(spot_query, conn, params=(symbol, tag, start_date, end_date))
    
    # Загрузка данных фьючерсов
    futures_query = '''
    SELECT time, close as futures_price FROM futures_data 
    WHERE symbol = ? AND dataset_tag = ? AND time BETWEEN ? AND ?
    ORDER BY time
    '''
    futures_df = pd.read_sql_query(futures_query, conn, params=(symbol, tag, start_date, end_date))
    
    # Загрузка данных IV
    iv_query = '''
    SELECT time, iv_30d as iv_mean_all, skew_30d as skew FROM iv_agg
    WHERE timeframe = ? AND symbol = ? AND dataset_tag = ? AND time BETWEEN ? AND ?
    ORDER BY time
    '''
    iv_timeframe = config.get('data', {}).get('timeframes', {}).get('iv', '1m')
    iv_df = pd.read_sql_query(iv_query, conn, params=(iv_timeframe, symbol, tag, start_date, end_date))
    
    # Загрузка трендов
    trend_query = '''
    SELECT timestamp, confidence FROM trend_signals_15m
    WHERE symbol = ? AND dataset_tag = ? AND timestamp BETWEEN ? AND ?
    ORDER BY timestamp
    '''
    trend_df = pd.read_sql_query(trend_query, conn, params=(symbol, tag, start_date, end_date))
    
    conn.close()
    
    # Преобразование временных меток
    spot_df['time'] = pd.to_datetime(spot_df['time'])
    futures_df['time'] = pd.to_datetime(futures_df['time'])
    iv_df['time'] = pd.to_datetime(iv_df['time'])
    trend_df['timestamp'] = pd.to_datetime(trend_df['timestamp'])
    
    # Объединение данных по временным меткам
    # Сначала объединяем спот и фьючерсы
    merged_df = pd.merge_asof(
        spot_df.sort_values('time'),
        futures_df.sort_values('time'),
        on='time',
        direction='nearest',
        tolerance=pd.Timedelta('1min')
    )
    
    # Затем добавляем IV данные
    merged_df = pd.merge_asof(
        merged_df,
        iv_df.sort_values('time'),
        on='time',
        direction='nearest',
        tolerance=pd.Timedelta('1min')
    )
    
    # Наконец, добавляем тренды
    merged_df = pd.merge_asof(
        merged_df,
        trend_df.sort_values('timestamp'),
        left_on='time',
        right_on='timestamp',
        direction='backward',
        tolerance=pd.Timedelta('15min')
    )
    
    return merged_df

def run_backtest(symbol, tag, start_date, end_date):
    """
    Основная функция для запуска бэктеста.
    """
    # Загрузка конфигурации
    config = get_config()
    
    print(f"Запуск бэктеста для {symbol} с тегом {tag} с {start_date} по {end_date}")
    
    # Замер времени начала
    start_time = time.time()
    
    # Загрузка данных
    data_df = load_data(symbol, tag, start_date, end_date)
    
    if data_df.empty:
        print("Нет данных для анализа")
        return
    
    # Инициализация анализаторов с правильными параметрами
    basis_analyzer = BasisAnalyzer(symbol=symbol, dataset_tag=tag)
    historical_analyzer = HistoricalAnalyzer(symbol=symbol, dataset_tag=tag)
    
    # Загрузка исторических данных для historical_analyzer
    historical_analyzer.load_historical_data()
    
    # Инициализация PredictionLayer и ErrorMonitor
    window_size = config.get('prediction_layer', {}).get('window_size', 5)
    prediction_layer = PredictionLayer(window_size=window_size)
    error_monitor = ErrorMonitor()
    
    # Инициализация компонентов адаптивного механизма
    block_detector = BlockDetector('server_opc.db')
    block_analyzer = BlockAnalyzer('server_opc.db')
    formula_engine_blocks = FormulaEngineBlocks('server_opc.db')
    
    # Переменные для отслеживания рыночных режимов
    market_regimes = {}  # Словарь для подсчета режимов
    current_regime = 'unknown'  # Текущий рыночный режим
    current_parameters = {'threshold': 0.7}  # Текущие параметры формулы
    
    # Векторизованные расчеты
    print("Выполнение векторизованных расчетов...")
    
    # Расчет basis_rel
    data_df['basis_rel'] = (data_df['futures_price'] - data_df['spot_price']) / data_df['spot_price']
    
    # Расчет iv_spike (разница между текущим и предыдущим значением IV)
    data_df['iv_spike'] = data_df['iv_mean_all'].diff()
    
    # Заполнение NaN значений
    data_df['iv_spike'] = data_df['iv_spike'].fillna(0)
    
    # Векторизованное прогнозирование (упрощенная версия)
    # Для демонстрации создадим простой прогноз как скользящее среднее
    # data_df['predicted_price'] = data_df['spot_price'].rolling(window=5, min_periods=1).mean()
    # data_df['predicted_price'] = data_df['predicted_price'].fillna(data_df['spot_price'])
    
    # Векторизованный расчет ошибок
    data_df['actual_price'] = data_df['spot_price']
    data_df['volatility'] = data_df['spot_price'].diff().abs().fillna(0)
    
    # Добавляем колонку для прогнозов ML модели
    data_df['ml_predicted_price'] = 0.0
    
    # Итеративное обновление монитора ошибок с использованием ML модели
    print("Обновление монитора ошибок с использованием ML модели...")
    for i in range(len(data_df)):  # Для всех точек
        if i > 0:
            # Подготовка признаков для ML модели
            features = prepare_ml_features(data_df, i, window_size)
            
            # Создание массива признаков для модели
            feature_columns = [
                'spot_price', 'volume', 'spot_price_iv', 'skew_30d', 'basis_rel',
                'confidence', 'trend_iv_30d', 'trend_skew_30d', 'spot_ma_7', 'spot_ma_30',
                'iv_ma_7', 'iv_ma_30', 'spot_volatility_7', 'spot_volatility_30',
                'spot_lag_1', 'spot_lag_2', 'spot_lag_3', 'iv_lag_1', 'iv_lag_2',
                'spot_pct_change', 'iv_pct_change', 'rsi'
            ]
            
            # Создание массива признаков
            X = np.array([features[col] for col in feature_columns]).reshape(1, -1)
            
            # Прогнозирование с использованием ML модели
            try:
                ml_predicted_price = prediction_layer._ml_random_forest(X)
                data_df.at[i, 'ml_predicted_price'] = ml_predicted_price
            except Exception as e:
                print(f"Ошибка прогнозирования ML моделью: {e}")
                # Используем простое скользящее среднее как запасной вариант
                ml_predicted_price = data_df.iloc[max(0, i-window_size):i+1]['spot_price'].mean()
                data_df.at[i, 'ml_predicted_price'] = ml_predicted_price
            
            # Обновление монитора ошибок с прогнозом ML модели
            error_monitor.update(
                timestamp=data_df.iloc[i]['time'],
                predicted=ml_predicted_price,
                actual=data_df.iloc[i]['actual_price'],
                volatility=data_df.iloc[i]['volatility'],
                formula_id='F01',
                confidence=1.0,
                method='ml_random_forest'
            )
            
            # Периодический запуск адаптации каждые 100 точек
            if i % 100 == 0 and i > 0:
                print(f"Адаптация параметров на шаге {i}...")
                
                # а) Получаем историю ошибок
                error_history = error_monitor.get_errors()
                if len(error_history) > 500:
                    error_history = error_history.tail(500)
                
                if not error_history.empty:
                    # б) Передаем данные в детектор
                    block_threshold = config.get('block_detector', {}).get('threshold', 1.5)
                    block_window = config.get('block_detector', {}).get('window', 50)
                    blocks = block_detector.detect_block_boundaries(error_history, threshold=block_threshold, window=block_window)
                    
                    if len(blocks) > 0:
                        # в) Анализируем последний блок
                        latest_block = blocks[-1]
                        
                        # Сохраняем блок в БД
                        block_detector.save_blocks([latest_block])
                        
                        # Получаем ID последнего блока из БД
                        blocks_df = block_detector.get_blocks()
                        if not blocks_df.empty:
                            latest_block_id = blocks_df.iloc[-1]['id']
                            
                            # Анализируем последний блок и определяем рыночный режим
                            market_regime = block_analyzer.classify_market_regime(latest_block_id)
                            print(f"📊 Обнаружен рыночный режим: {market_regime}")
                            
                            # Обновляем статистику режимов
                            if market_regime in market_regimes:
                                market_regimes[market_regime] += 1
                            else:
                                market_regimes[market_regime] = 1
                            
                            # Сохраняем текущий режим
                            current_regime = market_regime
                            
                            # г) Адаптируем параметры формулы
                            current_parameters = formula_engine_blocks.get_formula_parameters(
                                'balanced', latest_block_id, market_regime
                            )
                            print(f"⚙️ Адаптированы параметры для режима {market_regime}: {current_parameters}")
    
    
    # Векторизованное принятие решений
    print("Принятие решений...")
    
    # Создаем базовые оценки (в реальной реализации это требует вызова анализаторов)
    # Для демонстрации используем упрощенные расчеты
    data_df['basis_score'] = np.where(data_df['basis_rel'] > 0.001, 0.8, 
                             np.where(data_df['basis_rel'] < -0.001, 0.8, 0.5))
    
    data_df['historical_score'] = 0.5  # Заглушка
    
    # Используем адаптированные параметры
    weights = config.get('backtester', {}).get('weights', {})
    basis_weight = current_parameters.get('basis_weight', weights.get('basis', 0.4))
    historical_weight = weights.get('history', 0.4)
    news_weight = weights.get('news', 0.2)
    
    # Векторизованная агрегация результатов
    data_df['final_score'] = (data_df['basis_score'] * basis_weight +
                             data_df['historical_score'] * historical_weight +
                             0.5 * news_weight)  # 0.5 как заглушка для news_score
    
    # Векторизованное принятие решений с использованием адаптированного порога
    threshold = current_parameters.get('threshold', 0.7)
    
    # Получаем пороги из конфигурации
    thresholds = config.get('backtester', {}).get('decision_thresholds', {})
    strong_buy_threshold = thresholds.get('strong_buy', 0.8)
    buy_threshold = thresholds.get('buy', 0.7)
    sell_threshold = thresholds.get('sell', 0.3)
    strong_sell_threshold = thresholds.get('strong_sell', 0.2)
    
    # Используем np.select для векторизованного принятия решений
    conditions = [
        data_df['final_score'] > strong_buy_threshold,  # STRONG BUY
        data_df['final_score'] > buy_threshold,         # BUY
        data_df['final_score'] < sell_threshold,        # SELL
        data_df['final_score'] < strong_sell_threshold  # STRONG SELL
    ]
    
    choices = ['STRONG BUY', 'BUY', 'SELL', 'STRONG SELL']
    data_df['decision'] = np.select(conditions, choices, default='HOLD')
    
    # Фильтрация и вывод отчетов для сигналов, отличных от HOLD
    signals_df = data_df[data_df['decision'] != 'HOLD']
    
    if not signals_df.empty:
        print(f"Найдено {len(signals_df)} сигналов:")
        for _, row in signals_df.head(10).iterrows():  # Показываем первые 10 сигналов
            report = {
                'timestamp': row['time'],
                'decision': row['decision'],
                'basis_score': row['basis_score'],
                'historical_score': row['historical_score'],
                'news_score': 0.5,  # Заглушка
                'final_score': row['final_score'],
                'regime': current_regime,
                'threshold': threshold
            }
            print(f"Отчет: {report}")
    
    # Замер времени окончания
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Бэктест с использованием ML модели завершен за {duration:.2f} секунд")
    
    # Финальная отчетность по обнаруженным режимам
    print("\n" + "="*50)
    print("--- СВОДКА ПО РЫНОЧНЫМ РЕЖИМАМ ---")
    
    if market_regimes:
        total_regimes = sum(market_regimes.values())
        print(f"Обнаружено {len(market_regimes)} уникальных режима:")
        
        # Сортируем режимы по количеству обнаружений
        sorted_regimes = sorted(market_regimes.items(), key=lambda x: x[1], reverse=True)
        
        for regime, count in sorted_regimes:
            percentage = (count / total_regimes) * 100
            # Склонение слова "раз" в зависимости от числа
            if count % 10 == 1 and count % 100 != 11:
                times_word = "раз"
            elif count % 10 in [2, 3, 4] and count % 100 not in [12, 13, 14]:
                times_word = "раза"
            else:
                times_word = "раз"
            print(f"- {regime.upper()}: {count} {times_word} ({percentage:.1f}%)")
    else:
        print("Режимы не обнаружены")
    
    print("-" * 36)
    print("=" * 50)

def main():
    parser = argparse.ArgumentParser(description='Запуск бэктеста для заданного актива и набора данных.')
    parser.add_argument('--symbol', required=True, help='Символ актива (например, BTCUSDT или SOLUSDT)')
    parser.add_argument('--tag', required=True, help='Тег набора данных (например, training_2023)')
    parser.add_argument('--start', required=True, help='Дата начала (например, 2023-01-01)')
    parser.add_argument('--end', required=True, help='Дата окончания (например, 2023-01-07)')
    
    args = parser.parse_args()
    
    run_backtest(args.symbol, args.tag, args.start, args.end)

if __name__ == "__main__":
    main()