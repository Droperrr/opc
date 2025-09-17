import argparse
import sqlite3
import sys
from datetime import datetime
import pandas as pd

# Импорты из проекта
from basis_analyzer import BasisAnalyzer
from historical_analyzer import HistoricalAnalyzer
from prediction_layer import PredictionLayer
from error_monitor import ErrorMonitor
from block_detector import BlockDetector
from block_analyzer import BlockAnalyzer
from formula_engine_blocks import FormulaEngineBlocks

def load_data(symbol, tag, start_date, end_date):
    """
    Загружает исторические данные из базы данных для заданного символа и тега.
    """
    conn = sqlite3.connect('server_opc.db')
    
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
    WHERE timeframe = '1m' AND symbol = ? AND dataset_tag = ? AND time BETWEEN ? AND ?
    ORDER BY time
    '''
    iv_df = pd.read_sql_query(iv_query, conn, params=(symbol, tag, start_date, end_date))
    
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
    print(f"Запуск бэктеста для {symbol} с тегом {tag} с {start_date} по {end_date}")
    
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
    prediction_layer = PredictionLayer(window_size=5)
    error_monitor = ErrorMonitor()
    
    # Инициализация компонентов адаптивного механизма
    block_detector = BlockDetector('server_opc.db')
    block_analyzer = BlockAnalyzer('server_opc.db')
    formula_engine_blocks = FormulaEngineBlocks('server_opc.db')
    
    # Переменные для отслеживания рыночных режимов
    market_regimes = {}  # Словарь для подсчета режимов
    current_regime = 'unknown'  # Текущий рыночный режим
    current_parameters = {'threshold': 0.7}  # Текущие параметры формулы
    
    # Создание цикла по историческим данным
    for index, row in data_df.iterrows():
        # Периодическая проверка для обнаружения блоков (каждые 100 итераций)
        if index > 0 and index % 100 == 0:
            print(f"🔍 Периодическая проверка блоков на итерации {index}")
            
            # Получаем свежую историю ошибок (последние 500 записей)
            error_history = error_monitor.get_errors()
            if len(error_history) > 500:
                error_history = error_history.tail(500)
            
            if not error_history.empty:
                # Обнаруживаем блоки
                blocks = block_detector.detect_block_boundaries(error_history, threshold=1.5, window=50)
                
                if len(blocks) > 0:
                    # Берем последний блок
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
                        
                        # Адаптируем параметры формулы
                        current_parameters = formula_engine_blocks.get_formula_parameters(
                            'balanced', latest_block_id, market_regime
                        )
                        print(f"⚙️ Адаптированы параметры для режима {market_regime}: {current_parameters}")
        
        # Прогнозирование
        historical_prices = data_df['spot_price'].iloc[:index]
        if len(historical_prices) > 5:  # Минимальное окно для прогноза
            predicted_price = prediction_layer.predict_next_price(historical_prices)
        else:
            predicted_price = row['spot_price']  # Если данных мало, прогноз = текущая цена
        
        # Фактическая цена
        actual_price = row['spot_price']
        
        # Запись ошибки
        if index > 0:  # Начинаем запись ошибок со второй итерации
            volatility = abs(actual_price - data_df['spot_price'].iloc[index-1]) if index > 0 else 0
            error_monitor.update(
                timestamp=row['time'],
                predicted=predicted_price,
                actual=actual_price,
                volatility=volatility,
                formula_id='F01',
                confidence=1.0,
                method='simple_moving_average'
            )
        
        # Подготовка данных для анализаторов
        # Для basis_analyzer нам нужно обновить его внутренние данные
        # Поскольку метод analyze_current_basis не принимает параметров,
        # мы создаем временные таблицы в БД для каждого шага
        # Это неэффективно, но соответствует интерфейсу
        
        # Альтернативно, можно модифицировать BasisAnalyzer для приема данных
        # Но для соответствия требованиям задачи оставим как есть
        
        # Для historical_analyzer формируем current_signal
        current_signal = {
            'iv_spike': row['iv_mean_all'] if pd.notna(row['iv_mean_all']) else 0,
            'skew': row['skew'] if pd.notna(row['skew']) else 0,
            'trend_confidence': row['confidence'] if pd.notna(row['confidence']) else 0,
            'direction': 'NEUTRAL',  # Заглушка, так как направление не определено
            'spot_price': row['spot_price'] if pd.notna(row['spot_price']) else 0
        }
        
        # Вызов анализаторов
        # Для basis_analyzer передаем данные в качестве параметра
        basis_data = {
            'spot_price': row['spot_price'] if pd.notna(row['spot_price']) else 0,
            'futures_price': row['futures_price'] if pd.notna(row['futures_price']) else 0
        }
        basis_result = basis_analyzer.analyze_current_basis(basis_data)
        historical_result = historical_analyzer.find_analogies(current_signal)
        
        # Заглушка для новостного анализа
        news_result = {'score': 0.5, 'sentiment': 'NEUTRAL'}
        
        # Агрегация результатов с использованием адаптированных параметров
        # Простая взвешенная сумма для демонстрации
        basis_weight = current_parameters.get('basis_weight', 0.4)
        historical_weight = 0.4  # Можно также адаптировать
        news_weight = 0.2
        
        # Для historical_result используем win_rate как score
        historical_score = historical_result.get('win_rate', 0)
        
        final_score = (basis_result.get('score', 0) * basis_weight +
                      historical_score * historical_weight +
                      news_result.get('score', 0) * news_weight)
        
        # Принятие решения на основе финального счета и адаптированного порога
        threshold = current_parameters.get('threshold', 0.7)
        if final_score > threshold + 0.1:  # STRONG BUY
            decision = "STRONG BUY"
        elif final_score > threshold:  # BUY
            decision = "BUY"
        elif final_score < threshold - 0.4:  # SELL
            decision = "SELL"
        elif final_score < threshold - 0.3:  # STRONG SELL
            decision = "STRONG SELL"
        else:
            decision = "HOLD"
        
        # Вывод отчета, если решение не HOLD
        if decision != "HOLD":
            timestamp = row['time'] if pd.notna(row['time']) else "N/A"
            report = {
                'timestamp': timestamp,
                'decision': decision,
                'basis_score': basis_result.get('score', 0),
                'historical_score': historical_score,
                'news_score': news_result.get('score', 0),
                'final_score': final_score,
                'regime': current_regime,
                'threshold': threshold
            }
            print(f"Отчет: {report}")

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