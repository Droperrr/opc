import argparse
import sqlite3
import sys
from datetime import datetime
import pandas as pd

# Импорты из проекта
from basis_analyzer import BasisAnalyzer

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
    
    conn.close()
    
    # Преобразование временных меток
    spot_df['time'] = pd.to_datetime(spot_df['time'])
    futures_df['time'] = pd.to_datetime(futures_df['time'])
    
    # Объединение данных по временным меткам
    merged_df = pd.merge_asof(
        spot_df.sort_values('time'),
        futures_df.sort_values('time'),
        on='time',
        direction='nearest',
        tolerance=pd.Timedelta('1min')
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
    
    # Создание цикла по историческим данным
    for index, row in data_df.iterrows():
        # Вызов анализаторов
        basis_result = basis_analyzer.analyze_current_basis()
        
        # Заглушка для новостного анализа
        news_result = {'score': 0.5, 'sentiment': 'NEUTRAL'}
        
        # Агрегация результатов
        # Простая взвешенная сумма для демонстрации
        basis_weight = 0.5
        news_weight = 0.5
        
        final_score = (basis_result.get('score', 0) * basis_weight +
                      news_result.get('score', 0) * news_weight)
        
        # Принятие решения на основе финального счета
        if final_score > 0.7:
            decision = "STRONG BUY"
        elif final_score > 0.6:
            decision = "BUY"
        elif final_score < 0.3:
            decision = "SELL"
        elif final_score < 0.4:
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
                'news_score': news_result.get('score', 0),
                'final_score': final_score
            }
            print(f"Отчет: {report}")

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