#!/usr/bin/env python3
"""
Упрощенный скрипт для оптимизации весов на основе наших данных
"""

import pandas as pd
import numpy as np
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simple_optimize_weights.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def calculate_formula_value(row, a, b, c):
    """
    Вычисляет значение формулы Y = a*IV + b*Skew - c*Basis
    """
    iv = row.get('iv_30d', 0) if not pd.isna(row.get('iv_30d', 0)) else 0
    skew = row.get('skew_30d', 0) if not pd.isna(row.get('skew_30d', 0)) else 0
    basis = row.get('basis_rel', 0) if not pd.isna(row.get('basis_rel', 0)) else 0
    
    Y = a * iv + b * skew - c * basis
    return Y

def generate_signals(df, a, b, c, threshold=0.7):
    """
    Генерирует сигналы на основе формулы
    """
    signals = []
    
    for i, row in df.iterrows():
        Y = calculate_formula_value(row, a, b, c)
        
        if Y > threshold:
            signal = 'BUY'
        elif Y < -threshold:
            signal = 'SELL'
        else:
            signal = 'NEUTRAL'
        
        signals.append({
            'time': row['time'],
            'Y_value': Y,
            'signal': signal,
            'spot_price': row['spot_price']
        })
    
    return pd.DataFrame(signals)

def evaluate_strategy(signals_df, price_data):
    """
    Оценивает стратегию на основе сигналов и реальных цен
    """
    if signals_df.empty:
        return 0
    
    # Фильтруем только BUY и SELL сигналы
    trading_signals = signals_df[signals_df['signal'].isin(['BUY', 'SELL'])].copy()
    
    if len(trading_signals) == 0:
        return 0
    
    # Для каждого сигнала находим цену через 15 минут (примерно 15 записей при 1-минутных данных)
    results = []
    
    for i, signal_row in trading_signals.iterrows():
        signal_time = signal_row['time']
        signal_type = signal_row['signal']
        signal_price = signal_row['spot_price']
        
        # Ищем данные через 15 минут
        future_data = price_data[price_data['time'] > signal_time]
        if len(future_data) > 15:
            future_price = future_data.iloc[15]['spot_price']
            
            # Рассчитываем результат
            if signal_type == 'BUY':
                result = (future_price - signal_price) / signal_price * 100  # Процентный результат
            else:  # SELL
                result = (signal_price - future_price) / signal_price * 100  # Процентный результат
            
            results.append(result)
    
    if len(results) > 0:
        # Возвращаем среднюю доходность
        return np.mean(results)
    else:
        return 0

def optimize_weights(df):
    """
    Оптимизирует веса для максимизации доходности
    """
    logger.info("Начало оптимизации весов")
    
    # Базовые параметры для поиска
    param_ranges = {
        'a': [0.5, 1.0, 1.5],
        'b': [0.3, 0.5, 0.7],
        'c': [0.2, 0.3, 0.4]
    }
    
    best_params = None
    best_return = -float('inf')
    
    # Простой grid search
    for a in param_ranges['a']:
        for b in param_ranges['b']:
            for c in param_ranges['c']:
                # Генерируем сигналы с текущими параметрами
                signals = generate_signals(df, a, b, c, threshold=0.7)
                
                # Оцениваем стратегию
                strategy_return = evaluate_strategy(signals, df)
                
                logger.info(f"Параметры a={a}, b={b}, c={c}: доходность={strategy_return:.3f}%")
                
                if strategy_return > best_return:
                    best_return = strategy_return
                    best_params = {'a': a, 'b': b, 'c': c, 'threshold': 0.7}
    
    logger.info(f"Лучшие параметры: {best_params} (доходность: {best_return:.3f}%)")
    return best_params

def main():
    """Основная функция оптимизации"""
    logger.info("Начало оптимизации весов для SOLUSDT")
    
    # Загружаем датасет
    dataset_path = 'ml_dataset_aug2025_SOLUSDT.parquet'
    logger.info(f"Загрузка датасета из {dataset_path}")
    
    try:
        df = pd.read_parquet(dataset_path)
        logger.info(f"Загружено {len(df)} записей")
        
        # Оптимизируем веса
        best_params = optimize_weights(df)
        
        if best_params:
            logger.info(f"Оптимизация завершена. Лучшие параметры: {best_params}")
            
            # Сохраняем параметры в файл
            import json
            with open('optimized_weights.json', 'w') as f:
                json.dump(best_params, f, indent=2)
            logger.info("Параметры сохранены в optimized_weights.json")
        else:
            logger.error("Оптимизация не удалась")
            
    except Exception as e:
        logger.error(f"Ошибка оптимизации: {e}")
        raise

if __name__ == "__main__":
    main()