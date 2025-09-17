#!/usr/bin/env python3
"""
Оптимизатор параметров для MVP-стратегии.
Использует Grid Search и Байесовскую оптимизацию для нахождения оптимальных параметров.
"""

import itertools
import json
import os
import subprocess
import time
from datetime import datetime
import pandas as pd
from skopt import gp_minimize
from skopt.space import Real
from logger import get_logger

logger = get_logger()

# Пространство поиска параметров для Байесовской оптимизации
bayesian_param_space = [
    Real(1.0, 3.0, name='atr_risk_multiplier'),  # Множитель для Stop Loss
    Real(1.0, 5.0, name='reward_ratio'),         # Соотношение Risk/Reward
    Real(0.05, 0.2, name='position_size')        # Размер позиции в процентах от капитала
]

def bayesian_objective_function(params):
    """
    Целевая функция для Байесовской оптимизации.
    Принимает список параметров и возвращает -sharpe_ratio.
    
    Args:
        params (list): Список параметров [atr_risk_multiplier, reward_ratio, position_size]
        
    Returns:
        float: Отрицательное значение Sharpe Ratio.
    """
    atr_risk_multiplier, reward_ratio, position_size = params
    
    # Формируем фиксированные параметры для бэктеста
    fixed_trading_params = {
        'atr_risk_multiplier': atr_risk_multiplier,
        'reward_ratio': reward_ratio
    }
    
    # Создаем словарь параметров для логирования
    params_dict = {
        'atr_risk_multiplier': atr_risk_multiplier,
        'reward_ratio': reward_ratio,
        'position_size': position_size
    }
    
    logger.info(f"OPTIMIZER: Sending params to backtester -> {params_dict}")
    
    print(f"  📤 Отправляемые параметры в бэктест:")
    print(f"    atr_risk_multiplier: {atr_risk_multiplier}")
    print(f"    reward_ratio: {reward_ratio}")
    print(f"    position_size: {position_size}")
    print(f"    fixed_trading_params JSON: {json.dumps(fixed_trading_params)}")
    
    # Формируем команду для запуска бэктеста
    cmd = [
        'python3', 'advanced_backtest_system.py',
        '--signals_file', 'candle_signals_btc.csv',  # Используем сигналы от candle_signal_generator
        '--fixed_trading_params', json.dumps(fixed_trading_params),
        '--initial_capital', '10000',
        '--position_size', str(position_size),
        '--results_dir', 'backtest_results_btc'
    ]
    
    print(f"  🚀 Запуск бэктеста с параметрами:")
    print(f"    atr_risk_multiplier: {atr_risk_multiplier}")
    print(f"    reward_ratio: {reward_ratio}")
    print(f"    position_size: {position_size}")
    
    try:
        # Запускаем бэктест
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 минут таймаут
        
        if result.returncode != 0:
            print(f"  ❌ Ошибка запуска бэктеста: {result.stderr}")
            return 1000.0  # Возвращаем большое значение в случае ошибки
        
        # Читаем результаты из performance_metrics.json
        metrics_file = os.path.join('backtest_results_btc', 'performance_metrics.json')
        if os.path.exists(metrics_file):
            with open(metrics_file, 'r', encoding='utf-8') as f:
                metrics = json.load(f)
            
            # Получаем Sharpe Ratio
            sharpe_ratio = metrics.get('sharpe_ratio', 0)
            
            # Возвращаем отрицательное значение, так как оптимизатор ищет минимум
            print(f"    ✅ Sharpe Ratio: {sharpe_ratio:.3f}")
            return -sharpe_ratio
        else:
            print(f"  ❌ Файл {metrics_file} не найден")
            return 1000.0
            
    except subprocess.TimeoutExpired:
        print(f"  ❌ Таймаут при запуске бэктеста")
        return 1000.0
    except Exception as e:
        print(f"  ❌ Ошибка при запуске бэктеста: {e}")
        return 1000.0

def run_bayesian_optimization(n_calls=50):
    """
    Запускает Байесовскую оптимизацию.
    
    Args:
        n_calls (int): Количество итераций оптимизации.
    """
    print("🚀 Запуск Байесовской оптимизации...")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Запускаем оптимизацию
    result = gp_minimize(
        func=bayesian_objective_function,
        dimensions=bayesian_param_space,
        n_calls=n_calls,
        random_state=42,
        verbose=True
    )
    
    # Выводим результаты
    print("\n=== Результаты оптимизации ===")
    print(f"Лучшие параметры:")
    print(f"  atr_risk_multiplier: {result.x[0]:.3f}")
    print(f"  reward_ratio: {result.x[1]:.3f}")
    print(f"  position_size: {result.x[2]:.3f}")
    print(f"Лучшее значение (отрицательный Sharpe Ratio): {result.fun:.6f}")
    print(f"Лучший Sharpe Ratio: {-result.fun:.6f}")
    
    # Сохраняем результаты
    results_file = 'bayesian_optimization_results_btc.json'
    results = {
        'best_params': {
            'atr_risk_multiplier': result.x[0],
            'reward_ratio': result.x[1],
            'position_size': result.x[2]
        },
        'best_negative_sharpe': result.fun,
        'best_sharpe': -result.fun,
        'all_params': result.x_iters,
        'all_values': result.func_vals,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"💾 Результаты сохранены в {results_file}")
    return result

def main():
    """Основная функция оптимизатора."""
    print("🚀 Запуск оптимизатора параметров...")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Сначала генерируем сигналы от candle_signal_generator
    print("📈 Генерация \"свечных\" сигналов...")
    try:
        subprocess.run(['python3', 'candle_signal_generator_btc.py'], check=True)
        print("✅ Сигналы успешно сгенерированы")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка генерации сигналов: {e}")
        return
    
    # Запускаем Байесовскую оптимизацию
    result = run_bayesian_optimization(n_calls=20)  # Начнем с 20 итераций для тестирования

# Тестовый блок для отладки передачи параметров
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print("🔍 Запуск тестового блока для отладки передачи параметров")
        print("--- TEST RUN 1 (Консервативные параметры) ---")
        bayesian_objective_function([1.5, 1.5, 0.1])
        print("--- TEST RUN 2 (Агрессивные параметры) ---")
        bayesian_objective_function([3.0, 3.0, 0.5])
    else:
        main()