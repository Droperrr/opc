#!/usr/bin/env python3
"""
Оптимизатор параметров для MVP-стратегии с фильтром SMA-20.
Использует Байесовскую оптимизацию для нахождения оптимальных параметров TP/SL.
"""

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
# Оптимизируем три параметра:
# 1. atr_risk_multiplier: диапазон от 1.0 до 4.0
# 2. reward_ratio: диапазон от 0.5 до 4.0
# 3. sma_period: диапазон от 10 до 100 (целочисленный)
from skopt.space import Integer
bayesian_param_space = [
    Real(1.0, 4.0, name='atr_risk_multiplier'),  # Множитель для Stop Loss
    Real(0.5, 4.0, name='reward_ratio'),         # Соотношение Risk/Reward
    Integer(10, 100, name='sma_period')          # Период SMA для тренд-фильтра
]

def bayesian_objective_function(params):
    """
    Целевая функция для Байесовской оптимизации.
    Принимает список параметров и возвращает -sharpe_ratio.
    
    Args:
        params (list): Список параметров [atr_risk_multiplier, reward_ratio, sma_period]
        
    Returns:
        float: Отрицательное значение Sharpe Ratio.
    """
    atr_risk_multiplier, reward_ratio, sma_period = params
    
    # Фиксированный размер позиции
    position_size = 0.1
    # Фиксированный порог z-score
    z_score_threshold = 2.0
    
    # Формируем фиксированные параметры для бэктеста
    fixed_trading_params = {
        'atr_risk_multiplier': atr_risk_multiplier,
        'reward_ratio': reward_ratio
    }
    
    # Создаем словарь параметров для логирования
    params_dict = {
        'atr_risk_multiplier': atr_risk_multiplier,
        'reward_ratio': reward_ratio,
        'sma_period': sma_period,
        'position_size': position_size
    }
    
    logger.info(f"OPTIMIZER: Sending params to backtester -> {params_dict}")
    
    print(f"  📤 Отправляемые параметры в бэктест:")
    print(f"    atr_risk_multiplier: {atr_risk_multiplier}")
    print(f"    reward_ratio: {reward_ratio}")
    print(f"    sma_period: {sma_period}")
    print(f"    z_score_threshold: {z_score_threshold}")
    print(f"    position_size: {position_size}")
    print(f"    fixed_trading_params JSON: {json.dumps(fixed_trading_params)}")
    
    # Формируем команду для запуска бэктеста
    # Используем Walk-Forward бэктест с нашей стратегией SMA-20
    cmd = [
        'python3', 'run_walk_forward_backtest.py'
    ]
    
    # Добавляем параметры для бэктеста
    cmd.extend([
        '--fixed_trading_params', json.dumps(fixed_trading_params),
        '--initial_capital', '10000',
        '--position_size', str(position_size),
        '--data_file', 'basis_features_1m.parquet',
        '--asset', 'SOL',
        '--sma_period', str(sma_period),
        '--z_score_threshold', str(z_score_threshold)
    ])
    
    print(f"  🚀 Запуск Walk-Forward бэктеста с параметрами:")
    print(f"    atr_risk_multiplier: {atr_risk_multiplier}")
    print(f"    reward_ratio: {reward_ratio}")
    print(f"    sma_period: {sma_period}")
    print(f"    z_score_threshold: {z_score_threshold}")
    print(f"    position_size: {position_size}")
    
    try:
        # Запускаем бэктест
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 минут таймаут
        
        if result.returncode != 0:
            print(f"  ❌ Ошибка запуска бэктеста: {result.stderr}")
            return 1000.0  # Возвращаем большое значение в случае ошибки
        
        # Читаем результаты из performance_metrics.json
        metrics_file = os.path.join('backtest_results_mvp', 'performance_metrics.json')
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
    print("🚀 Запуск Байесовской оптимизации для стратегии SMA-20...")
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
    print(f"  sma_period: {result.x[2]:.0f}")
    print(f"Лучшее значение (отрицательный Sharpe Ratio): {result.fun:.6f}")
    print(f"Лучший Sharpe Ratio: {-result.fun:.6f}")
    
    # Сохраняем результаты
    results_file = 'bayesian_optimization_results_sma20.json'
    results = {
        'best_params': {
            'atr_risk_multiplier': result.x[0],
            'reward_ratio': result.x[1],
            'sma_period': result.x[2]
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
    print("🚀 Запуск оптимизатора параметров для стратегии SMA-20...")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Запускаем Байесовскую оптимизацию
    result = run_bayesian_optimization(n_calls=50)  # 50 итераций как в оригинале

if __name__ == "__main__":
    main()