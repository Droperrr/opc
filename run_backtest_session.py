#!/usr/bin/env python3
"""
Оркестратор для запуска полного цикла оптимизации и бэктеста
"""

import os
import sys
import json
import subprocess
from datetime import datetime
import pandas as pd
from logger import get_logger

logger = get_logger()

def main():
    """Основная функция оркестратора"""
    # Генерируем уникальный ID запуска
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"🚀 Запуск сессии бэктеста: {run_id}")
    
    # Создаем директорию для результатов этой сессии
    session_results_dir = os.path.join('backtest_results', run_id)
    os.makedirs(session_results_dir, exist_ok=True)
    print(f"📁 Директория результатов: {session_results_dir}")
    
    try:
        # Шаг 1: Запуск оптимизатора
        print("\n" + "=" * 60)
        print("ШАГ 1: ЗАПУСК ОПТИМИЗАТОРА")
        print("=" * 60)
        
        # Запускаем оптимизатор
        optimizer_cmd = [
            'python3', 'optimizer.py'
        ]
        
        print(f"🔧 Команда: {' '.join(optimizer_cmd)}")
        optimizer_result = subprocess.run(
            optimizer_cmd, 
            capture_output=True, 
            text=True, 
            timeout=3600  # 1 час таймаут
        )
        
        if optimizer_result.returncode != 0:
            print(f"❌ Ошибка запуска оптимизатора:")
            print(f"STDOUT: {optimizer_result.stdout}")
            print(f"STDERR: {optimizer_result.stderr}")
            return 1
        
        print("✅ Оптимизатор завершен успешно")
        print(f"STDOUT: {optimizer_result.stdout}")
        
        # Читаем результаты оптимизации
        if not os.path.exists('bayesian_optimization_results_sma20.json'):
            print("❌ Файл с результатами оптимизации не найден")
            return 1
            
        with open('bayesian_optimization_results_sma20.json', 'r', encoding='utf-8') as f:
            opt_results = json.load(f)
        
        best_params = opt_results['best_params']
        print(f"📊 Лучшие параметры: {best_params}")
        
        # Шаг 2: Запуск финального бэктеста с лучшими параметрами
        print("\n" + "=" * 60)
        print("ШАГ 2: ЗАПУСК ФИНАЛЬНОГО БЭКТЕСТА")
        print("=" * 60)
        
        # Формируем параметры для бэктеста
        fixed_trading_params = {
            'atr_risk_multiplier': best_params['atr_risk_multiplier'],
            'reward_ratio': best_params['reward_ratio']
        }
        
        # Запускаем бэктест
        backtest_cmd = [
            'python3', 'run_walk_forward_backtest.py',
            '--fixed_trading_params', json.dumps(fixed_trading_params),
            '--initial_capital', '10000',
            '--position_size', '0.1',
            '--data_file', 'basis_features_1m.parquet',
            '--asset', 'SOL',
            '--sma_period', str(int(best_params['sma_period'])),
            '--z_score_threshold', '2.0',
            '--results_dir', session_results_dir  # Передаем уникальную директорию
        ]
        
        print(f"🔧 Команда: {' '.join(backtest_cmd)}")
        backtest_result = subprocess.run(
            backtest_cmd, 
            capture_output=True, 
            text=True, 
            timeout=3600  # 1 час таймаут
        )
        
        if backtest_result.returncode != 0:
            print(f"❌ Ошибка запуска бэктеста:")
            print(f"STDOUT: {backtest_result.stdout}")
            print(f"STDERR: {backtest_result.stderr}")
            return 1
        
        print("✅ Бэктест завершен успешно")
        print(f"STDOUT: {backtest_result.stdout}")
        
        # Шаг 3: Подготовка данных для дашборда
        print("\n" + "=" * 60)
        print("ШАГ 3: ПОДГОТОВКА ДАННЫХ ДЛЯ ДАШБОРДА")
        print("=" * 60)
        
        # Запускаем подготовку данных для дашборда
        prepare_cmd = [
            'python3', 'prepare_dashboard_data.py',
            '--results_dir', session_results_dir,  # Передаем директорию результатов
            '--run_id', run_id  # Передаем ID запуска
        ]
        
        print(f"🔧 Команда: {' '.join(prepare_cmd)}")
        prepare_result = subprocess.run(
            prepare_cmd, 
            capture_output=True, 
            text=True, 
            timeout=300  # 5 минут таймаут
        )
        
        if prepare_result.returncode != 0:
            print(f"❌ Ошибка подготовки данных для дашборда:")
            print(f"STDOUT: {prepare_result.stdout}")
            print(f"STDERR: {prepare_result.stderr}")
            return 1
        
        print("✅ Данные для дашборда подготовлены успешно")
        print(f"STDOUT: {prepare_result.stdout}")
        
        # Шаг 4: Финальный отчет
        print("\n" + "=" * 60)
        print("ШАГ 4: ФИНАЛЬНЫЙ ОТЧЕТ")
        print("=" * 60)
        
        # Читаем метрики из результатов бэктеста
        metrics_file = os.path.join(session_results_dir, 'performance_metrics.json')
        if os.path.exists(metrics_file):
            with open(metrics_file, 'r', encoding='utf-8') as f:
                metrics = json.load(f)
            
            print(f"📈 ФИНАЛЬНЫЕ МЕТРИКИ:")
            print(f"   Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.3f}")
            print(f"   Win Rate: {metrics.get('win_rate', 0):.2%}")
            print(f"   Total P&L: {metrics.get('total_pnl', 0):.2f} USD")
            print(f"   Max Drawdown: {metrics.get('max_drawdown', 0):.2%}")
        else:
            print(f"❌ Файл метрик не найден: {metrics_file}")
        
        print(f"\n✅ Сессия бэктеста {run_id} завершена успешно!")
        print(f"📁 Результаты сохранены в: {session_results_dir}")
        
        return 0
        
    except subprocess.TimeoutExpired as e:
        print(f"❌ Таймаут при выполнении команды: {e}")
        return 1
    except Exception as e:
        print(f"❌ Ошибка в оркестраторе: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())