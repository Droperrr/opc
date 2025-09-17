#!/usr/bin/env python3
"""
Скрипт для запуска Walk-Forward бэктеста на MVP-стратегии
"""

import sys
import os
import pandas as pd
import json
from advanced_backtest_system import AdvancedBacktestSystem

def main():
    """Основная функция для запуска Walk-Forward бэктеста"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Walk-Forward Backtest for MVP Strategy')
    parser.add_argument('--fixed_trading_params', type=str, default=None, help='JSON string with fixed trading parameters (atr_risk_multiplier, reward_ratio)')
    parser.add_argument('--initial_capital', type=float, default=10000, help='Initial capital for backtest')
    parser.add_argument('--position_size', type=float, default=0.1, help='Position size as fraction of capital (default: 0.1)')
    parser.add_argument('--data_file', type=str, default='basis_features_1m.parquet', help='Path to features data file')
    parser.add_argument('--asset', type=str, default='SOL', help='Asset to backtest (SOL or BTC)')
    parser.add_argument('--limit_days', type=int, default=None, help='Limit the number of days for backtest (default: None - all data)')
    parser.add_argument('--sma_period', type=int, default=20, help='SMA period for trend filter')
    parser.add_argument('--z_score_threshold', type=float, default=2.0, help='Z-Score threshold for signal generation')
    parser.add_argument('--results_dir', type=str, default='backtest_results_mvp', help='Directory for backtest results')
    parser.add_argument('--run_id', type=str, default=None, help='Run ID for this backtest session')
    
    args = parser.parse_args()
    
    # Парсим JSON-строку параметров, если она предоставлена
    fixed_trading_params = None
    if args.fixed_trading_params:
        try:
            fixed_trading_params = json.loads(args.fixed_trading_params)
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга fixed_trading_params: {e}")
            return 1
    
    print("=" * 60)
    print("ЗАПУСК WALK-FORWARD БЭКТЕСТА НА MVP-СТРАТЕГИИ")
    print("=" * 60)
    
    try:
        # Проверяем наличие необходимых файлов
        required_files = [args.data_file]
        for file in required_files:
            if not os.path.exists(file):
                print(f"❌ Файл {file} не найден")
                return 1
        
        # Загружаем данные для проверки структуры
        print("🔍 Проверка структуры данных...")
        df = pd.read_parquet(args.data_file)
        print(f"✅ Загружено {len(df)} записей")
        print(f"📅 Период данных: {df['time'].min()} - {df['time'].max()}")
        print(f"📊 Колонки: {list(df.columns)}")
        
        # Создаем экземпляр бэктест системы
        backtest = AdvancedBacktestSystem(
            data_file=args.data_file,
            signals_file='basis_signals.csv',  # Временный файл для сигналов
            z_score_threshold=args.z_score_threshold,
            initial_capital=args.initial_capital,
            position_size=args.position_size,
            fixed_trading_params=fixed_trading_params,
            results_dir=args.results_dir,
            asset=args.asset,
            sma_period=args.sma_period,
            run_id=args.run_id
        )
        
        # Запускаем Walk-Forward бэктест
        print("\n🚀 Запуск Walk-Forward бэктеста...")
        backtest.run(walk_forward=True, limit_days=args.limit_days)
        
        # Проверяем наличие результатов
        results_dir = 'backtest_results_mvp'
        if os.path.exists(results_dir):
            print(f"\n✅ Результаты сохранены в {results_dir}/")
            
            # Проверяем наличие ключевых файлов результатов
            expected_files = [
                'backtest_report.md',
                'backtest_trades.csv',
                'equity_curve.csv',
                'performance_metrics.json'
            ]
            
            for file in expected_files:
                file_path = os.path.join(results_dir, file)
                if os.path.exists(file_path):
                    print(f"  📄 {file}")
                else:
                    print(f"  ❌ {file} (отсутствует)")
        else:
            print(f"❌ Директория с результатами {results_dir} не найдена")
            return 1
            
        print("\n" + "=" * 60)
        print("WALK-FORWARD БЭКТЕСТ ЗАВЕРШЕН")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка при запуске бэктеста: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())