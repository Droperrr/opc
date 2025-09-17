#!/usr/bin/env python3
"""
Скрипт для запуска бэктеста стратегии с робастным тренд-фильтром (SMA_20)
"""

import os
import sys
import json
from datetime import datetime
from advanced_backtest_system import AdvancedBacktestSystem

def main():
    """Основная функция запуска бэктеста"""
    print("=" * 60)
    print("🚀 ЗАПУСК БЭКТЕСТА СТРАТЕГИИ С РОБАСТНЫМ ТРЕНД-ФИЛЬТРОМ (SMA_20)")
    print("=" * 60)
    
    # Параметры бэктеста
    backtest_params = {
        'data_file': 'basis_features_1m.parquet',
        'z_score_threshold': 2.0,
        'initial_capital': 10000,
        'position_size': 0.1,
        'fixed_trading_params': {
            'atr_risk_multiplier': 2.0,
            'reward_ratio': 3.0
        },
        'results_dir': 'backtest_results_robust',
        'asset': 'SOL'
    }
    
    print("📊 Параметры бэктеста:")
    for key, value in backtest_params.items():
        if key != 'fixed_trading_params':
            print(f"  {key}: {value}")
        else:
            print(f"  {key}:")
            for k, v in value.items():
                print(f"    {k}: {v}")
    print()
    
    try:
        # Создаем экземпляр бэктест системы
        backtest = AdvancedBacktestSystem(**backtest_params)
        
        # Запускаем Walk-Forward бэктест
        print("🔄 Запуск Walk-Forward бэктеста...")
        backtest.run(walk_forward=True)
        
        # Проверяем наличие результатов
        results_dir = backtest_params['results_dir']
        if os.path.exists(results_dir):
            print(f"✅ Бэктест завершен успешно!")
            print(f"📁 Результаты сохранены в: {results_dir}")
            
            # Выводим ключевые метрики из отчета
            report_file = os.path.join(results_dir, 'backtest_report.md')
            if os.path.exists(report_file):
                print("\n📈 Ключевые метрики из отчета:")
                with open(report_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # Ищем секцию с ключевыми метриками
                    in_metrics_section = False
                    for line in lines:
                        if "## 📈 Ключевые метрики производительности" in line:
                            in_metrics_section = True
                            continue
                        if in_metrics_section:
                            if line.startswith("## "):
                                break
                            if "|" in line and "Метрика" not in line and "---------" not in line:
                                print(f"  {line.strip()}")
        else:
            print("❌ Результаты бэктеста не найдены")
            
    except Exception as e:
        print(f"❌ Ошибка при запуске бэктеста: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()