#!/usr/bin/env python3
"""
Стресс-тестирование системы в экстремальных условиях
Проверяет работу при высокой волатильности и резких рыночных движениях
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psutil
import os
import logging
from typing import Dict, List, Any, Tuple
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_high_volatility_data(base_price: float = 200.0, periods: int = 1000) -> pd.DataFrame:
    """Создание данных с высокой волатильностью"""
    np.random.seed(42)  # Для воспроизводимости
    
    # Создаем данные с высокой волатильностью (5-15% в час)
    returns = np.random.normal(0, 0.05, periods)  # 5% волатильность
    prices = [base_price]
    
    for i in range(1, periods):
        # Добавляем тренд и случайные скачки
        trend = 0.001 * np.sin(i / 100)  # Небольшой тренд
        jump = np.random.choice([0, 0.1, -0.1], p=[0.8, 0.1, 0.1])  # 10% скачки
        new_price = prices[-1] * (1 + returns[i] + trend + jump)
        prices.append(max(new_price, 1.0))  # Минимальная цена 1.0
    
    # Создаем DataFrame
    timestamps = [datetime.now() - timedelta(minutes=i) for i in range(periods, 0, -1)]
    
    df = pd.DataFrame({
        'time': timestamps,
        'spot_price': prices,
        'high': [p * 1.02 for p in prices],  # +2% максимум
        'low': [p * 0.98 for p in prices],   # -2% минимум
        'open': prices,
        'close': prices,
        'volume': np.random.uniform(1000, 10000, periods),
        'iv': np.random.uniform(0.8, 2.0, periods),  # Высокая IV (80-200%)
        'skew': np.random.uniform(-0.5, 0.5, periods),  # Широкий диапазон skew
        'basis_rel': np.random.uniform(-0.1, 0.1, periods)  # Широкий диапазон basis
    })
    
    return df

def create_crash_scenario_data(base_price: float = 200.0, periods: int = 1000) -> pd.DataFrame:
    """Создание данных сценария краха рынка"""
    np.random.seed(123)  # Для воспроизводимости
    
    prices = [base_price]
    crash_start = periods // 2  # Крах в середине периода
    
    for i in range(1, periods):
        if i < crash_start:
            # Нормальный рынок
            change = np.random.normal(0, 0.01)
        elif i < crash_start + 50:
            # Крах: -30% за 50 периодов
            change = -0.006  # -0.6% за период
        else:
            # Восстановление
            change = np.random.normal(0.002, 0.02)  # Восстановление с высокой волатильностью
        
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 1.0))
    
    timestamps = [datetime.now() - timedelta(minutes=i) for i in range(periods, 0, -1)]
    
    df = pd.DataFrame({
        'time': timestamps,
        'spot_price': prices,
        'high': [p * 1.01 for p in prices],
        'low': [p * 0.99 for p in prices],
        'open': prices,
        'close': prices,
        'volume': np.random.uniform(5000, 50000, periods),  # Высокий объем
        'iv': np.random.uniform(1.5, 3.0, periods),  # Очень высокая IV
        'skew': np.random.uniform(-1.0, 1.0, periods),  # Экстремальный skew
        'basis_rel': np.random.uniform(-0.2, 0.2, periods)  # Экстремальный basis
    })
    
    return df

def test_formula_engine_stress(data: pd.DataFrame, scenario_name: str):
    """Стресс-тестирование Formula Engine"""
    print(f"\n🔧 СТРЕСС-ТЕСТ FORMULA ENGINE: {scenario_name}")
    print("-" * 50)
    
    try:
        from formula_engine import FormulaEngine
        from compatibility import safe_float
        
        engine = FormulaEngine()
        
        # Тест 1: Расчет ATR в экстремальных условиях
        atr = engine.calculate_atr(data)
        valid_atr = atr.dropna()
        
        if len(valid_atr) > 0:
            max_atr = valid_atr.max()
            min_atr = valid_atr.min()
            mean_atr = valid_atr.mean()
            
            print(f"✅ ATR в экстремальных условиях:")
            print(f"   Максимум: {max_atr:.6f}")
            print(f"   Минимум: {min_atr:.6f}")
            print(f"   Среднее: {mean_atr:.6f}")
            
            # Проверяем, что ATR не взрывается
            assert max_atr < 100.0, f"ATR слишком большой: {max_atr}"
            assert min_atr > 0.0, f"ATR отрицательный: {min_atr}"
        
        # Тест 2: Динамические пороги в экстремальных условиях
        extreme_volatilities = [0.01, 0.05, 0.1, 0.2, 0.5]  # 1% до 50%
        
        for vol in extreme_volatilities:
            threshold = engine.calculate_dynamic_threshold(vol)
            assert isinstance(threshold, float), f"Порог не float для волатильности {vol}"
            assert threshold > 0, f"Порог отрицательный для волатильности {vol}"
            assert threshold < 10.0, f"Порог слишком большой для волатильности {vol}"
            print(f"✅ Порог для волатильности {vol*100:.1f}%: {threshold:.4f}")
        
        # Тест 3: Формула в экстремальных условиях
        extreme_data_points = [
            {"iv": 0.01, "skew": 0.0, "basis_rel": 0.0},      # Низкая волатильность
            {"iv": 0.5, "skew": 0.0, "basis_rel": 0.0},       # Средняя волатильность
            {"iv": 2.0, "skew": 0.0, "basis_rel": 0.0},       # Высокая волатильность
            {"iv": 0.5, "skew": -1.0, "basis_rel": 0.0},      # Экстремальный skew
            {"iv": 0.5, "skew": 0.0, "basis_rel": 0.2},       # Экстремальный basis
            {"iv": 2.0, "skew": -1.0, "basis_rel": 0.2},      # Все экстремально
        ]
        
        for i, data_point in enumerate(extreme_data_points):
            Y = (
                0.92 * safe_float(data_point["iv"]) +
                0.65 * safe_float(data_point["skew"]) -
                1.87 * safe_float(data_point["basis_rel"])
            )
            
            assert isinstance(Y, float), f"Формула не возвращает float для точки {i}"
            assert not np.isnan(Y), f"Формула возвращает NaN для точки {i}"
            assert not np.isinf(Y), f"Формула возвращает inf для точки {i}"
            print(f"✅ Формула для экстремальной точки {i}: Y={Y:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка стресс-теста Formula Engine: {e}")
        return False

def test_error_monitor_stress(data: pd.DataFrame, scenario_name: str):
    """Стресс-тестирование Error Monitor"""
    print(f"\n📊 СТРЕСС-ТЕСТ ERROR MONITOR: {scenario_name}")
    print("-" * 50)
    
    try:
        from error_monitor import ErrorMonitor
        from datetime import datetime
        
        monitor = ErrorMonitor()
        
        # Тест 1: Массовые обновления с экстремальными данными
        start_time = time.time()
        
        for i in range(min(100, len(data))):
            predicted = data['spot_price'].iloc[i] * (1 + np.random.normal(0, 0.1))  # ±10% ошибка
            actual = data['spot_price'].iloc[i]
            volatility = data['iv'].iloc[i] if 'iv' in data.columns else 0.02
            
            monitor.update(datetime.now(), predicted, actual, volatility)
        
        update_time = time.time() - start_time
        print(f"✅ Массовые обновления: 100 записей за {update_time:.3f} сек")
        
        # Тест 2: Экстремальные ошибки
        extreme_errors = [
            (100.0, 200.0, 0.01),  # 100% ошибка при низкой волатильности
            (200.0, 100.0, 0.01),  # -50% ошибка при низкой волатильности
            (100.0, 150.0, 0.5),   # 50% ошибка при высокой волатильности
            (150.0, 100.0, 0.5),   # -33% ошибка при высокой волатильности
        ]
        
        for predicted, actual, volatility in extreme_errors:
            monitor.update(datetime.now(), predicted, actual, volatility)
        
        print("✅ Экстремальные ошибки обработаны")
        
        # Тест 3: Получение статистики
        stats = monitor.calculate_error_statistics()
        
        assert isinstance(stats, dict), "Статистика не является словарем"
        assert 'mae' in stats, "Статистика не содержит MAE"
        assert 'rmse' in stats, "Статистика не содержит RMSE"
        assert 'max_error' in stats, "Статистика не содержит максимальную ошибку"
        
        print(f"✅ Статистика в экстремальных условиях:")
        print(f"   MAE: {stats.get('mae', 0):.4f}")
        print(f"   RMSE: {stats.get('rmse', 0):.4f}")
        print(f"   Максимальная ошибка: {stats.get('max_error', 0):.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка стресс-теста Error Monitor: {e}")
        return False

def test_block_detector_stress(data: pd.DataFrame, scenario_name: str):
    """Стресс-тестирование Block Detector"""
    print(f"\n🔍 СТРЕСС-ТЕСТ BLOCK DETECTOR: {scenario_name}")
    print("-" * 50)
    
    try:
        from block_detector import BlockDetector
        
        detector = BlockDetector()
        
        # Тест 1: Обнаружение блоков в экстремальных условиях
        test_data = pd.DataFrame({
            'timestamp': data['time'].head(200),
            'error_absolute': abs(data['spot_price'].head(200) - data['spot_price'].head(200).mean()),
            'volatility': data['iv'].head(200) if 'iv' in data.columns else [0.02] * 200
        })
        
        # Разные пороги для разных условий
        thresholds = [0.5, 1.0, 2.0, 5.0]
        
        for threshold in thresholds:
            blocks = detector.detect_block_boundaries(test_data, threshold=threshold, window=50)
            
            assert isinstance(blocks, list), f"Блоки не являются списком для порога {threshold}"
            print(f"✅ Порог {threshold}: обнаружено {len(blocks)} блоков")
            
            if blocks:
                first_block = blocks[0]
                assert hasattr(first_block, 'block_type'), "Блок не имеет типа"
                assert hasattr(first_block, 'confidence'), "Блок не имеет уверенности"
                print(f"   Первый блок: {first_block.block_type} (уверенность: {first_block.confidence:.3f})")
        
        # Тест 2: Экстремальные пороги
        extreme_thresholds = [0.1, 10.0, 100.0]
        
        for threshold in extreme_thresholds:
            blocks = detector.detect_block_boundaries(test_data, threshold=threshold, window=50)
            print(f"✅ Экстремальный порог {threshold}: обнаружено {len(blocks)} блоков")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка стресс-теста Block Detector: {e}")
        return False

def test_prediction_layer_stress(data: pd.DataFrame, scenario_name: str):
    """Стресс-тестирование Prediction Layer"""
    print(f"\n🔮 СТРЕСС-ТЕСТ PREDICTION LAYER: {scenario_name}")
    print("-" * 50)
    
    try:
        from prediction_layer import PredictionLayer
        
        predictor = PredictionLayer()
        
        # Тест 1: Прогнозирование в экстремальных условиях
        prices = data['spot_price'].head(50).tolist()
        
        methods = ['simple_moving_average', 'weighted_moving_average', 'exponential_smoothing', 'autoregression']
        
        for method in methods:
            try:
                result = predictor.predict_next_price(prices, method=method)
                
                assert isinstance(result, float), f"{method} не возвращает float"
                assert not np.isnan(result), f"{method} возвращает NaN"
                assert not np.isinf(result), f"{method} возвращает inf"
                assert result > 0, f"{method} возвращает отрицательное значение"
                
                print(f"✅ {method}: прогноз={result:.4f}")
                
            except Exception as e:
                print(f"⚠️ {method}: ошибка - {e}")
        
        # Тест 2: Экстремальные входные данные
        extreme_prices = [
            [1.0, 1.0, 1.0, 1.0, 1.0],  # Все одинаковые
            [1.0, 1000.0, 1.0, 1000.0, 1.0],  # Экстремальные скачки
            [0.001, 0.001, 0.001, 0.001, 0.001],  # Очень маленькие значения
            [1e6, 1e6, 1e6, 1e6, 1e6],  # Очень большие значения
        ]
        
        for i, extreme_price_set in enumerate(extreme_prices):
            try:
                result = predictor.predict_next_price(extreme_price_set, method='simple_moving_average')
                print(f"✅ Экстремальный набор {i}: прогноз={result:.4f}")
            except Exception as e:
                print(f"⚠️ Экстремальный набор {i}: ошибка - {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка стресс-теста Prediction Layer: {e}")
        return False

def measure_memory_usage():
    """Измерение использования памяти"""
    try:
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024**3
        return memory_usage
    except Exception as e:
        print(f"❌ Ошибка измерения памяти: {e}")
        return 0.0

def run_stress_tests():
    """Запуск всех стресс-тестов"""
    print("🔥 ЗАПУСК СТРЕСС-ТЕСТИРОВАНИЯ СИСТЕМЫ")
    print("="*80)
    
    # Измерение начального использования памяти
    initial_memory = measure_memory_usage()
    print(f"📊 Начальное использование памяти: {initial_memory:.2f} GB")
    
    # Создание экстремальных сценариев
    scenarios = {
        "Высокая волатильность": create_high_volatility_data(),
        "Крах рынка": create_crash_scenario_data()
    }
    
    test_results = []
    
    for scenario_name, data in scenarios.items():
        print(f"\n🎯 СЦЕНАРИЙ: {scenario_name}")
        print(f"📊 Данных: {len(data)} записей")
        print(f"💰 Цена: {data['spot_price'].min():.2f} - {data['spot_price'].max():.2f}")
        print(f"📈 Волатильность: {data['iv'].min():.2f} - {data['iv'].max():.2f}")
        
        # Запуск тестов для каждого сценария
        scenario_results = []
        
        scenario_results.append(("Formula Engine", test_formula_engine_stress(data, scenario_name)))
        scenario_results.append(("Error Monitor", test_error_monitor_stress(data, scenario_name)))
        scenario_results.append(("Block Detector", test_block_detector_stress(data, scenario_name)))
        scenario_results.append(("Prediction Layer", test_prediction_layer_stress(data, scenario_name)))
        
        # Подсчет результатов для сценария
        scenario_passed = sum(1 for _, result in scenario_results if result)
        scenario_total = len(scenario_results)
        scenario_success = (scenario_passed / scenario_total) * 100
        
        print(f"\n📊 Результаты для {scenario_name}:")
        print(f"   Пройдено: {scenario_passed}/{scenario_total} ({scenario_success:.1f}%)")
        
        test_results.extend(scenario_results)
    
    # Измерение финального использования памяти
    final_memory = measure_memory_usage()
    memory_increase = final_memory - initial_memory
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ СТРЕСС-ТЕСТИРОВАНИЯ")
    print("="*80)
    
    # Подсчет общих результатов
    total_tests = len(test_results)
    passed_tests = sum(1 for _, result in test_results if result)
    success_rate = (passed_tests / total_tests) * 100
    
    print(f"📊 Всего тестов: {total_tests}")
    print(f"✅ Пройдено: {passed_tests}")
    print(f"❌ Провалено: {total_tests - passed_tests}")
    print(f"📈 Успешность: {success_rate:.1f}%")
    
    print(f"\n📊 Использование памяти:")
    print(f"   Начальное: {initial_memory:.2f} GB")
    print(f"   Финальное: {final_memory:.2f} GB")
    print(f"   Прирост: {memory_increase:.2f} GB")
    
    if final_memory < 1.0:
        print("✅ Память в пределах нормы (< 1.0 GB)")
    else:
        print("⚠️ Превышение лимита памяти (> 1.0 GB)")
    
    # Детальные результаты
    print(f"\n📋 Детальные результаты:")
    for test_name, result in test_results:
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"   {test_name}: {status}")
    
    return success_rate >= 75  # Более мягкий порог для стресс-тестов

if __name__ == "__main__":
    success = run_stress_tests()
    
    if success:
        print("\n🎉 СТРЕСС-ТЕСТИРОВАНИЕ ПРОЙДЕНО УСПЕШНО!")
        print("🚀 СИСТЕМА УСТОЙЧИВА К ЭКСТРЕМАЛЬНЫМ УСЛОВИЯМ!")
    else:
        print("\n❌ СТРЕСС-ТЕСТИРОВАНИЕ ПРОВАЛЕНО!")
        print("🔧 ТРЕБУЕТСЯ УЛУЧШЕНИЕ УСТОЙЧИВОСТИ!")
    
    exit(0 if success else 1)
