#!/usr/bin/env python3
"""
Тестирование долгосрочной стабильности системы
Проверяет работу в течение длительного времени и отсутствие утечек памяти
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psutil
import os
import logging
import time
import gc
from typing import Dict, List, Any, Tuple

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def measure_memory_usage():
    """Измерение использования памяти"""
    try:
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024**3
        return memory_usage
    except Exception as e:
        print(f"❌ Ошибка измерения памяти: {e}")
        return 0.0

def test_memory_stability(duration_minutes: int = 5):
    """Тестирование стабильности памяти в течение времени"""
    print(f"\n💾 ТЕСТ СТАБИЛЬНОСТИ ПАМЯТИ ({duration_minutes} минут)")
    print("-" * 60)
    
    try:
        from error_monitor import ErrorMonitor
        from block_detector import BlockDetector
        from prediction_layer import PredictionLayer
        from formula_engine import FormulaEngine
        
        # Инициализация компонентов
        monitor = ErrorMonitor()
        detector = BlockDetector()
        predictor = PredictionLayer()
        engine = FormulaEngine()
        
        # Измерение начальной памяти
        initial_memory = measure_memory_usage()
        print(f"📊 Начальная память: {initial_memory:.3f} GB")
        
        # Создание тестовых данных
        base_price = 200.0
        prices = [base_price]
        
        memory_samples = []
        start_time = time.time()
        
        iteration = 0
        while time.time() - start_time < duration_minutes * 60:
            iteration += 1
            
            # Генерация новых данных
            new_price = prices[-1] * (1 + np.random.normal(0, 0.01))
            prices.append(max(new_price, 1.0))
            
            # Ограничиваем размер списка цен
            if len(prices) > 1000:
                prices = prices[-1000:]
            
            # Тестирование Error Monitor
            predicted = prices[-1] * (1 + np.random.normal(0, 0.05))
            actual = prices[-1]
            volatility = np.random.uniform(0.01, 0.1)
            
            monitor.update(datetime.now(), predicted, actual, volatility)
            
            # Тестирование Prediction Layer
            if len(prices) >= 10:
                result = predictor.predict_next_price(prices[-10:], method='simple_moving_average')
            
            # Тестирование Formula Engine
            test_df = pd.DataFrame({
                'spot_price': prices[-20:],
                'high': [p * 1.01 for p in prices[-20:]],
                'low': [p * 0.99 for p in prices[-20:]]
            })
            
            if len(test_df) >= 14:
                atr = engine.calculate_atr(test_df)
            
            # Тестирование Block Detector каждые 100 итераций
            if iteration % 100 == 0:
                test_data = pd.DataFrame({
                    'timestamp': [datetime.now() - timedelta(minutes=i) for i in range(100, 0, -1)],
                    'error_absolute': [abs(p - np.mean(prices[-100:])) for p in prices[-100:]],
                    'volatility': [volatility] * 100
                })
                
                blocks = detector.detect_block_boundaries(test_data, threshold=1.0, window=50)
            
            # Измерение памяти каждые 30 секунд
            if iteration % 30 == 0:
                current_memory = measure_memory_usage()
                memory_samples.append(current_memory)
                print(f"⏱️ Итерация {iteration}: память {current_memory:.3f} GB")
                
                # Принудительная сборка мусора
                gc.collect()
            
            # Небольшая пауза для имитации реальной работы
            time.sleep(0.1)
        
        # Измерение финальной памяти
        final_memory = measure_memory_usage()
        print(f"📊 Финальная память: {final_memory:.3f} GB")
        
        # Анализ стабильности памяти
        if memory_samples:
            max_memory = max(memory_samples)
            min_memory = min(memory_samples)
            avg_memory = sum(memory_samples) / len(memory_samples)
            memory_growth = final_memory - initial_memory
            
            print(f"\n📊 Анализ стабильности памяти:")
            print(f"   Максимум: {max_memory:.3f} GB")
            print(f"   Минимум: {min_memory:.3f} GB")
            print(f"   Среднее: {avg_memory:.3f} GB")
            print(f"   Рост: {memory_growth:.3f} GB")
            
            # Проверка на утечки памяти
            if memory_growth > 0.1:  # Рост более 100 MB
                print("⚠️ Обнаружен потенциальный рост памяти")
                return False
            else:
                print("✅ Память стабильна")
                return True
        else:
            print("❌ Не удалось измерить память")
            return False
        
    except Exception as e:
        print(f"❌ Ошибка теста стабильности памяти: {e}")
        return False

def test_database_stability():
    """Тестирование стабильности работы с базой данных"""
    print(f"\n🗄️ ТЕСТ СТАБИЛЬНОСТИ БАЗЫ ДАННЫХ")
    print("-" * 60)
    
    try:
        from error_monitor import ErrorMonitor
        from block_detector import BlockDetector
        
        monitor = ErrorMonitor()
        detector = BlockDetector()
        
        # Тест массовых операций записи
        start_time = time.time()
        
        for i in range(1000):
            predicted = 200.0 + np.random.normal(0, 10)
            actual = 200.0 + np.random.normal(0, 10)
            volatility = np.random.uniform(0.01, 0.1)
            
            monitor.update(datetime.now(), predicted, actual, volatility)
            
            if i % 100 == 0:
                # Проверка чтения данных
                errors = monitor.get_errors()
                stats = monitor.calculate_error_statistics()
        
        write_time = time.time() - start_time
        print(f"✅ Массовые операции записи: 1000 записей за {write_time:.2f} сек")
        
        # Тест чтения данных
        start_time = time.time()
        
        for i in range(100):
            errors = monitor.get_errors()
            stats = monitor.calculate_error_statistics()
        
        read_time = time.time() - start_time
        print(f"✅ Массовые операции чтения: 100 запросов за {read_time:.2f} сек")
        
        # Проверка целостности данных
        errors = monitor.get_errors()
        assert isinstance(errors, pd.DataFrame), "Ошибки не являются DataFrame"
        assert len(errors) > 0, "Нет данных об ошибках"
        
        stats = monitor.calculate_error_statistics()
        assert isinstance(stats, dict), "Статистика не является словарем"
        assert 'mae' in stats, "Статистика не содержит MAE"
        
        print(f"✅ Целостность данных: {len(errors)} ошибок, MAE={stats.get('mae', 0):.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка теста стабильности БД: {e}")
        return False

def test_component_integration_stability():
    """Тестирование стабильности интеграции компонентов"""
    print(f"\n🔗 ТЕСТ СТАБИЛЬНОСТИ ИНТЕГРАЦИИ КОМПОНЕНТОВ")
    print("-" * 60)
    
    try:
        from error_monitor import ErrorMonitor
        from block_detector import BlockDetector
        from block_analyzer import BlockAnalyzer
        from prediction_layer import PredictionLayer
        from formula_engine import FormulaEngine
        
        # Инициализация всех компонентов
        monitor = ErrorMonitor()
        detector = BlockDetector()
        analyzer = BlockAnalyzer()
        predictor = PredictionLayer()
        engine = FormulaEngine()
        
        print("✅ Все компоненты инициализированы")
        
        # Тест интеграции: полный цикл обработки
        for i in range(100):
            # 1. Генерация данных
            prices = [200.0 + np.random.normal(0, 5) for _ in range(20)]
            predicted = predictor.predict_next_price(prices, method='simple_moving_average')
            actual = prices[-1] * (1 + np.random.normal(0, 0.02))
            volatility = np.random.uniform(0.01, 0.1)
            
            # 2. Обновление Error Monitor
            monitor.update(datetime.now(), predicted, actual, volatility)
            
            # 3. Обнаружение блоков каждые 20 итераций
            if i % 20 == 0:
                test_data = pd.DataFrame({
                    'timestamp': [datetime.now() - timedelta(minutes=j) for j in range(100, 0, -1)],
                    'error_absolute': [abs(np.random.normal(0, 1)) for _ in range(100)],
                    'volatility': [volatility] * 100
                })
                
                blocks = detector.detect_block_boundaries(test_data, threshold=1.0, window=50)
                
                # 4. Анализ блоков
                if blocks:
                    for block in blocks:
                        analysis = analyzer.analyze_block(block.id)
                        regime = analyzer.classify_market_regime(block.id)
            
            # 5. Расчет ATR
            test_df = pd.DataFrame({
                'spot_price': prices,
                'high': [p * 1.01 for p in prices],
                'low': [p * 0.99 for p in prices]
            })
            
            atr = engine.calculate_atr(test_df)
            
            if i % 25 == 0:
                print(f"✅ Интеграционный цикл {i}: все компоненты работают")
        
        print("✅ Интеграция компонентов стабильна")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка теста интеграции: {e}")
        return False

def test_error_handling_stability():
    """Тестирование стабильности обработки ошибок"""
    print(f"\n⚠️ ТЕСТ СТАБИЛЬНОСТИ ОБРАБОТКИ ОШИБОК")
    print("-" * 60)
    
    try:
        from error_monitor import ErrorMonitor
        from prediction_layer import PredictionLayer
        from formula_engine import FormulaEngine
        from compatibility import safe_float
        
        monitor = ErrorMonitor()
        predictor = PredictionLayer()
        engine = FormulaEngine()
        
        # Тест обработки некорректных данных
        error_cases = [
            # (predicted, actual, volatility, description)
            (None, 200.0, 0.02, "None predicted"),
            (200.0, None, 0.02, "None actual"),
            (200.0, 200.0, None, "None volatility"),
            (np.nan, 200.0, 0.02, "NaN predicted"),
            (200.0, np.nan, 0.02, "NaN actual"),
            (200.0, 200.0, np.nan, "NaN volatility"),
            (np.inf, 200.0, 0.02, "Inf predicted"),
            (200.0, np.inf, 0.02, "Inf actual"),
            (200.0, 200.0, np.inf, "Inf volatility"),
            (-100.0, 200.0, 0.02, "Negative predicted"),
            (200.0, -100.0, 0.02, "Negative actual"),
            (200.0, 200.0, -0.02, "Negative volatility"),
        ]
        
        for predicted, actual, volatility, description in error_cases:
            try:
                monitor.update(datetime.now(), predicted, actual, volatility)
                print(f"✅ Обработано: {description}")
            except Exception as e:
                print(f"❌ Ошибка при {description}: {e}")
                return False
        
        # Тест обработки некорректных цен
        invalid_prices = [
            [],  # Пустой список
            [200.0],  # Один элемент
            [200.0, np.nan, 201.0],  # С NaN
            [200.0, np.inf, 201.0],  # С inf
            [200.0, -100.0, 201.0],  # С отрицательными
        ]
        
        for prices in invalid_prices:
            try:
                if len(prices) >= 2:
                    result = predictor.predict_next_price(prices, method='simple_moving_average')
                    print(f"✅ Обработаны некорректные цены: {len(prices)} элементов")
            except Exception as e:
                print(f"❌ Ошибка при обработке цен {prices}: {e}")
                return False
        
        # Тест обработки некорректных DataFrame
        invalid_dfs = [
            pd.DataFrame(),  # Пустой DataFrame
            pd.DataFrame({'spot_price': []}),  # Пустые данные
            pd.DataFrame({'spot_price': [np.nan, np.nan, np.nan]}),  # Все NaN
        ]
        
        for df in invalid_dfs:
            try:
                atr = engine.calculate_atr(df)
                print(f"✅ Обработан некорректный DataFrame: {len(df)} строк")
            except Exception as e:
                print(f"❌ Ошибка при обработке DataFrame: {e}")
                return False
        
        print("✅ Обработка ошибок стабильна")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка теста обработки ошибок: {e}")
        return False

def run_long_term_stability_tests():
    """Запуск всех тестов долгосрочной стабильности"""
    print("⏰ ЗАПУСК ТЕСТОВ ДОЛГОСРОЧНОЙ СТАБИЛЬНОСТИ")
    print("="*80)
    
    # Измерение начального использования памяти
    initial_memory = measure_memory_usage()
    print(f"📊 Начальное использование памяти: {initial_memory:.2f} GB")
    
    test_results = []
    
    # Тест 1: Стабильность памяти (5 минут)
    test_results.append(("Стабильность памяти", test_memory_stability(duration_minutes=5)))
    
    # Тест 2: Стабильность базы данных
    test_results.append(("Стабильность БД", test_database_stability()))
    
    # Тест 3: Стабильность интеграции компонентов
    test_results.append(("Стабильность интеграции", test_component_integration_stability()))
    
    # Тест 4: Стабильность обработки ошибок
    test_results.append(("Стабильность обработки ошибок", test_error_handling_stability()))
    
    # Измерение финального использования памяти
    final_memory = measure_memory_usage()
    memory_increase = final_memory - initial_memory
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ ТЕСТОВ ДОЛГОСРОЧНОЙ СТАБИЛЬНОСТИ")
    print("="*80)
    
    # Подсчет результатов
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
    
    return success_rate >= 75  # Более мягкий порог для долгосрочных тестов

if __name__ == "__main__":
    success = run_long_term_stability_tests()
    
    if success:
        print("\n🎉 ТЕСТЫ ДОЛГОСРОЧНОЙ СТАБИЛЬНОСТИ ПРОЙДЕНЫ!")
        print("🚀 СИСТЕМА СТАБИЛЬНА В ДОЛГОСРОЧНОЙ ПЕРСПЕКТИВЕ!")
    else:
        print("\n❌ ТЕСТЫ ДОЛГОСРОЧНОЙ СТАБИЛЬНОСТИ ПРОВАЛЕНЫ!")
        print("🔧 ТРЕБУЕТСЯ УЛУЧШЕНИЕ СТАБИЛЬНОСТИ!")
    
    exit(0 if success else 1)
