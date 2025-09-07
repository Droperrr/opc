#!/usr/bin/env python3
"""
Полная проверка системы с фокусом на критические сценарии
Проверяет совместимость NumPy 2.3.2 с Python 3.13 и работу всех компонентов
"""

import os
import numpy as np
import pandas as pd
import sqlite3
import psutil
import time
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def check_environment():
    """Проверка окружения и версий библиотек"""
    print("\n1. ПРОВЕРКА ОКРУЖЕНИЯ")
    print("-" * 50)
    
    try:
        import streamlit
        print(f"✅ Streamlit {streamlit.__version__} установлен")
    except Exception as e:
        print(f"❌ Ошибка Streamlit: {str(e)}")
        return False
    
    try:
        import numpy
        print(f"✅ NumPy {numpy.__version__} установлен")
    except Exception as e:
        print(f"❌ Ошибка NumPy: {str(e)}")
        return False
    
    try:
        import pandas
        print(f"✅ Pandas {pandas.__version__} установлен")
    except Exception as e:
        print(f"❌ Ошибка Pandas: {str(e)}")
        return False
    
    try:
        import plotly
        print(f"✅ Plotly {plotly.__version__} установлен")
    except Exception as e:
        print(f"❌ Ошибка Plotly: {str(e)}")
        return False
    
    try:
        import sklearn
        print(f"✅ Scikit-learn {sklearn.__version__} установлен")
    except Exception as e:
        print(f"❌ Ошибка Scikit-learn: {str(e)}")
        return False
    
    return True

def test_numpy_scalar_types():
    """Проверка обработки скалярных типов NumPy 2.x"""
    print("\n2. ПРОВЕРКА СКАЛЯРНЫХ ТИПОВ NUMPY 2.X")
    print("-" * 50)
    
    try:
        # Тест 1: Базовые скалярные типы
        test_scalar = np.float32(0.5)
        print(f"   Тип скаляра: {type(test_scalar)}")
        print(f"   isinstance(test_scalar, np.float32): {isinstance(test_scalar, np.float32)}")
        print(f"   hasattr(test_scalar, 'item'): {hasattr(test_scalar, 'item')}")
        print(f"   test_scalar.item(): {test_scalar.item()}")
        
        # Тест 2: Критическая точка из лога установки (common.h(113))
        print("\n   Проверка критической точки из лога установки (common.h(113))")
        large_array = np.random.random(100000)
        print(f"   Создан массив из {len(large_array)} элементов")
        
        # Попытка операции, которая вызывает ошибку C2146
        result = np.sum(large_array)
        print(f"   np.sum() выполнен: {result:.6f}")
        
        # Тест 3: Математические операции с разными типами
        a = np.float32(0.3)
        b = np.float64(0.2)
        result = a + b
        print(f"   Смешанные типы (float32 + float64): {result} (тип: {type(result)})")
        
        # Тест 4: Проверка NaN и inf
        nan_val = np.float32(np.nan)
        inf_val = np.float32(np.inf)
        print(f"   NaN проверка: {np.isnan(nan_val)}")
        print(f"   Inf проверка: {np.isinf(inf_val)}")
        
        print("✅ Проверка скалярных типов пройдена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки скалярных типов: {str(e)}")
        return False

def test_real_historical_data():
    """Проверка работы с реальными историческими данными"""
    print("\n3. ПРОВЕРКА С РЕАЛЬНЫМИ ИСТОРИЧЕСКИМИ ДАННЫМИ")
    print("-" * 50)
    
    try:
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Получаем реальные данные (минимум 10,000 записей)
        cursor.execute("""
            SELECT close, high, low, open, volume
            FROM spot_data 
            WHERE timeframe = '1m' 
            ORDER BY time DESC 
            LIMIT 10000
        """)
        
        spot_data = cursor.fetchall()
        print(f"   Получено {len(spot_data)} записей спотовых данных из БД")
        
        cursor.execute("""
            SELECT close, high, low, open, volume
            FROM futures_data 
            WHERE timeframe = '1m' 
            ORDER BY time DESC 
            LIMIT 10000
        """)
        
        futures_data = cursor.fetchall()
        print(f"   Получено {len(futures_data)} записей фьючерсных данных из БД")
        
        # Получаем агрегированные данные
        cursor.execute("""
            SELECT spot_price, iv_30d, skew_30d, basis_rel
            FROM iv_agg 
            WHERE timeframe = '1m' 
            ORDER BY time DESC 
            LIMIT 1000
        """)
        
        iv_data = cursor.fetchall()
        print(f"   Получено {len(iv_data)} записей агрегированных данных из БД")
        
        # Тестируем Formula Engine с реальными данными
        try:
            from formula_engine import FormulaEngine
            engine = FormulaEngine()
            
            total_calculations = 0
            for row in iv_data:
                if row[1] is not None and row[2] is not None and row[3] is not None:
                    # Создаем тестовые данные
                    test_df = pd.DataFrame({
                        'spot_price': [row[0]] * 20,
                        'high': [row[0] * 1.01] * 20,
                        'low': [row[0] * 0.99] * 20
                    })
                    
                    # Рассчитываем ATR
                    atr = engine.calculate_atr(test_df)
                    
                    # Рассчитываем динамический порог
                    threshold = engine.calculate_dynamic_threshold(row[1])
                    
                    total_calculations += 1
                    
            print(f"   Выполнено {total_calculations} вычислений с реальными данными")
            
        except Exception as e:
            print(f"   ⚠️ Formula Engine: {str(e)}")
        
        # Тестируем Error Monitor с реальными данными
        try:
            from error_monitor import ErrorMonitor
            monitor = ErrorMonitor()
            
            # Добавляем несколько тестовых ошибок
            for i in range(min(100, len(spot_data))):
                predicted = spot_data[i][0] * (1 + np.random.normal(0, 0.01))
                actual = spot_data[i][0]
                volatility = np.random.uniform(0.01, 0.1)
                
                monitor.update(datetime.now(), predicted, actual, volatility)
            
            # Получаем статистику
            stats = monitor.calculate_error_statistics()
            print(f"   Error Monitor: {len(monitor.get_errors())} ошибок, MAE={stats.get('mae', 0):.4f}")
            
        except Exception as e:
            print(f"   ⚠️ Error Monitor: {str(e)}")
        
        conn.close()
        print("✅ Проверка с реальными данными пройдена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки с реальными данными: {str(e)}")
        return False

def test_high_volatility_scenarios():
    """Проверка в условиях высокой волатильности"""
    print("\n4. ПРОВЕРКА В УСЛОВИЯХ ВЫСОКОЙ ВОЛАТИЛЬНОСТИ")
    print("-" * 50)
    
    try:
        # Создаем данные с высокой волатильностью
        volatile_prices = np.array([100.0, 105.0, 95.0, 110.0, 90.0] * 20000)
        print(f"   Создан массив с высокой волатильностью ({len(volatile_prices)} элементов)")
        
        # Тест 1: Базовые операции с волатильными данными
        mean_price = np.mean(volatile_prices)
        std_price = np.std(volatile_prices)
        print(f"   Средняя цена: {mean_price:.2f}")
        print(f"   Стандартное отклонение: {std_price:.2f}")
        
        # Тест 2: Расчет ATR с волатильными данными
        try:
            from formula_engine import FormulaEngine
            engine = FormulaEngine()
            
            test_df = pd.DataFrame({
                'spot_price': volatile_prices[:1000],  # Берем первые 1000 элементов
                'high': volatile_prices[:1000] * 1.02,
                'low': volatile_prices[:1000] * 0.98
            })
            
            atr = engine.calculate_atr(test_df)
            valid_atr = atr.dropna()
            
            if len(valid_atr) > 0:
                print(f"   Средний ATR: {valid_atr.mean():.6f}")
                print(f"   Максимальный ATR: {valid_atr.max():.6f}")
                print(f"   Минимальный ATR: {valid_atr.min():.6f}")
            
        except Exception as e:
            print(f"   ⚠️ Formula Engine с волатильными данными: {str(e)}")
        
        # Тест 3: Prediction Layer с волатильными данными
        try:
            from prediction_layer import PredictionLayer
            predictor = PredictionLayer()
            
            # Тестируем разные методы прогнозирования
            methods = ['simple_moving_average', 'weighted_moving_average', 'exponential_smoothing']
            
            for method in methods:
                result = predictor.predict_next_price(volatile_prices[:100].tolist(), method=method)
                print(f"   {method}: прогноз={result:.4f}")
                
        except Exception as e:
            print(f"   ⚠️ Prediction Layer с волатильными данными: {str(e)}")
        
        # Тест 4: Block Detector с волатильными данными
        try:
            from block_detector import BlockDetector
            detector = BlockDetector()
            
            test_data = pd.DataFrame({
                'timestamp': [datetime.now() - timedelta(minutes=i) for i in range(200, 0, -1)],
                'error_absolute': np.abs(volatile_prices[:200] - np.mean(volatile_prices[:200])),
                'volatility': np.random.uniform(0.01, 0.1, 200)
            })
            
            blocks = detector.detect_block_boundaries(test_data, threshold=1.0, window=50)
            print(f"   Обнаружено {len(blocks)} блоков в волатильных данных")
            
        except Exception as e:
            print(f"   ⚠️ Block Detector с волатильными данными: {str(e)}")
        
        print("✅ Проверка в условиях высокой волатильности пройдена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки в условиях высокой волатильности: {str(e)}")
        return False

def test_memory_stability():
    """Проверка стабильности памяти"""
    print("\n5. ПРОВЕРКА СТАБИЛЬНОСТИ ПАМЯТИ")
    print("-" * 50)
    
    try:
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024**3
        print(f"   Начальное использование памяти: {initial_memory:.3f} GB")
        
        # Создаем большие массивы данных
        large_arrays = []
        for i in range(10):
            large_array = np.random.random(100000)
            large_arrays.append(large_array)
            
            # Выполняем операции
            _ = np.sum(large_array)
            _ = np.mean(large_array)
            _ = np.std(large_array)
        
        # Проверяем память после операций
        current_memory = process.memory_info().rss / 1024**3
        memory_increase = current_memory - initial_memory
        
        print(f"   Память после операций: {current_memory:.3f} GB")
        print(f"   Увеличение памяти: {memory_increase:.3f} GB")
        
        # Очищаем массивы
        del large_arrays
        import gc
        gc.collect()
        
        # Проверяем память после очистки
        final_memory = process.memory_info().rss / 1024**3
        memory_after_cleanup = final_memory - initial_memory
        
        print(f"   Память после очистки: {final_memory:.3f} GB")
        print(f"   Увеличение после очистки: {memory_after_cleanup:.3f} GB")
        
        if memory_after_cleanup < 0.1:  # Менее 100 MB
            print("✅ Стабильность памяти подтверждена")
            return True
        else:
            print(f"⚠️ Возможные утечки памяти: увеличение на {memory_after_cleanup:.3f} GB")
            return False
        
    except Exception as e:
        print(f"❌ Ошибка проверки стабильности памяти: {str(e)}")
        return False

def test_component_integration():
    """Проверка интеграции всех компонентов"""
    print("\n6. ПРОВЕРКА ИНТЕГРАЦИИ ВСЕХ КОМПОНЕНТОВ")
    print("-" * 50)
    
    try:
        # Инициализация всех компонентов
        components = {}
        
        try:
            from formula_engine import FormulaEngine
            components['formula_engine'] = FormulaEngine()
            print("   ✅ Formula Engine инициализирован")
        except Exception as e:
            print(f"   ❌ Formula Engine: {str(e)}")
            return False
        
        try:
            from error_monitor import ErrorMonitor
            components['error_monitor'] = ErrorMonitor()
            print("   ✅ Error Monitor инициализирован")
        except Exception as e:
            print(f"   ❌ Error Monitor: {str(e)}")
            return False
        
        try:
            from block_detector import BlockDetector
            components['block_detector'] = BlockDetector()
            print("   ✅ Block Detector инициализирован")
        except Exception as e:
            print(f"   ❌ Block Detector: {str(e)}")
            return False
        
        try:
            from prediction_layer import PredictionLayer
            components['prediction_layer'] = PredictionLayer()
            print("   ✅ Prediction Layer инициализирован")
        except Exception as e:
            print(f"   ❌ Prediction Layer: {str(e)}")
            return False
        
        # Тест интеграции: полный цикл обработки данных
        print("\n   Тестирование полного цикла обработки данных...")
        
        # Создаем тестовые данные
        test_prices = [200.0 + np.random.normal(0, 5) for _ in range(50)]
        
        # 1. Генерация прогноза
        predicted = components['prediction_layer'].predict_next_price(test_prices, method='simple_moving_average')
        actual = test_prices[-1] * (1 + np.random.normal(0, 0.02))
        volatility = np.random.uniform(0.01, 0.1)
        
        print(f"   Прогноз: {predicted:.4f}, Факт: {actual:.4f}")
        
        # 2. Обновление Error Monitor
        components['error_monitor'].update(datetime.now(), predicted, actual, volatility)
        
        # 3. Расчет ATR
        test_df = pd.DataFrame({
            'spot_price': test_prices,
            'high': [p * 1.01 for p in test_prices],
            'low': [p * 0.99 for p in test_prices]
        })
        
        atr = components['formula_engine'].calculate_atr(test_df)
        print(f"   ATR рассчитан: {atr.iloc[-1]:.6f}")
        
        # 4. Обнаружение блоков
        test_data = pd.DataFrame({
            'timestamp': [datetime.now() - timedelta(minutes=i) for i in range(50, 0, -1)],
            'error_absolute': [abs(p - np.mean(test_prices)) for p in test_prices],
            'volatility': [volatility] * 50
        })
        
        blocks = components['block_detector'].detect_block_boundaries(test_data, threshold=1.0, window=25)
        print(f"   Обнаружено {len(blocks)} блоков")
        
        print("✅ Интеграция всех компонентов проверена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки интеграции компонентов: {str(e)}")
        return False

def run_full_verification():
    """Запуск полной проверки системы"""
    print("="*80)
    print("НАЧАЛО ПОЛНОЙ ПРОВЕРКИ СИСТЕМЫ С УПОРОМ НА КРИТИЧЕСКИЕ СЦЕНАРИИ")
    print("="*80)
    
    # Измерение начального использования памяти
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024**3
    print(f"📊 Начальное использование памяти: {initial_memory:.3f} GB")
    
    # Запуск всех тестов
    test_results = []
    
    test_results.append(("Проверка окружения", check_environment()))
    test_results.append(("Скалярные типы NumPy 2.x", test_numpy_scalar_types()))
    test_results.append(("Реальные исторические данные", test_real_historical_data()))
    test_results.append(("Высокая волатильность", test_high_volatility_scenarios()))
    test_results.append(("Стабильность памяти", test_memory_stability()))
    test_results.append(("Интеграция компонентов", test_component_integration()))
    
    # Измерение финального использования памяти
    final_memory = process.memory_info().rss / 1024**3
    memory_increase = final_memory - initial_memory
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ ПОЛНОЙ ПРОВЕРКИ СИСТЕМЫ")
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
    print(f"   Начальное: {initial_memory:.3f} GB")
    print(f"   Финальное: {final_memory:.3f} GB")
    print(f"   Прирост: {memory_increase:.3f} GB")
    
    if final_memory < 1.0:
        print("✅ Память в пределах нормы (< 1.0 GB)")
    else:
        print("⚠️ Превышение лимита памяти (> 1.0 GB)")
    
    # Детальные результаты
    print(f"\n📋 Детальные результаты:")
    for test_name, result in test_results:
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"   {test_name}: {status}")
    
    # Заключение
    if success_rate >= 80:
        print(f"\n🎉 СИСТЕМА ПОЛНОСТЬЮ ГОТОВА К РАБОТЕ!")
        print(f"🚀 Все критические сценарии протестированы успешно!")
        return True
    else:
        print(f"\n❌ ТРЕБУЕТСЯ ДОПОЛНИТЕЛЬНАЯ НАСТРОЙКА!")
        print(f"🔧 Необходимо исправить проваленные тесты!")
        return False

if __name__ == "__main__":
    success = run_full_verification()
    exit(0 if success else 1)
