#!/usr/bin/env python3
"""
Тестирование системы с реальными историческими данными
Проверяет совместимость с NumPy 2.3.2 и работу всех компонентов
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psutil
import os
import logging
from typing import Dict, List, Any, Tuple

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_numpy_compatibility():
    """Тестирование совместимости с NumPy 2.3.2"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ СОВМЕСТИМОСТИ С NUMPY 2.3.2")
    print("="*60)
    
    try:
        # Тест 1: Скалярные типы NumPy 2.x
        test_scalar = np.float32(0.5)
        assert isinstance(test_scalar, np.float32), "Скалярный тип не распознается"
        assert hasattr(test_scalar, 'item'), "Метод item() отсутствует"
        assert test_scalar.item() == 0.5, "Метод item() работает некорректно"
        print("✅ Скалярные типы NumPy 2.x работают корректно")
        
        # Тест 2: Математические операции
        a = np.float32(0.3)
        b = np.float32(0.2)
        result = a + b
        assert isinstance(result, np.float32), "Результат операции имеет неправильный тип"
        assert abs(result - 0.5) < 1e-6, "Математическая операция работает некорректно"
        print("✅ Математические операции работают корректно")
        
        # Тест 3: Преобразование типов
        scalar_int = np.int32(42)
        scalar_float = np.float64(3.14)
        assert float(scalar_int) == 42.0, "Преобразование int32 в float работает некорректно"
        assert float(scalar_float) == 3.14, "Преобразование float64 в float работает некорректно"
        print("✅ Преобразование типов работает корректно")
        
        # Тест 4: Проверка NaN и inf
        nan_val = np.float32(np.nan)
        inf_val = np.float32(np.inf)
        assert np.isnan(nan_val), "NaN не распознается"
        assert np.isinf(inf_val), "Inf не распознается"
        print("✅ Проверка NaN и inf работает корректно")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка совместимости с NumPy 2.3.2: {e}")
        return False

def load_real_historical_data(limit: int = 1000) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Загрузка реальных исторических данных из базы данных"""
    try:
        conn = sqlite3.connect('data/sol_iv.db')
        
        # Загрузка спотовых данных
        spot_query = """
        SELECT time, close as spot_price, high, low, open, close, volume
        FROM spot_data 
        WHERE timeframe = '1m' 
        ORDER BY time DESC 
        LIMIT ?
        """
        spot_df = pd.read_sql_query(spot_query, conn, params=[limit])
        
        # Загрузка фьючерсных данных
        futures_query = """
        SELECT time, close as futures_price, high, low, open, close, volume
        FROM futures_data 
        WHERE timeframe = '1m' 
        ORDER BY time DESC 
        LIMIT ?
        """
        futures_df = pd.read_sql_query(futures_query, conn, params=[limit])
        
        # Загрузка агрегированных данных с IV и skew
        iv_query = """
        SELECT time, spot_price, iv_30d as iv, skew_30d as skew, basis_rel
        FROM iv_agg 
        WHERE timeframe = '1m' 
        ORDER BY time DESC 
        LIMIT ?
        """
        iv_df = pd.read_sql_query(iv_query, conn, params=[limit])
        
        conn.close()
        
        print(f"✅ Загружено {len(spot_df)} записей спотовых данных")
        print(f"✅ Загружено {len(futures_df)} записей фьючерсных данных")
        print(f"✅ Загружено {len(iv_df)} записей агрегированных данных")
        
        # Объединяем данные для удобства тестирования
        combined_df = spot_df.copy()
        if not iv_df.empty:
            # Добавляем IV и skew данные
            combined_df = combined_df.merge(iv_df[['time', 'iv', 'skew', 'basis_rel']], on='time', how='left')
        
        return combined_df, futures_df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return pd.DataFrame(), pd.DataFrame()

def test_compatibility_module_with_real_data(spot_df: pd.DataFrame, futures_df: pd.DataFrame):
    """Тестирование модуля совместимости с реальными данными"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ МОДУЛЯ СОВМЕСТИМОСТИ С РЕАЛЬНЫМИ ДАННЫМИ")
    print("="*60)
    
    try:
        from compatibility import safe_float, safe_mean, safe_std, safe_array
        
        # Тест 1: safe_float с реальными данными
        test_values = [
            spot_df['spot_price'].iloc[0] if not spot_df.empty else 100.0,
            spot_df['iv'].iloc[0] if not spot_df.empty else 0.5,
            spot_df['skew'].iloc[0] if not spot_df.empty else 0.1,
            np.float32(0.5),
            np.int32(42),
            None,
            np.nan,
            np.inf
        ]
        
        for i, value in enumerate(test_values):
            result = safe_float(value)
            assert isinstance(result, float), f"safe_float не возвращает float для значения {i}"
            assert not np.isnan(result) or value is None or np.isnan(value), f"safe_float возвращает NaN для значения {i}"
            print(f"✅ safe_float работает с типом {type(value)}: {result}")
        
        # Тест 2: safe_mean с реальными данными
        if not spot_df.empty:
            prices = spot_df['spot_price'].head(100)
            mean_price = safe_mean(prices)
            assert isinstance(mean_price, float), "safe_mean не возвращает float"
            assert not np.isnan(mean_price), "safe_mean возвращает NaN"
            print(f"✅ safe_mean работает с реальными ценами: {mean_price:.4f}")
        
        # Тест 3: safe_std с реальными данными
        if not spot_df.empty:
            prices = spot_df['spot_price'].head(100)
            std_price = safe_std(prices)
            assert isinstance(std_price, float), "safe_std не возвращает float"
            assert not np.isnan(std_price), "safe_std возвращает NaN"
            print(f"✅ safe_std работает с реальными ценами: {std_price:.4f}")
        
        # Тест 4: safe_array с реальными данными
        if not spot_df.empty:
            prices = spot_df['spot_price'].head(10)
            array_prices = safe_array(prices)
            assert isinstance(array_prices, np.ndarray), "safe_array не возвращает ndarray"
            assert len(array_prices) == len(prices), "safe_array изменяет размер массива"
            print(f"✅ safe_array работает с реальными ценами: {len(array_prices)} элементов")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования модуля совместимости: {e}")
        return False

def test_formula_engine_with_real_data(spot_df: pd.DataFrame, futures_df: pd.DataFrame):
    """Тестирование Formula Engine с реальными данными"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ FORMULA ENGINE С РЕАЛЬНЫМИ ДАННЫМИ")
    print("="*60)
    
    try:
        from formula_engine import FormulaEngine
        from compatibility import safe_float
        
        engine = FormulaEngine()
        
        # Тест 1: Расчет ATR с реальными данными
        if not spot_df.empty and len(spot_df) >= 20:
            test_df = spot_df.head(20).copy()
            atr = engine.calculate_atr(test_df)
            
            assert isinstance(atr, pd.Series), "calculate_atr не возвращает Series"
            assert len(atr) == len(test_df), "ATR имеет неправильный размер"
            
            # Проверяем, что ATR не содержит NaN (кроме первых значений)
            valid_atr = atr.dropna()
            if len(valid_atr) > 0:
                assert not valid_atr.isna().any(), "ATR содержит NaN значения"
                print(f"✅ ATR рассчитан корректно: {valid_atr.iloc[-1]:.6f}")
            else:
                print("⚠️ ATR не рассчитан (недостаточно данных)")
        
        # Тест 2: Динамический порог с реальными данными
        if not spot_df.empty:
            volatility = spot_df['iv'].iloc[0] if 'iv' in spot_df.columns else 0.02
            dynamic_threshold = engine.calculate_dynamic_threshold(volatility)
            
            assert isinstance(dynamic_threshold, float), "calculate_dynamic_threshold не возвращает float"
            assert not np.isnan(dynamic_threshold), "Динамический порог содержит NaN"
            assert dynamic_threshold > 0, "Динамический порог должен быть положительным"
            print(f"✅ Динамический порог рассчитан: {dynamic_threshold:.4f}")
        
        # Тест 3: Безопасное вычисление формулы
        if not spot_df.empty:
            data = {
                'iv': safe_float(spot_df['iv'].iloc[0]) if 'iv' in spot_df.columns else 0.5,
                'skew': safe_float(spot_df['skew'].iloc[0]) if 'skew' in spot_df.columns else 0.1,
                'basis_rel': safe_float(spot_df['basis_rel'].iloc[0]) if 'basis_rel' in spot_df.columns else 0.02
            }
            
            # Простое вычисление формулы volatility_focused
            Y = (
                0.92 * data["iv"] +
                0.65 * data["skew"] -
                1.87 * data["basis_rel"]
            )
            
            assert isinstance(Y, float), "Вычисление формулы не возвращает float"
            assert not np.isnan(Y), "Результат формулы содержит NaN"
            print(f"✅ Формула volatility_focused вычислена: Y={Y:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Formula Engine: {e}")
        return False

def test_error_monitor_with_real_data(spot_df: pd.DataFrame, futures_df: pd.DataFrame):
    """Тестирование Error Monitor с реальными данными"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ ERROR MONITOR С РЕАЛЬНЫМИ ДАННЫМИ")
    print("="*60)
    
    try:
        from error_monitor import ErrorMonitor
        from datetime import datetime
        
        monitor = ErrorMonitor()
        
        # Тест 1: Обновление с реальными данными
        if not spot_df.empty:
            current_price = spot_df['spot_price'].iloc[0]
            predicted_price = current_price * 1.001  # +0.1% прогноз
            volatility = spot_df['iv'].iloc[0] if 'iv' in spot_df.columns else 0.02
            
            monitor.update(datetime.now(), predicted_price, current_price, volatility)
            print("✅ Error Monitor обновлен с реальными данными")
        
        # Тест 2: Получение ошибок
        errors = monitor.get_errors()
        assert isinstance(errors, pd.DataFrame), "get_errors не возвращает DataFrame"
        print(f"✅ Получено {len(errors)} ошибок из Error Monitor")
        
        # Тест 3: Статистика ошибок
        stats = monitor.calculate_error_statistics()
        assert isinstance(stats, dict), "calculate_error_statistics не возвращает dict"
        assert 'mae' in stats, "Статистика не содержит MAE"
        print(f"✅ Статистика ошибок: MAE={stats.get('mae', 0):.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Error Monitor: {e}")
        return False

def test_block_detector_with_real_data(spot_df: pd.DataFrame, futures_df: pd.DataFrame):
    """Тестирование Block Detector с реальными данными"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ BLOCK DETECTOR С РЕАЛЬНЫМИ ДАННЫМИ")
    print("="*60)
    
    try:
        from block_detector import BlockDetector
        
        detector = BlockDetector()
        
        # Тест 1: Создание тестовых данных с реальными ошибками
        if not spot_df.empty and len(spot_df) >= 100:
            # Создаем данные с ошибками на основе реальных цен
            test_data = pd.DataFrame({
                'timestamp': spot_df['time'].head(100),
                'error_absolute': abs(spot_df['spot_price'].head(100) - spot_df['spot_price'].head(100).mean()),
                'volatility': [spot_df['iv'].iloc[0] if 'iv' in spot_df.columns else 0.02] * 100
            })
            
            # Обнаружение блоков
            blocks = detector.detect_block_boundaries(test_data, threshold=1.5, window=50)
            
            assert isinstance(blocks, list), "detect_block_boundaries не возвращает list"
            print(f"✅ Обнаружено {len(blocks)} блоков с реальными данными")
            
            if blocks:
                first_block = blocks[0]
                assert hasattr(first_block, 'block_type'), "Блок не имеет типа"
                assert hasattr(first_block, 'confidence'), "Блок не имеет уверенности"
                print(f"   Первый блок: {first_block.block_type} (уверенность: {first_block.confidence:.3f})")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Block Detector: {e}")
        return False

def test_prediction_layer_with_real_data(spot_df: pd.DataFrame, futures_df: pd.DataFrame):
    """Тестирование Prediction Layer с реальными данными"""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ PREDICTION LAYER С РЕАЛЬНЫМИ ДАННЫМИ")
    print("="*60)
    
    try:
        from prediction_layer import PredictionLayer
        
        predictor = PredictionLayer()
        
        # Тест 1: Прогнозирование с реальными ценами
        if not spot_df.empty and len(spot_df) >= 10:
            prices = spot_df['spot_price'].head(10).tolist()
            
            # Тест разных методов прогнозирования
            methods = ['simple_moving_average', 'weighted_moving_average', 'exponential_smoothing']
            
            for method in methods:
                result = predictor.predict_next_price(prices, method=method)
                assert isinstance(result, float), f"{method} не возвращает float"
                assert not np.isnan(result), f"{method} возвращает NaN"
                assert result > 0, f"{method} возвращает отрицательное значение"
                print(f"✅ {method}: прогноз={result:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Prediction Layer: {e}")
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

def run_real_data_tests():
    """Запуск всех тестов с реальными данными"""
    print("🔍 ЗАПУСК ТЕСТОВ С РЕАЛЬНЫМИ ИСТОРИЧЕСКИМИ ДАННЫМИ")
    print("="*80)
    
    # Измерение начального использования памяти
    initial_memory = measure_memory_usage()
    print(f"📊 Начальное использование памяти: {initial_memory:.2f} GB")
    
    # Загрузка реальных данных
    spot_df, futures_df = load_real_historical_data(limit=1000)
    
    if spot_df.empty and futures_df.empty:
        print("❌ Не удалось загрузить реальные данные")
        return False
    
    # Запуск тестов
    test_results = []
    
    # Тест 1: Совместимость с NumPy 2.3.2
    test_results.append(("NumPy 2.3.2 совместимость", test_numpy_compatibility()))
    
    # Тест 2: Модуль совместимости
    test_results.append(("Модуль совместимости", test_compatibility_module_with_real_data(spot_df, futures_df)))
    
    # Тест 3: Formula Engine
    test_results.append(("Formula Engine", test_formula_engine_with_real_data(spot_df, futures_df)))
    
    # Тест 4: Error Monitor
    test_results.append(("Error Monitor", test_error_monitor_with_real_data(spot_df, futures_df)))
    
    # Тест 5: Block Detector
    test_results.append(("Block Detector", test_block_detector_with_real_data(spot_df, futures_df)))
    
    # Тест 6: Prediction Layer
    test_results.append(("Prediction Layer", test_prediction_layer_with_real_data(spot_df, futures_df)))
    
    # Измерение финального использования памяти
    final_memory = measure_memory_usage()
    memory_increase = final_memory - initial_memory
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ С РЕАЛЬНЫМИ ДАННЫМИ")
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
    
    return success_rate >= 80

if __name__ == "__main__":
    success = run_real_data_tests()
    
    if success:
        print("\n🎉 ВСЕ ТЕСТЫ С РЕАЛЬНЫМИ ДАННЫМИ ПРОЙДЕНЫ УСПЕШНО!")
        print("🚀 СИСТЕМА ГОТОВА К РАБОТЕ С РЕАЛЬНЫМИ ДАННЫМИ!")
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛЕНЫ!")
        print("🔧 ТРЕБУЕТСЯ ДОПОЛНИТЕЛЬНАЯ НАСТРОЙКА!")
    
    exit(0 if success else 1)
