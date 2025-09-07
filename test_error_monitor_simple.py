#!/usr/bin/env python3
"""
Простой тест для Error Monitor
Проверяет основные функции без сложных тестовых сценариев
"""

import sys
import os
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging
import uuid

# Добавляем путь к модулям
sys.path.append('.')

from error_monitor import ErrorMonitor

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_basic_functionality():
    """Тест основных функций Error Monitor"""
    logger.info("🧪 Тест основных функций Error Monitor")
    
    # Создаем уникальную тестовую БД
    test_db = f'test_error_monitor_{uuid.uuid4().hex[:8]}.db'
    
    try:
        # Инициализация
        monitor = ErrorMonitor(test_db)
        logger.info("✅ ErrorMonitor инициализирован")
        
        # Тест добавления ошибки
        timestamp = datetime.now()
        monitor.update(
            timestamp, 100.5, 101.0, 0.01, 'F01', 0.8, 'sma'
        )
        logger.info("✅ Ошибка добавлена")
        
        # Проверяем сохранение в БД
        with sqlite3.connect(test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM error_history")
            count = cursor.fetchone()[0]
            
            if count == 1:
                logger.info("✅ Данные сохранены в БД")
            else:
                logger.error(f"❌ Ожидалось 1 запись, получено {count}")
                return False
        
        # Тест получения ошибок
        errors_df = monitor.get_errors()
        if len(errors_df) == 1:
            logger.info("✅ История ошибок получена")
        else:
            logger.error(f"❌ Ожидалось 1 запись, получено {len(errors_df)}")
            return False
        
        # Тест расчета статистики
        statistics = monitor.calculate_error_statistics('F01', 'sma', 1)
        if 'mae' in statistics and statistics['mae'] > 0:
            logger.info(f"✅ Статистика рассчитана: MAE={statistics['mae']:.4f}")
        else:
            logger.error("❌ Статистика не рассчитана")
            return False
        
        # Тест сводки
        summary = monitor.get_error_summary(1)
        if 'total_predictions' in summary:
            logger.info(f"✅ Сводка создана: {summary['total_predictions']} прогнозов")
        else:
            logger.error("❌ Сводка не создана")
            return False
        
        logger.info("🎉 Все основные функции работают!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        return False
        
    finally:
        # Очистка
        try:
            if os.path.exists(test_db):
                os.remove(test_db)
        except PermissionError:
            pass

def test_prediction_layer():
    """Тест Prediction Layer"""
    logger.info("🧪 Тест Prediction Layer")
    
    try:
        from prediction_layer import PredictionLayer
        
        # Создаем тестовые данные
        prices = [100.0, 101.0, 102.0, 103.0, 104.0]
        
        # Инициализация
        predictor = PredictionLayer(window_size=3)
        
        # Тест прогнозирования
        prediction = predictor.predict_next_price(prices, 'simple_moving_average')
        if prediction > 0:
            logger.info(f"✅ Прогноз создан: {prediction:.2f}")
        else:
            logger.error("❌ Прогноз не создан")
            return False
        
        # Тест уверенности
        confidence = predictor.calculate_prediction_confidence(prices, 'simple_moving_average')
        if 0 <= confidence <= 1:
            logger.info(f"✅ Уверенность рассчитана: {confidence:.3f}")
        else:
            logger.error(f"❌ Некорректная уверенность: {confidence}")
            return False
        
        logger.info("🎉 Prediction Layer работает!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования Prediction Layer: {e}")
        return False

def test_compatibility():
    """Тест модуля совместимости"""
    logger.info("🧪 Тест модуля совместимости")
    
    try:
        from compatibility import safe_float, safe_mean, safe_std, validate_numpy_compatibility
        
        # Тест совместимости
        if validate_numpy_compatibility():
            logger.info("✅ Совместимость с NumPy подтверждена")
        else:
            logger.error("❌ Проблемы с совместимостью")
            return False
        
        # Тест основных функций
        test_values = [1.0, 2.0, 3.0, None, np.nan]
        
        for val in test_values:
            result = safe_float(val)
            if isinstance(result, float):
                logger.debug(f"safe_float({val}) = {result}")
            else:
                logger.error(f"❌ safe_float({val}) вернул {type(result)}")
                return False
        
        # Тест статистических функций
        mean_result = safe_mean([1.0, 2.0, 3.0])
        if mean_result == 2.0:
            logger.info("✅ safe_mean работает корректно")
        else:
            logger.error(f"❌ safe_mean вернул {mean_result}")
            return False
        
        std_result = safe_std([1.0, 2.0, 3.0])
        if std_result > 0:
            logger.info("✅ safe_std работает корректно")
        else:
            logger.error(f"❌ safe_std вернул {std_result}")
            return False
        
        logger.info("🎉 Модуль совместимости работает!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования совместимости: {e}")
        return False

def run_simple_tests():
    """Запуск простых тестов"""
    logger.info("🎯 Запуск простых тестов S-14")
    logger.info(f"📅 Время: {datetime.now()}")
    
    tests = [
        ("Модуль совместимости", test_compatibility),
        ("Prediction Layer", test_prediction_layer),
        ("Error Monitor", test_basic_functionality)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в тесте '{test_name}': {e}")
            results[test_name] = False
    
    # Итоговый отчет
    logger.info("📊 Итоговый отчет простых тестов:")
    passed_tests = sum(results.values())
    total_tests = len(results)
    success_rate = (passed_tests / total_tests) * 100
    
    for test_name, result in results.items():
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        logger.info(f"   {test_name}: {status}")
    
    logger.info(f"📊 Общий результат: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        logger.info("🎉 ПРОСТЫЕ ТЕСТЫ ПРОЙДЕНЫ!")
        return True
    else:
        logger.error("❌ ПРОСТЫЕ ТЕСТЫ ПРОВАЛЕНЫ!")
        return False

if __name__ == "__main__":
    success = run_simple_tests()
    sys.exit(0 if success else 1)

