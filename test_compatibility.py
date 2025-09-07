#!/usr/bin/env python3
"""
Тесты совместимости для модуля compatibility.py
Проверяет корректную работу с NumPy 2.3.2 в Python 3.13
"""

import sys
import numpy as np
import pandas as pd
import unittest
from datetime import datetime
import logging

# Добавляем путь к модулям
sys.path.append('.')

from compatibility import (
    safe_float, safe_mean, safe_std, safe_array, 
    safe_divide, safe_sqrt, safe_log, validate_numpy_compatibility
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestCompatibility(unittest.TestCase):
    """Тесты совместимости с NumPy 2.3.2"""
    
    def setUp(self):
        """Настройка тестов"""
        self.test_values = [
            1.0, 2.5, -3.0, 0.0, 100.0,
            np.float32(1.5), np.float64(2.5),
            np.int32(10), np.int64(20),
            None, np.nan, np.inf, -np.inf,
            '5.0', 'invalid', '', 'nan',
            [1, 2, 3], (4, 5, 6),
            pd.Series([1.0, 2.0, 3.0])
        ]
    
    def test_safe_float_scalar_types(self):
        """Тест 1: Обработка скалярных типов NumPy"""
        logger.info("🧪 Тест 1: Обработка скалярных типов NumPy")
        
        test_cases = [
            (1.0, 1.0),
            (np.float32(2.5), 2.5),
            (np.float64(3.5), 3.5),
            (np.int32(10), 10.0),
            (np.int64(20), 20.0),
            (None, 0.0),
            (np.nan, 0.0),
            (np.inf, 0.0),
            (-np.inf, 0.0),
            ('5.0', 5.0),
            ('invalid', 0.0),
            ('', 0.0),
            ('nan', 0.0)
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = safe_float(input_val)
                self.assertIsInstance(result, float)
                self.assertEqual(result, expected)
        
        logger.info("✅ Тест 1: Обработка скалярных типов NumPy → PASSED")
    
    def test_safe_float_collections(self):
        """Тест 2: Обработка коллекций"""
        logger.info("🧪 Тест 2: Обработка коллекций")
        
        # Тест списков
        result = safe_float([1, 2, 3])
        self.assertEqual(result, 1.0)
        
        # Тест кортежей
        result = safe_float((4, 5, 6))
        self.assertEqual(result, 4.0)
        
        # Тест numpy массивов
        result = safe_float(np.array([7, 8, 9]))
        self.assertEqual(result, 7.0)
        
        # Тест pandas Series
        result = safe_float(pd.Series([10, 11, 12]))
        self.assertEqual(result, 10.0)
        
        # Тест пустых коллекций
        result = safe_float([])
        self.assertEqual(result, 0.0)
        
        logger.info("✅ Тест 2: Обработка коллекций → PASSED")
    
    def test_safe_mean_empty_lists(self):
        """Тест 3: Работа с пустыми списками"""
        logger.info("🧪 Тест 3: Работа с пустыми списками")
        
        # Пустой список
        result = safe_mean([])
        self.assertEqual(result, 0.0)
        
        # None
        result = safe_mean(None)
        self.assertEqual(result, 0.0)
        
        # Список с None
        result = safe_mean([None, None, None])
        self.assertEqual(result, 0.0)
        
        # Список с NaN
        result = safe_mean([np.nan, np.nan, np.nan])
        self.assertEqual(result, 0.0)
        
        logger.info("✅ Тест 3: Работа с пустыми списками → PASSED")
    
    def test_safe_mean_normal_cases(self):
        """Тест 4: Нормальные случаи вычисления среднего"""
        logger.info("🧪 Тест 4: Нормальные случаи вычисления среднего")
        
        # Обычный список
        result = safe_mean([1, 2, 3, 4, 5])
        self.assertAlmostEqual(result, 3.0, places=5)
        
        # Смешанные типы
        result = safe_mean([1.0, np.float32(2.0), np.int32(3)])
        self.assertAlmostEqual(result, 2.0, places=5)
        
        # Pandas Series
        result = safe_mean(pd.Series([10, 20, 30]))
        self.assertAlmostEqual(result, 20.0, places=5)
        
        # Numpy array
        result = safe_mean(np.array([100, 200, 300]))
        self.assertAlmostEqual(result, 200.0, places=5)
        
        logger.info("✅ Тест 4: Нормальные случаи вычисления среднего → PASSED")
    
    def test_safe_std_edge_cases(self):
        """Тест 5: Крайние случаи стандартного отклонения"""
        logger.info("🧪 Тест 5: Крайние случаи стандартного отклонения")
        
        # Пустой список
        result = safe_std([])
        self.assertEqual(result, 0.0)
        
        # Один элемент
        result = safe_std([5])
        self.assertEqual(result, 0.0)
        
        # Два элемента
        result = safe_std([1, 3])
        self.assertAlmostEqual(result, 1.0, places=5)
        
        # Смешанные типы
        result = safe_std([1.0, np.float32(2.0), np.int32(3)])
        self.assertGreater(result, 0.0)
        
        logger.info("✅ Тест 5: Крайние случаи стандартного отклонения → PASSED")
    
    def test_safe_array_creation(self):
        """Тест 6: Создание безопасных массивов"""
        logger.info("🧪 Тест 6: Создание безопасных массивов")
        
        # Обычный список
        result = safe_array([1, 2, 3])
        self.assertIsInstance(result, np.ndarray)
        self.assertEqual(result.dtype, np.float32)
        self.assertEqual(len(result), 3)
        
        # Смешанные типы
        result = safe_array([1.0, np.float32(2.0), '3.0'])
        self.assertIsInstance(result, np.ndarray)
        self.assertEqual(len(result), 3)
        
        # Пустой список
        result = safe_array([])
        self.assertIsInstance(result, np.ndarray)
        self.assertEqual(len(result), 0)
        
        logger.info("✅ Тест 6: Создание безопасных массивов → PASSED")
    
    def test_mathematical_operations(self):
        """Тест 7: Математические операции"""
        logger.info("🧪 Тест 7: Математические операции")
        
        # Безопасное деление
        result = safe_divide(10, 2)
        self.assertEqual(result, 5.0)
        
        result = safe_divide(10, 0)
        self.assertEqual(result, 0.0)
        
        # Безопасный квадратный корень
        result = safe_sqrt(16)
        self.assertEqual(result, 4.0)
        
        result = safe_sqrt(-4)
        self.assertEqual(result, 0.0)
        
        # Безопасный логарифм
        result = safe_log(np.e)
        self.assertAlmostEqual(result, 1.0, places=5)
        
        result = safe_log(0)
        self.assertEqual(result, 0.0)
        
        logger.info("✅ Тест 7: Математические операции → PASSED")
    
    def test_numpy_compatibility(self):
        """Тест 8: Проверка совместимости с NumPy"""
        logger.info("🧪 Тест 8: Проверка совместимости с NumPy")
        
        # Проверка версии
        numpy_version = np.__version__
        major_version = int(numpy_version.split('.')[0])
        
        self.assertGreaterEqual(major_version, 2, 
                              f"Требуется NumPy 2.x, установлена {numpy_version}")
        
        # Проверка функций совместимости
        is_compatible = validate_numpy_compatibility()
        self.assertTrue(is_compatible, "Проверка совместимости не прошла")
        
        logger.info("✅ Тест 8: Проверка совместимости с NumPy → PASSED")
    
    def test_memory_efficiency(self):
        """Тест 9: Эффективность использования памяти"""
        logger.info("🧪 Тест 9: Эффективность использования памяти")
        
        # Создание большого массива
        large_data = list(range(10000))
        
        # Проверка использования float32
        result = safe_array(large_data)
        self.assertEqual(result.dtype, np.float32)
        
        # Проверка вычислений
        mean_result = safe_mean(large_data)
        std_result = safe_std(large_data)
        
        self.assertIsInstance(mean_result, float)
        self.assertIsInstance(std_result, float)
        
        logger.info("✅ Тест 9: Эффективность использования памяти → PASSED")
    
    def test_error_handling(self):
        """Тест 10: Обработка ошибок"""
        logger.info("🧪 Тест 10: Обработка ошибок")
        
        # Тест с некорректными данными
        problematic_values = [
            object(),  # Неподдерживаемый тип
            complex(1, 2),  # Комплексное число
            {'key': 'value'},  # Словарь
        ]
        
        for val in problematic_values:
            result = safe_float(val)
            self.assertIsInstance(result, float)
            self.assertEqual(result, 0.0)
        
        logger.info("✅ Тест 10: Обработка ошибок → PASSED")

def run_compatibility_tests():
    """Запуск всех тестов совместимости"""
    logger.info("🎯 Запуск тестов совместимости с NumPy 2.3.2")
    logger.info(f"📅 Время: {datetime.now()}")
    
    # Создание тестового набора
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCompatibility)
    
    # Запуск тестов
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Подсчет результатов
    total_tests = result.testsRun
    failed_tests = len(result.failures)
    error_tests = len(result.errors)
    passed_tests = total_tests - failed_tests - error_tests
    
    success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    logger.info("📊 Итоговый отчет тестирования совместимости:")
    logger.info(f"   Всего тестов: {total_tests}")
    logger.info(f"   Пройдено: {passed_tests}")
    logger.info(f"   Провалено: {failed_tests}")
    logger.info(f"   Ошибок: {error_tests}")
    logger.info(f"   Успешность: {success_rate:.1f}%")
    
    if success_rate >= 90:
        logger.info("🎉 ТЕСТЫ СОВМЕСТИМОСТИ ПРОЙДЕНЫ!")
        return True
    else:
        logger.error("❌ ТЕСТЫ СОВМЕСТИМОСТИ ПРОВАЛЕНЫ!")
        return False

if __name__ == "__main__":
    success = run_compatibility_tests()
    sys.exit(0 if success else 1)

