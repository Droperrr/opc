#!/usr/bin/env python3
"""
Тесты для Error Monitor
Проверяет корректную работу мониторинга ошибок прогнозирования
"""

import sys
import os
import unittest
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging

# Добавляем путь к модулям
sys.path.append('.')

from error_monitor import ErrorMonitor

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestErrorMonitor(unittest.TestCase):
    """Тесты для Error Monitor"""
    
    def setUp(self):
        """Настройка тестов"""
        import uuid
        self.test_db = f'test_error_monitor_{uuid.uuid4().hex[:8]}.db'
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        
        self.monitor = ErrorMonitor(self.test_db)
        
        # Тестовые данные
        self.test_timestamp = datetime.now()
        self.test_predicted = 100.5
        self.test_actual = 101.0
        self.test_volatility = 0.01
        self.test_formula_id = 'F01'
        self.test_confidence = 0.8
        self.test_method = 'simple_moving_average'
    
    def tearDown(self):
        """Очистка после тестов"""
        try:
            if os.path.exists(self.test_db):
                os.remove(self.test_db)
        except PermissionError:
            # Игнорируем ошибки доступа к файлу
            pass
    
    def test_database_initialization(self):
        """Тест 1: Инициализация базы данных"""
        logger.info("🧪 Тест 1: Инициализация базы данных")
        
        # Проверяем существование таблиц
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            
            # Проверяем таблицу error_history
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='error_history'")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Таблица error_history не создана")
            
            # Проверяем таблицу error_statistics
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='error_statistics'")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Таблица error_statistics не создана")
            
            # Проверяем индексы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]
            
            expected_indexes = ['idx_error_timestamp', 'idx_error_formula', 'idx_error_method']
            for idx in expected_indexes:
                self.assertIn(idx, indexes, f"Индекс {idx} не создан")
        
        logger.info("✅ Тест 1: Инициализация базы данных → PASSED")
    
    def test_error_update(self):
        """Тест 2: Обновление ошибок"""
        logger.info("🧪 Тест 2: Обновление ошибок")
        
        # Добавляем ошибку
        self.monitor.update(
            self.test_timestamp, self.test_predicted, self.test_actual,
            self.test_volatility, self.test_formula_id, self.test_confidence, self.test_method
        )
        
        # Проверяем, что данные сохранились
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM error_history")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, "Ошибка не сохранена в БД")
            
            # Проверяем содержимое
            cursor.execute("SELECT * FROM error_history")
            row = cursor.fetchone()
            
            self.assertIsNotNone(row, "Запись не найдена")
            self.assertEqual(row[2], self.test_formula_id, "Неверный formula_id")
            self.assertEqual(row[3], self.test_predicted, "Неверное predicted")
            self.assertEqual(row[4], self.test_actual, "Неверное actual")
            self.assertEqual(row[8], self.test_volatility, "Неверная volatility")
            self.assertEqual(row[9], self.test_confidence, "Неверная confidence")
            self.assertEqual(row[10], self.test_method, "Неверный method")
        
        logger.info("✅ Тест 2: Обновление ошибок → PASSED")
    
    def test_error_calculation(self):
        """Тест 3: Расчет ошибок"""
        logger.info("🧪 Тест 3: Расчет ошибок")
        
        # Добавляем ошибку
        self.monitor.update(
            self.test_timestamp, self.test_predicted, self.test_actual,
            self.test_volatility, self.test_formula_id, self.test_confidence, self.test_method
        )
        
        # Проверяем расчет ошибок
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT error_absolute, error_relative, error_normalized FROM error_history")
            row = cursor.fetchone()
            
            error_abs, error_rel, error_norm = row
            
            # Проверяем абсолютную ошибку
            expected_abs = abs(self.test_predicted - self.test_actual)
            self.assertAlmostEqual(error_abs, expected_abs, places=4)
            
            # Проверяем относительную ошибку
            expected_rel = expected_abs / self.test_actual
            self.assertAlmostEqual(error_rel, expected_rel, places=4)
            
            # Проверяем нормализованную ошибку
            expected_norm = expected_abs / self.test_volatility
            self.assertAlmostEqual(error_norm, expected_norm, places=4)
        
        logger.info("✅ Тест 3: Расчет ошибок → PASSED")
    
    def test_get_errors_filtering(self):
        """Тест 4: Фильтрация ошибок"""
        logger.info("🧪 Тест 4: Фильтрация ошибок")
        
        # Добавляем несколько ошибок
        timestamps = [
            datetime.now() - timedelta(hours=i)
            for i in range(5)
        ]
        
        for i, timestamp in enumerate(timestamps):
            self.monitor.update(
                timestamp, 100.0 + i, 100.5 + i, 0.01,
                f'F{i:02d}', 0.8, 'sma'
            )
        
        # Тест фильтрации по формуле
        errors_f01 = self.monitor.get_errors(formula_id='F00')
        self.assertEqual(len(errors_f01), 1, "Неверная фильтрация по формуле")
        
        # Тест фильтрации по времени
        start_time = datetime.now() - timedelta(hours=3)
        errors_recent = self.monitor.get_errors(start_time=start_time)
        self.assertGreaterEqual(len(errors_recent), 3, "Неверная фильтрация по времени")
        
        # Тест фильтрации по методу
        errors_sma = self.monitor.get_errors(method='sma')
        self.assertEqual(len(errors_sma), 5, "Неверная фильтрация по методу")
        
        logger.info("✅ Тест 4: Фильтрация ошибок → PASSED")
    
    def test_error_statistics(self):
        """Тест 5: Статистика ошибок"""
        logger.info("🧪 Тест 5: Статистика ошибок")
        
        # Добавляем тестовые данные
        test_data = [
            (100.0, 100.5, 0.5),   # Ошибка 0.5
            (101.0, 101.2, 0.2),   # Ошибка 0.2
            (102.0, 101.8, 0.2),   # Ошибка 0.2
            (103.0, 103.5, 0.5),   # Ошибка 0.5
            (104.0, 104.1, 0.1),   # Ошибка 0.1
        ]
        
        for i, (predicted, actual, volatility) in enumerate(test_data):
            timestamp = datetime.now() - timedelta(hours=i)
            self.monitor.update(
                timestamp, predicted, actual, volatility,
                'F01', 0.8, 'sma'
            )
        
        # Вычисляем статистику
        statistics = self.monitor.calculate_error_statistics('F01', 'sma', 1)
        
        # Проверяем метрики
        self.assertIn('total_predictions', statistics)
        self.assertEqual(statistics['total_predictions'], 5)
        
        self.assertIn('mae', statistics)
        expected_mae = (0.5 + 0.2 + 0.2 + 0.5 + 0.1) / 5
        self.assertAlmostEqual(statistics['mae'], expected_mae, places=4)
        
        self.assertIn('max_error', statistics)
        self.assertEqual(statistics['max_error'], 0.5)
        
        self.assertIn('min_error', statistics)
        self.assertAlmostEqual(statistics['min_error'], 0.1, places=4)
        
        logger.info("✅ Тест 5: Статистика ошибок → PASSED")
    
    def test_daily_statistics_update(self):
        """Тест 6: Обновление дневной статистики"""
        logger.info("🧪 Тест 6: Обновление дневной статистики")
        
        # Добавляем данные за сегодня
        today = datetime.now().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        
        for i in range(3):
            timestamp = start_of_day + timedelta(hours=i)
            self.monitor.update(
                timestamp, 100.0 + i, 100.5 + i, 0.01,
                'F01', 0.8, 'sma'
            )
        
        # Обновляем дневную статистику
        self.monitor.update_daily_statistics('F01', 'sma')
        
        # Проверяем, что статистика сохранилась
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM error_statistics")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, "Дневная статистика не сохранена")
            
            # Проверяем содержимое
            cursor.execute("SELECT * FROM error_statistics")
            row = cursor.fetchone()
            
            self.assertEqual(row[1], 'F01', "Неверный formula_id в статистике")
            self.assertEqual(row[2], 'sma', "Неверный method в статистике")
            self.assertEqual(row[4], 3, "Неверное количество прогнозов")
        
        logger.info("✅ Тест 6: Обновление дневной статистики → PASSED")
    
    def test_error_summary(self):
        """Тест 7: Сводка по ошибкам"""
        logger.info("🧪 Тест 7: Сводка по ошибкам")
        
        # Добавляем разнообразные данные
        formulas = ['F01', 'F02', 'F03']
        methods = ['sma', 'wma', 'ar']
        
        for i in range(9):
            timestamp = datetime.now() - timedelta(hours=i)
            formula = formulas[i % len(formulas)]
            method = methods[i % len(methods)]
            
            self.monitor.update(
                timestamp, 100.0 + i, 100.5 + i, 0.01,
                formula, 0.8, method
            )
        
        # Получаем сводку
        summary = self.monitor.get_error_summary(1)
        
        # Проверяем общую информацию
        self.assertIn('total_predictions', summary)
        self.assertEqual(summary['total_predictions'], 9)
        
        self.assertIn('unique_formulas', summary)
        self.assertEqual(summary['unique_formulas'], 3)
        
        self.assertIn('unique_methods', summary)
        self.assertEqual(summary['unique_methods'], 3)
        
        # Проверяем статистику по формулам
        self.assertIn('formula_statistics', summary)
        formula_stats = summary['formula_statistics']
        self.assertEqual(len(formula_stats), 3)
        
        # Проверяем статистику по методам
        self.assertIn('method_statistics', summary)
        method_stats = summary['method_statistics']
        self.assertEqual(len(method_stats), 3)
        
        logger.info("✅ Тест 7: Сводка по ошибкам → PASSED")
    
    def test_edge_cases(self):
        """Тест 8: Крайние случаи"""
        logger.info("🧪 Тест 8: Крайние случаи")
        
        # Тест с нулевыми значениями
        self.monitor.update(
            datetime.now(), 0.0, 0.0, 0.0, 'F01', 0.0, 'sma'
        )
        
        # Тест с очень большими значениями
        self.monitor.update(
            datetime.now(), 1e6, 1e6 + 100, 0.01, 'F01', 0.8, 'sma'
        )
        
        # Тест с отрицательными значениями
        self.monitor.update(
            datetime.now(), -100.0, -99.5, 0.01, 'F01', 0.8, 'sma'
        )
        
        # Проверяем, что все данные сохранились
        errors_df = self.monitor.get_errors()
        self.assertEqual(len(errors_df), 3, "Не все крайние случаи обработаны")
        
        # Проверяем статистику
        statistics = self.monitor.calculate_error_statistics('F01', 'sma', 1)
        self.assertIn('total_predictions', statistics)
        self.assertEqual(statistics['total_predictions'], 3)
        
        logger.info("✅ Тест 8: Крайние случаи → PASSED")
    
    def test_memory_efficiency(self):
        """Тест 9: Эффективность использования памяти"""
        logger.info("🧪 Тест 9: Эффективность использования памяти")
        
        # Добавляем много данных
        for i in range(1000):
            timestamp = datetime.now() - timedelta(minutes=i)
            self.monitor.update(
                timestamp, 100.0 + i * 0.01, 100.5 + i * 0.01, 0.01,
                f'F{i % 10:02d}', 0.8, 'sma'
            )
        
        # Проверяем, что данные сохранились
        errors_df = self.monitor.get_errors()
        self.assertEqual(len(errors_df), 1000, "Не все данные сохранены")
        
        # Проверяем производительность запросов
        import time
        start_time = time.time()
        
        statistics = self.monitor.calculate_error_statistics('F00', 'sma', 1)
        
        end_time = time.time()
        query_time = end_time - start_time
        
        # Запрос должен выполняться быстро (менее 1 секунды)
        self.assertLess(query_time, 1.0, f"Медленный запрос: {query_time:.2f}s")
        
        logger.info("✅ Тест 9: Эффективность использования памяти → PASSED")
    
    def test_data_integrity(self):
        """Тест 10: Целостность данных"""
        logger.info("🧪 Тест 10: Целостность данных")
        
        # Добавляем данные
        self.monitor.update(
            self.test_timestamp, self.test_predicted, self.test_actual,
            self.test_volatility, self.test_formula_id, self.test_confidence, self.test_method
        )
        
        # Проверяем целостность данных
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            
            # Проверяем, что все поля заполнены
            cursor.execute("SELECT * FROM error_history")
            row = cursor.fetchone()
            
            # Проверяем, что нет NULL значений в критических полях
            self.assertIsNotNone(row[1], "timestamp не может быть NULL")
            self.assertIsNotNone(row[3], "prediction не может быть NULL")
            self.assertIsNotNone(row[4], "actual не может быть NULL")
            self.assertIsNotNone(row[5], "error_absolute не может быть NULL")
            self.assertIsNotNone(row[6], "error_relative не может быть NULL")
            self.assertIsNotNone(row[7], "error_normalized не может быть NULL")
            
            # Проверяем логическую целостность
            self.assertGreaterEqual(row[5], 0, "error_absolute не может быть отрицательным")
            self.assertGreaterEqual(row[6], 0, "error_relative не может быть отрицательным")
            self.assertGreaterEqual(row[7], 0, "error_normalized не может быть отрицательным")
        
        logger.info("✅ Тест 10: Целостность данных → PASSED")

def run_error_monitor_tests():
    """Запуск всех тестов Error Monitor"""
    logger.info("🎯 Запуск тестов Error Monitor")
    logger.info(f"📅 Время: {datetime.now()}")
    
    # Создание тестового набора
    suite = unittest.TestLoader().loadTestsFromTestCase(TestErrorMonitor)
    
    # Запуск тестов
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Подсчет результатов
    total_tests = result.testsRun
    failed_tests = len(result.failures)
    error_tests = len(result.errors)
    passed_tests = total_tests - failed_tests - error_tests
    
    success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    logger.info("📊 Итоговый отчет тестирования Error Monitor:")
    logger.info(f"   Всего тестов: {total_tests}")
    logger.info(f"   Пройдено: {passed_tests}")
    logger.info(f"   Провалено: {failed_tests}")
    logger.info(f"   Ошибок: {error_tests}")
    logger.info(f"   Успешность: {success_rate:.1f}%")
    
    if success_rate >= 90:
        logger.info("🎉 ТЕСТЫ ERROR MONITOR ПРОЙДЕНЫ!")
        return True
    else:
        logger.error("❌ ТЕСТЫ ERROR MONITOR ПРОВАЛЕНЫ!")
        return False

if __name__ == "__main__":
    success = run_error_monitor_tests()
    sys.exit(0 if success else 1)
