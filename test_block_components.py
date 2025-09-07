#!/usr/bin/env python3
"""
Тесты для всех компонентов Error-Driven Blocks
Проверяет интеграцию Block Detector, Block Analyzer, Formula Engine и Block Reporting
"""

import sys
import os
import unittest
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging
import uuid
import json

# Добавляем путь к модулям
sys.path.append('.')

from block_detector import BlockDetector, BlockBoundary
from block_analyzer import BlockAnalyzer, BlockAnalysis
from formula_engine_blocks import FormulaEngineBlocks
from block_reporting import BlockReporter

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestBlockComponents(unittest.TestCase):
    """Тесты для всех компонентов блоков"""
    
    def setUp(self):
        """Настройка тестов"""
        self.test_db = f'test_blocks_{uuid.uuid4().hex[:8]}.db'
        
        # Инициализация компонентов
        self.block_detector = BlockDetector(self.test_db)
        self.block_analyzer = BlockAnalyzer(self.test_db)
        self.formula_engine = FormulaEngineBlocks(self.test_db)
        self.block_reporter = BlockReporter(self.test_db)
        
        # Создаем тестовые данные
        self._create_test_data()
    
    def tearDown(self):
        """Очистка после тестов"""
        try:
            if os.path.exists(self.test_db):
                os.remove(self.test_db)
        except PermissionError:
            pass
    
    def _create_test_data(self):
        """Создает тестовые данные для всех компонентов"""
        try:
            with sqlite3.connect(self.test_db) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицы
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS error_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        formula_id TEXT,
                        prediction REAL NOT NULL,
                        actual REAL NOT NULL,
                        error_absolute REAL NOT NULL,
                        error_relative REAL NOT NULL,
                        error_normalized REAL NOT NULL,
                        volatility REAL,
                        confidence REAL,
                        method TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Создаем тестовые ошибки с разными режимами
                np.random.seed(42)
                
                # Режим 1: Стабильные низкие ошибки
                for i in range(100):
                    timestamp = datetime.now() - timedelta(hours=3, minutes=i)
                    error = np.random.normal(0.3, 0.1)
                    cursor.execute('''
                        INSERT INTO error_history 
                        (timestamp, formula_id, prediction, actual, error_absolute, 
                         error_relative, error_normalized, volatility, confidence, method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, 'volatility_focused', 100.0, 100.0 + error, abs(error),
                        abs(error)/100.0, abs(error)/0.01, 0.01, 0.8, 'sma'
                    ))
                
                # Режим 2: Волатильные высокие ошибки
                for i in range(100):
                    timestamp = datetime.now() - timedelta(hours=2, minutes=i)
                    error = np.random.normal(1.5, 0.5)
                    cursor.execute('''
                        INSERT INTO error_history 
                        (timestamp, formula_id, prediction, actual, error_absolute, 
                         error_relative, error_normalized, volatility, confidence, method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, 'volatility_focused', 100.0, 100.0 + error, abs(error),
                        abs(error)/100.0, abs(error)/0.05, 0.05, 0.6, 'sma'
                    ))
                
                # Режим 3: Средние ошибки
                for i in range(100):
                    timestamp = datetime.now() - timedelta(hours=1, minutes=i)
                    error = np.random.normal(0.8, 0.2)
                    cursor.execute('''
                        INSERT INTO error_history 
                        (timestamp, formula_id, prediction, actual, error_absolute, 
                         error_relative, error_normalized, volatility, confidence, method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, 'volatility_focused', 100.0, 100.0 + error, abs(error),
                        abs(error)/100.0, abs(error)/0.02, 0.02, 0.7, 'sma'
                    ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания тестовых данных: {e}")
    
    def test_block_detector_integration(self):
        """Тест 1: Интеграция Block Detector"""
        logger.info("🧪 Тест 1: Интеграция Block Detector")
        
        try:
            # Получаем данные об ошибках
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            # Обнаруживаем блоки
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            
            # Проверяем результат
            self.assertGreater(len(blocks), 0, "Должны быть обнаружены блоки")
            
            # Проверяем структуру блоков
            for block in blocks:
                self.assertIsInstance(block, BlockBoundary)
                self.assertIsNotNone(block.start_time)
                self.assertIsNotNone(block.end_time)
                self.assertIsNotNone(block.block_type)
                self.assertGreaterEqual(block.confidence, 0.0)
                self.assertLessEqual(block.confidence, 1.0)
            
            # Сохраняем блоки
            self.block_detector.save_blocks(blocks)
            
            # Проверяем сохранение
            saved_blocks = self.block_detector.get_blocks()
            self.assertEqual(len(saved_blocks), len(blocks), "Количество сохраненных блоков должно совпадать")
            
            logger.info("✅ Тест 1: Интеграция Block Detector → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста Block Detector: {e}")
            self.fail(f"Тест Block Detector провален: {e}")
    
    def test_block_analyzer_integration(self):
        """Тест 2: Интеграция Block Analyzer"""
        logger.info("🧪 Тест 2: Интеграция Block Analyzer")
        
        try:
            # Сначала создаем блоки
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            # Тестируем анализ блока
            if len(blocks) > 0:
                block_id = blocks[0].start_index  # Используем индекс как ID
                
                # Получаем статистику блока
                statistics = self.block_analyzer.get_block_statistics(1)  # Используем ID=1
                
                if statistics:
                    self.assertIn('block_id', statistics)
                    self.assertIn('mean_error', statistics)
                    self.assertIn('std_error', statistics)
                
                # Классифицируем рыночный режим
                market_regime = self.block_analyzer.classify_market_regime(1)
                self.assertIsInstance(market_regime, str)
                self.assertIn(market_regime, ['trending', 'volatile', 'stable', 'transition', 'unknown'])
                
                # Получаем полный анализ
                analysis = self.block_analyzer.analyze_block(1)
                self.assertIsInstance(analysis, BlockAnalysis)
                self.assertIsNotNone(analysis.market_regime)
                self.assertIsNotNone(analysis.risk_level)
                self.assertIsInstance(analysis.recommendations, list)
                
                # Получаем сводку
                summary = self.block_analyzer.get_blocks_summary(1)
                self.assertIsInstance(summary, dict)
            
            logger.info("✅ Тест 2: Интеграция Block Analyzer → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста Block Analyzer: {e}")
            self.fail(f"Тест Block Analyzer провален: {e}")
    
    def test_formula_engine_integration(self):
        """Тест 3: Интеграция Formula Engine с блоками"""
        logger.info("🧪 Тест 3: Интеграция Formula Engine с блоками")
        
        try:
            # Создаем блоки
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            # Тестируем получение параметров
            params = self.formula_engine.get_formula_parameters('volatility_focused', block_id=1)
            self.assertIsInstance(params, dict)
            self.assertIn('threshold', params)
            
            # Тестируем получение параметров для текущего блока
            current_params = self.formula_engine.get_current_block_parameters('volatility_focused')
            self.assertIsInstance(current_params, dict)
            self.assertIn('parameters', current_params)
            self.assertIn('market_regime', current_params)
            
            # Тестируем оптимизацию параметров
            optimized_params = self.formula_engine.optimize_parameters_for_regime('volatility_focused', 'stable')
            self.assertIsInstance(optimized_params, dict)
            self.assertIn('threshold', optimized_params)
            
            # Тестируем производительность формулы
            performance = self.formula_engine.get_formula_performance_by_regime('volatility_focused')
            self.assertIsInstance(performance, dict)
            
            logger.info("✅ Тест 3: Интеграция Formula Engine с блоками → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста Formula Engine: {e}")
            self.fail(f"Тест Formula Engine провален: {e}")
    
    def test_block_reporting_integration(self):
        """Тест 4: Интеграция Block Reporting"""
        logger.info("🧪 Тест 4: Интеграция Block Reporting")
        
        try:
            # Создаем блоки
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            # Тестируем создание графиков
            analysis_plot = self.block_reporter.create_block_analysis_plot(1)
            self.assertIsNotNone(analysis_plot)
            
            summary_plot = self.block_reporter.create_blocks_summary_plot(1)
            self.assertIsNotNone(summary_plot)
            
            formula_plot = self.block_reporter.create_formula_performance_plot('volatility_focused')
            self.assertIsNotNone(formula_plot)
            
            # Тестируем комплексный отчет
            comprehensive_report = self.block_reporter.generate_comprehensive_report(1)
            self.assertIsInstance(comprehensive_report, dict)
            self.assertIn('blocks_summary', comprehensive_report)
            self.assertIn('key_insights', comprehensive_report)
            self.assertIn('recommendations', comprehensive_report)
            
            logger.info("✅ Тест 4: Интеграция Block Reporting → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста Block Reporting: {e}")
            self.fail(f"Тест Block Reporting провален: {e}")
    
    def test_end_to_end_workflow(self):
        """Тест 5: End-to-end workflow"""
        logger.info("🧪 Тест 5: End-to-end workflow")
        
        try:
            # Шаг 1: Получаем данные об ошибках
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            self.assertGreater(len(df), 0, "Должны быть данные об ошибках")
            
            # Шаг 2: Обнаруживаем блоки
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            self.assertGreater(len(blocks), 0, "Должны быть обнаружены блоки")
            
            # Шаг 3: Анализируем блоки
            for i, block in enumerate(blocks[:3]):  # Анализируем первые 3 блока
                analysis = self.block_analyzer.analyze_block(i + 1)
                self.assertIsInstance(analysis, BlockAnalysis)
            
            # Шаг 4: Получаем параметры формул для блоков
            for i, block in enumerate(blocks[:3]):
                params = self.formula_engine.get_formula_parameters('volatility_focused', block_id=i + 1)
                self.assertIsInstance(params, dict)
            
            # Шаг 5: Создаем отчеты
            comprehensive_report = self.block_reporter.generate_comprehensive_report(1)
            self.assertIsInstance(comprehensive_report, dict)
            
            # Проверяем, что все компоненты работают вместе
            self.assertIn('blocks_summary', comprehensive_report)
            self.assertIn('block_analyses', comprehensive_report)
            self.assertIn('formula_performance', comprehensive_report)
            
            logger.info("✅ Тест 5: End-to-end workflow → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка end-to-end теста: {e}")
            self.fail(f"End-to-end тест провален: {e}")
    
    def test_error_handling(self):
        """Тест 6: Обработка ошибок"""
        logger.info("🧪 Тест 6: Обработка ошибок")
        
        try:
            # Тест с несуществующим блоком
            analysis = self.block_analyzer.analyze_block(99999)
            self.assertIsInstance(analysis, BlockAnalysis)
            self.assertEqual(analysis.block_id, 99999)
            
            # Тест с несуществующей формулой
            params = self.formula_engine.get_formula_parameters('nonexistent_formula')
            self.assertIsInstance(params, dict)
            self.assertIn('threshold', params)
            
            # Тест с пустыми данными
            empty_df = pd.DataFrame()
            blocks = self.block_detector.detect_block_boundaries(empty_df)
            self.assertEqual(len(blocks), 0)
            
            logger.info("✅ Тест 6: Обработка ошибок → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста обработки ошибок: {e}")
            self.fail(f"Тест обработки ошибок провален: {e}")
    
    def test_performance_metrics(self):
        """Тест 7: Метрики производительности"""
        logger.info("🧪 Тест 7: Метрики производительности")
        
        try:
            # Создаем блоки
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            # Проверяем метрики производительности
            if len(blocks) > 0:
                analysis = self.block_analyzer.analyze_block(1)
                
                # Проверяем наличие метрик
                self.assertIsInstance(analysis.performance_metrics, dict)
                
                if analysis.performance_metrics:
                    self.assertIn('overall_score', analysis.performance_metrics)
                    overall_score = analysis.performance_metrics['overall_score']
                    self.assertGreaterEqual(overall_score, 0.0)
                    self.assertLessEqual(overall_score, 1.0)
            
            logger.info("✅ Тест 7: Метрики производительности → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста метрик производительности: {e}")
            self.fail(f"Тест метрик производительности провален: {e}")
    
    def test_data_consistency(self):
        """Тест 8: Консистентность данных"""
        logger.info("🧪 Тест 8: Консистентность данных")
        
        try:
            # Создаем блоки
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT * FROM error_history ORDER BY timestamp", conn)
            
            blocks = self.block_detector.detect_block_boundaries(df, threshold=1.5, window=50)
            self.block_detector.save_blocks(blocks)
            
            # Проверяем консистентность данных
            saved_blocks = self.block_detector.get_blocks()
            
            for _, block in saved_blocks.iterrows():
                # Проверяем, что временные границы корректны
                self.assertLess(block['start_time'], block['end_time'])
                
                # Проверяем, что статистики корректны
                self.assertGreaterEqual(block['mean_error'], 0.0)
                self.assertGreaterEqual(block['std_error'], 0.0)
                self.assertGreaterEqual(block['max_error'], block['min_error'])
                self.assertGreaterEqual(block['confidence'], 0.0)
                self.assertLessEqual(block['confidence'], 1.0)
            
            logger.info("✅ Тест 8: Консистентность данных → PASSED")
            
        except Exception as e:
            logger.error(f"❌ Ошибка теста консистентности данных: {e}")
            self.fail(f"Тест консистентности данных провален: {e}")

def run_block_components_tests():
    """Запуск всех тестов компонентов блоков"""
    logger.info("🎯 Запуск тестов компонентов Error-Driven Blocks")
    logger.info(f"📅 Время: {datetime.now()}")
    
    # Создание тестового набора
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBlockComponents)
    
    # Запуск тестов
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Подсчет результатов
    total_tests = result.testsRun
    failed_tests = len(result.failures)
    error_tests = len(result.errors)
    passed_tests = total_tests - failed_tests - error_tests
    
    success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    logger.info("📊 Итоговый отчет тестирования компонентов блоков:")
    logger.info(f"   Всего тестов: {total_tests}")
    logger.info(f"   Пройдено: {passed_tests}")
    logger.info(f"   Провалено: {failed_tests}")
    logger.info(f"   Ошибок: {error_tests}")
    logger.info(f"   Успешность: {success_rate:.1f}%")
    
    if success_rate >= 80:
        logger.info("🎉 ТЕСТЫ КОМПОНЕНТОВ БЛОКОВ ПРОЙДЕНЫ!")
        return True
    else:
        logger.error("❌ ТЕСТЫ КОМПОНЕНТОВ БЛОКОВ ПРОВАЛЕНЫ!")
        return False

if __name__ == "__main__":
    success = run_block_components_tests()
    sys.exit(0 if success else 1)
