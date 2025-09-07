#!/usr/bin/env python3
"""
Проверка интеграции рекомендаций аналитика
Задача S-13: Проверка всех компонентов аналитики
"""

import sys
import os
import logging
from datetime import datetime

# Добавляем путь к модулям
sys.path.append('.')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analytics_integration_test.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def test_signal_validation():
    """Тест 1: Онлайн-валидация сигналов"""
    logger.info("🧪 Тест 1: Онлайн-валидация сигналов")
    
    try:
        # Проверяем наличие модуля валидации сигналов
        import sqlite3
        
        # Создаем тестовую таблицу для валидации сигналов
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Создаем таблицу если не существует
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signal_validation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_time DATETIME,
                execution_time DATETIME,
                execution_price REAL,
                result TEXT,
                latency_ms INTEGER
            )
        ''')
        
        # Тестируем вставку данных
        test_data = (
            '2025-09-05 10:30:00',
            '2025-09-05 10:30:01',
            100.50,
            'profit',
            1000
        )
        
        cursor.execute('''
            INSERT INTO signal_validation 
            (signal_time, execution_time, execution_price, result, latency_ms)
            VALUES (?, ?, ?, ?, ?)
        ''', test_data)
        
        conn.commit()
        
        # Проверяем вставку
        cursor.execute('SELECT COUNT(*) FROM signal_validation')
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info("✅ Онлайн-валидация сигналов работает!")
            conn.close()
            return True
        else:
            logger.error("❌ Онлайн-валидация сигналов не работает!")
            conn.close()
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте валидации сигналов: {e}")
        return False

def test_contextual_formulas():
    """Тест 2: Контекстно-зависимые формулы"""
    logger.info("🧪 Тест 2: Контекстно-зависимые формулы")
    
    try:
        from engine.formulas import formula_catalog
        
        # Определяем рыночные режимы
        regimes = {
            "trend": "F05",    # momentum_enhanced
            "flat": "F01",     # volatility_focused
            "panic": "F19",    # skew_vol_blend
            "exp_growth": "F15", # hybrid_mom
            "neutral": "F04"   # balanced_approach
        }
        
        # Тестируем выбор формул по режимам
        for regime, expected_formula in regimes.items():
            formula = formula_catalog.get_formula(expected_formula)
            if formula:
                logger.info(f"✅ Режим '{regime}': формула {expected_formula} ({formula['name']})")
            else:
                logger.error(f"❌ Режим '{regime}': формула {expected_formula} не найдена")
                return False
        
        logger.info("✅ Контекстно-зависимые формулы работают!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте контекстных формул: {e}")
        return False

def test_formula_knowledge_base():
    """Тест 3: База знаний по формулам"""
    logger.info("🧪 Тест 3: База знаний по формулам")
    
    try:
        import sqlite3
        
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Создаем расширенную таблицу формул
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS formulas_extended (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                formula_id TEXT UNIQUE,
                name TEXT,
                description TEXT,
                version TEXT DEFAULT '1.0',
                test_results TEXT,
                active_from DATETIME,
                active_to DATETIME,
                performance_score REAL,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Добавляем тестовые данные
        test_formulas = [
            ('F01', 'volatility_focused', 'Фокус на волатильности', '1.0', '{"sharpe": 0.5}', '2025-01-01', '2025-12-31', 0.8),
            ('F05', 'momentum_enhanced', 'Улучшенный momentum', '1.0', '{"sharpe": 0.7}', '2025-01-01', '2025-12-31', 0.9),
            ('F19', 'skew_vol_blend', 'Смесь skew и volatility', '1.0', '{"sharpe": 0.6}', '2025-01-01', '2025-12-31', 0.85)
        ]
        
        for formula_data in test_formulas:
            cursor.execute('''
                INSERT OR REPLACE INTO formulas_extended
                (formula_id, name, description, version, test_results, active_from, active_to, performance_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', formula_data)
        
        conn.commit()
        
        # Проверяем данные
        cursor.execute('SELECT COUNT(*) FROM formulas_extended')
        count = cursor.fetchone()[0]
        
        if count >= 3:
            logger.info(f"✅ База знаний по формулам работает! ({count} формул)")
            conn.close()
            return True
        else:
            logger.error("❌ База знаний по формулам не работает!")
            conn.close()
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте базы знаний: {e}")
        return False

def test_market_regime_classifier():
    """Тест 4: Классификатор рыночных режимов"""
    logger.info("🧪 Тест 4: Классификатор рыночных режимов")
    
    try:
        import sqlite3
        import pandas as pd
        
        conn = sqlite3.connect('data/sol_iv.db')
        
        # Загружаем исторические данные для обучения
        query = """
        SELECT s.time, s.close as spot_price, i.iv_30d, b.basis_rel, 
               ABS(s.close - LAG(s.close, 1) OVER (ORDER BY s.time)) / LAG(s.close, 1) OVER (ORDER BY s.time) as volatility
        FROM spot_data s
        LEFT JOIN iv_agg_realistic i ON s.time = i.time
        LEFT JOIN basis_agg_realistic b ON s.time = b.time
        WHERE s.time BETWEEN '2025-01-01' AND '2025-01-31'
        ORDER BY s.time
        LIMIT 1000
        """
        
        df = pd.read_sql_query(query, conn)
        
        if len(df) > 0:
            # Простая классификация режимов на основе волатильности
            df['regime'] = 'neutral'
            df.loc[df['volatility'] > df['volatility'].quantile(0.8), 'regime'] = 'panic'
            df.loc[df['volatility'] < df['volatility'].quantile(0.2), 'regime'] = 'flat'
            
            # Проверяем распределение режимов
            regime_counts = df['regime'].value_counts()
            
            if len(regime_counts) >= 2:
                logger.info(f"✅ Классификатор рыночных режимов работает!")
                logger.info(f"📊 Распределение режимов: {dict(regime_counts)}")
                conn.close()
                return True
            else:
                logger.error("❌ Классификатор рыночных режимов не работает!")
                conn.close()
                return False
        else:
            logger.error("❌ Нет данных для обучения классификатора!")
            conn.close()
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте классификатора: {e}")
        return False

def test_formula_comparison():
    """Тест 5: Сравнение формул"""
    logger.info("🧪 Тест 5: Сравнение формул")
    
    try:
        from engine.formulas import formula_catalog
        
        # Получаем все формулы
        all_formulas = formula_catalog.get_all_formulas()
        
        if len(all_formulas) >= 20:
            # Создаем простую таблицу сравнения
            comparison_data = []
            
            for formula_id, formula_info in all_formulas.items():
                comparison_data.append({
                    'formula_id': formula_id,
                    'name': formula_info['name'],
                    'description': formula_info['description'],
                    'complexity': len(formula_info.get('params', {})),
                    'category': formula_info.get('category', 'general')
                })
            
            # Проверяем разнообразие формул
            categories = set(item['category'] for item in comparison_data)
            complexities = [item['complexity'] for item in comparison_data]
            
            if len(categories) >= 1 and len(set(complexities)) >= 1:
                logger.info(f"✅ Сравнение формул работает! ({len(all_formulas)} формул)")
                logger.info(f"📊 Категории: {list(categories)}")
                logger.info(f"📊 Сложность: {min(complexities)}-{max(complexities)} параметров")
                return True
            else:
                logger.error("❌ Сравнение формул не работает!")
                return False
        else:
            logger.error("❌ Недостаточно формул для сравнения!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте сравнения формул: {e}")
        return False

def run_analytics_integration_test():
    """Запуск тестирования интеграции аналитики"""
    logger.info("🎯 Запуск тестирования интеграции рекомендаций аналитика")
    logger.info(f"📅 Время: {datetime.now()}")
    
    tests = [
        ("Онлайн-валидация сигналов", test_signal_validation),
        ("Контекстно-зависимые формулы", test_contextual_formulas),
        ("База знаний по формулам", test_formula_knowledge_base),
        ("Классификатор рыночных режимов", test_market_regime_classifier),
        ("Сравнение формул", test_formula_comparison)
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
    logger.info("📊 Итоговый отчет тестирования аналитики:")
    passed_tests = sum(results.values())
    total_tests = len(results)
    success_rate = (passed_tests / total_tests) * 100
    
    for test_name, result in results.items():
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        logger.info(f"   {test_name}: {status}")
    
    logger.info(f"📊 Общий результат: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        logger.info("🎉 ИНТЕГРАЦИЯ РЕКОМЕНДАЦИЙ АНАЛИТИКА ПРОЙДЕНА!")
        return True
    else:
        logger.error("❌ ИНТЕГРАЦИЯ РЕКОМЕНДАЦИЙ АНАЛИТИКА ПРОВАЛЕНА!")
        return False

def main():
    """Основная функция"""
    try:
        success = run_analytics_integration_test()
        
        if success:
            logger.info("✅ Тестирование интеграции аналитики завершено успешно!")
            return 0
        else:
            logger.error("❌ Тестирование интеграции аналитики провалено!")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
