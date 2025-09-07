#!/usr/bin/env python3
"""
Тест оптимизации памяти для системы поиска торговых стратегий
Задача S-13: Решение проблем с памятью и подготовка к S-10 FULL RUN
"""

import sys
import os
import logging
import time
from datetime import datetime

# Добавляем путь к модулям
sys.path.append('.')

try:
    from memory_optimizer import MemoryOptimizer
    from memory_monitor import MemoryProfiler
    from search.coarse_search import CoarseSearch
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_memory_optimization.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def test_memory_optimizer():
    """Тест оптимизатора памяти"""
    logger.info("🧪 Тест 1: MemoryOptimizer")
    
    try:
        # Создаем оптимизатор
        optimizer = MemoryOptimizer(chunk_size=10000, max_memory_gb=1.5)
        
        # Тест оптимизации типов данных
        import pandas as pd
        import numpy as np
        
        # Создаем тестовые данные
        test_data = pd.DataFrame({
            'iv_z': np.random.randn(50000).astype('float64'),
            'skew_z': np.random.randn(50000).astype('float64'),
            'basis_z': np.random.randn(50000).astype('float64'),
            'momentum': np.random.randn(50000).astype('float64'),
            'volatility': np.random.randn(50000).astype('float64')
        })
        
        logger.info(f"📊 Исходные данные: {test_data.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
        
        # Оптимизируем типы данных
        optimized_data = optimizer.optimize_dtypes(test_data)
        logger.info(f"📊 Оптимизированные данные: {optimized_data.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
        
        # Тест чанковой обработки
        def process_chunk(chunk):
            return chunk * 2
        
        results = list(optimizer.process_in_chunks(optimized_data, process_chunk))
        logger.info(f"✅ Чанковая обработка завершена: {len(results)} чанков")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте MemoryOptimizer: {e}")
        return False

def test_coarse_search_memory():
    """Тест CoarseSearch с оптимизацией памяти"""
    logger.info("🧪 Тест 2: CoarseSearch с оптимизацией памяти")
    
    try:
        # Создаем экземпляр CoarseSearch
        search = CoarseSearch("config/experiment.yaml")
        
        # Проверяем инициализацию оптимизатора памяти
        if hasattr(search, 'memory_optimizer') and search.memory_optimizer:
            logger.info("✅ MemoryOptimizer инициализирован в CoarseSearch")
        else:
            logger.warning("⚠️ MemoryOptimizer не инициализирован в CoarseSearch")
        
        # Тест загрузки данных
        logger.info("📊 Тест загрузки данных...")
        df = search.load_historical_data("2025-01-01", "2025-01-02")  # Только 1 день для теста
        
        if df is not None and len(df) > 0:
            logger.info(f"✅ Данные загружены: {len(df)} записей")
            
            # Тест добавления индикаторов
            logger.info("📊 Тест добавления индикаторов...")
            df_with_indicators = search._add_technical_indicators(df)
            
            if df_with_indicators is not None:
                logger.info(f"✅ Индикаторы добавлены: {len(df_with_indicators)} записей")
                
                # Тест вычисления формулы
                logger.info("📊 Тест вычисления формулы...")
                test_params = {'a': 1.0, 'b': 0.5, 'c': 0.3, 'd': 0.2}
                Y = search.calculate_formula_value(df_with_indicators, "F01", test_params)
                
                if Y is not None and len(Y) > 0:
                    logger.info(f"✅ Формула вычислена: {len(Y)} значений")
                    
                    # Тест бэктеста
                    logger.info("📊 Тест бэктеста...")
                    metrics = search.run_backtest(df_with_indicators, "F01", test_params)
                    
                    if metrics:
                        logger.info(f"✅ Бэктест завершен: Sharpe={metrics.get('sharpe_ratio', 0):.3f}")
                        return True
                    else:
                        logger.error("❌ Бэктест не вернул метрики")
                        return False
                else:
                    logger.error("❌ Формула не вычислена")
                    return False
            else:
                logger.error("❌ Индикаторы не добавлены")
                return False
        else:
            logger.error("❌ Данные не загружены")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте CoarseSearch: {e}")
        return False

def test_memory_profiler():
    """Тест профилировщика памяти"""
    logger.info("🧪 Тест 3: MemoryProfiler")
    
    try:
        profiler = MemoryProfiler("test_memory_profiler.log")
        
        # Тест базового логирования
        profiler.log_memory_usage("Начало теста")
        
        # Тест проверки порогов
        exceeds_threshold = profiler.check_memory_threshold(1.0)
        logger.info(f"📊 Превышение порога 1GB: {exceeds_threshold}")
        
        # Тест генерации отчета
        report = profiler.generate_memory_report()
        logger.info(f"✅ Отчет сгенерирован: {report['total_samples']} образцов")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте MemoryProfiler: {e}")
        return False

def test_full_memory_optimization():
    """Полный тест оптимизации памяти"""
    logger.info("🚀 Полный тест оптимизации памяти")
    
    start_time = time.time()
    results = {}
    
    # Тест 1: MemoryOptimizer
    results['memory_optimizer'] = test_memory_optimizer()
    
    # Тест 2: CoarseSearch
    results['coarse_search'] = test_coarse_search_memory()
    
    # Тест 3: MemoryProfiler
    results['memory_profiler'] = test_memory_profiler()
    
    # Итоговый отчет
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info("📊 Итоговый отчет:")
    logger.info(f"   Время выполнения: {duration:.2f} секунд")
    logger.info(f"   MemoryOptimizer: {'✅' if results['memory_optimizer'] else '❌'}")
    logger.info(f"   CoarseSearch: {'✅' if results['coarse_search'] else '❌'}")
    logger.info(f"   MemoryProfiler: {'✅' if results['memory_profiler'] else '❌'}")
    
    success_rate = sum(results.values()) / len(results) * 100
    logger.info(f"   Общий успех: {success_rate:.1f}%")
    
    if success_rate >= 80:
        logger.info("🎉 Тест оптимизации памяти ПРОЙДЕН!")
        return True
    else:
        logger.error("❌ Тест оптимизации памяти ПРОВАЛЕН!")
        return False

def main():
    """Основная функция"""
    logger.info("🎯 Запуск тестов оптимизации памяти S-13")
    logger.info(f"📅 Время: {datetime.now()}")
    
    try:
        success = test_full_memory_optimization()
        
        if success:
            logger.info("✅ Все тесты оптимизации памяти пройдены успешно!")
            logger.info("🚀 Система готова к S-10 FULL RUN")
            return 0
        else:
            logger.error("❌ Тесты оптимизации памяти провалены!")
            logger.error("🔧 Требуется дополнительная настройка")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
