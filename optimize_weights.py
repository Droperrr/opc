#!/usr/bin/env python3
"""
Скрипт для оптимизации весов с помощью formula_engine.py
"""

import pandas as pd
import numpy as np
import sys
import os
from formula_engine import FormulaEngine
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('optimize_weights.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_dataset(dataset_path):
    """
    Загружает датасет из Parquet файла
    """
    try:
        logger.info(f"Загрузка датасета из {dataset_path}")
        df = pd.read_parquet(dataset_path)
        logger.info(f"Загружено {len(df)} записей")
        return df
    except Exception as e:
        logger.error(f"Ошибка загрузки датасета: {e}")
        return None

def optimize_weights_for_dataset(df, symbol):
    """
    Оптимизирует веса для конкретного датасета
    """
    logger.info(f"Начало оптимизации весов для {symbol}")
    
    try:
        # Создаем экземпляр FormulaEngine
        engine = FormulaEngine()
        
        # Оптимизируем параметры для формулы volatility_focused
        best_params = engine.optimize_params('volatility_focused', df, target_metric='accuracy')
        
        if best_params:
            logger.info(f"Лучшие параметры для {symbol}: {best_params}")
            
            # Сохраняем лучшие параметры
            engine.update_formula_params('volatility_focused', best_params)
            
            # Тестируем формулу с лучшими параметрами
            test_results = engine.batch_evaluate('volatility_focused', df.head(1000), best_params, True)
            
            if not test_results.empty:
                signals = test_results[test_results['signal'].isin(['BUY', 'SELL'])]
                if len(signals) > 0:
                    accuracy = len(signals[signals['signal'] == 'BUY']) / len(signals)
                    logger.info(f"Тестовая точность для {symbol}: {accuracy:.3f}")
                else:
                    logger.warning(f"Нет торговых сигналов для {symbol}")
            
            return best_params
        else:
            logger.error(f"Не удалось оптимизировать параметры для {symbol}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка оптимизации для {symbol}: {e}")
        return None

def main():
    """Основная функция оптимизации"""
    logger.info("Начало оптимизации весов")
    
    # Пути к датасетам
    datasets = {
        'SOLUSDT': 'ml_dataset_aug2025_SOLUSDT.parquet'
    }
    
    results = {}
    
    # Оптимизируем веса для каждого датасета
    for symbol, dataset_path in datasets.items():
        if os.path.exists(dataset_path):
            # Загружаем датасет
            df = load_dataset(dataset_path)
            
            if df is not None and not df.empty:
                # Оптимизируем веса
                best_params = optimize_weights_for_dataset(df, symbol)
                results[symbol] = best_params
            else:
                logger.warning(f"Датасет для {symbol} пуст или не существует")
        else:
            logger.warning(f"Файл датасета {dataset_path} не найден")
    
    # Выводим результаты
    logger.info("Результаты оптимизации:")
    for symbol, params in results.items():
        if params:
            logger.info(f"{symbol}: {params}")
        else:
            logger.info(f"{symbol}: оптимизация не удалась")
    
    logger.info("Оптимизация весов завершена")

if __name__ == "__main__":
    main()