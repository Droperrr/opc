#!/usr/bin/env python3
"""
Тестовый скрипт для проверки системы грубого поиска
Задача S-10: Тестирование на подмножестве данных
"""

import sys
import os
sys.path.append('.')

from search.coarse_search import CoarseSearch
from engine.formulas import formula_catalog
import pandas as pd
import numpy as np

def test_formula_catalog():
    """Тестирует каталог формул"""
    print("🔍 Тестирование каталога формул...")
    
    formulas = formula_catalog.get_all_formulas()
    print(f"✅ Найдено {len(formulas)} формул")
    
    # Проверяем первые 3 формулы
    for i, (formula_id, formula) in enumerate(list(formulas.items())[:3]):
        print(f"  {formula_id}: {formula['name']}")
        print(f"    Параметры: {list(formula['params'].keys())}")
        
        # Генерируем тестовые параметры
        params_list = formula_catalog.generate_random_params(formula_id, n_samples=5)
        print(f"    Сгенерировано {len(params_list)} наборов параметров")
        print()

def test_data_loading():
    """Тестирует загрузку данных"""
    print("📊 Тестирование загрузки данных...")
    
    searcher = CoarseSearch()
    
    # Загружаем данные за короткий период
    start_date = "2025-08-01"
    end_date = "2025-08-07"
    
    df = searcher.load_historical_data(start_date, end_date)
    
    if not df.empty:
        print(f"✅ Загружено {len(df)} записей")
        print(f"   Период: {df['time'].min()} - {df['time'].max()}")
        print(f"   Колонки: {list(df.columns)}")
        print(f"   Размер: {df.shape}")
        print()
        return df
    else:
        print("❌ Не удалось загрузить данные")
        return None

def test_formula_calculation(df):
    """Тестирует вычисление формул"""
    print("🧮 Тестирование вычисления формул...")
    
    searcher = CoarseSearch()
    
    # Тестируем первые 3 формулы
    formulas = list(formula_catalog.get_all_formulas().items())[:3]
    
    for formula_id, formula in formulas:
        print(f"  Тестируем {formula_id}: {formula['name']}")
        
        # Генерируем один набор параметров
        params_list = formula_catalog.generate_random_params(formula_id, n_samples=1)
        params = params_list[0]
        
        print(f"    Параметры: {params}")
        
        # Вычисляем Y
        Y = searcher.calculate_formula_value(df, formula_id, params)
        
        print(f"    Y: min={Y.min():.3f}, max={Y.max():.3f}, mean={Y.mean():.3f}, std={Y.std():.3f}")
        print()

def test_backtest(df):
    """Тестирует backtest"""
    print("📈 Тестирование backtest...")
    
    searcher = CoarseSearch()
    
    # Тестируем одну формулу
    formula_id = "F01"
    params = {'a': 1.5, 'b': 0.5, 'c': 1.0, 'd': 1.0}
    
    print(f"  Тестируем {formula_id} с параметрами {params}")
    
    # Запускаем backtest
    metrics = searcher.run_backtest(df, formula_id, params)
    
    print(f"    Sharpe Ratio: {metrics['sharpe_ratio']:.3f}")
    print(f"    Profit Factor: {metrics['profit_factor']:.3f}")
    print(f"    Win Rate: {metrics['win_rate']:.3f}")
    print(f"    Max Drawdown: {metrics['max_drawdown']:.3f}")
    print(f"    Total Return: {metrics['total_return']:.3f}")
    print()

def test_coarse_search_limited(df):
    """Тестирует ограниченный грубый поиск"""
    print("🔍 Тестирование ограниченного грубого поиска...")
    
    searcher = CoarseSearch()
    
    # Модифицируем конфигурацию для быстрого теста
    searcher.config['search']['coarse_search']['n_samples_per_formula'] = 10
    searcher.config['search']['coarse_search']['n_top_candidates'] = 3
    
    # Тестируем только первые 3 формулы
    formulas = list(formula_catalog.get_all_formulas().items())[:3]
    
    all_results = []
    
    for formula_id, formula in formulas:
        print(f"  Тестируем {formula_id}: {formula['name']}")
        
        # Генерируем параметры
        params_list = formula_catalog.generate_random_params(formula_id, n_samples=10)
        
        formula_results = []
        
        for params in params_list:
            # Запускаем backtest
            metrics = searcher.run_backtest(df, formula_id, params)
            
            result = {
                'formula_id': formula_id,
                'formula_name': formula['name'],
                'params': params,
                'metrics': metrics,
                'score': metrics['sharpe_ratio']
            }
            
            formula_results.append(result)
        
        # Сортируем и берем топ-3
        formula_results.sort(key=lambda x: x['score'], reverse=True)
        top_results = formula_results[:3]
        all_results.extend(top_results)
        
        print(f"    Найдено {len(top_results)} топ-кандидатов")
        print(f"    Лучший Sharpe: {top_results[0]['metrics']['sharpe_ratio']:.3f}")
        print()
    
    # Показываем общий топ-3
    all_results.sort(key=lambda x: x['score'], reverse=True)
    
    print("🏆 ОБЩИЙ ТОП-3:")
    for i, result in enumerate(all_results[:3]):
        print(f"{i+1}. {result['formula_id']} - {result['formula_name']}")
        print(f"   Sharpe: {result['metrics']['sharpe_ratio']:.3f}")
        print(f"   Profit Factor: {result['metrics']['profit_factor']:.3f}")
        print(f"   Win Rate: {result['metrics']['win_rate']:.3f}")
        print()

def main():
    """Основная функция тестирования"""
    print("🧪 ТЕСТИРОВАНИЕ СИСТЕМЫ ГРУБОГО ПОИСКА")
    print("=" * 60)
    
    # Тест 1: Каталог формул
    test_formula_catalog()
    
    # Тест 2: Загрузка данных
    df = test_data_loading()
    if df is None:
        print("❌ Тестирование прервано из-за ошибки загрузки данных")
        return
    
    # Тест 3: Вычисление формул
    test_formula_calculation(df)
    
    # Тест 4: Backtest
    test_backtest(df)
    
    # Тест 5: Ограниченный грубый поиск
    test_coarse_search_limited(df)
    
    print("✅ Тестирование завершено успешно!")

if __name__ == "__main__":
    main()
