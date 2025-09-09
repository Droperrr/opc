#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы HistoricalAnalyzer
"""

import pandas as pd
from historical_analyzer import HistoricalAnalyzer

def test_historical_analyzer():
    """Тестирует работу HistoricalAnalyzer"""
    print("🚀 Тестирование HistoricalAnalyzer")
    
    # Создаем экземпляр анализатора
    analyzer = HistoricalAnalyzer()
    
    # Загружаем исторические данные
    print("📥 Загрузка исторических данных...")
    history_df = analyzer.load_historical_data()
    
    if history_df.empty:
        print("⚠️ Нет исторических данных для анализа")
        return
    
    print(f"📊 Загружено {len(history_df)} записей")
    print("📋 Первые несколько записей:")
    print(history_df.head())
    
    # Создаем тестовый сигнал
    # Используем данные из первой записи в истории для создания тестового сигнала
    if not history_df.empty:
        test_signal = {
            'iv_spike': history_df.iloc[0]['iv_mean_all'] - history_df.iloc[0]['iv_mean_all'],  # 0 для первой записи
            'skew': history_df.iloc[0]['skew'],
            'trend_confidence': history_df.iloc[0]['confidence'] if 'confidence' in history_df.columns else 0.5,
            'direction': 'BUY'  # Добавляем направление сигнала
        }
        
        print(f"\n🔍 Тестовый сигнал: {test_signal}")
        
        # Ищем аналоги
        print("🔍 Поиск исторических аналогов...")
        result = analyzer.find_analogies(test_signal)
        
        print(f"✅ Результат поиска аналогов:")
        print(f"   Всего найдено: {result['total_found']}")
        print(f"   Прибыльных: {result.get('profitable_count', 'N/A')}")
        print(f"   Win rate: {result.get('win_rate', 'N/A'):.2%}")
        print(f"   Сводка: {result.get('summary', 'N/A')}")
        print(f"   Причина (если есть): {result.get('reason', 'N/A')}")

if __name__ == "__main__":
    test_historical_analyzer()