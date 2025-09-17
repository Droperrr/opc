#!/usr/bin/env python3
"""
Скрипт для подготовки данных для визуализации на дашборде
"""

import pandas as pd
import numpy as np
import argparse
import os
from logger import get_logger

logger = get_logger()

def prepare_dashboard_data(results_dir='backtest_results_mvp', run_id=None):
    """Подготавливает данные для визуализации на дашборде"""
    try:
        # Загружаем данные признаков
        logger.info("Загрузка данных признаков...")
        features_df = pd.read_parquet('basis_features_1m.parquet')
        logger.info(f"Загружено {len(features_df)} записей признаков")
        
        # Загружаем данные сделок из бэктеста
        logger.info("Загрузка данных сделок...")
        trades_file = os.path.join(results_dir, 'backtest_trades.csv')
        trades_df = pd.read_csv(trades_file)
        logger.info(f"Загружено {len(trades_df)} записей сделок из {trades_file}")
        
        # Преобразуем timestamp в datetime
        features_df['time'] = pd.to_datetime(features_df['time'])
        trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'])
        
        # Сортируем данные по времени
        features_df = features_df.sort_values('time').reset_index(drop=True)
        trades_df = trades_df.sort_values('timestamp').reset_index(drop=True)
        
        # Рассчитываем короткую скользящую среднюю (SMA_10)
        logger.info("Расчет короткой скользящей средней (SMA_10)...")
        features_df['SMA_10'] = features_df['spot_price'].rolling(window=10).mean()
        
        # Заполняем NaN значения в SMA_10
        features_df['SMA_10'] = features_df['SMA_10'].fillna(features_df['spot_price'])
        
        # Объединяем данные признаков и сделок
        # Для каждой сделки находим соответствующую запись в features_df по времени
        logger.info("Объединение данных признаков и сделок...")
        
        # Создаем копию trades_df для объединения
        trades_enriched = trades_df.copy()
        
        # Для каждой сделки находим ближайшую запись в features_df по времени
        # Используем merge_asof для эффективного объединения по времени
        dashboard_data = pd.merge_asof(
            trades_enriched.sort_values('timestamp'),
            features_df.sort_values('time'),
            left_on='timestamp',
            right_on='time',
            direction='backward',
            tolerance=pd.Timedelta('1min')  # Толерантность 1 минута
        )
        
        # Удаляем дубликаты колонок и переименовываем при необходимости
        if 'time' in dashboard_data.columns and 'timestamp' in dashboard_data.columns:
            # Оставляем только одну колонку времени
            dashboard_data = dashboard_data.drop(columns=['time'])
        
        # Добавляем run_id в данные, если он предоставлен
        if run_id:
            dashboard_data['run_id'] = run_id
            logger.info(f"Добавлен run_id: {run_id}")
        
        # Сохраняем итоговый DataFrame в новый файл
        logger.info("Сохранение обогащенных данных...")
        dashboard_data.to_parquet('dashboard_data.parquet', index=False)
        logger.info(f"Сохранено {len(dashboard_data)} записей в dashboard_data.parquet")
        
        # Выводим первые несколько строк для проверки
        logger.info("Первые 5 строк обогащенных данных:")
        print(dashboard_data.head())
        
        # Выводим информацию о колонках
        logger.info("Колонки в итоговом датасете:")
        print(list(dashboard_data.columns))
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Ошибка при подготовке данных для дашборда: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Prepare dashboard data')
    parser.add_argument('--results_dir', type=str, default='backtest_results_mvp', help='Directory with backtest results')
    parser.add_argument('--run_id', type=str, default=None, help='Run ID for this backtest session')
    
    args = parser.parse_args()
    
    logger.info("=" * 50)
    logger.info("ПОДГОТОВКА ДАННЫХ ДЛЯ ВИЗУАЛИЗАЦИИ НА ДАШБОРДЕ")
    logger.info("=" * 50)
    
    data = prepare_dashboard_data(results_dir=args.results_dir, run_id=args.run_id)
    
    if data is not None:
        logger.info("✅ Подготовка данных завершена успешно!")
    else:
        logger.error("❌ Подготовка данных завершена с ошибками!")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    main()