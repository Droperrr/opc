#!/usr/bin/env python3
"""
Модуль генерации трендовых сигналов на основе агрегатов IV и OI
"""

import sqlite3
import pandas as pd
from datetime import datetime
from logger import get_logger
import argparse

logger = get_logger()

def generate_trend_signals(symbol='BTCUSDT', dataset_tag='training_2023', db='server_opc.db', iv_table='iv_agg', timeframe='15m', out_table='trend_signals'):
    """Генерирует трендовые сигналы на основе агрегатов IV и OI с фильтрацией по symbol и dataset_tag"""
    logger.info(f"🚀 Генерация трендовых сигналов для {symbol} ({dataset_tag})")
    
    try:
        conn = sqlite3.connect(db)
        
        # Проверяем существование таблицы агрегатов
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (iv_table,))
        if not cursor.fetchone():
            logger.error(f"❌ Таблица {iv_table} не найдена")
            return None
        
        # Загружаем данные агрегатов с фильтрацией по таймфрейму, symbol и dataset_tag
        df = pd.read_sql_query(f"SELECT * FROM {iv_table} WHERE timeframe = ? AND symbol = ? AND dataset_tag = ?", 
                              conn, params=(timeframe, symbol, dataset_tag), parse_dates=['time'])
        # Переименовываем столбец time в timestamp для совместимости
        df = df.rename(columns={'time': 'timestamp'})
        
        if df.empty:
            logger.warning(f"⚠️ Нет данных в таблице {iv_table} для таймфрейма {timeframe}, symbol {symbol}, tag {dataset_tag}")
            return None
        
        logger.info(f"📊 Загружено {len(df)} записей агрегатов")
        
        # Параметры для генерации сигналов (смягченные)
        call_put_margin = 0.05  # 5% разница между call и put IV
        iv_momentum_periods = 1  # Периоды для расчета momentum
        min_oi_ratio = 0.51     # Минимальный OI ratio для подтверждения
        max_oi_ratio = 0.49      # Максимальный OI ratio для противоположного сигнала
        
        # Сортируем по времени
        df = df.sort_values('timestamp')
        
        # Вычисляем IV momentum (изменение IV за последние периоды)
        df['iv_mom'] = df['iv_30d'].diff(iv_momentum_periods)
        
        signals = []
        
        for _, row in df.iterrows():
            direction = None
            reason = []
            confidence = 0.5
            
            # Определяем направление тренда на основе изменения IV
            if row['iv_mom'] > 0:
                direction = 'BULLISH'
                reason = f"IV растет на {row['iv_mom']:.4f}"
                confidence = min(1.0, abs(row['iv_mom']) * 10 + 0.5)
            elif row['iv_mom'] < 0:
                direction = 'BEARISH'
                reason = f"IV падает на {row['iv_mom']:.4f}"
                confidence = min(1.0, abs(row['iv_mom']) * 10 + 0.5)
            else:
                direction = 'NEUTRAL'
                reason = "IV стабилен"
                confidence = 0.5
            
            if direction:
                signals.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'timeframe': row.get('timeframe', '15m'),
                    'direction': direction,
                    'confidence': round(confidence, 2),
                    'reason': reason,
                    'iv_30d': row['iv_30d'],
                    'skew_30d': row['skew_30d'],
                    'iv_momentum': row['iv_mom'],
                    'symbol': symbol,
                    'dataset_tag': dataset_tag
                })
        
        if signals:
            # Сохраняем в БД
            signals_df = pd.DataFrame(signals)
            signals_df.to_sql(out_table, conn, if_exists='replace', index=False)
            
            # Сохраняем в CSV
            csv_filename = f'trend_signals_{symbol}_{dataset_tag}.csv'
            signals_df.to_csv(csv_filename, index=False)
            
            logger.info(f"💾 Сохранено {len(signals)} трендовых сигналов в БД и CSV")
            
            # Показываем примеры
            logger.info("📋 Примеры трендовых сигналов:")
            print(signals_df.head(5).to_string(index=False))
            
            return signals_df
        else:
            logger.warning("⚠️ Не найдено сигналов по заданным критериям")
            return None
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации трендовых сигналов: {e}")
        return None

def generate_signals_for_all_timeframes(symbol='BTCUSDT', dataset_tag='training_2023'):
    """Генерирует сигналы для всех таймфреймов"""
    logger.info(f"🚀 Генерация сигналов для всех таймфреймов для {symbol} ({dataset_tag})")
    
    timeframes = ['15m', '1h']
    all_signals = []
    
    for timeframe in timeframes:
        logger.info(f"📊 Обрабатываем таймфрейм: {timeframe}")
        
        iv_table = 'iv_agg'
        out_table = f'trend_signals_{timeframe}'
        
        signals = generate_trend_signals(symbol=symbol, dataset_tag=dataset_tag, iv_table=iv_table, timeframe=timeframe, out_table=out_table)
        
        if signals is not None:
            all_signals.append(signals)
            logger.info(f"✅ Сгенерировано {len(signals)} сигналов для {timeframe}")
        else:
            logger.warning(f"⚠️ Не удалось сгенерировать сигналы для {timeframe}")
    
    # Объединяем все сигналы
    if all_signals:
        combined_signals = pd.concat(all_signals, ignore_index=True)
        
        # Сохраняем общий файл
        combined_signals.to_csv(f'trend_signals_all_{symbol}_{dataset_tag}.csv', index=False)
        
        logger.info(f"💾 Общий файл: {len(combined_signals)} сигналов")
        
        return combined_signals
    
    return None

def analyze_signal_distribution(symbol='BTCUSDT', dataset_tag='training_2023'):
    """Анализирует распределение сигналов"""
    try:
        df = pd.read_csv(f'trend_signals_{symbol}_{dataset_tag}.csv')
        
        if df.empty:
            logger.warning("⚠️ Нет сигналов для анализа")
            return
        
        logger.info("📊 Анализ распределения сигналов:")
        
        # Распределение по направлениям
        direction_counts = df['direction'].value_counts()
        print(f"\nРаспределение по направлениям:")
        for direction, count in direction_counts.items():
            print(f"  {direction}: {count}")
        
        # Статистика по confidence
        print(f"\nСтатистика confidence:")
        print(f"  Среднее: {df['confidence'].mean():.3f}")
        print(f"  Медиана: {df['confidence'].median():.3f}")
        print(f"  Минимум: {df['confidence'].min():.3f}")
        print(f"  Максимум: {df['confidence'].max():.3f}")
        
        # Статистика по IV 30d
        print(f"\nСтатистика IV 30d:")
        print(f"  Среднее: {df['iv_30d'].mean():.4f}")
        print(f"  Медиана: {df['iv_30d'].median():.4f}")
        
        # Статистика по skew 30d
        print(f"\nСтатистика skew 30d:")
        print(f"  Среднее: {df['skew_30d'].mean():.4f}")
        print(f"  Медиана: {df['skew_30d'].median():.4f}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка анализа распределения: {e}")

def run_trend_signals_demo(symbol='BTCUSDT', dataset_tag='training_2023'):
    """Демонстрация генерации трендовых сигналов"""
    logger.info(f"🎯 Демонстрация генерации трендовых сигналов для {symbol} ({dataset_tag})")
    
    # Генерируем сигналы для всех таймфреймов
    all_signals = generate_signals_for_all_timeframes(symbol, dataset_tag)
    
    if all_signals is not None:
        logger.info("✅ Трендовые сигналы успешно сгенерированы")
        
        # Анализируем распределение
        analyze_signal_distribution(symbol, dataset_tag)
        
        # Показываем общую статистику
        print(f"\n📊 Общая статистика:")
        print(f"Всего сигналов: {len(all_signals)}")
        print(f"Уникальных направлений: {all_signals['direction'].nunique()}")
        print(f"Таймфреймы: {all_signals['timeframe'].unique()}")
    
    logger.info("✅ Демонстрация завершена")

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Генерация трендовых сигналов для системы OPC')
    parser.add_argument('--symbol', default='BTCUSDT',
                       help='Символ актива (например, BTCUSDT, SOLUSDT)')
    parser.add_argument('--tag', default='training_2023',
                       help='Тег набора данных (например, training_2023, live_2025)')
    
    args = parser.parse_args()
    
    # Демонстрация генерации трендовых сигналов
    run_trend_signals_demo(args.symbol, args.tag)

if __name__ == "__main__":
    main()
