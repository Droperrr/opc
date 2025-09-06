#!/usr/bin/env python3
"""
Модуль генерации трендовых сигналов на основе агрегатов IV и OI
"""

import sqlite3
import pandas as pd
from datetime import datetime
from logger import get_logger

logger = get_logger()

def generate_trend_signals(db='data/options_enriched.db', iv_table='iv_agg_15m', out_table='trend_signals'):
    """Генерирует трендовые сигналы на основе агрегатов IV и OI"""
    logger.info("🚀 Генерация трендовых сигналов")
    
    try:
        conn = sqlite3.connect(db)
        
        # Проверяем существование таблицы агрегатов
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{iv_table}'")
        if not cursor.fetchone():
            logger.error(f"❌ Таблица {iv_table} не найдена")
            return None
        
        # Загружаем данные агрегатов
        df = pd.read_sql_query(f"SELECT * FROM {iv_table}", conn, parse_dates=['timestamp'])
        
        if df.empty:
            logger.warning(f"⚠️ Нет данных в таблице {iv_table}")
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
        df['iv_mom'] = df['iv_mean_all'].diff(iv_momentum_periods)
        
        signals = []
        
        for _, row in df.iterrows():
            direction = None
            reason = []
            confidence = 0.5
            
            # Правило 1: BUY сигнал (упрощенное)
            if row['iv_call_mean'] > row['iv_put_mean']:
                direction = 'BUY'
                call_put_diff = ((row['iv_call_mean'] / row['iv_put_mean'] - 1) * 100)
                reason = f"call>put by {call_put_diff:.1f}%"
                confidence = min(1.0, (row['iv_call_mean'] / row['iv_put_mean'] - 1) + 0.5)
            
            # Правило 2: SELL сигнал (упрощенное)
            elif row['iv_put_mean'] > row['iv_call_mean']:
                direction = 'SELL'
                put_call_diff = ((row['iv_put_mean'] / row['iv_call_mean'] - 1) * 100)
                reason = f"put>call by {put_call_diff:.1f}%"
                confidence = min(1.0, (row['iv_put_mean'] / row['iv_call_mean'] - 1) + 0.5)
            
            # Правило 3: BULLISH сигнал (по skew)
            elif row['skew'] > 0.01:
                direction = 'BULLISH'
                reason = f"positive skew={row['skew']:.4f}"
                confidence = min(1.0, row['skew'] * 5 + 0.5)
            
            # Правило 4: BEARISH сигнал (по skew)
            elif row['skew'] < -0.01:
                direction = 'BEARISH'
                reason = f"negative skew={row['skew']:.4f}"
                confidence = min(1.0, abs(row['skew']) * 5 + 0.5)
            
            if direction:
                signals.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'timeframe': row.get('timeframe', '15m'),
                    'direction': direction,
                    'confidence': round(confidence, 2),
                    'reason': reason,
                    'iv_call_mean': row['iv_call_mean'],
                    'iv_put_mean': row['iv_put_mean'],
                    'skew': row['skew'],
                    'oi_ratio': row['oi_ratio'],
                    'iv_momentum': row['iv_mom']
                })
        
        if signals:
            # Сохраняем в БД
            signals_df = pd.DataFrame(signals)
            signals_df.to_sql(out_table, conn, if_exists='replace', index=False)
            
            # Сохраняем в CSV
            csv_filename = 'trend_signals.csv'
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

def generate_signals_for_all_timeframes():
    """Генерирует сигналы для всех таймфреймов"""
    logger.info("🚀 Генерация сигналов для всех таймфреймов")
    
    timeframes = ['15m', '1h']
    all_signals = []
    
    for timeframe in timeframes:
        logger.info(f"📊 Обрабатываем таймфрейм: {timeframe}")
        
        iv_table = f'iv_agg_{timeframe}'
        out_table = f'trend_signals_{timeframe}'
        
        signals = generate_trend_signals(iv_table=iv_table, out_table=out_table)
        
        if signals is not None:
            all_signals.append(signals)
            logger.info(f"✅ Сгенерировано {len(signals)} сигналов для {timeframe}")
        else:
            logger.warning(f"⚠️ Не удалось сгенерировать сигналы для {timeframe}")
    
    # Объединяем все сигналы
    if all_signals:
        combined_signals = pd.concat(all_signals, ignore_index=True)
        
        # Сохраняем общий файл
        combined_signals.to_csv('trend_signals_all.csv', index=False)
        
        logger.info(f"💾 Общий файл: {len(combined_signals)} сигналов")
        
        return combined_signals
    
    return None

def analyze_signal_distribution():
    """Анализирует распределение сигналов"""
    try:
        df = pd.read_csv('trend_signals.csv')
        
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
        
        # Статистика по skew
        print(f"\nСтатистика skew:")
        print(f"  Среднее: {df['skew'].mean():.4f}")
        print(f"  Медиана: {df['skew'].median():.4f}")
        
        # Статистика по OI ratio
        print(f"\nСтатистика OI ratio:")
        print(f"  Среднее: {df['oi_ratio'].mean():.3f}")
        print(f"  Медиана: {df['oi_ratio'].median():.3f}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка анализа распределения: {e}")

def run_trend_signals_demo():
    """Демонстрация генерации трендовых сигналов"""
    logger.info("🎯 Демонстрация генерации трендовых сигналов")
    
    # Генерируем сигналы для всех таймфреймов
    all_signals = generate_signals_for_all_timeframes()
    
    if all_signals is not None:
        logger.info("✅ Трендовые сигналы успешно сгенерированы")
        
        # Анализируем распределение
        analyze_signal_distribution()
        
        # Показываем общую статистику
        print(f"\n📊 Общая статистика:")
        print(f"Всего сигналов: {len(all_signals)}")
        print(f"Уникальных направлений: {all_signals['direction'].nunique()}")
        print(f"Таймфреймы: {all_signals['timeframe'].unique()}")
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация генерации трендовых сигналов
    run_trend_signals_demo()
