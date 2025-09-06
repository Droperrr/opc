#!/usr/bin/env python3
"""
Модуль генерации минутных точек входа, согласованных с трендом
"""

import sqlite3
import pandas as pd
from datetime import timedelta
from logger import get_logger

logger = get_logger()

def gen_entries(db='data/options_enriched.db', iv_table='iv_agg_1m', trend_table='trend_signals', out_csv='entry_points.csv'):
    """Генерирует минутные точки входа, согласованные с трендом"""
    logger.info("🚀 Генерация минутных точек входа")
    
    try:
        conn = sqlite3.connect(db)
        
        # Загружаем минутные агрегаты
        iv1 = pd.read_sql_query(f"SELECT * FROM {iv_table} ORDER BY timestamp ASC", conn, parse_dates=['timestamp'])
        if iv1.empty:
            logger.warning(f"⚠️ Нет данных в таблице {iv_table}")
            return None
        
        # Загружаем трендовые сигналы
        trend = pd.read_sql_query(f"SELECT * FROM {trend_table}", conn, parse_dates=['timestamp'])
        if trend.empty:
            logger.warning(f"⚠️ Нет данных в таблице {trend_table}")
            return None
        
        trend = trend.sort_values('timestamp')
        logger.info(f"📊 Загружено {len(iv1)} минутных записей и {len(trend)} трендовых сигналов")
        
        # Параметры для генерации точек входа
        spike_threshold = 0.02  # 2% spike
        min_trend_conf = 0.3   # Минимальная уверенность тренда
        
        # Вычисляем IV spike для минутных данных
        iv1['iv_prev5'] = iv1['iv_mean_all'].rolling(5, min_periods=1).mean().shift(1)
        iv1['spike_pct'] = (iv1['iv_mean_all'] - iv1['iv_prev5']) / iv1['iv_prev5'].replace(0, 1)
        
        entries = []
        
        for _, r in iv1.iterrows():
            if abs(r['spike_pct']) >= spike_threshold:
                # Находим ближайший трендовый сигнал (15m или 1h)
                current_time = r['timestamp']
                
                # Ищем трендовые сигналы за последние 30 минут
                recent_trends = trend[trend['timestamp'] >= current_time - timedelta(minutes=30)]
                
                if recent_trends.empty:
                    continue
                
                # Берем самый последний тренд
                last_trend = recent_trends.iloc[-1]
                
                if last_trend['confidence'] < min_trend_conf:
                    continue
                
                # Определяем направление входа на основе spike и тренда
                entry_direction = None
                entry_reason = ""
                
                # Если IV растет и тренд BUY/BULLISH
                if (r['spike_pct'] > 0 and 
                    last_trend['direction'] in ['BUY', 'BULLISH']):
                    entry_direction = 'BUY'
                    entry_reason = f"IV spike +{r['spike_pct']*100:.1f}% + {last_trend['direction']} trend"
                
                # Если IV падает и тренд SELL/BEARISH
                elif (r['spike_pct'] < 0 and 
                      last_trend['direction'] in ['SELL', 'BEARISH']):
                    entry_direction = 'SELL'
                    entry_reason = f"IV spike {r['spike_pct']*100:.1f}% + {last_trend['direction']} trend"
                
                # Дополнительные правила на основе skew
                elif r['skew'] > 0.01 and last_trend['direction'] in ['BUY', 'BULLISH']:
                    entry_direction = 'BUY'
                    entry_reason = f"positive skew {r['skew']:.4f} + {last_trend['direction']} trend"
                
                elif r['skew'] < -0.01 and last_trend['direction'] in ['SELL', 'BEARISH']:
                    entry_direction = 'SELL'
                    entry_reason = f"negative skew {r['skew']:.4f} + {last_trend['direction']} trend"
                
                if entry_direction:
                    # Рассчитываем confidence на основе spike и тренда
                    spike_confidence = min(1.0, abs(r['spike_pct']) * 10)
                    trend_confidence = last_trend['confidence']
                    combined_confidence = (spike_confidence + trend_confidence) / 2
                    
                    entries.append({
                        'timestamp': r['timestamp'].isoformat(),
                        'direction': entry_direction,
                        'timeframe': '1m',
                        'confidence': round(combined_confidence, 2),
                        'reason': entry_reason,
                        'underlying_price': r['underlying_price'],
                        'iv_spike': round(r['spike_pct'] * 100, 2),
                        'skew': round(r['skew'], 4),
                        'trend_direction': last_trend['direction'],
                        'trend_confidence': last_trend['confidence']
                    })
        
        if entries:
            # Сохраняем в CSV
            df = pd.DataFrame(entries)
            df.to_csv(out_csv, index=False)
            
            logger.info(f"💾 Сохранено {len(entries)} точек входа в {out_csv}")
            
            # Показываем примеры
            logger.info("📋 Примеры точек входа:")
            print(df.head(5).to_string(index=False))
            
            return df
        else:
            logger.warning("⚠️ Не найдено точек входа по заданным критериям")
            return None
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации точек входа: {e}")
        return None

def generate_entries_for_all_timeframes():
    """Генерирует точки входа для всех таймфреймов"""
    logger.info("🚀 Генерация точек входа для всех таймфреймов")
    
    all_entries = []
    
    # Генерируем точки входа на основе 1m агрегатов
    entries_1m = gen_entries(iv_table='iv_agg_1m', trend_table='trend_signals_15m')
    if entries_1m is not None:
        all_entries.append(entries_1m)
        logger.info(f"✅ Сгенерировано {len(entries_1m)} точек входа для 1m")
    
    # Также можно добавить точки входа на основе 15m агрегатов
    entries_15m = gen_entries(iv_table='iv_agg_15m', trend_table='trend_signals_1h')
    if entries_15m is not None:
        all_entries.append(entries_15m)
        logger.info(f"✅ Сгенерировано {len(entries_15m)} точек входа для 15m")
    
    # Объединяем все точки входа
    if all_entries:
        combined_entries = pd.concat(all_entries, ignore_index=True)
        
        # Сохраняем общий файл
        combined_entries.to_csv('entry_points_all.csv', index=False)
        
        logger.info(f"💾 Общий файл: {len(combined_entries)} точек входа")
        
        return combined_entries
    
    return None

def analyze_entry_distribution():
    """Анализирует распределение точек входа"""
    try:
        df = pd.read_csv('entry_points.csv')
        
        if df.empty:
            logger.warning("⚠️ Нет точек входа для анализа")
            return
        
        logger.info("📊 Анализ распределения точек входа:")
        
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
        
        # Статистика по IV spike
        print(f"\nСтатистика IV spike:")
        print(f"  Среднее: {df['iv_spike'].mean():.2f}%")
        print(f"  Медиана: {df['iv_spike'].median():.2f}%")
        
        # Статистика по skew
        print(f"\nСтатистика skew:")
        print(f"  Среднее: {df['skew'].mean():.4f}")
        print(f"  Медиана: {df['skew'].median():.4f}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка анализа распределения: {e}")

def run_entry_generator_demo():
    """Демонстрация генерации точек входа"""
    logger.info("🎯 Демонстрация генерации точек входа")
    
    # Генерируем точки входа для всех таймфреймов
    all_entries = generate_entries_for_all_timeframes()
    
    if all_entries is not None:
        logger.info("✅ Точки входа успешно сгенерированы")
        
        # Анализируем распределение
        analyze_entry_distribution()
        
        # Показываем общую статистику
        print(f"\n📊 Общая статистика:")
        print(f"Всего точек входа: {len(all_entries)}")
        print(f"Уникальных направлений: {all_entries['direction'].nunique()}")
        print(f"Таймфреймы: {all_entries['timeframe'].unique()}")
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация генерации точек входа
    run_entry_generator_demo()
