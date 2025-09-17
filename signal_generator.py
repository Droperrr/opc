import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime, timedelta
from logger import get_logger
import os
import argparse

logger = get_logger()

class SignalGenerator:
    def __init__(self, symbol='BTCUSDT', dataset_tag='training_2023', db_path='server_opc.db'):
        self.symbol = symbol
        self.dataset_tag = dataset_tag
        self.db_path = db_path
        self.signals = []
        
    def load_aggregated_data(self):
        """Загружает агрегированные данные IV, Skew, OI с фильтрацией по symbol и dataset_tag"""
        try:
            # Подключаемся к базе данных
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем данные из таблицы iv_agg с фильтрацией
            df = pd.read_sql_query("""
                SELECT * FROM iv_agg 
                WHERE symbol = ? AND dataset_tag = ?
                ORDER BY time
            """, conn, params=(self.symbol, self.dataset_tag), parse_dates=['time'])
            
            # Переименовываем столбец time в timestamp для совместимости
            df = df.rename(columns={'time': 'timestamp'})
            
            conn.close()
            
            if df.empty:
                logger.warning(f"⚠️ Нет агрегированных данных для {self.symbol} ({self.dataset_tag})")
                return pd.DataFrame()
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"📊 Загружено {len(df)} записей агрегированных данных для {self.symbol} ({self.dataset_tag})")
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки агрегированных данных: {e}")
            return pd.DataFrame()
    
    def load_trend_signals(self):
        """Загружает трендовые сигналы с фильтрацией по symbol и dataset_tag"""
        try:
            # Подключаемся к базе данных
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем данные из таблицы trend_signals_15m с фильтрацией
            df_15m = pd.read_sql_query("""
                SELECT * FROM trend_signals_15m 
                WHERE symbol = ? AND dataset_tag = ?
                ORDER BY timestamp
            """, conn, params=(self.symbol, self.dataset_tag), parse_dates=['timestamp'])
            
            # Загружаем данные из таблицы trend_signals_1h с фильтрацией
            df_1h = pd.read_sql_query("""
                SELECT * FROM trend_signals_1h 
                WHERE symbol = ? AND dataset_tag = ?
                ORDER BY timestamp
            """, conn, params=(self.symbol, self.dataset_tag), parse_dates=['timestamp'])
            
            conn.close()
            
            # Объединяем данные
            df = pd.concat([df_15m, df_1h], ignore_index=True)
            
            if df.empty:
                logger.warning(f"⚠️ Нет трендовых сигналов для {self.symbol} ({self.dataset_tag})")
                return pd.DataFrame()
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"📈 Загружено {len(df)} трендовых сигналов для {self.symbol} ({self.dataset_tag})")
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки трендовых сигналов: {e}")
            return pd.DataFrame()
    
    def load_entry_points(self):
        """Загружает точки входа с фильтрацией по symbol и dataset_tag"""
        try:
            # Подключаемся к базе данных
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем данные из таблицы entry_points с фильтрацией
            df = pd.read_sql_query("""
                SELECT * FROM entry_points 
                WHERE symbol = ? AND dataset_tag = ?
                ORDER BY timestamp
            """, conn, params=(self.symbol, self.dataset_tag), parse_dates=['timestamp'])
            
            conn.close()
            
            if df.empty:
                logger.warning(f"⚠️ Нет точек входа для {self.symbol} ({self.dataset_tag})")
                return pd.DataFrame()
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"🎯 Загружено {len(df)} точек входа для {self.symbol} ({self.dataset_tag})")
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки точек входа: {e}")
            return pd.DataFrame()
    
    def combine_data(self, agg_data, trend_data, entry_data):
        """Объединяет данные по временным меткам"""
        combined_data = []
        
        # Создаем временные индексы для быстрого поиска
        trend_dict = {}
        for _, row in trend_data.iterrows():
            timeframe = row['timeframe']
            if timeframe not in trend_dict:
                trend_dict[timeframe] = {}
            trend_dict[timeframe][row['timestamp']] = row
        
        entry_dict = {}
        for _, row in entry_data.iterrows():
            entry_dict[row['timestamp']] = row
        
        # Обрабатываем каждую запись агрегированных данных
        for _, agg_row in agg_data.iterrows():
            timestamp = agg_row['timestamp']
            timeframe = agg_row['timeframe']
            
            # Ищем соответствующий тренд
            trend_info = None
            if timeframe in trend_dict:
                # Ищем ближайший тренд по времени
                trend_times = list(trend_dict[timeframe].keys())
                if trend_times:
                    # Находим ближайший тренд (не позже текущего времени)
                    valid_trends = [t for t in trend_times if t <= timestamp]
                    if valid_trends:
                        nearest_trend_time = max(valid_trends)
                        trend_info = trend_dict[timeframe][nearest_trend_time]
            
            # Ищем точку входа
            entry_info = entry_dict.get(timestamp)
            
            # Собираем комбинированную запись
            combined_record = {
                'timestamp': timestamp,
                'timeframe': timeframe,
                'underlying_price': agg_row.get('spot_price'),
                'iv_mean_all': agg_row.get('iv_30d'),
                'iv_call_mean': None,  # Для совместимости
                'iv_put_mean': None,   # Для совместимости
                'skew': agg_row.get('skew_30d'),
                'oi_ratio': agg_row.get('oi_total'),
                'skew_percentile': None,  # Для совместимости
                'trend_direction': trend_info['direction'] if trend_info is not None else None,
                'trend_confidence': trend_info['confidence'] if trend_info is not None else None,
                'trend_reason': trend_info['reason'] if trend_info is not None else None,
                'entry_direction': entry_info['direction'] if entry_info is not None else None,
                'entry_confidence': entry_info['confidence'] if entry_info is not None else None,
                'entry_reason': entry_info['reason'] if entry_info is not None else None,
                'iv_spike': entry_info['iv_momentum'] if entry_info is not None else None,
                'symbol': self.symbol,
                'dataset_tag': self.dataset_tag
            }
            
            combined_data.append(combined_record)
        
        return pd.DataFrame(combined_data)
    
    def generate_signals(self, combined_data):
        """Генерирует сигналы на основе объединенных данных"""
        signals = []
        
        logger.info(f"🔍 Анализируем {len(combined_data)} записей для генерации сигналов для {self.symbol} ({self.dataset_tag})")
        
        for _, row in combined_data.iterrows():
            # Пропускаем записи без необходимых данных
            if pd.isna(row['underlying_price']) or pd.isna(row['iv_mean_all']):
                continue
            
            signal = None
            confidence = 0.0
            reason = []
            
            # 1. Фильтр по тренду (15m и 1h)
            if row['trend_direction']:
                if row['trend_direction'] == 'BUY':
                    signal = 'BUY'
                    confidence += row['trend_confidence'] * 0.4  # 40% веса
                    reason.append(f"Trend: {row['trend_direction']} (conf: {row['trend_confidence']:.2f})")
                elif row['trend_direction'] == 'SELL':
                    signal = 'SELL'
                    confidence += row['trend_confidence'] * 0.4  # 40% веса
                    reason.append(f"Trend: {row['trend_direction']} (conf: {row['trend_confidence']:.2f})")
            
            # 2. Фильтр по IV и Skew (более мягкие критерии для демо)
            if not pd.isna(row['skew']):
                if row['skew'] > 0.01:  # Положительный skew (спрос на коллы)
                    if signal == 'BUY':
                        confidence += 0.3  # Подтверждение
                        reason.append(f"Skew bullish: {row['skew']:.4f}")
                    elif signal is None:
                        signal = 'BUY'
                        confidence += 0.3
                        reason.append(f"Skew bullish: {row['skew']:.4f}")
                elif row['skew'] < -0.01:  # Отрицательный skew (спрос на путы)
                    if signal == 'SELL':
                        confidence += 0.3  # Подтверждение
                        reason.append(f"Skew bearish: {row['skew']:.4f}")
                    elif signal is None:
                        signal = 'SELL'
                        confidence += 0.3
                        reason.append(f"Skew bearish: {row['skew']:.4f}")
            
            # 3. Фильтр по OI (если доступен)
            if not pd.isna(row['oi_ratio']):
                if row['oi_ratio'] > 0.55:  # Больше коллов
                    if signal == 'BUY':
                        confidence += 0.2
                        reason.append(f"OI call-heavy: {row['oi_ratio']:.2f}")
                    elif signal is None:
                        signal = 'BUY'
                        confidence += 0.2
                        reason.append(f"OI call-heavy: {row['oi_ratio']:.2f}")
                elif row['oi_ratio'] < 0.45:  # Больше путов
                    if signal == 'SELL':
                        confidence += 0.2
                        reason.append(f"OI put-heavy: {row['oi_ratio']:.2f}")
                    elif signal is None:
                        signal = 'SELL'
                        confidence += 0.2
                        reason.append(f"OI put-heavy: {row['oi_ratio']:.2f}")
            
            # 4. Подтверждение по минутному графику
            if row['entry_direction'] and row['entry_direction'] == signal:
                confidence += row['entry_confidence'] * 0.1  # 10% веса
                reason.append(f"1m confirmation: {row['entry_direction']}")
            
            # Нормализуем confidence до 0-1
            confidence = min(1.0, max(0.0, confidence))
            
            # Сохраняем сигнал только если confidence >= 0.25 (сниженный порог для демо)
            if signal and confidence >= 0.25:
                signals.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'signal': signal,
                    'confidence': round(confidence, 3),
                    'reason': ' | '.join(reason),
                    'underlying_price': row['underlying_price'],
                    'timeframe': row['timeframe'],
                    'iv_mean_all': row['iv_mean_all'],
                    'skew': row['skew'],
                    'oi_ratio': row['oi_ratio'],
                    'trend_direction': row['trend_direction'],
                    'trend_confidence': row['trend_confidence'],
                    'entry_direction': row['entry_direction'],
                    'entry_confidence': row['entry_confidence'],
                    'symbol': self.symbol,
                    'dataset_tag': self.dataset_tag
                })
        
        return signals
    
    def save_signals(self, signals):
        """Сохраняет сигналы в CSV файл"""
        if signals:
            df = pd.DataFrame(signals)
            filename = f'signals_{self.symbol}_{self.dataset_tag}.csv'
            df.to_csv(filename, index=False)
            logger.info(f"💾 Сохранено {len(signals)} сигналов в {filename}")
            return df
        else:
            logger.warning("⚠️ Нет сигналов для сохранения")
            return pd.DataFrame()
    
    def generate_statistics(self, signals_df):
        """Генерирует статистику по сигналам"""
        if signals_df.empty:
            logger.warning("⚠️ Нет данных для статистики")
            return
        
        stats = {
            'total_signals': len(signals_df),
            'buy_signals': len(signals_df[signals_df['signal'] == 'BUY']),
            'sell_signals': len(signals_df[signals_df['signal'] == 'SELL']),
            'avg_confidence': signals_df['confidence'].mean(),
            'avg_iv': signals_df['iv_mean_all'].mean(),
            'avg_skew': signals_df['skew'].mean(),
            'avg_oi_ratio': signals_df['oi_ratio'].mean()
        }
        
        # Статистика по временным фреймам
        timeframe_stats = signals_df.groupby('timeframe').agg({
            'signal': 'count',
            'confidence': 'mean',
            'iv_mean_all': 'mean',
            'skew': 'mean'
        }).round(3)
        
        logger.info(f"📊 Статистика сигналов для {self.symbol} ({self.dataset_tag}):")
        logger.info(f"   Всего сигналов: {stats['total_signals']}")
        logger.info(f"   BUY: {stats['buy_signals']}, SELL: {stats['sell_signals']}")
        logger.info(f"   Средняя уверенность: {stats['avg_confidence']:.3f}")
        logger.info(f"   Средняя IV: {stats['avg_iv']:.3f}")
        logger.info(f"   Средний Skew: {stats['avg_skew']:.4f}")
        logger.info(f"   Средний OI ratio: {stats['avg_oi_ratio']:.3f}")
        
        logger.info("📈 Статистика по временным фреймам:")
        for timeframe, data in timeframe_stats.iterrows():
            logger.info(f"   {timeframe}: {data['signal']} сигналов, conf: {data['confidence']:.3f}")
        
        return stats
    
    def run(self):
        """Основной метод запуска генератора сигналов"""
        logger.info(f"🚀 Запуск генератора сигналов для {self.symbol} ({self.dataset_tag})...")
        
        # Загружаем данные
        agg_data = self.load_aggregated_data()
        trend_data = self.load_trend_signals()
        entry_data = self.load_entry_points()
        
        if agg_data.empty:
            logger.error("❌ Не удалось загрузить агрегированные данные")
            return
        
        # Объединяем данные
        combined_data = self.combine_data(agg_data, trend_data, entry_data)
        logger.info(f"🔗 Объединено {len(combined_data)} записей")
        
        # Генерируем сигналы
        signals = self.generate_signals(combined_data)
        logger.info(f"🎯 Сгенерировано {len(signals)} сигналов")
        
        # Сохраняем сигналы
        signals_df = self.save_signals(signals)
        
        # Генерируем статистику
        if not signals_df.empty:
            self.generate_statistics(signals_df)
            
            # Показываем примеры сигналов
            logger.info("📋 Примеры сигналов:")
            for i, signal in enumerate(signals[:3]):  # Первые 3 сигнала
                logger.info(f"   {i+1}. [{signal['timestamp']}] {signal['signal']} | "
                           f"Conf: {signal['confidence']:.3f} | "
                           f"Price: {signal['underlying_price']} | "
                           f"Reason: {signal['reason'][:50]}...")
        else:
            logger.warning("⚠️ Не удалось сгенерировать сигналы")

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Генератор сигналов для системы OPC')
    parser.add_argument('--symbol', default='BTCUSDT',
                       help='Символ актива (например, BTCUSDT, SOLUSDT)')
    parser.add_argument('--tag', default='training_2023',
                       help='Тег набора данных (например, training_2023, live_2025)')
    parser.add_argument('--db', default='server_opc.db',
                       help='Путь к базе данных')
    
    args = parser.parse_args()
    
    generator = SignalGenerator(args.symbol, args.tag, args.db)
    generator.run()

if __name__ == "__main__":
    main()
