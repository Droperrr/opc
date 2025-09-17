import pandas as pd
import sqlite3

class HistoricalAnalyzer:
    def __init__(self, db_path='server_opc.db', symbol='BTCUSDT', dataset_tag='training_2023'):
        self.db_path = db_path
        self.symbol = symbol
        self.dataset_tag = dataset_tag
        self.historical_data = None

    def load_historical_data(self):
        # Загружаем все данные один раз для производительности
        if self.historical_data is None:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    # Загружаем данные из таблицы iv_agg с фильтрацией по таймфрейму 1m
                    iv_query = """
                        SELECT * FROM iv_agg
                        WHERE timeframe = '1m'
                        AND symbol = ?
                        AND dataset_tag = ?
                    """
                    iv_df = pd.read_sql_query(iv_query, conn, params=(self.symbol, self.dataset_tag))
                    
                    # Загружаем данные из таблицы trend_signals_15m
                    trend_query = """
                        SELECT * FROM trend_signals_15m
                        WHERE symbol = ?
                        AND dataset_tag = ?
                    """
                    trend_df = pd.read_sql_query(trend_query, conn, params=(self.symbol, self.dataset_tag))
                    
                    # Преобразуем временные метки в datetime
                    iv_df['time'] = pd.to_datetime(iv_df['time'])
                    trend_df['timestamp'] = pd.to_datetime(trend_df['timestamp'])
                    
                    # Переименовываем столбцы для совместимости
                    iv_df = iv_df.rename(columns={'time': 'timestamp', 'iv_30d': 'iv_mean_all', 'skew_30d': 'skew'})
                    
                    # Объединяем данные по временной метке
                    # Используем left join, чтобы сохранить все записи из iv_df
                    self.historical_data = pd.merge_asof(
                        iv_df.sort_values('timestamp'),
                        trend_df.sort_values('timestamp'),
                        on='timestamp',
                        direction='backward',
                        tolerance=pd.Timedelta('30min')
                    )
            except Exception as e:
                # Если таблица не существует, возвращаем пустой DataFrame
                print(f"Предупреждение: Не удалось загрузить исторические данные: {e}")
                self.historical_data = pd.DataFrame()
        return self.historical_data

    def find_analogies(self, current_signal: dict) -> dict:
        """Ищет похожие ситуации в прошлом и анализирует их исход."""
        
        history_df = self.load_historical_data()
        if history_df.empty:
            return {'total_found': 0, 'win_rate': 0.0, 'reason': "Нет исторических данных"}

        # Шаг 1: Извлекаем "Вектор Признаков" из словаря current_signal
        iv_spike = current_signal.get('iv_spike', 0)
        skew = current_signal.get('skew', 0)
        trend_confidence = current_signal.get('trend_confidence', 0)
        
        # Шаг 2: Ищем аналоги в истории с допустимыми отклонениями
        # Определяем допуски для каждого признака
        iv_spike_tolerance = abs(iv_spike * 0.15)  # ± 15% от значения в сигнале
        skew_tolerance = 0.02  # ± 0.02 от значения в сигнале
        trend_confidence_tolerance = 0.15  # ± 0.15 от значения в сигнале
        
        # Фильтруем исторические данные по допустимым отклонениям
        analogies_df = history_df[
            (history_df['iv_mean_all'].diff().fillna(0) >= (iv_spike - iv_spike_tolerance)) &
            (history_df['iv_mean_all'].diff().fillna(0) <= (iv_spike + iv_spike_tolerance)) &
            (history_df['skew'] >= (skew - skew_tolerance)) &
            (history_df['skew'] <= (skew + skew_tolerance)) &
            (history_df['confidence'].fillna(0) >= (trend_confidence - trend_confidence_tolerance)) &
            (history_df['confidence'].fillna(0) <= (trend_confidence + trend_confidence_tolerance))
        ]
        
        total_found = len(analogies_df)
        if total_found == 0:
            return {'total_found': 0, 'win_rate': 0.0, 'reason': "Аналоги не найдены"}

        # Шаг 3: Анализируем исход аналогов (реальный анализ прибыльности)
        # Для каждого найденного аналога проверяем реальное движение цены в течение следующих 60 минут
        profitable_count = 0
        
        # Получаем направление сигнала из current_signal
        signal_direction = current_signal.get('direction', 'NEUTRAL')
        
        # Целевой уровень изменения цены (0.5%)
        target_change = 0.005
        
        # Для каждого аналога проверяем реальное движение цены
        for _, analogy_row in analogies_df.iterrows():
            analogy_timestamp = analogy_row['timestamp']
            analogy_price = analogy_row['spot_price']
            
            # Находим срез данных за следующие 60 минут
            future_data = self.historical_data[
                (self.historical_data['timestamp'] > analogy_timestamp) &
                (self.historical_data['timestamp'] <= analogy_timestamp + pd.Timedelta(minutes=60))
            ]
            
            if not future_data.empty:
                # Получаем цены в будущем
                future_prices = future_data['spot_price']
                max_price = future_prices.max()
                min_price = future_prices.min()
                
                # Проверяем, был ли достигнут целевой уровень цены
                if signal_direction == 'BUY' and max_price >= analogy_price * (1 + target_change):
                    # Для BUY: цена выросла на 0.5% или больше
                    profitable_count += 1
                elif signal_direction == 'SELL' and min_price <= analogy_price * (1 - target_change):
                    # Для SELL: цена упала на 0.5% или больше
                    profitable_count += 1
        
        win_rate = profitable_count / total_found if total_found > 0 else 0.0
        
        return {
            'total_found': total_found,
            'profitable_count': profitable_count,
            'win_rate': win_rate,
            'summary': f"{profitable_count} из {total_found} случаев были прибыльными."
        }