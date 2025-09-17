#!/usr/bin/env python3
"""
Генератор сигналов для MVP-стратегии на основе базиса
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
from logger import get_logger

logger = get_logger()

def calculate_thresholds(data: pd.DataFrame, z_score_threshold: float = 2.0) -> Dict[str, float]:
    """
    Рассчитывает пороги для генерации сигналов на основе комбинированного z-score.
    
    Args:
        data: DataFrame с признаками
        z_score_threshold: Пороговое значение z-score
        
    Returns:
        Dict с порогами
    """
    try:
        # Проверяем, что в данных есть необходимые колонки
        required_columns = ['combined_z_score']
        for col in required_columns:
            if col not in data.columns:
                logger.error(f"❌ Отсутствует колонка {col} в данных")
                return {}
        
        # Рассчитываем статистики на обучающем срезе
        mean_combined_z = data['combined_z_score'].mean()
        std_combined_z = data['combined_z_score'].std()
        
        # Определяем пороги
        upper_threshold = mean_combined_z + z_score_threshold * std_combined_z
        lower_threshold = mean_combined_z - z_score_threshold * std_combined_z
        
        thresholds = {
            'upper_threshold': upper_threshold,
            'lower_threshold': lower_threshold,
            'mean_combined_z': mean_combined_z,
            'std_combined_z': std_combined_z
        }
        
        logger.info(f"📊 Пороги рассчитаны: нижний = {lower_threshold:.4f}, верхний = {upper_threshold:.4f}")
        
        return thresholds
        
    except Exception as e:
        logger.error(f"❌ Ошибка расчета порогов: {e}")
        return {}

def generate_signals_from_data(data: pd.DataFrame, thresholds: Dict[str, float], z_score_threshold: float = 2.0, sma_period: int = 20) -> pd.DataFrame:
    """
    Генерирует сигналы на основе данных и порогов с новой логикой и SMA(20) фильтром.
    
    Args:
        data: DataFrame с признаками
        thresholds: Пороги для генерации сигналов
        z_score_threshold: Пороговое значение z-score
        
    Returns:
        DataFrame с сигналами
    """
    try:
        # Проверяем, что в данных есть необходимые колонки
        required_columns = ['time', 'spot_price', 'combined_z_score']
        for col in required_columns:
            if col not in data.columns:
                logger.error(f"❌ Отсутствует колонка {col} в данных")
                return pd.DataFrame()
        
        # Создаем копию данных для работы
        df = data.copy()
        
        # Рассчитываем SMA с заданным периодом
        sma_column = f'SMA_{sma_period}'
        df[sma_column] = df['spot_price'].rolling(window=sma_period).mean()
        # Заполняем NaN значения в начале
        df[sma_column] = df[sma_column].fillna(df['spot_price'])
        logger.info(f"📊 Рассчитана {sma_column} для тренд-фильтра")
        
        # Инициализируем колонки сигналов
        df['signal'] = 'HOLD'
        df['confidence'] = 0.0
        df['reason'] = ''
        
        # Получаем пороги
        upper_threshold = thresholds.get('upper_threshold', z_score_threshold)
        lower_threshold = thresholds.get('lower_threshold', -z_score_threshold)
        
        # Упрощенная логика генерации сигналов (инвертированная)
        # LONG сигналы: combined_z_score < lower_threshold (аномально низкое значение, ожидаем рост)
        long_signals = (df['combined_z_score'] < lower_threshold)
        df.loc[long_signals, 'signal'] = 'LONG'
        df.loc[long_signals, 'confidence'] = abs(df.loc[long_signals, 'combined_z_score'])
        df.loc[long_signals, 'reason'] = 'z_score_long'
        
        # SHORT сигналы: combined_z_score > upper_threshold (аномально высокое значение, ожидаем падение)
        short_signals = (df['combined_z_score'] > upper_threshold)
        df.loc[short_signals, 'signal'] = 'SHORT'
        df.loc[short_signals, 'confidence'] = abs(df.loc[short_signals, 'combined_z_score'])
        df.loc[short_signals, 'reason'] = 'z_score_short'
        
        # Применяем жесткий тренд-фильтр
        # Правильная логика для LONG: spot_price > SMA_period (торгуем по тренду)
        # Правильная логика для SHORT: spot_price < SMA_period (торгуем по тренду)
        long_filter = (df['signal'] == 'LONG') & (df['spot_price'] > df[sma_column])
        short_filter = (df['signal'] == 'SHORT') & (df['spot_price'] < df[sma_column])
        df.loc[~(long_filter | short_filter), 'signal'] = 'HOLD'
        
        # Фильтруем только сигналы (LONG, SHORT)
        signals_df = df[df['signal'].isin(['LONG', 'SHORT'])].copy()
        
        # Логируем количество отфильтрованных сигналов
        filtered_long = len(df[(df['signal'] == 'LONG') & ~long_filter])
        filtered_short = len(df[(df['signal'] == 'SHORT') & ~short_filter])
        if filtered_long > 0 or filtered_short > 0:
            logger.info(f"📊 Отфильтровано сигналов: LONG={filtered_long}, SHORT={filtered_short} (не соответствуют тренду)")
        
        # Добавляем необходимые колонки для бэктеста
        signals_df['timestamp'] = signals_df['time']
        signals_df['open_price'] = signals_df['spot_price']
        signals_df['close_price'] = signals_df['spot_price']  # Для упрощения используем spot_price как open и close
        signals_df['underlying_price'] = signals_df['spot_price']
        
        # Определяем торговую сессию (упрощенно)
        signals_df['hour'] = pd.to_datetime(signals_df['time']).dt.hour
        signals_df['session'] = 'regular'  # По умолчанию регулярная сессия
        # Азиатская сессия (00:00-08:00 UTC)
        signals_df.loc[(signals_df['hour'] >= 0) & (signals_df['hour'] < 8), 'session'] = 'asian'
        # Европейская сессия (08:00-16:00 UTC)
        signals_df.loc[(signals_df['hour'] >= 8) & (signals_df['hour'] < 16), 'session'] = 'european'
        # Американская сессия (16:00-00:00 UTC)
        signals_df.loc[(signals_df['hour'] >= 16) & (signals_df['hour'] < 24), 'session'] = 'american'
        
        # Таймфрейм
        signals_df['timeframe'] = '1m'
        
        # Добавляем волатильность, если есть в данных
        if 'historical_volatility_24h' in signals_df.columns:
            signals_df['historical_volatility'] = signals_df['historical_volatility_24h']
        else:
            signals_df['historical_volatility'] = 0.0
        
        # Выбираем только нужные колонки
        signal_columns = [
            'timestamp', 'signal', 'confidence', 'reason', 'open_price', 'close_price',
            'underlying_price', 'session', 'timeframe', 'historical_volatility'
        ]
        
        # Проверяем, что все нужные колонки существуют
        existing_columns = [col for col in signal_columns if col in signals_df.columns]
        signals_df = signals_df[existing_columns]
        
        logger.info(f"📈 Сгенерировано {len(signals_df)} сигналов")
        
        return signals_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации сигналов: {e}")
        return pd.DataFrame()

# Пример использования
if __name__ == "__main__":
    # Пример использования функций
    print("Пример генерации сигналов для MVP-стратегии")
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'time': pd.date_range('2023-01-01', periods=100, freq='1min'),
        'spot_price': np.random.normal(100, 1, 100),
        'combined_z_score': np.random.normal(0, 1, 100),
        'historical_volatility_24h': np.random.normal(0.01, 0.005, 100)
    })
    
    # Рассчитываем пороги
    thresholds = calculate_thresholds(test_data, z_score_threshold=2.0)
    
    # Генерируем сигналы
    if thresholds:
        signals = generate_signals_from_data(test_data, thresholds, z_score_threshold=2.0)
        print(f"Сгенерировано {len(signals)} сигналов")
        print(signals.head())