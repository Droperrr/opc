import sqlite3
import pandas as pd

# Подключаемся к базе данных
conn = sqlite3.connect('server_opc.db')

# Загружаем данные из таблицы iv_agg для таймфрейма 1m
iv_data = pd.read_sql_query("SELECT * FROM iv_agg WHERE timeframe = '1m' ORDER BY time ASC", conn, parse_dates=['time'])
print(f"IV data shape: {iv_data.shape}")
print(f"IV data timeframe: {iv_data['timeframe'].unique()}")

# Показываем первые несколько записей
print("\nFirst 5 IV records:")
print(iv_data.head())

# Статистика по IV
print(f"\nIV statistics:")
print(f"  Min IV: {iv_data['iv_30d'].min():.4f}")
print(f"  Max IV: {iv_data['iv_30d'].max():.4f}")
print(f"  Mean IV: {iv_data['iv_30d'].mean():.4f}")

# Статистика по skew
print(f"\nSkew statistics:")
print(f"  Min skew: {iv_data['skew_30d'].min():.4f}")
print(f"  Max skew: {iv_data['skew_30d'].max():.4f}")
print(f"  Mean skew: {iv_data['skew_30d'].mean():.4f}")

# Загружаем данные из таблицы signals для таймфрейма 15m
signal_data = pd.read_sql_query("SELECT * FROM signals WHERE timeframe = '15m'", conn, parse_dates=['time'])
print(f"\nSignal data shape: {signal_data.shape}")
print(f"Signal data timeframe: {signal_data['timeframe'].unique()}")

# Показываем первые несколько записей
print("\nFirst 5 signal records:")
print(signal_data.head())

# Статистика по сигналам
print(f"\nSignal statistics:")
print(f"  Unique signals: {signal_data['signal'].unique()}")
print(f"  Signal counts:")
for signal in signal_data['signal'].unique():
    count = signal_data[signal_data['signal'] == signal].shape[0]
    print(f"    {signal}: {count}")

# Статистика по confidence
print(f"\nConfidence statistics:")
print(f"  Min confidence: {signal_data['confidence'].min():.4f}")
print(f"  Max confidence: {signal_data['confidence'].max():.4f}")
print(f"  Mean confidence: {signal_data['confidence'].mean():.4f}")

# Закрываем соединение
conn.close()