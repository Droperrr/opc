import sqlite3

# Подключаемся к базе данных
conn = sqlite3.connect('server_opc.db')
cursor = conn.cursor()

# Получаем уникальные таймфреймы из таблицы iv_agg
cursor.execute("SELECT DISTINCT timeframe FROM iv_agg;")
iv_timeframes = cursor.fetchall()

print('Timeframes in iv_agg table:')
for timeframe in iv_timeframes:
    print(f"  {timeframe[0]}")

# Получаем уникальные таймфреймы из таблицы signals
cursor.execute("SELECT DISTINCT timeframe FROM signals;")
signal_timeframes = cursor.fetchall()

print('\nTimeframes in signals table:')
for timeframe in signal_timeframes:
    print(f"  {timeframe[0]}")

# Закрываем соединение
conn.close()