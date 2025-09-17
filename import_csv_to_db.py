import sqlite3
import csv
import os
import glob
from datetime import datetime

# Подключение к базе данных
conn = sqlite3.connect('server_opc.db')
cursor = conn.cursor()

# Получение списка всех CSV-файлов в текущей директории
csv_files = glob.glob('BTC_26MAY23_*.csv')

print(f"Найдено {len(csv_files)} CSV-файлов для импорта")

# Обработка каждого файла
for csv_file in csv_files:
    print(f"Импорт файла: {csv_file}")
    
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        
        # Пропуск заголовка
        next(csv_reader)
        next(csv_reader)  # Пропуск второй строки с URL
        
        # Обработка строк данных
        count = 0
        for row in csv_reader:
            if len(row) >= 9:  # Проверка на достаточное количество столбцов
                try:
                    # Преобразование данных
                    timestamp = int(row[0]) // 1000  # Преобразование из миллисекунд в секунды
                    instrument_name = row[2]
                    open_price = float(row[3])
                    high_price = float(row[4])
                    low_price = float(row[5])
                    close_price = float(row[6])
                    volume = float(row[7])
                    base_volume = float(row[8])
                    
                    # Вставка данных в таблицу
                    cursor.execute('''
                        INSERT OR IGNORE INTO deribit_options_kline 
                        (timestamp, instrument_name, open, high, low, close, volume, cost, ticks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (timestamp, instrument_name, open_price, high_price, low_price, close_price, volume, base_volume, 0))
                    
                    count += 1
                except Exception as e:
                    print(f"Ошибка при обработке строки: {e}")
                    continue
        
        print(f"Импортировано {count} записей из файла {csv_file}")
    
    # Сохранение изменений после каждого файла
    conn.commit()

# Закрытие соединения
conn.close()

print("Импорт завершен")