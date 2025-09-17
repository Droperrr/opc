#!/usr/bin/env python3
"""
Скрипт для верификации Parquet-файлов
"""
import pandas as pd
import pyarrow.parquet as pq
import os

def analyze_parquet_file(filename):
    """Анализирует Parquet-файл и выводит информацию о нем"""
    if not os.path.exists(filename):
        print(f"Файл {filename} не найден")
        return
    
    try:
        # Читаем Parquet-файл
        table = pq.read_table(filename)
        df = table.to_pandas()
        
        print(f"\nАнализ файла: {filename}")
        print("=" * 50)
        print(f"Общее количество строк: {len(df)}")
        
        if len(df) > 0:
            # Преобразуем столбец minute в datetime, если он существует
            if 'minute' in df.columns:
                df['minute'] = pd.to_datetime(df['minute'])
                min_date = df['minute'].min()
                max_date = df['minute'].max()
                print(f"Минимальная дата: {min_date}")
                print(f"Максимальная дата: {max_date}")
            else:
                print("Столбец 'minute' не найден в файле")
                
            # Показываем первые и последние несколько строк для понимания структуры
            print("\nПервые 5 строк:")
            print(df.head())
            
            print("\nПоследние 5 строк:")
            print(df.tail())
        else:
            print("Файл пуст")
            
    except Exception as e:
        print(f"Ошибка при анализе файла {filename}: {e}")

def main():
    """Основная функция"""
    print("Верификация Parquet-файлов")
    print("=" * 80)
    
    # Анализируем оба файла
    btc_file = "btc_options_1m_2023.parquet"
    sol_file = "sol_options_1m_2023.parquet"
    
    analyze_parquet_file(btc_file)
    analyze_parquet_file(sol_file)

if __name__ == "__main__":
    main()