#!/usr/bin/env python3
"""
Скрипт для переноса исторических данных на сервер
Решает проблему потери накопленных за год данных
"""

import os
import subprocess
import zipfile
from pathlib import Path
import shutil

def create_data_archive():
    """Создает архив с историческими данными"""
    
    print("📦 Создание архива с историческими данными...")
    
    # Папки с данными для переноса
    data_folders = [
        'data',
        'backups'
    ]
    
    # CSV файлы с результатами
    csv_files = [
        'signals.csv',
        'signals_enhanced.csv', 
        'signals_optimized.csv',
        'signals_optimized_extended.csv',
        'trend_signals.csv',
        'trend_signals_extended.csv',
        'trend_signals_all.csv',
        'sol_all_options_iv.csv',
        'sol_iv_2025-09-02.csv',
        'sol_iv_2025-09-03.csv',
        'sol_iv_2025-09-04.csv',
        'sol_option_iv_current.csv'
    ]
    
    # JSON файлы с результатами
    json_files = [
        's10_memory_report.json',
        'error_analysis_table.csv',
        'error_statistics.csv'
    ]
    
    # Создаем временную папку для архива
    temp_dir = Path('temp_data_transfer')
    temp_dir.mkdir(exist_ok=True)
    
    # Копируем папки с данными
    for folder in data_folders:
        if os.path.exists(folder):
            dest = temp_dir / folder
            shutil.copytree(folder, dest, dirs_exist_ok=True)
            print(f"✅ Скопирована папка: {folder}")
    
    # Копируем CSV файлы
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            shutil.copy2(csv_file, temp_dir)
            print(f"✅ Скопирован файл: {csv_file}")
    
    # Копируем JSON файлы
    for json_file in json_files:
        if os.path.exists(json_file):
            shutil.copy2(json_file, temp_dir)
            print(f"✅ Скопирован файл: {json_file}")
    
    # Создаем ZIP архив
    archive_name = 'historical_data_complete.zip'
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, temp_dir)
                zipf.write(file_path, arcname)
    
    # Удаляем временную папку
    shutil.rmtree(temp_dir)
    
    # Проверяем размер архива
    archive_size = os.path.getsize(archive_name) / (1024 * 1024)  # MB
    print(f"📊 Создан архив: {archive_name} ({archive_size:.1f} MB)")
    
    return archive_name

def show_transfer_instructions(archive_name):
    """Показывает инструкции по переносу данных"""
    
    print("\n" + "="*60)
    print("🚀 ИНСТРУКЦИИ ПО ПЕРЕНОСУ ДАННЫХ НА СЕРВЕР")
    print("="*60)
    
    print(f"\n📁 Архив создан: {archive_name}")
    print(f"📊 Размер: {os.path.getsize(archive_name) / (1024 * 1024):.1f} MB")
    
    print("\n🔧 Варианты переноса:")
    
    print("\n1️⃣ Через SCP (если SSH работает):")
    print(f"   scp {archive_name} asp_ural@89.169.189.43:~/projects/opc/")
    
    print("\n2️⃣ Через облачное хранилище:")
    print("   - Загрузите архив в Google Drive / Dropbox / Яндекс.Диск")
    print("   - Скачайте на сервере через wget/curl")
    
    print("\n3️⃣ Через USB/внешний носитель:")
    print("   - Скопируйте архив на флешку")
    print("   - Перенесите на сервер физически")
    
    print("\n📋 После переноса на сервере выполните:")
    print("   cd ~/projects/opc")
    print(f"   unzip {archive_name}")
    print("   rm historical_data_complete.zip")
    print("   ls -la data/  # проверка данных")
    
    print("\n✅ Проверка целостности данных:")
    print("   python -c \"import sqlite3; conn=sqlite3.connect('data/sol_iv.db'); print('База данных OK:', len(conn.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()), 'таблиц')\"")

def main():
    """Основная функция"""
    
    print("🎯 ПЕРЕНОС ИСТОРИЧЕСКИХ ДАННЫХ НА СЕРВЕР")
    print("="*50)
    print("Цель: Сохранить накопленные за год данные")
    print("Проблема: Не тратить время и деньги на повторный сбор")
    print("Решение: Перенести готовые данные на сервер")
    
    try:
        # Создаем архив
        archive_name = create_data_archive()
        
        # Показываем инструкции
        show_transfer_instructions(archive_name)
        
        print("\n🎉 ГОТОВО! Архив с историческими данными создан.")
        print("📋 Следуйте инструкциям выше для переноса на сервер.")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
