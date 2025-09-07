#!/usr/bin/env python3
"""
Скрипт для настройки Git репозитория и публикации оптимизированного кода
"""

import os
import subprocess
import shutil
from pathlib import Path

def run_command(command, cwd=None):
    """Выполняет команду в терминале"""
    try:
        result = subprocess.run(command, shell=True, cwd=cwd, 
                              capture_output=True, text=True, check=True)
        print(f"✅ {command}")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка выполнения команды: {command}")
        print(f"Код ошибки: {e.returncode}")
        if e.stderr:
            print(f"Ошибка: {e.stderr}")
        return False

def setup_git_repository():
    """Настраивает Git репозиторий для оптимизированного проекта"""
    
    print("🚀 Настройка Git репозитория для OPC проекта...")
    
    # 1. Инициализация Git репозитория
    if not run_command("git init -b main"):
        return False
    
    # 2. Добавление удаленного репозитория
    remote_url = "git@github.com:Droperrr/opc.git"
    if not run_command(f"git remote add origin {remote_url}"):
        print("⚠️ Удаленный репозиторий уже существует или произошла ошибка")
    
    # 3. Проверка статуса
    run_command("git status")
    
    print("✅ Git репозиторий настроен успешно!")
    return True

def copy_optimized_files():
    """Копирует оптимизированные файлы в проект"""
    
    print("📁 Копирование оптимизированных файлов...")
    
    # Список файлов для копирования
    files_to_copy = {
        '.gitignore_optimized': '.gitignore',
        'PROJECT_MANIFEST_OPTIMIZED.md': 'PROJECT_MANIFEST.md',
        'requirements_optimized.txt': 'requirements.txt',
        'README_OPTIMIZED.md': 'README.md'
    }
    
    for source, destination in files_to_copy.items():
        if os.path.exists(source):
            shutil.copy2(source, destination)
            print(f"✅ Скопирован: {source} → {destination}")
        else:
            print(f"⚠️ Файл не найден: {source}")
    
    print("✅ Оптимизированные файлы скопированы!")

def add_core_files():
    """Добавляет основные файлы проекта в Git"""
    
    print("📦 Добавление основных файлов в Git...")
    
    # Основные модули системы
    core_files = [
        # Ядро системы
        'main_pipeline.py',
        'prediction_layer.py', 
        'error_monitor.py',
        'block_detector.py',
        'block_analyzer.py',
        'formula_engine.py',
        'block_reporting.py',
        
        # Сбор данных
        'database.py',
        'get_iv.py',
        'futures_collector.py',
        'enrich_options.py',
        'historical_iv_collector.py',
        'historical_basis_collector.py',
        
        # Генерация сигналов
        'signal_generator.py',
        'signal_generator_v2.py',
        'signal_generator_v3.py',
        'trend_signals.py',
        'signals.py',
        'entry_generator.py',
        
        # Бэктестинг
        'backtest_engine.py',
        'backtest_signals.py',
        'advanced_backtest_system.py',
        
        # Инфраструктура
        'scheduler.py',
        'logger.py',
        'compatibility.py',
        'ui_dashboard.py',
        
        # Тестирование
        'test_main_pipeline.py',
        'test_block_components.py',
        'test_compatibility.py',
        'test_error_monitor.py',
        
        # Конфигурация
        'requirements.txt',
        'README.md',
        '.gitignore',
        'PROJECT_MANIFEST.md'
    ]
    
    # Добавляем существующие файлы
    existing_files = []
    for file in core_files:
        if os.path.exists(file):
            existing_files.append(file)
        else:
            print(f"⚠️ Файл не найден: {file}")
    
    if existing_files:
        files_str = " ".join(existing_files)
        if run_command(f"git add {files_str}"):
            print(f"✅ Добавлено {len(existing_files)} файлов в Git")
        else:
            return False
    
    return True

def commit_and_push():
    """Создает коммит и отправляет в репозиторий"""
    
    print("💾 Создание коммита и отправка в репозиторий...")
    
    # Создание коммита
    commit_message = "Initial commit: Optimized OPC trading system"
    if not run_command(f'git commit -m "{commit_message}"'):
        print("⚠️ Нет изменений для коммита или произошла ошибка")
        return False
    
    # Отправка в репозиторий
    if not run_command("git push -u origin main"):
        print("❌ Ошибка отправки в репозиторий")
        return False
    
    print("✅ Код успешно отправлен в репозиторий!")
    return True

def main():
    """Основная функция настройки"""
    
    print("🎯 Настройка оптимизированного Git репозитория для OPC проекта")
    print("=" * 60)
    
    # Проверяем, что мы в правильной директории
    if not os.path.exists('main_pipeline.py'):
        print("❌ Ошибка: main_pipeline.py не найден. Убедитесь, что вы в корне проекта.")
        return False
    
    try:
        # 1. Копируем оптимизированные файлы
        copy_optimized_files()
        
        # 2. Настраиваем Git репозиторий
        if not setup_git_repository():
            return False
        
        # 3. Добавляем основные файлы
        if not add_core_files():
            return False
        
        # 4. Создаем коммит и отправляем
        if not commit_and_push():
            return False
        
        print("\n🎉 УСПЕХ! Оптимизированный проект OPC успешно настроен!")
        print("\n📋 Что было сделано:")
        print("✅ Скопированы оптимизированные конфигурационные файлы")
        print("✅ Настроен Git репозиторий")
        print("✅ Добавлены основные модули системы")
        print("✅ Создан коммит и отправлен в GitHub")
        
        print("\n🔗 Репозиторий доступен по адресу:")
        print("https://github.com/Droperrr/opc")
        
        print("\n🚀 Следующие шаги:")
        print("1. Проверьте репозиторий на GitHub")
        print("2. Настройте автоматическое развертывание")
        print("3. Запустите тесты: pytest test_main_pipeline.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Задача выполнена успешно!")
    else:
        print("\n❌ Задача выполнена с ошибками!")
