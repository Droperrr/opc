#!/usr/bin/env python3
"""
Модуль логирования для проекта Bybit IV Data Collector
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logger(name='bybit_iv_logger', log_file='bybit_iv.log', max_bytes=10*1024*1024, backup_count=5):
    """
    Настройка логгера с ротацией файлов и выводом в консоль
    
    Args:
        name (str): Имя логгера
        log_file (str): Путь к файлу логов
        max_bytes (int): Максимальный размер файла в байтах (по умолчанию 10MB)
        backup_count (int): Количество файлов для ротации
    
    Returns:
        logging.Logger: Настроенный логгер
    """
    
    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Очищаем существующие обработчики (если есть)
    if logger.handlers:
        logger.handlers.clear()
    
    # Создаем форматтер для логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Обработчик для файла с ротацией
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Добавляем обработчики к логгеру
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_logger():
    """
    Получить настроенный логгер для проекта
    
    Returns:
        logging.Logger: Настроенный логгер
    """
    return setup_logger()

# Создаем глобальный логгер
logger = get_logger()

if __name__ == "__main__":
    # Тестирование логгера
    logger.info("🔧 Логгер успешно инициализирован")
    logger.info("📊 Начинаем тестирование логирования")
    
    # Симулируем успешные операции
    logger.info("✅ API подключение установлено")
    logger.info("📈 Получены данные IV для SOL-26SEP25-360-P-USDT")
    logger.info("💾 Файл sol_option_iv_current.csv сохранен")
    
    # Симулируем ошибки
    logger.error("❌ Ошибка API: HTTP 404 - Endpoint не найден")
    logger.error("❌ Исключение: Connection timeout")
    
    print("\n🎯 Тест логирования завершен!")
