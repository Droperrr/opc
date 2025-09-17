#!/usr/bin/env python3
"""
Скрипт для сбора данных за август 2025 для BTCUSDT и SOLUSDT
"""

import subprocess
import logging
import sys
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('collect_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_command(command):
    """Выполняет команду и возвращает результат"""
    try:
        logger.info(f"Выполнение команды: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Команда выполнена успешно")
            return True
        else:
            logger.error(f"Ошибка выполнения команды: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Исключение при выполнении команды: {e}")
        return False

def collect_data_for_asset(asset, symbol):
    """Собирает данные для конкретного актива"""
    logger.info(f"Начало сбора данных для {asset} ({symbol})")
    
    # Сбор спотовых данных
    cmd_spot = f"python futures_collector.py --market spot --pair {asset}/USDT --symbol {symbol} --tag live_2025 --start 2025-08-01 --end 2025-08-31"
    if not run_command(cmd_spot):
        logger.error(f"Ошибка сбора спотовых данных для {asset}")
        return False
    
    # Сбор фьючерсных данных
    cmd_futures = f"python futures_collector.py --market linear --pair {asset}/USDT --symbol {symbol} --tag live_2025 --start 2025-08-01 --end 2025-08-31"
    if not run_command(cmd_futures):
        logger.error(f"Ошибка сбора фьючерсных данных для {asset}")
        return False
    
    # Сбор данных IV (если доступно)
    try:
        cmd_iv = f"python historical_iv_collector.py --start 2025-08-01 --end 2025-08-31 --max-symbols 50"
        if not run_command(cmd_iv):
            logger.warning(f"Предупреждение: ошибка сбора IV данных для {asset}")
    except Exception as e:
        logger.warning(f"Предупреждение: IV данные недоступны для {asset}: {e}")
    
    # Сбор данных basis
    try:
        cmd_basis = f"python historical_basis_collector.py --symbol {symbol} --tag live_2025 --start 2025-08-01 --end 2025-08-31"
        if not run_command(cmd_basis):
            logger.warning(f"Предупреждение: ошибка сбора basis данных для {asset}")
    except Exception as e:
        logger.warning(f"Предупреждение: basis данные недоступны для {asset}: {e}")
    
    logger.info(f"Завершен сбор данных для {asset}")
    return True

def main():
    """Основная функция"""
    logger.info("Начало сбора данных за август 2025")
    
    # Создаем директорию data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Собираем данные для BTCUSDT
    if not collect_data_for_asset("BTC", "BTCUSDT"):
        logger.error("Ошибка сбора данных для BTC")
        sys.exit(1)
    
    # Собираем данные для SOLUSDT
    if not collect_data_for_asset("SOL", "SOLUSDT"):
        logger.error("Ошибка сбора данных для SOL")
        sys.exit(1)
    
    logger.info("Все данные успешно собраны")

if __name__ == "__main__":
    main()