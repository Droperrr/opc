#!/usr/bin/env python3
import shutil
import sys
import os

def check_disk_space(path, required_gb):
    """
    Проверяет свободное дисковое пространство в указанной директории.
    
    Args:
        path (str): Путь к директории для проверки
        required_gb (float): Требуемое свободное место в гигабайтах
        
    Returns:
        bool: True если достаточно места, False если недостаточно
    """
    # Получаем информацию о дисковом пространстве
    total, used, free = shutil.disk_usage(path)
    
    # Преобразуем в гигабайты
    free_gb = free / (1024**3)
    
    # Проверяем, достаточно ли свободного места
    if free_gb < required_gb:
        print(f"CRITICAL ERROR: Insufficient disk space. Required: {required_gb} GB, Available: {free_gb:.2f} GB. Halting execution. Escalate to the Architect.")
        return False
    else:
        print(f"Disk space check passed. Available: {free_gb:.2f} GB, Required: {required_gb} GB.")
        return True

if __name__ == "__main__":
    # Если скрипт запущен напрямую, проверяем дисковое пространство
    if len(sys.argv) != 3:
        print("Usage: python3 system_check.py <path> <required_gb>")
        sys.exit(1)
    
    path = sys.argv[1]
    try:
        required_gb = float(sys.argv[2])
    except ValueError:
        print("Error: required_gb must be a number")
        sys.exit(1)
    
    if not check_disk_space(path, required_gb):
        sys.exit(1)