#!/usr/bin/env python3
"""
Загрузчик конфигурации для проекта OPC
"""

import yaml
import os
from typing import Dict, Any

def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    Загружает конфигурацию из YAML файла
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Dict[str, Any]: Словарь с конфигурацией
    """
    try:
        # Проверяем существование файла
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")
        
        # Загружаем конфигурацию
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        
        return config or {}
        
    except yaml.YAMLError as e:
        raise ValueError(f"Ошибка парсинга YAML файла: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка загрузки конфигурации: {e}")

# Глобальный экземпляр конфигурации
_config = None

def get_config() -> Dict[str, Any]:
    """
    Возвращает глобальный экземпляр конфигурации
    Загружает конфигурацию при первом вызове
    
    Returns:
        Dict[str, Any]: Словарь с конфигурацией
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config

if __name__ == "__main__":
    # Тестирование загрузчика конфигурации
    try:
        config = get_config()
        print("✅ Конфигурация успешно загружена")
        print(f"Путь к базе данных: {config.get('database', {}).get('path', 'Не указан')}")
        print(f"Веса бэктестера: {config.get('backtester', {}).get('weights', {})}")
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")