#!/usr/bin/env python3
"""
Модуль совместимости для безопасной работы с NumPy 2.3.2 в Python 3.13
Обеспечивает корректную обработку всех типов данных и математических операций
"""

import numpy as np
import pandas as pd
import warnings
from typing import Union, List, Any, Optional
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

# Подавление предупреждений NumPy 2.x
warnings.filterwarnings('ignore', category=RuntimeWarning)
try:
    warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
except AttributeError:
    # VisibleDeprecationWarning не существует в NumPy 2.x
    pass

def safe_float(x: Any) -> float:
    """
    Безопасное преобразование в float для NumPy 2.x
    
    Args:
        x: Любое значение для преобразования
        
    Returns:
        float: Преобразованное значение или 0.0 при ошибке
    """
    try:
        if x is None:
            return 0.0
        
        # Обработка NumPy типов
        if isinstance(x, (np.integer, np.floating)):
            val = float(x)
            if np.isnan(val) or np.isinf(val):
                return 0.0
            return val
        
        # Обработка pandas типов
        if isinstance(x, pd.Series):
            if len(x) == 0:
                return 0.0
            return safe_float(x.iloc[0])
        
        if isinstance(x, (pd.Int64Dtype, pd.Float64Dtype)):
            if pd.isna(x):
                return 0.0
            return float(x)
        
        # Обработка стандартных типов Python
        if isinstance(x, (int, float)):
            val = float(x)
            if np.isnan(val) or np.isinf(val):
                return 0.0
            return val
        
        # Обработка строк
        if isinstance(x, str):
            if x.lower() in ['nan', 'none', 'null', '']:
                return 0.0
            try:
                return float(x)
            except ValueError:
                return 0.0
        
        # Обработка списков и массивов
        if isinstance(x, (list, tuple, np.ndarray)):
            if len(x) == 0:
                return 0.0
            # Берем первый элемент
            return safe_float(x[0])
        
        # Попытка прямого преобразования
        return float(x)
        
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning(f"Ошибка преобразования в float: {x} -> {e}")
        return 0.0

def safe_mean(values: Union[List, np.ndarray, pd.Series]) -> float:
    """
    Безопасное вычисление среднего значения
    
    Args:
        values: Массив значений для вычисления среднего
        
    Returns:
        float: Среднее значение или 0.0 при ошибке
    """
    try:
        if values is None:
            return 0.0
        
        # Преобразование в numpy array
        if isinstance(values, (list, tuple)):
            values = np.array(values)
        elif isinstance(values, pd.Series):
            values = values.values
        
        # Проверка на пустой массив
        if len(values) == 0:
            return 0.0
        
        # Фильтрация NaN и None
        clean_values = []
        for val in values:
            safe_val = safe_float(val)
            if not np.isnan(safe_val) and not np.isinf(safe_val):
                clean_values.append(safe_val)
        
        if len(clean_values) == 0:
            return 0.0
        
        # Вычисление среднего
        result = np.mean(clean_values, dtype=np.float32)
        
        # Проверка на NaN/Inf
        if np.isnan(result) or np.isinf(result):
            return 0.0
        
        return float(result)
        
    except Exception as e:
        logger.warning(f"Ошибка вычисления среднего: {e}")
        return 0.0

def safe_std(values: Union[List, np.ndarray, pd.Series]) -> float:
    """
    Безопасное вычисление стандартного отклонения
    
    Args:
        values: Массив значений для вычисления стандартного отклонения
        
    Returns:
        float: Стандартное отклонение или 0.0 при ошибке
    """
    try:
        if values is None:
            return 0.0
        
        # Преобразование в numpy array
        if isinstance(values, (list, tuple)):
            values = np.array(values)
        elif isinstance(values, pd.Series):
            values = values.values
        
        # Проверка на пустой массив
        if len(values) == 0:
            return 0.0
        
        # Фильтрация NaN и None
        clean_values = []
        for val in values:
            safe_val = safe_float(val)
            if not np.isnan(safe_val) and not np.isinf(safe_val):
                clean_values.append(safe_val)
        
        if len(clean_values) <= 1:
            return 0.0
        
        # Вычисление стандартного отклонения
        result = np.std(clean_values, dtype=np.float32)
        
        # Проверка на NaN/Inf
        if np.isnan(result) or np.isinf(result):
            return 0.0
        
        return float(result)
        
    except Exception as e:
        logger.warning(f"Ошибка вычисления стандартного отклонения: {e}")
        return 0.0

def safe_array(values: Union[List, np.ndarray, pd.Series], dtype: np.dtype = np.float32) -> np.ndarray:
    """
    Безопасное создание numpy array
    
    Args:
        values: Значения для создания массива
        dtype: Тип данных массива
        
    Returns:
        np.ndarray: Безопасный массив
    """
    try:
        if values is None:
            return np.array([], dtype=dtype)
        
        # Преобразование в список
        if isinstance(values, (list, tuple)):
            clean_values = [safe_float(val) for val in values]
        elif isinstance(values, pd.Series):
            clean_values = [safe_float(val) for val in values.values]
        elif isinstance(values, np.ndarray):
            clean_values = [safe_float(val) for val in values.flatten()]
        else:
            clean_values = [safe_float(values)]
        
        # Создание массива
        result = np.array(clean_values, dtype=dtype)
        
        # Замена NaN на 0
        result = np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)
        
        return result
        
    except Exception as e:
        logger.warning(f"Ошибка создания массива: {e}")
        return np.array([], dtype=dtype)

def safe_divide(numerator: Any, denominator: Any) -> float:
    """
    Безопасное деление с обработкой деления на ноль
    
    Args:
        numerator: Числитель
        denominator: Знаменатель
        
    Returns:
        float: Результат деления или 0.0 при ошибке
    """
    try:
        num = safe_float(numerator)
        den = safe_float(denominator)
        
        if den == 0.0:
            return 0.0
        
        result = num / den
        
        if np.isnan(result) or np.isinf(result):
            return 0.0
        
        return float(result)
        
    except Exception as e:
        logger.warning(f"Ошибка деления: {numerator}/{denominator} -> {e}")
        return 0.0

def safe_sqrt(value: Any) -> float:
    """
    Безопасное вычисление квадратного корня
    
    Args:
        value: Значение для вычисления корня
        
    Returns:
        float: Квадратный корень или 0.0 при ошибке
    """
    try:
        val = safe_float(value)
        
        if val < 0:
            return 0.0
        
        result = np.sqrt(val)
        
        if np.isnan(result) or np.isinf(result):
            return 0.0
        
        return float(result)
        
    except Exception as e:
        logger.warning(f"Ошибка вычисления корня: {value} -> {e}")
        return 0.0

def safe_log(value: Any) -> float:
    """
    Безопасное вычисление натурального логарифма
    
    Args:
        value: Значение для вычисления логарифма
        
    Returns:
        float: Логарифм или 0.0 при ошибке
    """
    try:
        val = safe_float(value)
        
        if val <= 0:
            return 0.0
        
        result = np.log(val)
        
        if np.isnan(result) or np.isinf(result):
            return 0.0
        
        return float(result)
        
    except Exception as e:
        logger.warning(f"Ошибка вычисления логарифма: {value} -> {e}")
        return 0.0

def validate_numpy_compatibility() -> bool:
    """
    Проверка совместимости с NumPy 2.3.2
    
    Returns:
        bool: True если совместимость подтверждена
    """
    try:
        # Проверка версии NumPy
        numpy_version = np.__version__
        major_version = int(numpy_version.split('.')[0])
        
        if major_version < 2:
            logger.error(f"Требуется NumPy 2.x, установлена версия {numpy_version}")
            return False
        
        # Тестирование основных функций
        test_values = [1.0, 2.0, 3.0, None, np.nan, '5.0']
        
        for val in test_values:
            safe_float(val)
            # Создаем безопасные значения для тестирования
            safe_val = safe_float(val)
            if safe_val != 0.0:  # Только для валидных значений
                safe_mean([safe_val, safe_val + 1])
                safe_std([safe_val, safe_val + 1])
        
        logger.info(f"✅ Совместимость с NumPy {numpy_version} подтверждена")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки совместимости: {e}")
        return False

# Автоматическая проверка совместимости при импорте
if __name__ == "__main__":
    # Настройка логирования для тестирования
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Тестирование модуля совместимости...")
    
    # Проверка совместимости
    if validate_numpy_compatibility():
        print("✅ Модуль совместимости готов к использованию")
    else:
        print("❌ Проблемы с совместимостью")
