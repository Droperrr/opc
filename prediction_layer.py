#!/usr/bin/env python3
"""
Базовый Prediction Layer для системы Error-Driven Adaptive Blocks
Реализует простые методы прогнозирования с использованием модуля совместимости
"""

import numpy as np
import pandas as pd
from typing import List, Union, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

# Импорт модуля совместимости
from compatibility import safe_float, safe_mean, safe_std, safe_array, safe_divide

# Настройка логирования
logger = logging.getLogger(__name__)

class PredictionLayer:
    """Базовый слой прогнозирования с поддержкой различных методов"""
    
    def __init__(self, window_size: int = 5):
        """
        Инициализация слоя прогнозирования
        
        Args:
            window_size: Размер окна для анализа
        """
        self.window_size = window_size
        self.methods = {
            'simple_moving_average': self._simple_moving_average,
            'weighted_moving_average': self._weighted_moving_average,
            'autoregressive': self._autoregressive,
            'exponential_smoothing': self._exponential_smoothing
        }
        logger.info(f"🔮 PredictionLayer инициализирован с окном {window_size}")
    
    def predict_next_price(self, prices: Union[List, np.ndarray, pd.Series], 
                          method: str = 'simple_moving_average') -> float:
        """
        Простой прогноз следующей цены
        
        Args:
            prices: Исторические цены
            method: Метод прогнозирования
            
        Returns:
            float: Прогнозируемая цена
        """
        try:
            if prices is None or len(prices) == 0:
                logger.warning("Пустой массив цен для прогнозирования")
                return 0.0
            
            # Преобразование в безопасный массив
            safe_prices = safe_array(prices)
            
            if len(safe_prices) < self.window_size:
                logger.warning(f"Недостаточно данных для окна {self.window_size}")
                return safe_mean(safe_prices)
            
            # Выбор метода прогнозирования
            if method not in self.methods:
                logger.warning(f"Неизвестный метод {method}, используется simple_moving_average")
                method = 'simple_moving_average'
            
            prediction = self.methods[method](safe_prices)
            
            # Проверка на валидность
            if np.isnan(prediction) or np.isinf(prediction):
                logger.warning(f"Некорректный прогноз методом {method}")
                return safe_mean(safe_prices[-self.window_size:])
            
            logger.debug(f"Прогноз методом {method}: {prediction}")
            return float(prediction)
            
        except Exception as e:
            logger.error(f"Ошибка прогнозирования: {e}")
            return 0.0
    
    def _simple_moving_average(self, prices: np.ndarray) -> float:
        """Простое скользящее среднее"""
        recent_prices = prices[-self.window_size:]
        return safe_mean(recent_prices)
    
    def _weighted_moving_average(self, prices: np.ndarray) -> float:
        """Взвешенное скользящее среднее"""
        recent_prices = prices[-self.window_size:]
        
        # Веса: более свежие цены имеют больший вес
        weights = np.arange(1, len(recent_prices) + 1, dtype=np.float32)
        weights = weights / weights.sum()
        
        weighted_sum = 0.0
        for i, price in enumerate(recent_prices):
            weighted_sum += safe_float(price) * weights[i]
        
        return weighted_sum
    
    def _autoregressive(self, prices: np.ndarray) -> float:
        """Простейший авторегрессионный прогноз"""
        if len(prices) < 2:
            return safe_mean(prices)
        
        # Простая линейная экстраполяция
        recent_prices = prices[-self.window_size:]
        
        if len(recent_prices) < 2:
            return safe_mean(recent_prices)
        
        # Вычисляем тренд
        x = np.arange(len(recent_prices), dtype=np.float32)
        y = recent_prices.astype(np.float32)
        
        # Простая линейная регрессия
        n = len(x)
        sum_x = np.sum(x)
        sum_y = np.sum(y)
        sum_xy = np.sum(x * y)
        sum_x2 = np.sum(x * x)
        
        # Коэффициенты
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return safe_mean(recent_prices)
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        intercept = (sum_y - slope * sum_x) / n
        
        # Прогноз на следующий период
        next_x = len(recent_prices)
        prediction = slope * next_x + intercept
        
        return float(prediction)
    
    def _exponential_smoothing(self, prices: np.ndarray, alpha: float = 0.3) -> float:
        """Экспоненциальное сглаживание"""
        if len(prices) == 0:
            return 0.0
        
        # Начальное значение
        smoothed = safe_float(prices[0])
        
        # Применяем экспоненциальное сглаживание
        for price in prices[1:]:
            smoothed = alpha * safe_float(price) + (1 - alpha) * smoothed
        
        return float(smoothed)
    
    def predict_multiple_steps(self, prices: Union[List, np.ndarray, pd.Series], 
                              steps: int = 5, method: str = 'simple_moving_average') -> List[float]:
        """
        Прогноз на несколько шагов вперед
        
        Args:
            prices: Исторические цены
            steps: Количество шагов прогноза
            method: Метод прогнозирования
            
        Returns:
            List[float]: Список прогнозов
        """
        try:
            predictions = []
            current_prices = safe_array(prices)
            
            for step in range(steps):
                prediction = self.predict_next_price(current_prices, method)
                predictions.append(prediction)
                
                # Добавляем прогноз к историческим данным для следующего шага
                current_prices = np.append(current_prices, prediction)
            
            logger.info(f"Создан прогноз на {steps} шагов методом {method}")
            return predictions
            
        except Exception as e:
            logger.error(f"Ошибка многошагового прогнозирования: {e}")
            return [0.0] * steps
    
    def calculate_prediction_confidence(self, prices: Union[List, np.ndarray, pd.Series], 
                                      method: str = 'simple_moving_average') -> float:
        """
        Расчет уверенности в прогнозе на основе стабильности данных
        
        Args:
            prices: Исторические цены
            method: Метод прогнозирования
            
        Returns:
            float: Уверенность от 0.0 до 1.0
        """
        try:
            safe_prices = safe_array(prices)
            
            if len(safe_prices) < self.window_size:
                return 0.0
            
            # Вычисляем стабильность данных
            recent_prices = safe_prices[-self.window_size:]
            volatility = safe_std(recent_prices)
            mean_price = safe_mean(recent_prices)
            
            if mean_price == 0:
                return 0.0
            
            # Коэффициент вариации (обратно пропорционален уверенности)
            cv = safe_divide(volatility, mean_price)
            
            # Преобразуем в уверенность (чем меньше CV, тем больше уверенность)
            confidence = max(0.0, min(1.0, 1.0 - cv))
            
            logger.debug(f"Уверенность в прогнозе: {confidence:.3f}")
            return confidence
            
        except Exception as e:
            logger.error(f"Ошибка расчета уверенности: {e}")
            return 0.0
    
    def get_method_performance(self, prices: Union[List, np.ndarray, pd.Series], 
                              actual_prices: Union[List, np.ndarray, pd.Series]) -> Dict[str, float]:
        """
        Оценка производительности различных методов прогнозирования
        
        Args:
            prices: Исторические цены для обучения
            actual_prices: Фактические цены для сравнения
            
        Returns:
            Dict[str, float]: Метрики производительности
        """
        try:
            safe_prices = safe_array(prices)
            safe_actual = safe_array(actual_prices)
            
            if len(safe_prices) != len(safe_actual):
                logger.warning("Разная длина массивов цен")
                return {}
            
            performance = {}
            
            for method_name in self.methods.keys():
                # Прогнозируем каждую точку
                predictions = []
                
                for i in range(self.window_size, len(safe_prices)):
                    historical = safe_prices[:i]
                    prediction = self.predict_next_price(historical, method_name)
                    predictions.append(prediction)
                
                # Сравниваем с фактическими значениями
                actual_values = safe_actual[self.window_size:]
                
                if len(predictions) == len(actual_values):
                    # Вычисляем метрики
                    mae = safe_mean([abs(p - a) for p, a in zip(predictions, actual_values)])
                    mse = safe_mean([(p - a) ** 2 for p, a in zip(predictions, actual_values)])
                    rmse = safe_sqrt(mse)
                    
                    performance[method_name] = {
                        'mae': mae,
                        'mse': mse,
                        'rmse': rmse
                    }
            
            logger.info(f"Оценка производительности методов завершена")
            return performance
            
        except Exception as e:
            logger.error(f"Ошибка оценки производительности: {e}")
            return {}

def create_sample_predictions():
    """Создание примеров прогнозов для тестирования"""
    logger.info("🔮 Создание примеров прогнозов")
    
    # Создаем тестовые данные
    np.random.seed(42)
    base_price = 100.0
    trend = 0.1
    noise = 0.05
    
    prices = []
    for i in range(50):
        price = base_price + trend * i + np.random.normal(0, noise * base_price)
        prices.append(price)
    
    # Создаем слой прогнозирования
    predictor = PredictionLayer(window_size=10)
    
    # Тестируем различные методы
    methods = ['simple_moving_average', 'weighted_moving_average', 'autoregressive', 'exponential_smoothing']
    
    results = {}
    for method in methods:
        prediction = predictor.predict_next_price(prices, method)
        confidence = predictor.calculate_prediction_confidence(prices, method)
        
        results[method] = {
            'prediction': prediction,
            'confidence': confidence
        }
        
        logger.info(f"Метод {method}: прогноз={prediction:.2f}, уверенность={confidence:.3f}")
    
    return results

if __name__ == "__main__":
    # Настройка логирования для тестирования
    logging.basicConfig(level=logging.INFO)
    
    print("🔮 Тестирование Prediction Layer...")
    
    # Создание примеров
    results = create_sample_predictions()
    
    print("✅ Prediction Layer готов к использованию")
    print(f"📊 Протестировано методов: {len(results)}")

