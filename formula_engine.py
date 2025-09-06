#!/usr/bin/env python3
"""
Formula Engine с поддержкой динамических параметров
Задача S-04: Адаптивные пороги для volatility_focused
"""

import pandas as pd
import numpy as np
import sqlite3
from logger import get_logger

logger = get_logger()

class FormulaEngine:
    def __init__(self):
        self.db_path = 'data/sol_iv.db'
        
    def calculate_atr(self, df, period=14):
        """
        Рассчитывает Average True Range (ATR) для DataFrame
        """
        try:
            df = df.copy()
            
            # Если нет колонок high/low, используем spot_price как приближение
            if 'high' not in df.columns:
                df['high'] = df['spot_price'] * 1.001  # +0.1%
            if 'low' not in df.columns:
                df['low'] = df['spot_price'] * 0.999   # -0.1%
            
            df['prev_close'] = df['spot_price'].shift(1)
            
            # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
            df['tr1'] = df['high'] - df['low']
            df['tr2'] = abs(df['high'] - df['prev_close'])
            df['tr3'] = abs(df['low'] - df['prev_close'])
            df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            # ATR = скользящее среднее True Range
            df['atr'] = df['true_range'].rolling(window=period).mean()
            
            return df['atr']
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета ATR: {e}")
            return pd.Series([0.01] * len(df))  # Fallback значение
    
    def calculate_dynamic_threshold(self, atr_value, base_threshold=0.7, volatility_factor=1.2, avg_volatility=0.02):
        """
        Вычисляет адаптивный порог на основе волатильности
        """
        try:
            if pd.isna(atr_value) or atr_value == 0:
                return base_threshold
            
            # Нормализуем ATR относительно средней волатильности
            volatility_ratio = atr_value / avg_volatility
            
            # Адаптивный порог
            dynamic_threshold = base_threshold * (1 + volatility_factor * (volatility_ratio - 1))
            
            # Ограничиваем порог разумными пределами
            dynamic_threshold = max(0.3, min(2.0, dynamic_threshold))
            
            return dynamic_threshold
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета адаптивного порога: {e}")
            return base_threshold
    
    def evaluate_formula(self, formula_name, data_row, params=None, use_adaptive_threshold=True):
        """
        Оценивает формулу с поддержкой динамических параметров
        """
        try:
            if params is None:
                params = {}
            
            # Базовые параметры для volatility_focused
            if formula_name == 'volatility_focused':
                a = params.get('a', 1.0)
                b = params.get('b', 0.5)
                c = params.get('c', 0.3)
                base_threshold = params.get('base_threshold', 0.7)
                volatility_factor = params.get('volatility_factor', 1.2)
                
                # Получаем значения из данных
                iv = data_row.get('iv_30d', 0)
                skew = data_row.get('skew_30d', 0)
                basis_rel = data_row.get('basis_rel', 0)
                atr = data_row.get('atr', 0.02)  # Средняя волатильность по умолчанию
                
                # Вычисляем Y
                Y = a * iv + b * skew - c * basis_rel
                
                # Определяем порог
                if use_adaptive_threshold:
                    threshold = self.calculate_dynamic_threshold(atr, base_threshold, volatility_factor)
                else:
                    threshold = base_threshold
                
                # Генерируем сигнал
                if Y > threshold:
                    signal = 'BUY'
                elif Y < -threshold:
                    signal = 'SELL'
                else:
                    signal = 'NEUTRAL'
                
                return {
                    'Y_value': Y,
                    'threshold': threshold,
                    'signal': signal,
                    'atr': atr,
                    'params_used': {
                        'a': a, 'b': b, 'c': c,
                        'base_threshold': base_threshold,
                        'volatility_factor': volatility_factor
                    }
                }
            
            else:
                logger.warning(f"⚠️ Формула {formula_name} не поддерживается")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка оценки формулы: {e}")
            return None
    
    def get_formula_params(self, formula_name):
        """
        Получает параметры формулы из базы данных
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = "SELECT params FROM formulas WHERE name = ? AND status = 'active'"
            cursor.execute(query, (formula_name,))
            result = cursor.fetchone()
            
            conn.close()
            
            if result and result[0]:
                import json
                return json.loads(result[0])
            else:
                # Возвращаем дефолтные параметры
                return {
                    'a': 1.0, 'b': 0.5, 'c': 0.3,
                    'base_threshold': 0.7,
                    'volatility_factor': 1.2
                }
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения параметров формулы: {e}")
            return {}
    
    def update_formula_params(self, formula_name, new_params):
        """
        Обновляет параметры формулы в базе данных
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            import json
            params_json = json.dumps(new_params)
            
            query = "UPDATE formulas SET params = ? WHERE name = ?"
            cursor.execute(query, (params_json, formula_name))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Параметры формулы {formula_name} обновлены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления параметров: {e}")
    
    def batch_evaluate(self, formula_name, data_df, params=None, use_adaptive_threshold=True):
        """
        Пакетная оценка формулы для всего DataFrame
        """
        try:
            results = []
            
            # Рассчитываем ATR если его нет
            if 'atr' not in data_df.columns:
                data_df['atr'] = self.calculate_atr(data_df)
            
            # Получаем параметры формулы
            if params is None:
                params = self.get_formula_params(formula_name)
            
            for i, row in data_df.iterrows():
                result = self.evaluate_formula(formula_name, row, params, use_adaptive_threshold)
                if result:
                    result['time'] = row.get('time', i)
                    result['spot_price'] = row.get('spot_price', 0)
                    results.append(result)
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"❌ Ошибка пакетной оценки: {e}")
            return pd.DataFrame()
    
    def optimize_params(self, formula_name, data_df, target_metric='accuracy'):
        """
        Оптимизирует параметры формулы для максимизации целевой метрики
        """
        try:
            logger.info(f"🔍 Начинаю оптимизацию параметров для {formula_name}")
            
            # Базовые параметры для поиска
            param_ranges = {
                'a': [0.5, 1.0, 1.5, 2.0],
                'b': [0.3, 0.5, 0.7, 1.0],
                'c': [0.2, 0.3, 0.4, 0.5],
                'base_threshold': [0.5, 0.7, 0.9, 1.1],
                'volatility_factor': [0.8, 1.0, 1.2, 1.5]
            }
            
            best_params = None
            best_metric = 0
            
            # Простой grid search
            for a in param_ranges['a']:
                for b in param_ranges['b']:
                    for c in param_ranges['c']:
                        for base_threshold in param_ranges['base_threshold']:
                            for volatility_factor in param_ranges['volatility_factor']:
                                
                                params = {
                                    'a': a, 'b': b, 'c': c,
                                    'base_threshold': base_threshold,
                                    'volatility_factor': volatility_factor
                                }
                                
                                # Оцениваем формулу с текущими параметрами
                                results = self.batch_evaluate(formula_name, data_df, params, True)
                                
                                if not results.empty:
                                    # Рассчитываем метрику
                                    signals = results[results['signal'].isin(['BUY', 'SELL'])]
                                    if len(signals) > 0:
                                        accuracy = len(signals[signals['signal'] == 'BUY']) / len(signals)
                                        
                                        if accuracy > best_metric:
                                            best_metric = accuracy
                                            best_params = params
            
            logger.info(f"✅ Лучшие параметры: {best_params} (accuracy: {best_metric:.3f})")
            return best_params
            
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации параметров: {e}")
            return None

def main():
    """Тестовая функция"""
    engine = FormulaEngine()
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'time': pd.date_range('2024-01-01', periods=100, freq='1min'),
        'spot_price': np.random.uniform(100, 200, 100),
        'iv_30d': np.random.uniform(0.3, 0.8, 100),
        'skew_30d': np.random.uniform(-0.5, 0.5, 100),
        'basis_rel': np.random.uniform(-0.02, 0.02, 100)
    })
    
    # Тестируем формулу
    result = engine.evaluate_formula('volatility_focused', test_data.iloc[0])
    print(f"Результат теста: {result}")

if __name__ == "__main__":
    main()
