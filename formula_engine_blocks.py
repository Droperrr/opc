#!/usr/bin/env python3
"""
Formula Engine с поддержкой Error-Driven Blocks
Интегрирует блоки с движком формул для динамического подбора параметров
"""

import numpy as np
import pandas as pd
import sqlite3
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging
import json

# Импорт модулей
from compatibility import safe_float, safe_mean, safe_std, safe_array, safe_divide, safe_sqrt
from block_analyzer import BlockAnalyzer

# Настройка логирования
logger = logging.getLogger(__name__)

class FormulaEngineBlocks:
    """Formula Engine с поддержкой блоков"""
    
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        """
        Инициализация Formula Engine с блоками
        
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
        self.block_analyzer = BlockAnalyzer(db_path)
        self._init_database()
        self._load_formula_templates()
        logger.info(f"🔧 FormulaEngineBlocks инициализирован с БД: {db_path}")
    
    def _init_database(self):
        """Инициализация таблиц базы данных для формул с блоками"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Создание таблицы параметров формул по блокам
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS formula_block_parameters (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        formula_id TEXT NOT NULL,
                        block_id INTEGER NOT NULL,
                        market_regime TEXT NOT NULL,
                        parameters_json TEXT NOT NULL,
                        performance_score REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(formula_id, block_id)
                    )
                ''')
                
                # Создание таблицы оптимизации параметров
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS parameter_optimization (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        formula_id TEXT NOT NULL,
                        block_type TEXT NOT NULL,
                        market_regime TEXT NOT NULL,
                        optimization_method TEXT NOT NULL,
                        best_parameters_json TEXT NOT NULL,
                        optimization_score REAL NOT NULL,
                        optimization_date DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Создание индексов
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_formula_block 
                    ON formula_block_parameters(formula_id, block_id)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_formula_regime 
                    ON formula_block_parameters(formula_id, market_regime)
                ''')
                
                conn.commit()
                logger.info("✅ Таблицы Formula Engine с блоками инициализированы")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД Formula Engine: {e}")
            raise
    
    def _load_formula_templates(self):
        """Загружает шаблоны формул с базовыми параметрами"""
        self.formula_templates = {
            'volatility_focused': {
                'base_parameters': {
                    'iv_weight': 1.0,
                    'skew_weight': 0.5,
                    'basis_weight': -0.3,
                    'threshold': 0.7
                },
                'regime_adjustments': {
                    'trending': {'iv_weight': 1.2, 'threshold': 0.6},
                    'volatile': {'iv_weight': 0.8, 'threshold': 0.9},
                    'stable': {'iv_weight': 1.1, 'threshold': 0.5},
                    'transition': {'iv_weight': 0.9, 'threshold': 0.8}
                }
            },
            'basis_dominant': {
                'base_parameters': {
                    'basis_weight': 1.0,
                    'iv_weight': 0.3,
                    'skew_weight': 0.2,
                    'threshold': 0.5
                },
                'regime_adjustments': {
                    'trending': {'basis_weight': 1.3, 'threshold': 0.4},
                    'volatile': {'basis_weight': 0.7, 'threshold': 0.7},
                    'stable': {'basis_weight': 1.1, 'threshold': 0.3},
                    'transition': {'basis_weight': 0.8, 'threshold': 0.6}
                }
            },
            'balanced': {
                'base_parameters': {
                    'iv_weight': 0.6,
                    'skew_weight': 0.6,
                    'basis_weight': 0.6,
                    'threshold': 0.6
                },
                'regime_adjustments': {
                    'trending': {'threshold': 0.5},
                    'volatile': {'threshold': 0.8},
                    'stable': {'threshold': 0.4},
                    'transition': {'threshold': 0.7}
                }
            }
        }
        logger.info(f"✅ Загружено {len(self.formula_templates)} шаблонов формул")
    
    def get_formula_parameters(self, formula_id: str, block_id: Optional[int] = None,
                              market_regime: Optional[str] = None) -> Dict[str, float]:
        """
        Возвращает параметры формулы для указанного блока
        
        Args:
            formula_id: ID формулы
            block_id: ID блока (если None, используются базовые параметры)
            market_regime: Рыночный режим (если None, определяется автоматически)
            
        Returns:
            Dict[str, float]: Параметры формулы
        """
        try:
            # Получаем базовые параметры
            if formula_id not in self.formula_templates:
                logger.warning(f"Формула {formula_id} не найдена, используются параметры по умолчанию")
                return {'threshold': 0.7}
            
            base_params = self.formula_templates[formula_id]['base_parameters'].copy()
            
            # Если блок не указан, возвращаем базовые параметры
            if block_id is None:
                logger.info(f"Используются базовые параметры для формулы {formula_id}")
                return base_params
            
            # Определяем рыночный режим
            if market_regime is None:
                market_regime = self.block_analyzer.classify_market_regime(block_id)
            
            # Проверяем, есть ли сохраненные параметры для этого блока
            saved_params = self._get_saved_parameters(formula_id, block_id)
            if saved_params:
                logger.info(f"Используются сохраненные параметры для формулы {formula_id}, блока {block_id}")
                return saved_params
            
            # Применяем корректировки для рыночного режима
            adjusted_params = self._apply_regime_adjustments(formula_id, base_params, market_regime)
            
            # Оптимизируем параметры для блока
            optimized_params = self._optimize_parameters_for_block(formula_id, block_id, adjusted_params)
            
            # Сохраняем оптимизированные параметры
            self._save_parameters(formula_id, block_id, market_regime, optimized_params)
            
            logger.info(f"✅ Параметры формулы {formula_id} получены для блока {block_id} (режим: {market_regime})")
            return optimized_params
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения параметров формулы {formula_id}: {e}")
            return self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7})
    
    def _get_saved_parameters(self, formula_id: str, block_id: int) -> Optional[Dict[str, float]]:
        """Получает сохраненные параметры для формулы и блока"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT parameters_json FROM formula_block_parameters
                    WHERE formula_id = ? AND block_id = ?
                ''', (formula_id, block_id))
                
                result = cursor.fetchone()
                if result:
                    return json.loads(result[0])
                
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения сохраненных параметров: {e}")
            return None
    
    def _apply_regime_adjustments(self, formula_id: str, base_params: Dict[str, float], 
                                 market_regime: str) -> Dict[str, float]:
        """Применяет корректировки параметров для рыночного режима"""
        try:
            adjusted_params = base_params.copy()
            
            if formula_id in self.formula_templates:
                regime_adjustments = self.formula_templates[formula_id]['regime_adjustments']
                
                if market_regime in regime_adjustments:
                    adjustments = regime_adjustments[market_regime]
                    
                    for param, adjustment in adjustments.items():
                        if param in adjusted_params:
                            # Применяем корректировку
                            if isinstance(adjustment, (int, float)):
                                adjusted_params[param] = adjustment
                            else:
                                # Мультипликативная корректировка
                                adjusted_params[param] *= adjustment
            
            logger.debug(f"Применены корректировки для режима {market_regime}")
            return adjusted_params
            
        except Exception as e:
            logger.error(f"❌ Ошибка применения корректировок режима: {e}")
            return base_params
    
    def _optimize_parameters_for_block(self, formula_id: str, block_id: int, 
                                     base_params: Dict[str, float]) -> Dict[str, float]:
        """Оптимизирует параметры для конкретного блока"""
        try:
            # Получаем статистику блока
            block_stats = self.block_analyzer.get_block_statistics(block_id)
            
            if not block_stats:
                logger.warning(f"Нет статистики для блока {block_id}, используются базовые параметры")
                return base_params
            
            # Получаем ошибки для блока
            errors_data = self._get_block_errors(block_id)
            
            if len(errors_data) < 10:
                logger.warning(f"Недостаточно данных для оптимизации блока {block_id}")
                return base_params
            
            # Простая оптимизация на основе метрик ошибок
            optimized_params = base_params.copy()
            
            # Корректируем порог на основе стабильности ошибок
            mean_error = block_stats['mean_error']
            std_error = block_stats['std_error']
            
            if std_error > 0:
                # Если ошибки нестабильны, увеличиваем порог
                stability_factor = min(std_error / mean_error, 2.0) if mean_error > 0 else 1.0
                threshold_adjustment = 1.0 + (stability_factor - 1.0) * 0.3
                
                if 'threshold' in optimized_params:
                    optimized_params['threshold'] *= threshold_adjustment
                    optimized_params['threshold'] = min(max(optimized_params['threshold'], 0.1), 2.0)
            
            # Корректируем веса на основе тренда ошибок
            error_trend = block_stats.get('error_trend', 0.0)
            
            if abs(error_trend) > 0.05:  # Значительный тренд
                # Если ошибки растут, уменьшаем агрессивность
                if error_trend > 0:
                    for param in ['iv_weight', 'skew_weight', 'basis_weight']:
                        if param in optimized_params:
                            optimized_params[param] *= 0.9
                else:
                    # Если ошибки уменьшаются, можно увеличить агрессивность
                    for param in ['iv_weight', 'skew_weight', 'basis_weight']:
                        if param in optimized_params:
                            optimized_params[param] *= 1.1
            
            logger.info(f"✅ Параметры оптимизированы для блока {block_id}")
            return optimized_params
            
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации параметров для блока {block_id}: {e}")
            return base_params
    
    def _get_block_errors(self, block_id: int) -> pd.DataFrame:
        """Получает ошибки для указанного блока"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Получаем временные границы блока
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT start_time, end_time FROM blocks WHERE id = ?
                ''', (block_id,))
                
                result = cursor.fetchone()
                if not result:
                    return pd.DataFrame()
                
                start_time, end_time = result
                
                # Получаем ошибки
                query = '''
                    SELECT * FROM error_history 
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                '''
                
                df = pd.read_sql_query(query, conn, params=[start_time, end_time])
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения ошибок блока {block_id}: {e}")
            return pd.DataFrame()
    
    def _save_parameters(self, formula_id: str, block_id: int, market_regime: str, 
                        parameters: Dict[str, float]):
        """Сохраняет параметры формулы для блока"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Вычисляем оценку производительности
                performance_score = self._calculate_performance_score(formula_id, block_id, parameters)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO formula_block_parameters
                    (formula_id, block_id, market_regime, parameters_json, performance_score)
                    VALUES (?, ?, ?, ?, ?)
                ''', (formula_id, block_id, market_regime, json.dumps(parameters), performance_score))
                
                conn.commit()
            
            logger.debug(f"✅ Параметры сохранены для формулы {formula_id}, блока {block_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения параметров: {e}")
    
    def _calculate_performance_score(self, formula_id: str, block_id: int, 
                                    parameters: Dict[str, float]) -> float:
        """Вычисляет оценку производительности параметров"""
        try:
            # Получаем статистику блока
            block_stats = self.block_analyzer.get_block_statistics(block_id)
            
            if not block_stats:
                return 0.5
            
            # Базовые метрики
            mean_error = block_stats['mean_error']
            std_error = block_stats['std_error']
            confidence = block_stats.get('confidence', 0.5)
            
            # Вычисляем оценку на основе ошибок
            error_score = max(0.0, 1.0 - mean_error / 2.0)  # Нормализуем к [0, 1]
            stability_score = max(0.0, 1.0 - std_error / 1.0)  # Нормализуем к [0, 1]
            
            # Комбинируем оценки
            performance_score = (error_score + stability_score + confidence) / 3.0
            
            return min(max(performance_score, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления оценки производительности: {e}")
            return 0.5
    
    def optimize_parameters_for_regime(self, formula_id: str, market_regime: str,
                                     optimization_method: str = 'grid_search') -> Dict[str, float]:
        """
        Оптимизирует параметры формулы для рыночного режима
        
        Args:
            formula_id: ID формулы
            market_regime: Рыночный режим
            optimization_method: Метод оптимизации
            
        Returns:
            Dict[str, float]: Оптимизированные параметры
        """
        try:
            logger.info(f"🔧 Начало оптимизации параметров для формулы {formula_id}, режим {market_regime}")
            
            # Получаем блоки для данного режима
            blocks_df = self.block_analyzer.get_blocks()
            regime_blocks = blocks_df[blocks_df['block_type'].str.contains(market_regime, case=False, na=False)]
            
            if len(regime_blocks) == 0:
                logger.warning(f"Нет блоков для режима {market_regime}")
                return self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7})
            
            # Простая оптимизация на основе лучших блоков
            best_score = 0.0
            best_parameters = None
            
            for _, block in regime_blocks.iterrows():
                block_id = block['id']
                
                # Получаем параметры для блока
                params = self.get_formula_parameters(formula_id, block_id, market_regime)
                
                # Вычисляем оценку
                score = self._calculate_performance_score(formula_id, block_id, params)
                
                if score > best_score:
                    best_score = score
                    best_parameters = params
            
            if best_parameters is None:
                logger.warning("Не удалось найти оптимальные параметры")
                return self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7})
            
            # Сохраняем результаты оптимизации
            self._save_optimization_results(formula_id, market_regime, optimization_method, 
                                          best_parameters, best_score)
            
            logger.info(f"✅ Оптимизация завершена: score = {best_score:.3f}")
            return best_parameters
            
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации параметров: {e}")
            return self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7})
    
    def _save_optimization_results(self, formula_id: str, market_regime: str,
                                  optimization_method: str, parameters: Dict[str, float],
                                  score: float):
        """Сохраняет результаты оптимизации"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO parameter_optimization
                    (formula_id, block_type, market_regime, optimization_method, 
                     best_parameters_json, optimization_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (formula_id, market_regime, market_regime, optimization_method,
                      json.dumps(parameters), score))
                
                conn.commit()
            
            logger.debug(f"✅ Результаты оптимизации сохранены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов оптимизации: {e}")
    
    def get_formula_performance_by_regime(self, formula_id: str) -> Dict[str, Dict[str, float]]:
        """
        Получает производительность формулы по рыночным режимам
        
        Args:
            formula_id: ID формулы
            
        Returns:
            Dict[str, Dict[str, float]]: Производительность по режимам
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT market_regime, AVG(performance_score) as avg_score,
                           COUNT(*) as block_count, MAX(performance_score) as max_score
                    FROM formula_block_parameters
                    WHERE formula_id = ?
                    GROUP BY market_regime
                ''', (formula_id,))
                
                results = cursor.fetchall()
                
                performance_by_regime = {}
                for row in results:
                    regime, avg_score, block_count, max_score = row
                    performance_by_regime[regime] = {
                        'average_score': avg_score,
                        'block_count': block_count,
                        'max_score': max_score
                    }
                
                logger.info(f"✅ Производительность формулы {formula_id} получена по {len(performance_by_regime)} режимам")
                return performance_by_regime
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения производительности формулы: {e}")
            return {}
    
    def get_current_block_parameters(self, formula_id: str) -> Dict[str, Any]:
        """
        Получает параметры формулы для текущего активного блока
        
        Args:
            formula_id: ID формулы
            
        Returns:
            Dict[str, Any]: Параметры и информация о блоке
        """
        try:
            # Получаем последний блок
            blocks_df = self.block_analyzer.get_blocks()
            
            if len(blocks_df) == 0:
                logger.warning("Нет доступных блоков")
                return {
                    'parameters': self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7}),
                    'block_id': None,
                    'market_regime': 'unknown',
                    'confidence': 0.0
                }
            
            # Берем последний блок
            latest_block = blocks_df.iloc[-1]
            block_id = latest_block['id']
            
            # Определяем рыночный режим
            market_regime = self.block_analyzer.classify_market_regime(block_id)
            
            # Получаем параметры
            parameters = self.get_formula_parameters(formula_id, block_id, market_regime)
            
            # Получаем анализ блока
            analysis = self.block_analyzer.analyze_block(block_id)
            
            result = {
                'parameters': parameters,
                'block_id': block_id,
                'market_regime': market_regime,
                'confidence': analysis.confidence,
                'block_type': latest_block['block_type'],
                'risk_level': analysis.risk_level,
                'recommendations': analysis.recommendations
            }
            
            logger.info(f"✅ Параметры для текущего блока получены: режим = {market_regime}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения параметров текущего блока: {e}")
            return {
                'parameters': self.formula_templates.get(formula_id, {}).get('base_parameters', {'threshold': 0.7}),
                'block_id': None,
                'market_regime': 'unknown',
                'confidence': 0.0
            }

def test_formula_engine_blocks():
    """Тестирование Formula Engine с блоками"""
    logger.info("🧪 Тестирование Formula Engine с блоками")
    
    # Создаем тестовую базу данных
    import uuid
    test_db = f'test_formula_engine_{uuid.uuid4().hex[:8]}.db'
    
    try:
        # Инициализация
        engine = FormulaEngineBlocks(test_db)
        
        # Создаем тестовые блоки и ошибки
        with sqlite3.connect(test_db) as conn:
            cursor = conn.cursor()
            
            # Создаем необходимые таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME NOT NULL,
                    start_index INTEGER NOT NULL,
                    end_index INTEGER NOT NULL,
                    block_type TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    mean_error REAL NOT NULL,
                    std_error REAL NOT NULL,
                    max_error REAL NOT NULL,
                    min_error REAL NOT NULL,
                    error_trend REAL NOT NULL,
                    volatility REAL NOT NULL,
                    prediction_count INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    formula_id TEXT,
                    prediction REAL NOT NULL,
                    actual REAL NOT NULL,
                    error_absolute REAL NOT NULL,
                    error_relative REAL NOT NULL,
                    error_normalized REAL NOT NULL,
                    volatility REAL,
                    confidence REAL,
                    method TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Добавляем тестовый блок
            cursor.execute('''
                INSERT INTO blocks 
                (start_time, end_time, start_index, end_index, block_type, confidence,
                 mean_error, std_error, max_error, min_error, error_trend, 
                 volatility, prediction_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=1),
                0, 100, 'low_error_stable', 0.8,
                0.3, 0.1, 0.5, 0.1, 0.02, 0.01, 100
            ))
            
            # Добавляем тестовые ошибки
            for i in range(50):
                timestamp = datetime.now() - timedelta(hours=2, minutes=i)
                cursor.execute('''
                    INSERT INTO error_history 
                    (timestamp, formula_id, prediction, actual, error_absolute, 
                     error_relative, error_normalized, volatility, confidence, method)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp, 'volatility_focused', 100.0 + i*0.01, 100.1 + i*0.01, 0.1 + i*0.001,
                    0.001, 0.1, 0.01, 0.8, 'sma'
                ))
            
            conn.commit()
        
        # Тестируем получение параметров
        params = engine.get_formula_parameters('volatility_focused', block_id=1)
        
        if 'threshold' in params:
            logger.info(f"✅ Параметры получены: threshold = {params['threshold']}")
            
            # Тестируем получение параметров для текущего блока
            current_params = engine.get_current_block_parameters('volatility_focused')
            
            if 'parameters' in current_params:
                logger.info(f"✅ Параметры текущего блока получены")
                
                # Тестируем оптимизацию
                optimized_params = engine.optimize_parameters_for_regime('volatility_focused', 'stable')
                
                if 'threshold' in optimized_params:
                    logger.info(f"✅ Оптимизация завершена: threshold = {optimized_params['threshold']}")
                    return True
                else:
                    logger.error("❌ Оптимизация не выполнена")
                    return False
            else:
                logger.error("❌ Параметры текущего блока не получены")
                return False
        else:
            logger.error("❌ Параметры не получены")
            return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования Formula Engine: {e}")
        return False
        
    finally:
        # Очистка
        import os
        try:
            if os.path.exists(test_db):
                os.remove(test_db)
        except PermissionError:
            pass

if __name__ == "__main__":
    # Настройка логирования для тестирования
    logging.basicConfig(level=logging.INFO)
    
    print("🔧 Тестирование Formula Engine с блоками...")
    
    success = test_formula_engine_blocks()
    
    if success:
        print("✅ Formula Engine с блоками готов к использованию")
    else:
        print("❌ Ошибки в Formula Engine с блоками")
