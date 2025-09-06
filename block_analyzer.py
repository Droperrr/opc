#!/usr/bin/env python3
"""
Block Analyzer для системы Error-Driven Adaptive Blocks
Анализирует блоки и классифицирует рыночные режимы
"""

import numpy as np
import pandas as pd
import sqlite3
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass

# Импорт модуля совместимости
from compatibility import safe_float, safe_mean, safe_std, safe_array, safe_divide, safe_sqrt

# Настройка логирования
logger = logging.getLogger(__name__)

@dataclass
class BlockAnalysis:
    """Результат анализа блока"""
    block_id: int
    block_type: str
    market_regime: str
    confidence: float
    performance_metrics: Dict[str, float]
    recommendations: List[str]
    risk_level: str

class BlockAnalyzer:
    """Анализатор блоков и рыночных режимов"""
    
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        """
        Инициализация анализатора блоков
        
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
        self.market_regime_thresholds = {
            'trending': {'mean_error': (0.0, 0.8), 'std_error': (0.0, 0.4), 'trend': (-0.1, 0.1)},
            'volatile': {'mean_error': (1.0, 3.0), 'std_error': (0.5, 2.0), 'trend': (-0.2, 0.2)},
            'stable': {'mean_error': (0.0, 0.5), 'std_error': (0.0, 0.3), 'trend': (-0.05, 0.05)},
            'transition': {'mean_error': (0.5, 1.5), 'std_error': (0.3, 0.8), 'trend': (-0.15, 0.15)}
        }
        logger.info(f"📊 BlockAnalyzer инициализирован с БД: {db_path}")
    
    def get_block_statistics(self, block_id: int) -> Dict[str, Any]:
        """
        Получает статистику для указанного блока
        
        Args:
            block_id: ID блока
            
        Returns:
            Dict[str, Any]: Статистика блока
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Получаем данные блока
                cursor.execute('''
                    SELECT * FROM blocks WHERE id = ?
                ''', (block_id,))
                
                block_data = cursor.fetchone()
                
                if not block_data:
                    logger.warning(f"Блок {block_id} не найден")
                    return {}
                
                # Получаем ошибки для этого блока
                cursor.execute('''
                    SELECT * FROM error_history 
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                ''', (block_data[1], block_data[2]))  # start_time, end_time
                
                errors_data = cursor.fetchall()
                
                if not errors_data:
                    logger.warning(f"Нет данных об ошибках для блока {block_id}")
                    return {}
                
                # Анализируем ошибки
                errors_df = pd.DataFrame(errors_data, columns=[
                    'id', 'timestamp', 'formula_id', 'prediction', 'actual',
                    'error_absolute', 'error_relative', 'error_normalized',
                    'volatility', 'confidence', 'method', 'created_at'
                ])
                
                # Вычисляем дополнительные метрики
                additional_metrics = self._calculate_additional_metrics(errors_df)
                
                # Формируем результат
                statistics = {
                    'block_id': block_id,
                    'start_time': block_data[1],
                    'end_time': block_data[2],
                    'block_type': block_data[5],
                    'confidence': block_data[6],
                    'mean_error': block_data[7],
                    'std_error': block_data[8],
                    'max_error': block_data[9],
                    'min_error': block_data[10],
                    'error_trend': block_data[11],
                    'volatility': block_data[12],
                    'prediction_count': block_data[13],
                    'additional_metrics': additional_metrics
                }
                
                logger.info(f"✅ Статистика блока {block_id} получена")
                return statistics
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики блока {block_id}: {e}")
            return {}
    
    def _calculate_additional_metrics(self, errors_df: pd.DataFrame) -> Dict[str, float]:
        """Вычисляет дополнительные метрики для блока"""
        try:
            errors = safe_array(errors_df['error_absolute'])
            relative_errors = safe_array(errors_df['error_relative'])
            normalized_errors = safe_array(errors_df['error_normalized'])
            
            # Базовые метрики
            mae = safe_mean(errors)
            rmse = safe_sqrt(safe_mean(errors ** 2))
            mape = safe_mean(relative_errors) * 100
            
            # Метрики стабильности
            error_stability = 1.0 - safe_divide(safe_std(errors), mae) if mae > 0 else 0.0
            
            # Метрики тренда
            if len(errors) > 1:
                error_trend = self._calculate_error_trend(errors)
                trend_stability = 1.0 - abs(error_trend)
            else:
                error_trend = 0.0
                trend_stability = 1.0
            
            # Метрики волатильности
            volatility_mean = safe_mean(safe_array(errors_df['volatility']))
            volatility_consistency = 1.0 - safe_divide(safe_std(safe_array(errors_df['volatility'])), volatility_mean) if volatility_mean > 0 else 0.0
            
            # Метрики уверенности
            confidence_mean = safe_mean(safe_array(errors_df['confidence']))
            confidence_consistency = 1.0 - safe_divide(safe_std(safe_array(errors_df['confidence'])), confidence_mean) if confidence_mean > 0 else 0.0
            
            return {
                'mae': mae,
                'rmse': rmse,
                'mape': mape,
                'error_stability': error_stability,
                'error_trend': error_trend,
                'trend_stability': trend_stability,
                'volatility_mean': volatility_mean,
                'volatility_consistency': volatility_consistency,
                'confidence_mean': confidence_mean,
                'confidence_consistency': confidence_consistency
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления дополнительных метрик: {e}")
            return {}
    
    def _calculate_error_trend(self, errors: np.ndarray) -> float:
        """Вычисляет тренд ошибок"""
        try:
            if len(errors) < 2:
                return 0.0
            
            # Простая линейная регрессия
            x = np.arange(len(errors))
            y = safe_array(errors)
            
            n = len(x)
            sum_x = np.sum(x)
            sum_y = np.sum(y)
            sum_xy = np.sum(x * y)
            sum_x2 = np.sum(x * x)
            
            # Коэффициент наклона
            denominator = n * sum_x2 - sum_x * sum_x
            if denominator == 0:
                return 0.0
            
            slope = (n * sum_xy - sum_x * sum_y) / denominator
            return float(slope)
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления тренда ошибок: {e}")
            return 0.0
    
    def classify_market_regime(self, block_id: int) -> str:
        """
        Классифицирует рыночный режим для блока
        
        Args:
            block_id: ID блока
            
        Returns:
            str: Классификация рыночного режима
        """
        try:
            statistics = self.get_block_statistics(block_id)
            
            if not statistics:
                return 'unknown'
            
            mean_error = statistics['mean_error']
            std_error = statistics['std_error']
            error_trend = statistics['error_trend']
            
            # Классификация на основе порогов
            best_match = 'unknown'
            best_score = 0.0
            
            for regime, thresholds in self.market_regime_thresholds.items():
                score = self._calculate_regime_score(
                    mean_error, std_error, error_trend, thresholds
                )
                
                if score > best_score:
                    best_score = score
                    best_match = regime
            
            logger.info(f"✅ Блок {block_id} классифицирован как '{best_match}' (score: {best_score:.3f})")
            return best_match
            
        except Exception as e:
            logger.error(f"❌ Ошибка классификации блока {block_id}: {e}")
            return 'unknown'
    
    def _calculate_regime_score(self, mean_error: float, std_error: float, 
                               error_trend: float, thresholds: Dict[str, Tuple[float, float]]) -> float:
        """Вычисляет оценку соответствия режиму"""
        try:
            score = 0.0
            
            # Проверяем соответствие среднему значению ошибки
            mean_range = thresholds['mean_error']
            if mean_range[0] <= mean_error <= mean_range[1]:
                score += 1.0
            else:
                # Штраф за отклонение
                distance = min(abs(mean_error - mean_range[0]), abs(mean_error - mean_range[1]))
                score += max(0.0, 1.0 - distance / mean_range[1])
            
            # Проверяем соответствие стандартному отклонению
            std_range = thresholds['std_error']
            if std_range[0] <= std_error <= std_range[1]:
                score += 1.0
            else:
                distance = min(abs(std_error - std_range[0]), abs(std_error - std_range[1]))
                score += max(0.0, 1.0 - distance / std_range[1])
            
            # Проверяем соответствие тренду
            trend_range = thresholds['trend']
            if trend_range[0] <= error_trend <= trend_range[1]:
                score += 1.0
            else:
                distance = min(abs(error_trend - trend_range[0]), abs(error_trend - trend_range[1]))
                score += max(0.0, 1.0 - distance / max(abs(trend_range[0]), abs(trend_range[1])))
            
            return score / 3.0  # Нормализуем к [0, 1]
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления оценки режима: {e}")
            return 0.0
    
    def analyze_block(self, block_id: int) -> BlockAnalysis:
        """
        Полный анализ блока
        
        Args:
            block_id: ID блока
            
        Returns:
            BlockAnalysis: Результат анализа
        """
        try:
            statistics = self.get_block_statistics(block_id)
            
            if not statistics:
                return BlockAnalysis(
                    block_id=block_id,
                    block_type='unknown',
                    market_regime='unknown',
                    confidence=0.0,
                    performance_metrics={},
                    recommendations=[],
                    risk_level='unknown'
                )
            
            # Классифицируем рыночный режим
            market_regime = self.classify_market_regime(block_id)
            
            # Вычисляем метрики производительности
            performance_metrics = self._calculate_performance_metrics(statistics)
            
            # Генерируем рекомендации
            recommendations = self._generate_recommendations(statistics, market_regime)
            
            # Определяем уровень риска
            risk_level = self._assess_risk_level(statistics, market_regime)
            
            # Вычисляем общую уверенность
            confidence = self._calculate_analysis_confidence(statistics, market_regime)
            
            analysis = BlockAnalysis(
                block_id=block_id,
                block_type=statistics['block_type'],
                market_regime=market_regime,
                confidence=confidence,
                performance_metrics=performance_metrics,
                recommendations=recommendations,
                risk_level=risk_level
            )
            
            logger.info(f"✅ Анализ блока {block_id} завершен")
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Ошибка анализа блока {block_id}: {e}")
            return BlockAnalysis(
                block_id=block_id,
                block_type='unknown',
                market_regime='unknown',
                confidence=0.0,
                performance_metrics={},
                recommendations=['Ошибка анализа'],
                risk_level='unknown'
            )
    
    def _calculate_performance_metrics(self, statistics: Dict[str, Any]) -> Dict[str, float]:
        """Вычисляет метрики производительности блока"""
        try:
            additional_metrics = statistics.get('additional_metrics', {})
            
            # Базовые метрики
            metrics = {
                'accuracy': max(0.0, 1.0 - statistics['mean_error'] / 2.0),  # Нормализованная точность
                'stability': additional_metrics.get('error_stability', 0.0),
                'consistency': additional_metrics.get('confidence_consistency', 0.0),
                'volatility_score': 1.0 - min(statistics['volatility'] / 0.1, 1.0),  # Нормализованная волатильность
                'trend_score': additional_metrics.get('trend_stability', 0.0)
            }
            
            # Общая оценка производительности
            metrics['overall_score'] = safe_mean(list(metrics.values()))
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления метрик производительности: {e}")
            return {}
    
    def _generate_recommendations(self, statistics: Dict[str, Any], market_regime: str) -> List[str]:
        """Генерирует рекомендации на основе анализа блока"""
        try:
            recommendations = []
            
            mean_error = statistics['mean_error']
            std_error = statistics['std_error']
            volatility = statistics['volatility']
            
            # Рекомендации на основе ошибок
            if mean_error > 1.5:
                recommendations.append("Высокие ошибки прогноза - рассмотрите изменение параметров формулы")
            elif mean_error < 0.3:
                recommendations.append("Низкие ошибки прогноза - формула работает эффективно")
            
            # Рекомендации на основе стабильности
            if std_error > 1.0:
                recommendations.append("Высокая нестабильность - добавьте фильтры волатильности")
            elif std_error < 0.2:
                recommendations.append("Высокая стабильность - можно увеличить агрессивность стратегии")
            
            # Рекомендации на основе рыночного режима
            if market_regime == 'volatile':
                recommendations.append("Волатильный режим - используйте консервативные параметры")
            elif market_regime == 'stable':
                recommendations.append("Стабильный режим - можно использовать более агрессивные параметры")
            elif market_regime == 'trending':
                recommendations.append("Трендовый режим - фокусируйтесь на трендовых сигналах")
            elif market_regime == 'transition':
                recommendations.append("Переходный режим - будьте осторожны с параметрами")
            
            # Рекомендации на основе волатильности
            if volatility > 0.05:
                recommendations.append("Высокая волатильность рынка - уменьшите размер позиций")
            elif volatility < 0.01:
                recommendations.append("Низкая волатильность рынка - можно увеличить размер позиций")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации рекомендаций: {e}")
            return ["Ошибка генерации рекомендаций"]
    
    def _assess_risk_level(self, statistics: Dict[str, Any], market_regime: str) -> str:
        """Оценивает уровень риска блока"""
        try:
            mean_error = statistics['mean_error']
            std_error = statistics['std_error']
            volatility = statistics['volatility']
            
            risk_score = 0.0
            
            # Факторы риска
            if mean_error > 1.0:
                risk_score += 0.3
            if std_error > 0.8:
                risk_score += 0.3
            if volatility > 0.05:
                risk_score += 0.2
            if market_regime in ['volatile', 'transition']:
                risk_score += 0.2
            
            # Классификация риска
            if risk_score < 0.3:
                return 'low'
            elif risk_score < 0.6:
                return 'medium'
            else:
                return 'high'
                
        except Exception as e:
            logger.error(f"❌ Ошибка оценки риска: {e}")
            return 'unknown'
    
    def _calculate_analysis_confidence(self, statistics: Dict[str, Any], market_regime: str) -> float:
        """Вычисляет уверенность в анализе"""
        try:
            # Базовая уверенность из статистик блока
            base_confidence = statistics.get('confidence', 0.5)
            
            # Дополнительные факторы
            prediction_count = statistics.get('prediction_count', 0)
            count_confidence = min(prediction_count / 100.0, 1.0)  # Больше данных = больше уверенности
            
            # Уверенность в классификации режима
            regime_confidence = self._calculate_regime_score(
                statistics['mean_error'],
                statistics['std_error'],
                statistics['error_trend'],
                self.market_regime_thresholds.get(market_regime, {'mean_error': (0, 1), 'std_error': (0, 1), 'trend': (-1, 1)})
            )
            
            # Комбинируем уверенности
            total_confidence = (base_confidence + count_confidence + regime_confidence) / 3.0
            
            return min(max(total_confidence, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления уверенности анализа: {e}")
            return 0.5
    
    def get_blocks(self, start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   block_type: Optional[str] = None) -> pd.DataFrame:
        """Получает блоки из базы данных"""
        try:
            import sqlite3
            
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT * FROM blocks WHERE 1=1"
                params = []
                
                if start_time:
                    query += " AND start_time >= ?"
                    params.append(start_time)
                
                if end_time:
                    query += " AND end_time <= ?"
                    params.append(end_time)
                
                if block_type:
                    query += " AND block_type = ?"
                    params.append(block_type)
                
                query += " ORDER BY start_time"
                
                df = pd.read_sql_query(query, conn, params=params)
                
                logger.info(f"✅ Получено {len(df)} блоков из БД")
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения блоков: {e}")
            return pd.DataFrame()
    
    def get_blocks_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Получает сводку по блокам за указанный период
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            Dict[str, Any]: Сводка по блокам
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # Получаем блоки за период
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM blocks 
                    WHERE start_time >= ? AND end_time <= ?
                    ORDER BY start_time
                ''', (start_time, end_time))
                
                blocks_data = cursor.fetchall()
                
                if not blocks_data:
                    return {'message': 'Нет блоков за указанный период'}
                
                # Анализируем блоки
                blocks_df = pd.DataFrame(blocks_data, columns=[
                    'id', 'start_time', 'end_time', 'start_index', 'end_index',
                    'block_type', 'confidence', 'mean_error', 'std_error',
                    'max_error', 'min_error', 'error_trend', 'volatility', 'prediction_count', 'created_at'
                ])
                
                # Статистика по типам блоков
                type_counts = blocks_df['block_type'].value_counts().to_dict()
                
                # Статистика по режимам (классифицируем каждый блок)
                regime_counts = {}
                for _, block in blocks_df.iterrows():
                    regime = self.classify_market_regime(block['id'])
                    regime_counts[regime] = regime_counts.get(regime, 0) + 1
                
                # Общие метрики
                summary = {
                    'period_days': days,
                    'total_blocks': len(blocks_df),
                    'block_types': type_counts,
                    'market_regimes': regime_counts,
                    'average_confidence': safe_mean(safe_array(blocks_df['confidence'])),
                    'average_error': safe_mean(safe_array(blocks_df['mean_error'])),
                    'total_predictions': int(np.sum(blocks_df['prediction_count'])),
                    'most_common_type': blocks_df['block_type'].mode().iloc[0] if len(blocks_df) > 0 else 'unknown',
                    'most_common_regime': max(regime_counts.items(), key=lambda x: x[1])[0] if regime_counts else 'unknown'
                }
                
                logger.info(f"✅ Сводка по блокам создана за {days} дней")
                return summary
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания сводки по блокам: {e}")
            return {'error': str(e)}

def test_block_analyzer():
    """Тестирование Block Analyzer"""
    logger.info("🧪 Тестирование Block Analyzer")
    
    # Создаем тестовую базу данных
    import uuid
    test_db = f'test_block_analyzer_{uuid.uuid4().hex[:8]}.db'
    
    try:
        # Инициализация
        analyzer = BlockAnalyzer(test_db)
        
        # Создаем тестовые блоки в БД
        with sqlite3.connect(test_db) as conn:
            cursor = conn.cursor()
            
            # Создаем таблицу блоков
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
            
            # Создаем таблицу ошибок
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
                    timestamp, 'F01', 100.0 + i*0.01, 100.1 + i*0.01, 0.1 + i*0.001,
                    0.001, 0.1, 0.01, 0.8, 'sma'
                ))
            
            conn.commit()
        
        # Тестируем анализ блока
        analysis = analyzer.analyze_block(1)
        
        if analysis.block_id == 1 and analysis.market_regime != 'unknown':
            logger.info(f"✅ Анализ блока выполнен: режим = {analysis.market_regime}")
            
            # Тестируем сводку
            summary = analyzer.get_blocks_summary(1)
            
            if 'total_blocks' in summary:
                logger.info(f"✅ Сводка создана: {summary['total_blocks']} блоков")
                return True
            else:
                logger.error("❌ Сводка не создана")
                return False
        else:
            logger.error("❌ Анализ блока не выполнен")
            return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования Block Analyzer: {e}")
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
    
    print("📊 Тестирование Block Analyzer...")
    
    success = test_block_analyzer()
    
    if success:
        print("✅ Block Analyzer готов к использованию")
    else:
        print("❌ Ошибки в Block Analyzer")
