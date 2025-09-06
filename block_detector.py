#!/usr/bin/env python3
"""
Block Detector для системы Error-Driven Adaptive Blocks
Реализует алгоритм выделения рыночных режимов на основе ошибок прогноза
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
class BlockBoundary:
    """Структура для хранения границы блока"""
    start_time: datetime
    end_time: datetime
    start_index: int
    end_index: int
    block_type: str
    confidence: float
    error_statistics: Dict[str, float]

class BlockDetector:
    """Детектор рыночных режимов на основе ошибок прогноза"""
    
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        """
        Инициализация детектора блоков
        
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
        self._init_database()
        logger.info(f"🔍 BlockDetector инициализирован с БД: {db_path}")
    
    def _init_database(self):
        """Инициализация таблиц базы данных для блоков"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Создание таблицы блоков
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
                
                # Создание индексов для быстрого поиска
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_blocks_time 
                    ON blocks(start_time, end_time)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_blocks_type 
                    ON blocks(block_type)
                ''')
                
                conn.commit()
                logger.info("✅ Таблицы блоков инициализированы")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД блоков: {e}")
            raise
    
    def detect_block_boundaries(self, error_history: pd.DataFrame, 
                               threshold: float = 2.0, window: int = 100,
                               min_block_size: int = 50) -> List[BlockBoundary]:
        """
        Определяет границы блоков на основе статистики ошибок
        
        Args:
            error_history: DataFrame с историей ошибок
            threshold: Порог для определения смены режима (в стандартных отклонениях)
            window: Размер окна для анализа
            min_block_size: Минимальный размер блока
            
        Returns:
            List[BlockBoundary]: Список границ блоков
        """
        try:
            logger.info(f"🔍 Начало обнаружения блоков: {len(error_history)} записей")
            
            if len(error_history) < min_block_size:
                logger.warning("Недостаточно данных для обнаружения блоков")
                return []
            
            # Сортируем по времени
            error_history = error_history.sort_values('timestamp').reset_index(drop=True)
            
            # Вычисляем скользящие статистики
            rolling_stats = self._calculate_rolling_statistics(error_history, window)
            
            # Определяем точки смены режима
            change_points = self._find_change_points(rolling_stats, threshold)
            
            # Создаем блоки
            blocks = self._create_blocks(error_history, change_points, min_block_size)
            
            # Классифицируем блоки
            classified_blocks = self._classify_blocks(blocks, error_history)
            
            logger.info(f"✅ Обнаружено {len(classified_blocks)} блоков")
            return classified_blocks
            
        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения блоков: {e}")
            return []
    
    def _calculate_rolling_statistics(self, df: pd.DataFrame, window: int) -> pd.DataFrame:
        """Вычисляет скользящие статистики ошибок"""
        try:
            # Используем безопасные функции для вычислений
            errors = safe_array(df['error_absolute'])
            
            # Скользящие статистики
            rolling_mean = pd.Series(errors).rolling(window=window, min_periods=window//2).apply(
                lambda x: safe_mean(x.values), raw=False
            )
            rolling_std = pd.Series(errors).rolling(window=window, min_periods=window//2).apply(
                lambda x: safe_std(x.values), raw=False
            )
            
            # Скользящий тренд ошибок
            rolling_trend = pd.Series(errors).rolling(window=window, min_periods=window//2).apply(
                lambda x: self._calculate_trend(x.values), raw=False
            )
            
            # Скользящая волатильность (если есть данные)
            if 'volatility' in df.columns:
                volatility = safe_array(df['volatility'])
                rolling_volatility = pd.Series(volatility).rolling(window=window, min_periods=window//2).apply(
                    lambda x: safe_mean(x.values), raw=False
                )
            else:
                rolling_volatility = pd.Series([0.01] * len(df))
            
            # Создаем DataFrame со статистиками
            stats_df = pd.DataFrame({
                'timestamp': df['timestamp'],
                'rolling_mean': rolling_mean,
                'rolling_std': rolling_std,
                'rolling_trend': rolling_trend,
                'rolling_volatility': rolling_volatility,
                'error_absolute': errors
            })
            
            return stats_df.dropna()
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления скользящих статистик: {e}")
            return pd.DataFrame()
    
    def _calculate_trend(self, values: np.ndarray) -> float:
        """Вычисляет тренд для массива значений"""
        try:
            if len(values) < 2:
                return 0.0
            
            # Простая линейная регрессия
            x = np.arange(len(values))
            y = safe_array(values)
            
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
            logger.error(f"❌ Ошибка вычисления тренда: {e}")
            return 0.0
    
    def _find_change_points(self, stats_df: pd.DataFrame, threshold: float) -> List[int]:
        """Находит точки смены режима"""
        try:
            change_points = [0]  # Начинаем с первой точки
            
            rolling_mean = stats_df['rolling_mean'].values
            rolling_std = stats_df['rolling_std'].values
            
            for i in range(1, len(rolling_mean)):
                # Проверяем значительное изменение среднего
                if rolling_std[i] > 0:
                    z_score = abs(rolling_mean[i] - rolling_mean[i-1]) / rolling_std[i]
                    
                    if z_score > threshold:
                        change_points.append(i)
            
            # Добавляем последнюю точку
            change_points.append(len(stats_df) - 1)
            
            # Удаляем дубликаты и сортируем
            change_points = sorted(list(set(change_points)))
            
            logger.info(f"🔍 Найдено {len(change_points)} точек смены режима")
            return change_points
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска точек смены: {e}")
            return [0, len(stats_df) - 1]
    
    def _create_blocks(self, error_history: pd.DataFrame, 
                      change_points: List[int], min_block_size: int) -> List[BlockBoundary]:
        """Создает блоки на основе точек смены режима"""
        try:
            blocks = []
            
            for i in range(len(change_points) - 1):
                start_idx = change_points[i]
                end_idx = change_points[i + 1]
                
                # Проверяем минимальный размер блока
                if end_idx - start_idx < min_block_size:
                    continue
                
                # Получаем данные блока
                block_data = error_history.iloc[start_idx:end_idx]
                
                if len(block_data) == 0:
                    continue
                
                # Вычисляем статистики блока
                errors = safe_array(block_data['error_absolute'])
                
                block_stats = {
                    'mean_error': safe_mean(errors),
                    'std_error': safe_std(errors),
                    'max_error': float(np.max(errors)) if len(errors) > 0 else 0.0,
                    'min_error': float(np.min(errors)) if len(errors) > 0 else 0.0,
                    'error_trend': self._calculate_trend(errors),
                    'volatility': safe_mean(safe_array(block_data['volatility'])) if 'volatility' in block_data.columns else 0.01,
                    'prediction_count': len(errors)
                }
                
                # Создаем блок
                block = BlockBoundary(
                    start_time=block_data['timestamp'].iloc[0],
                    end_time=block_data['timestamp'].iloc[-1],
                    start_index=start_idx,
                    end_index=end_idx,
                    block_type='unknown',  # Будет определено позже
                    confidence=0.0,  # Будет вычислено позже
                    error_statistics=block_stats
                )
                
                blocks.append(block)
            
            logger.info(f"🔍 Создано {len(blocks)} блоков")
            return blocks
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания блоков: {e}")
            return []
    
    def _classify_blocks(self, blocks: List[BlockBoundary], 
                        error_history: pd.DataFrame) -> List[BlockBoundary]:
        """Классифицирует блоки по типу рыночного режима"""
        try:
            classified_blocks = []
            
            for block in blocks:
                # Определяем тип блока на основе статистик
                block_type = self._determine_block_type(block.error_statistics)
                
                # Вычисляем уверенность в классификации
                confidence = self._calculate_classification_confidence(block.error_statistics)
                
                # Обновляем блок
                block.block_type = block_type
                block.confidence = confidence
                
                classified_blocks.append(block)
            
            logger.info(f"🔍 Классифицировано {len(classified_blocks)} блоков")
            return classified_blocks
            
        except Exception as e:
            logger.error(f"❌ Ошибка классификации блоков: {e}")
            return blocks
    
    def _determine_block_type(self, stats: Dict[str, float]) -> str:
        """Определяет тип блока на основе статистик"""
        try:
            mean_error = stats['mean_error']
            std_error = stats['std_error']
            error_trend = stats['error_trend']
            volatility = stats['volatility']
            
            # Классификация на основе комбинации факторов
            if mean_error < 0.5 and std_error < 0.3:
                return 'low_error_stable'
            elif mean_error > 2.0 and std_error > 1.0:
                return 'high_error_volatile'
            elif error_trend > 0.1:
                return 'increasing_errors'
            elif error_trend < -0.1:
                return 'decreasing_errors'
            elif volatility > 0.05:
                return 'high_volatility'
            else:
                return 'normal'
                
        except Exception as e:
            logger.error(f"❌ Ошибка определения типа блока: {e}")
            return 'unknown'
    
    def _calculate_classification_confidence(self, stats: Dict[str, float]) -> float:
        """Вычисляет уверенность в классификации блока"""
        try:
            # Уверенность основана на четкости различий в статистиках
            mean_error = stats['mean_error']
            std_error = stats['std_error']
            error_trend = abs(stats['error_trend'])
            
            # Нормализуем метрики
            mean_score = min(mean_error / 2.0, 1.0)  # Нормализуем к [0, 1]
            std_score = min(std_error / 1.0, 1.0)    # Нормализуем к [0, 1]
            trend_score = min(error_trend / 0.2, 1.0)  # Нормализуем к [0, 1]
            
            # Комбинируем метрики
            confidence = (mean_score + std_score + trend_score) / 3.0
            
            return min(max(confidence, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления уверенности: {e}")
            return 0.5
    
    def save_blocks(self, blocks: List[BlockBoundary]):
        """Сохраняет блоки в базу данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for block in blocks:
                    cursor.execute('''
                        INSERT INTO blocks 
                        (start_time, end_time, start_index, end_index, block_type, confidence,
                         mean_error, std_error, max_error, min_error, error_trend, 
                         volatility, prediction_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        block.start_time, block.end_time, block.start_index, block.end_index,
                        block.block_type, block.confidence,
                        block.error_statistics['mean_error'],
                        block.error_statistics['std_error'],
                        block.error_statistics['max_error'],
                        block.error_statistics['min_error'],
                        block.error_statistics['error_trend'],
                        block.error_statistics['volatility'],
                        block.error_statistics['prediction_count']
                    ))
                
                conn.commit()
            
            logger.info(f"✅ Сохранено {len(blocks)} блоков в БД")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения блоков: {e}")
    
    def get_blocks(self, start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   block_type: Optional[str] = None) -> pd.DataFrame:
        """Получает блоки из базы данных"""
        try:
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

def test_block_detector():
    """Тестирование Block Detector"""
    logger.info("🧪 Тестирование Block Detector")
    
    # Создаем тестовую базу данных
    import uuid
    test_db = f'test_block_detector_{uuid.uuid4().hex[:8]}.db'
    
    try:
        # Инициализация
        detector = BlockDetector(test_db)
        
        # Создаем тестовые данные с разными режимами
        np.random.seed(42)
        
        # Режим 1: Низкие ошибки (стабильный рынок)
        timestamps_1 = [datetime.now() - timedelta(hours=i) for i in range(100, 0, -1)]
        errors_1 = np.random.normal(0.3, 0.1, 100)
        
        # Режим 2: Высокие ошибки (волатильный рынок)
        timestamps_2 = [datetime.now() - timedelta(hours=i) for i in range(200, 100, -1)]
        errors_2 = np.random.normal(1.5, 0.5, 100)
        
        # Режим 3: Средние ошибки (нормальный рынок)
        timestamps_3 = [datetime.now() - timedelta(hours=i) for i in range(300, 200, -1)]
        errors_3 = np.random.normal(0.8, 0.2, 100)
        
        # Объединяем данные
        all_timestamps = timestamps_1 + timestamps_2 + timestamps_3
        all_errors = np.concatenate([errors_1, errors_2, errors_3])
        
        # Создаем DataFrame
        test_data = pd.DataFrame({
            'timestamp': all_timestamps,
            'error_absolute': all_errors,
            'volatility': [0.01] * len(all_timestamps)
        })
        
        # Обнаруживаем блоки
        blocks = detector.detect_block_boundaries(test_data, threshold=1.5, window=50)
        
        if len(blocks) >= 2:
            logger.info(f"✅ Обнаружено {len(blocks)} блоков")
            
            # Сохраняем блоки
            detector.save_blocks(blocks)
            
            # Получаем блоки из БД
            saved_blocks = detector.get_blocks()
            
            if len(saved_blocks) == len(blocks):
                logger.info("✅ Блоки сохранены и получены из БД")
                return True
            else:
                logger.error(f"❌ Несоответствие количества блоков: {len(saved_blocks)} != {len(blocks)}")
                return False
        else:
            logger.error(f"❌ Обнаружено недостаточно блоков: {len(blocks)}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования Block Detector: {e}")
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
    
    print("🔍 Тестирование Block Detector...")
    
    success = test_block_detector()
    
    if success:
        print("✅ Block Detector готов к использованию")
    else:
        print("❌ Ошибки в Block Detector")
