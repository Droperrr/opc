#!/usr/bin/env python3
"""
Система грубого поиска (Random Search) для формул F01-F20
Задача S-10: Поиск оптимальной стратегии на 8-месячной истории
Задача S-13: Оптимизация памяти для полного поиска
"""

import numpy as np
import pandas as pd
import sqlite3
import yaml
import json
import os
import gc
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Generator
import sys
sys.path.append('..')

from engine.formulas import formula_catalog
from logger import get_logger

logger = get_logger()

# Импорт оптимизатора памяти
try:
    from memory_optimizer import MemoryOptimizer
except ImportError:
    logger.warning("⚠️ MemoryOptimizer не найден, используем базовую оптимизацию")
    MemoryOptimizer = None

class CoarseSearch:
    def __init__(self, config_path: str = "config/experiment.yaml"):
        """Инициализация системы грубого поиска с оптимизацией памяти"""
        self.config = self._load_config(config_path)
        self.db_path = 'data/sol_iv.db'
        self.results_dir = 'report_s10'
        
        # Создаем директории
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs('data/cache', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Фиксируем seeds для воспроизводимости
        np.random.seed(self.config['reproducibility']['numpy_seed'])
        
        # Инициализация оптимизатора памяти
        if MemoryOptimizer:
            self.memory_optimizer = MemoryOptimizer(
                chunk_size=self.config.get('memory', {}).get('chunk_size', 50000),
                max_memory_gb=self.config.get('memory', {}).get('max_memory_gb', 2.0)
            )
            logger.info("🔧 MemoryOptimizer инициализирован")
        else:
            self.memory_optimizer = None
            logger.warning("⚠️ MemoryOptimizer недоступен, используем базовую оптимизацию")
        
        # Кеш для рассчитанных значений (ограниченный размер)
        self.y_cache = {}
        self.max_cache_size = 1000  # Максимальный размер кеша
        
        # Настройка NumPy для экономии памяти
        import warnings
        warnings.filterwarnings('ignore', category=RuntimeWarning)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загружает конфигурацию из YAML файла"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Конфигурация загружена из {config_path}")
            return config
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
            return {}
    
    def load_historical_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Загружает исторические данные из базы"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем спотовые данные (они есть за 2025 год)
            query = """
            SELECT 
                time,
                open,
                high,
                low,
                close as spot_price,
                volume
            FROM spot_data 
            WHERE time BETWEEN ? AND ? 
            AND timeframe = '1m'
            ORDER BY time
            """
            
            df = pd.read_sql_query(query, conn, params=[start_date, end_date])
            df['time'] = pd.to_datetime(df['time'])
            
            # Загружаем реалистичные IV данные
            iv_query = """
            SELECT 
                time,
                iv_30d,
                skew_30d
            FROM iv_agg_realistic 
            WHERE time BETWEEN ? AND ? 
            AND timeframe = '1m'
            ORDER BY time
            """
            
            iv_df = pd.read_sql_query(iv_query, conn, params=[start_date, end_date])
            iv_df['time'] = pd.to_datetime(iv_df['time'])
            
            # Загружаем реалистичные basis данные
            basis_query = """
            SELECT 
                time,
                basis_rel,
                funding_rate,
                oi_total
            FROM basis_agg_realistic 
            WHERE time BETWEEN ? AND ? 
            AND timeframe = '1m'
            ORDER BY time
            """
            
            basis_df = pd.read_sql_query(basis_query, conn, params=[start_date, end_date])
            basis_df['time'] = pd.to_datetime(basis_df['time'])
            
            # Объединяем данные
            df = df.merge(iv_df, on='time', how='left')
            df = df.merge(basis_df, on='time', how='left')
            
            # Заполняем пропуски
            df = df.ffill().fillna(0)
            
            # Добавляем технические индикаторы
            df = self._add_technical_indicators(df)
            
            conn.close()
            
            logger.info(f"📊 Загружено {len(df)} записей за период {start_date} - {end_date}")
            logger.info(f"📊 IV данные: среднее={df['iv_30d'].mean():.3f}, std={df['iv_30d'].std():.3f}")
            logger.info(f"📊 Basis данные: среднее={df['basis_rel'].mean():.6f}, std={df['basis_rel'].std():.6f}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            return pd.DataFrame()
    
    def _add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет технические индикаторы к данным"""
        try:
            # Z-score для IV
            df['iv_z'] = (df['iv_30d'] - df['iv_30d'].rolling(60).mean()) / df['iv_30d'].rolling(60).std()
            
            # Изменение IV
            df['iv_change'] = df['iv_30d'].pct_change()
            
            # Z-score для skew
            df['skew_z'] = (df['skew_30d'] - df['skew_30d'].rolling(60).mean()) / df['skew_30d'].rolling(60).std()
            
            # Z-score для basis
            df['basis_z'] = (df['basis_rel'] - df['basis_rel'].rolling(60).mean()) / df['basis_rel'].rolling(60).std()
            
            # Моментум индикаторы
            df['momentum_1h'] = df['spot_price'].pct_change(60)  # 1 час
            df['momentum_4h'] = df['spot_price'].pct_change(240)  # 4 часа
            
            # Волатильность
            df['volatility'] = df['spot_price'].rolling(20).std()
            
            # Трендовые индикаторы
            df['trend_1h'] = df['spot_price'].rolling(60).mean()
            df['trend_4h'] = df['spot_price'].rolling(240).mean()
            
            # Заполняем пропуски
            df = df.ffill().fillna(0)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления индикаторов: {e}")
            return df
    
    def calculate_formula_value(self, df: pd.DataFrame, formula_id: str, params: Dict[str, float]) -> pd.Series:
        """Вычисляет значение Y для формулы с заданными параметрами (оптимизированная версия)"""
        try:
            formula = formula_catalog.get_formula(formula_id)
            if not formula:
                return pd.Series([0] * len(df), dtype='float32')
            
            # Создаем ключ кеша
            cache_key = f"{formula_id}_{hash(json.dumps(params, sort_keys=True))}"
            
            # Проверяем кеш (с ограничением размера)
            if cache_key in self.y_cache:
                return self.y_cache[cache_key]
            
            # Очищаем кеш если он слишком большой
            if len(self.y_cache) > self.max_cache_size:
                # Удаляем половину старых записей
                keys_to_remove = list(self.y_cache.keys())[:len(self.y_cache)//2]
                for key in keys_to_remove:
                    del self.y_cache[key]
                gc.collect()
            
            # Используем оптимизатор памяти если доступен
            if self.memory_optimizer:
                Y = self.memory_optimizer.optimize_formula_calculation(df, formula_id, params)
            else:
                # Базовое вычисление с оптимизацией типов данных
                Y = self._calculate_formula_basic(df, formula_id, params)
            
            # Сохраняем в кеш
            self.y_cache[cache_key] = Y
            
            return Y
            
        except MemoryError as e:
            logger.error(f"❌ Нехватка памяти при вычислении формулы {formula_id}: {e}")
            # Принудительная очистка памяти
            gc.collect()
            return pd.Series([0] * len(df), dtype='float32')
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления формулы {formula_id}: {e}")
            return pd.Series([0] * len(df), dtype='float32')
    
    def _calculate_formula_basic(self, df: pd.DataFrame, formula_id: str, params: Dict[str, float]) -> pd.Series:
        """Базовое вычисление формулы с оптимизацией памяти"""
        try:
            # Оптимизируем типы данных
            df_opt = df.copy()
            for col in df_opt.select_dtypes(include=['float64']).columns:
                if col in ['iv_z', 'skew_z', 'basis_z', 'momentum_1h', 'momentum_4h', 'volatility']:
                    df_opt[col] = df_opt[col].astype('float32')
            
            # Вычисляем Y в зависимости от формулы
            if formula_id == "F01":  # volatility_focused
                a, b, c, d = params['a'], params['b'], params['c'], params['d']
                Y = (a * df_opt['iv_z'] + 
                     b * df_opt['iv_change'] + 
                     c * (1 / (abs(df_opt['basis_rel']) + 1e-6)) - 
                     d * df_opt['basis_rel'])
            
            elif formula_id == "F02":  # skew_momentum
                a, b, c = params['a'], params['b'], params['c']
                trend_filter = np.where(df_opt['trend_1h'] > df_opt['trend_4h'], 1, -1)
                Y = a * df_opt['skew_z'] + b * df_opt['skew_30d'].pct_change() + c * trend_filter
            
            elif formula_id == "F03":  # basis_reversal
                a, b, c = params['a'], params['b'], params['c']
                vol_filter = np.where(df_opt['volatility'] > df_opt['volatility'].rolling(60).mean(), 1, 0)
                Y = a * df_opt['basis_z'] + b * df_opt['basis_rel'].pct_change() + c * vol_filter
            
            elif formula_id == "F04":  # iv_skew_combo
                a, b, c = params['a'], params['b'], params['c']
                corr_factor = df_opt['iv_30d'].rolling(20).corr(df_opt['skew_30d'])
                Y = a * df_opt['iv_z'] + b * df_opt['skew_z'] + c * corr_factor
            
            elif formula_id == "F05":  # momentum_enhanced
                a, b, c = params['a'], params['b'], params['c']
                vol_ratio = df_opt['volatility'] / df_opt['volatility'].rolling(60).mean()
                Y = a * df_opt['momentum_1h'] + b * df_opt['momentum_4h'] + c * vol_ratio
            
            else:
                # Для остальных формул используем базовую комбинацию
                Y = self._calculate_generic_formula(df_opt, formula_id, params)
            
            # Нормализуем Y с оптимизацией памяти
            rolling_mean = Y.rolling(60, min_periods=1).mean()
            rolling_std = Y.rolling(60, min_periods=1).std()
            Y = (Y - rolling_mean) / (rolling_std + 1e-6)
            
            # Конвертируем в float32 для экономии памяти
            return Y.astype('float32')
            
        except Exception as e:
            logger.error(f"❌ Ошибка базового вычисления формулы {formula_id}: {e}")
            return pd.Series([0] * len(df), dtype='float32')
    
    def _calculate_generic_formula(self, df: pd.DataFrame, formula_id: str, params: Dict[str, float]) -> pd.Series:
        """Вычисляет значение для общих формул"""
        try:
            # Базовые компоненты
            components = {
                'iv': df['iv_z'],
                'skew': df['skew_z'],
                'basis': df['basis_z'],
                'momentum': df['momentum_1h'],
                'vol': df['volatility'],
                'trend': np.where(df['trend_1h'] > df['trend_4h'], 1, -1)
            }
            
            # Вычисляем Y как взвешенную сумму компонентов
            Y = pd.Series([0] * len(df))
            for i, (param_name, value) in enumerate(params.items()):
                if param_name in components:
                    Y += value * components[param_name]
                else:
                    # Если параметр не найден, используем случайный компонент
                    component_names = list(components.keys())
                    if component_names:
                        random_component = component_names[i % len(component_names)]
                        Y += value * components[random_component]
            
            return Y
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления общей формулы: {e}")
            return pd.Series([0] * len(df))
    
    def run_backtest(self, df: pd.DataFrame, formula_id: str, params: Dict[str, float]) -> Dict[str, float]:
        """Запускает backtest для формулы с заданными параметрами (оптимизированная версия)"""
        try:
            # Проверяем использование памяти
            if self.memory_optimizer:
                memory_before = self.memory_optimizer.get_memory_usage()
                if memory_before['rss_gb'] > self.memory_optimizer.max_memory_gb:
                    logger.warning(f"⚠️ Высокое использование памяти: {memory_before['rss_gb']:.2f}GB")
                    gc.collect()
            
            # Вычисляем Y
            Y = self.calculate_formula_value(df, formula_id, params)
            
            # Определяем пороги
            th_long = 1.5
            th_short = -1.5
            
            # Генерируем сигналы (оптимизированно)
            signals = pd.Series([0] * len(df), dtype='int8')  # Используем int8 для экономии памяти
            signals[Y > th_long] = 1    # Long
            signals[Y < th_short] = -1  # Short
            
            # Симулируем торговлю
            returns = df['spot_price'].pct_change().astype('float32')
            strategy_returns = signals.shift(1) * returns
            
            # Учитываем комиссии
            fees = self.config['backtest']['fees']['taker_bps'] / 10000
            strategy_returns = strategy_returns - fees * abs(signals.diff())
            
            # Вычисляем метрики
            metrics = self._calculate_metrics(strategy_returns)
            
            # Освобождаем память
            del Y, signals, returns, strategy_returns
            gc.collect()
            
            return metrics
            
        except MemoryError as e:
            logger.error(f"❌ Нехватка памяти при backtest для {formula_id}: {e}")
            gc.collect()
            return {
                'sharpe_ratio': 0,
                'profit_factor': 0,
                'win_rate': 0,
                'max_drawdown': 1,
                'total_return': 0,
                'volatility': 1
            }
        except Exception as e:
            logger.error(f"❌ Ошибка backtest для {formula_id}: {e}")
            return {
                'sharpe_ratio': 0,
                'profit_factor': 0,
                'win_rate': 0,
                'max_drawdown': 1,
                'total_return': 0,
                'volatility': 1
            }
    
    def _calculate_metrics(self, returns: pd.Series) -> Dict[str, float]:
        """Вычисляет метрики производительности (оптимизированная версия)"""
        try:
            # Убираем NaN и конвертируем в float32
            returns = returns.dropna().astype('float32')
            
            if len(returns) == 0:
                return {
                    'sharpe_ratio': 0,
                    'profit_factor': 0,
                    'win_rate': 0,
                    'max_drawdown': 1,
                    'total_return': 0,
                    'volatility': 1
                }
            
            # Базовые метрики (оптимизированные вычисления)
            total_return = float((1 + returns).prod() - 1)
            volatility = float(returns.std() * np.sqrt(252 * 24 * 60))  # Годовая волатильность
            sharpe_ratio = float((returns.mean() * 252 * 24 * 60) / (returns.std() * np.sqrt(252 * 24 * 60))) if returns.std() > 0 else 0.0
            
            # Profit Factor (оптимизированно)
            positive_mask = returns > 0
            negative_mask = returns < 0
            positive_sum = float(returns[positive_mask].sum())
            negative_sum = float(returns[negative_mask].sum())
            profit_factor = abs(positive_sum / negative_sum) if negative_sum != 0 else 0.0
            
            # Win Rate
            win_rate = float(len(returns[positive_mask]) / len(returns)) if len(returns) > 0 else 0.0
            
            # Maximum Drawdown (оптимизированно)
            cumulative = (1 + returns).cumprod().astype('float32')
            running_max = cumulative.expanding().max().astype('float32')
            drawdown = ((cumulative - running_max) / running_max).astype('float32')
            max_drawdown = float(abs(drawdown.min()))
            
            # Sortino Ratio (оптимизированно)
            downside_returns = returns[negative_mask]
            downside_vol = float(downside_returns.std() * np.sqrt(252 * 24 * 60)) if len(downside_returns) > 0 else 1.0
            sortino_ratio = float((returns.mean() * 252 * 24 * 60) / downside_vol) if downside_vol > 0 else 0.0
            
            # Calmar Ratio
            calmar_ratio = float(total_return / max_drawdown) if max_drawdown > 0 else 0.0
            
            # Освобождаем память
            del cumulative, running_max, drawdown, downside_returns
            
            return {
                'sharpe_ratio': sharpe_ratio,
                'profit_factor': profit_factor,
                'win_rate': win_rate,
                'max_drawdown': max_drawdown,
                'total_return': total_return,
                'volatility': volatility,
                'sortino_ratio': sortino_ratio,
                'calmar_ratio': calmar_ratio
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления метрик: {e}")
            return {
                'sharpe_ratio': 0,
                'profit_factor': 0,
                'win_rate': 0,
                'max_drawdown': 1,
                'total_return': 0,
                'volatility': 1
            }
    
    def run_coarse_search(self) -> List[Dict[str, Any]]:
        """Запускает грубый поиск для всех формул"""
        try:
            logger.info("🚀 Начинаем грубый поиск оптимальной стратегии...")
            
            # Загружаем данные
            start_date = self.config['experiment']['start_date']
            end_date = self.config['experiment']['end_date']
            df = self.load_historical_data(start_date, end_date)
            
            if df.empty:
                logger.error("❌ Не удалось загрузить данные")
                return []
            
            all_results = []
            
            # Проходим по всем формулам
            formulas = formula_catalog.get_all_formulas()
            
            for formula_id, formula in formulas.items():
                logger.info(f"🔍 Тестируем формулу {formula_id}: {formula['name']}")
                
                # Генерируем случайные параметры
                n_samples = self.config['search']['coarse_search']['n_samples_per_formula']
                params_list = formula_catalog.generate_random_params(formula_id, n_samples)
                
                formula_results = []
                
                # Тестируем каждый набор параметров
                for i, params in enumerate(params_list):
                    if i % 500 == 0:
                        logger.info(f"  Прогресс: {i}/{len(params_list)}")
                    
                    # Запускаем backtest
                    metrics = self.run_backtest(df, formula_id, params)
                    
                    # Добавляем результат
                    result = {
                        'formula_id': formula_id,
                        'formula_name': formula['name'],
                        'params': params,
                        'metrics': metrics,
                        'score': metrics['sharpe_ratio']  # Основная метрика
                    }
                    
                    formula_results.append(result)
                
                # Сортируем по score и берем топ-кандидатов
                formula_results.sort(key=lambda x: x['score'], reverse=True)
                top_k = self.config['search']['coarse_search']['n_top_candidates']
                top_results = formula_results[:top_k]
                
                all_results.extend(top_results)
                
                logger.info(f"✅ Формула {formula_id}: найдено {len(top_results)} топ-кандидатов")
            
            # Сохраняем результаты
            self._save_coarse_results(all_results)
            
            logger.info(f"🎉 Грубый поиск завершен! Найдено {len(all_results)} кандидатов")
            
            return all_results
            
        except Exception as e:
            logger.error(f"❌ Ошибка грубого поиска: {e}")
            return []
    
    def _save_coarse_results(self, results: List[Dict[str, Any]]):
        """Сохраняет результаты грубого поиска"""
        try:
            # Создаем DataFrame
            df_results = []
            for result in results:
                row = {
                    'formula_id': result['formula_id'],
                    'formula_name': result['formula_name'],
                    'params': json.dumps(result['params']),
                    'sharpe_ratio': result['metrics']['sharpe_ratio'],
                    'profit_factor': result['metrics']['profit_factor'],
                    'win_rate': result['metrics']['win_rate'],
                    'max_drawdown': result['metrics']['max_drawdown'],
                    'total_return': result['metrics']['total_return'],
                    'volatility': result['metrics']['volatility'],
                    'sortino_ratio': result['metrics']['sortino_ratio'],
                    'calmar_ratio': result['metrics']['calmar_ratio'],
                    'score': result['score']
                }
                df_results.append(row)
            
            df = pd.DataFrame(df_results)
            
            # Сохраняем в CSV
            output_path = os.path.join(self.results_dir, 'coarse_search_results.csv')
            df.to_csv(output_path, index=False)
            
            # Сохраняем топ-20 в отдельный файл
            df_top20 = df.nlargest(20, 'score')
            top20_path = os.path.join(self.results_dir, 'leaderboard.csv')
            df_top20.to_csv(top20_path, index=False)
            
            logger.info(f"💾 Результаты сохранены в {output_path}")
            logger.info(f"🏆 Топ-20 сохранен в {top20_path}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов: {e}")

def main():
    """Основная функция"""
    try:
        # Создаем экземпляр поиска
        searcher = CoarseSearch()
        
        # Запускаем грубый поиск
        results = searcher.run_coarse_search()
        
        if results:
            # Показываем топ-5 результатов
            print("\n🏆 ТОП-5 РЕЗУЛЬТАТОВ ГРУБОГО ПОИСКА:")
            print("=" * 80)
            
            for i, result in enumerate(results[:5]):
                print(f"{i+1}. {result['formula_id']} - {result['formula_name']}")
                print(f"   Sharpe: {result['metrics']['sharpe_ratio']:.3f}")
                print(f"   Profit Factor: {result['metrics']['profit_factor']:.3f}")
                print(f"   Win Rate: {result['metrics']['win_rate']:.3f}")
                print(f"   Max DD: {result['metrics']['max_drawdown']:.3f}")
                print()
        
    except Exception as e:
        logger.error(f"❌ Ошибка в main: {e}")

if __name__ == "__main__":
    main()
