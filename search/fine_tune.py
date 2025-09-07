#!/usr/bin/env python3
"""
Система точной настройки (Grid Search) для топ-кандидатов
Задача S-10: Точная настройка оптимальной стратегии
"""

import numpy as np
import pandas as pd
import yaml
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Any
import sys
sys.path.append('..')

from engine.formulas import formula_catalog
from search.coarse_search import CoarseSearch
from logger import get_logger

logger = get_logger()

class FineTune:
    def __init__(self, config_path: str = "config/experiment.yaml"):
        """Инициализация системы точной настройки"""
        self.config = self._load_config(config_path)
        self.results_dir = 'report_s10'
        self.coarse_searcher = CoarseSearch(config_path)
        
        # Создаем директории
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Фиксируем seeds для воспроизводимости
        np.random.seed(self.config['reproducibility']['numpy_seed'])
        
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
    
    def load_coarse_results(self) -> List[Dict[str, Any]]:
        """Загружает результаты грубого поиска"""
        try:
            coarse_results_path = os.path.join(self.results_dir, 'coarse_search_results.csv')
            
            if not os.path.exists(coarse_results_path):
                logger.warning(f"⚠️ Файл {coarse_results_path} не найден. Запускаем грубый поиск...")
                return self.coarse_searcher.run_coarse_search()
            
            # Загружаем результаты
            df = pd.read_csv(coarse_results_path)
            
            # Конвертируем обратно в список словарей
            results = []
            for _, row in df.iterrows():
                result = {
                    'formula_id': row['formula_id'],
                    'formula_name': row['formula_name'],
                    'params': json.loads(row['params']),
                    'metrics': {
                        'sharpe_ratio': row['sharpe_ratio'],
                        'profit_factor': row['profit_factor'],
                        'win_rate': row['win_rate'],
                        'max_drawdown': row['max_drawdown'],
                        'total_return': row['total_return'],
                        'volatility': row['volatility'],
                        'sortino_ratio': row['sortino_ratio'],
                        'calmar_ratio': row['calmar_ratio']
                    },
                    'score': row['score']
                }
                results.append(result)
            
            logger.info(f"📊 Загружено {len(results)} результатов грубого поиска")
            return results
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки результатов грубого поиска: {e}")
            return []
    
    def create_grid_around(self, base_params: Dict[str, float], step: float = 0.05) -> List[Dict[str, float]]:
        """Создает сетку параметров вокруг базовых значений"""
        try:
            grid_params = []
            
            # Создаем сетку для каждого параметра
            for param_name, base_value in base_params.items():
                # Определяем диапазон для параметра
                formula_id = None
                for fid, formula in formula_catalog.get_all_formulas().items():
                    if param_name in formula['params']:
                        formula_id = fid
                        break
                
                if formula_id:
                    param_range = formula_catalog.get_formula(formula_id)['params'][param_name]
                    min_val, max_val = param_range
                    
                    # Создаем сетку вокруг базового значения
                    for offset in [-step, -step/2, 0, step/2, step]:
                        new_value = base_value + offset
                        
                        # Ограничиваем значения диапазоном
                        new_value = max(min_val, min(max_val, new_value))
                        
                        new_params = base_params.copy()
                        new_params[param_name] = new_value
                        grid_params.append(new_params)
                else:
                    # Если параметр не найден в формуле, используем простую сетку
                    for offset in [-step, 0, step]:
                        new_value = base_value + offset
                        new_params = base_params.copy()
                        new_params[param_name] = new_value
                        grid_params.append(new_params)
            
            # Убираем дубликаты
            unique_params = []
            seen = set()
            for params in grid_params:
                param_tuple = tuple(sorted(params.items()))
                if param_tuple not in seen:
                    seen.add(param_tuple)
                    unique_params.append(params)
            
            logger.info(f"🔧 Создана сетка из {len(unique_params)} параметров вокруг базовых значений")
            return unique_params
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания сетки: {e}")
            return [base_params]
    
    def run_fine_tune(self, top_candidates: int = 20) -> List[Dict[str, Any]]:
        """Запускает точную настройку для топ-кандидатов"""
        try:
            logger.info("🔧 Начинаем точную настройку оптимальной стратегии...")
            
            # Загружаем результаты грубого поиска
            coarse_results = self.load_coarse_results()
            
            if not coarse_results:
                logger.error("❌ Нет результатов грубого поиска для точной настройки")
                return []
            
            # Сортируем по score и берем топ-кандидатов
            coarse_results.sort(key=lambda x: x['score'], reverse=True)
            top_results = coarse_results[:top_candidates]
            
            logger.info(f"🎯 Выбрано {len(top_results)} топ-кандидатов для точной настройки")
            
            # Загружаем данные
            start_date = self.config['experiment']['start_date']
            end_date = self.config['experiment']['end_date']
            df = self.coarse_searcher.load_historical_data(start_date, end_date)
            
            if df.empty:
                logger.error("❌ Не удалось загрузить данные для точной настройки")
                return []
            
            refined_results = []
            
            # Проходим по каждому топ-кандидату
            for i, candidate in enumerate(top_results):
                logger.info(f"🔍 Точная настройка {i+1}/{len(top_results)}: {candidate['formula_id']} - {candidate['formula_name']}")
                
                # Создаем сетку вокруг лучших параметров
                grid_step = self.config['search']['fine_tune']['grid_step']
                grid_params = self.create_grid_around(candidate['params'], grid_step)
                
                candidate_results = []
                
                # Тестируем каждый набор параметров из сетки
                for j, params in enumerate(grid_params):
                    if j % 10 == 0:
                        logger.info(f"  Прогресс: {j}/{len(grid_params)}")
                    
                    # Запускаем backtest
                    metrics = self.coarse_searcher.run_backtest(df, candidate['formula_id'], params)
                    
                    # Добавляем результат
                    result = {
                        'formula_id': candidate['formula_id'],
                        'formula_name': candidate['formula_name'],
                        'params': params,
                        'metrics': metrics,
                        'score': metrics['sharpe_ratio'],
                        'refinement_level': 'fine_tune'
                    }
                    
                    candidate_results.append(result)
                
                # Сортируем результаты кандидата
                candidate_results.sort(key=lambda x: x['score'], reverse=True)
                
                # Добавляем лучший результат в общий список
                refined_results.append(candidate_results[0])
                
                logger.info(f"✅ {candidate['formula_id']}: лучший Sharpe после настройки = {candidate_results[0]['metrics']['sharpe_ratio']:.3f}")
            
            # Сортируем все результаты по score
            refined_results.sort(key=lambda x: x['score'], reverse=True)
            
            # Сохраняем результаты
            self._save_fine_tune_results(refined_results)
            
            logger.info(f"🎉 Точная настройка завершена! Найдено {len(refined_results)} улучшенных конфигураций")
            
            return refined_results
            
        except Exception as e:
            logger.error(f"❌ Ошибка точной настройки: {e}")
            return []
    
    def _save_fine_tune_results(self, results: List[Dict[str, Any]]):
        """Сохраняет результаты точной настройки"""
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
                    'score': result['score'],
                    'refinement_level': result.get('refinement_level', 'fine_tune')
                }
                df_results.append(row)
            
            df = pd.DataFrame(df_results)
            
            # Сохраняем в CSV
            output_path = os.path.join(self.results_dir, 'fine_tune_results.csv')
            df.to_csv(output_path, index=False)
            
            # Сохраняем топ-10 в отдельный файл
            df_top10 = df.nlargest(10, 'score')
            top10_path = os.path.join(self.results_dir, 'fine_tune_top10.csv')
            df_top10.to_csv(top10_path, index=False)
            
            # Обновляем общий leaderboard
            self._update_leaderboard(results)
            
            logger.info(f"💾 Результаты точной настройки сохранены в {output_path}")
            logger.info(f"🏆 Топ-10 сохранен в {top10_path}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов точной настройки: {e}")
    
    def _update_leaderboard(self, fine_tune_results: List[Dict[str, Any]]):
        """Обновляет общий leaderboard с результатами точной настройки"""
        try:
            # Загружаем существующий leaderboard
            leaderboard_path = os.path.join(self.results_dir, 'leaderboard.csv')
            
            if os.path.exists(leaderboard_path):
                df_leaderboard = pd.read_csv(leaderboard_path)
                
                # Конвертируем результаты точной настройки в DataFrame
                fine_tune_df = []
                for result in fine_tune_results:
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
                        'score': result['score'],
                        'search_type': 'fine_tune'
                    }
                    fine_tune_df.append(row)
                
                df_fine_tune = pd.DataFrame(fine_tune_df)
                df_fine_tune['search_type'] = 'fine_tune'
                df_leaderboard['search_type'] = 'coarse'
                
                # Объединяем результаты
                df_combined = pd.concat([df_leaderboard, df_fine_tune], ignore_index=True)
                
                # Сортируем по score и берем топ-20
                df_combined = df_combined.nlargest(20, 'score')
                
                # Сохраняем обновленный leaderboard
                df_combined.to_csv(leaderboard_path, index=False)
                
                logger.info(f"🏆 Leaderboard обновлен с результатами точной настройки")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления leaderboard: {e}")
    
    def get_best_strategy(self) -> Dict[str, Any]:
        """Возвращает лучшую стратегию после точной настройки"""
        try:
            fine_tune_path = os.path.join(self.results_dir, 'fine_tune_results.csv')
            
            if not os.path.exists(fine_tune_path):
                logger.warning("⚠️ Результаты точной настройки не найдены")
                return {}
            
            # Загружаем результаты
            df = pd.read_csv(fine_tune_path)
            
            if df.empty:
                return {}
            
            # Находим лучший результат
            best_idx = df['score'].idxmax()
            best_result = df.iloc[best_idx]
            
            # Формируем результат
            best_strategy = {
                'formula_id': best_result['formula_id'],
                'formula_name': best_result['formula_name'],
                'params': json.loads(best_result['params']),
                'metrics': {
                    'sharpe_ratio': best_result['sharpe_ratio'],
                    'profit_factor': best_result['profit_factor'],
                    'win_rate': best_result['win_rate'],
                    'max_drawdown': best_result['max_drawdown'],
                    'total_return': best_result['total_return'],
                    'volatility': best_result['volatility'],
                    'sortino_ratio': best_result['sortino_ratio'],
                    'calmar_ratio': best_result['calmar_ratio']
                },
                'score': best_result['score']
            }
            
            return best_strategy
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения лучшей стратегии: {e}")
            return {}

def main():
    """Основная функция"""
    try:
        # Создаем экземпляр точной настройки
        fine_tuner = FineTune()
        
        # Запускаем точную настройку
        results = fine_tuner.run_fine_tune(top_candidates=10)
        
        if results:
            # Показываем топ-5 результатов
            print("\n🏆 ТОП-5 РЕЗУЛЬТАТОВ ТОЧНОЙ НАСТРОЙКИ:")
            print("=" * 80)
            
            for i, result in enumerate(results[:5]):
                print(f"{i+1}. {result['formula_id']} - {result['formula_name']}")
                print(f"   Sharpe: {result['metrics']['sharpe_ratio']:.3f}")
                print(f"   Profit Factor: {result['metrics']['profit_factor']:.3f}")
                print(f"   Win Rate: {result['metrics']['win_rate']:.3f}")
                print(f"   Max DD: {result['metrics']['max_drawdown']:.3f}")
                print()
            
            # Получаем лучшую стратегию
            best_strategy = fine_tuner.get_best_strategy()
            if best_strategy:
                print("🥇 ЛУЧШАЯ СТРАТЕГИЯ:")
                print(f"   Формула: {best_strategy['formula_id']} - {best_strategy['formula_name']}")
                print(f"   Параметры: {best_strategy['params']}")
                print(f"   Sharpe Ratio: {best_strategy['metrics']['sharpe_ratio']:.3f}")
                print(f"   Profit Factor: {best_strategy['metrics']['profit_factor']:.3f}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в main: {e}")

if __name__ == "__main__":
    main()
