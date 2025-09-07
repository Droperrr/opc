#!/usr/bin/env python3
"""
Каталог формул F01-F20 для поиска оптимальной торговой стратегии
Задача S-10: Поиск оптимальной стратегии на 8-месячной истории
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any
import json

# Фиксированные seeds для воспроизводимости
np.random.seed(42)

class FormulaCatalog:
    def __init__(self):
        self.formulas = self._initialize_formulas()
    
    def _initialize_formulas(self) -> Dict[str, Dict[str, Any]]:
        """Инициализирует каталог формул F01-F20"""
        
        formulas = {
            "F01": {
                "name": "volatility_focused",
                "description": "Фокус на волатильности с адаптивными порогами",
                "expression": "Y = a*IV_z + b*ΔIV + c*|basis_rel|^-1 − d*basis_rel",
                "params": {
                    "a": (0.5, 2.5),      # Коэффициент IV z-score
                    "b": (0.1, 1.0),      # Коэффициент изменения IV
                    "c": (0.5, 2.0),      # Коэффициент обратного basis
                    "d": (0.5, 2.0)       # Коэффициент basis
                },
                "thresholds": {
                    "th_long": (1.0, 3.0),
                    "th_short": (-3.0, -1.0)
                }
            },
            
            "F02": {
                "name": "skew_momentum",
                "description": "Моментум на основе skew с трендовым фильтром",
                "expression": "Y = a*skew_z + b*skew_momentum + c*trend_filter",
                "params": {
                    "a": (0.8, 2.2),      # Коэффициент skew z-score
                    "b": (0.3, 1.5),     # Коэффициент моментума skew
                    "c": (0.2, 1.0)       # Коэффициент трендового фильтра
                },
                "thresholds": {
                    "th_long": (1.2, 2.5),
                    "th_short": (-2.5, -1.2)
                }
            },
            
            "F03": {
                "name": "basis_reversal",
                "description": "Реверсивная стратегия на основе basis",
                "expression": "Y = a*basis_z + b*basis_reversal + c*vol_filter",
                "params": {
                    "a": (0.6, 2.0),      # Коэффициент basis z-score
                    "b": (0.4, 1.8),     # Коэффициент реверсии basis
                    "c": (0.3, 1.2)      # Коэффициент волатильностного фильтра
                },
                "thresholds": {
                    "th_long": (1.5, 2.8),
                    "th_short": (-2.8, -1.5)
                }
            },
            
            "F04": {
                "name": "iv_skew_combo",
                "description": "Комбинация IV и skew с адаптивными весами",
                "expression": "Y = a*IV_z + b*skew_z + c*correlation_factor",
                "params": {
                    "a": (0.7, 2.3),      # Вес IV
                    "b": (0.5, 1.8),     # Вес skew
                    "c": (0.2, 1.0)      # Фактор корреляции
                },
                "thresholds": {
                    "th_long": (1.3, 2.6),
                    "th_short": (-2.6, -1.3)
                }
            },
            
            "F05": {
                "name": "momentum_enhanced",
                "description": "Улучшенный моментум с множественными таймфреймами",
                "expression": "Y = a*momentum_1h + b*momentum_4h + c*vol_ratio",
                "params": {
                    "a": (0.6, 2.0),      # Моментум 1h
                    "b": (0.4, 1.6),     # Моментум 4h
                    "c": (0.3, 1.4)      # Отношение волатильностей
                },
                "thresholds": {
                    "th_long": (1.1, 2.4),
                    "th_short": (-2.4, -1.1)
                }
            },
            
            "F06": {
                "name": "volatility_regime",
                "description": "Стратегия для разных режимов волатильности",
                "expression": "Y = a*low_vol_signal + b*high_vol_signal + c*regime_filter",
                "params": {
                    "a": (0.8, 2.2),      # Сигнал низкой волатильности
                    "b": (0.6, 1.8),     # Сигнал высокой волатильности
                    "c": (0.4, 1.2)      # Фильтр режима
                },
                "thresholds": {
                    "th_long": (1.2, 2.7),
                    "th_short": (-2.7, -1.2)
                }
            },
            
            "F07": {
                "name": "mean_reversion",
                "description": "Средне-реверсивная стратегия с адаптивными границами",
                "expression": "Y = a*mean_deviation + b*reversion_strength + c*vol_adjust",
                "params": {
                    "a": (0.5, 2.0),      # Отклонение от среднего
                    "b": (0.7, 2.2),     # Сила реверсии
                    "c": (0.3, 1.5)      # Волатильностная корректировка
                },
                "thresholds": {
                    "th_long": (1.4, 2.9),
                    "th_short": (-2.9, -1.4)
                }
            },
            
            "F08": {
                "name": "trend_following",
                "description": "Трендследующая стратегия с фильтрами",
                "expression": "Y = a*trend_strength + b*trend_consistency + c*noise_filter",
                "params": {
                    "a": (0.6, 2.1),      # Сила тренда
                    "b": (0.5, 1.7),     # Консистентность тренда
                    "c": (0.4, 1.3)      # Фильтр шума
                },
                "thresholds": {
                    "th_long": (1.0, 2.3),
                    "th_short": (-2.3, -1.0)
                }
            },
            
            "F09": {
                "name": "breakout_detector",
                "description": "Детектор пробоев с подтверждением",
                "expression": "Y = a*breakout_signal + b*volume_confirmation + c*vol_filter",
                "params": {
                    "a": (0.7, 2.3),      # Сигнал пробоя
                    "b": (0.4, 1.6),     # Подтверждение объемом
                    "c": (0.3, 1.2)      # Волатильностный фильтр
                },
                "thresholds": {
                    "th_long": (1.3, 2.6),
                    "th_short": (-2.6, -1.3)
                }
            },
            
            "F10": {
                "name": "multi_timeframe",
                "description": "Мульти-таймфреймовая стратегия",
                "expression": "Y = a*signal_1m + b*signal_5m + c*signal_15m + d*signal_1h",
                "params": {
                    "a": (0.3, 1.5),      # Сигнал 1m
                    "b": (0.4, 1.8),     # Сигнал 5m
                    "c": (0.5, 2.0),     # Сигнал 15m
                    "d": (0.6, 2.2)      # Сигнал 1h
                },
                "thresholds": {
                    "th_long": (1.5, 2.8),
                    "th_short": (-2.8, -1.5)
                }
            },
            
            "F11": {
                "name": "volatility_breakout",
                "description": "Пробой волатильности с адаптивными порогами",
                "expression": "Y = a*vol_breakout + b*vol_momentum + c*regime_switch",
                "params": {
                    "a": (0.8, 2.4),      # Пробой волатильности
                    "b": (0.5, 1.9),     # Моментум волатильности
                    "c": (0.3, 1.4)      # Переключение режима
                },
                "thresholds": {
                    "th_long": (1.2, 2.5),
                    "th_short": (-2.5, -1.2)
                }
            },
            
            "F12": {
                "name": "correlation_enhanced",
                "description": "Улучшенная корреляционная стратегия",
                "expression": "Y = a*corr_signal + b*corr_stability + c*outlier_filter",
                "params": {
                    "a": (0.6, 2.0),      # Корреляционный сигнал
                    "b": (0.4, 1.6),     # Стабильность корреляции
                    "c": (0.3, 1.2)      # Фильтр выбросов
                },
                "thresholds": {
                    "th_long": (1.1, 2.4),
                    "th_short": (-2.4, -1.1)
                }
            },
            
            "F13": {
                "name": "adaptive_momentum",
                "description": "Адаптивный моментум с динамическими весами",
                "expression": "Y = a*momentum_short + b*momentum_long + c*adaptation_factor",
                "params": {
                    "a": (0.5, 2.1),      # Краткосрочный моментум
                    "b": (0.6, 2.2),     # Долгосрочный моментум
                    "c": (0.4, 1.5)      # Фактор адаптации
                },
                "thresholds": {
                    "th_long": (1.3, 2.7),
                    "th_short": (-2.7, -1.3)
                }
            },
            
            "F14": {
                "name": "regime_switching",
                "description": "Переключение режимов с фильтрами",
                "expression": "Y = a*regime_signal + b*regime_confidence + c*transition_filter",
                "params": {
                    "a": (0.7, 2.3),      # Сигнал режима
                    "b": (0.5, 1.8),     # Уверенность режима
                    "c": (0.3, 1.3)      # Фильтр перехода
                },
                "thresholds": {
                    "th_long": (1.4, 2.6),
                    "th_short": (-2.6, -1.4)
                }
            },
            
            "F15": {
                "name": "volatility_harvesting",
                "description": "Сбор волатильности с риск-менеджментом",
                "expression": "Y = a*vol_harvest + b*risk_adjust + c*position_size",
                "params": {
                    "a": (0.6, 2.0),      # Сбор волатильности
                    "b": (0.4, 1.6),     # Корректировка риска
                    "c": (0.3, 1.2)      # Размер позиции
                },
                "thresholds": {
                    "th_long": (1.0, 2.3),
                    "th_short": (-2.3, -1.0)
                }
            },
            
            "F16": {
                "name": "momentum_contrarian",
                "description": "Контрарианский моментум с фильтрами",
                "expression": "Y = a*momentum_signal + b*contrarian_factor + c*reversal_filter",
                "params": {
                    "a": (0.5, 2.2),      # Моментум сигнал
                    "b": (0.6, 2.1),     # Контрарианский фактор
                    "c": (0.4, 1.4)      # Фильтр разворота
                },
                "thresholds": {
                    "th_long": (1.2, 2.5),
                    "th_short": (-2.5, -1.2)
                }
            },
            
            "F17": {
                "name": "multi_factor",
                "description": "Мульти-факторная модель с PCA",
                "expression": "Y = a*factor1 + b*factor2 + c*factor3 + d*residual",
                "params": {
                    "a": (0.4, 2.0),      # Фактор 1
                    "b": (0.4, 2.0),     # Фактор 2
                    "c": (0.4, 2.0),     # Фактор 3
                    "d": (0.2, 1.0)      # Остаточный фактор
                },
                "thresholds": {
                    "th_long": (1.5, 2.8),
                    "th_short": (-2.8, -1.5)
                }
            },
            
            "F18": {
                "name": "adaptive_threshold",
                "description": "Адаптивные пороги с машинным обучением",
                "expression": "Y = a*signal + b*threshold_adjust + c*confidence",
                "params": {
                    "a": (0.6, 2.1),      # Базовый сигнал
                    "b": (0.5, 1.8),     # Корректировка порога
                    "c": (0.4, 1.3)      # Уровень уверенности
                },
                "thresholds": {
                    "th_long": (1.1, 2.4),
                    "th_short": (-2.4, -1.1)
                }
            },
            
            "F19": {
                "name": "ensemble_strategy",
                "description": "Ансамблевая стратегия с весами",
                "expression": "Y = a*strategy1 + b*strategy2 + c*strategy3",
                "params": {
                    "a": (0.3, 1.8),      # Вес стратегии 1
                    "b": (0.3, 1.8),     # Вес стратегии 2
                    "c": (0.3, 1.8)      # Вес стратегии 3
                },
                "thresholds": {
                    "th_long": (1.3, 2.6),
                    "th_short": (-2.6, -1.3)
                }
            },
            
            "F20": {
                "name": "optimal_combination",
                "description": "Оптимальная комбинация всех факторов",
                "expression": "Y = a*IV + b*skew + c*basis + d*momentum + e*vol + f*trend",
                "params": {
                    "a": (0.2, 1.5),      # Вес IV
                    "b": (0.2, 1.5),     # Вес skew
                    "c": (0.2, 1.5),     # Вес basis
                    "d": (0.2, 1.5),     # Вес моментума
                    "e": (0.2, 1.5),     # Вес волатильности
                    "f": (0.2, 1.5)      # Вес тренда
                },
                "thresholds": {
                    "th_long": (1.6, 2.9),
                    "th_short": (-2.9, -1.6)
                }
            }
        }
        
        return formulas
    
    def get_formula(self, formula_id: str) -> Dict[str, Any]:
        """Возвращает формулу по ID"""
        return self.formulas.get(formula_id, {})
    
    def get_all_formulas(self) -> Dict[str, Dict[str, Any]]:
        """Возвращает все формулы"""
        return self.formulas
    
    def generate_random_params(self, formula_id: str, n_samples: int = 3000) -> List[Dict[str, float]]:
        """Генерирует случайные параметры для формулы"""
        formula = self.get_formula(formula_id)
        if not formula:
            return []
        
        params_list = []
        for _ in range(n_samples):
            params = {}
            for param_name, (min_val, max_val) in formula["params"].items():
                params[param_name] = np.random.uniform(min_val, max_val)
            params_list.append(params)
        
        return params_list
    
    def create_grid_around(self, base_params: Dict[str, float], step: float = 0.05) -> List[Dict[str, float]]:
        """Создает сетку параметров вокруг базовых значений"""
        grid_params = []
        
        for param_name, base_value in base_params.items():
            # Создаем сетку вокруг базового значения
            for offset in [-step, 0, step]:
                new_value = base_value + offset
                new_params = base_params.copy()
                new_params[param_name] = new_value
                grid_params.append(new_params)
        
        return grid_params

# Глобальный экземпляр каталога
formula_catalog = FormulaCatalog()
