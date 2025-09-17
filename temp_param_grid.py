#!/usr/bin/env python3
"""
Временный файл с уменьшенным param_grid для тестирования оптимизатора.
"""

# Уменьшенное пространство поиска параметров для тестирования
param_grid = {
    'z_score_threshold': [1.5, 2.0],  # Порог для z-score
    'atr_risk_multiplier': [1.5, 2.0],     # Множитель для Stop Loss
    'reward_ratio': [1.5, 2.0]         # Соотношение Risk/Reward
}