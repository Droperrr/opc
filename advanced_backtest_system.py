#!/usr/bin/env python3
"""
Модуль расширенного бэктестинга v1.0 - Комплексная система валидации сигналов
Включает реалистичную симуляцию торговли, управление рисками и детальную аналитику
Период анализа: 25.08.2025 - 03.09.2025
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from logger import get_logger
import json
import os
from typing import Dict, List, Tuple, Optional

# Импортируем функции из basis_signal_generator по умолчанию
from basis_signal_generator import calculate_thresholds, generate_signals_from_data

logger = get_logger()

class AdvancedBacktestSystem:
    def __init__(self, data_file='basis_features_1m.parquet', signals_file='candle_signals.csv',
                 z_score_threshold=2.0, low_vol_params=None, mid_vol_params=None, high_vol_params=None,
                 initial_capital=10000, position_size=0.1, fixed_trading_params=None, results_dir=None, asset='SOL',
                 signal_generator_module='basis_signal_generator', sma_period=20, run_id=None):
        # Импортируем функции из указанного модуля генератора сигналов
        if signal_generator_module == 'basis_signal_generator':
            from basis_signal_generator import calculate_thresholds, generate_signals_from_data
        elif signal_generator_module == 'mean_reversion_signal_generator':
            from mean_reversion_signal_generator import calculate_thresholds, generate_signals_from_data
        else:
            # По умолчанию используем базовый генератор
            from basis_signal_generator import calculate_thresholds, generate_signals_from_data
        
        # Сохраняем функции как атрибуты класса
        self.calculate_thresholds = calculate_thresholds
        self.generate_signals_from_data = generate_signals_from_data
        logger.info(f"📥 Полученные параметры в бэктест:")
        logger.info(f"  initial_capital: {initial_capital}")
        logger.info(f"  position_size: {position_size}")
        logger.info(f"  fixed_trading_params: {fixed_trading_params}")
        logger.info(f"  asset: {asset}")
        
        self.asset = asset
        self.run_id = run_id
        
        # Извлекаем индивидуальные параметры из fixed_trading_params для более детального логирования
        if fixed_trading_params:
            atr_risk_multiplier = fixed_trading_params.get('atr_risk_multiplier', 'Not specified')
            reward_ratio = fixed_trading_params.get('reward_ratio', 'Not specified')
            logger.info(f"BACKTESTER: Received params -> risk_multiplier={atr_risk_multiplier}, reward_ratio={reward_ratio}, position_size={position_size}")
        
        self.data_file = data_file
        self.signals_file = signals_file
        self.results_dir = results_dir or 'backtest_results'
        
        # Параметры оптимизации
        self.z_score_threshold = z_score_threshold
        self.sma_period = sma_period
        
        # Адаптивные параметры для каждого режима волатильности
        self.low_vol_params = low_vol_params or {'atr_risk_multiplier': 1.5, 'reward_ratio': 3.0}
        self.mid_vol_params = mid_vol_params or {'atr_risk_multiplier': 2.0, 'reward_ratio': 2.0}
        self.high_vol_params = high_vol_params or {'atr_risk_multiplier': 3.0, 'reward_ratio': 1.5}
        
        # Фиксированные параметры торговли (если заданы, будут использоваться вместо адаптивных)
        self.fixed_trading_params = fixed_trading_params
        
        # Переменные для хранения порогов волатильности (будут рассчитаны в run_walk_forward_backtest)
        self.volatility_thresholds = None
        
        # Параметры торговли (будут динамически рассчитываться для каждой сделки)
        self.trading_params = {
            'initial_capital': initial_capital,      # Начальный капитал в USD
            'position_size': position_size,          # Размер позиции в процентах от капитала
            'max_positions': 5,                      # Максимум одновременных позиций
            'commission': 0.001,                     # 0.1% комиссия
            'slippage': 0.0005                       # 0.05% проскальзывание
        }
        
        # Параметры управления рисками
        self.risk_params = {
            'max_daily_loss': 0.05,       # Максимум 5% потерь в день
            'max_drawdown': 0.20,         # Максимум 20% просадки
            'correlation_threshold': 0.7,  # Порог корреляции между позициями
            'volatility_filter': True,     # Фильтр по волатильности
            'session_filter': True        # Фильтр по торговым сессиям
        }
        
        # Параметры Walk-Forward
        self.walk_forward_params = {
            'train_window_size_days': 21,  # Размер обучающего окна в днях
            'test_window_size_days': 7,    # Размер тестового окна в днях
            'step_size_days': 7            # Шаг сдвига окон в днях
        }
        
        # Метрики производительности
        self.performance_metrics = {}
        self.trades_history = []
        self.equity_curve = []
        self.walk_forward_results = []  # Результаты Walk-Forward
        
        # Создаем директорию для результатов
        os.makedirs(self.results_dir, exist_ok=True)
    
    def load_signals_data(self, signals_file=None) -> pd.DataFrame:
        """Загружает данные сигналов для бэктестинга"""
        try:
            file_to_load = signals_file if signals_file else self.signals_file
            
            if os.path.exists(file_to_load):
                df = pd.read_csv(file_to_load)
                logger.info(f"📊 Загружено {len(df)} сигналов из {file_to_load}")
            else:
                logger.error(f"❌ Файл сигналов {file_to_load} не найден")
                return pd.DataFrame()
            
            # Преобразуем timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных сигналов: {e}")
            return pd.DataFrame()
    
    def load_features_data(self, data_file=None) -> pd.DataFrame:
        """Загружает данные признаков для бэктестинга"""
        try:
            file_to_load = data_file if data_file else self.data_file
            
            if os.path.exists(file_to_load):
                df = pd.read_parquet(file_to_load)
                logger.info(f"📊 Загружено {len(df)} записей из {file_to_load}")
            else:
                logger.error(f"❌ Файл данных {file_to_load} не найден")
                return pd.DataFrame()
            
            # Преобразуем timestamp
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time').reset_index(drop=True)
            
            # Проверяем, что загруженные данные соответствуют запрошенному активу
            if 'asset' in df.columns:
                loaded_asset = df['asset'].iloc[0]
                logger.info(f"BACKTESTER: Successfully loaded data for asset: {loaded_asset}. Verifying against target asset: {self.asset}")
                if loaded_asset != self.asset:
                    logger.critical(f"CRITICAL MISMATCH: Backtester was asked to test '{self.asset}' but loaded data for '{loaded_asset}'!")
                    raise ValueError(f"Asset mismatch: expected {self.asset}, got {loaded_asset}")
            else:
                logger.warning("WARNING: 'asset' column not found in data. Skipping asset verification.")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных признаков: {e}")
            return pd.DataFrame()
    
    
    def calculate_position_size(self, capital: float, price: float, trend_agreement_weight: float = 1.0) -> float:
        """Рассчитывает размер позиции в SOL с учетом весового коэффициента тренд-фильтра"""
        position_value = capital * self.trading_params['position_size'] * trend_agreement_weight
        return position_value / price
    
    def apply_risk_management(self, signal: Dict, current_positions: List[Dict]) -> bool:
        """Применяет правила управления рисками"""
        try:
            # Проверка максимального количества позиций
            if len(current_positions) >= self.trading_params['max_positions']:
                return False
            
            # Проверка корреляции с существующими позициями
            for position in current_positions:
                if position['signal'] == signal['signal']:
                    # Если уже есть позиция в том же направлении, проверяем корреляцию
                    time_diff = abs((signal['timestamp'] - position['timestamp']).total_seconds() / 3600)
                    if time_diff < 2:  # Если сигналы близки по времени
                        return False
            
            # Проверка фильтра волатильности
            if self.risk_params['volatility_filter']:
                iv = signal.get('iv', 0)  # Если поле 'iv' отсутствует, используем 0
                if iv > 1.5:  # Слишком высокая волатильность
                    return False
            
            # Проверка фильтра торговых сессий
            if self.risk_params['session_filter']:
                session = signal['session']
                hour = signal['timestamp'].hour
                
                # Избегаем торговли в неактивные часы
                if session == 'asian' and (hour < 2 or hour > 6):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в управлении рисками: {e}")
            return False
    
    def determine_volatility_mode(self, historical_volatility: float) -> str:
        """Определяет режим волатильности на основе текущего значения и порогов"""
        if self.volatility_thresholds is None:
            return 'mid'  # По умолчанию, если пороги не рассчитаны
            
        low_threshold = self.volatility_thresholds['low']
        high_threshold = self.volatility_thresholds['high']
        
        if historical_volatility < low_threshold:
            return 'low'
        elif historical_volatility > high_threshold:
            return 'high'
        else:
            return 'mid'
    
    def get_trading_params_for_mode(self, mode: str) -> Dict:
        """Возвращает параметры торговли для заданного режима волатильности"""
        # Если заданы фиксированные параметры, используем их
        if self.fixed_trading_params is not None:
            logger.info(f"🔧 Используем фиксированные параметры: {self.fixed_trading_params}")
            # Рассчитываем stop_loss и take_profit на основе фиксированных параметров
            base_stop_loss = 0.05
            atr_risk_multiplier = self.fixed_trading_params.get('atr_risk_multiplier', 2.0)
            reward_ratio = self.fixed_trading_params.get('reward_ratio', 3.0)
            
            stop_loss = base_stop_loss * (atr_risk_multiplier / 2.0)  # Нормализуем относительно значения по умолчанию 2.0
            take_profit = stop_loss * reward_ratio
            
            return {
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'atr_risk_multiplier': atr_risk_multiplier,
                'reward_ratio': reward_ratio
            }
        
        # Иначе используем адаптивные параметры
        logger.info(f"🔧 Используем адаптивные параметры для режима {mode}")
        if mode == 'low':
            params = self.low_vol_params
        elif mode == 'high':
            params = self.high_vol_params
        else:  # mid
            params = self.mid_vol_params
            
        # Рассчитываем stop_loss и take_profit на основе параметров режима
        base_stop_loss = 0.05
        atr_risk_multiplier = params.get('atr_risk_multiplier', 2.0)
        reward_ratio = params.get('reward_ratio', 3.0)
        
        stop_loss = base_stop_loss * (atr_risk_multiplier / 2.0)  # Нормализуем относительно значения по умолчанию 2.0
        take_profit = stop_loss * reward_ratio
        
        return {
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'atr_risk_multiplier': atr_risk_multiplier,
            'reward_ratio': reward_ratio
        }
    
    def execute_trade(self, signal: Dict, capital: float, historical_volatility: float) -> Dict:
        """Выполняет торговую операцию с учетом режима волатильности"""
        try:
            price = signal['underlying_price']
            # Получаем весовой коэффициент тренд-фильтра (по умолчанию 1.0, если не задан)
            trend_agreement_weight = signal.get('trend_agreement_weight', 1.0)
            position_size_sol = self.calculate_position_size(capital, price, trend_agreement_weight)
            
            # Определяем режим волатильности
            vol_mode = self.determine_volatility_mode(historical_volatility)
            
            # Получаем параметры торговли для текущего режима
            mode_params = self.get_trading_params_for_mode(vol_mode)
            
            # Применяем комиссию и проскальзывание
            execution_price = price * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
            
            trade = {
                'timestamp': signal['timestamp'],
                'signal': signal['signal'],
                'confidence': signal['confidence'],
                'entry_price': execution_price,
                'position_size_sol': position_size_sol,
                'position_value_usd': position_size_sol * execution_price,
                'session': signal['session'],
                'timeframe': signal['timeframe'],
                'reason': signal['reason'],
                'status': 'open',
                'exit_price': None,
                'exit_timestamp': None,
                'pnl': None,
                'pnl_percent': None,
                'volatility_mode': vol_mode,  # Сохраняем режим для использования при выходе
                'mode_params': mode_params    # Сохраняем параметры режима
            }
            
            return trade
            
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения сделки: {e}")
            return {}
    
    def check_exit_conditions(self, trade: Dict, current_price: float, current_timestamp: datetime) -> Tuple[bool, str]:
        """Проверяет условия выхода из позиции"""
        entry_price = trade['entry_price']
        signal_type = trade['signal']
        
        # Получаем параметры сделки (они уже рассчитаны для конкретного режима волатильности)
        mode_params = trade.get('mode_params', {})
        stop_loss = mode_params.get('stop_loss', 0.05)  # Значение по умолчанию, если не задано
        take_profit = mode_params.get('take_profit', 0.15)  # Значение по умолчанию, если не задано
        
        # Рассчитываем текущий P&L
        if signal_type == 'LONG':
            pnl_percent = (current_price - entry_price) / entry_price
        else:  # SHORT
            pnl_percent = (entry_price - current_price) / entry_price
        
        # Проверяем стоп-лосс
        if pnl_percent <= -stop_loss:
            return True, 'stop_loss'
        
        # Проверяем тейк-профит
        if pnl_percent >= take_profit:
            return True, 'take_profit'
        
        # Проверяем время в позиции (максимум 4 часа)
        time_in_position = (current_timestamp - trade['timestamp']).total_seconds() / 3600
        if time_in_position >= 4:
            return True, 'time_exit'
        
        return False, ''
    
    def run_backtest(self) -> Dict:
        """Запускает полный бэктест системы"""
        try:
            logger.info("🚀 Запуск расширенного бэктестинга...")
            
            # Загружаем данные сигналов
            signals_df = self.load_signals_data()
            if signals_df.empty:
                return {}
            
            # Инициализируем переменные
            capital = self.trading_params['initial_capital']
            open_positions = []
            closed_trades = []
            equity_curve = []
            
            # Проходим по всем сигналам
            for idx, signal in signals_df.iterrows():
                signal_dict = signal.to_dict()
                
                # Проверяем тип сигнала
                signal_type = signal_dict.get('signal')
                
                # Получаем цены открытия и закрытия свечи
                open_price = signal_dict.get('open_price')
                close_price = signal_dict.get('close_price')
                
                # Проверяем условия выхода для всех открытых позиций
                # на основе цен открытия и закрытия текущей свечи
                if open_price is not None and close_price is not None:
                    # Проверяем условия выхода по цене открытия свечи
                    # Создаем список индексов для удаления, чтобы избежать проблем с индексами во время итерации
                    positions_to_remove = []
                    for i, position in enumerate(open_positions):
                        should_exit, exit_reason = self.check_exit_conditions(position, open_price, signal_dict['timestamp'])
                        
                        if should_exit:
                            # Закрываем позицию по цене открытия
                            # Для LONG позиции мы продаем по цене открытия минус комиссия и проскальзывание
                            # Для SHORT позиции мы покупаем по цене открытия плюс комиссия и проскальзывание
                            exit_price = open_price * (1 - self.trading_params['commission'] - self.trading_params['slippage']) if position['signal'] == 'LONG' else open_price * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
                            
                            # Рассчитываем P&L
                            if position['signal'] == 'LONG':
                                pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                            else:
                                pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                            
                            pnl_percent = pnl / position['position_value_usd']
                            
                            # Обновляем позицию
                            position.update({
                                'exit_price': exit_price,
                                'exit_timestamp': signal_dict['timestamp'],
                                'pnl': pnl,
                                'pnl_percent': pnl_percent,
                                'status': 'closed',
                                'exit_reason': exit_reason
                            })
                            
                            # Перемещаем в закрытые сделки
                            closed_trades.append(position)
                            positions_to_remove.append(i)
                            
                            # Обновляем капитал
                            capital += pnl
                            
                            # Проверяем лимиты потерь
                            if capital < self.trading_params['initial_capital'] * (1 - self.risk_params['max_drawdown']):
                                logger.warning(f"⚠️ Достигнут лимит просадки: {capital:.2f} USD")
                                break
                    
                    # Удаляем закрытые позиции из списка открытых позиций (в обратном порядке, чтобы не нарушить индексы)
                    for i in reversed(positions_to_remove):
                        open_positions.pop(i)
                    
                    # Проверяем условия выхода по цене закрытия свечи (если позиция еще открыта)
                    # Создаем список индексов для удаления, чтобы избежать проблем с индексами во время итерации
                    positions_to_remove = []
                    for i, position in enumerate(open_positions):
                        should_exit, exit_reason = self.check_exit_conditions(position, close_price, signal_dict['timestamp'])
                        
                        if should_exit:
                            # Закрываем позицию по цене закрытия
                            # Для LONG позиции мы продаем по цене закрытия минус комиссия и проскальзывание
                            # Для SHORT позиции мы покупаем по цене закрытия плюс комиссия и проскальзывание
                            exit_price = close_price * (1 - self.trading_params['commission'] - self.trading_params['slippage']) if position['signal'] == 'LONG' else close_price * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
                            
                            # Рассчитываем P&L
                            if position['signal'] == 'LONG':
                                pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                            else:
                                pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                            
                            pnl_percent = pnl / position['position_value_usd']
                            
                            # Обновляем позицию
                            position.update({
                                'exit_price': exit_price,
                                'exit_timestamp': signal_dict['timestamp'],
                                'pnl': pnl,
                                'pnl_percent': pnl_percent,
                                'status': 'closed',
                                'exit_reason': exit_reason
                            })
                            
                            # Перемещаем в закрытые сделки
                            closed_trades.append(position)
                            positions_to_remove.append(i)
                            
                            # Обновляем капитал
                            capital += pnl
                            
                            # Проверяем лимиты потерь
                            if capital < self.trading_params['initial_capital'] * (1 - self.risk_params['max_drawdown']):
                                logger.warning(f"⚠️ Достигнут лимит просадки: {capital:.2f} USD")
                                break
                    
                    # Удаляем закрытые позиции из списка открытых позиций (в обратном порядке, чтобы не нарушить индексы)
                    for i in reversed(positions_to_remove):
                        open_positions.pop(i)
                
                # Обработка сигналов закрытия позиций
                if signal_type in ['CLOSE_LONG', 'CLOSE_SHORT']:
                    # Ищем соответствующую открытую позицию
                    position_to_remove_index = None
                    for i, position in enumerate(open_positions):
                        if (signal_type == 'CLOSE_LONG' and position['signal'] == 'LONG') or \
                           (signal_type == 'CLOSE_SHORT' and position['signal'] == 'SHORT'):
                            # Закрываем позицию по цене открытия текущей свечи
                            # Для LONG позиции мы продаем по цене открытия минус комиссия и проскальзывание
                            # Для SHORT позиции мы покупаем по цене открытия плюс комиссия и проскальзывание
                            open_price = signal_dict.get('open_price', position['entry_price'])
                            exit_price = open_price * (1 - self.trading_params['commission'] - self.trading_params['slippage']) if position['signal'] == 'LONG' else open_price * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
                            
                            # Рассчитываем P&L
                            if position['signal'] == 'LONG':
                                pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                            else:
                                pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                            
                            pnl_percent = pnl / position['position_value_usd']
                            
                            # Обновляем позицию
                            position.update({
                                'exit_price': exit_price,
                                'exit_timestamp': signal_dict['timestamp'],
                                'pnl': pnl,
                                'pnl_percent': pnl_percent,
                                'status': 'closed',
                                'exit_reason': 'signal_close'
                            })
                            
                            # Перемещаем в закрытые сделки
                            closed_trades.append(position)
                            position_to_remove_index = i
                            # Обновляем капитал
                            capital += pnl
                            break  # Закрываем только одну позицию за раз
                    
                    # Удаляем закрытую позицию из списка открытых позиций
                    if position_to_remove_index is not None:
                        open_positions.pop(position_to_remove_index)
                else:
                    # Проверяем управление рисками для сигналов открытия позиций
                    if not self.apply_risk_management(signal_dict, open_positions):
                        continue
                    
                    # Для передачи historical_volatility в execute_trade, нам нужно получить соответствующую запись из features_data
                    # Поскольку signals_df и full_data (из run_walk_forward_backtest) должны быть синхронизированы по времени,
                    # мы можем найти соответствующую запись в full_data по времени сигнала.
                    # Однако в run_backtest у нас нет прямого доступа к full_data.
                    # В run_walk_forward_backtest перед вызовом run_backtest мы можем передать нужные данные.
                    # Но для простоты, давайте предположим, что signals_df содержит все необходимые данные, включая historical_volatility.
                    # Проверим, есть ли в signal_dict поле historical_volatility. Если нет, будем использовать 0 по умолчанию.
                    historical_volatility = signal_dict.get('historical_volatility_24h', 0.0)
                    
                    # Выполняем сделку
                    trade = self.execute_trade(signal_dict, capital, historical_volatility)
                    if not trade:
                        continue
                    
                    open_positions.append(trade)
                    
                    # Для исправления "look-ahead bias" мы не симулируем движение цены,
                    # а проверяем условия выхода на следующих сигналах
                    # Пока что просто продолжаем, проверка условий выхода будет происходить
                    # при обработке следующих сигналов
                
                # Проверяем, не исчерпан ли капитал
                if capital <= 0:
                    logger.error(f"❌ Депозит исчерпан. Дата и время: {signal_dict['timestamp']}")
                    # Закрываем все открытые позиции по цене входа с учетом комиссии и проскальзывания
                    for position in open_positions:
                        # Для LONG позиции мы продаем по цене входа минус комиссия и проскальзывание
                        # Для SHORT позиции мы покупаем по цене входа плюс комиссия и проскальзывание
                        exit_price = position['entry_price'] * (1 - self.trading_params['commission'] - self.trading_params['slippage']) if position['signal'] == 'LONG' else position['entry_price'] * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
                        
                        # Рассчитываем P&L
                        if position['signal'] == 'LONG':
                            pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                        else:
                            pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                        
                        pnl_percent = pnl / position['position_value_usd']
                        
                        position.update({
                            'exit_price': exit_price,
                            'exit_timestamp': signal_dict['timestamp'],
                            'pnl': pnl,
                            'pnl_percent': pnl_percent,
                            'status': 'closed',
                            'exit_reason': 'margin_call'
                        })
                        closed_trades.append(position)
                    open_positions.clear()
                    break  # Останавливаем бэктест
                
                # Записываем точку equity curve
                equity_curve.append({
                    'timestamp': signal_dict['timestamp'],
                    'capital': capital,
                    'open_positions': len(open_positions)
                })
                
                # Логируем прогресс
                if idx % 100 == 0:
                    logger.info(f"📊 Обработано {idx}/{len(signals_df)} сигналов, капитал: {capital:.2f} USD")
            
            # Закрываем все оставшиеся позиции по цене входа с учетом комиссии и проскальзывания
            for position in open_positions:
                # Для LONG позиции мы продаем по цене входа минус комиссия и проскальзывание
                # Для SHORT позиции мы покупаем по цене входа плюс комиссия и проскальзывание
                exit_price = position['entry_price'] * (1 - self.trading_params['commission'] - self.trading_params['slippage']) if position['signal'] == 'LONG' else position['entry_price'] * (1 + self.trading_params['commission'] + self.trading_params['slippage'])
                
                # Рассчитываем P&L
                if position['signal'] == 'LONG':
                    pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                else:
                    pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                
                pnl_percent = pnl / position['position_value_usd']
                
                position.update({
                    'exit_price': exit_price,
                    'exit_timestamp': signals_df.iloc[-1]['timestamp'],
                    'pnl': pnl,
                    'pnl_percent': pnl_percent,
                    'status': 'closed',
                    'exit_reason': 'end_of_period'
                })
                closed_trades.append(position)
            
            # Сохраняем результаты
            self.trades_history = closed_trades
            self.equity_curve = equity_curve
            
            # Рассчитываем метрики производительности
            self.calculate_performance_metrics(closed_trades, equity_curve)
            
            # Сохраняем результаты
            self.save_backtest_results(closed_trades, equity_curve)
            
            logger.info("✅ Бэктест завершен успешно!")
            return self.performance_metrics
            
        except Exception as e:
            logger.error(f"❌ Ошибка в бэктесте: {e}")
            return {}
    
    def calculate_performance_metrics(self, trades: List[Dict], equity_curve: List[Dict]) -> None:
        """Рассчитывает детальные метрики производительности"""
        try:
            if not trades:
                return
            
            # Базовые метрики
            total_trades = len(trades)
            winning_trades = len([t for t in trades if t['pnl'] > 0])
            losing_trades = len([t for t in trades if t['pnl'] < 0])
            
            win_rate = winning_trades / total_trades if total_trades > 0 else 0
            
            # P&L метрики
            total_pnl = sum(t['pnl'] for t in trades)
            avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
            
            winning_pnl = sum(t['pnl'] for t in trades if t['pnl'] > 0)
            losing_pnl = sum(t['pnl'] for t in trades if t['pnl'] < 0)
            
            avg_win = winning_pnl / winning_trades if winning_trades > 0 else 0
            avg_loss = losing_pnl / losing_trades if losing_trades > 0 else 0
            
            profit_factor = abs(winning_pnl / losing_pnl) if losing_pnl != 0 else float('inf')
            
            # Метрики риска
            initial_capital = self.trading_params['initial_capital']
            final_capital = initial_capital + total_pnl
            
            total_return = (final_capital - initial_capital) / initial_capital
            
            # Рассчитываем просадку
            if equity_curve:
                capitals = [e['capital'] for e in equity_curve]
                peak = initial_capital
                max_drawdown = 0
                
                for capital in capitals:
                    if capital > peak:
                        peak = capital
                    drawdown = (peak - capital) / peak
                    max_drawdown = max(max_drawdown, drawdown)
            else:
                max_drawdown = 0
            
            # Sharpe Ratio (профессиональный расчет)
            sharpe_ratio = 0
            annualized_return = 0
            calmar_ratio = 0
            
            if equity_curve and len(equity_curve) > 1:
                # Рассчитываем доходности по периодам
                returns = []
                for i in range(1, len(equity_curve)):
                    prev_capital = equity_curve[i-1]['capital']
                    curr_capital = equity_curve[i]['capital']
                    if prev_capital > 0:
                        ret = (curr_capital - prev_capital) / prev_capital
                        returns.append(ret)
                    else:
                        returns.append(0)
                
                if returns:
                    # Для минутных данных: 365 * 24 * 60 периодов в году
                    periods_per_year = 365 * 24 * 60
                    
                    # Средняя доходность за период
                    avg_return = np.mean(returns)
                    std_return = np.std(returns)
                    
                    # Годовая доходность
                    annualized_return = ((1 + avg_return) ** periods_per_year) - 1
                    
                    # Sharpe Ratio
                    if std_return != 0:
                        sharpe_ratio = (avg_return / std_return) * np.sqrt(periods_per_year)
                    
                    # Calmar Ratio
                    if max_drawdown != 0:
                        calmar_ratio = annualized_return / abs(max_drawdown)
            
            # Статистика по сессиям
            session_stats = {}
            for trade in trades:
                session = trade['session']
                if session not in session_stats:
                    session_stats[session] = {'trades': 0, 'pnl': 0, 'wins': 0}
                
                session_stats[session]['trades'] += 1
                session_stats[session]['pnl'] += trade['pnl']
                if trade['pnl'] > 0:
                    session_stats[session]['wins'] += 1
            
            # Сохраняем метрики
            self.performance_metrics = {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'total_return': total_return,
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'annualized_return': annualized_return,
                'calmar_ratio': calmar_ratio,
                'initial_capital': initial_capital,
                'final_capital': final_capital,
                'session_stats': session_stats,
                'run_id': self.run_id
            }
            
            logger.info(f"📊 Метрики производительности рассчитаны:")
            logger.info(f"   Всего сделок: {total_trades}")
            logger.info(f"   Винрейт: {win_rate:.2%}")
            logger.info(f"   Общий P&L: {total_pnl:.2f} USD")
            logger.info(f"   Общая доходность: {total_return:.2%}")
            logger.info(f"   Максимальная просадка: {max_drawdown:.2%}")
            logger.info(f"   Коэффициент Шарпа: {sharpe_ratio:.3f}")
            logger.info(f"   Calmar Ratio: {calmar_ratio:.3f}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета метрик: {e}")
    
    def save_backtest_results(self, trades: List[Dict], equity_curve: List[Dict]) -> None:
        """Сохраняет результаты бэктеста"""
        try:
            # Сохраняем сделки
            trades_df = pd.DataFrame(trades)
            trades_file = os.path.join(self.results_dir, 'backtest_trades.csv')
            trades_df.to_csv(trades_file, index=False)
            
            # Сохраняем equity curve
            equity_df = pd.DataFrame(equity_curve)
            equity_file = os.path.join(self.results_dir, 'equity_curve.csv')
            equity_df.to_csv(equity_file, index=False)
            
            # Сохраняем метрики
            metrics_file = os.path.join(self.results_dir, 'performance_metrics.json')
            with open(metrics_file, 'w', encoding='utf-8') as f:
                json.dump(self.performance_metrics, f, indent=2, ensure_ascii=False, default=str)
            
            # Создаем отчет
            self.generate_backtest_report()
            
            logger.info(f"💾 Результаты сохранены в {self.results_dir}/")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов: {e}")
    
    def generate_backtest_report(self) -> None:
        """Генерирует детальный отчет по бэктесту"""
        try:
            report_file = os.path.join(self.results_dir, 'backtest_report.md')
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("# 📊 Отчёт по Расширенному Бэктестингу v1.0\n\n")
                f.write("## 🎯 Цель анализа\n")
                f.write("Валидация производительности оптимизированной системы сигналов с реалистичными условиями торговли.\n\n")
                
                f.write("## 📈 Ключевые метрики производительности\n\n")
                f.write("| Метрика | Значение |\n")
                f.write("|---------|----------|\n")
                f.write(f"| Всего сделок | {self.performance_metrics['total_trades']} |\n")
                f.write(f"| Прибыльных сделок | {self.performance_metrics['winning_trades']} |\n")
                f.write(f"| Убыточных сделок | {self.performance_metrics['losing_trades']} |\n")
                f.write(f"| Винрейт | {self.performance_metrics['win_rate']:.2%} |\n")
                f.write(f"| Общий P&L | {self.performance_metrics['total_pnl']:.2f} USD |\n")
                f.write(f"| Средний P&L | {self.performance_metrics['avg_pnl']:.2f} USD |\n")
                f.write(f"| Общая доходность | {self.performance_metrics['total_return']:.2%} |\n")
                f.write(f"| Максимальная просадка | {self.performance_metrics['max_drawdown']:.2%} |\n")
                f.write(f"| Коэффициент Шарпа | {self.performance_metrics['sharpe_ratio']:.3f} |\n")
                f.write(f"| Profit Factor | {self.performance_metrics['profit_factor']:.2f} |\n\n")
                
                f.write("## 📊 Статистика по торговым сессиям\n\n")
                f.write("| Сессия | Сделок | P&L | Винрейт |\n")
                f.write("|--------|--------|-----|---------|\n")
                for session, stats in self.performance_metrics['session_stats'].items():
                    win_rate = stats['wins'] / stats['trades'] if stats['trades'] > 0 else 0
                    f.write(f"| {session} | {stats['trades']} | {stats['pnl']:.2f} USD | {win_rate:.2%} |\n")
                
                f.write("\n## 🔧 Параметры торговли\n\n")
                f.write("| Параметр | Значение |\n")
                f.write("|----------|----------|\n")
                for param, value in self.trading_params.items():
                    f.write(f"| {param} | {value} |\n")
                
                f.write("\n## 🛡️ Параметры управления рисками\n\n")
                f.write("| Параметр | Значение |\n")
                f.write("|----------|----------|\n")
                for param, value in self.risk_params.items():
                    f.write(f"| {param} | {value} |\n")
                
                f.write("\n## 📋 Рекомендации\n\n")
                
                if self.performance_metrics['win_rate'] > 0.6:
                    f.write("✅ **Высокий винрейт** - система показывает хорошую точность сигналов\n")
                elif self.performance_metrics['win_rate'] > 0.5:
                    f.write("⚠️ **Средний винрейт** - рекомендуется оптимизация параметров\n")
                else:
                    f.write("❌ **Низкий винрейт** - требуется пересмотр стратегии\n")
                
                if self.performance_metrics['profit_factor'] > 1.5:
                    f.write("✅ **Высокий Profit Factor** - хорошее соотношение прибыли к убыткам\n")
                elif self.performance_metrics['profit_factor'] > 1.0:
                    f.write("⚠️ **Положительный Profit Factor** - система прибыльна\n")
                else:
                    f.write("❌ **Отрицательный Profit Factor** - система убыточна\n")
                
                if self.performance_metrics['max_drawdown'] < 0.1:
                    f.write("✅ **Низкая просадка** - хорошее управление рисками\n")
                elif self.performance_metrics['max_drawdown'] < 0.2:
                    f.write("⚠️ **Средняя просадка** - приемлемый уровень риска\n")
                else:
                    f.write("❌ **Высокая просадка** - требуется улучшение risk management\n")
                
                f.write("\n## 🏆 Заключение\n\n")
                f.write("Система прошла комплексную валидацию с реалистичными условиями торговли. ")
                f.write("Результаты показывают потенциал для дальнейшего развития и оптимизации.\n\n")
                
                f.write(f"*Отчёт сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
            
            logger.info(f"📄 Отчёт сохранен в {report_file}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации отчёта: {e}")
    
    def run_walk_forward_backtest(self, limit_days=None) -> None:
        """Запускает Walk-Forward бэктест системы"""
        try:
            logger.info("🚀 Запуск Walk-Forward бэктестинга...")
            
            # Загружаем полные данные
            full_data = self.load_features_data()
            if full_data.empty:
                logger.error("❌ Нет данных для Walk-Forward бэктестинга")
                return
            
            # Если задан лимит дней, используем только последние N дней
            if limit_days:
                start_date = full_data['time'].max() - pd.Timedelta(days=limit_days)
                full_data = full_data[full_data['time'] >= start_date]
                logger.info(f"📉 Используем данные за последние {limit_days} дней")
            
            # Сортируем данные
            full_data = full_data.sort_values('time').reset_index(drop=True)
            
            # Определяем параметры окон
            train_window_size_days = self.walk_forward_params['train_window_size_days']
            test_window_size_days = self.walk_forward_params['test_window_size_days']
            step_size_days = self.walk_forward_params['step_size_days']
            
            # Определяем начальную и конечную даты
            start_date = full_data['time'].min()
            end_date = full_data['time'].max()
            
            logger.info(f"📅 Период данных: {start_date} - {end_date}")
            
            # Инициализация переменных для агрегации результатов
            all_trades = []
            all_equity_curves = []
            walk_forward_results = []
            
            # Основной цикл Walk-Forward
            current_date = start_date
            
            while current_date + pd.Timedelta(days=train_window_size_days + test_window_size_days) <= end_date:
                # Определяем границы обучающего окна
                train_start = current_date
                train_end = current_date + pd.Timedelta(days=train_window_size_days)
                
                # Определяем границы тестового окна
                test_start = train_end
                test_end = train_end + pd.Timedelta(days=test_window_size_days)
                
                # Извлекаем обучающие и тестовые данные
                train_data = full_data[(full_data['time'] >= train_start) & (full_data['time'] < train_end)]
                test_data = full_data[(full_data['time'] >= test_start) & (full_data['time'] < test_end)]
                
                # Проверяем, что оба набора данных не пусты
                if not train_data.empty and not test_data.empty:
                    logger.info(f"🔄 Processing Train: {train_start} - {train_end}, Test: {test_start} - {test_end}")
                    logger.info(f"   📊 Train data: {len(train_data)} records, Test data: {len(test_data)} records")
                    
                    # Рассчитываем пороги на обучающем срезе
                    thresholds = self.calculate_thresholds(train_data, z_score_threshold=self.z_score_threshold)
                    logger.info(f"   📊 Пороги: нижний = {thresholds['lower_threshold']:.4f}, верхний = {thresholds['upper_threshold']:.4f}")
                    
                    # Рассчитываем пороги волатильности (33-й и 66-й перцентили)
                    if 'historical_volatility_24h' in train_data.columns:
                        vol_data = train_data['historical_volatility_24h'].dropna()
                        if len(vol_data) > 0:
                            low_vol_threshold = np.percentile(vol_data, 33)
                            high_vol_threshold = np.percentile(vol_data, 66)
                            self.volatility_thresholds = {
                                'low': low_vol_threshold,
                                'high': high_vol_threshold
                            }
                            logger.info(f"   📊 Пороги волатильности: low = {low_vol_threshold:.6f}, high = {high_vol_threshold:.6f}")
                        else:
                            logger.warning("   ⚠️ Нет данных для расчета порогов волатильности")
                            self.volatility_thresholds = None
                    else:
                        logger.warning("   ⚠️ В данных отсутствует колонка historical_volatility_24h")
                        self.volatility_thresholds = None
                    
                    # Генерируем сигналы на тестовом срезе
                    test_signals_df = self.generate_signals_from_data(test_data, thresholds, z_score_threshold=self.z_score_threshold, sma_period=self.sma_period)
                    logger.info(f"   📈 Сгенерировано сигналов: {len(test_signals_df)}")
                    
                    if not test_signals_df.empty:
                        # Сохраняем оригинальные параметры
                        original_signals_file = self.signals_file
                        
                        # Создаем временный файл с сигналами для текущего окна
                        temp_signals_file = f"temp_signals_{test_start.strftime('%Y%m%d')}_{test_end.strftime('%Y%m%d')}.csv"
                        test_signals_df.to_csv(temp_signals_file, index=False)
                        
                        # Устанавливаем временный файл сигналов
                        self.signals_file = temp_signals_file
                        
                        # Запускаем бэктест на тестовых данных
                        window_results = self.run_backtest()
                        
                        # Восстанавливаем оригинальный файл сигналов
                        self.signals_file = original_signals_file
                        
                        # Удаляем временный файл
                        if os.path.exists(temp_signals_file):
                            os.remove(temp_signals_file)
                            
                        # Сохраняем результаты бэктеста для этого окна
                        if window_results:
                            walk_forward_results.append({
                                'train_period': (train_start, train_end),
                                'test_period': (test_start, test_end),
                                'thresholds': thresholds,
                                'signals_count': len(test_signals_df),
                                'metrics': window_results
                            })
                            # Добавляем сделки и кривую эквити в общие списки
                            all_trades.extend(self.trades_history)
                            all_equity_curves.extend(self.equity_curve)
                
                # Сдвигаем окно
                current_date += pd.Timedelta(days=step_size_days)
            
            logger.info("✅ Walk-Forward бэктест завершен успешно!")
            
            # Сохраняем результаты Walk-Forward
            self.walk_forward_results = walk_forward_results
            
            # Агрегируем результаты
            self.aggregate_walk_forward_results(walk_forward_results)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в Walk-Forward бэктесте: {e}")
    
    def aggregate_walk_forward_results(self, results: List[Dict]) -> None:
        """Агрегирует результаты Walk-Forward бэктеста"""
        try:
            logger.info("📊 Агрегация результатов Walk-Forward...")
            logger.info(f"   Всего окон: {len(results)}")
            
            if not results:
                logger.warning("   Нет результатов для агрегации")
                return
            
            # Собираем все метрики из окон
            all_metrics = []
            all_sharpe_ratios = []
            all_returns = []
            all_drawdowns = []
            
            # Для "сшивания" кривых эквити нам нужно объединить equity_curve из всех окон
            # и пересчитать метрики на основе объединенной кривой
            
            # Сначала соберем все сделки из всех окон
            all_trades = []
            for result in results:
                # У каждого окна есть свои метрики, но мы хотим пересчитать их на основе всех данных
                # Поэтому соберем все сделки
                pass  # Сделки уже собраны в run_walk_forward_backtest
            
            # Используем уже собранные сделки и кривую эквити
            # Пересчитываем метрики на основе всех данных
            self.calculate_performance_metrics(self.trades_history, self.equity_curve)
            
            # Генерируем финальный отчет
            self.generate_backtest_report()
            
            # Выводим сводку по окнам
            logger.info("   📊 Сводка по окнам:")
            for i, result in enumerate(results):
                train_period = result['train_period']
                test_period = result['test_period']
                signals_count = result['signals_count']
                metrics = result['metrics']
                
                logger.info(f"   Окно {i+1}: Train {train_period[0].strftime('%Y-%m-%d')} - {train_period[1].strftime('%Y-%m-%d')}, "
                           f"Test {test_period[0].strftime('%Y-%m-%d')} - {test_period[1].strftime('%Y-%m-%d')}")
                logger.info(f"     Сигналов: {signals_count}, Sharpe: {metrics.get('sharpe_ratio', 0):.3f}, "
                           f"Доходность: {metrics.get('total_return', 0):.2%}, Просадка: {metrics.get('max_drawdown', 0):.2%}")
            
            logger.info("✅ Агрегация результатов завершена!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка агрегации результатов: {e}")
    
    def run(self, walk_forward=False, limit_days=None):
        """Основной метод запуска бэктестинга"""
        try:
            if walk_forward:
                logger.info("🚀 Запуск Walk-Forward бэктестинга системы сигналов...")
                self.run_walk_forward_backtest(limit_days=limit_days)
            else:
                logger.info("🚀 Запуск расширенного бэктестинга системы сигналов...")
                # Запускаем бэктест
                results = self.run_backtest()
                
                if results:
                    logger.info("✅ Бэктестинг завершен успешно!")
                    logger.info(f"📊 Результаты сохранены в {self.results_dir}/")
                else:
                    logger.error("❌ Бэктестинг завершился с ошибками")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в основном методе бэктестинга: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Advanced Backtest System')
    parser.add_argument('--walk_forward', action='store_true', help='Run Walk-Forward backtest')
    parser.add_argument('--limit_days', type=int, default=None, help='Limit the number of days for backtest')
    parser.add_argument('--z_score_threshold', type=float, default=2.0, help='Z-Score threshold for signal generation')
    parser.add_argument('--low_vol_params', type=str, default=None, help='JSON string with low volatility parameters (atr_risk_multiplier, reward_ratio)')
    parser.add_argument('--mid_vol_params', type=str, default=None, help='JSON string with mid volatility parameters (atr_risk_multiplier, reward_ratio)')
    parser.add_argument('--high_vol_params', type=str, default=None, help='JSON string with high volatility parameters (atr_risk_multiplier, reward_ratio)')
    parser.add_argument('--initial_capital', type=float, default=10000, help='Initial capital for backtest')
    parser.add_argument('--signals_file', type=str, default='candle_signals.csv', help='Path to signals file')
    parser.add_argument('--position_size', type=float, default=0.1, help='Position size as fraction of capital (default: 0.1)')
    parser.add_argument('--fixed_trading_params', type=str, default=None, help='JSON string with fixed trading parameters (atr_risk_multiplier, reward_ratio)')
    parser.add_argument('--results_dir', type=str, default=None, help='Directory for backtest results')
    parser.add_argument('--data_file', type=str, default='basis_features_1m.parquet', help='Path to features data file')
    parser.add_argument('--asset', type=str, default='SOL', help='Asset to backtest (SOL or BTC)')
    parser.add_argument('--signal_generator_module', type=str, default='basis_signal_generator', help='Signal generator module to use')
    
    args = parser.parse_args()
    
    # Парсим JSON-строки параметров, если они предоставлены
    import json
    low_vol_params = None
    mid_vol_params = None
    high_vol_params = None
    fixed_trading_params = None
    
    if args.low_vol_params:
        try:
            low_vol_params = json.loads(args.low_vol_params)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга low_vol_params: {e}")
            low_vol_params = None
    
    if args.mid_vol_params:
        try:
            mid_vol_params = json.loads(args.mid_vol_params)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга mid_vol_params: {e}")
            mid_vol_params = None
            
    if args.high_vol_params:
        try:
            high_vol_params = json.loads(args.high_vol_params)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга high_vol_params: {e}")
            high_vol_params = None
            
    if args.fixed_trading_params:
        try:
            fixed_trading_params = json.loads(args.fixed_trading_params)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга fixed_trading_params: {e}")
            fixed_trading_params = None
    
    backtest = AdvancedBacktestSystem(
        z_score_threshold=args.z_score_threshold,
        low_vol_params=low_vol_params,
        mid_vol_params=mid_vol_params,
        high_vol_params=high_vol_params,
        initial_capital=args.initial_capital,
        position_size=args.position_size,
        fixed_trading_params=fixed_trading_params,
        signals_file=args.signals_file,
        results_dir=args.results_dir,
        data_file=args.data_file,
        asset=args.asset,
        signal_generator_module=args.signal_generator_module
    )
    backtest.run(walk_forward=args.walk_forward, limit_days=args.limit_days)
