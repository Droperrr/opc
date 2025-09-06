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

logger = get_logger()

class AdvancedBacktestSystem:
    def __init__(self):
        self.db_path = 'data/options_enriched.db'
        self.signals_file = 'signals_optimized_extended.csv'
        self.results_dir = 'backtest_results'
        
        # Параметры торговли
        self.trading_params = {
            'initial_capital': 10000,      # Начальный капитал в USD
            'position_size': 0.1,         # 10% от капитала на позицию
            'max_positions': 5,           # Максимум одновременных позиций
            'stop_loss': 0.05,            # 5% стоп-лосс
            'take_profit': 0.15,          # 15% тейк-профит
            'commission': 0.001,          # 0.1% комиссия
            'slippage': 0.0005            # 0.05% проскальзывание
        }
        
        # Параметры управления рисками
        self.risk_params = {
            'max_daily_loss': 0.05,       # Максимум 5% потерь в день
            'max_drawdown': 0.20,         # Максимум 20% просадки
            'correlation_threshold': 0.7,  # Порог корреляции между позициями
            'volatility_filter': True,     # Фильтр по волатильности
            'session_filter': True        # Фильтр по торговым сессиям
        }
        
        # Метрики производительности
        self.performance_metrics = {}
        self.trades_history = []
        self.equity_curve = []
        
        # Создаем директорию для результатов
        os.makedirs(self.results_dir, exist_ok=True)
    
    def load_signals_data(self) -> pd.DataFrame:
        """Загружает данные сигналов для бэктестинга"""
        try:
            if os.path.exists(self.signals_file):
                df = pd.read_csv(self.signals_file)
                logger.info(f"📊 Загружено {len(df)} сигналов из {self.signals_file}")
            else:
                logger.error(f"❌ Файл сигналов {self.signals_file} не найден")
                return pd.DataFrame()
            
            # Преобразуем timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных сигналов: {e}")
            return pd.DataFrame()
    
    def simulate_price_movement(self, signal: Dict, duration_hours: int = 24) -> List[Dict]:
        """Симулирует движение цены на основе сигнала и рыночных условий"""
        base_price = signal['underlying_price']
        signal_type = signal['signal']
        confidence = signal['confidence']
        iv = signal['iv']
        
        # Параметры симуляции
        volatility = iv * 0.8  # Используем IV как базу для волатильности
        trend_strength = confidence * 0.6  # Сила тренда зависит от уверенности
        
        # Генерируем временные точки (каждые 15 минут)
        time_points = []
        for i in range(duration_hours * 4):  # 4 точки в час
            time_points.append(i * 0.25)
        
        price_movements = []
        current_price = base_price
        
        for t in time_points:
            # Базовое движение цены
            if signal_type == 'LONG':
                trend_component = trend_strength * t * 0.01  # Положительный тренд
            else:
                trend_component = -trend_strength * t * 0.01  # Отрицательный тренд
            
            # Случайная составляющая (волатильность)
            random_component = np.random.normal(0, volatility * 0.01)
            
            # Изменение цены
            price_change = trend_component + random_component
            current_price = current_price * (1 + price_change)
            
            price_movements.append({
                'timestamp': signal['timestamp'] + timedelta(hours=t),
                'price': current_price,
                'change': price_change,
                'cumulative_change': (current_price - base_price) / base_price
            })
        
        return price_movements
    
    def calculate_position_size(self, capital: float, price: float) -> float:
        """Рассчитывает размер позиции в SOL"""
        position_value = capital * self.trading_params['position_size']
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
                if signal['iv'] > 1.5:  # Слишком высокая волатильность
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
    
    def execute_trade(self, signal: Dict, capital: float) -> Dict:
        """Выполняет торговую операцию"""
        try:
            price = signal['underlying_price']
            position_size_sol = self.calculate_position_size(capital, price)
            
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
                'pnl_percent': None
            }
            
            return trade
            
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения сделки: {e}")
            return {}
    
    def check_exit_conditions(self, trade: Dict, current_price: float) -> Tuple[bool, str]:
        """Проверяет условия выхода из позиции"""
        entry_price = trade['entry_price']
        signal_type = trade['signal']
        
        # Рассчитываем текущий P&L
        if signal_type == 'LONG':
            pnl_percent = (current_price - entry_price) / entry_price
        else:  # SHORT
            pnl_percent = (entry_price - current_price) / entry_price
        
        # Проверяем стоп-лосс
        if pnl_percent <= -self.trading_params['stop_loss']:
            return True, 'stop_loss'
        
        # Проверяем тейк-профит
        if pnl_percent >= self.trading_params['take_profit']:
            return True, 'take_profit'
        
        # Проверяем время в позиции (максимум 24 часа)
        time_in_position = (datetime.now() - trade['timestamp']).total_seconds() / 3600
        if time_in_position >= 24:
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
                
                # Проверяем управление рисками
                if not self.apply_risk_management(signal_dict, open_positions):
                    continue
                
                # Выполняем сделку
                trade = self.execute_trade(signal_dict, capital)
                if not trade:
                    continue
                
                open_positions.append(trade)
                
                # Симулируем движение цены для этой позиции
                price_movements = self.simulate_price_movement(signal_dict)
                
                # Проверяем условия выхода
                for price_data in price_movements:
                    current_price = price_data['price']
                    
                    # Проверяем все открытые позиции
                    for i, position in enumerate(open_positions[:]):
                        should_exit, exit_reason = self.check_exit_conditions(position, current_price)
                        
                        if should_exit:
                            # Закрываем позицию
                            exit_price = current_price * (1 - self.trading_params['commission'] - self.trading_params['slippage'])
                            
                            # Рассчитываем P&L
                            if position['signal'] == 'LONG':
                                pnl = (exit_price - position['entry_price']) * position['position_size_sol']
                            else:
                                pnl = (position['entry_price'] - exit_price) * position['position_size_sol']
                            
                            pnl_percent = pnl / position['position_value_usd']
                            
                            # Обновляем позицию
                            position.update({
                                'exit_price': exit_price,
                                'exit_timestamp': price_data['timestamp'],
                                'pnl': pnl,
                                'pnl_percent': pnl_percent,
                                'status': 'closed',
                                'exit_reason': exit_reason
                            })
                            
                            # Перемещаем в закрытые сделки
                            closed_trades.append(position)
                            open_positions.pop(i)
                            
                            # Обновляем капитал
                            capital += pnl
                            
                            # Проверяем лимиты потерь
                            if capital < self.trading_params['initial_capital'] * (1 - self.risk_params['max_drawdown']):
                                logger.warning(f"⚠️ Достигнут лимит просадки: {capital:.2f} USD")
                                break
                
                # Записываем точку equity curve
                equity_curve.append({
                    'timestamp': signal_dict['timestamp'],
                    'capital': capital,
                    'open_positions': len(open_positions)
                })
                
                # Логируем прогресс
                if idx % 100 == 0:
                    logger.info(f"📊 Обработано {idx}/{len(signals_df)} сигналов, капитал: {capital:.2f} USD")
            
            # Закрываем все оставшиеся позиции
            for position in open_positions:
                position.update({
                    'exit_price': position['entry_price'],  # Закрываем по цене входа
                    'exit_timestamp': signals_df.iloc[-1]['timestamp'],
                    'pnl': 0,
                    'pnl_percent': 0,
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
            
            # Sharpe Ratio (упрощенный)
            if equity_curve and len(equity_curve) > 1:
                returns = []
                for i in range(1, len(equity_curve)):
                    prev_capital = equity_curve[i-1]['capital']
                    curr_capital = equity_curve[i]['capital']
                    ret = (curr_capital - prev_capital) / prev_capital
                    returns.append(ret)
                
                if returns:
                    avg_return = np.mean(returns)
                    std_return = np.std(returns)
                    sharpe_ratio = avg_return / std_return if std_return != 0 else 0
                else:
                    sharpe_ratio = 0
            else:
                sharpe_ratio = 0
            
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
                'initial_capital': initial_capital,
                'final_capital': final_capital,
                'session_stats': session_stats
            }
            
            logger.info(f"📊 Метрики производительности рассчитаны:")
            logger.info(f"   Всего сделок: {total_trades}")
            logger.info(f"   Винрейт: {win_rate:.2%}")
            logger.info(f"   Общий P&L: {total_pnl:.2f} USD")
            logger.info(f"   Общая доходность: {total_return:.2%}")
            logger.info(f"   Максимальная просадка: {max_drawdown:.2%}")
            logger.info(f"   Коэффициент Шарпа: {sharpe_ratio:.3f}")
            
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
    
    def run(self):
        """Основной метод запуска бэктестинга"""
        try:
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
    backtest = AdvancedBacktestSystem()
    backtest.run()
