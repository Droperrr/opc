#!/usr/bin/env python3
"""
Модуль оценки результатов бэктеста сигналов
"""

import pandas as pd
import numpy as np
from datetime import datetime
from logger import get_logger

logger = get_logger()

class BacktestEvaluator:
    """Оценщик результатов бэктеста"""
    
    def __init__(self, csv_file='signals_backtest.csv'):
        """Инициализация оценщика"""
        self.csv_file = csv_file
        self.trades_df = None
        
    def load_trades(self):
        """Загружает сделки из CSV файла"""
        try:
            self.trades_df = pd.read_csv(self.csv_file)
            logger.info(f"📊 Загружено {len(self.trades_df)} сделок из {self.csv_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сделок: {e}")
            return False
    
    def calculate_strategy_stats(self, result_column):
        """Рассчитывает статистику для конкретной стратегии"""
        if self.trades_df is None:
            return None
        
        # Фильтруем завершенные сделки
        completed_trades = self.trades_df[self.trades_df[result_column].notna()]
        
        if len(completed_trades) == 0:
            return {
                'total_trades': 0,
                'winrate': 0.0,
                'avg_profit': 0.0,
                'total_profit': 0.0,
                'max_drawdown': 0.0,
                'avg_hold_time': 0.0
            }
        
        # Базовые метрики
        total_trades = len(completed_trades)
        profitable_trades = len(completed_trades[completed_trades[result_column] > 0])
        winrate = (profitable_trades / total_trades) * 100
        
        # Прибыль
        avg_profit = completed_trades[result_column].mean()
        total_profit = completed_trades[result_column].sum()
        
        # Максимальная просадка
        cumulative_returns = completed_trades[result_column].cumsum()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max * 100
        max_drawdown = abs(drawdown.min())
        
        # Среднее время удержания
        time_columns = {
            'result_1pct': 'exit_time_1pct',
            'result_2pct': 'exit_time_2pct',
            'result_5pct': 'exit_time_5pct',
            'result_trend_v1': 'exit_time_trend_v1',
            'result_trend_v2': 'exit_time_trend_v2',
            'result_trend_v3': 'exit_time_trend_v3'
        }
        
        time_column = time_columns.get(result_column)
        avg_hold_time = 0.0
        
        if time_column and time_column in completed_trades.columns:
            # Рассчитываем время удержания
            completed_trades['signal_time'] = pd.to_datetime(completed_trades['signal_time'])
            completed_trades[time_column] = pd.to_datetime(completed_trades[time_column])
            
            hold_times = (completed_trades[time_column] - completed_trades['signal_time']).dt.total_seconds() / 3600  # в часах
            avg_hold_time = hold_times.mean()
        
        return {
            'total_trades': total_trades,
            'winrate': round(winrate, 2),
            'avg_profit': round(avg_profit, 2),
            'total_profit': round(total_profit, 2),
            'max_drawdown': round(max_drawdown, 2),
            'avg_hold_time': round(avg_hold_time, 2)
        }
    
    def generate_summary_report(self):
        """Генерирует итоговый отчет"""
        if self.trades_df is None:
            logger.error("❌ Нет данных для анализа")
            return None
        
        logger.info("📊 Генерация итогового отчета")
        
        # Рассчитываем статистику для всех стратегий
        strategies = {
            '1% таргет': 'result_1pct',
            '2% таргет': 'result_2pct',
            '5% таргет': 'result_5pct',
            'Тренд v1': 'result_trend_v1',
            'Тренд v2': 'result_trend_v2',
            'Тренд v3': 'result_trend_v3'
        }
        
        report = []
        report.append("=== Итоги бэктеста ===")
        report.append(f"Период: последний месяц")
        report.append(f"Кол-во сигналов: {len(self.trades_df)}")
        report.append("")
        report.append("--- Результаты по стратегиям ---")
        
        for strategy_name, result_column in strategies.items():
            stats = self.calculate_strategy_stats(result_column)
            if stats:
                report.append(f"{strategy_name}:")
                report.append(f"  Winrate: {stats['winrate']}%")
                report.append(f"  Средний профит: {stats['avg_profit']}%")
                report.append(f"  Суммарный профит: {stats['total_profit']}%")
                report.append(f"  Макс. просадка: {stats['max_drawdown']}%")
                report.append(f"  Среднее удержание: {stats['avg_hold_time']}ч")
                report.append("")
        
        # Добавляем общую статистику
        report.append("--- Общая статистика ---")
        report.append(f"Всего сделок: {len(self.trades_df)}")
        
        # Анализ типов сигналов
        signal_counts = self.trades_df['signal'].value_counts()
        report.append("Распределение сигналов:")
        for signal, count in signal_counts.items():
            report.append(f"  {signal}: {count}")
        
        report.append("")
        report.append(f"Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "\n".join(report)
    
    def save_report(self, report, filename='results_summary.txt'):
        """Сохраняет отчет в файл"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"💾 Отчет сохранен в {filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения отчета: {e}")
            return False
    
    def get_best_strategy(self):
        """Определяет лучшую стратегию по совокупности метрик"""
        if self.trades_df is None:
            return None
        
        strategies = {
            '1% таргет': 'result_1pct',
            '2% таргет': 'result_2pct',
            '5% таргет': 'result_5pct',
            'Тренд v1': 'result_trend_v1',
            'Тренд v2': 'result_trend_v2',
            'Тренд v3': 'result_trend_v3'
        }
        
        best_strategy = None
        best_score = -float('inf')
        
        for strategy_name, result_column in strategies.items():
            stats = self.calculate_strategy_stats(result_column)
            if stats and stats['total_trades'] > 0:
                # Простая оценка: winrate + total_profit - max_drawdown
                score = stats['winrate'] + stats['total_profit'] - stats['max_drawdown']
                
                if score > best_score:
                    best_score = score
                    best_strategy = {
                        'name': strategy_name,
                        'stats': stats,
                        'score': score
                    }
        
        return best_strategy
    
    def run_evaluation(self):
        """Запускает полную оценку результатов"""
        logger.info("🚀 Запуск оценки результатов бэктеста")
        
        # Загружаем данные
        if not self.load_trades():
            return None
        
        # Генерируем отчет
        report = self.generate_summary_report()
        if report is None:
            return None
        
        # Сохраняем отчет
        self.save_report(report)
        
        # Определяем лучшую стратегию
        best_strategy = self.get_best_strategy()
        if best_strategy:
            logger.info(f"🏆 Лучшая стратегия: {best_strategy['name']}")
            logger.info(f"📊 Оценка: {best_strategy['score']:.2f}")
        
        logger.info("✅ Оценка завершена успешно")
        return report

def run_evaluation_demo():
    """Демонстрация работы оценщика"""
    logger.info("🎯 Демонстрация оценщика результатов")
    
    # Создаем оценщик
    evaluator = BacktestEvaluator()
    
    # Запускаем оценку
    report = evaluator.run_evaluation()
    
    if report:
        print("\n📋 Итоговый отчет:")
        print(report)
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация работы оценщика
    run_evaluation_demo()
