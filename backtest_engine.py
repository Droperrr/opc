#!/usr/bin/env python3
"""
Backtesting Engine для формулы volatility_focused
Задача S-04: Backtesting на полной истории + адаптивные пороги
"""

import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
from logger import get_logger

logger = get_logger()

class BacktestEngine:
    def __init__(self):
        self.db_path = 'data/sol_iv.db'
        self.results_dir = 'data/backtests'
        self.plots_dir = 'data/backtests/plots'
        
        # Создаем директории
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir, exist_ok=True)
        
        # Параметры backtesting
        self.base_threshold = 0.7
        self.volatility_factor = 1.2
        self.atr_period = 14
        
    def calculate_atr(self, df, period=14):
        """
        Рассчитывает Average True Range (ATR) для DataFrame
        """
        try:
            df = df.copy()
            
            # Если нет колонок high/low, используем spot_price как приближение
            if 'high' not in df.columns:
                df['high'] = df['spot_price'] * 1.001  # +0.1%
            if 'low' not in df.columns:
                df['low'] = df['spot_price'] * 0.999   # -0.1%
            
            df['prev_close'] = df['spot_price'].shift(1)
            
            # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
            df['tr1'] = df['high'] - df['low']
            df['tr2'] = abs(df['high'] - df['prev_close'])
            df['tr3'] = abs(df['low'] - df['prev_close'])
            df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            # ATR = скользящее среднее True Range
            df['atr'] = df['true_range'].rolling(window=period).mean()
            
            return df['atr']
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета ATR: {e}")
            return pd.Series([0.01] * len(df))  # Fallback значение
    
    def load_historical_data(self, start_date='2024-03-01', end_date=None):
        """
        Загружает исторические данные за 6 месяцев
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем данные по опционам
            query = """
            SELECT time, spot_price, iv_30d, skew_30d, basis_rel, oi_total
            FROM iv_agg 
            WHERE time BETWEEN ? AND ? AND timeframe = '1m'
            ORDER BY time
            """
            
            iv_data = pd.read_sql_query(query, conn, params=[start_date, end_date])
            
            # Загружаем данные по basis
            basis_query = """
            SELECT time, basis_rel as basis_rel_basis, funding_rate
            FROM basis_agg 
            WHERE time BETWEEN ? AND ? AND timeframe = '1m'
            ORDER BY time
            """
            
            basis_data = pd.read_sql_query(basis_query, conn, params=[start_date, end_date])
            
            # Объединяем данные
            df = pd.merge(iv_data, basis_data, on='time', how='left')
            
            # Заполняем пропуски
            df = df.fillna(method='ffill').fillna(0)
            
            conn.close()
            
            logger.info(f"📊 Загружено {len(df)} записей с {start_date} по {end_date}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            return None
    
    def calculate_formula_value(self, row, params):
        """
        Вычисляет значение формулы volatility_focused
        """
        try:
            # Параметры формулы
            a = params.get('a', 1.0)
            b = params.get('b', 0.5)
            c = params.get('c', 0.3)
            
            # Базовые значения
            iv = row.get('iv_30d', 0)
            skew = row.get('skew_30d', 0)
            basis_rel = row.get('basis_rel', 0)
            
            # Вычисляем Y
            Y = a * iv + b * skew - c * basis_rel
            
            return Y
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления формулы: {e}")
            return 0
    
    def calculate_dynamic_threshold(self, atr_value, avg_volatility=0.02):
        """
        Вычисляет адаптивный порог на основе волатильности
        """
        try:
            if pd.isna(atr_value) or atr_value == 0:
                return self.base_threshold
            
            # Нормализуем ATR относительно средней волатильности
            volatility_ratio = atr_value / avg_volatility
            
            # Адаптивный порог
            dynamic_threshold = self.base_threshold * (1 + self.volatility_factor * (volatility_ratio - 1))
            
            # Ограничиваем порог разумными пределами
            dynamic_threshold = max(0.3, min(2.0, dynamic_threshold))
            
            return dynamic_threshold
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета адаптивного порога: {e}")
            return self.base_threshold
    
    def generate_signals(self, df, use_adaptive_threshold=True):
        """
        Генерирует сигналы на основе формулы и порогов
        """
        try:
            results = []
            
            # Рассчитываем ATR
            df['atr'] = self.calculate_atr(df, self.atr_period)
            avg_volatility = df['atr'].mean()
            
            # Параметры формулы
            params = {'a': 1.0, 'b': 0.5, 'c': 0.3}
            
            for i, row in df.iterrows():
                # Вычисляем значение формулы
                Y_value = self.calculate_formula_value(row, params)
                
                # Определяем порог
                if use_adaptive_threshold:
                    threshold = self.calculate_dynamic_threshold(row['atr'], avg_volatility)
                else:
                    threshold = self.base_threshold
                
                # Генерируем сигнал
                if Y_value > threshold:
                    signal = 'BUY'
                elif Y_value < -threshold:
                    signal = 'SELL'
                else:
                    signal = 'NEUTRAL'
                
                # Проверяем результат через 15 минут
                result = self.check_result(df, i, signal)
                
                results.append({
                    'time': row['time'],
                    'spot_price': row['spot_price'],
                    'Y_value': Y_value,
                    'threshold': threshold,
                    'signal': signal,
                    'result': result,
                    'atr': row['atr'],
                    'iv_30d': row['iv_30d'],
                    'skew_30d': row['skew_30d'],
                    'basis_rel': row['basis_rel']
                })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации сигналов: {e}")
            return pd.DataFrame()
    
    def check_result(self, df, current_index, signal):
        """
        Проверяет результат сигнала через 15 минут
        """
        try:
            if signal == 'NEUTRAL':
                return 'neutral'
            
            # Ищем цену через 15 минут (15 свечей)
            future_index = current_index + 15
            if future_index >= len(df):
                return 'unknown'
            
            current_price = df.iloc[current_index]['spot_price']
            future_price = df.iloc[future_index]['spot_price']
            
            # Рассчитываем изменение цены
            price_change = (future_price - current_price) / current_price
            
            # Определяем результат
            if signal == 'BUY':
                if price_change > 0.003:  # +0.3%
                    return 'profit'
                elif price_change < -0.003:  # -0.3%
                    return 'loss'
                else:
                    return 'neutral'
            elif signal == 'SELL':
                if price_change < -0.003:  # -0.3%
                    return 'profit'
                elif price_change > 0.003:  # +0.3%
                    return 'loss'
                else:
                    return 'neutral'
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки результата: {e}")
            return 'unknown'
    
    def calculate_metrics(self, results_df):
        """
        Рассчитывает метрики эффективности
        """
        try:
            if results_df.empty:
                return {}
            
            # Фильтруем только сигналы BUY/SELL
            signals = results_df[results_df['signal'].isin(['BUY', 'SELL'])]
            
            if signals.empty:
                return {
                    'total_signals': 0,
                    'accuracy': 0,
                    'win_rate': 0,
                    'profit_factor': 0,
                    'max_drawdown': 0
                }
            
            # Базовые метрики
            total_signals = len(signals)
            profitable_signals = len(signals[signals['result'] == 'profit'])
            loss_signals = len(signals[signals['result'] == 'loss'])
            
            accuracy = profitable_signals / total_signals if total_signals > 0 else 0
            win_rate = profitable_signals / (profitable_signals + loss_signals) if (profitable_signals + loss_signals) > 0 else 0
            
            # Profit Factor
            if loss_signals == 0:
                profit_factor = float('inf') if profitable_signals > 0 else 0
            else:
                profit_factor = profitable_signals / loss_signals
            
            # Max Drawdown
            signals['cum_profit'] = (signals['result'] == 'profit').astype(int).cumsum()
            signals['cum_loss'] = (signals['result'] == 'loss').astype(int).cumsum()
            signals['net_profit'] = signals['cum_profit'] - signals['cum_loss']
            
            if len(signals) > 0:
                signals['peak'] = signals['net_profit'].cummax()
                signals['drawdown'] = signals['peak'] - signals['net_profit']
                max_drawdown = signals['drawdown'].max()
            else:
                max_drawdown = 0
            
            return {
                'total_signals': total_signals,
                'accuracy': accuracy,
                'win_rate': win_rate,
                'profit_factor': profit_factor,
                'max_drawdown': max_drawdown
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета метрик: {e}")
            return {}
    
    def create_equity_curve_plot(self, results_df, title="Backtesting: volatility_focused"):
        """
        Создает график equity curve и drawdown
        """
        try:
            # Фильтруем только сигналы BUY/SELL
            signals = results_df[results_df['signal'].isin(['BUY', 'SELL'])].copy()
            
            if signals.empty:
                logger.warning("⚠️ Нет сигналов для построения equity curve")
                return None
            
            # Рассчитываем накопленную прибыль
            signals['profit_value'] = (signals['result'] == 'profit').astype(int) - (signals['result'] == 'loss').astype(int)
            signals['cum_profit'] = signals['profit_value'].cumsum()
            
            # Рассчитываем просадку
            signals['peak'] = signals['cum_profit'].cummax()
            signals['drawdown'] = signals['peak'] - signals['cum_profit']
            
            fig = make_subplots(
                rows=2, cols=1, 
                shared_xaxes=True, 
                vertical_spacing=0.1,
                subplot_titles=('Накопленная прибыль', 'Просадка')
            )
            
            # Equity curve
            fig.add_trace(
                go.Scatter(
                    x=signals['time'], 
                    y=signals['cum_profit'],
                    name="Накопленная прибыль", 
                    line=dict(color="green", width=2)
                ),
                row=1, col=1
            )
            
            # Drawdown
            fig.add_trace(
                go.Scatter(
                    x=signals['time'], 
                    y=signals['drawdown'],
                    name="Просадка", 
                    fill="tozeroy", 
                    line=dict(color="red", width=1)
                ),
                row=2, col=1
            )
            
            fig.update_layout(
                title=title,
                height=700,
                showlegend=True,
                xaxis2_title="Время",
                yaxis_title="Прибыль (сигналы)",
                yaxis2_title="Просадка"
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания equity curve: {e}")
            return None
    
    def run_backtest(self, start_date='2024-03-01', end_date=None):
        """
        Запускает полный backtest
        """
        try:
            logger.info("🚀 Начинаю backtesting формулы volatility_focused")
            
            # Загружаем данные
            df = self.load_historical_data(start_date, end_date)
            if df is None or df.empty:
                logger.error("❌ Не удалось загрузить данные для backtesting")
                return None
            
            # Запускаем backtest с фиксированным порогом
            logger.info("📊 Запуск backtest с фиксированным порогом")
            fixed_results = self.generate_signals(df, use_adaptive_threshold=False)
            
            # Запускаем backtest с адаптивным порогом
            logger.info("📊 Запуск backtest с адаптивным порогом")
            adaptive_results = self.generate_signals(df, use_adaptive_threshold=True)
            
            # Сохраняем результаты
            fixed_path = os.path.join(self.results_dir, 'volatility_focused_fixed.csv')
            adaptive_path = os.path.join(self.results_dir, 'volatility_focused_adaptive.csv')
            
            fixed_results.to_csv(fixed_path, index=False)
            adaptive_results.to_csv(adaptive_path, index=False)
            
            logger.info(f"💾 Результаты сохранены: {fixed_path}, {adaptive_path}")
            
            # Рассчитываем метрики
            fixed_metrics = self.calculate_metrics(fixed_results)
            adaptive_metrics = self.calculate_metrics(adaptive_results)
            
            # Создаем графики
            fixed_fig = self.create_equity_curve_plot(fixed_results, "Фиксированный порог")
            adaptive_fig = self.create_equity_curve_plot(adaptive_results, "Адаптивный порог")
            
            # Сохраняем графики
            if fixed_fig:
                fixed_fig.write_html(os.path.join(self.plots_dir, 'equity_curve_fixed.html'))
            if adaptive_fig:
                adaptive_fig.write_html(os.path.join(self.plots_dir, 'equity_curve_adaptive.html'))
            
            # Создаем отчет
            self.create_backtest_report(fixed_metrics, adaptive_metrics, fixed_results, adaptive_results)
            
            return {
                'fixed_metrics': fixed_metrics,
                'adaptive_metrics': adaptive_metrics,
                'fixed_results': fixed_results,
                'adaptive_results': adaptive_results
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка backtesting: {e}")
            return None
    
    def create_backtest_report(self, fixed_metrics, adaptive_metrics, fixed_results, adaptive_results):
        """
        Создает отчет по результатам backtesting
        """
        try:
            report_path = os.path.join(self.results_dir, 'backtest_report.md')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 📊 Отчет по Backtesting: volatility_focused\n\n")
                f.write(f"**Дата:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("## 📈 Ключевые метрики\n\n")
                f.write("| Параметр | Фиксированный порог | Адаптивный порог |\n")
                f.write("|----------|---------------------|------------------|\n")
                f.write(f"| Общая accuracy | {fixed_metrics.get('accuracy', 0):.1%} | **{adaptive_metrics.get('accuracy', 0):.1%}** |\n")
                f.write(f"| Win Rate | {fixed_metrics.get('win_rate', 0):.1%} | **{adaptive_metrics.get('win_rate', 0):.1%}** |\n")
                f.write(f"| Profit Factor | {fixed_metrics.get('profit_factor', 0):.2f} | **{adaptive_metrics.get('profit_factor', 0):.2f}** |\n")
                f.write(f"| Max Drawdown | {fixed_metrics.get('max_drawdown', 0)} | **{adaptive_metrics.get('max_drawdown', 0)}** |\n")
                f.write(f"| Всего сигналов | {fixed_metrics.get('total_signals', 0)} | {adaptive_metrics.get('total_signals', 0)} |\n\n")
                
                f.write("## 📊 Анализ по рыночным условиям\n\n")
                
                # Анализ по волатильности
                if not fixed_results.empty:
                    high_vol_fixed = fixed_results[fixed_results['atr'] > fixed_results['atr'].quantile(0.75)]
                    low_vol_fixed = fixed_results[fixed_results['atr'] < fixed_results['atr'].quantile(0.25)]
                    
                    high_vol_metrics = self.calculate_metrics(high_vol_fixed)
                    low_vol_metrics = self.calculate_metrics(low_vol_fixed)
                    
                    f.write("### Высокая волатильность (75% квантиль)\n")
                    f.write(f"- Фиксированный порог: {high_vol_metrics.get('accuracy', 0):.1%} accuracy\n")
                    f.write(f"- Адаптивный порог: {self.calculate_metrics(adaptive_results[adaptive_results['atr'] > adaptive_results['atr'].quantile(0.75)]).get('accuracy', 0):.1%} accuracy\n\n")
                    
                    f.write("### Низкая волатильность (25% квантиль)\n")
                    f.write(f"- Фиксированный порог: {low_vol_metrics.get('accuracy', 0):.1%} accuracy\n")
                    f.write(f"- Адаптивный порог: {self.calculate_metrics(adaptive_results[adaptive_results['atr'] < adaptive_results['atr'].quantile(0.25)]).get('accuracy', 0):.1%} accuracy\n\n")
                
                f.write("## 🎯 Рекомендации\n\n")
                f.write("1. **Использовать адаптивный порог** как стандарт для volatility_focused\n")
                f.write("2. **Добавить фильтр волатильности**: отключать сигналы при ATR < 0.5%\n")
                f.write("3. **Рассмотреть машинное обучение** для динамического подбора коэффициентов\n\n")
                
                f.write("## 📁 Файлы результатов\n\n")
                f.write("- `volatility_focused_fixed.csv` - результаты с фиксированным порогом\n")
                f.write("- `volatility_focused_adaptive.csv` - результаты с адаптивным порогом\n")
                f.write("- `equity_curve_fixed.html` - график накопленной прибыли (фиксированный)\n")
                f.write("- `equity_curve_adaptive.html` - график накопленной прибыли (адаптивный)\n")
            
            logger.info(f"📄 Отчет создан: {report_path}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания отчета: {e}")

def main():
    """Основная функция"""
    engine = BacktestEngine()
    
    # Запускаем backtest
    results = engine.run_backtest()
    
    if results:
        logger.info("✅ Backtesting завершен успешно!")
        logger.info(f"📊 Фиксированный порог: {results['fixed_metrics'].get('accuracy', 0):.1%} accuracy")
        logger.info(f"📊 Адаптивный порог: {results['adaptive_metrics'].get('accuracy', 0):.1%} accuracy")
    else:
        logger.error("❌ Backtesting завершился с ошибкой")

if __name__ == "__main__":
    main()
