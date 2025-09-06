#!/usr/bin/env python3
"""
UI Дашборд для визуализации экспериментов S-03
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
import os
from datetime import datetime
import sqlite3
from logger import get_logger

logger = get_logger()

class ExperimentDashboard:
    def __init__(self):
        self.experiments_dir = 'data/experiments'
        self.plots_dir = 'data/experiments/plots'
        self.data_dir = 'data/experiment_sets'
        self.db_path = 'data/sol_iv.db'
        
    def load_formulas(self):
        """Загружает формулы из БД"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('''
                SELECT id, name, description, formula_text, parameters 
                FROM formulas 
                WHERE is_active = 1
                ORDER BY name
            ''', conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки формул: {e}")
            return pd.DataFrame()
    
    def load_segments(self):
        """Загружает список сегментов"""
        try:
            csv_files = [f.replace('.csv', '') for f in os.listdir(self.data_dir) if f.endswith('.csv')]
            return sorted(csv_files)
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сегментов: {e}")
            return []
    
    def load_experiment_data(self, formula_name, segment_name):
        """Загружает данные эксперимента"""
        try:
            csv_path = os.path.join(self.experiments_dir, formula_name, f"{segment_name}.csv")
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                df['time'] = pd.to_datetime(df['time'])
                return df
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных эксперимента: {e}")
            return None
    
    def create_experiment_plot(self, segment_df, formula_name, threshold=0.7):
        """Создает график эксперимента"""
        try:
            fig = go.Figure()
            
            # Ось Y1: цена спота
            fig.add_trace(go.Scatter(
                x=segment_df['time'], y=segment_df['spot_price'],
                name="Spot Price", line=dict(color="blue"), yaxis="y1"
            ))
            
            # Ось Y2: значение Y
            fig.add_trace(go.Scatter(
                x=segment_df['time'], y=segment_df['Y_value'],
                name="Y (formula)", line=dict(color="purple", width=2), yaxis="y2"
            ))
            
            # Пороги
            fig.add_hline(y=threshold, line_dash="dash", line_color="green", 
                         annotation_text=f"BUY threshold ({threshold})")
            fig.add_hline(y=-threshold, line_dash="dash", line_color="red",
                         annotation_text=f"SELL threshold ({-threshold})")
            
            # Маркеры с результатом
            profitable_buy = segment_df[(segment_df['signal'] == 'BUY') & (segment_df['result'] == 'profit')]
            loss_buy = segment_df[(segment_df['signal'] == 'BUY') & (segment_df['result'] == 'loss')]
            profitable_sell = segment_df[(segment_df['signal'] == 'SELL') & (segment_df['result'] == 'profit')]
            loss_sell = segment_df[(segment_df['signal'] == 'SELL') & (segment_df['result'] == 'loss')]
            neutral = segment_df[segment_df['signal'] == 'NEUTRAL']
            
            # Добавляем маркеры
            if not profitable_buy.empty:
                fig.add_trace(go.Scatter(
                    x=profitable_buy['time'], y=profitable_buy['spot_price'],
                    mode='markers', marker=dict(symbol='triangle-up', size=12, color='green'),
                    name="Profitable BUY"
                ))
            
            if not loss_buy.empty:
                fig.add_trace(go.Scatter(
                    x=loss_buy['time'], y=loss_buy['spot_price'],
                    mode='markers', marker=dict(symbol='triangle-down', size=12, color='red'),
                    name="Loss BUY"
                ))
            
            if not profitable_sell.empty:
                fig.add_trace(go.Scatter(
                    x=profitable_sell['time'], y=profitable_sell['spot_price'],
                    mode='markers', marker=dict(symbol='triangle-down', size=12, color='green'),
                    name="Profitable SELL"
                ))
            
            if not loss_sell.empty:
                fig.add_trace(go.Scatter(
                    x=loss_sell['time'], y=loss_sell['spot_price'],
                    mode='markers', marker=dict(symbol='triangle-up', size=12, color='red'),
                    name="Loss SELL"
                ))
            
            if not neutral.empty:
                fig.add_trace(go.Scatter(
                    x=neutral['time'], y=neutral['spot_price'],
                    mode='markers', marker=dict(symbol='circle', size=8, color='gray'),
                    name="NEUTRAL"
                ))
            
            fig.update_layout(
                title=f"Эксперимент: {formula_name} | Участок: {segment_df['segment_name'].iloc[0]}",
                yaxis=dict(title="Spot Price", side="left"),
                yaxis2=dict(title="Y Value", overlaying="y", side="right"),
                hovermode="x unified",
                height=600
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графика: {e}")
            return None
    
    def calculate_metrics(self, df):
        """Рассчитывает метрики для отображения"""
        try:
            if df is None or df.empty:
                return {
                    'accuracy': 0,
                    'win_rate': 0,
                    'profit_factor': 0,
                    'total_signals': 0,
                    'profitable_signals': 0,
                    'loss_signals': 0
                }
            
            # Фильтруем только BUY и SELL сигналы
            trading_signals = df[df['signal'].isin(['BUY', 'SELL'])]
            
            if len(trading_signals) == 0:
                return {
                    'accuracy': 0,
                    'win_rate': 0,
                    'profit_factor': 0,
                    'total_signals': 0,
                    'profitable_signals': 0,
                    'loss_signals': 0
                }
            
            # Базовые метрики
            total_signals = len(trading_signals)
            profitable_signals = len(trading_signals[trading_signals['result'] == 'profit'])
            loss_signals = len(trading_signals[trading_signals['result'] == 'loss'])
            
            # Винрейт
            win_rate = profitable_signals / total_signals if total_signals > 0 else 0
            
            # Profit Factor
            profit_factor = profitable_signals / loss_signals if loss_signals > 0 else float('inf')
            
            return {
                'accuracy': round(win_rate, 3),
                'win_rate': round(win_rate, 3),
                'profit_factor': round(profit_factor, 3),
                'total_signals': total_signals,
                'profitable_signals': profitable_signals,
                'loss_signals': loss_signals
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета метрик: {e}")
            return {}
    
    def load_backtest_results(self):
        """Загружает результаты backtesting"""
        try:
            backtest_dir = 'data/backtests'
            
            # Загружаем CSV файлы
            fixed_path = os.path.join(backtest_dir, 'volatility_focused_fixed.csv')
            adaptive_path = os.path.join(backtest_dir, 'volatility_focused_adaptive.csv')
            
            if not os.path.exists(fixed_path) or not os.path.exists(adaptive_path):
                return None
            
            fixed_results = pd.read_csv(fixed_path)
            adaptive_results = pd.read_csv(adaptive_path)
            
            # Конвертируем время
            fixed_results['time'] = pd.to_datetime(fixed_results['time'])
            adaptive_results['time'] = pd.to_datetime(adaptive_results['time'])
            
            # Рассчитываем метрики
            fixed_metrics = self.calculate_metrics(fixed_results)
            adaptive_metrics = self.calculate_metrics(adaptive_results)
            
            # Создаем графики equity curve
            from plotly.subplots import make_subplots
            
            # Фиксированный порог
            fixed_signals = fixed_results[fixed_results['signal'].isin(['BUY', 'SELL'])].copy()
            if not fixed_signals.empty:
                fixed_signals['profit_value'] = (fixed_signals['result'] == 'profit').astype(int) - (fixed_signals['result'] == 'loss').astype(int)
                fixed_signals['cum_profit'] = fixed_signals['profit_value'].cumsum()
                fixed_signals['peak'] = fixed_signals['cum_profit'].cummax()
                fixed_signals['drawdown'] = fixed_signals['peak'] - fixed_signals['cum_profit']
                
                fixed_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)
                fixed_fig.add_trace(
                    go.Scatter(x=fixed_signals['time'], y=fixed_signals['cum_profit'],
                              name="Накопленная прибыль", line=dict(color="green")),
                    row=1, col=1
                )
                fixed_fig.add_trace(
                    go.Scatter(x=fixed_signals['time'], y=fixed_signals['drawdown'],
                              name="Просадка", fill="tozeroy", line=dict(color="red")),
                    row=2, col=1
                )
                fixed_fig.update_layout(title="Фиксированный порог", height=400)
            else:
                fixed_fig = None
            
            # Адаптивный порог
            adaptive_signals = adaptive_results[adaptive_results['signal'].isin(['BUY', 'SELL'])].copy()
            if not adaptive_signals.empty:
                adaptive_signals['profit_value'] = (adaptive_signals['result'] == 'profit').astype(int) - (adaptive_signals['result'] == 'loss').astype(int)
                adaptive_signals['cum_profit'] = adaptive_signals['profit_value'].cumsum()
                adaptive_signals['peak'] = adaptive_signals['cum_profit'].cummax()
                adaptive_signals['drawdown'] = adaptive_signals['peak'] - adaptive_signals['cum_profit']
                
                adaptive_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)
                adaptive_fig.add_trace(
                    go.Scatter(x=adaptive_signals['time'], y=adaptive_signals['cum_profit'],
                              name="Накопленная прибыль", line=dict(color="green")),
                    row=1, col=1
                )
                adaptive_fig.add_trace(
                    go.Scatter(x=adaptive_signals['time'], y=adaptive_signals['drawdown'],
                              name="Просадка", fill="tozeroy", line=dict(color="red")),
                    row=2, col=1
                )
                adaptive_fig.update_layout(title="Адаптивный порог", height=400)
            else:
                adaptive_fig = None
            
            return {
                'fixed_metrics': fixed_metrics,
                'adaptive_metrics': adaptive_metrics,
                'fixed_results': fixed_results,
                'adaptive_results': adaptive_results,
                'fixed_fig': fixed_fig,
                'adaptive_fig': adaptive_fig
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки результатов backtesting: {e}")
            return None
    
    def run_dashboard(self):
        """Запускает дашборд"""
        st.set_page_config(
            page_title="Эксперименты S-03",
            page_icon="📊",
            layout="wide"
        )
        
        st.title("📊 Дашборд экспериментов S-03")
        st.markdown("Визуализация экспериментов с формулами для выбора стратегии")
        
        # Загружаем данные
        formulas = self.load_formulas()
        segments = self.load_segments()
        
        if formulas.empty:
            st.error("❌ Не удалось загрузить формулы")
            return
        
        if not segments:
            st.error("❌ Не удалось загрузить сегменты")
            return
        
        # Создаем вкладки
        tab1, tab2, tab3, tab4 = st.tabs(["🔬 Эксперименты", "📈 Сводная статистика", "📋 Формулы", "📊 Backtesting"])
        
        with tab1:
            st.header("🔬 Интерактивные эксперименты")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                selected_formula = st.selectbox(
                    "Выберите формулу:",
                    formulas['name'].tolist(),
                    index=0
                )
            
            with col2:
                selected_segment = st.selectbox(
                    "Выберите сегмент:",
                    segments,
                    index=0
                )
            
            with col3:
                threshold = st.slider(
                    "Порог сигнала:",
                    min_value=0.1,
                    max_value=2.0,
                    value=0.7,
                    step=0.1
                )
            
            # Загружаем данные эксперимента
            experiment_data = self.load_experiment_data(selected_formula, selected_segment)
            
            if experiment_data is not None:
                # Показываем график
                fig = self.create_experiment_plot(experiment_data, selected_formula, threshold)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                
                # Показываем метрики
                metrics = self.calculate_metrics(experiment_data)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Accuracy", f"{metrics['accuracy']:.1%}")
                
                with col2:
                    st.metric("Win Rate", f"{metrics['win_rate']:.1%}")
                
                with col3:
                    st.metric("Profit Factor", f"{metrics['profit_factor']:.2f}")
                
                with col4:
                    st.metric("Всего сигналов", metrics['total_signals'])
                
                # Детальная таблица
                st.subheader("📋 Детализация сигналов")
                
                if not experiment_data.empty:
                    # Фильтруем только торговые сигналы
                    trading_signals = experiment_data[experiment_data['signal'].isin(['BUY', 'SELL'])]
                    
                    if not trading_signals.empty:
                        # Показываем таблицу
                        display_df = trading_signals[['time', 'spot_price', 'Y_value', 'signal', 'result']].copy()
                        display_df['time'] = display_df['time'].dt.strftime('%Y-%m-%d %H:%M')
                        display_df['spot_price'] = display_df['spot_price'].round(2)
                        display_df['Y_value'] = display_df['Y_value'].round(4)
                        
                        st.dataframe(display_df, use_container_width=True)
                    else:
                        st.info("ℹ️ Нет торговых сигналов в выбранном эксперименте")
                else:
                    st.warning("⚠️ Нет данных для отображения")
            else:
                st.error("❌ Не удалось загрузить данные эксперимента")
        
        with tab2:
            st.header("📈 Сводная статистика")
            
            # Загружаем сводный отчет
            summary_path = os.path.join(self.experiments_dir, 'experiments_summary.md')
            
            if os.path.exists(summary_path):
                with open(summary_path, 'r', encoding='utf-8') as f:
                    summary_content = f.read()
                
                st.markdown(summary_content)
            else:
                st.error("❌ Сводный отчет не найден")
        
        with tab3:
            st.header("📋 Доступные формулы")
            
            for _, formula in formulas.iterrows():
                with st.expander(f"📊 {formula['name']}"):
                    st.write(f"**Описание:** {formula['description']}")
                    st.write(f"**Формула:** {formula['formula_text']}")
                    
                    # Показываем параметры
                    try:
                        params = json.loads(formula['parameters'])
                        st.write("**Параметры:**")
                        for param, value in params.items():
                            st.write(f"  - {param}: {value}")
                    except:
                        st.write("**Параметры:** Не удалось загрузить")
        
        with tab4:
            st.header("📊 Backtesting")
            
            # Загружаем результаты backtesting
            backtest_results = self.load_backtest_results()
            
            if backtest_results:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📈 Сравнение метрик")
                    
                    # Создаем таблицу сравнения
                    comparison_data = {
                        'Метрика': ['Accuracy', 'Win Rate', 'Profit Factor', 'Max Drawdown', 'Всего сигналов'],
                        'Фиксированный порог': [
                            f"{backtest_results['fixed_metrics'].get('accuracy', 0):.1%}",
                            f"{backtest_results['fixed_metrics'].get('win_rate', 0):.1%}",
                            f"{backtest_results['fixed_metrics'].get('profit_factor', 0):.2f}",
                            f"{backtest_results['fixed_metrics'].get('max_drawdown', 0)}",
                            f"{backtest_results['fixed_metrics'].get('total_signals', 0)}"
                        ],
                        'Адаптивный порог': [
                            f"{backtest_results['adaptive_metrics'].get('accuracy', 0):.1%}",
                            f"{backtest_results['adaptive_metrics'].get('win_rate', 0):.1%}",
                            f"{backtest_results['adaptive_metrics'].get('profit_factor', 0):.2f}",
                            f"{backtest_results['adaptive_metrics'].get('max_drawdown', 0)}",
                            f"{backtest_results['adaptive_metrics'].get('total_signals', 0)}"
                        ]
                    }
                    
                    comparison_df = pd.DataFrame(comparison_data)
                    st.dataframe(comparison_df, use_container_width=True)
                
                with col2:
                    st.subheader("🎛️ Настройки")
                    
                    # Переключатель типа порога
                    use_adaptive = st.checkbox("Использовать адаптивный порог", value=True)
                    
                    # Параметры адаптивного порога
                    if use_adaptive:
                        base_threshold = st.slider("Базовый порог:", 0.3, 1.5, 0.7, 0.1)
                        volatility_factor = st.slider("Коэффициент волатильности:", 0.5, 2.0, 1.2, 0.1)
                        
                        st.info(f"ℹ️ Адаптивный порог: {base_threshold:.2f} × (1 + {volatility_factor:.1f} × volatility_ratio)")
                    else:
                        fixed_threshold = st.slider("Фиксированный порог:", 0.3, 1.5, 0.7, 0.1)
                        st.info(f"ℹ️ Фиксированный порог: {fixed_threshold:.2f}")
                
                # Графики equity curve
                st.subheader("📈 Equity Curve")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'fixed_fig' in backtest_results:
                        st.plotly_chart(backtest_results['fixed_fig'], use_container_width=True)
                        st.caption("Фиксированный порог")
                
                with col2:
                    if 'adaptive_fig' in backtest_results:
                        st.plotly_chart(backtest_results['adaptive_fig'], use_container_width=True)
                        st.caption("Адаптивный порог")
                
                # Анализ по рыночным условиям
                st.subheader("🔍 Анализ по рыночным условиям")
                
                if 'fixed_results' in backtest_results and not backtest_results['fixed_results'].empty:
                    # Анализ по волатильности
                    fixed_results = backtest_results['fixed_results']
                    adaptive_results = backtest_results['adaptive_results']
                    
                    # Разбиваем по квантилям волатильности
                    high_vol_fixed = fixed_results[fixed_results['atr'] > fixed_results['atr'].quantile(0.75)]
                    low_vol_fixed = fixed_results[fixed_results['atr'] < fixed_results['atr'].quantile(0.25)]
                    
                    high_vol_adaptive = adaptive_results[adaptive_results['atr'] > adaptive_results['atr'].quantile(0.75)]
                    low_vol_adaptive = adaptive_results[adaptive_results['atr'] < adaptive_results['atr'].quantile(0.25)]
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Высокая волатильность (75% квантиль)**")
                        high_vol_metrics = self.calculate_metrics(high_vol_fixed)
                        high_vol_adaptive_metrics = self.calculate_metrics(high_vol_adaptive)
                        
                        st.metric("Фиксированный порог", f"{high_vol_metrics.get('accuracy', 0):.1%}")
                        st.metric("Адаптивный порог", f"{high_vol_adaptive_metrics.get('accuracy', 0):.1%}")
                    
                    with col2:
                        st.write("**Низкая волатильность (25% квантиль)**")
                        low_vol_metrics = self.calculate_metrics(low_vol_fixed)
                        low_vol_adaptive_metrics = self.calculate_metrics(low_vol_adaptive)
                        
                        st.metric("Фиксированный порог", f"{low_vol_metrics.get('accuracy', 0):.1%}")
                        st.metric("Адаптивный порог", f"{low_vol_adaptive_metrics.get('accuracy', 0):.1%}")
                
                # Кнопка экспорта отчета
                if st.button("📄 Экспорт отчета (PDF)"):
                    st.info("ℹ️ Функция экспорта в разработке")
            else:
                st.warning("⚠️ Результаты backtesting не найдены. Запустите backtest_engine.py")

def main():
    """Основная функция запуска дашборда"""
    try:
        dashboard = ExperimentDashboard()
        dashboard.run_dashboard()
    except Exception as e:
        logger.error(f"❌ Ошибка запуска дашборда: {e}")
        st.error(f"❌ Ошибка запуска дашборда: {e}")

if __name__ == "__main__":
    main()
