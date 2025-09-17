#!/usr/bin/env python3
"""
Визуальный исследовательский дашборд для MVP-стратегии
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from logger import get_logger

logger = get_logger()

class MVPCashboard:
    def __init__(self):
        """Инициализация дашборда"""
        self.data_file = 'dashboard_data.parquet'
        self.data = None
        
    @st.cache_data(ttl=3600)  # Кэшируем данные на 1 час
    def _load_data_cached(_self, _file_path):
        """Загружает данные для дашборда с кэшированием"""
        try:
            logger.info("Загрузка данных дашборда...")
            data = pd.read_parquet(_file_path)
            logger.info(f"Загружено {len(data)} записей")
            
            # Преобразуем timestamp в datetime
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            
            # Сортируем по времени
            data = data.sort_values('timestamp').reset_index(drop=True)
            
            return data
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            st.error(f"❌ Ошибка загрузки данных: {e}")
            return None
    
    def load_data(self, force_reload=False):
        """Загружает данные для дашборда"""
        try:
            # Если force_reload=True, очищаем кэш
            if force_reload:
                st.cache_data.clear()
            
            # Загружаем данные через кэшированную функцию
            self.data = self._load_data_cached(self.data_file)
            
            return self.data is not None
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            st.error(f"❌ Ошибка загрузки данных: {e}")
            return False
    
    def create_price_chart(self, start_time, end_time, sma_period=10):
        """Создает график цены спота и SMA"""
        try:
            # Фильтруем данные по временному диапазону
            filtered_data = self.data[
                (self.data['timestamp'] >= start_time) &
                (self.data['timestamp'] <= end_time)
            ].copy()
            
            if filtered_data.empty:
                st.warning("Нет данных для отображения в выбранном временном диапазоне")
                return None
            
            # Рассчитываем SMA с заданным периодом
            filtered_data[f'SMA_{sma_period}'] = filtered_data['spot_price'].rolling(window=sma_period).mean()
            
            # Создаем график
            fig = go.Figure()
            
            # Линия цены спота
            fig.add_trace(go.Scatter(
                x=filtered_data['timestamp'],
                y=filtered_data['spot_price'],
                mode='lines',
                name='Spot Price',
                line=dict(color='blue', width=2)
            ))
            
            # Линия SMA
            fig.add_trace(go.Scatter(
                x=filtered_data['timestamp'],
                y=filtered_data[f'SMA_{sma_period}'],
                mode='lines',
                name=f'SMA {sma_period}',
                line=dict(color='orange', width=1, dash='dash')
            ))
            
            # Добавляем маркеры сделок
            # Прибыльные LONG-сделки (зеленый треугольник вверх)
            profitable_long = filtered_data[
                (filtered_data['signal'] == 'LONG') & 
                (filtered_data['pnl'] > 0)
            ]
            if not profitable_long.empty:
                fig.add_trace(go.Scatter(
                    x=profitable_long['timestamp'],
                    y=profitable_long['spot_price'],
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=10, color='green'),
                    name='Profitable LONG'
                ))
            
            # Убыточные LONG-сделки (красный треугольник вверх)
            loss_long = filtered_data[
                (filtered_data['signal'] == 'LONG') & 
                (filtered_data['pnl'] <= 0)
            ]
            if not loss_long.empty:
                fig.add_trace(go.Scatter(
                    x=loss_long['timestamp'],
                    y=loss_long['spot_price'],
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=10, color='red'),
                    name='Loss LONG'
                ))
            
            # Прибыльные SHORT-сделки (зеленый треугольник вниз)
            profitable_short = filtered_data[
                (filtered_data['signal'] == 'SHORT') & 
                (filtered_data['pnl'] > 0)
            ]
            if not profitable_short.empty:
                fig.add_trace(go.Scatter(
                    x=profitable_short['timestamp'],
                    y=profitable_short['spot_price'],
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=10, color='green'),
                    name='Profitable SHORT'
                ))
            
            # Убыточные SHORT-сделки (красный треугольник вниз)
            loss_short = filtered_data[
                (filtered_data['signal'] == 'SHORT') & 
                (filtered_data['pnl'] <= 0)
            ]
            if not loss_short.empty:
                fig.add_trace(go.Scatter(
                    x=loss_short['timestamp'],
                    y=loss_short['spot_price'],
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=10, color='red'),
                    name='Loss SHORT'
                ))
            
            fig.update_layout(
                title='Цена спота и SMA',
                xaxis_title='Время',
                yaxis_title='Цена',
                hovermode='x unified',
                height=500
            )
            
            # Включаем панорамирование и масштабирование
            fig.update_xaxes(fixedrange=False)
            fig.update_yaxes(fixedrange=False)
            
            return fig
            
        except Exception as e:
            logger.error(f"Ошибка создания графика цены: {e}")
            st.error(f"❌ Ошибка создания графика цены: {e}")
            return None
    
    def create_zscore_chart(self, start_time, end_time):
        """Создает график z-score"""
        try:
            # Фильтруем данные по временному диапазону
            filtered_data = self.data[
                (self.data['timestamp'] >= start_time) & 
                (self.data['timestamp'] <= end_time)
            ].copy()
            
            if filtered_data.empty:
                return None
            
            # Создаем график
            fig = go.Figure()
            
            # Линия z-score
            fig.add_trace(go.Scatter(
                x=filtered_data['timestamp'],
                y=filtered_data['combined_z_score'],
                mode='lines',
                name='Combined Z-Score',
                line=dict(color='purple', width=2)
            ))
            
            # Горизонтальные линии порогов
            fig.add_hline(y=2.0, line_dash="dash", line_color="red", 
                         annotation_text="Порог +2.0")
            fig.add_hline(y=-2.0, line_dash="dash", line_color="green",
                         annotation_text="Порог -2.0")
            fig.add_hline(y=0, line_dash="dot", line_color="gray")
            
            fig.update_layout(
                title='Combined Z-Score',
                xaxis_title='Время',
                yaxis_title='Z-Score',
                hovermode='x unified',
                height=300
            )
            
            # Включаем панорамирование и масштабирование
            fig.update_xaxes(fixedrange=False)
            fig.update_yaxes(fixedrange=False)
            
            return fig
            
        except Exception as e:
            logger.error(f"Ошибка создания графика z-score: {e}")
            st.error(f"❌ Ошибка создания графика z-score: {e}")
            return None
    
    def run_dashboard(self):
        """Запускает дашборд"""
        st.set_page_config(
            page_title="MVP Strategy Dashboard",
            page_icon="📊",
            layout="wide"
        )
        
        st.title("📊 Визуальный исследовательский дашборд для MVP-стратегии")
        
        # Отображаем run_id, если он есть в данных
        if 'run_id' in self.data.columns and not self.data['run_id'].isna().all():
            run_id = self.data['run_id'].iloc[0]
            st.markdown(f"**Run ID:** `{run_id}`")
        
        st.markdown("Анализ поведения стратегии на исторических данных")
        
        # Кнопка для обновления данных
        if st.button("🔄 Обновить данные"):
            st.info("Обновление данных...")
            # Перезагружаем данные с очисткой кэша
            if self.load_data(force_reload=True):
                st.success("Данные успешно обновлены!")
            else:
                st.error("Ошибка обновления данных")
        
        # Загружаем данные
        if not self.load_data():
            return
        
        # Определяем временной диапазон данных
        min_time = self.data['timestamp'].min()
        max_time = self.data['timestamp'].max()
        
        # Виджет слайдер для выбора временного диапазона
        st.subheader("🕒 Выбор временного диапазона")
        start_time, end_time = st.slider(
            "Выберите период:",
            min_value=min_time.to_pydatetime(),
            max_value=max_time.to_pydatetime(),
            value=(min_time.to_pydatetime(), max_time.to_pydatetime()),
            format="YYYY-MM-DD HH:mm"
        )
        
        # Виджет для выбора периода SMA
        st.subheader("📊 Параметры индикаторов")
        sma_period = st.slider("Период SMA", min_value=5, max_value=50, value=20, step=1)
        
        # Создаем графики
        st.subheader("📈 Анализ стратегии")
        
        # Главный график цены и SMA
        price_fig = self.create_price_chart(start_time, end_time, sma_period)
        if price_fig:
            st.plotly_chart(price_fig, use_container_width=True)
        
        # График z-score
        zscore_fig = self.create_zscore_chart(start_time, end_time)
        if zscore_fig:
            st.plotly_chart(zscore_fig, use_container_width=True)
        
        # Статистика по сделкам
        st.subheader("📊 Статистика по сделкам")
        
        # Фильтруем данные по выбранному диапазону
        filtered_data = self.data[
            (self.data['timestamp'] >= start_time) & 
            (self.data['timestamp'] <= end_time)
        ].copy()
        
        if not filtered_data.empty:
            # Общая статистика
            total_trades = len(filtered_data)
            profitable_trades = len(filtered_data[filtered_data['pnl'] > 0])
            loss_trades = len(filtered_data[filtered_data['pnl'] <= 0])
            win_rate = profitable_trades / total_trades if total_trades > 0 else 0
            total_pnl = filtered_data['pnl'].sum()
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Всего сделок", total_trades)
            
            with col2:
                st.metric("Прибыльных", profitable_trades)
            
            with col3:
                st.metric("Убыточных", loss_trades)
            
            with col4:
                st.metric("Win Rate", f"{win_rate:.1%}")
            
            with col5:
                st.metric("Общий P&L", f"{total_pnl:.2f} USD")
            
            # Детализация по типам сигналов
            st.subheader("📋 Детализация по типам сигналов")
            
            # LONG сигналы
            long_signals = filtered_data[filtered_data['signal'] == 'LONG']
            if not long_signals.empty:
                long_profitable = len(long_signals[long_signals['pnl'] > 0])
                long_loss = len(long_signals[long_signals['pnl'] <= 0])
                long_win_rate = long_profitable / len(long_signals) if len(long_signals) > 0 else 0
                long_pnl = long_signals['pnl'].sum()
                
                st.write("**LONG сигналы:**")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Всего", len(long_signals))
                
                with col2:
                    st.metric("Прибыльных", long_profitable)
                
                with col3:
                    st.metric("Win Rate", f"{long_win_rate:.1%}")
                
                with col4:
                    st.metric("P&L", f"{long_pnl:.2f} USD")
            
            # SHORT сигналы
            short_signals = filtered_data[filtered_data['signal'] == 'SHORT']
            if not short_signals.empty:
                short_profitable = len(short_signals[short_signals['pnl'] > 0])
                short_loss = len(short_signals[short_signals['pnl'] <= 0])
                short_win_rate = short_profitable / len(short_signals) if len(short_signals) > 0 else 0
                short_pnl = short_signals['pnl'].sum()
                
                st.write("**SHORT сигналы:**")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Всего", len(short_signals))
                
                with col2:
                    st.metric("Прибыльных", short_profitable)
                
                with col3:
                    st.metric("Win Rate", f"{short_win_rate:.1%}")
                
                with col4:
                    st.metric("P&L", f"{short_pnl:.2f} USD")
            
            # Таблица сделок
            st.subheader("📋 Таблица сделок")
            display_df = filtered_data[[
                'timestamp', 'signal', 'entry_price', 'exit_price', 'pnl', 'pnl_percent'
            ]].copy()
            display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
            display_df['entry_price'] = display_df['entry_price'].round(2)
            display_df['exit_price'] = display_df['exit_price'].round(2)
            display_df['pnl'] = display_df['pnl'].round(2)
            display_df['pnl_percent'] = display_df['pnl_percent'].round(4)
            
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("ℹ️ Нет сделок в выбранном временном диапазоне")

def main():
    """Основная функция запуска дашборда"""
    try:
        dashboard = MVPCashboard()
        dashboard.run_dashboard()
    except Exception as e:
        logger.error(f"❌ Ошибка запуска дашборда: {e}")
        st.error(f"❌ Ошибка запуска дашборда: {e}")

if __name__ == "__main__":
    main()