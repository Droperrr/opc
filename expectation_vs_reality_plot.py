#!/usr/bin/env python3
"""
Создание графика Expectation vs Reality
Визуализация прогнозов и фактических значений
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional

# Импорт модулей
from compatibility import safe_float, safe_mean, safe_std, safe_array, safe_sqrt
from prediction_layer import PredictionLayer
from error_monitor import ErrorMonitor

# Настройка логирования
logger = logging.getLogger(__name__)

def create_sample_data(days: int = 30, points_per_day: int = 24) -> pd.DataFrame:
    """
    Создание тестовых данных для демонстрации
    
    Args:
        days: Количество дней
        points_per_day: Количество точек в день
        
    Returns:
        pd.DataFrame: Тестовые данные
    """
    logger.info(f"🔮 Создание тестовых данных: {days} дней, {points_per_day} точек/день")
    
    # Параметры симуляции
    np.random.seed(42)
    base_price = 100.0
    trend = 0.001  # Небольшой восходящий тренд
    volatility = 0.02
    noise_level = 0.01
    
    # Создание временных меток
    start_time = datetime.now() - timedelta(days=days)
    timestamps = []
    for day in range(days):
        for hour in range(0, 24, 24 // points_per_day):
            timestamp = start_time + timedelta(days=day, hours=hour)
            timestamps.append(timestamp)
    
    # Создание цен
    prices = []
    for i, timestamp in enumerate(timestamps):
        # Базовый тренд
        trend_component = base_price + trend * i
        
        # Сезонность (дневные циклы)
        hour = timestamp.hour
        seasonal_component = 0.5 * np.sin(2 * np.pi * hour / 24)
        
        # Случайная составляющая
        random_component = np.random.normal(0, volatility * base_price)
        
        # Итоговая цена
        price = trend_component + seasonal_component + random_component
        prices.append(max(price, 1.0))  # Минимальная цена 1.0
    
    # Создание DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'price': prices,
        'volatility': [volatility] * len(prices)
    })
    
    logger.info(f"✅ Создано {len(df)} точек данных")
    return df

def generate_predictions(df: pd.DataFrame, method: str = 'simple_moving_average') -> pd.DataFrame:
    """
    Генерация прогнозов для данных
    
    Args:
        df: DataFrame с историческими данными
        method: Метод прогнозирования
        
    Returns:
        pd.DataFrame: Данные с прогнозами
    """
    logger.info(f"🔮 Генерация прогнозов методом {method}")
    
    predictor = PredictionLayer(window_size=10)
    
    predictions = []
    confidences = []
    
    for i in range(10, len(df)):  # Начинаем с 10-й точки для окна
        historical_prices = df['price'].iloc[:i].values
        
        # Прогноз
        prediction = predictor.predict_next_price(historical_prices, method)
        predictions.append(prediction)
        
        # Уверенность
        confidence = predictor.calculate_prediction_confidence(historical_prices, method)
        confidences.append(confidence)
    
    # Добавляем прогнозы к данным
    df_with_predictions = df.copy()
    df_with_predictions['prediction'] = np.nan
    df_with_predictions['confidence'] = np.nan
    
    # Заполняем прогнозы
    for i, (pred, conf) in enumerate(zip(predictions, confidences)):
        df_with_predictions.iloc[10 + i, df_with_predictions.columns.get_loc('prediction')] = pred
        df_with_predictions.iloc[10 + i, df_with_predictions.columns.get_loc('confidence')] = conf
    
    logger.info(f"✅ Создано {len(predictions)} прогнозов")
    return df_with_predictions

def plot_expectation_vs_reality(df: pd.DataFrame, title: str = "Expectation vs Reality") -> go.Figure:
    """
    Создание графика Expectation vs Reality
    
    Args:
        df: DataFrame с данными и прогнозами
        title: Заголовок графика
        
    Returns:
        go.Figure: График Plotly
    """
    logger.info("📊 Создание графика Expectation vs Reality")
    
    # Фильтруем данные с прогнозами
    df_with_predictions = df.dropna(subset=['prediction'])
    
    if len(df_with_predictions) == 0:
        logger.warning("Нет данных с прогнозами для построения графика")
        return go.Figure()
    
    # Создание подграфиков
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=[
            "Цены и прогнозы",
            "Ошибки прогнозирования",
            "Уверенность в прогнозах"
        ],
        vertical_spacing=0.08,
        row_heights=[0.5, 0.3, 0.2]
    )
    
    # График 1: Цены и прогнозы
    fig.add_trace(
        go.Scatter(
            x=df_with_predictions['timestamp'],
            y=df_with_predictions['price'],
            mode='lines',
            name='Фактическая цена',
            line=dict(color='blue', width=2),
            hovertemplate='<b>Фактическая цена</b><br>' +
                         'Время: %{x}<br>' +
                         'Цена: %{y:.2f}<br>' +
                         '<extra></extra>'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df_with_predictions['timestamp'],
            y=df_with_predictions['prediction'],
            mode='lines',
            name='Прогноз',
            line=dict(color='red', width=2, dash='dash'),
            hovertemplate='<b>Прогноз</b><br>' +
                         'Время: %{x}<br>' +
                         'Прогноз: %{y:.2f}<br>' +
                         '<extra></extra>'
        ),
        row=1, col=1
    )
    
    # График 2: Ошибки прогнозирования
    errors = np.abs(df_with_predictions['price'] - df_with_predictions['prediction'])
    relative_errors = errors / df_with_predictions['price'] * 100
    
    fig.add_trace(
        go.Scatter(
            x=df_with_predictions['timestamp'],
            y=relative_errors,
            mode='lines+markers',
            name='Относительная ошибка (%)',
            line=dict(color='orange', width=1),
            marker=dict(size=4),
            hovertemplate='<b>Ошибка прогноза</b><br>' +
                         'Время: %{x}<br>' +
                         'Ошибка: %{y:.2f}%<br>' +
                         '<extra></extra>'
        ),
        row=2, col=1
    )
    
    # График 3: Уверенность в прогнозах
    fig.add_trace(
        go.Scatter(
            x=df_with_predictions['timestamp'],
            y=df_with_predictions['confidence'],
            mode='lines+markers',
            name='Уверенность',
            line=dict(color='green', width=1),
            marker=dict(size=4),
            hovertemplate='<b>Уверенность в прогнозе</b><br>' +
                         'Время: %{x}<br>' +
                         'Уверенность: %{y:.3f}<br>' +
                         '<extra></extra>'
        ),
        row=3, col=1
    )
    
    # Настройка макета
    fig.update_layout(
        title={
            'text': title,
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20}
        },
        height=800,
        showlegend=True,
        hovermode='x unified',
        template='plotly_white'
    )
    
    # Настройка осей
    fig.update_xaxes(title_text="Время", row=3, col=1)
    fig.update_yaxes(title_text="Цена", row=1, col=1)
    fig.update_yaxes(title_text="Ошибка (%)", row=2, col=1)
    fig.update_yaxes(title_text="Уверенность", row=3, col=1)
    
    logger.info("✅ График Expectation vs Reality создан")
    return fig

def create_error_analysis_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создание таблицы анализа ошибок
    
    Args:
        df: DataFrame с данными и прогнозами
        
    Returns:
        pd.DataFrame: Таблица анализа ошибок
    """
    logger.info("📊 Создание таблицы анализа ошибок")
    
    # Фильтруем данные с прогнозами
    df_with_predictions = df.dropna(subset=['prediction'])
    
    if len(df_with_predictions) == 0:
        return pd.DataFrame()
    
    # Вычисляем метрики ошибок
    errors = np.abs(df_with_predictions['price'] - df_with_predictions['prediction'])
    relative_errors = errors / df_with_predictions['price'] * 100
    
    # Создаем таблицу
    analysis_table = pd.DataFrame({
        'timestamp': df_with_predictions['timestamp'],
        'actual_price': df_with_predictions['price'],
        'predicted_price': df_with_predictions['prediction'],
        'absolute_error': errors,
        'relative_error_percent': relative_errors,
        'confidence': df_with_predictions['confidence']
    })
    
    # Добавляем статистику
    stats = {
        'metric': ['MAE', 'RMSE', 'MAPE', 'Max Error', 'Min Error', 'Avg Confidence'],
        'value': [
            safe_mean(errors),
            safe_sqrt(safe_mean(errors ** 2)),
            safe_mean(relative_errors),
            float(np.max(errors)),
            float(np.min(errors)),
            safe_mean(df_with_predictions['confidence'])
        ]
    }
    
    stats_df = pd.DataFrame(stats)
    
    logger.info("✅ Таблица анализа ошибок создана")
    return analysis_table, stats_df

def save_plot_to_html(fig: go.Figure, filename: str = "expectation_vs_reality.html"):
    """
    Сохранение графика в HTML файл
    
    Args:
        fig: График Plotly
        filename: Имя файла
    """
    try:
        fig.write_html(filename)
        logger.info(f"✅ График сохранен в {filename}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения графика: {e}")

def demo_expectation_vs_reality():
    """Демонстрация создания графика Expectation vs Reality"""
    logger.info("🎯 Демонстрация Expectation vs Reality")
    
    try:
        # Создание тестовых данных
        df = create_sample_data(days=7, points_per_day=12)  # 7 дней, 12 точек в день
        
        # Генерация прогнозов
        df_with_predictions = generate_predictions(df, 'simple_moving_average')
        
        # Создание графика
        fig = plot_expectation_vs_reality(df_with_predictions, "SOL/USDT: Expectation vs Reality")
        
        # Сохранение графика
        save_plot_to_html(fig, "expectation_vs_reality_demo.html")
        
        # Создание таблицы анализа
        analysis_table, stats_df = create_error_analysis_table(df_with_predictions)
        
        # Сохранение таблиц
        analysis_table.to_csv("error_analysis_table.csv", index=False)
        stats_df.to_csv("error_statistics.csv", index=False)
        
        logger.info("🎉 Демонстрация завершена успешно!")
        
        # Вывод статистики
        print("\n📊 Статистика ошибок:")
        for _, row in stats_df.iterrows():
            print(f"   {row['metric']}: {row['value']:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка демонстрации: {e}")
        return False

if __name__ == "__main__":
    # Настройка логирования для демонстрации
    logging.basicConfig(level=logging.INFO)
    
    print("📊 Создание графика Expectation vs Reality...")
    
    success = demo_expectation_vs_reality()
    
    if success:
        print("✅ График Expectation vs Reality создан успешно!")
        print("📁 Файлы:")
        print("   - expectation_vs_reality_demo.html")
        print("   - error_analysis_table.csv")
        print("   - error_statistics.csv")
    else:
        print("❌ Ошибка создания графика")
