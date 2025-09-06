#!/usr/bin/env python3
"""
Block Reporting для системы Error-Driven Adaptive Blocks
Создает отчеты и визуализации по блокам
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
from block_detector import BlockDetector
from block_analyzer import BlockAnalyzer
from formula_engine_blocks import FormulaEngineBlocks

# Настройка логирования
logger = logging.getLogger(__name__)

class BlockReporter:
    """Создание отчетов и визуализаций по блокам"""
    
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        """
        Инициализация репортера блоков
        
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
        self.block_detector = BlockDetector(db_path)
        self.block_analyzer = BlockAnalyzer(db_path)
        self.formula_engine = FormulaEngineBlocks(db_path)
        logger.info(f"📊 BlockReporter инициализирован с БД: {db_path}")
    
    def create_block_analysis_plot(self, block_id: int, title: str = None) -> go.Figure:
        """
        Создает график анализа блока с метриками эффективности
        
        Args:
            block_id: ID блока
            title: Заголовок графика
            
        Returns:
            go.Figure: График Plotly
        """
        try:
            logger.info(f"📊 Создание графика анализа блока {block_id}")
            
            # Получаем анализ блока
            analysis = self.block_analyzer.analyze_block(block_id)
            
            if analysis.block_id == 0:
                logger.warning(f"Блок {block_id} не найден")
                return go.Figure()
            
            # Получаем статистику блока
            block_stats = self.block_analyzer.get_block_statistics(block_id)
            
            if not block_stats:
                logger.warning(f"Нет статистики для блока {block_id}")
                return go.Figure()
            
            # Получаем ошибки блока
            errors_df = self._get_block_errors(block_id)
            
            if len(errors_df) == 0:
                logger.warning(f"Нет ошибок для блока {block_id}")
                return go.Figure()
            
            # Создаем подграфики
            fig = make_subplots(
                rows=4, cols=1,
                subplot_titles=[
                    f"Ошибки прогнозирования (Блок {block_id})",
                    "Распределение ошибок",
                    "Метрики производительности",
                    "Рекомендации"
                ],
                vertical_spacing=0.08,
                row_heights=[0.4, 0.2, 0.2, 0.2]
            )
            
            # График 1: Ошибки во времени
            fig.add_trace(
                go.Scatter(
                    x=errors_df['timestamp'],
                    y=errors_df['error_absolute'],
                    mode='lines+markers',
                    name='Абсолютная ошибка',
                    line=dict(color='blue', width=2),
                    marker=dict(size=4),
                    hovertemplate='<b>Ошибка прогноза</b><br>' +
                                 'Время: %{x}<br>' +
                                 'Ошибка: %{y:.4f}<br>' +
                                 '<extra></extra>'
                ),
                row=1, col=1
            )
            
            # Добавляем среднюю ошибку
            mean_error = block_stats['mean_error']
            fig.add_hline(
                y=mean_error,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Средняя ошибка: {mean_error:.4f}",
                row=1, col=1
            )
            
            # График 2: Распределение ошибок
            errors = safe_array(errors_df['error_absolute'])
            fig.add_trace(
                go.Histogram(
                    x=errors,
                    nbinsx=20,
                    name='Распределение ошибок',
                    marker_color='lightblue',
                    opacity=0.7
                ),
                row=2, col=1
            )
            
            # График 3: Метрики производительности
            performance_metrics = analysis.performance_metrics
            metrics_names = list(performance_metrics.keys())
            metrics_values = list(performance_metrics.values())
            
            fig.add_trace(
                go.Bar(
                    x=metrics_names,
                    y=metrics_values,
                    name='Метрики производительности',
                    marker_color='green',
                    opacity=0.7
                ),
                row=3, col=1
            )
            
            # График 4: Рекомендации (текстовый)
            recommendations_text = '<br>'.join([f"• {rec}" for rec in analysis.recommendations])
            
            fig.add_annotation(
                text=f"<b>Рекомендации:</b><br>{recommendations_text}",
                xref="x4", yref="y4",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=12),
                row=4, col=1
            )
            
            # Настройка макета
            fig.update_layout(
                title={
                    'text': title or f"Анализ блока {block_id} - {analysis.market_regime}",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                height=1000,
                showlegend=True,
                hovermode='x unified',
                template='plotly_white'
            )
            
            # Настройка осей
            fig.update_xaxes(title_text="Время", row=1, col=1)
            fig.update_yaxes(title_text="Ошибка", row=1, col=1)
            fig.update_xaxes(title_text="Значение ошибки", row=2, col=1)
            fig.update_yaxes(title_text="Частота", row=2, col=1)
            fig.update_xaxes(title_text="Метрика", row=3, col=1)
            fig.update_yaxes(title_text="Значение", row=3, col=1)
            
            logger.info(f"✅ График анализа блока {block_id} создан")
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графика анализа блока {block_id}: {e}")
            return go.Figure()
    
    def create_blocks_summary_plot(self, days: int = 30) -> go.Figure:
        """
        Создает график сводки по блокам
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            go.Figure: График Plotly
        """
        try:
            logger.info(f"📊 Создание графика сводки по блокам за {days} дней")
            
            # Получаем сводку по блокам
            summary = self.block_analyzer.get_blocks_summary(days)
            
            if 'total_blocks' not in summary:
                logger.warning("Нет данных для создания сводки")
                return go.Figure()
            
            # Получаем данные блоков
            blocks_df = self.block_analyzer.get_blocks()
            
            if len(blocks_df) == 0:
                logger.warning("Нет блоков для анализа")
                return go.Figure()
            
            # Создаем подграфики
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=[
                    "Блоки по времени",
                    "Распределение по типам",
                    "Распределение по режимам",
                    "Производительность блоков"
                ]
            )
            
            # График 1: Блоки по времени
            fig.add_trace(
                go.Scatter(
                    x=blocks_df['start_time'],
                    y=blocks_df['mean_error'],
                    mode='markers',
                    name='Средняя ошибка',
                    marker=dict(
                        size=blocks_df['prediction_count'] / 10,
                        color=blocks_df['confidence'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title="Уверенность")
                    ),
                    hovertemplate='<b>Блок %{text}</b><br>' +
                                 'Время: %{x}<br>' +
                                 'Ошибка: %{y:.4f}<br>' +
                                 'Уверенность: %{marker.color:.3f}<br>' +
                                 '<extra></extra>',
                    text=blocks_df['id']
                ),
                row=1, col=1
            )
            
            # График 2: Распределение по типам
            type_counts = blocks_df['block_type'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=type_counts.index,
                    values=type_counts.values,
                    name="Типы блоков"
                ),
                row=1, col=2
            )
            
            # График 3: Распределение по режимам
            regime_counts = {}
            for _, block in blocks_df.iterrows():
                regime = self.block_analyzer.classify_market_regime(block['id'])
                regime_counts[regime] = regime_counts.get(regime, 0) + 1
            
            if regime_counts:
                fig.add_trace(
                    go.Bar(
                        x=list(regime_counts.keys()),
                        y=list(regime_counts.values()),
                        name="Рыночные режимы",
                        marker_color='lightcoral'
                    ),
                    row=2, col=1
                )
            
            # График 4: Производительность блоков
            fig.add_trace(
                go.Scatter(
                    x=blocks_df['mean_error'],
                    y=blocks_df['confidence'],
                    mode='markers',
                    name='Производительность',
                    marker=dict(
                        size=blocks_df['prediction_count'] / 20,
                        color=blocks_df['std_error'],
                        colorscale='RdYlGn',
                        showscale=True,
                        colorbar=dict(title="Стандартное отклонение")
                    ),
                    hovertemplate='<b>Блок %{text}</b><br>' +
                                 'Средняя ошибка: %{x:.4f}<br>' +
                                 'Уверенность: %{y:.3f}<br>' +
                                 'Стд. отклонение: %{marker.color:.4f}<br>' +
                                 '<extra></extra>',
                    text=blocks_df['id']
                ),
                row=2, col=2
            )
            
            # Настройка макета
            fig.update_layout(
                title={
                    'text': f"Сводка по блокам за {days} дней",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                height=800,
                showlegend=True,
                template='plotly_white'
            )
            
            logger.info(f"✅ График сводки по блокам создан")
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графика сводки: {e}")
            return go.Figure()
    
    def create_formula_performance_plot(self, formula_id: str) -> go.Figure:
        """
        Создает график производительности формулы по блокам
        
        Args:
            formula_id: ID формулы
            
        Returns:
            go.Figure: График Plotly
        """
        try:
            logger.info(f"📊 Создание графика производительности формулы {formula_id}")
            
            # Получаем производительность по режимам
            performance_data = self.formula_engine.get_formula_performance_by_regime(formula_id)
            
            if not performance_data:
                logger.warning(f"Нет данных о производительности для формулы {formula_id}")
                return go.Figure()
            
            # Подготавливаем данные
            regimes = list(performance_data.keys())
            avg_scores = [performance_data[regime]['average_score'] for regime in regimes]
            max_scores = [performance_data[regime]['max_score'] for regime in regimes]
            block_counts = [performance_data[regime]['block_count'] for regime in regimes]
            
            # Создаем график
            fig = go.Figure()
            
            # Средние оценки
            fig.add_trace(
                go.Bar(
                    x=regimes,
                    y=avg_scores,
                    name='Средняя оценка',
                    marker_color='lightblue',
                    opacity=0.7
                )
            )
            
            # Максимальные оценки
            fig.add_trace(
                go.Bar(
                    x=regimes,
                    y=max_scores,
                    name='Максимальная оценка',
                    marker_color='darkblue',
                    opacity=0.7
                )
            )
            
            # Добавляем количество блоков как текст
            for i, count in enumerate(block_counts):
                fig.add_annotation(
                    x=regimes[i],
                    y=max(avg_scores[i], max_scores[i]) + 0.05,
                    text=f"Блоков: {count}",
                    showarrow=False,
                    font=dict(size=10)
                )
            
            # Настройка макета
            fig.update_layout(
                title={
                    'text': f"Производительность формулы {formula_id} по рыночным режимам",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 16}
                },
                xaxis_title="Рыночный режим",
                yaxis_title="Оценка производительности",
                barmode='group',
                template='plotly_white',
                height=500
            )
            
            logger.info(f"✅ График производительности формулы {formula_id} создан")
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графика производительности: {e}")
            return go.Figure()
    
    def _get_block_errors(self, block_id: int) -> pd.DataFrame:
        """Получает ошибки для указанного блока"""
        try:
            import sqlite3
            
            with sqlite3.connect(self.db_path) as conn:
                # Получаем временные границы блока
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT start_time, end_time FROM blocks WHERE id = ?
                ''', (block_id,))
                
                result = cursor.fetchone()
                if not result:
                    return pd.DataFrame()
                
                start_time, end_time = result
                
                # Получаем ошибки
                query = '''
                    SELECT * FROM error_history 
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                '''
                
                df = pd.read_sql_query(query, conn, params=[start_time, end_time])
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения ошибок блока {block_id}: {e}")
            return pd.DataFrame()
    
    def generate_comprehensive_report(self, days: int = 30) -> Dict[str, Any]:
        """
        Генерирует комплексный отчет по блокам
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            Dict[str, Any]: Комплексный отчет
        """
        try:
            logger.info(f"📊 Генерация комплексного отчета за {days} дней")
            
            # Получаем сводку по блокам
            blocks_summary = self.block_analyzer.get_blocks_summary(days)
            
            # Получаем данные блоков
            blocks_df = self.block_analyzer.get_blocks()
            
            # Анализируем каждый блок
            block_analyses = []
            for _, block in blocks_df.iterrows():
                analysis = self.block_analyzer.analyze_block(block['id'])
                block_analyses.append({
                    'block_id': block['id'],
                    'block_type': block['block_type'],
                    'market_regime': analysis.market_regime,
                    'confidence': analysis.confidence,
                    'risk_level': analysis.risk_level,
                    'performance_score': analysis.performance_metrics.get('overall_score', 0.0),
                    'recommendations_count': len(analysis.recommendations)
                })
            
            # Анализируем производительность формул
            formula_performance = {}
            for formula_id in ['volatility_focused', 'basis_dominant', 'balanced']:
                performance_data = self.formula_engine.get_formula_performance_by_regime(formula_id)
                formula_performance[formula_id] = performance_data
            
            # Создаем комплексный отчет
            comprehensive_report = {
                'report_period_days': days,
                'generation_date': datetime.now().isoformat(),
                'blocks_summary': blocks_summary,
                'block_analyses': block_analyses,
                'formula_performance': formula_performance,
                'key_insights': self._generate_key_insights(blocks_summary, block_analyses),
                'recommendations': self._generate_report_recommendations(block_analyses, formula_performance)
            }
            
            logger.info(f"✅ Комплексный отчет сгенерирован")
            return comprehensive_report
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации комплексного отчета: {e}")
            return {'error': str(e)}
    
    def _generate_key_insights(self, blocks_summary: Dict[str, Any], 
                             block_analyses: List[Dict[str, Any]]) -> List[str]:
        """Генерирует ключевые инсайты из анализа"""
        try:
            insights = []
            
            # Анализ общего количества блоков
            total_blocks = blocks_summary.get('total_blocks', 0)
            if total_blocks > 0:
                insights.append(f"Обнаружено {total_blocks} рыночных режимов за анализируемый период")
            
            # Анализ наиболее частого режима
            most_common_regime = blocks_summary.get('most_common_regime', 'unknown')
            if most_common_regime != 'unknown':
                insights.append(f"Наиболее частый рыночный режим: {most_common_regime}")
            
            # Анализ производительности блоков
            if block_analyses:
                avg_performance = safe_mean([block['performance_score'] for block in block_analyses])
                insights.append(f"Средняя производительность блоков: {avg_performance:.3f}")
                
                high_performance_blocks = [b for b in block_analyses if b['performance_score'] > 0.7]
                if high_performance_blocks:
                    insights.append(f"Высокопроизводительных блоков: {len(high_performance_blocks)}")
            
            # Анализ рисков
            high_risk_blocks = [b for b in block_analyses if b['risk_level'] == 'high']
            if high_risk_blocks:
                insights.append(f"Блоков с высоким риском: {len(high_risk_blocks)}")
            
            return insights
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации инсайтов: {e}")
            return ["Ошибка генерации инсайтов"]
    
    def _generate_report_recommendations(self, block_analyses: List[Dict[str, Any]], 
                                      formula_performance: Dict[str, Any]) -> List[str]:
        """Генерирует рекомендации на основе анализа"""
        try:
            recommendations = []
            
            # Рекомендации на основе производительности блоков
            if block_analyses:
                low_performance_blocks = [b for b in block_analyses if b['performance_score'] < 0.5]
                if low_performance_blocks:
                    recommendations.append("Рассмотрите оптимизацию параметров для блоков с низкой производительностью")
                
                high_risk_blocks = [b for b in block_analyses if b['risk_level'] == 'high']
                if high_risk_blocks:
                    recommendations.append("Увеличьте осторожность при работе с блоками высокого риска")
            
            # Рекомендации на основе производительности формул
            for formula_id, performance_data in formula_performance.items():
                if performance_data:
                    avg_scores = [data['average_score'] for data in performance_data.values()]
                    if avg_scores:
                        overall_score = safe_mean(avg_scores)
                        if overall_score < 0.6:
                            recommendations.append(f"Формула {formula_id} показывает низкую производительность - рассмотрите пересмотр параметров")
            
            # Общие рекомендации
            recommendations.append("Регулярно обновляйте параметры формул на основе новых данных")
            recommendations.append("Мониторьте изменения рыночных режимов для своевременной адаптации")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации рекомендаций: {e}")
            return ["Ошибка генерации рекомендаций"]

def test_block_reporting():
    """Тестирование Block Reporting"""
    logger.info("🧪 Тестирование Block Reporting")
    
    # Создаем тестовую базу данных
    import uuid
    test_db = f'test_block_reporting_{uuid.uuid4().hex[:8]}.db'
    
    try:
        # Инициализация
        reporter = BlockReporter(test_db)
        
        # Создаем тестовые данные
        import sqlite3
        
        with sqlite3.connect(test_db) as conn:
            cursor = conn.cursor()
            
            # Создаем необходимые таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME NOT NULL,
                    start_index INTEGER NOT NULL,
                    end_index INTEGER NOT NULL,
                    block_type TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    mean_error REAL NOT NULL,
                    std_error REAL NOT NULL,
                    max_error REAL NOT NULL,
                    min_error REAL NOT NULL,
                    error_trend REAL NOT NULL,
                    volatility REAL NOT NULL,
                    prediction_count INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    formula_id TEXT,
                    prediction REAL NOT NULL,
                    actual REAL NOT NULL,
                    error_absolute REAL NOT NULL,
                    error_relative REAL NOT NULL,
                    error_normalized REAL NOT NULL,
                    volatility REAL,
                    confidence REAL,
                    method TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Добавляем тестовые блоки
            for i in range(3):
                cursor.execute('''
                    INSERT INTO blocks 
                    (start_time, end_time, start_index, end_index, block_type, confidence,
                     mean_error, std_error, max_error, min_error, error_trend, 
                     volatility, prediction_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now() - timedelta(hours=3-i),
                    datetime.now() - timedelta(hours=2-i),
                    i*100, (i+1)*100, f'block_type_{i}', 0.7 + i*0.1,
                    0.3 + i*0.2, 0.1 + i*0.1, 0.5 + i*0.2, 0.1, 0.02 + i*0.01, 0.01, 100
                ))
                
                # Добавляем тестовые ошибки
                for j in range(50):
                    timestamp = datetime.now() - timedelta(hours=3-i, minutes=j)
                    cursor.execute('''
                        INSERT INTO error_history 
                        (timestamp, formula_id, prediction, actual, error_absolute, 
                         error_relative, error_normalized, volatility, confidence, method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, 'volatility_focused', 100.0 + j*0.01, 100.1 + j*0.01, 0.1 + j*0.001,
                        0.001, 0.1, 0.01, 0.8, 'sma'
                    ))
            
            conn.commit()
        
        # Тестируем создание графиков
        analysis_plot = reporter.create_block_analysis_plot(1)
        
        if len(analysis_plot.data) > 0:
            logger.info("✅ График анализа блока создан")
            
            # Тестируем сводку
            summary_plot = reporter.create_blocks_summary_plot(1)
            
            if len(summary_plot.data) > 0:
                logger.info("✅ График сводки создан")
                
                # Тестируем комплексный отчет
                comprehensive_report = reporter.generate_comprehensive_report(1)
                
                if 'blocks_summary' in comprehensive_report:
                    logger.info("✅ Комплексный отчет создан")
                    return True
                else:
                    logger.error("❌ Комплексный отчет не создан")
                    return False
            else:
                logger.error("❌ График сводки не создан")
                return False
        else:
            logger.error("❌ График анализа блока не создан")
            return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования Block Reporting: {e}")
        return False
        
    finally:
        # Очистка
        import os
        try:
            if os.path.exists(test_db):
                os.remove(test_db)
        except PermissionError:
            pass

if __name__ == "__main__":
    # Настройка логирования для тестирования
    logging.basicConfig(level=logging.INFO)
    
    print("📊 Тестирование Block Reporting...")
    
    success = test_block_reporting()
    
    if success:
        print("✅ Block Reporting готов к использованию")
    else:
        print("❌ Ошибки в Block Reporting")
