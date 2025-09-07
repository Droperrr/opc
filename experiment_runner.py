#!/usr/bin/env python3
"""
Система запуска экспериментов S-03
Прогоняет формулы через все сегменты данных и создает визуализацию
"""

import pandas as pd
import numpy as np
import json
import os
import sqlite3
from datetime import datetime, timedelta
from logger import get_logger
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

logger = get_logger()

class ExperimentRunner:
    def __init__(self):
        self.data_dir = 'data/experiment_sets'
        self.experiments_dir = 'data/experiments'
        self.plots_dir = 'data/experiments/plots'
        self.db_path = 'data/sol_iv.db'
        
        # Создаем директории
        os.makedirs(self.experiments_dir, exist_ok=True)
        os.makedirs(self.plots_dir, exist_ok=True)
        
        # Параметры экспериментов
        self.threshold = 0.7
        self.lookforward_minutes = 15
        
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
        """Загружает сегменты данных"""
        segments = []
        
        try:
            # Получаем список CSV файлов
            csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
            
            for csv_file in csv_files:
                segment_name = csv_file.replace('.csv', '')
                csv_path = os.path.join(self.data_dir, csv_file)
                metadata_path = os.path.join(self.data_dir, f"{segment_name}_metadata.json")
                
                # Загружаем данные
                df = pd.read_csv(csv_path)
                df['time'] = pd.to_datetime(df['time'])
                
                # Загружаем метаданные
                metadata = {}
                if os.path.exists(metadata_path):
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                
                segments.append({
                    'name': segment_name,
                    'data': df,
                    'metadata': metadata
                })
                
            logger.info(f"📊 Загружено {len(segments)} сегментов")
            return segments
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сегментов: {e}")
            return []
    
    def calculate_formula_value(self, row, formula_name, parameters):
        """Вычисляет значение Y для формулы"""
        try:
            # Извлекаем параметры
            params = json.loads(parameters) if isinstance(parameters, str) else parameters
            
            # Базовые компоненты
            iv = row.get('iv', 0)
            skew = row.get('skew', 0)
            basis_rel = row.get('basis_rel', 0)
            oi_ratio = row.get('oi_ratio', 0.5)
            
            # Вычисляем oi_ratio если нет
            if 'oi_ratio' not in row:
                oi_call = row.get('oi_call', 1000)
                oi_put = row.get('oi_put', 1000)
                total_oi = oi_call + oi_put
                oi_ratio = oi_call / total_oi if total_oi > 0 else 0.5
            
            # Применяем формулу в зависимости от типа
            if formula_name == 'IV_skew_weighted':
                Y = (params.get('iv_weight', 0.4) * iv + 
                     params.get('skew_weight', 0.35) * skew + 
                     params.get('basis_weight', 0.15) * basis_rel + 
                     params.get('oi_weight', 0.1) * oi_ratio)
            
            elif formula_name == 'spread_dominant':
                Y = (params.get('iv_weight', 0.5) * iv + 
                     params.get('skew_weight', 0.3) * skew + 
                     params.get('basis_weight', -0.2) * basis_rel)
            
            elif formula_name == 'volatility_focused':
                Y = (params.get('iv_weight', 0.6) * iv + 
                     params.get('oi_weight', 0.25) * oi_ratio + 
                     params.get('skew_weight', 0.15) * skew)
            
            elif formula_name == 'balanced_approach':
                Y = (params.get('iv_weight', 0.3) * iv + 
                     params.get('skew_weight', 0.3) * skew + 
                     params.get('basis_weight', 0.2) * basis_rel + 
                     params.get('oi_weight', 0.2) * oi_ratio)
            
            elif formula_name == 'momentum_based':
                # Для momentum нужны предыдущие значения
                Y = (params.get('iv_momentum_weight', 0.4) * iv + 
                     params.get('skew_momentum_weight', 0.4) * skew + 
                     params.get('oi_weight', 0.2) * oi_ratio)
            
            else:
                # Дефолтная формула
                Y = 0.4 * iv + 0.3 * skew + 0.2 * basis_rel + 0.1 * oi_ratio
            
            return Y
            
        except Exception as e:
            logger.error(f"❌ Ошибка вычисления формулы {formula_name}: {e}")
            return 0
    
    def generate_signals(self, df, formula_name, parameters):
        """Генерирует сигналы на основе формулы"""
        signals = []
        
        for i, row in df.iterrows():
            # Вычисляем Y
            Y = self.calculate_formula_value(row, formula_name, parameters)
            
            # Определяем сигнал
            if Y > self.threshold:
                signal = 'BUY'
            elif Y < -self.threshold:
                signal = 'SELL'
            else:
                signal = 'NEUTRAL'
            
            # Проверяем результат через 15 минут
            result = self.check_result(df, i, signal)
            
            signals.append({
                'time': row['time'],
                'spot_price': row['spot_price'],
                'Y_value': Y,
                'signal': signal,
                'result': result,
                'segment_name': df.get('segment_name', 'unknown').iloc[0] if 'segment_name' in df.columns else 'unknown'
            })
        
        return pd.DataFrame(signals)
    
    def check_result(self, df, current_idx, signal):
        """Проверяет результат сигнала через 15 минут"""
        try:
            if signal == 'NEUTRAL':
                return 'neutral'
            
            # Ищем цену через 15 минут
            current_time = df.iloc[current_idx]['time']
            future_time = current_time + timedelta(minutes=self.lookforward_minutes)
            
            # Ищем ближайшую запись после future_time
            future_data = df[df['time'] > future_time]
            
            if future_data.empty:
                return 'unknown'
            
            future_price = future_data.iloc[0]['spot_price']
            current_price = df.iloc[current_idx]['spot_price']
            
            # Определяем результат
            if signal == 'BUY':
                if future_price > current_price * 1.003:  # +0.3%
                    return 'profit'
                else:
                    return 'loss'
            elif signal == 'SELL':
                if future_price < current_price * 0.997:  # -0.3%
                    return 'profit'
                else:
                    return 'loss'
            
            return 'unknown'
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки результата: {e}")
            return 'unknown'
    
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
    
    def run_experiment(self, formula, segment):
        """Запускает эксперимент для одной формулы на одном сегменте"""
        try:
            formula_name = formula['name']
            segment_name = segment['name']
            
            logger.info(f"🔬 Эксперимент: {formula_name} на {segment_name}")
            
            # Добавляем имя сегмента к данным
            df = segment['data'].copy()
            df['segment_name'] = segment_name
            
            # Генерируем сигналы
            signals_df = self.generate_signals(df, formula_name, formula['parameters'])
            
            # Сохраняем результаты
            experiment_dir = os.path.join(self.experiments_dir, formula_name)
            os.makedirs(experiment_dir, exist_ok=True)
            
            csv_path = os.path.join(experiment_dir, f"{segment_name}.csv")
            signals_df.to_csv(csv_path, index=False)
            
            # Создаем график
            fig = self.create_experiment_plot(signals_df, formula_name, self.threshold)
            if fig:
                try:
                    plot_path = os.path.join(self.plots_dir, f"{formula_name}_{segment_name}.png")
                    # Используем HTML как fallback если PNG не работает
                    fig.write_image(plot_path, engine="kaleido")
                    logger.info(f"📊 График сохранен: {plot_path}")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось сохранить PNG, сохраняем HTML: {e}")
                    html_path = os.path.join(self.plots_dir, f"{formula_name}_{segment_name}.html")
                    fig.write_html(html_path)
                    logger.info(f"📊 HTML график сохранен: {html_path}")
            
            # Рассчитываем метрики
            metrics = self.calculate_metrics(signals_df)
            
            return {
                'formula': formula_name,
                'segment': segment_name,
                'metrics': metrics,
                'signals_count': len(signals_df),
                'csv_path': csv_path
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка эксперимента {formula_name} на {segment_name}: {e}")
            return None
    
    def calculate_metrics(self, signals_df):
        """Рассчитывает метрики производительности"""
        try:
            # Фильтруем только BUY и SELL сигналы
            trading_signals = signals_df[signals_df['signal'].isin(['BUY', 'SELL'])]
            
            if len(trading_signals) == 0:
                return {
                    'accuracy': 0,
                    'win_rate': 0,
                    'profit_factor': 0,
                    'total_signals': 0,
                    'profitable_signals': 0
                }
            
            # Базовые метрики
            total_signals = len(trading_signals)
            profitable_signals = len(trading_signals[trading_signals['result'] == 'profit'])
            loss_signals = len(trading_signals[trading_signals['result'] == 'loss'])
            
            # Винрейт
            win_rate = profitable_signals / total_signals if total_signals > 0 else 0
            
            # Profit Factor (упрощенный)
            profit_factor = profitable_signals / loss_signals if loss_signals > 0 else float('inf')
            
            # Accuracy (общая точность)
            accuracy = win_rate
            
            return {
                'accuracy': round(accuracy, 3),
                'win_rate': round(win_rate, 3),
                'profit_factor': round(profit_factor, 3),
                'total_signals': total_signals,
                'profitable_signals': profitable_signals
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета метрик: {e}")
            return {}
    
    def run_all_experiments(self):
        """Запускает все эксперименты"""
        try:
            logger.info("🚀 Запуск всех экспериментов...")
            
            # Загружаем формулы и сегменты
            formulas = self.load_formulas()
            segments = self.load_segments()
            
            if formulas.empty or not segments:
                logger.error("❌ Нет данных для экспериментов")
                return
            
            results = []
            
            # Запускаем эксперименты
            for _, formula in formulas.iterrows():
                for segment in segments:
                    result = self.run_experiment(formula, segment)
                    if result:
                        results.append(result)
            
            # Сохраняем сводный отчет
            self.create_summary_report(results, formulas, segments)
            
            logger.info(f"✅ Завершено {len(results)} экспериментов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска экспериментов: {e}")
    
    def create_summary_report(self, results, formulas, segments):
        """Создает сводный отчет по всем экспериментам"""
        try:
            # Группируем результаты по формулам
            formula_results = {}
            for result in results:
                formula_name = result['formula']
                if formula_name not in formula_results:
                    formula_results[formula_name] = []
                formula_results[formula_name].append(result)
            
            # Рассчитываем средние метрики
            summary_data = []
            for formula_name, formula_results_list in formula_results.items():
                avg_accuracy = np.mean([r['metrics']['accuracy'] for r in formula_results_list])
                avg_win_rate = np.mean([r['metrics']['win_rate'] for r in formula_results_list])
                avg_profit_factor = np.mean([r['metrics']['profit_factor'] for r in formula_results_list])
                total_signals = sum([r['metrics']['total_signals'] for r in formula_results_list])
                
                summary_data.append({
                    'formula': formula_name,
                    'accuracy': round(avg_accuracy, 3),
                    'win_rate': round(avg_win_rate, 3),
                    'profit_factor': round(avg_profit_factor, 3),
                    'total_signals': total_signals
                })
            
            # Сортируем по accuracy
            summary_data.sort(key=lambda x: x['accuracy'], reverse=True)
            
            # Сохраняем отчет
            report_path = os.path.join(self.experiments_dir, 'experiments_summary.md')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 📊 Сводный отчет по экспериментам S-03\n\n")
                f.write(f"**Дата создания:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"**Всего экспериментов:** {len(results)}\n")
                f.write(f"**Формул:** {len(formulas)}\n")
                f.write(f"**Сегментов:** {len(segments)}\n\n")
                
                f.write("## 📈 Ключевые метрики\n\n")
                f.write("| Формула | Accuracy | Win Rate | Profit Factor | Всего сигналов |\n")
                f.write("|---------|----------|----------|---------------|----------------|\n")
                
                for data in summary_data:
                    f.write(f"| {data['formula']} | {data['accuracy']:.1%} | {data['win_rate']:.1%} | {data['profit_factor']:.2f} | {data['total_signals']} |\n")
                
                f.write(f"\n## 🏆 Лучшая формула: {summary_data[0]['formula']}\n")
                f.write(f"- **Accuracy:** {summary_data[0]['accuracy']:.1%}\n")
                f.write(f"- **Win Rate:** {summary_data[0]['win_rate']:.1%}\n")
                f.write(f"- **Profit Factor:** {summary_data[0]['profit_factor']:.2f}\n")
                f.write(f"- **Всего сигналов:** {summary_data[0]['total_signals']}\n\n")
                
                f.write("## 📋 Детализация по сегментам\n\n")
                for data in summary_data[:3]:  # Топ-3 формулы
                    f.write(f"### {data['formula']}\n")
                    formula_results_list = [r for r in results if r['formula'] == data['formula']]
                    
                    for result in formula_results_list:
                        f.write(f"- **{result['segment']}:** Accuracy {result['metrics']['accuracy']:.1%}, "
                               f"Win Rate {result['metrics']['win_rate']:.1%}, "
                               f"Signals {result['metrics']['total_signals']}\n")
                    f.write("\n")
            
            logger.info(f"📄 Сводный отчет сохранен: {report_path}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания отчета: {e}")

if __name__ == "__main__":
    runner = ExperimentRunner()
    runner.run_all_experiments()
