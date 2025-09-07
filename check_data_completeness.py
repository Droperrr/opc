#!/usr/bin/env python3
"""
Скрипт для проверки полноты собранных данных
Анализирует пропуски и генерирует отчеты
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
import argparse
from typing import Dict, List, Tuple
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCompletenessChecker:
    def __init__(self, db_path: str = 'data/sol_iv.db'):
        self.db_path = db_path
        self.timeframe_minutes = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60,
            '4h': 240,
            '1d': 1440
        }
    
    def check_data_completeness(self, timeframe: str, market_type: str) -> Dict:
        """
        Проверяет полноту данных для указанного таймфрейма и типа рынка
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Определение таблицы в зависимости от типа рынка
            table = 'spot_data' if market_type == 'spot' else 'futures_data'
            
            # Получение данных
            query = f"""
            SELECT time FROM {table} 
            WHERE timeframe = ? 
            ORDER BY time
            """
            df = pd.read_sql_query(query, conn, params=[timeframe])
            
            if df.empty:
                logger.warning(f"⚠️ Нет данных для {market_type} {timeframe}")
                return {
                    'timeframe': timeframe,
                    'market_type': market_type,
                    'start': None,
                    'end': None,
                    'expected_count': 0,
                    'actual_count': 0,
                    'completeness': 0,
                    'gaps': 0,
                    'gap_details': []
                }
            
            # Конвертация времени
            df['time'] = pd.to_datetime(df['time'])
            start = df['time'].min()
            end = df['time'].max()
            
            # Расчет ожидаемого количества
            total_minutes = (end - start).total_seconds() / 60
            expected_count = total_minutes / self.timeframe_minutes[timeframe]
            
            # Проверка пропусков
            df['diff'] = df['time'].diff().dt.total_seconds() / 60
            gaps = df[df['diff'] > self.timeframe_minutes[timeframe] * 1.1]
            
            completeness = (len(df) / expected_count) * 100 if expected_count > 0 else 0
            
            gap_details = []
            if not gaps.empty:
                for _, row in gaps.iterrows():
                    gap_details.append({
                        'time': row['time'].strftime('%Y-%m-%d %H:%M:%S'),
                        'gap_minutes': int(row['diff'])
                    })
            
            conn.close()
            
            return {
                'timeframe': timeframe,
                'market_type': market_type,
                'start': start.strftime('%Y-%m-%d %H:%M:%S'),
                'end': end.strftime('%Y-%m-%d %H:%M:%S'),
                'expected_count': int(expected_count),
                'actual_count': len(df),
                'completeness': round(completeness, 2),
                'gaps': len(gaps),
                'gap_details': gap_details
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки {market_type} {timeframe}: {e}")
            return None
    
    def generate_completeness_report(self, market_types: List[str] = ['spot', 'linear']) -> Dict:
        """
        Генерирует полный отчет о полноте данных
        """
        logger.info("🔍 Начинаю анализ полноты данных")
        
        all_results = {}
        total_records = 0
        
        for market_type in market_types:
            market_results = {}
            
            for timeframe in self.timeframe_minutes.keys():
                result = self.check_data_completeness(timeframe, market_type)
                if result:
                    market_results[timeframe] = result
                    total_records += result['actual_count']
                    logger.info(f"✅ {market_type} {timeframe}: {result['completeness']}% ({result['actual_count']} записей)")
            
            all_results[market_type] = market_results
        
        return {
            'results': all_results,
            'total_records': total_records,
            'generated_at': datetime.now().isoformat()
        }
    
    def create_completeness_visualization(self, results: Dict, output_dir: str = 'report_s06'):
        """
        Создает визуализации для анализа полноты данных
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Подготовка данных для графика
            data_for_plot = []
            
            for market_type, market_results in results['results'].items():
                for timeframe, result in market_results.items():
                    data_for_plot.append({
                        'Market': market_type,
                        'Timeframe': timeframe,
                        'Completeness': result['completeness'],
                        'Records': result['actual_count']
                    })
            
            df_plot = pd.DataFrame(data_for_plot)
            
            # График полноты данных
            plt.figure(figsize=(12, 8))
            
            # Создаем subplot
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # График полноты
            pivot_completeness = df_plot.pivot(index='Timeframe', columns='Market', values='Completeness')
            pivot_completeness.plot(kind='bar', ax=ax1, color=['#1f77b4', '#ff7f0e'])
            ax1.set_title('Полнота данных по таймфреймам', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Полнота (%)')
            ax1.set_xlabel('Таймфрейм')
            ax1.legend(['Spot', 'Futures'])
            ax1.grid(True, alpha=0.3)
            
            # График количества записей
            pivot_records = df_plot.pivot(index='Timeframe', columns='Market', values='Records')
            pivot_records.plot(kind='bar', ax=ax2, color=['#2ca02c', '#d62728'])
            ax2.set_title('Количество записей по таймфреймам', fontsize=14, fontweight='bold')
            ax2.set_ylabel('Количество записей')
            ax2.set_xlabel('Таймфрейм')
            ax2.legend(['Spot', 'Futures'])
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'data_completeness_analysis.png'), dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"📊 График сохранен: {output_dir}/data_completeness_analysis.png")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания визуализации: {e}")
    
    def export_sample_data(self, output_dir: str = 'report_s06/sample_data'):
        """
        Экспортирует образцы данных для каждого таймфрейма
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            conn = sqlite3.connect(self.db_path)
            
            for market_type in ['spot', 'linear']:
                table = 'spot_data' if market_type == 'linear' else 'futures_data'
                
                for timeframe in self.timeframe_minutes.keys():
                    # Получаем первые 100 записей для 1m, 30 для остальных
                    limit = 100 if timeframe == '1m' else 30
                    
                    query = f"""
                    SELECT * FROM {table} 
                    WHERE timeframe = ? 
                    ORDER BY time 
                    LIMIT {limit}
                    """
                    
                    df = pd.read_sql_query(query, conn, params=[timeframe])
                    
                    if not df.empty:
                        filename = f"{market_type}_data_{timeframe.replace('m', 'm').replace('h', 'h').replace('d', 'd')}_sample.csv"
                        filepath = os.path.join(output_dir, filename)
                        df.to_csv(filepath, index=False)
                        logger.info(f"💾 Образец сохранен: {filepath} ({len(df)} записей)")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта образцов: {e}")
    
    def generate_markdown_report(self, results: Dict, output_file: str = 'report_s06/data_summary.md'):
        """
        Генерирует отчет в формате Markdown
        """
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# Отчёт о сборе исторических данных за 2025 год\n\n")
                
                # Общая статистика
                f.write("## Общая статистика\n")
                f.write("| Показатель | Значение |\n")
                f.write("|------------|----------|\n")
                f.write(f"| Общий период | {self._get_period_range(results)} |\n")
                f.write(f"| Таймфреймы | {', '.join(self.timeframe_minutes.keys())} |\n")
                f.write(f"| Всего записей | {results['total_records']:,} |\n")
                f.write(f"| Средняя полнота | {self._calculate_average_completeness(results):.2f}% |\n\n")
                
                # Детали по таймфреймам
                f.write("## Детали по таймфреймам\n")
                f.write("| Таймфрейм | Рынок | Период | Ожидаемо | Получено | Полнота | Пропуски |\n")
                f.write("|-----------|-------|--------|----------|----------|---------|----------|\n")
                
                for market_type, market_results in results['results'].items():
                    for timeframe, result in market_results.items():
                        f.write(f"| {timeframe} | {market_type} | {result['start']} - {result['end']} | ")
                        f.write(f"{result['expected_count']:,} | {result['actual_count']:,} | ")
                        f.write(f"{result['completeness']}% | {result['gaps']} |\n")
                
                f.write("\n")
                
                # Обнаруженные пропуски
                f.write("## Обнаруженные пропуски\n")
                gaps_found = False
                
                for market_type, market_results in results['results'].items():
                    for timeframe, result in market_results.items():
                        if result['gaps'] > 0:
                            gaps_found = True
                            f.write(f"\n### {market_type} {timeframe}\n")
                            f.write(f"- Количество пропусков: {result['gaps']}\n")
                            f.write("- Детали пропусков:\n")
                            for gap in result['gap_details'][:5]:  # Показываем только первые 5
                                f.write(f"  - {gap['time']} (пропуск {gap['gap_minutes']} минут)\n")
                            if len(result['gap_details']) > 5:
                                f.write(f"  - ... и еще {len(result['gap_details']) - 5} пропусков\n")
                
                if not gaps_found:
                    f.write("Пропусков не обнаружено.\n")
                
                f.write("\n")
                
                # Рекомендации
                f.write("## Рекомендации\n")
                f.write("- Для backtesting рекомендуется использовать 5m как оптимальный баланс между детализацией и полнотой\n")
                f.write("- Для высокочастотной торговли использовать 1m данные с учетом возможных пропусков\n")
                f.write("- Для долгосрочного анализа использовать 1h и 4h данные\n")
                f.write("- Регулярно проверять полноту данных и дозагружать пропуски\n")
                
                f.write(f"\n\n*Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
            
            logger.info(f"📄 Отчет сохранен: {output_file}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации отчета: {e}")
    
    def _get_period_range(self, results: Dict) -> str:
        """Получает общий диапазон периодов"""
        all_starts = []
        all_ends = []
        
        for market_type, market_results in results['results'].items():
            for timeframe, result in market_results.items():
                if result['start']:
                    all_starts.append(result['start'])
                    all_ends.append(result['end'])
        
        if all_starts and all_ends:
            min_start = min(all_starts)
            max_end = max(all_ends)
            return f"{min_start.split()[0]} - {max_end.split()[0]}"
        
        return "Не определен"
    
    def _calculate_average_completeness(self, results: Dict) -> float:
        """Вычисляет среднюю полноту данных"""
        total_completeness = 0
        count = 0
        
        for market_type, market_results in results['results'].items():
            for timeframe, result in market_results.items():
                total_completeness += result['completeness']
                count += 1
        
        return total_completeness / count if count > 0 else 0

def main():
    parser = argparse.ArgumentParser(description='Проверка полноты собранных данных')
    parser.add_argument('--db', default='data/sol_iv.db', help='Путь к базе данных')
    parser.add_argument('--output', default='report_s06', help='Папка для отчетов')
    parser.add_argument('--markets', nargs='+', default=['spot', 'linear'], 
                       help='Типы рынков для проверки')
    
    args = parser.parse_args()
    
    # Создаем чекер
    checker = DataCompletenessChecker(args.db)
    
    # Генерируем отчет
    results = checker.generate_completeness_report(args.markets)
    
    # Создаем визуализации
    checker.create_completeness_visualization(results, args.output)
    
    # Экспортируем образцы данных
    checker.export_sample_data(os.path.join(args.output, 'sample_data'))
    
    # Генерируем Markdown отчет
    checker.generate_markdown_report(results, os.path.join(args.output, 'data_summary.md'))
    
    logger.info("🎉 Анализ полноты данных завершен")

if __name__ == "__main__":
    main()
