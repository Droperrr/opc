#!/usr/bin/env python3
"""
Подготовка "золотого набора" данных для экспериментов S-03
Создает 20 исторических участков с размеченными условиями
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from logger import get_logger
import sqlite3

logger = get_logger()

class ExperimentDataPreparator:
    def __init__(self):
        self.data_dir = 'data/experiment_sets'
        self.db_path = 'data/sol_iv.db'
        
        # Создаем директорию если не существует
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Определяем типы рыночных условий
        self.market_conditions = {
            'bull_run': 6,      # Резкий рост
            'bear_drop': 6,     # Резкий спад  
            'flat_false_break': 6,  # Флэт с ложными пробоями
            'high_volatility': 8    # Высокая волатильность
        }
        
    def load_historical_data(self):
        """Загружает исторические данные из БД"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Загружаем данные IV
            iv_query = """
            SELECT time, underlyingPrice as spot_price, markIv as iv, 
                   (bid1Iv + ask1Iv)/2 as iv_mid
            FROM iv_data 
            WHERE underlyingPrice IS NOT NULL 
            ORDER BY time
            """
            iv_df = pd.read_sql_query(iv_query, conn, parse_dates=['time'])
            
            # Если данных мало, создаем синтетические данные
            if len(iv_df) < 100:
                logger.warning("⚠️ Мало исторических данных, создаю синтетические")
                iv_df = self.generate_synthetic_data()
            
            conn.close()
            return iv_df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            return self.generate_synthetic_data()
    
    def generate_synthetic_data(self):
        """Генерирует синтетические данные для экспериментов"""
        logger.info("🔄 Генерация синтетических данных...")
        
        # Создаем временной ряд на 30 дней с часовыми интервалами
        start_date = datetime(2025, 8, 1)
        end_date = datetime(2025, 9, 1)
        timestamps = pd.date_range(start_date, end_date, freq='H')
        
        data = []
        base_price = 200.0
        base_iv = 0.8
        
        for i, ts in enumerate(timestamps):
            # Базовое движение цены с трендом и шумом
            trend = np.sin(i / 24) * 0.02  # Дневной цикл
            noise = np.random.normal(0, 0.01)
            price_change = trend + noise
            
            # Обновляем цену
            base_price = base_price * (1 + price_change)
            
            # IV зависит от волатильности цены
            iv_change = abs(price_change) * 2 + np.random.normal(0, 0.05)
            current_iv = max(0.3, min(1.5, base_iv + iv_change))
            
            # Skew (разница между call и put IV)
            skew = np.random.normal(0, 0.1)
            
            # Basis (относительная разница между фьючерсом и спотом)
            basis_rel = np.random.normal(0, 0.002)
            
            # OI (Open Interest) - симулируем
            oi_call = np.random.uniform(1000, 5000)
            oi_put = np.random.uniform(1000, 5000)
            
            data.append({
                'time': ts,
                'spot_price': base_price,
                'iv': current_iv,
                'iv_mid': current_iv,
                'skew': skew,
                'basis_rel': basis_rel,
                'oi_call': oi_call,
                'oi_put': oi_put
            })
        
        return pd.DataFrame(data)
    
    def create_market_segments(self, df):
        """Создает сегменты с разными рыночными условиями"""
        segments = []
        
        # 1. Бычьи тренды (6 сегментов)
        for i in range(6):
            start_idx = i * 24  # Каждый сегмент = 24 часа
            end_idx = start_idx + 24
            
            if end_idx < len(df):
                segment_data = df.iloc[start_idx:end_idx].copy()
                
                # Создаем бычий тренд
                segment_data['spot_price'] = segment_data['spot_price'] * (1 + np.linspace(0, 0.15, len(segment_data)))
                segment_data['iv'] = segment_data['iv'] * (1 + np.linspace(0, 0.2, len(segment_data)))
                
                segments.append({
                    'name': f'bull_run_{i+1:02d}',
                    'type': 'bull_run',
                    'data': segment_data,
                    'metadata': {
                        'market_condition': 'Резкий рост',
                        'volatility': 'Средняя',
                        'trend_strength': 'Сильная',
                        'key_events': 'Позитивные новости',
                        'duration_hours': 24
                    }
                })
        
        # 2. Медвежьи тренды (6 сегментов)
        for i in range(6):
            start_idx = (i + 6) * 24
            end_idx = start_idx + 24
            
            if end_idx < len(df):
                segment_data = df.iloc[start_idx:end_idx].copy()
                
                # Создаем медвежий тренд
                segment_data['spot_price'] = segment_data['spot_price'] * (1 - np.linspace(0, 0.12, len(segment_data)))
                segment_data['iv'] = segment_data['iv'] * (1 + np.linspace(0, 0.25, len(segment_data)))
                
                segments.append({
                    'name': f'bear_drop_{i+1:02d}',
                    'type': 'bear_drop',
                    'data': segment_data,
                    'metadata': {
                        'market_condition': 'Резкий спад',
                        'volatility': 'Высокая',
                        'trend_strength': 'Сильная',
                        'key_events': 'Негативные новости',
                        'duration_hours': 24
                    }
                })
        
        # 3. Флэт с ложными пробоями (6 сегментов)
        for i in range(6):
            start_idx = (i + 12) * 24
            end_idx = start_idx + 24
            
            if end_idx < len(df):
                segment_data = df.iloc[start_idx:end_idx].copy()
                
                # Создаем флэт с ложными пробоями
                base_price = segment_data['spot_price'].iloc[0]
                segment_data['spot_price'] = base_price + np.sin(np.linspace(0, 4*np.pi, len(segment_data))) * 5
                segment_data['iv'] = segment_data['iv'] * (1 + np.random.normal(0, 0.1, len(segment_data)))
                
                segments.append({
                    'name': f'flat_false_break_{i+1:02d}',
                    'type': 'flat_false_break',
                    'data': segment_data,
                    'metadata': {
                        'market_condition': 'Флэт с ложными пробоями',
                        'volatility': 'Низкая',
                        'trend_strength': 'Слабая',
                        'key_events': 'Отсутствуют',
                        'duration_hours': 24
                    }
                })
        
        # 4. Высокая волатильность (8 сегментов)
        for i in range(8):
            start_idx = (i + 18) * 24
            end_idx = start_idx + 24
            
            if end_idx < len(df):
                segment_data = df.iloc[start_idx:end_idx].copy()
                
                # Создаем высокую волатильность
                segment_data['spot_price'] = segment_data['spot_price'] * (1 + np.random.normal(0, 0.03, len(segment_data)))
                segment_data['iv'] = segment_data['iv'] * (1 + np.random.uniform(0.3, 0.8, len(segment_data)))
                
                segments.append({
                    'name': f'high_volatility_{i+1:02d}',
                    'type': 'high_volatility',
                    'data': segment_data,
                    'metadata': {
                        'market_condition': 'Высокая волатильность',
                        'volatility': 'Очень высокая',
                        'trend_strength': 'Переменная',
                        'key_events': 'Важные новости',
                        'duration_hours': 24
                    }
                })
        
        return segments
    
    def save_segments(self, segments):
        """Сохраняет сегменты в файлы"""
        logger.info(f"💾 Сохранение {len(segments)} сегментов...")
        
        for segment in segments:
            # Сохраняем данные сегмента
            csv_path = os.path.join(self.data_dir, f"{segment['name']}.csv")
            segment['data'].to_csv(csv_path, index=False)
            
            # Сохраняем метаданные
            metadata_path = os.path.join(self.data_dir, f"{segment['name']}_metadata.json")
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(segment['metadata'], f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"✅ Сохранен сегмент: {segment['name']}")
    
    def run(self):
        """Основной метод запуска подготовки данных"""
        try:
            logger.info("🚀 Начало подготовки экспериментальных данных...")
            
            # Загружаем исторические данные
            df = self.load_historical_data()
            logger.info(f"📊 Загружено {len(df)} записей исторических данных")
            
            # Создаем сегменты
            segments = self.create_market_segments(df)
            logger.info(f"📈 Создано {len(segments)} рыночных сегментов")
            
            # Сохраняем сегменты
            self.save_segments(segments)
            
            # Создаем сводный отчет
            self.create_summary_report(segments)
            
            logger.info("✅ Подготовка экспериментальных данных завершена!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в подготовке данных: {e}")
    
    def create_summary_report(self, segments):
        """Создает сводный отчет по сегментам"""
        report_path = os.path.join(self.data_dir, 'segments_summary.md')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 📊 Сводный отчет по экспериментальным сегментам\n\n")
            f.write(f"**Дата создания:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Всего сегментов:** {len(segments)}\n\n")
            
            f.write("## 📈 Статистика по типам сегментов\n\n")
            f.write("| Тип | Количество | Описание |\n")
            f.write("|-----|------------|----------|\n")
            f.write("| bull_run | 6 | Резкий рост цены |\n")
            f.write("| bear_drop | 6 | Резкий спад цены |\n")
            f.write("| flat_false_break | 6 | Флэт с ложными пробоями |\n")
            f.write("| high_volatility | 8 | Высокая волатильность |\n\n")
            
            f.write("## 📋 Детализация сегментов\n\n")
            for segment in segments:
                f.write(f"### {segment['name']}\n")
                f.write(f"- **Тип:** {segment['metadata']['market_condition']}\n")
                f.write(f"- **Волатильность:** {segment['metadata']['volatility']}\n")
                f.write(f"- **Сила тренда:** {segment['metadata']['trend_strength']}\n")
                f.write(f"- **Ключевые события:** {segment['metadata']['key_events']}\n")
                f.write(f"- **Длительность:** {segment['metadata']['duration_hours']} часов\n\n")
        
        logger.info(f"📄 Сводный отчет сохранен: {report_path}")

if __name__ == "__main__":
    preparator = ExperimentDataPreparator()
    preparator.run()
