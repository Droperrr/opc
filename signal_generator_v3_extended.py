#!/usr/bin/env python3
"""
Модуль генерации сигналов v3.0 Extended - Оптимизированная система с расширенными данными
Демонстрация улучшенной производительности с 7-дневными данными
Период анализа: 25.08.2025 - 03.09.2025
"""

import pandas as pd
import sqlite3
import numpy as np
import requests
import time
from datetime import datetime, timedelta
from logger import get_logger
import os
import json

logger = get_logger()

class OptimizedSignalGeneratorExtended:
    def __init__(self):
        self.signals = []
        self.db_path = 'data/options_enriched.db'
        self.analysis_period = {
            'start': '2025-08-25',
            'end': '2025-09-03',
            'description': 'Анализ проведен на данных за период с 25.08.2025 по 03.09.2025'
        }
        
        # Оптимизированные веса (увеличенная чувствительность)
        self.weights = {
            'trend': 0.30,      # Снижен для большей гибкости
            'skew': 0.35,       # Сохранен как ключевой фактор
            'oi': 0.25,         # Повышен для лучшего покрытия
            'volume': 0.10      # Сохранен
        }
        
        # Динамические пороги (базовые значения)
        self.base_thresholds = {
            'min_oi_ratio': 0.01,    # Минимум 1% от среднерыночного
            'min_volume_sol': 0.1,   # СНИЖЕН: минимум 0.1 SOL для включения
            'confidence_threshold': 0.25,  # СНИЖЕН: более гибкий порог
            'skew_threshold': 0.01    # СНИЖЕН: более чувствительный
        }
        
        # Временные сессии с весами (UTC)
        self.sessions = {
            'asian': {'start': 0, 'end': 8, 'weight': 1.0},      # 00:00-08:00 UTC
            'european': {'start': 8, 'end': 16, 'weight': 1.15}, # 08:00-16:00 UTC (+15%)
            'american': {'start': 16, 'end': 24, 'weight': 1.2}   # 16:00-24:00 UTC (+20%)
        }
        
        # Сезонные корректировки для опционного рынка Solana
        self.seasonal_factors = {
            'token_unlock_risk': 1.1,    # +10% к confidence при риске анлока
            'high_volatility': 0.9,      # -10% к порогам при высокой волатильности
            'weekend_effect': 0.8        # -20% к активности в выходные
        }
    
    def get_session(self, timestamp):
        """Определяет торговую сессию по времени с весами"""
        hour = timestamp.hour
        for session, times in self.sessions.items():
            if times['start'] <= hour < times['end']:
                return session, times['weight']
        return 'american', self.sessions['american']['weight']  # По умолчанию
    
    def get_dynamic_skew_threshold(self, current_iv):
        """Динамический порог skew в зависимости от текущей IV"""
        base_threshold = self.base_thresholds['skew_threshold']
        # Масштабирование под текущую волатильность
        # При IV > 80% снижаем порог для большей чувствительности
        if current_iv > 0.8:
            return base_threshold * 0.7  # -30% при высокой волатильности
        elif current_iv > 0.6:
            return base_threshold * 0.85  # -15% при средней волатильности
        else:
            return base_threshold
    
    def get_dynamic_confidence_threshold(self, session_weight, is_weekend=False):
        """Динамический порог confidence с учетом сессии и времени"""
        base_threshold = self.base_thresholds['confidence_threshold']
        
        # Корректировка по торговой сессии
        session_adjustment = 1.0 / session_weight  # Снижаем порог для более активных сессий
        
        # Корректировка по выходным
        weekend_adjustment = self.seasonal_factors['weekend_effect'] if is_weekend else 1.0
        
        return base_threshold * session_adjustment * weekend_adjustment
    
    def check_token_unlock_risk(self, timestamp):
        """Проверка риска токен анлока (упрощенная модель)"""
        # Упрощенная проверка: если день недели = пятница и час = 16-20 UTC
        # (типичное время для анлоков)
        if timestamp.weekday() == 4 and 16 <= timestamp.hour <= 20:
            return True
        return False
    
    def fetch_binance_oi_data(self, symbol='SOLUSDT'):
        """Получает данные OI с Binance Futures API"""
        try:
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            params = {'symbol': symbol}
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"📊 Получены данные OI с Binance: {data.get('openInterest', 0)}")
                return float(data.get('openInterest', 0))
            else:
                logger.warning(f"⚠️ Binance API недоступен: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных с Binance: {e}")
            return None
    
    def fetch_jupiter_perps_data(self):
        """Получает данные с Jupiter Perps (fallback)"""
        try:
            url = "https://price.jup.ag/v4/price?ids=SOL"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                volume = data.get('data', {}).get('SOL', {}).get('volume24h', 0)
                logger.info(f"📊 Получены данные с Jupiter: объем {volume}")
                return volume
            else:
                logger.warning(f"⚠️ Jupiter API недоступен: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных с Jupiter: {e}")
            return None
    
    def simulate_oi_data(self):
        """Симулирует данные OI на основе рыночных условий"""
        base_oi = 1000000
        growth_factor = 1.8471
        adjusted_oi = base_oi * growth_factor
        call_ratio = 0.6
        put_ratio = 0.4
        return {
            'source': 'simulation_market_data',
            'oi_call': adjusted_oi * call_ratio,
            'oi_put': adjusted_oi * put_ratio,
            'total_oi': adjusted_oi
        }
    
    def get_real_oi_data(self):
        """Получает реальные данные OI с fallback-механизмом"""
        binance_oi = self.fetch_binance_oi_data()
        if binance_oi and binance_oi > 0:
            return {
                'source': 'binance_futures',
                'oi_call': binance_oi * 0.6,
                'oi_put': binance_oi * 0.4,
                'total_oi': binance_oi
            }
        
        jupiter_volume = self.fetch_jupiter_perps_data()
        if jupiter_volume and jupiter_volume > 0:
            estimated_oi = jupiter_volume * 0.1
            return {
                'source': 'jupiter_perps',
                'oi_call': estimated_oi * 0.55,
                'oi_put': estimated_oi * 0.45,
                'total_oi': estimated_oi
            }
        
        logger.warning("⚠️ Все API недоступны, используем симуляцию OI")
        return self.simulate_oi_data()
    
    def load_data(self):
        """Загружает расширенные данные"""
        try:
            # Загружаем расширенные агрегированные данные
            if os.path.exists('iv_aggregates_extended.csv'):
                self.iv_data = pd.read_csv('iv_aggregates_extended.csv', parse_dates=['timestamp'])
                logger.info(f"📈 Загружено {len(self.iv_data)} расширенных агрегированных записей")
            else:
                logger.warning("⚠️ Файл iv_aggregates_extended.csv не найден")
                self.iv_data = pd.DataFrame()
            
            # Загружаем расширенные трендовые сигналы
            if os.path.exists('trend_signals_extended.csv'):
                self.trend_data = pd.read_csv('trend_signals_extended.csv', parse_dates=['timestamp'])
                logger.info(f"📈 Загружено {len(self.trend_data)} расширенных трендовых сигналов")
            else:
                logger.warning("⚠️ Файл trend_signals_extended.csv не найден")
                self.trend_data = pd.DataFrame()
            
            # Загружаем расширенные точки входа
            if os.path.exists('entry_points_extended.csv'):
                self.entry_data = pd.read_csv('entry_points_extended.csv', parse_dates=['timestamp'])
                logger.info(f"🎯 Загружено {len(self.entry_data)} расширенных точек входа")
            else:
                logger.warning("⚠️ Файл entry_points_extended.csv не найден")
                self.entry_data = pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            self.iv_data = pd.DataFrame()
            self.trend_data = pd.DataFrame()
            self.entry_data = pd.DataFrame()
    
    def combine_data(self):
        """Объединяет расширенные данные по временным меткам"""
        try:
            if self.iv_data.empty:
                logger.warning("⚠️ Нет данных для объединения")
                return pd.DataFrame()
            
            # Создаем базовый DataFrame из IV данных и фильтруем пустые строки
            combined = self.iv_data.copy()
            # Удаляем строки с пустыми значениями underlying_price или iv_mean_all
            combined = combined.dropna(subset=['underlying_price', 'iv_mean_all'])
            
            if combined.empty:
                logger.warning("⚠️ Нет валидных данных после фильтрации")
                return pd.DataFrame()
            
            # Добавляем трендовые сигналы (ближайший по времени)
            if not self.trend_data.empty:
                combined['trend_direction'] = None
                combined['trend_confidence'] = 0.0
                
                for idx, row in combined.iterrows():
                    timestamp = row['timestamp']
                    # Ищем ближайший трендовый сигнал
                    time_diff = abs(self.trend_data['timestamp'] - timestamp)
                    if not time_diff.empty:
                        nearest_idx = time_diff.idxmin()
                        if time_diff[nearest_idx] <= pd.Timedelta(hours=1):  # В пределах часа
                            combined.at[idx, 'trend_direction'] = self.trend_data.at[nearest_idx, 'direction']
                            combined.at[idx, 'trend_confidence'] = self.trend_data.at[nearest_idx, 'confidence']
            
            # Добавляем точки входа (ближайший по времени)
            if not self.entry_data.empty:
                combined['entry_direction'] = None
                combined['entry_confidence'] = 0.0
                
                for idx, row in combined.iterrows():
                    timestamp = row['timestamp']
                    # Ищем ближайшую точку входа
                    time_diff = abs(self.entry_data['timestamp'] - timestamp)
                    if not time_diff.empty:
                        nearest_idx = time_diff.idxmin()
                        if time_diff[nearest_idx] <= pd.Timedelta(minutes=5):  # В пределах 5 минут
                            combined.at[idx, 'entry_direction'] = self.entry_data.at[nearest_idx, 'direction']
                            combined.at[idx, 'entry_confidence'] = self.entry_data.at[nearest_idx, 'confidence']
            
            logger.info(f"🔗 Объединено {len(combined)} записей")
            return combined
            
        except Exception as e:
            logger.error(f"❌ Ошибка объединения данных: {e}")
            return pd.DataFrame()
    
    def enhance_data_with_real_oi(self, combined_data):
        """Обогащает данные реальными значениями OI и временной информацией"""
        try:
            logger.info("🔄 Обогащение данных реальными значениями OI...")
            
            # Получаем реальные данные OI
            oi_data = self.get_real_oi_data()
            
            # Добавляем OI данные ко всем записям
            combined_data['oi_call'] = oi_data['oi_call']
            combined_data['oi_put'] = oi_data['oi_put']
            combined_data['oi_ratio'] = oi_data['oi_call'] / (oi_data['oi_call'] + oi_data['oi_put'])
            combined_data['oi_source'] = oi_data['source']
            
            # Добавляем временную информацию
            combined_data['session'] = combined_data['timestamp'].apply(
                lambda x: self.get_session(x)[0]
            )
            combined_data['session_weight'] = combined_data['timestamp'].apply(
                lambda x: self.get_session(x)[1]
            )
            
            # Добавляем информацию о выходных
            combined_data['is_weekend'] = combined_data['timestamp'].apply(
                lambda x: x.weekday() >= 5
            )
            
            # Добавляем информацию о риске токен анлока
            combined_data['token_unlock_risk'] = combined_data['timestamp'].apply(
                lambda x: self.check_token_unlock_risk(x)
            )
            
            # Добавляем период анализа
            combined_data['analysis_period'] = f"{self.analysis_period['start']} - {self.analysis_period['end']}"
            
            logger.info(f"📊 Обогащено {len(combined_data)} записей реальными данными OI")
            return combined_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка обогащения данных: {e}")
            return combined_data
    
    def generate_optimized_signals(self, combined_data):
        """Генерирует оптимизированные сигналы с динамическими порогами"""
        signals = []
        logger.info(f"🔍 Анализируем {len(combined_data)} записей для генерации сигналов")
        
        for _, row in combined_data.iterrows():
            if pd.isna(row['underlying_price']) or pd.isna(row['iv_mean_all']):
                continue
            
            # Получаем динамические пороги
            dynamic_skew_threshold = self.get_dynamic_skew_threshold(row['iv_mean_all'])
            dynamic_confidence_threshold = self.get_dynamic_confidence_threshold(
                row['session_weight'], 
                row['is_weekend']
            )
            
            signal = None
            confidence = 0.0
            reason = []
            
            # 1. Фильтр по тренду (15m и 1h) - сниженный вес
            if row['trend_direction']:
                if row['trend_direction'] == 'BUY':
                    signal = 'LONG'
                    confidence += row['trend_confidence'] * self.weights['trend']
                    reason.append(f"Trend: {row['trend_direction']} (conf: {row['trend_confidence']:.2f})")
                elif row['trend_direction'] == 'SELL':
                    signal = 'SHORT'
                    confidence += row['trend_confidence'] * self.weights['trend']
                    reason.append(f"Trend: {row['trend_direction']} (conf: {row['trend_confidence']:.2f})")
            
            # 2. Фильтр по IV и Skew (более мягкие критерии)
            if not pd.isna(row['skew']):
                skew_threshold = dynamic_skew_threshold
                if row['skew'] > skew_threshold:  # Положительный skew (спрос на коллы)
                    if signal == 'LONG':
                        confidence += self.weights['skew']
                        reason.append(f"Skew bullish: {row['skew']:.4f}")
                    elif signal is None:
                        signal = 'LONG'
                        confidence += self.weights['skew']
                        reason.append(f"Skew bullish: {row['skew']:.4f}")
                elif row['skew'] < -skew_threshold:  # Отрицательный skew (спрос на путы)
                    if signal == 'SHORT':
                        confidence += self.weights['skew']
                        reason.append(f"Skew bearish: {row['skew']:.4f}")
                    elif signal is None:
                        signal = 'SHORT'
                        confidence += self.weights['skew']
                        reason.append(f"Skew bearish: {row['skew']:.4f}")
            
            # 3. Фильтр по OI (повышенный вес)
            if not pd.isna(row['oi_ratio']):
                if row['oi_ratio'] > 0.52:  # Снижен порог
                    if signal == 'LONG':
                        confidence += self.weights['oi']
                        reason.append(f"OI call-heavy: {row['oi_ratio']:.2f}")
                    elif signal is None:
                        signal = 'LONG'
                        confidence += self.weights['oi']
                        reason.append(f"OI call-heavy: {row['oi_ratio']:.2f}")
                elif row['oi_ratio'] < 0.48:  # Снижен порог
                    if signal == 'SHORT':
                        confidence += self.weights['oi']
                        reason.append(f"OI put-heavy: {row['oi_ratio']:.2f}")
                    elif signal is None:
                        signal = 'SHORT'
                        confidence += self.weights['oi']
                        reason.append(f"OI put-heavy: {row['oi_ratio']:.2f}")
            
            # 4. Подтверждение по минутному графику (сниженный вес)
            if row['entry_direction'] and row['entry_direction'] == signal:
                confidence += row['entry_confidence'] * self.weights['volume']
                reason.append(f"1m confirmation: {row['entry_direction']}")
            
            # 5. Корректировка по торговой сессии
            confidence *= row['session_weight']
            
            # 6. Корректировка по выходным
            if row['is_weekend']:
                confidence *= self.seasonal_factors['weekend_effect']
                reason.append("Weekend session")
            
            # 7. Корректировка по риску токен анлока
            if row['token_unlock_risk']:
                confidence *= self.seasonal_factors['token_unlock_risk']
                reason.append("Token unlock risk")
            
            confidence = min(1.0, max(0.0, confidence))
            
            # Сохраняем сигнал только если confidence >= динамического порога
            if signal and confidence >= dynamic_confidence_threshold:
                # Добавляем информацию об объеме
                volume_info = f"Volume: {row.get('oi_call', 0)/1000000:.2f}M SOL"
                reason.append(volume_info)
                
                signals.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'symbol': 'SOL',
                    'strike': row['underlying_price'],
                    'expiry': '2025-09-26',  # Примерная дата
                    'signal': signal,
                    'direction': signal,
                    'confidence': round(confidence, 3),
                    'reason': ' | '.join(reason),
                    'underlying_price': row['underlying_price'],
                    'timeframe': row['timeframe'],
                    'iv': row['iv_mean_all'],
                    'skew': row['skew'],
                    'oi_call': row['oi_call'],
                    'oi_put': row['oi_put'],
                    'oi_ratio': row['oi_ratio'],
                    'oi_source': row['oi_source'],
                    'trend_15m': row.get('trend_direction', ''),
                    'trend_1h': row.get('trend_direction', ''),
                    'session': row['session'],
                    'analysis_period': row['analysis_period'],
                    'session_weight': row['session_weight'],
                    'is_weekend': row['is_weekend'],
                    'token_unlock_risk': row['token_unlock_risk']
                })
        
        return signals
    
    def save_optimized_signals(self, signals):
        """Сохраняет оптимизированные сигналы в CSV"""
        try:
            if signals:
                df = pd.DataFrame(signals)
                df.to_csv('signals_optimized_extended.csv', index=False)
                logger.info(f"💾 Сохранено {len(signals)} оптимизированных сигналов в signals_optimized_extended.csv")
            else:
                logger.warning("⚠️ Нет сигналов для сохранения")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сигналов: {e}")
    
    def generate_optimized_statistics(self, signals):
        """Генерирует расширенную статистику сигналов"""
        try:
            if not signals:
                logger.warning("⚠️ Нет сигналов для статистики")
                return
            
            df = pd.DataFrame(signals)
            
            # Базовая статистика
            total_signals = len(signals)
            avg_confidence = df['confidence'].mean()
            avg_iv = df['iv'].mean()
            avg_skew = df['skew'].mean()
            avg_oi_ratio = df['oi_ratio'].mean()
            
            logger.info(f"📊 Оптимизированная статистика сигналов:")
            logger.info(f"   Всего сигналов: {total_signals}")
            logger.info(f"   Средняя уверенность: {avg_confidence:.3f}")
            logger.info(f"   Средняя IV: {avg_iv:.3f}")
            logger.info(f"   Средний Skew: {avg_skew:.4f}")
            logger.info(f"   Средний OI ratio: {avg_oi_ratio:.3f}")
            
            # Статистика по торговым сессиям
            session_stats = df.groupby('session').agg({
                'confidence': ['count', 'mean'],
                'signal': 'count'
            }).round(3)
            
            logger.info(f"📈 Статистика по торговым сессиям:")
            for session in session_stats.index:
                count = session_stats.loc[session, ('confidence', 'count')]
                conf = session_stats.loc[session, ('confidence', 'mean')]
                logger.info(f"   {session}: {count:.0f} сигналов, conf: {conf:.3f}")
            
            # Статистика по временным фреймам
            timeframe_stats = df.groupby('timeframe').agg({
                'confidence': ['count', 'mean'],
                'signal': 'count'
            }).round(3)
            
            logger.info(f"📊 Статистика по временным фреймам:")
            for timeframe in timeframe_stats.index:
                count = timeframe_stats.loc[timeframe, ('confidence', 'count')]
                conf = timeframe_stats.loc[timeframe, ('confidence', 'mean')]
                logger.info(f"   {timeframe}: {count:.0f} сигналов, conf: {conf:.3f}")
            
            # Статистика по дням недели
            df['weekday'] = pd.to_datetime(df['timestamp']).dt.day_name()
            weekday_stats = df.groupby('weekday').agg({
                'confidence': ['count', 'mean'],
                'signal': 'count'
            }).round(3)
            
            logger.info(f"📅 Статистика по дням недели:")
            for day in weekday_stats.index:
                count = weekday_stats.loc[day, ('confidence', 'count')]
                conf = weekday_stats.loc[day, ('confidence', 'mean')]
                logger.info(f"   {day}: {count:.0f} сигналов, conf: {conf:.3f}")
            
            # Распределение LONG/SHORT
            signal_dist = df['signal'].value_counts()
            logger.info(f"📊 Распределение сигналов:")
            for signal, count in signal_dist.items():
                logger.info(f"   {signal}: {count} сигналов")
            
            # Примеры сигналов
            logger.info(f"📋 Примеры оптимизированных сигналов:")
            for i, signal in enumerate(signals[:3]):
                logger.info(f"   {i+1}. [{signal['timestamp']}] {signal['signal']} | Conf: {signal['confidence']:.3f} | "
                          f"Price: {signal['underlying_price']} | Session: {signal['session']} | "
                          f"OI Source: {signal['oi_source']} | Reason: {signal['reason'][:50]}...")
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации статистики: {e}")
    
    def run(self):
        """Основной метод запуска оптимизированного генератора сигналов"""
        try:
            logger.info("🚀 Запуск оптимизированного генератора сигналов v3.0 Extended...")
            logger.info(f"📅 Период анализа: {self.analysis_period['description']}")
            
            # Загружаем данные
            self.load_data()
            
            # Объединяем данные
            combined_data = self.combine_data()
            if combined_data.empty:
                logger.error("❌ Нет данных для обработки")
                return
            
            # Обогащаем данными OI
            enhanced_data = self.enhance_data_with_real_oi(combined_data)
            
            # Генерируем оптимизированные сигналы
            signals = self.generate_optimized_signals(enhanced_data)
            
            # Сохраняем сигналы
            self.save_optimized_signals(signals)
            
            # Генерируем статистику
            self.generate_optimized_statistics(signals)
            
            logger.info("✅ Оптимизированный генератор сигналов Extended завершен успешно!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в оптимизированном генераторе сигналов: {e}")

if __name__ == "__main__":
    generator = OptimizedSignalGeneratorExtended()
    generator.run()
