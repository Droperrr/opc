#!/usr/bin/env python3
"""
Модуль бэктеста сигналов на основе данных IV опционов SOL
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from logger import get_logger
from signals import IVSignalGenerator

logger = get_logger()

class SignalBacktester:
    """Бэктестер сигналов на основе данных IV"""
    
    def __init__(self, db_path='data/sol_iv.db'):
        """Инициализация бэктестера"""
        self.db_path = db_path
        self.signal_generator = IVSignalGenerator(db_path)
        
    def get_monthly_data(self):
        """Получает данные за последний месяц"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Получаем данные за последний месяц
                cutoff_time = datetime.now() - timedelta(days=30)
                cutoff_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
                
                query = """
                SELECT time, symbol, markIv, underlyingPrice, delta, gamma, vega, theta
                FROM iv_data 
                WHERE time > ?
                ORDER BY time ASC
                """
                
                df = pd.read_sql_query(query, conn, params=(cutoff_str,))
                
                if df.empty:
                    logger.warning("⚠️ Нет данных за последний месяц")
                    return None
                
                # Конвертируем время
                df['time'] = pd.to_datetime(df['time'])
                
                logger.info(f"📊 Получено {len(df)} записей за последний месяц")
                logger.info(f"📅 Период: {df['time'].min()} - {df['time'].max()}")
                
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных: {e}")
            return None
    
    def generate_signals_for_period(self, df):
        """Генерирует сигналы для всего периода данных"""
        if df is None or len(df) == 0:
            return []
        
        logger.info("🎯 Генерация сигналов для бэктеста")
        
        # Группируем данные по дням для генерации сигналов
        df['date'] = df['time'].dt.date
        signals = []
        
        for date in df['date'].unique():
            day_data = df[df['date'] == date].copy()
            
            if len(day_data) < 2:
                continue
            
            # Рассчитываем изменения для этого дня
            changes = self._calculate_changes_for_day(day_data)
            if changes is not None:
                day_signals = self._analyze_patterns_for_day(changes)
                signals.extend(day_signals)
        
        logger.info(f"📈 Сгенерировано {len(signals)} сигналов для бэктеста")
        return signals
    
    def _calculate_changes_for_day(self, day_data):
        """Рассчитывает изменения для одного дня"""
        if len(day_data) < 2:
            return None
        
        # Группируем по символам
        changes = []
        
        for symbol in day_data['symbol'].unique():
            symbol_data = day_data[day_data['symbol'] == symbol].copy()
            if len(symbol_data) < 2:
                continue
            
            # Сортируем по времени
            symbol_data = symbol_data.sort_values('time')
            
            # Рассчитываем изменения
            symbol_data['iv_change'] = symbol_data['markIv'].diff()
            symbol_data['price_change'] = symbol_data['underlyingPrice'].diff()
            
            # Убираем NaN
            symbol_data = symbol_data.dropna()
            
            if not symbol_data.empty:
                changes.append(symbol_data)
        
        if changes:
            return pd.concat(changes, ignore_index=True)
        return None
    
    def _analyze_patterns_for_day(self, changes_df):
        """Анализирует паттерны для одного дня и генерирует сигналы"""
        if changes_df is None or len(changes_df) == 0:
            return []
        
        signals = []
        
        # Группируем по времени
        for timestamp in changes_df['time'].unique():
            time_data = changes_df[changes_df['time'] == timestamp]
            
            # Рассчитываем средние изменения
            avg_iv_change = time_data['iv_change'].mean()
            avg_price_change = time_data['price_change'].mean()
            current_price = time_data['underlyingPrice'].iloc[0]
            
            # Определяем тип опционов
            call_options = time_data[time_data['symbol'].str.contains('-C-')]
            put_options = time_data[time_data['symbol'].str.contains('-P-')]
            
            signal = None
            confidence = 0.5
            reason = ""
            
            # Применяем те же правила, что и в signals.py
            if avg_iv_change > 0.005 and avg_price_change < -0.5:
                signal = "SELL"
                reason = f"рост IV ({avg_iv_change:.4f}) при падении цены ({avg_price_change:.2f})"
            elif avg_iv_change < -0.005 and avg_price_change > 0.5:
                signal = "BUY"
                reason = f"падение IV ({avg_iv_change:.4f}) при росте цены ({avg_price_change:.2f})"
            elif avg_iv_change > 0.02 and abs(avg_price_change) < 0.2:
                signal = "VOLATILITY SPIKE"
                reason = f"резкий рост IV ({avg_iv_change:.4f}) при стабильной цене"
            elif len(call_options) > 0 and len(put_options) > 0:
                call_iv_avg = call_options['markIv'].mean()
                put_iv_avg = put_options['markIv'].mean()
                
                if call_iv_avg > put_iv_avg * 1.05:
                    signal = "BULLISH"
                    reason = f"IV call ({call_iv_avg:.4f}) выше put ({put_iv_avg:.4f})"
                elif put_iv_avg > call_iv_avg * 1.05:
                    signal = "BEARISH"
                    reason = f"IV put ({put_iv_avg:.4f}) выше call ({call_iv_avg:.4f})"
            
            if signal:
                signals.append({
                    'time': timestamp,
                    'signal': signal,
                    'confidence': confidence,
                    'reason': reason,
                    'entry_price': current_price
                })
        
        return signals
    
    def simulate_exits(self, signals, price_data):
        """Симулирует различные стратегии выхода"""
        if not signals:
            return []
        
        logger.info("🎯 Симуляция стратегий выхода")
        
        trades = []
        
        for signal in signals:
            signal_time = signal['time']
            entry_price = signal['entry_price']
            signal_type = signal['signal']
            
            # Получаем данные после сигнала
            future_data = price_data[price_data['time'] > signal_time].copy()
            
            if len(future_data) == 0:
                continue
            
            # Сортируем по времени
            future_data = future_data.sort_values('time')
            
            # Инициализируем результаты
            trade = {
                'signal_time': signal_time,
                'signal': signal['signal'],
                'confidence': signal['confidence'],
                'reason': signal['reason'],
                'entry_price': entry_price,
                'exit_time_1pct': None,
                'exit_price_1pct': None,
                'result_1pct': None,
                'exit_time_2pct': None,
                'exit_price_2pct': None,
                'result_2pct': None,
                'exit_time_5pct': None,
                'exit_price_5pct': None,
                'result_5pct': None,
                'exit_time_trend_v1': None,
                'exit_price_trend_v1': None,
                'result_trend_v1': None,
                'exit_time_trend_v2': None,
                'exit_price_trend_v2': None,
                'result_trend_v2': None,
                'exit_time_trend_v3': None,
                'exit_price_trend_v3': None,
                'result_trend_v3': None
            }
            
            # Стратегия 1: Таргет 1%
            if signal_type in ['BUY', 'BULLISH']:
                target_price = entry_price * 1.01
                target_hit = future_data[future_data['underlyingPrice'] >= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_1pct'] = exit_time
                    trade['exit_price_1pct'] = exit_price
                    trade['result_1pct'] = 1.0
            
            elif signal_type in ['SELL', 'BEARISH']:
                target_price = entry_price * 0.99
                target_hit = future_data[future_data['underlyingPrice'] <= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_1pct'] = exit_time
                    trade['exit_price_1pct'] = exit_price
                    trade['result_1pct'] = 1.0
            
            # Стратегия 2: Таргет 2%
            if signal_type in ['BUY', 'BULLISH']:
                target_price = entry_price * 1.02
                target_hit = future_data[future_data['underlyingPrice'] >= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_2pct'] = exit_time
                    trade['exit_price_2pct'] = exit_price
                    trade['result_2pct'] = 2.0
            
            elif signal_type in ['SELL', 'BEARISH']:
                target_price = entry_price * 0.98
                target_hit = future_data[future_data['underlyingPrice'] <= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_2pct'] = exit_time
                    trade['exit_price_2pct'] = exit_price
                    trade['result_2pct'] = 2.0
            
            # Стратегия 3: Таргет 5%
            if signal_type in ['BUY', 'BULLISH']:
                target_price = entry_price * 1.05
                target_hit = future_data[future_data['underlyingPrice'] >= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_5pct'] = exit_time
                    trade['exit_price_5pct'] = exit_price
                    trade['result_5pct'] = 5.0
            
            elif signal_type in ['SELL', 'BEARISH']:
                target_price = entry_price * 0.95
                target_hit = future_data[future_data['underlyingPrice'] <= target_price]
                if not target_hit.empty:
                    exit_time = target_hit.iloc[0]['time']
                    exit_price = target_hit.iloc[0]['underlyingPrice']
                    trade['exit_time_5pct'] = exit_time
                    trade['exit_price_5pct'] = exit_price
                    trade['result_5pct'] = 5.0
            
            # Стратегия 4: Смена тренда v1 (противоположный сигнал)
            opposite_signals = {
                'BUY': ['SELL', 'BEARISH'],
                'SELL': ['BUY', 'BULLISH'],
                'BULLISH': ['SELL', 'BEARISH'],
                'BEARISH': ['BUY', 'BULLISH'],
                'VOLATILITY SPIKE': ['BUY', 'SELL', 'BULLISH', 'BEARISH']
            }
            
            if signal_type in opposite_signals:
                opposite_list = opposite_signals[signal_type]
                for future_time in future_data['time']:
                    future_signals = self._get_signals_at_time(future_time, price_data)
                    if any(s['signal'] in opposite_list for s in future_signals):
                        exit_price = future_data[future_data['time'] == future_time]['underlyingPrice'].iloc[0]
                        trade['exit_time_trend_v1'] = future_time
                        trade['exit_price_trend_v1'] = exit_price
                        trade['result_trend_v1'] = ((exit_price - entry_price) / entry_price) * 100
                        break
            
            # Стратегия 5: Смена тренда v2 (любой сигнал)
            for future_time in future_data['time']:
                future_signals = self._get_signals_at_time(future_time, price_data)
                if future_signals:
                    exit_price = future_data[future_data['time'] == future_time]['underlyingPrice'].iloc[0]
                    trade['exit_time_trend_v2'] = future_time
                    trade['exit_price_trend_v2'] = exit_price
                    trade['result_trend_v2'] = ((exit_price - entry_price) / entry_price) * 100
                    break
            
            # Стратегия 6: Смена тренда v3 (противоречащий сигнал)
            for future_time in future_data['time']:
                future_signals = self._get_signals_at_time(future_time, price_data)
                if any(s['signal'] != signal_type for s in future_signals):
                    exit_price = future_data[future_data['time'] == future_time]['underlyingPrice'].iloc[0]
                    trade['exit_time_trend_v3'] = future_time
                    trade['exit_price_trend_v3'] = exit_price
                    trade['result_trend_v3'] = ((exit_price - entry_price) / entry_price) * 100
                    break
            
            trades.append(trade)
        
        logger.info(f"📊 Симулировано {len(trades)} сделок")
        return trades
    
    def _get_signals_at_time(self, time, price_data):
        """Получает сигналы в определенное время (упрощенная версия)"""
        # Для демонстрации возвращаем случайные сигналы
        import random
        signals = ['BUY', 'SELL', 'BULLISH', 'BEARISH', 'VOLATILITY SPIKE']
        if random.random() < 0.3:  # 30% вероятность сигнала
            return [{'signal': random.choice(signals)}]
        return []
    
    def save_trades_to_csv(self, trades, filename='signals_backtest.csv'):
        """Сохраняет сделки в CSV файл"""
        if not trades:
            logger.warning("⚠️ Нет сделок для сохранения")
            return
        
        try:
            df = pd.DataFrame(trades)
            df.to_csv(filename, index=False)
            logger.info(f"💾 Сохранено {len(trades)} сделок в {filename}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сделок: {e}")
    
    def run_backtest(self):
        """Запускает полный бэктест"""
        logger.info("🚀 Запуск полного бэктеста сигналов")
        
        # Получаем данные за месяц
        price_data = self.get_monthly_data()
        if price_data is None:
            logger.error("❌ Не удалось получить данные для бэктеста")
            return None
        
        # Генерируем сигналы
        signals = self.generate_signals_for_period(price_data)
        if not signals:
            logger.warning("⚠️ Не удалось сгенерировать сигналы")
            return None
        
        # Симулируем выходы
        trades = self.simulate_exits(signals, price_data)
        if not trades:
            logger.warning("⚠️ Не удалось симулировать сделки")
            return None
        
        # Сохраняем результаты
        self.save_trades_to_csv(trades)
        
        logger.info("✅ Бэктест завершен успешно")
        return trades

def run_backtest_demo():
    """Демонстрация работы бэктестера"""
    logger.info("🎯 Демонстрация бэктестера сигналов")
    
    # Создаем бэктестер
    backtester = SignalBacktester()
    
    # Запускаем бэктест
    trades = backtester.run_backtest()
    
    if trades:
        print(f"\n📊 Результаты бэктеста:")
        print(f"Всего сделок: {len(trades)}")
        
        # Показываем первые 5 сделок
        df = pd.DataFrame(trades)
        print(f"\n📋 Первые 5 сделок:")
        print(df.head().to_string(index=False))
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация работы бэктестера
    run_backtest_demo()
