#!/usr/bin/env python3
"""
Модуль генератора сигналов на основе данных IV опционов SOL
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from logger import get_logger
from database import get_database

logger = get_logger()

class IVSignalGenerator:
    """Генератор сигналов на основе данных IV"""
    
    def __init__(self, db_path='data/sol_iv.db'):
        """Инициализация генератора сигналов"""
        self.db_path = db_path
        self.init_signals_table()
        
    def init_signals_table(self):
        """Инициализирует таблицу signals если её нет"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                cursor.execute(create_table_sql)
                conn.commit()
                
                # Проверяем количество записей
                cursor.execute("SELECT COUNT(*) FROM signals")
                count = cursor.fetchone()[0]
                logger.info(f"📊 Таблица signals инициализирована: {count} записей")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации таблицы signals: {e}")
            raise
    
    def get_recent_iv_data(self, hours=2):
        """Получает последние данные IV за указанное количество часов"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Получаем данные за последние N часов
                cutoff_time = datetime.now() - timedelta(hours=hours)
                cutoff_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
                
                query = """
                SELECT time, symbol, markIv, underlyingPrice, delta, gamma, vega, theta
                FROM iv_data 
                WHERE time > ?
                ORDER BY time DESC
                """
                
                df = pd.read_sql_query(query, conn, params=(cutoff_str,))
                
                if df.empty:
                    logger.warning(f"⚠️ Нет данных IV за последние {hours} часов")
                    return None
                
                logger.info(f"📊 Получено {len(df)} записей IV за последние {hours} часов")
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных IV: {e}")
            return None
    
    def calculate_iv_changes(self, df):
        """Рассчитывает изменения IV и цены"""
        if df is None or len(df) < 2:
            return None
        
        # Группируем по символам и сортируем по времени
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values(['symbol', 'time'])
        
        changes = []
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].copy()
            if len(symbol_data) < 2:
                continue
            
            # Рассчитываем изменения
            symbol_data = symbol_data.sort_values('time')
            symbol_data['iv_change'] = symbol_data['markIv'].diff()
            symbol_data['price_change'] = symbol_data['underlyingPrice'].diff()
            symbol_data['time_diff'] = symbol_data['time'].diff().dt.total_seconds() / 60  # в минутах
            
            # Убираем первую строку (NaN после diff)
            symbol_data = symbol_data.dropna()
            
            if not symbol_data.empty:
                changes.append(symbol_data)
        
        if changes:
            result_df = pd.concat(changes, ignore_index=True)
            logger.info(f"📈 Рассчитаны изменения для {len(result_df)} записей")
            return result_df
        else:
            logger.warning("⚠️ Не удалось рассчитать изменения")
            return None
    
    def analyze_iv_patterns(self, changes_df):
        """Анализирует паттерны IV и генерирует сигналы"""
        if changes_df is None or len(changes_df) == 0:
            return []
        
        signals = []
        
        # Группируем по времени для анализа всех символов одновременно
        for timestamp in changes_df['time'].unique():
            time_data = changes_df[changes_df['time'] == timestamp]
            
            # Рассчитываем средние изменения
            avg_iv_change = time_data['iv_change'].mean()
            avg_price_change = time_data['price_change'].mean()
            
            # Рассчитываем волатильность изменений
            iv_volatility = time_data['iv_change'].std()
            price_volatility = time_data['price_change'].std()
            
            # Определяем тип опционов (call/put)
            call_options = time_data[time_data['symbol'].str.contains('-C-')]
            put_options = time_data[time_data['symbol'].str.contains('-P-')]
            
            signal = None
            confidence = 0.0
            reason = ""
            
            # Правило 1: IV растет, цена падает → SELL
            if avg_iv_change > 0.005 and avg_price_change < -0.5:
                signal = "SELL"
                confidence = min(abs(avg_iv_change) / max(iv_volatility, 0.001), 1.0)
                if pd.isna(confidence):
                    confidence = 0.5
                reason = f"рост IV ({avg_iv_change:.4f}) при падении цены ({avg_price_change:.2f})"
            
            # Правило 2: IV падает, цена растет → BUY
            elif avg_iv_change < -0.005 and avg_price_change > 0.5:
                signal = "BUY"
                confidence = min(abs(avg_iv_change) / max(iv_volatility, 0.001), 1.0)
                if pd.isna(confidence):
                    confidence = 0.5
                reason = f"падение IV ({avg_iv_change:.4f}) при росте цены ({avg_price_change:.2f})"
            
            # Правило 3: IV резко растет, цена стабильна → VOLATILITY SPIKE
            elif avg_iv_change > 0.02 and abs(avg_price_change) < 0.2:
                signal = "VOLATILITY SPIKE"
                confidence = min(abs(avg_iv_change) / max(iv_volatility, 0.001), 1.0)
                if pd.isna(confidence):
                    confidence = 0.5
                reason = f"резкий рост IV ({avg_iv_change:.4f}) при стабильной цене"
            
            # Правило 3.5: IV растет при росте цены → VOLATILITY SPIKE (альтернативный паттерн)
            elif avg_iv_change > 0.015 and avg_price_change > 0.3:
                signal = "VOLATILITY SPIKE"
                confidence = min(abs(avg_iv_change) / max(iv_volatility, 0.001), 1.0)
                if pd.isna(confidence):
                    confidence = 0.5
                reason = f"рост IV ({avg_iv_change:.4f}) при росте цены ({avg_price_change:.2f}) - волатильность"
            
            # Правило 4: IV call выше put → BULLISH
            elif len(call_options) > 0 and len(put_options) > 0:
                call_iv_avg = call_options['markIv'].mean()
                put_iv_avg = put_options['markIv'].mean()
                
                if call_iv_avg > put_iv_avg * 1.05:  # Call IV на 5% выше
                    signal = "BULLISH"
                    confidence = min((call_iv_avg - put_iv_avg) / put_iv_avg, 1.0)
                    if pd.isna(confidence):
                        confidence = 0.5
                    reason = f"IV call ({call_iv_avg:.4f}) выше put ({put_iv_avg:.4f})"
                
                elif put_iv_avg > call_iv_avg * 1.05:  # Put IV на 5% выше
                    signal = "BEARISH"
                    confidence = min((put_iv_avg - call_iv_avg) / call_iv_avg, 1.0)
                    if pd.isna(confidence):
                        confidence = 0.5
                    reason = f"IV put ({put_iv_avg:.4f}) выше call ({call_iv_avg:.4f})"
            
            if signal:
                signals.append({
                    'time': timestamp,
                    'timeframe': '15m',  # Фиксированный таймфрейм для демонстрации
                    'signal': signal,
                    'confidence': round(confidence, 2),
                    'reason': reason
                })
                
                # Выводим сигнал в консоль
                time_str = timestamp.strftime('%Y-%m-%d %H:%M')
                print(f"[{time_str}] (15m) Signal: {signal} | Confidence: {confidence:.2f}")
                print(f"Причина: {reason}")
                print("-" * 50)
        
        return signals
    
    def save_signals(self, signals):
        """Сохраняет сигналы в базу данных"""
        if not signals:
            logger.warning("⚠️ Нет сигналов для сохранения")
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                insert_sql = """
                INSERT INTO signals (time, timeframe, signal, confidence, reason)
                VALUES (?, ?, ?, ?, ?)
                """
                
                values = []
                for signal in signals:
                    values.append((
                        signal['time'].strftime('%Y-%m-%d %H:%M:%S'),
                        signal['timeframe'],
                        signal['signal'],
                        signal['confidence'],
                        signal['reason']
                    ))
                
                cursor.executemany(insert_sql, values)
                conn.commit()
                
                logger.info(f"💾 Сохранено {len(signals)} сигналов в базу данных")
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сигналов: {e}")
    
    def get_recent_signals(self, limit=10):
        """Получает последние сигналы из базы данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                SELECT time, timeframe, signal, confidence, reason
                FROM signals 
                ORDER BY time DESC 
                LIMIT ?
                """
                
                df = pd.read_sql_query(query, conn, params=(limit,))
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигналов: {e}")
            return pd.DataFrame()
    
    def generate_signals(self):
        """Основной метод генерации сигналов"""
        logger.info("🎯 Запуск генерации сигналов")
        
        # Получаем последние данные IV
        iv_data = self.get_recent_iv_data(hours=2)
        if iv_data is None:
            logger.error("❌ Не удалось получить данные IV")
            return []
        
        # Рассчитываем изменения
        changes = self.calculate_iv_changes(iv_data)
        if changes is None:
            logger.error("❌ Не удалось рассчитать изменения")
            return []
        
        # Анализируем паттерны и генерируем сигналы
        signals = self.analyze_iv_patterns(changes)
        
        if signals:
            # Сохраняем сигналы
            self.save_signals(signals)
            logger.info(f"✅ Сгенерировано {len(signals)} сигналов")
        else:
            logger.info("ℹ️ Сигналы не найдены")
        
        return signals

def run_signal_demo():
    """Демонстрация работы генератора сигналов"""
    logger.info("🎯 Демонстрация генератора сигналов")
    
    # Создаем генератор сигналов
    generator = IVSignalGenerator()
    
    # Генерируем сигналы
    signals = generator.generate_signals()
    
    if signals:
        print(f"\n📊 Сгенерировано {len(signals)} сигналов:")
        for i, signal in enumerate(signals, 1):
            time_str = signal['time'].strftime('%Y-%m-%d %H:%M')
            print(f"{i}. [{time_str}] {signal['signal']} (Confidence: {signal['confidence']:.2f})")
            print(f"   Причина: {signal['reason']}")
    
    # Показываем последние сигналы из БД
    recent_signals = generator.get_recent_signals(5)
    if not recent_signals.empty:
        print(f"\n🗄️ Последние сигналы в БД:")
        print(recent_signals.to_string(index=False))
    
    logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация работы генератора сигналов
    run_signal_demo()
