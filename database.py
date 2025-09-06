#!/usr/bin/env python3
"""
Модуль для работы с SQLite базой данных
"""

import sqlite3
import os
from datetime import datetime
from logger import get_logger

logger = get_logger()

class IVDatabase:
    """Класс для работы с базой данных IV данных"""
    
    def __init__(self, db_path='data/sol_iv.db'):
        """
        Инициализация базы данных
        
        Args:
            db_path (str): Путь к файлу базы данных
        """
        self.db_path = db_path
        self.ensure_data_directory()
        self.init_database()
    
    def ensure_data_directory(self):
        """Создает директорию data если её нет"""
        data_dir = os.path.dirname(self.db_path)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"📁 Создана директория: {data_dir}")
    
    def init_database(self):
        """Инициализирует базу данных и создает таблицу если её нет"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицу iv_data если её нет
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS iv_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    markIv REAL,
                    bid1Iv REAL,
                    ask1Iv REAL,
                    markPrice REAL NOT NULL,
                    underlyingPrice REAL NOT NULL,
                    delta REAL NOT NULL,
                    gamma REAL NOT NULL,
                    vega REAL NOT NULL,
                    theta REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                cursor.execute(create_table_sql)
                conn.commit()
                
                # Проверяем количество записей в таблице
                cursor.execute("SELECT COUNT(*) FROM iv_data")
                count = cursor.fetchone()[0]
                
                logger.info(f"🗄️ База данных инициализирована: {self.db_path}")
                logger.info(f"📊 Записей в таблице iv_data: {count}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise
    
    def insert_iv_data(self, data_list):
        """
        Вставляет данные IV в базу данных
        
        Args:
            data_list (list): Список словарей с данными IV
        """
        if not data_list:
            logger.warning("⚠️ Пустой список данных для вставки")
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                insert_sql = """
                INSERT INTO iv_data (
                    time, symbol, markIv, bid1Iv, ask1Iv, 
                    markPrice, underlyingPrice, delta, gamma, vega, theta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Подготавливаем данные для вставки
                values = []
                for data in data_list:
                    values.append((
                        data['time'].isoformat() if hasattr(data['time'], 'isoformat') else str(data['time']),
                        data['symbol'],
                        data['markIv'],
                        data['bid1Iv'],
                        data['ask1Iv'],
                        data['markPrice'],
                        data['underlyingPrice'],
                        data['delta'],
                        data['gamma'],
                        data['vega'],
                        data['theta']
                    ))
                
                cursor.executemany(insert_sql, values)
                conn.commit()
                
                logger.info(f"💾 Вставлено {len(data_list)} записей в базу данных")
                
        except Exception as e:
            logger.error(f"❌ Ошибка вставки данных в базу: {e}")
            raise
    
    def get_data_by_date(self, date):
        """
        Получает данные за указанную дату
        
        Args:
            date (str): Дата в формате YYYY-MM-DD
            
        Returns:
            list: Список записей за указанную дату
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                select_sql = """
                SELECT time, symbol, markIv, bid1Iv, ask1Iv, 
                       markPrice, underlyingPrice, delta, gamma, vega, theta
                FROM iv_data 
                WHERE DATE(time) = ?
                ORDER BY time
                """
                
                cursor.execute(select_sql, (date,))
                rows = cursor.fetchall()
                
                # Преобразуем в список словарей
                data = []
                for row in rows:
                    data.append({
                        'time': row[0],
                        'symbol': row[1],
                        'markIv': row[2],
                        'bid1Iv': row[3],
                        'ask1Iv': row[4],
                        'markPrice': row[5],
                        'underlyingPrice': row[6],
                        'delta': row[7],
                        'gamma': row[8],
                        'vega': row[9],
                        'theta': row[10]
                    })
                
                logger.info(f"📊 Получено {len(data)} записей за {date}")
                return data
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных за {date}: {e}")
            return []
    
    def get_total_records(self):
        """Возвращает общее количество записей в базе"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM iv_data")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Ошибка подсчета записей: {e}")
            return 0
    
    def get_unique_symbols(self):
        """Возвращает список уникальных символов в базе"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT symbol FROM iv_data ORDER BY symbol")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения символов: {e}")
            return []

def export_to_csv(date, output_file=None):
    """
    Экспортирует данные за указанную дату в CSV файл
    
    Args:
        date (str): Дата в формате YYYY-MM-DD
        output_file (str): Путь к выходному файлу (опционально)
    
    Returns:
        str: Путь к созданному файлу
    """
    import pandas as pd
    
    if output_file is None:
        output_file = f'sol_iv_{date}.csv'
    
    try:
        # Создаем экземпляр базы данных
        db = IVDatabase()
        
        # Получаем данные за указанную дату
        data = db.get_data_by_date(date)
        
        if not data:
            logger.warning(f"⚠️ Нет данных за {date} для экспорта")
            return None
        
        # Создаем DataFrame и сохраняем в CSV
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
        
        logger.info(f"📤 Экспортировано {len(data)} записей за {date} в {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта данных за {date}: {e}")
        return None

# Глобальный экземпляр базы данных
db = None

def get_database():
    """Возвращает глобальный экземпляр базы данных"""
    global db
    if db is None:
        db = IVDatabase()
    return db

if __name__ == "__main__":
    # Тестирование модуля
    logger.info("🧪 Тестирование модуля базы данных")
    
    # Создаем тестовые данные
    test_data = [
        {
            'time': datetime.now(),
            'symbol': 'SOL-26SEP25-360-P-USDT',
            'markIv': 1.1563,
            'bid1Iv': None,
            'ask1Iv': 2.1109,
            'markPrice': 159.96,
            'underlyingPrice': 200.73,
            'delta': -0.9669,
            'gamma': 0.0012,
            'vega': 0.0377,
            'theta': -0.0921
        }
    ]
    
    # Тестируем вставку
    db = get_database()
    db.insert_iv_data(test_data)
    
    # Тестируем получение данных
    today = datetime.now().strftime('%Y-%m-%d')
    data = db.get_data_by_date(today)
    logger.info(f"📊 Получено {len(data)} записей за сегодня")
    
    # Тестируем экспорт
    export_file = export_to_csv(today)
    if export_file:
        logger.info(f"✅ Экспорт успешен: {export_file}")
    
    logger.info("🎯 Тестирование завершено")
