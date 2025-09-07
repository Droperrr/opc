#!/usr/bin/env python3
"""
Формулы для тестирования в экспериментах S-03
"""

import sqlite3
import pandas as pd
from logger import get_logger

logger = get_logger()

class FormulaSeeder:
    def __init__(self):
        self.db_path = 'data/sol_iv.db'
        
    def create_formulas_table(self):
        """Создает таблицу формул в БД"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу формул
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS formulas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    formula_text TEXT NOT NULL,
                    parameters TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Таблица formulas создана")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы формул: {e}")
    
    def insert_formulas(self):
        """Вставляет формулы в БД"""
        formulas = [
            {
                'name': 'IV_skew_weighted',
                'description': 'Взвешенная формула на основе IV и skew',
                'formula_text': 'Y = 0.4*iv + 0.35*skew + 0.15*basis_rel + 0.1*oi_ratio',
                'parameters': '{"iv_weight": 0.4, "skew_weight": 0.35, "basis_weight": 0.15, "oi_weight": 0.1}'
            },
            {
                'name': 'spread_dominant',
                'description': 'Формула с доминированием spread компонентов',
                'formula_text': 'Y = 0.5*iv + 0.3*skew - 0.2*basis_rel',
                'parameters': '{"iv_weight": 0.5, "skew_weight": 0.3, "basis_weight": -0.2}'
            },
            {
                'name': 'volatility_focused',
                'description': 'Фокус на волатильности и OI',
                'formula_text': 'Y = 0.6*iv + 0.25*oi_ratio + 0.15*skew',
                'parameters': '{"iv_weight": 0.6, "oi_weight": 0.25, "skew_weight": 0.15}'
            },
            {
                'name': 'balanced_approach',
                'description': 'Сбалансированный подход всех компонентов',
                'formula_text': 'Y = 0.3*iv + 0.3*skew + 0.2*basis_rel + 0.2*oi_ratio',
                'parameters': '{"iv_weight": 0.3, "skew_weight": 0.3, "basis_weight": 0.2, "oi_weight": 0.2}'
            },
            {
                'name': 'momentum_based',
                'description': 'Формула на основе моментума IV и skew',
                'formula_text': 'Y = 0.4*iv_momentum + 0.4*skew_momentum + 0.2*oi_ratio',
                'parameters': '{"iv_momentum_weight": 0.4, "skew_momentum_weight": 0.4, "oi_weight": 0.2}'
            }
        ]
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for formula in formulas:
                cursor.execute('''
                    INSERT OR REPLACE INTO formulas (name, description, formula_text, parameters)
                    VALUES (?, ?, ?, ?)
                ''', (formula['name'], formula['description'], formula['formula_text'], formula['parameters']))
            
            conn.commit()
            conn.close()
            logger.info(f"✅ Вставлено {len(formulas)} формул")
            
        except Exception as e:
            logger.error(f"❌ Ошибка вставки формул: {e}")
    
    def get_formulas(self):
        """Получает все активные формулы"""
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
            logger.error(f"❌ Ошибка получения формул: {e}")
            return pd.DataFrame()
    
    def run(self):
        """Основной метод запуска"""
        try:
            logger.info("🚀 Создание формул для экспериментов...")
            
            # Создаем таблицу
            self.create_formulas_table()
            
            # Вставляем формулы
            self.insert_formulas()
            
            # Показываем результат
            formulas = self.get_formulas()
            logger.info(f"📊 Доступно формул: {len(formulas)}")
            
            for _, formula in formulas.iterrows():
                logger.info(f"  - {formula['name']}: {formula['description']}")
            
            logger.info("✅ Формулы созданы успешно!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания формул: {e}")

if __name__ == "__main__":
    seeder = FormulaSeeder()
    seeder.run()
