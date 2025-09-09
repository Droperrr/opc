"""
Error Monitor Module for BANT Project
Monitors prediction errors and maintains error history
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ErrorMonitor:
    """Monitors prediction errors and maintains error history"""
    
    def __init__(self, db_path: str = "data/sol_iv.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize error history table"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    formula_id TEXT NOT NULL,
                    predicted REAL NOT NULL,
                    actual REAL NOT NULL,
                    error REAL NOT NULL,
                    normalized_error REAL NOT NULL,
                    volatility REAL NOT NULL,
                    confidence REAL NOT NULL,
                    method TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("✅ Error history table initialized")
            
        except Exception as e:
            logger.error(f"❌ Error initializing database: {e}")
    
    def update(self, timestamp: datetime, predicted: float, actual: float, 
               volatility: float, formula_id: str = 'F01', 
               confidence: float = 1.0, method: str = 'unknown'):
        """Update error history with new prediction error"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            error = abs(predicted - actual)
            normalized_error = error / volatility if volatility > 0 else error
            
            cursor.execute("""
                INSERT INTO error_history 
                (timestamp, formula_id, predicted, actual, error, normalized_error, 
                 volatility, confidence, method)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (timestamp.strftime('%Y-%m-%d %H:%M:%S'), formula_id, 
                  predicted, actual, error, normalized_error, 
                  volatility, confidence, method))
            
            conn.commit()
            conn.close()
            logger.info(f"✅ Error recorded: {error:.4f} (normalized: {normalized_error:.4f})")
            
        except Exception as e:
            logger.error(f"❌ Error updating error history: {e}")
    
    def get_errors(self, formula_id: str = None, 
                   start_time: datetime = None, 
                   end_time: datetime = None) -> pd.DataFrame:
        """Get error history for analysis"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = "SELECT * FROM error_history WHERE 1=1"
            params = []
            
            if formula_id:
                query += " AND formula_id = ?"
                params.append(formula_id)
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.strftime('%Y-%m-%d %H:%M:%S'))
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.strftime('%Y-%m-%d %H:%M:%S'))
            
            query += " ORDER BY timestamp DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            logger.info(f"✅ Retrieved {len(df)} error records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error retrieving error history: {e}")
            return pd.DataFrame()
    
    def calculate_error_statistics(self, formula_id: str = None) -> Dict:
        """Calculate error statistics for analysis"""
        try:
            df = self.get_errors(formula_id)
            
            if df.empty:
                return {}
            
            stats = {
                'total_errors': len(df),
                'mean_error': df['error'].mean(),
                'std_error': df['error'].std(),
                'mean_normalized_error': df['normalized_error'].mean(),
                'std_normalized_error': df['normalized_error'].std(),
                'max_error': df['error'].max(),
                'min_error': df['error'].min(),
                'error_percentile_95': df['error'].quantile(0.95),
                'error_percentile_99': df['error'].quantile(0.99)
            }
            
            logger.info(f"✅ Error statistics calculated: {stats['total_errors']} records")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error calculating statistics: {e}")
            return {}
    
    def calculate_daily_statistics(self, formula_id: str = None) -> pd.DataFrame:
        """Calculate daily error statistics"""
        try:
            df = self.get_errors(formula_id)
            
            if df.empty:
                return pd.DataFrame()
            
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            
            daily_stats = df.groupby('date').agg({
                'error': ['count', 'mean', 'std', 'max', 'min'],
                'normalized_error': ['mean', 'std']
            }).round(4)
            
            daily_stats.columns = ['_'.join(col).strip() for col in daily_stats.columns]
            daily_stats = daily_stats.reset_index()
            
            logger.info(f"✅ Daily statistics calculated for {len(daily_stats)} days")
            return daily_stats
            
        except Exception as e:
            logger.error(f"❌ Error calculating daily statistics: {e}")
            return pd.DataFrame()
    
    def get_error_summary(self) -> Dict:
        """Get comprehensive error summary"""
        try:
            # Получаем все ошибки через pandas
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("SELECT error, normalized_error FROM error_history", conn)
            conn.close()
            
            if df.empty:
                return {
                    'overall': {},
                    'by_formula': []
                }
            
            # Вычисляем статистики в Python, так как SQLite не имеет STDDEV
            overall_stats = {
                'total_errors': len(df),
                'mean_error': float(df['error'].mean()),
                'std_error': float(df['error'].std()),
                'mean_normalized_error': float(df['normalized_error'].mean()),
                'std_normalized_error': float(df['normalized_error'].std()),
                'max_error': float(df['error'].max()),
                'min_error': float(df['error'].min())
            }
            
            # Formula-wise statistics
            conn = sqlite3.connect(self.db_path)
            formula_stats = pd.read_sql_query("""
                SELECT
                    formula_id,
                    COUNT(*) as error_count,
                    AVG(error) as mean_error,
                    AVG(normalized_error) as mean_normalized_error
                FROM error_history
                GROUP BY formula_id
                ORDER BY error_count DESC
            """, conn)
            conn.close()
            
            summary = {
                'overall': overall_stats,
                'by_formula': formula_stats.to_dict('records') if not formula_stats.empty else []
            }
            
            logger.info("✅ Error summary generated")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error generating summary: {e}")
            return {}
