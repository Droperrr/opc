#!/usr/bin/env python3
"""
Проверка диапазона данных в базе
"""

import sqlite3

def check_data_range():
    try:
        conn = sqlite3.connect('data/sol_iv.db')
        cursor = conn.cursor()
        
        # Проверяем iv_agg
        cursor.execute("SELECT MIN(time), MAX(time), COUNT(*) FROM iv_agg WHERE timeframe = '1m'")
        result = cursor.fetchone()
        print(f"IV data: {result[0]} - {result[1]}, {result[2]} records")
        
        # Проверяем basis_agg
        cursor.execute("SELECT MIN(time), MAX(time), COUNT(*) FROM basis_agg WHERE timeframe = '1m'")
        result = cursor.fetchone()
        print(f"Basis data: {result[0]} - {result[1]}, {result[2]} records")
        
        # Проверяем spot_data
        cursor.execute("SELECT MIN(time), MAX(time), COUNT(*) FROM spot_data WHERE timeframe = '1m'")
        result = cursor.fetchone()
        print(f"Spot data: {result[0]} - {result[1]}, {result[2]} records")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_data_range()
