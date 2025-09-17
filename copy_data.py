#!/usr/bin/env python3
"""
Скрипт для копирования данных из data/sol_iv.db в server_opc.db
"""

import sqlite3
from datetime import datetime

def copy_data():
    """Копирует данные из одной базы в другую"""
    # Подключаемся к исходной базе данных
    source_conn = sqlite3.connect('data/sol_iv.db')
    source_cursor = source_conn.cursor()
    
    # Подключаемся к целевой базе данных
    target_conn = sqlite3.connect('server_opc.db')
    target_cursor = target_conn.cursor()
    
    try:
        # Получаем данные за сегодня из исходной базы
        today = datetime.now().strftime('%Y-%m-%d')
        source_cursor.execute("""
            SELECT time, symbol, dataset_tag, markIv, bid1Iv, ask1Iv,
                   markPrice, underlyingPrice, delta, gamma, vega, theta,
                   open_interest, volume_24h
            FROM iv_data
            WHERE DATE(time) = ?
            ORDER BY time
        """, (today,))
        
        rows = source_cursor.fetchall()
        print(f"Найдено {len(rows)} записей для копирования")
        
        # Вставляем данные в целевую базу
        for row in rows:
            target_cursor.execute("""
                INSERT INTO iv_data (
                    time, symbol, dataset_tag, markIv, bid1Iv, ask1Iv,
                    markPrice, underlyingPrice, delta, gamma, vega, theta,
                    open_interest, volume_24h
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        target_conn.commit()
        print(f"Успешно скопировано {len(rows)} записей в основную базу данных")
        
    except Exception as e:
        print(f"Ошибка при копировании данных: {e}")
        target_conn.rollback()
    finally:
        source_conn.close()
        target_conn.close()

if __name__ == "__main__":
    copy_data()