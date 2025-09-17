#!/usr/bin/env python3
"""
Симуляция API Deribit для тестирования
"""
import json
import random
from datetime import datetime, timedelta

def simulate_trades(currency, kind, start_timestamp, end_timestamp, count=10000):
    """
    Симуляция получения данных о сделках с Deribit API
    """
    # Преобразуем timestamp в datetime для удобства
    start_dt = datetime.fromtimestamp(start_timestamp / 1000)
    end_dt = datetime.fromtimestamp(end_timestamp / 1000)
    
    # Определяем период времени для генерации сделок
    time_diff = end_dt - start_dt
    total_seconds = int(time_diff.total_seconds())
    
    trades = []
    
    # Генерируем случайные сделки
    for i in range(min(count, max(1, total_seconds // 10))):  # Ограничиваем количество сделок
        # Генерируем случайное время в пределах периода
        random_seconds = random.randint(0, total_seconds)
        trade_time = start_dt + timedelta(seconds=random_seconds)
        timestamp = int(trade_time.timestamp() * 1000)
        
        # Генерируем другие параметры сделки
        trade = {
            "trade_id": f"TRADE_{currency}_{timestamp}_{i}",
            "timestamp": timestamp,
            "instrument_name": f"{currency}-26SEP25-360-C",
            "price": round(random.uniform(100, 500), 2),
            "amount": round(random.uniform(0.1, 10), 8),
            "direction": random.choice(["buy", "sell"]),
            "iv": round(random.uniform(0.1, 1.0), 4),
            "settlement_price": round(random.uniform(100, 500), 2),
            "underlying_price": round(random.uniform(100, 500), 2)
        }
        
        trades.append(trade)
    
    # Сортируем сделки по времени
    trades.sort(key=lambda x: x["timestamp"])
    
    return trades

if __name__ == "__main__":
    # Тест симуляции
    start_ts = int(datetime(2023, 1, 1, 0, 0, 0).timestamp() * 1000)
    end_ts = int(datetime(2023, 1, 1, 1, 0, 0).timestamp() * 1000)
    
    trades = simulate_trades("BTC", "option", start_ts, end_ts, 10)
    
    print("Симуляция сделок:")
    for trade in trades:
        print(json.dumps(trade, indent=2))