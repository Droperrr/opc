#!/usr/bin/env python3
import asyncio
import websockets
import json
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def websocket_client():
    """
    WebSocket клиент для получения одной сделки по BTC-PERPETUAL
    """
    uri = "wss://www.deribit.com/ws/api/v2"
    
    try:
        async with websockets.connect(uri) as websocket:
            logger.info("Подключение к WebSocket API Deribit установлено")
            
            # Подписка на ленту сделок по бессрочному фьючерсу BTC
            subscribe_message = {
                "jsonrpc": "2.0",
                "method": "public/subscribe",
                "id": 42,
                "params": {
                    "channels": ["trades.BTC-PERPETUAL.raw"]
                }
            }
            
            await websocket.send(json.dumps(subscribe_message))
            logger.info("Отправлен запрос на подписку на ленту сделок")
            
            # Ожидание ответа на запрос подписки
            response = await websocket.recv()
            response_data = json.loads(response)
            logger.info(f"Получен ответ на запрос подписки: {response_data}")
            
            # Ожидание первой сделки
            logger.info("Ожидание первой сделки...")
            trade_message = await websocket.recv()
            trade_data = json.loads(trade_message)
            
            # Вывод сделки в формате JSON
            print(json.dumps(trade_data, indent=2))
            logger.info("Получена сделка. Завершение работы.")
            
    except Exception as e:
        logger.error(f"Ошибка при работе с WebSocket: {e}")

if __name__ == "__main__":
    asyncio.run(websocket_client())