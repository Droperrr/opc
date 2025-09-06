#!/usr/bin/env python3
"""
Модуль планировщика для автоматического сбора данных IV
"""

import schedule
import time
import threading
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from logger import get_logger
from get_iv import get_all_sol_options_data
from database import get_database

# Загружаем переменные окружения
load_dotenv()

logger = get_logger()

class IVDataScheduler:
    """Планировщик для автоматического сбора данных IV"""
    
    def __init__(self):
        """Инициализация планировщика"""
        self.running = False
        self.thread = None
        
        # Получаем интервал из переменных окружения (по умолчанию 15 минут)
        self.interval_minutes = int(os.getenv('FETCH_INTERVAL_MINUTES', 15))
        
        logger.info(f"🔧 Планировщик инициализирован с интервалом {self.interval_minutes} минут")
        
        # Настраиваем расписание
        self.setup_schedule()
    
    def setup_schedule(self):
        """Настраивает расписание выполнения задач"""
        # Запускаем сбор данных каждые N минут
        schedule.every(self.interval_minutes).minutes.do(self.collect_data_job)
        
        # Также запускаем сразу при старте
        schedule.every().minute.at(":00").do(self.collect_data_job)
        
        logger.info(f"📅 Расписание настроено: сбор данных каждые {self.interval_minutes} минут")
    
    def collect_data_job(self):
        """Задача сбора данных IV"""
        start_time = datetime.now()
        logger.info(f"🚀 Запуск планового сбора данных IV в {start_time.strftime('%H:%M:%S')}")
        
        try:
            # Собираем данные (ограничиваем до 10 символов для демонстрации)
            # В продакшене можно убрать max_symbols для сбора всех опционов
            iv_df = get_all_sol_options_data(max_symbols=10)
            
            if iv_df is not None:
                # Показываем статистику базы данных
                db = get_database()
                total_records = db.get_total_records()
                unique_symbols = db.get_unique_symbols()
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"✅ Плановый сбор завершен успешно")
                logger.info(f"⏱️ Время выполнения: {duration:.2f} секунд")
                logger.info(f"📊 Записей в БД: {total_records}")
                logger.info(f"🎯 Уникальных символов: {len(unique_symbols)}")
                
                # Показываем время следующего запуска
                next_run = start_time + timedelta(minutes=self.interval_minutes)
                logger.info(f"⏰ Следующий запуск: {next_run.strftime('%H:%M:%S')}")
                
            else:
                logger.error("❌ Плановый сбор завершился с ошибкой")
                
        except Exception as e:
            logger.error(f"❌ Ошибка в плановом сборе данных: {e}")
    
    def start(self):
        """Запускает планировщик в отдельном потоке"""
        if self.running:
            logger.warning("⚠️ Планировщик уже запущен")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        
        logger.info("🎯 Планировщик запущен в фоновом режиме")
        logger.info(f"📅 Интервал сбора: {self.interval_minutes} минут")
        
        # Показываем время первого запуска
        first_run = datetime.now() + timedelta(minutes=1)
        logger.info(f"⏰ Первый запуск: {first_run.strftime('%H:%M:%S')}")
    
    def stop(self):
        """Останавливает планировщик"""
        if not self.running:
            logger.warning("⚠️ Планировщик не запущен")
            return
        
        self.running = False
        schedule.clear()
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        logger.info("🛑 Планировщик остановлен")
    
    def _run_scheduler(self):
        """Основной цикл планировщика"""
        logger.info("🔄 Запуск основного цикла планировщика")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Проверяем каждые 30 секунд
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле планировщика: {e}")
                time.sleep(60)  # При ошибке ждем дольше
    
    def get_status(self):
        """Возвращает статус планировщика"""
        status = {
            'running': self.running,
            'interval_minutes': self.interval_minutes,
            'next_jobs': []
        }
        
        if self.running:
            # Получаем информацию о следующих задачах
            for job in schedule.jobs:
                status['next_jobs'].append({
                    'function': job.job_func.__name__,
                    'next_run': job.next_run.strftime('%H:%M:%S') if job.next_run else 'N/A'
                })
        
        return status

def run_scheduler_demo():
    """Демонстрация работы планировщика"""
    logger.info("🎯 Демонстрация работы планировщика")
    
    # Создаем планировщик с коротким интервалом для демонстрации
    scheduler = IVDataScheduler()
    
    # Запускаем планировщик
    scheduler.start()
    
    try:
        # Ждем несколько циклов для демонстрации
        logger.info("⏳ Ожидание выполнения задач...")
        
        # Ждем 3 минуты для демонстрации нескольких запусков
        for i in range(6):  # 6 * 30 секунд = 3 минуты
            time.sleep(30)
            
            # Показываем статус
            status = scheduler.get_status()
            logger.info(f"📊 Статус планировщика: {'Запущен' if status['running'] else 'Остановлен'}")
            
            if status['next_jobs']:
                for job in status['next_jobs']:
                    logger.info(f"📅 Следующая задача {job['function']}: {job['next_run']}")
    
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    
    finally:
        # Останавливаем планировщик
        scheduler.stop()
        logger.info("✅ Демонстрация завершена")

if __name__ == "__main__":
    # Демонстрация работы планировщика
    run_scheduler_demo()
