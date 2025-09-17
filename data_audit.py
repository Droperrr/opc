#!/usr/bin/env python3
import sqlite3
import pandas as pd
from datetime import datetime

def get_db_connection(db_path='server_opc.db'):
    """Создает подключение к базе данных"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def get_general_summary(conn):
    """Получает общую сводку по данным"""
    try:
        cursor = conn.cursor()
        
        # Общее количество сделок для BTC и SOL
        cursor.execute("""
            SELECT 
                SUBSTR(instrument_name, 1, 3) AS currency, 
                COUNT(*) AS total_trades,
                COUNT(DISTINCT instrument_name) AS unique_contracts,
                MIN(datetime(timestamp / 1000, 'unixepoch')) AS first_trade,
                MAX(datetime(timestamp / 1000, 'unixepoch')) AS last_trade
            FROM deribit_option_trades 
            GROUP BY currency
        """)
        
        results = cursor.fetchall()
        
        print("Общая сводка:")
        print("-" * 80)
        print(f"{'Валюта':<10} {'Сделок':<15} {'Контрактов':<15} {'Первый трейд':<20} {'Последний трейд':<20}")
        print("-" * 80)
        
        summary = {}
        for row in results:
            currency, total_trades, unique_contracts, first_trade, last_trade = row
            summary[currency] = {
                'total_trades': total_trades,
                'unique_contracts': unique_contracts,
                'first_trade': first_trade,
                'last_trade': last_trade
            }
            print(f"{currency:<10} {total_trades:<15} {unique_contracts:<15} {first_trade:<20} {last_trade:<20}")
        
        return summary
    except Exception as e:
        print(f"Ошибка при получении общей сводки: {e}")
        return {}

def get_monthly_stats(conn):
    """Получает статистику по месяцам"""
    try:
        cursor = conn.cursor()
        
        # Получение данных по месяцам для BTC и SOL
        cursor.execute("""
            SELECT
                SUBSTR(instrument_name, 1, 3) AS currency,
                strftime('%Y-%m', datetime(timestamp / 1000, 'unixepoch')) AS month,
                COUNT(*) AS trades_count
            FROM deribit_option_trades
            GROUP BY currency, month
            ORDER BY currency, month
        """)
        
        results = cursor.fetchall()
        
        # Организуем данные в словарь
        monthly_stats = {}
        for row in results:
            currency, month, trades_count = row
            if currency not in monthly_stats:
                monthly_stats[currency] = {}
            monthly_stats[currency][month] = trades_count
        
        return monthly_stats
    except Exception as e:
        print(f"Ошибка при получении статистики по месяцам: {e}")
        return {}

def print_monthly_table(monthly_stats):
    """Выводит таблицу статистики по месяцам"""
    print("\nАктивность по месяцам:")
    print("-" * 40)
    
    # Получим все уникальные месяцы
    all_months = set()
    for currency_data in monthly_stats.values():
        all_months.update(currency_data.keys())
    all_months = sorted(list(all_months))
    
    # Выводим заголовок таблицы
    header = f"{'Месяц':<10}"
    for currency in ['BTC', 'SOL']:
        header += f" {currency:>10}"
    print(header)
    print("-" * len(header))
    
    # Выводим данные по месяцам
    for month in all_months:
        row = f"{month:<10}"
        for currency in ['BTC', 'SOL']:
            count = monthly_stats.get(currency, {}).get(month, 0)
            row += f" {count:>10}"
        print(row)

def print_text_chart(monthly_stats):
    """Выводит текстовый график активности по месяцам"""
    print("\nВизуализация активности:")
    print("-" * 40)
    
    for currency in ['BTC', 'SOL']:
        if currency in monthly_stats:
            print(f"\n{currency}:")
            data = monthly_stats[currency]
            if data:
                # Определяем максимальное значение для масштабирования
                max_value = max(data.values())
                scale = 20 / max_value if max_value > 0 else 1
                
                # Выводим график
                for month, count in sorted(data.items()):
                    bar_length = int(count * scale)
                    bar = '#' * bar_length
                    print(f"{month}: {bar} ({count})")

def detect_anomalies(monthly_stats):
    """Обнаруживает аномалии в данных по месяцам"""
    try:
        anomalies = {}
        
        for currency, data in monthly_stats.items():
            if not data:
                continue
                
            # Получаем значения количества сделок
            counts = list(data.values())
            
            # Вычисляем среднее и стандартное отклонение
            mean = sum(counts) / len(counts)
            variance = sum((x - mean) ** 2 for x in counts) / len(counts)
            std_dev = variance ** 0.5
            
            # Определяем пороги для аномалий (среднее ± 1 * стандартное отклонение)
            lower_threshold = mean - std_dev
            upper_threshold = mean + std_dev
            
            # Отладочный вывод
            print(f"\n{currency}:")
            print(f"  Среднее: {mean:.2f}")
            print(f"  Стандартное отклонение: {std_dev:.2f}")
            print(f"  Нижний порог аномалии: {lower_threshold:.2f}")
            print(f"  Верхний порог аномалии: {upper_threshold:.2f}")
            
            # Находим аномальные месяцы
            currency_anomalies = []
            for month, count in sorted(data.items()):
                is_low_anomaly = count < lower_threshold
                is_high_anomaly = count > upper_threshold
                is_anomaly = is_low_anomaly or is_high_anomaly
                anomaly_type = "низкая активность" if is_low_anomaly else ("высокая активность" if is_high_anomaly else "норма")
                print(f"  {month}: {count} ({anomaly_type})")
                if is_anomaly:
                    currency_anomalies.append({
                        'month': month,
                        'count': count,
                        'mean': mean,
                        'std_dev': std_dev,
                        'lower_threshold': lower_threshold,
                        'upper_threshold': upper_threshold
                    })
            
            if currency_anomalies:
                anomalies[currency] = currency_anomalies
        
        return anomalies
    except Exception as e:
        print(f"Ошибка при обнаружении аномалий: {e}")
        return {}

def generate_hypotheses(anomalies):
    """Генерирует гипотезы для аномалий"""
    hypotheses = {}
    
    for currency, currency_anomalies in anomalies.items():
        currency_hypotheses = []
        for anomaly in currency_anomalies:
            month = anomaly['month']
            count = anomaly['count']
            
            # Формулируем гипотезы в зависимости от месяца и значения
            # Проверяем, является ли аномалия низкой или высокой активностью
            is_low_activity = count < 1000  # Примерный порог для низкой активности
            is_high_activity = count > 1000  # Примерный порог для высокой активности
            
            if month.endswith('-01') or month.endswith('-02') or month.endswith('-12'):
                if is_low_activity:
                    hypothesis = f"Сезонный спад. Январь, февраль и декабрь часто характеризуются пониженной торговой активностью на рынках криптовалют."
                else:
                    hypothesis = f"Сезонный рост. Январь, февраль и декабрь могут характеризоваться повышенной торговой активностью из-за начала нового года и рыночных движений."
            elif month.endswith('-07') or month.endswith('-08'):
                if is_low_activity:
                    hypothesis = f"Летний спад. Июль и август часто характеризуются пониженной торговой активностью из-за отпусков и низкой ликвидности."
                else:
                    hypothesis = f"Летний рост. Июль и август могут характеризоваться повышенной торговой активностью из-за рыночных движений в летний период."
            elif count < 100:
                hypothesis = f"Критически низкая активность. Возможно, технические проблемы с API или отсутствие ликвидности в этот период."
            elif is_low_activity:
                hypothesis = f"Пониженная активность. Причины могут быть связаны с рыночными условиями или внешними факторами, влияющими на интерес к опционам {currency} в этот период."
            elif is_high_activity:
                hypothesis = f"Повышенная активность. Причины могут быть связаны с рыночными событиями, новостями или высокой волатильностью, влияющими на интерес к опционам {currency} в этот период."
            else:
                hypothesis = f"Нормальная активность. Уровень торговой активности соответствует среднему значению для этого актива."
            
            currency_hypotheses.append({
                'month': month,
                'count': count,
                'hypothesis': hypothesis
            })
        
        if currency_hypotheses:
            hypotheses[currency] = currency_hypotheses
    
    return hypotheses

def generate_markdown_report(summary, monthly_stats, anomalies, hypotheses, output_file='data_audit_report.md'):
    """Генерирует отчет в формате Markdown"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write("# Аудит Данных Deribit\n\n")
            
            # Общая сводка
            f.write("## 1. Общая Сводка\n\n")
            f.write("| Метрика | BTC | SOL |\n")
            f.write("|---|---|---|\n")
            f.write(f"| Общее кол-во сделок | {summary.get('BTC', {}).get('total_trades', 0):,} | {summary.get('SOL', {}).get('total_trades', 0):,} |\n")
            f.write(f"| Уникальных контрактов | {summary.get('BTC', {}).get('unique_contracts', 0):,} | {summary.get('SOL', {}).get('unique_contracts', 0):,} |\n")
            f.write(f"| Первый трейд | {summary.get('BTC', {}).get('first_trade', 'N/A')} | {summary.get('SOL', {}).get('first_trade', 'N/A')} |\n")
            f.write(f"| Последний трейд | {summary.get('BTC', {}).get('last_trade', 'N/A')} | {summary.get('SOL', {}).get('last_trade', 'N/A')} |\n\n")
            
            # Активность по месяцам
            f.write("## 2. Активность по Месяцам\n\n")
            
            # Таблица по месяцам
            f.write("| Месяц | Сделки BTC | Сделки SOL |\n")
            f.write("|---|---|---|\n")
            
            # Получим все уникальные месяцы
            all_months = set()
            for currency_data in monthly_stats.values():
                all_months.update(currency_data.keys())
            all_months = sorted(list(all_months))
            
            # Выводим данные по месяцам
            for month in all_months:
                btc_count = monthly_stats.get('BTC', {}).get(month, 0)
                sol_count = monthly_stats.get('SOL', {}).get(month, 0)
                f.write(f"| {month} | {btc_count:,} | {sol_count:,} |\n")
            
            f.write("\n")
            
            # Визуализация активности
            f.write("### Визуализация Активности\n\n")
            
            for currency in ['BTC', 'SOL']:
                if currency in monthly_stats:
                    f.write(f"**{currency}:**\n\n")
                    data = monthly_stats[currency]
                    if data:
                        # Определяем максимальное значение для масштабирования
                        max_value = max(data.values())
                        scale = 20 / max_value if max_value > 0 else 1
                        
                        # Выводим график
                        for month, count in sorted(data.items()):
                            bar_length = int(count * scale)
                            bar = '#' * bar_length
                            f.write(f"{month}: {bar} ({count:,})\n")
                    f.write("\n")
            
            # Анализ аномалий
            f.write("## 3. Анализ Аномалий\n\n")
            
            if not anomalies:
                f.write("Аномалии не обнаружены. Все месяцы имеют нормальный уровень активности.\n")
            else:
                f.write("Выявлены следующие месяцы со значительно пониженной активностью:\n\n")
                
                for currency, currency_anomalies in anomalies.items():
                    f.write(f"### {currency}\n\n")
                    currency_hypotheses = hypotheses.get(currency, [])
                    
                    for i, anomaly in enumerate(currency_anomalies):
                        month = anomaly['month']
                        count = anomaly['count']
                        
                        # Находим соответствующую гипотезу
                        hypothesis = "Гипотеза не сформулирована."
                        if i < len(currency_hypotheses):
                            hypothesis = currency_hypotheses[i]['hypothesis']
                        
                        f.write(f"* **{currency}, {month}:** Количество сделок ({count:,}) значительно отличается от нормального уровня.\n")
                        f.write(f"  * **Гипотеза:** {hypothesis}\n\n")
            
            f.write("---\n")
            f.write(f"Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print(f"Отчет успешно сохранен в файл: {output_file}")
        return True
    except Exception as e:
        print(f"Ошибка при генерации отчета: {e}")
        return False

def main():
    """Основная функция"""
    print("Аудит данных Deribit")
    print("=" * 80)
    print(f"Дата выполнения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Подключение к базе данных
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        # Получение общей сводки
        summary = get_general_summary(conn)
        
        # Получение статистики по месяцам
        monthly_stats = get_monthly_stats(conn)
        
        # Вывод таблицы по месяцам
        print_monthly_table(monthly_stats)
        
        # Вывод текстового графика
        print_text_chart(monthly_stats)
        
        # Обнаружение аномалий
        anomalies = detect_anomalies(monthly_stats)
        
        # Генерация гипотез
        hypotheses = generate_hypotheses(anomalies)
        
        # Генерация Markdown-отчета
        generate_markdown_report(summary, monthly_stats, anomalies, hypotheses)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()