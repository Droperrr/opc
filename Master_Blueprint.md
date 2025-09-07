# Project Master Blueprint

## 1. High-Level Overview

## 2. Key Components & Logic Flow

#### 2.1.1 Точка входа: get_iv.py

Файл `get_iv.py` является основной точкой входа для сбора данных об имплицированной волатильности с биржи Bybit. 

#### 2.1.2 Процесс сбора данных

1. **Инициализация**: Скрипт загружает API-ключи из файла .env через библиотеку dotenv
2. **Получение списка символов**: Функция `get_option_symbols()` отправляет запрос к API Bybit (`/v5/market/instruments-info`) для получения списка доступных опционов SOL
3. **Сбор данных по каждому символу**: Функция `get_all_sol_options_data()` последовательно запрашивает данные для каждого символа через API Bybit (`/v5/market/tickers`)
4. **Обработка данных**: Для каждого символа извлекаются метрики (markIv, bid1Iv, ask1Iv, markPrice, underlyingPrice, греки)
5. **Сохранение данных**: 
   - В CSV файл `sol_all_options_iv.csv` (с добавлением к существующим данным)
   - В базу данных через функцию `db.insert_iv_data()`

#### 2.1.3 Поток данных

```
get_iv.py -> API Bybit -> Данные опционов -> sol_all_options_iv.csv + База данных
```
| get_iv.py | File | Точка входа для сбора данных об имплицированной волатильности с Bybit | 2025-09-06 |
| data/ | Directory | Директория для хранения данных, включая backtests, cache, experiments | 2025-09-05 |
| sol_all_options_iv.csv | File | Основной файл для хранения собранных данных об IV | 2025-09-06 |


### 2.1 Data Flow from Bybit

## 3. Data Structure

## 4. Detailed File & Folder Audit

### 4.1 Root Directory
| File/Directory | Type | Description | Last Modified |
|----------------|------|-------------|---------------|
|                |      |             |               |

### 4.2 Data Directory
| File/Directory | Type | Description | Last Modified |
|----------------|------|-------------|---------------|
|                |      |             |               |
