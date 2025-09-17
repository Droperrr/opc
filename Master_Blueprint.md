# Project Master Blueprint

## 1. High-Level Overview

OPC (Optimized Production Code) - это система для разработки, тестирования и оптимизации торговых стратегий на криптовалютных фьючерсах. Основной фокус проекта - реализация и бэктестинг MVP (Minimum Viable Product) стратегии, основанной на анализе базиса (разница между фьючерсной и спотовой ценой) и технических индикаторах.

Проект следует принципам итеративной разработки и научного подхода к торговле, где каждая гипотеза тщательно тестируется на исторических данных перед возможным переходом к live-торговле.

## 2. Key Components & Logic Flow

### 2.1. Data Layer

#### 2.1.1. Data Sources
- **Deribit API**: Основной источник исторических данных о сделках и рыночных данных.
- **Historical Data Files**: Локально сохраненные данные в формате CSV/Parquet для ускорения итераций.

#### 2.1.2. ETL Process (`etl_basis.py`)
1.  **Извлечение**: Загрузка сырых данных из источников.
2.  **Трансформация**: 
    - Агрегация сделок в свечи.
    - Расчет базиса (funding rate, basis value).
    - Расчет технических индикаторов (SMA, Z-score и др.).
3.  **Загрузка**: Сохранение обработанных данных в базу данных SQLite.

#### 2.1.3. Data Flow
```
Deribit API / CSV Files -> ETL (etl_basis.py) -> SQLite DB -> Feature Engineering
```

### 2.2. Feature Engineering Layer (`feature_builder.py`)

- Создание и обновление набора признаков (features) на основе обработанных данных.
- Признаки включают в себя как базовые метрики (цены, объемы), так и производные (скользящие средние, z-score, RSI и т.д.).
- Подготовка данных для генерации сигналов и бэктестинга.

### 2.3. Signal Generation Layer (`candle_signal_generator.py`)

- Алгоритмы для генерации торговых сигналов на основе свечей и признаков.
- Текущий фокус: стратегии, основанные на комбинации z-score и фильтров (например, SMA).

### 2.4. Optimization Layer (`optimizer.py`)

- Использование байесовской оптимизации (scikit-optimize) для настройки гиперпараметров стратегии.
- Определение оптимальных значений для фильтров, порогов сигналов и других параметров.

### 2.5. Backtesting Layer (`advanced_backtest_system.py`, `run_robust_strategy_backtest.py`)

- Мощный движок для тестирования стратегий на исторических данных.
- Поддержка walk-forward анализа.
- Расчет ключевых метрик эффективности (PnL, Sharpe Ratio, Drawdown, Win Rate и др.).

### 2.6. Analysis Layer

- Генерация отчетов об эффективности стратегий (`analysis_pro_strategy.md`).
- Сравнительный анализ различных подходов и параметров.

## 3. Data Structure

### 3.1. Raw Data
- `deribit_historical_trades/`: Директория с сырыми данными о сделках в формате CSV/Parquet.

### 3.2. Processed Data (Database)
- **Таблица `candles`**: Агрегированные свечи.
- **Таблица `basis_data`**: Расчетные значения базиса.
- **Таблица `features`**: Признаки, созданные `feature_builder.py`.

### 3.3. Backtest Results
- `backtest_results/`: Директория с результатами бэктестов.
- `analysis_pro_strategy.md`: Основной отчет по стратегии.

## 4. Detailed File & Folder Audit

### 4.1 Root Directory
| File/Directory | Type | Description | Last Modified |
|----------------|------|-------------|---------------|
| `README.md` | File | Основная документация проекта | 2025-09-17 |
| `PROJECT_MANIFEST.md` | File | Манифест проекта | 2025-09-17 |
| `Master_Blueprint.md` | File | Этот файл | 2025-09-17 |
| `requirements.txt` | File | Зависимости Python | 2025-09-17 |
| `.gitignore` | File | Исключения для Git | - |
| `etl_basis.py` | File | ETL-пайплайн для данных базиса | - |
| `feature_builder.py` | File | Построение признаков | - |
| `candle_signal_generator.py` | File | Генерация сигналов на основе свечей | - |
| `optimizer.py` | File | Оптимизация параметров стратегии | - |
| `run_robust_strategy_backtest.py` | File | Запуск бэктеста MVP стратегии | - |
| `advanced_backtest_system.py` | File | Продвинутая система бэктестинга | - |
| `analysis_pro_strategy.md` | File | Анализ результатов бэктеста | - |
| `deribit_trades_collector.py` | File | Сбор исторических сделок с Deribit | - |
| `database.py` | File | Работа с базой данных | - |
| `deribit_historical_trades/` | Directory | Директория с историческими данными | - |
| `backtest_results/` | Directory | Директория с результатами бэктестов | - |
| `archive/` | Directory | Директория с архивными скриптами | 2025-09-17 |

### 4.2 Data Directory (`deribit_historical_trades/`)
| File/Directory | Type | Description | Last Modified |
|----------------|------|-------------|---------------|
| `*.csv` | File | Исторические данные о сделках | - |

### 4.3 Backtest Results Directory (`backtest_results/`)
| File/Directory | Type | Description | Last Modified |
|----------------|------|-------------|---------------|
| `*.json` | File | Результаты бэктестов в формате JSON | - |
| `*.png` | File | Графики и визуализации результатов | - |
| `analysis_pro_strategy.md` | File | Основной отчет по стратегии | - |
