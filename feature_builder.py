import pandas as pd
import numpy as np

def calculate_features():
    """
    Читает базовый датасет и вычисляет инженерные признаки.
    """
    # Чтение базового датасета
    df = pd.read_parquet('basis_raw_data_1m.parquet')
    
    print("Исходные данные:")
    print(df.head())
    print(df.info())
    
    # Расчет z-scores
    window = 60
    
    # Z-score для basis_relative
    df['basis_z_score'] = (df['basis_relative'] - df['basis_relative'].rolling(window=window).mean()) / df['basis_relative'].rolling(window=window).std()
    # Заменяем inf и -inf на NaN, затем NaN на 0
    df['basis_z_score'] = df['basis_z_score'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Z-score для funding_rate
    df['funding_z_score'] = (df['funding_rate'] - df['funding_rate'].rolling(window=window).mean()) / df['funding_rate'].rolling(window=window).std()
    # Заменяем inf и -inf на NaN, затем NaN на 0
    df['funding_z_score'] = df['funding_z_score'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Комбинированный z-score
    df['combined_z_score'] = df['basis_z_score'] - df['funding_z_score']
    
    # Расчет historical_volatility_24h (24 часа = 1440 минут)
    # Сначала рассчитываем логарифмические доходности
    df['returns'] = np.log(df['spot_price'] / df['spot_price'].shift(1))
    # Затем скользящее стандартное отклонение за 1440 периодов
    df['historical_volatility_24h'] = df['returns'].rolling(window=1440).std()
    # Заменяем inf и -inf на NaN, затем NaN на 0
    df['historical_volatility_24h'] = df['historical_volatility_24h'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Новый признак: z_score_momentum_5m
    # Скорость изменения combined_z_score за последние 5 минут (5 свечей по 1 минуте)
    df['z_score_momentum_5m'] = df['combined_z_score'] - df['combined_z_score'].shift(5)
    # Заменяем inf и -inf на NaN, затем NaN на 0
    df['z_score_momentum_5m'] = df['z_score_momentum_5m'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Новый признак: order_book_imbalance (OBI)
    # TODO: В будущем здесь будут реальные данные order book из базы данных
    # Пока добавляем заглушку со случайными значениями для демонстрации
    # Формула: (bid_volume - ask_volume) / (bid_volume + ask_volume)
    np.random.seed(42)  # Для воспроизводимости
    df['order_book_imbalance'] = np.random.uniform(-1, 1, len(df))
    
    # Вывод первых 65 строк для проверки
    print("\nДанные с признаками (первые 65 строк):")
    print(df.head(65))
    
    # Добавляем колонку для идентификации актива
    df['asset'] = 'SOL'
    
    # Сохранение в новый Parquet-файл
    df.to_parquet('basis_features_pro_1m.parquet', index=False)
    print("\nПризнаки сохранены в basis_features_pro_1m.parquet")
    
    # Показываем информацию о новых признаках
    print("\nНовые признаки:")
    print(f"z_score_momentum_5m: min={df['z_score_momentum_5m'].min():.4f}, max={df['z_score_momentum_5m'].max():.4f}")
    print(f"order_book_imbalance: min={df['order_book_imbalance'].min():.4f}, max={df['order_book_imbalance'].max():.4f}")

def calculate_mean_reversion_features():
    """
    Читает базовый датасет и вычисляет признаки для стратегии Mean Reversion.
    """
    # Чтение базового датасета
    df = pd.read_parquet('basis_raw_data_1m.parquet')
    
    print("Исходные данные для Mean Reversion:")
    print(df.head())
    print(df.info())
    
    # Новые признаки для Mean Reversion
    # 1. spread: Абсолютная разница (futures_price - spot_price)
    df['spread'] = df['futures_price'] - df['spot_price']
    
    # 2. rolling_mean_spread_60m: Скользящее среднее spread за последний час (60 минут)
    df['rolling_mean_spread_60m'] = df['spread'].rolling(window=60).mean()
    
    # 3. rolling_std_spread_60m: Скользящее стандартное отклонение spread за последний час
    df['rolling_std_spread_60m'] = df['spread'].rolling(window=60).std()
    
    # 4. z_score_spread: Z-score, рассчитанный на основе этих новых метрик
    df['z_score_spread'] = (df['spread'] - df['rolling_mean_spread_60m']) / df['rolling_std_spread_60m']
    
    # Заменяем inf и -inf на NaN, затем NaN на 0
    df['rolling_mean_spread_60m'] = df['rolling_mean_spread_60m'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df['rolling_std_spread_60m'] = df['rolling_std_spread_60m'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df['z_score_spread'] = df['z_score_spread'].replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Вывод первых 65 строк для проверки
    print("\nДанные с признаками Mean Reversion (первые 65 строк):")
    print(df.head(65))
    
    # Добавляем колонку для идентификации актива
    df['asset'] = 'SOL'
    
    # Сохранение в новый Parquet-файл
    df.to_parquet('features_mean_reversion.parquet', index=False)
    print("\nПризнаки Mean Reversion сохранены в features_mean_reversion.parquet")
    
    # Показываем информацию о новых признаках
    print("\nНовые признаки для Mean Reversion:")
    print(f"spread: min={df['spread'].min():.4f}, max={df['spread'].max():.4f}")
    print(f"rolling_mean_spread_60m: min={df['rolling_mean_spread_60m'].min():.4f}, max={df['rolling_mean_spread_60m'].max():.4f}")
    print(f"rolling_std_spread_60m: min={df['rolling_std_spread_60m'].min():.4f}, max={df['rolling_std_spread_60m'].max():.4f}")
    print(f"z_score_spread: min={df['z_score_spread'].min():.4f}, max={df['z_score_spread'].max():.4f}")

def main():
    """
    Основная функция для выполнения расчета признаков.
    """
    calculate_features()
    print("\n" + "="*50 + "\n")
    calculate_mean_reversion_features()

if __name__ == "__main__":
    main()