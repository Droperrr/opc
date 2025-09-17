import pandas as pd
import numpy as np

def calculate_features():
    """
    Читает базовый датасет и вычисляет инженерные признаки.
    """
    # Чтение базового датасета
    df = pd.read_parquet('basis_raw_data_1m_btc.parquet')
    
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
    
    # Вывод первых 65 строк для проверки
    print("\nДанные с признаками (первые 65 строк):")
    print(df.head(65))
    
    # Добавляем колонку для идентификации актива
    df['asset'] = 'BTC'
    
    # Сохранение в новый Parquet-файл
    df.to_parquet('basis_features_1m_btc.parquet', index=False)
    print("\nПризнаки сохранены в basis_features_1m_btc.parquet")

def main():
    """
    Основная функция для выполнения расчета признаков.
    """
    calculate_features()

if __name__ == "__main__":
    main()