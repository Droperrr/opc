#!/usr/bin/env python3
"""
Скрипт для обучения модели RandomForestRegressor для прогнозирования IV
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('train_iv_model.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_and_prepare_data(file_path='ml_dataset_2023.parquet'):
    """
    Загружает и подготавливает данные для обучения
    
    Args:
        file_path (str): Путь к файлу с данными
        
    Returns:
        tuple: (X, y) - признаки и целевая переменная
    """
    logger.info(f"Загрузка данных из {file_path}")
    
    # Загрузка данных
    df = pd.read_parquet(file_path)
    
    logger.info(f"Загружено {len(df)} записей")
    logger.info(f"Колонки: {list(df.columns)}")
    
    # Определение целевой переменной
    target = 'iv_30d'
    
    # Определение признаков (исключаем целевую переменную и нечисловые колонки)
    feature_columns = [
        'spot_price', 'volume', 'spot_price_iv', 'skew_30d', 'basis_rel', 
        'confidence', 'trend_iv_30d', 'trend_skew_30d', 'spot_ma_7', 'spot_ma_30', 
        'iv_ma_7', 'iv_ma_30', 'spot_volatility_7', 'spot_volatility_30', 
        'spot_lag_1', 'spot_lag_2', 'spot_lag_3', 'iv_lag_1', 'iv_lag_2', 
        'spot_pct_change', 'iv_pct_change', 'rsi'
    ]
    
    # Проверка наличия всех колонок
    missing_features = [col for col in feature_columns if col not in df.columns]
    if missing_features:
        logger.warning(f"Отсутствующие признаки: {missing_features}")
        feature_columns = [col for col in feature_columns if col in df.columns]
    
    # Удаление строк с NaN значениями
    df_clean = df.dropna(subset=[target] + feature_columns)
    
    logger.info(f"После очистки осталось {len(df_clean)} записей")
    
    # Подготовка признаков и целевой переменной
    X = df_clean[feature_columns]
    y = df_clean[target]
    
    logger.info(f"Размерность признаков: {X.shape}")
    logger.info(f"Размерность целевой переменной: {y.shape}")
    
    return X, y

def train_model(X, y):
    """
    Обучает модель RandomForestRegressor
    
    Args:
        X (pd.DataFrame): Признаки
        y (pd.Series): Целевая переменная
        
    Returns:
        tuple: (model, X_test, y_test) - обученная модель и тестовые данные
    """
    logger.info("Разделение данных на обучающую и тестовую выборки")
    
    # Разделение данных
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )
    
    logger.info(f"Размер обучающей выборки: {X_train.shape}")
    logger.info(f"Размер тестовой выборки: {X_test.shape}")
    
    # Создание и обучение модели
    logger.info("Создание и обучение модели RandomForestRegressor")
    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    logger.info("Модель успешно обучена")
    
    return model, X_test, y_test

def evaluate_model(model, X_test, y_test):
    """
    Оценивает качество модели
    
    Args:
        model: Обученная модель
        X_test (pd.DataFrame): Тестовые признаки
        y_test (pd.Series): Тестовая целевая переменная
    """
    logger.info("Оценка качества модели")
    
    # Предсказания на тестовой выборке
    y_pred = model.predict(X_test)
    
    # Вычисление метрик
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    logger.info(f"Mean Squared Error on test set: {mse:.6f}")
    logger.info(f"Mean Absolute Error on test set: {mae:.6f}")
    logger.info(f"R² Score on test set: {r2:.6f}")
    
    # Важность признаков
    feature_importance = pd.DataFrame({
        'feature': model.feature_names_in_,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("Топ-10 самых важных признаков:")
    for i, row in feature_importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.4f}")
    
    return y_pred

def save_model(model, file_path='iv_prediction_model.joblib'):
    """
    Сохраняет модель в файл
    
    Args:
        model: Обученная модель
        file_path (str): Путь к файлу для сохранения
    """
    logger.info(f"Сохранение модели в {file_path}")
    joblib.dump(model, file_path)
    logger.info("Модель успешно сохранена")

def main():
    """Основная функция обучения модели"""
    logger.info("Запуск обучения модели для прогнозирования IV")
    
    try:
        # 1. Загрузка и подготовка данных
        X, y = load_and_prepare_data()
        
        # 2. Обучение модели
        model, X_test, y_test = train_model(X, y)
        
        # 3. Оценка качества модели
        y_pred = evaluate_model(model, X_test, y_test)
        
        # 4. Сохранение модели
        save_model(model)
        
        logger.info("Обучение модели успешно завершено")
        
    except Exception as e:
        logger.error(f"Ошибка при обучении модели: {e}")
        raise

if __name__ == "__main__":
    main()