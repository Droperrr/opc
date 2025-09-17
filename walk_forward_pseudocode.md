# Логика Walk-Forward Optimization

## Определение параметров

- `total_data`: Полный датасет `basis_features_1m.parquet`, отсортированный по времени.
- `train_window_size`: Размер окна для "обучения" (например, 21 день).
- `test_window_size`: Размер окна для "тестирования" (например, 7 дней).
- `step_size`: Шаг, с которым мы сдвигаем окна (например, 7 дней).

## Псевдокод

```
# Загрузка полных данных
total_data = load_parquet('basis_features_1m.parquet')
total_data = sort_by_time(total_data)

# Инициализация начальной даты
start_date = min(total_data['time'])
end_date = max(total_data['time'])

# Определение параметров окон (в днях)
train_window_size_days = 21
test_window_size_days = 7
step_size_days = 7

# Преобразование дат в datetime, если они еще не в этом формате
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Основной цикл Walk-Forward
current_date = start_date

while current_date + pd.Timedelta(days=train_window_size_days + test_window_size_days) <= end_date:
    # Определяем границы обучающего окна
    train_start = current_date
    train_end = current_date + pd.Timedelta(days=train_window_size_days)
    
    # Определяем границы тестового окна
    test_start = train_end
    test_end = train_end + pd.Timedelta(days=test_window_size_days)
    
    # Извлекаем обучающие и тестовые данные
    train_data = total_data[(total_data['time'] >= train_start) & (total_data['time'] < train_end)]
    test_data = total_data[(total_data['time'] >= test_start) & (total_data['time'] < test_end)]
    
    # Проверяем, что оба набора данных не пусты
    if not train_data.empty and not test_data.empty:
        print(f"Processing Train: {train_start} - {train_end}, Test: {test_start} - {test_end}")
        
        # Здесь будет логика оптимизации параметров на train_data
        # Например, найти оптимальные пороги для сигналов
        # optimized_params = optimize_parameters(train_data)
        
        # Здесь будет логика генерации сигналов на test_data с использованием optimized_params
        # signals = generate_signals(test_data, optimized_params)
        
        # Здесь будет логика бэктестинга на test_data с сигналами
        # results = run_backtest(test_data, signals)
        
        # Сохраняем или агрегируем результаты
        # aggregate_results(results)
    
    # Сдвигаем окно
    current_date += pd.Timedelta(days=step_size_days)

print("Walk-Forward Optimization завершена.")
```

## Пояснения

1.  **Инициализация**: Загружаем полный датасет и определяем параметры окон.
2.  **Цикл**: Итерируемся по датам, выделяя обучающее и тестовое окно.
3.  **Обучение**: На обучающем окне можно оптимизировать параметры стратегии (в данном случае это может быть оптимизация порогов для генерации сигналов на основе `combined_z_score`).
4.  **Тестирование**: На тестовом окне генерируем сигналы с использованием оптимизированных параметров и запускаем бэктест.
5.  **Сдвиг**: Сдвигаем начальную дату на `step_size_days` и повторяем процесс.