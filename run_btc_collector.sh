#!/bin/bash
# Скрипт запуска сборщика данных для BTC

# Путь к Python и скрипту сборщика
PYTHON_PATH=$(which python3)
COLLECTOR_SCRIPT="deribit_trades_collector.py"

# Параметры запуска
CURRENCY="BTC"
START_DATE="2023-01-01"
END_DATE="2023-12-31"
LOG_FILE="btc_collector.log"

# Запуск сборщика данных с сохранением лога
$PYTHON_PATH $COLLECTOR_SCRIPT --currency $CURRENCY --start $START_DATE --end $END_DATE | tee $LOG_FILE