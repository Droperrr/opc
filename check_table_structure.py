import sqlite3

# Подключаемся к базе данных
conn = sqlite3.connect('server_opc.db')
cursor = conn.cursor()

# Получаем структуру таблицы iv_agg
cursor.execute("PRAGMA table_info(iv_agg);")
columns = cursor.fetchall()

print('Structure of iv_agg table:')
for column in columns:
    print(f"Column: {column[1]}, Type: {column[2]}, Not Null: {column[3]}, Default: {column[4]}, Primary Key: {column[5]}")

# Закрываем соединение
conn.close()