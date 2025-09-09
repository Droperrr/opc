import sqlite3

# Подключаемся к базе данных
conn = sqlite3.connect('server_opc.db')
cursor = conn.cursor()

# Получаем список всех таблиц
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print('Tables in database:')
for table in tables:
    print(table[0])

# Закрываем соединение
conn.close()
