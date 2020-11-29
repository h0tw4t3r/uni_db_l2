import psycopg2

# Закрити підключення до бази даних
def disconnect_from_db(conn):
    if conn is not None:
    conn.close()

# Підключення до бази даних
def connect_to_db(parameters = None):
    if parameters is None:
    conn = psycopg2.connect(database = 'postgres', user = 'postgres',
        password = 'qweasdzxc', host = 'localhost')

else :
    conn = psycopg2.connect(parameters)

return conn



# Створити новий запис у таблиці
def create_item(conn, tbl_name, columns, item):

    query = 'INSERT INTO ' + '"' + tbl_name + '" ('
query += '"' + columns[0] + '"'

for column in range(1, len(columns)):
    query += ', "' + str(columns[column]) + '"'

query += ') VALUES('

query += "'" + str(item[0]) + "'"
for field in range(1, len(item)):
    query += ", '" + str(item[field]) + "'"

query += ")"

cursor = conn.cursor()
cursor.execute(query)
conn.commit()


# Отримати назви стовпців для таблиці
def get_columns(conn, table_name):
    query = 'SELECT * FROM "' + table_name + '" LIMIT 0'
cursor = conn.cursor()
cursor.execute(query)
columns = [desc[0]
    for desc in cursor.description
]
return columns




# Прочитати запис з таблиці
def read_item(conn, tbl_name, columns = None, item_id = None):
    if columns is None:
    query = 'SELECT * FROM "' + tbl_name + '"'
columns = get_columns(conn, tbl_name)
else :
    pass
if item_id is not None:
    query += ' WHERE "' + columns[0] + '" = ' + str(item_id)
cursor = conn.cursor()
cursor.execute(query)
return cursor.fetchone()


# Створити декілька записів у табилці
def create_items(conn, tbl_name, columns, items):
    for item in items:
    create_item(conn, tbl_name, columns, item)

# Прочитати декілька записів у таблиці
def read_items(conn, tbl_name, columns = None):
    if columns is None:
    query = 'SELECT * FROM "' + tbl_name + '"'
cursor = conn.cursor()
cursor.execute(query)
else :
    pass
return cursor.fetchall()


# Видалити запис з бази даних
def delete_item(conn, tbl_name, columns, item_id):
    query = 'DELETE FROM "' + tbl_name + '" WHERE '
query += '"' + columns[0] + '" = ' + str(item_id)
cursor = conn.cursor()
cursor.execute(query)
conn.commit()



# Оновити дані у записі
def update_item(conn, tbl_name, columns, item, item_id):
    query = 'UPDATE "' + tbl_name + '" SET '
query += '"' + columns[0] + '" = ' + "'" + str(item[0]) + "'"

for field in range(1, len(columns)):
    query += ', "' + columns[field] + '" = ' + "'" + str(item[field]) + "'"

query += ' WHERE "' + columns[0] + '" = ' + str(item_id)
cursor = conn.cursor()
cursor.execute(query)
conn.commit()

# Тестування функцій
def main():
    connection = psycopg2.connect(database = 'postgres', user = 'postgres',
        password = 'qweasdzxc', host = 'localhost')

cursor = connection.cursor()

columns = ['testID', 'num', 'txt']
item = [10, 65, 'Fan']
tbl_name = "test"
try:
item0 = read_item(connection, tbl_name, None, 10)
print(item0)

except(Exception, psycopg2.DatabaseError) as error:
    print('Error')
print(error)

finally:
if connection is not None:
    connection.close()


if __name__ == '__main__':
    main()