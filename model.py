import psycopg2

import basic_db_backend
from exec_time import *


class ModelPostgre(object):
    def __init__(self, conn_parameters=None):
        self._conn = basic_db_backend.connect_to_db(conn_parameters)
        self._tables = []
        self._foreign_keys = []

    # Властивість, що містить назви таблиць
    @property
    def tables(self):
        return self._tables

    # Додати таблиці до моделі
    def add_tables(self, tables):
        self._tables = tables

    # Додати дані про зовнішній ключ
    def add_foreign_key(self, key_parameters):
        if len(key_parameters) != 4:
            return
        if (key_parameters.get('fk_table') is not None
                and key_parameters.get('fk_column') is not None
                and key_parameters.get('ref_table') is not None
                and key_parameters.get('ref_column')):
            self._foreign_keys.append(key_parameters)

    # Властивість, що містить дані про зовнішні ключі
    @property
    def foreign_keys(self):
        return self._foreign_keys

    # Взяти назви стовпів з таблиці
    def get_columns(self, table_name):
        return basic_db_backend.get_columns(self._conn, table_name)

    # Взяти тип стовпця з таблиці
    def get_column_type(self, table, column_name):

        cursor = self._conn.cursor()
        cursor.execute("SELECT column_name, data_type FROM information_schema.columns "
                       "WHERE table_name = '{}'".format(table))

        for col in cursor.fetchall():
            if column_name in col:
                return col[1]

        print(cursor.fetchall())

    # Взяти типи стовпців з таблиці
    def get_column_types(self, table):

        cursor = self._conn.cursor()
        cursor.execute("SELECT data_type FROM information_schema.columns "
                       "WHERE table_name = '{}'".format(table))

        columns_types = [col_type[0] for col_type in cursor.fetchall()]
        return columns_types

    # Генерувати числові дані
    def generate_numbers(self, quantity, max_value):
        query = 'SELECT trunc(random()*{0})::int from generate_series({1},{2})'.format(max_value, 1, quantity)
        cursor = self._conn.cursor()
        cursor.execute(query)
        numbers = [num[0] for num in cursor.fetchall()]
        return numbers

   

    # Генерувати дані, що містять дату
    def generate_date(self, quantity, days=90, shift=0, start_date=None):
        if start_date is None:
            start_date = "NOW()"
        query = "select to_char(NOW() + (random() * (NOW() + '{0} days' - NOW())) + '{1} days', 'DD/MM/YYYY') " \
                "from generate_series({2},{3})" \
            .format(days, shift, 1, quantity)
        cursor = self._conn.cursor()
        cursor.execute(query)
        dates = [date0[0] for date0 in cursor.fetchall()]
        return dates

 # Генерувати рядкові дані
    def generate_str(self, quantity, str_len):
        if str_len <= 0:
            return ''
        op = 'chr(trunc(65 + random()*25)::int)'
        parameters = op
        for i in range(1, str_len):
            parameters += ' || ' + op

        query = 'SELECT {0} from generate_series({1},{2})'.format(parameters, 1, quantity)
        cursor = self._conn.cursor()
        cursor.execute(query)
        str_res = [str0[0] for str0 in cursor.fetchall()]
        return str_res

    # Фільми, що будуть показувати після дати у кінотеатрі
    @timer
    def search_query1(self, after_date, cinema):
        query = 'SELECT f_name, f_genre, start_date, hall_name ' \
                'FROM "Films", "Sessions", "Cinema_Session", "Cinemas" ' \
                'WHERE "Films"."FilmID" = "Sessions"."FilmID"' \
                'AND "Sessions"."SessionID" = "Cinema_Session"."SessionID"' \
                'AND "Cinema_Session"."CinemaID" = "Cinemas"."CinemaID"' \
                'AND "Cinemas".c_name = \'{}\' AND "Sessions".start_date > \'{}\'' \
            .format(cinema, after_date)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        return [desc[0] for desc in cursor.description], cursor.fetchall()

    # Фільми, що будуть показувати після дати та тривалістю у рамках заданого
    @timer
    def search_query2(self, after_date, min_duration, max_duration):
        query = 'SELECT f_name, f_duration, start_date, c_name ' \
                'FROM "Films", "Sessions", "Cinema_Session", "Cinemas" ' \
                'WHERE "Films"."FilmID" = "Sessions"."FilmID" ' \
                'AND "Sessions"."SessionID" = "Cinema_Session"."SessionID" ' \
                'AND "Cinema_Session"."CinemaID" = "Cinemas"."CinemaID" ' \
                'AND "Sessions"."start_date" > \'{}\' ' \
                'AND "Films".f_duration BETWEEN {} AND {}' \
            .format(after_date, min_duration, max_duration)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        return [desc[0] for desc in cursor.description], cursor.fetchall()

    # Фільми, назви яких містять словосполучення та відповідного жанру
    @timer
    def search_query3(self, str_seq, str_genre):
        query = 'SELECT f_name, f_genre, f_duration, release_year ' \
                'FROM "Films"' \
                'WHERE ' \
                '"Films".f_name LIKE \'%{}%\' ' \
                'AND "Films".f_genre LIKE \'%{}%\'' \
            .format(str_seq, str_genre)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        return [desc[0] for desc in cursor.description], cursor.fetchall()

    def roll_back(self):
        self._conn.rollback()

    # Створити запис
    def create_item(self, table_name, columns, item):
        basic_db_backend.create_item(self._conn, table_name, columns, item)

    # Створити декілька записів
    def create_items(self, table_name, columns, items):
        basic_db_backend.create_items(self._conn, table_name, columns, items)

    # Взяти дані про запис з бази
    def read_item(self, table_name, columns, item_id):
        return basic_db_backend.read_item(self._conn, table_name, columns, item_id)

    # Прочитати дані таблиці з бази
    def read_items(self, table_name, columns):
        return basic_db_backend.read_items(self._conn, table_name, columns)

    # Оновити запис
    def update_item(self, table_name, columns, item, item_id):
        basic_db_backend.update_item(self._conn, table_name, columns, item, item_id)

    # Видалити запис за ключем
    def delete_item(self, table_name, item_id):
        columns = self.get_columns(table_name)
        basic_db_backend.delete_item(self._conn, table_name, columns, item_id)

    # Закрити підключення до бази даних
    def __del__(self):
        basic_db_backend.disconnect_from_db(self._conn)


# Тестування модуля
if __name__ == '__main__':
    model = ModelPostgre()
    tbl_name = "Sessions"
    item0 = model.read_item(tbl_name, None, 10)
    print(item0)
    column = "testID"

    column_type = model.get_column_type(tbl_name, column)
    print(column_type)

    column_types = model.get_column_types(tbl_name)
    print(column_types)

    model.generate_numbers(5, 10)
    model.generate_str(5, 5)
    model.generate_date(90, 30)

    rows = model.search_query1('20-09-16', 'Kiev')
    print(rows)

    rows = model.search_query2('20-09-16', 60, 116)
    print(rows)

    rows = model.search_query3('er', 'Fan')
    print(rows)

			