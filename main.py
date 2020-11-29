import psycopg2

from view import View
from model import ModelPostgre
from controller import Controller


# Введення даних з клавіатури в залежності від типу
def input_value(controller, type_name, fk_table=None):
    if fk_table is not None:
        controller.show_items(fk_table)
        print("Виберіть ключ зі списку")
        value = int(input())
    elif type_name == 'integer':
        print("Тип поля число")
        value = int(input())
    elif type_name == 'date':
        print("Тип поля дата")
        value = input()
    elif type_name == 'character varying':
        print("Тип поля текст")
        value = input()
    elif type_name == 'text':
        print("Тип поля текст")
        value = input()
    return value


# Генерація даних в залежності від типу даних
def generate_values(controller, type_name, num, size):

    if type_name == 'integer':
        print("Введіть максимальне число")
        value = int(input())
        return controller.model.generate_numbers(num, value)
    elif type_name == 'date':
        print("Введіть кількість днів в діапазоні")
        value = int(input())
        return controller.model.generate_date(num, value, 0)
    elif type_name == 'character varying':
        print("Введіть довжину рядка")
        value = int(input())
        return controller.model.generate_str(num, value)
    elif type_name == 'text':
        print("Введіть довжину рядка")
        value = int(input())
        return controller.model.generate_str(num, value)
    else:
        return controller.model.generate_numbers(num, 100)

    return None


# Завдення1. Базові операції над даними таблиць CRUD (Create, Read, Update, Delete)
def option1(controller):
    controller.show_table_list()
    table_num = -1
    try:
        table_num = int(input())
        tbl_name = controller.get_table_name(table_num - 1)
    except ValueError as e:
        print('Помилка введення даних!')
        return

    op = 0
    while op != 5:
        controller.show_operation_list()
        op = int(input())
        if op == 1:
            tbl_columns = controller.get_columns(tbl_name)
            tbl_column_types = controller.get_column_types(tbl_name)
            item = []
            try:
                for i, column in enumerate(tbl_columns):
                    parent_tbl = controller.get_parent_table(tbl_name, column)
                    print('Введіть значення поля {}'.format(column))
                    val = input_value(controller, tbl_column_types[i], parent_tbl)
                    item.append(val)
                controller.insert_item(tbl_name, tbl_columns, item)
            except ValueError as e:
                print('Помилка введення даних!')
                return
        elif op == 2:
            tbl_columns = controller.get_columns(tbl_name)
            tbl_column_types = controller.get_column_types(tbl_name)
            item = []
            try:
                for i, column in enumerate(tbl_columns):
                    parent_tbl = controller.get_parent_table(tbl_name, column)
                    print('Введіть значення поля {}'.format(column))
                    val = input_value(controller, tbl_column_types[i], parent_tbl)
                    item.append(val)
                controller.update_item(tbl_name, tbl_columns, item)
            except ValueError as e:
                print('Помилка введення даних!')
                return
        elif op == 3:
            controller.show_items(tbl_name)
        elif op == 4:
            try:
                print("Введіть ключ для запису який необхідно видалити")
                item_id = int(input())
                controller.delete_item(tbl_name, item_id)
            except ValueError as e:
                print('Помилка введення даних')
                return
        input("Для продовження натисніть Enter...")


# Завдання 2
def option2(controller):
    controller.show_table_list()
    table_num = int(input())
    tbl_name = controller.get_table_name(table_num - 1)
    op = 0
    print("Було обрано таблицю {}".format(tbl_name))
    print("Введіть кількість записів, що потрібно згенерувати")
    num = int(input())
    tbl_columns = controller.get_columns(tbl_name)
    tbl_column_types = controller.get_column_types(tbl_name)
    elements = []
    for i, column in enumerate(tbl_columns):
        print('Введіть дані для поля {}'.format(column))
        val = generate_values(controller, tbl_column_types[i], num, 5)
        elements.append(val)
    for j in range(num):
        item = []
        for k in range(len(tbl_columns)):
            item.append(elements[k][j])
        try:
            controller.model.create_item(tbl_name, tbl_columns, item)
        except(Exception, psycopg2.DatabaseError) as error:
            controller.model.roll_back()


# Завдання 3
# Фільми, що будут показувати після дати у кінотеатрі
# Фільми, що будут показувати після дати та тривалістю у рамках задного
# Фільми, назви яких містять словосполучення та відповідного жанру
def option3(controller):
    print('1. Фільми, що будуть показувати після дати у кінотеатрі')
    print('2. Фільми, що будуть показувати після дати та тривалістю у рамках заданого')
    print('3. Фільми, назви яких містять словосполучення та відповідного жанру')
    print('Виберіть одну з дій...')
    op = int(input())
    if op == 1:
        print('Введіть назву кінотеатру')
        cinema = input()
        print('Введіть дату після якої будуть показувати фільми (дд/мм/рр)')
        date = input()
        columns, items = controller.model.search_query1(date, cinema)
        print(columns)
        controller.view.show_number_point_list('Films', items)

    elif op == 2:
        print('Введіть дату після якої будуть показувати фільми (дд/мм/рр)')
        date = input()
        print('Введіть мінімальну тривалість фільму')
        min_duration = int(input())
        print('Введіть максимальну тривалість фільму')
        max_duration = int(input())
        columns, items = controller.model.search_query2(date, min_duration, max_duration)
        print(columns)
        controller.view.show_number_point_list('Films', items)

    elif op == 3:
        print('Введіть словосполучення, яке присутнє у назві фільма')
        str_seq = input()
        print('Введіть словосполучення, яке присутнє у назві жанру')
        str_genre = input()
        columns, items = controller.model.search_query3(str_seq, str_genre)
        print(columns)
        controller.view.show_number_point_list('Films', items)

    input("Для продовження натисніть Enter...")

if __name__ == '__main__':
    # Створюємо об'єкт для моделі бази даних Postgre
    model = ModelPostgre()
    # Додаємо назви таблиць до моделі
    model.add_tables(["Films", "Cinemas", "Cinema_Session", "Sessions"])
    # Задаємо інформацію про зовнішні ключі
    # Для контролю даних, що вводяться
    key1 = {'fk_table': 'Sessions', 'fk_column': 'FilmID', 'ref_table': 'Films', 'ref_column': 'FilmID'}
    model.add_foreign_key(key1)
    key2 = {'fk_table': 'Cinema_Session', 'fk_column': 'SessionID', 'ref_table': 'Sessions', 'ref_column': 'SessionID'}
    model.add_foreign_key(key2)
    key3 = {'fk_table': 'Cinema_Session', 'fk_column': 'CinemaID', 'ref_table': 'Cinemas', 'ref_column': 'CinemaID'}
    model.add_foreign_key(key3)
    # Створюємо об'єкт типу Controller
    c = Controller(model, View())

    option = 0
    while option != 4:
        c.show_menu()
        option = int(input())
        if option == 1:
            option1(c)
        elif option == 2:
            option2(c)
        elif option == 3:
            option3(c)

    #option1(c)
    #option2(c)
    #option3(c)

