class View(object):

    @staticmethod
    def show_menu():
        print("1. Редагувати дані таблиці")
        print("2. Генерація випадкових даних")
        print("3. Пошук даних ")
        print("4. Вихід")
        print("Виберіть один з пуктів...")

    @staticmethod
    def show_operation_list():
        print("1. Додати запис")
        print("2. Оновити дані запису")
        print("3. Відобразити дані")
        print("4. Видалити дані")
        print("5. Вихід")
        print("Виберіть один з пуктів...")

   
    @staticmethod
    def show_number_point_list(item_type, items):
        print("--- ТАБЛИЦЯ {} ---".format(item_type.upper()))
        for i, item in enumerate(items):
            print("{}. {}".format(i+1, item))
    @staticmethod
    def show_bullet_point_list(item_type, items):
        print("--- ТАБЛИЦЯ {} ---".format(item_type.upper()))
        for item in items:
            print("* {}".format(item))

    @staticmethod
    def show_item(item_type, item, item_info):
        print("**************************************************************")
        print("Данй запис було знайдено {}!".format(item))
        print("{} INFO: {}".format(item_type.upper(), item_info))
        print("**************************************************************")

    @staticmethod
    def display_missing_item_error(item, err):
        print("**************************************************************")
        print("Даний запис {} не було знайдено!".format(item))
        print("{}".format(err.args[0]))
        print("**************************************************************")


    @staticmethod
    def display_item_already_stored_error(item, item_type, err):
        print("**************************************************************")
        print("База даних вже містить елемент {} в таблиці {} !"
              .format(item, item_type))
        print("{}".format(err.args[0]))
        print("**************************************************************")


    @staticmethod
    def display_item_not_yet_stored_error(item, item_type, err):
        print("**************************************************************")
        print("We don\"t have any {} in our {} list. Please insert it first!"
              .format(item, item_type))
        print("{}".format(err.args[0]))
        print("**************************************************************")


    @staticmethod
    def display_item_stored(item, item_type):
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("Успіх! Запис з ідентифікатором {} було додано до таблиці {} !"
              .format(item, item_type))
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


    @staticmethod
    def display_change_item_type(older, newer):
        print("---   ---   ---   ---   ---   ---   ---   ---   ---   ---   --")
        print("Змінити тип з "{}" до "{}"".format(older, newer))
        print("---   ---   ---   ---   ---   ---   ---   ---   ---   ---   --")


    @staticmethod
    def display_item_updated(item, item_type):
        print("---   ---   ---   ---   ---   ---   ---   ---   ---   ---   --")
        print("Запис {} було змінено на {}"
              .format(item[0], item))
        print("---   ---   ---   ---   ---   ---   ---   ---   ---   ---   --")


    @staticmethod
    def display_item_deletion(name):
        print("--------------------------------------------------------------")
        print("Елемент {} було успішно видалено!".format(name))
        print("--------------------------------------------------------------")
			