import mvc_exceptions as mvc_exc


class Controller(object):
    def __init__(self, model, view):
        self.model = model
        self.view = view

    # Відобразити список таблиць, які додали до моделі
    def show_table_list(self):
        for i, item in enumerate(self.model.tables):
            print('{}. {}'.format(i + 1, item))
        print("Виберіть одну з таблиць...")

    # Відобразити головне меню
    def show_menu(self):
        self.view.show_menu()

    # За номером отримати таблицю
    def get_table_name(self, number):
        return self.model.tables[number]

    # Отримати імена стовпців для таблиці
    def get_columns(self, table_name):
        return self.model.get_columns(table_name)

    # Показати можливі операції
    def show_operation_list(self):
        self.view.show_operation_list()
   
    # Отримати ім'я батьківської (якщо така є) для заданої таблиці та стовпця
    def get_parent_table(self, table_name, column_name):
        for key in self.model.foreign_keys:
            if key['fk_table'] == table_name and key['fk_column'] == column_name:
                return key['ref_table']
        return None

    # Отримати типи для стовпців таблиці
    def get_column_types(self, table_name):
        return self.model.get_column_types(table_name)

    # Показати записи з таблиці
    def show_items(self, table_name, columns=None, bullet_points=False):
        items = self.model.read_items(table_name, columns)
        item_type = table_name
        print(self.get_columns(table_name))
        if bullet_points:
            self.view.show_bullet_point_list(item_type, items)
        else:
            self.view.show_number_point_list(item_type, items)

    # Додати запис
    def insert_item(self, table_name, columns, item):

        assert len(item) != 0, 'size = 0'
        item_type = table_name
        name = item[0]
        try:
            self.model.create_item(table_name, columns, item)
            self.view.display_item_stored(name, item_type)

        except Exception as e:
            self.model.roll_back()
            self.view.display_item_already_stored_error(name, item_type, e)

    # Показати один запис з таблиці
    def show_item(self, table_name, columns=None, item_id=None):
        try:
            item = self.model.read_item(table_name, columns, item_id)
            item_type = table_name
            if item is None:
                return
            self.view.show_item(item_type, item[0], item)
        except mvc_exc.ItemNotStored as e:
            self.view.display_missing_item_error(item_id, e)

    # Видалити запис з табилці за ключем
    def delete_item(self, table_name, item_id):
        item_type = table_name
        try:
            self.model.delete_item(table_name, item_id)
            self.view.display_item_deletion(item_id)
        except mvc_exc.ItemNotStored as e:
            self.view.display_item_not_yet_stored_error(item_id, item_type, e)

    # Оновити запис
    def update_item(self, table_name, columns, item):

        item_type = table_name
        name = item[0]
        try:
            self.model.update_item(table_name, columns, item, item[0])
            self.view.display_item_updated(item, item_type)

        except mvc_exc.ItemNotStored as e:
            self.view.display_item_not_yet_stored_error(name, item_type, e)
			