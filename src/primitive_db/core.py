from prettytable import PrettyTable

from src.primitive_db.utils import load_table_data, save_table_data
from src.decorators import confirm_action, create_cacher, handle_db_errors, log_time 

@handle_db_errors
def create_table(metadata, table_name, columns):
    """
    Создает новую таблицу в метаданных
    
    Args:
        metadata (dict): текущие метаданные БД
        table_name (str): имя таблицы
        columns (list): список столбцов в формате ["name:str", "age:int"]
    
    Returns:
        dict: обновленные метаданные
    """
    # проверяем, существует ли таблица
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    
    # проверяем корректность типов и формируем список столбцов
    valid_types = {'int', 'str', 'bool'}
    table_columns = ['ID:int']  # автоматически добавляем ID
    
    for column in columns:
        if ':' not in column:
            raise ValueError(
                f'Некорректный формат столбца: {column}')
        
        col_name, col_type = column.split(':', 1)
        if col_type not in valid_types:
            raise ValueError(f'Некорректный тип данных: {col_type}.\
Допустимые типы: int, str, bool')
        
        table_columns.append(f'{col_name}:{col_type}')
    
    # Добавляем таблицу в метаданные
    metadata[table_name] = table_columns
    return metadata

@handle_db_errors
@confirm_action("Удаление таблицы")
def drop_table(metadata, table_name):
    """
    Удаляет таблицу из метаданных и БД
    
    Args:
        metadata (dict): текущие метаданные БД
        table_name (str): имя таблицы для удаления
    
    Returns:
        dict: обновленные метаданные
    """
    if table_name not in metadata:
        raise KeyError(
f'Таблица "{table_name}" не существует.'
            )
    
    del metadata[table_name]
    return metadata

def list_tables(metadata):
    """
    Возвращает список всех таблиц
    """
    return list(metadata.keys())

@handle_db_errors
def validate_data_types(metadata, table_name, values):
    """
    Проверяет соответствие типов данных схеме таблицы
    """
    if table_name not in metadata:
        raise KeyError(
f"Таблица '{table_name}' не существует"
            )
    
    table_meta = metadata[table_name] 

    columns = [col for col in table_meta if\
                col.split(":")[0] != 'ID']
    
    if len(values) != len(columns):
        raise ValueError(f"Ожидается {len(columns)} значений,\
                          получено {len(values)}")
    
    for i, (value, column_meta) in enumerate(zip(values, columns)):
        # тип данных столбца, полученный при создании таблицы
        type = column_meta.split(":")[1] 
        # название столбца, полученное при ее создании
        name = column_meta.split(":")[0] 
        expected_type = type
        
        if expected_type == 'int' and not isinstance(int(value), int):
            raise ValueError(
                f"Столбец '{name}' должен иметь тип integer"
                )
        elif expected_type == 'text' and not isinstance(value, str):
            raise ValueError(
                f"Столбец '{name}' должен быть text"
                )
        elif expected_type == 'bool' and value\
              not in ["True", "False", True, False]:
            raise ValueError(
                f"Столбец '{name}' должен быть boolean"
                )
        
@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """
    Создает новую запись в таблице
    """
    if table_name not in metadata:
        raise KeyError(
            f"Таблица '{table_name}' не существует"
            )
    
    # загружаем текущие данные
    table_data = load_table_data(table_name)
    
    # проверяем типы данных
    validate_data_types(metadata, table_name, values)

    # данные о столбцах
    table_meta = metadata[table_name]
    
    # генерируем ID
    # проверим, добавляем ли мы данные впервые
    if not table_data:
        new_id = 0
        # cписок столбцов кроме ID
        columns = [col.split(":")[0]\
                   for col in table_meta if col.split(":")[0] != 'ID']
    else:
        new_id = max(item['ID'] for item in table_data) + 1
        # каждая запись в списке - это словарь, представляющий строку
        # ключи - все столбцы
        columns = list(table_data[0].keys())
        # cписок столбцов кроме ID
        columns = [value for value in columns if value != 'ID']
    
    # создаем новую запись
    new_record = {'ID': new_id}
    for i, (value, column_name) in enumerate(
        zip(values, columns)
        ):
        new_record[column_name] = str(value)       
    
    table_data.append(new_record)
    
    # сохраняем данные
    save_table_data(table_name, table_data)
    
    return new_id

@handle_db_errors
@log_time
def select(table_name, where_clause=None):
    """
    Читает записи из таблицы с возможностью фильтрации
    с кэшированием результатов
    """
    # ключ для кэша на основе параметров запроса
    cache_key = f"select_{table_name}_{str(where_clause)}"
    select_cacher = create_cacher()

    def fetch_data():
        """Внутренняя функция для получения данных (вызывается если нет данных в кэше)"""
        table_data = load_table_data(table_name)
        
        if where_clause is None:
            return table_data
        
        # Фильтруем данные
        filtered_data = []
        for record in table_data:
            match = True
            for column, value in where_clause.items():
                if record.get(column) != str(value):
                    match = False
                    #break
            if match:
                filtered_data.append(record)
                
        return filtered_data
    
    # используем кэшер для получения данных
    return select_cacher(cache_key, fetch_data)

@handle_db_errors
def update(metadata, table_name, set_clause, where_clause):
    """
    Обновляет записи в таблице
    """
    table_data = load_table_data(table_name)
    updated_data = []
    updated_ids = []
    
    if list(where_clause.keys())[0] not in \
        list(table_data[0].keys()):
        raise ValueError(
"В таблице отсутствует столбец " \
"с названием из условия where"
            )

    if list(set_clause.keys())[0] not in \
        list(table_data[0].keys()):
        raise ValueError(
"В таблице отсутствует столбец " \
"с названием из условия set"
            )
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record.get(column) != str(value):
                match = False
        
        if match:
            # создаем копию записи и обновляем ее
            updated_record = record.copy()
            updated_ids.append(str(updated_record["ID"]))
            for set_column, set_value in set_clause.items():
                updated_record[set_column] = str(set_value)
            updated_data.append(updated_record)
        else:
            updated_data.append(record)
    
    if updated_data != table_data:
        save_table_data(table_name, updated_data)
    
    return ", ".join(updated_ids)

@handle_db_errors
@confirm_action("Удаление значений")
def delete(table_name, where_clause):
    """
    Удаляет записи из таблицы
    """
    table_data = load_table_data(table_name)
    
    # фильтруем записи для удаления
    records_to_keep = []
    deleted_ids = []
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record.get(column) != value:
                match = False
        
        if not match:
            records_to_keep.append(record)
        else:
            deleted_ids.append(str(record["ID"]))
    
    if deleted_ids:
        save_table_data(table_name, records_to_keep)
    
        return ", ".join(deleted_ids)
    else:
        raise KeyError(
"В таблице отсутствуют подходящие данные для удаления"
            )

@handle_db_errors
def display_table(data, table_name, metadata):
    """
    Отображает данные в виде таблицы
    """
    if not data:
        raise KeyError("Нет данных для отображения")
        return
    
    # Получаем названия столбцов из метаданных
    if table_name in metadata:
        table_meta = metadata[table_name]
        columns = [col.split(":")[0] for col \
                   in table_meta if col.split(":")[0]]
    else:
        # Если метаданных нет
        raise KeyError(
"В файле метаданных отсутствует описание таблицы")
    
    table = PrettyTable()
    table.field_names = columns
    
    for record in data:
        row = [record.get(col, '') for col in columns]
        table.add_row(row)
    
    print(table)

@handle_db_errors
def info(table_name, metadata):
    """
    Выводит информацию о таблице
    """
    if table_name not in metadata:
        raise KeyError(f"Таблица '{table_name}' не существует")
    
    table_meta = metadata[table_name]
    table_data = load_table_data(table_name)
    
    print(f"Информация о таблице '{table_name}':")
    print(f"Количество записей: {len(table_data)}")
    print(f"Названия столбцов и типы данных: {table_meta}")