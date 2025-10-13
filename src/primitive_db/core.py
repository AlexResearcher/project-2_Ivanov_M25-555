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
    table_columns = ['ID:int']  # Автоматически добавляем ID
    
    for column in columns:
        if ':' not in column:
            raise ValueError(f'Некорректный формат столбца: {column}')
        
        col_name, col_type = column.split(':', 1)
        if col_type not in valid_types:
            raise ValueError(f'Некорректный тип данных: {col_type}.\
                              Допустимые типы: int, str, bool')
        
        table_columns.append(f'{col_name}:{col_type}')
    
    # Добавляем таблицу в метаданные
    metadata[table_name] = table_columns
    return metadata

def drop_table(metadata, table_name):
    """
    Удаляет таблицу из метаданных
    
    Args:
        metadata (dict): текущие метаданные БД
        table_name (str): имя таблицы для удаления
    
    Returns:
        dict: обновленные метаданные
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    del metadata[table_name]
    return metadata

def list_tables(metadata):
    """
    Возвращает список всех таблиц
    """
    return list(metadata.keys())