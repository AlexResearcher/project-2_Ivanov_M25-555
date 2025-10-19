def parse_where_clause(args):
    """
    Парсит условие where из списка аргументов args
    Возвращает словарь {column: value}
    """
    if not args:
        return None
    
    where_col = args[0]
    where_condition_value = args[2]

    column = where_col.strip()
    value_str = where_condition_value.strip()
    
    # убираем кавычки если есть
    if (value_str.startswith('"') and value_str.endswith('"')) or \
       (value_str.startswith("'") and value_str.endswith("'")):
        value = value_str[1:-1]
    
    # пытаемся определить тип
    try:
        if '.' in value_str:
            value = float(value_str)
        else:
            value = int(value_str)
    except ValueError:
        if value_str.lower() == 'true':
                value = "True"
        elif value_str.lower() == 'false':
            value = "False"
        else:
            value = value_str
            
    return {column: str(value)}

def parse_set_clause(args):
    """
    Парсит условие set из списка аргументов args"
    Возвращает словарь {column: value}
    """
    if not args:
        return None

    set_col = args[3]
    set_condition_value = args[5]
    
    set_clause = {}
        
    column = set_col.strip()
    value_str = set_condition_value.strip()
        
    # убираем лишние кавычки если есть
    if (value_str.startswith('"') and value_str.endswith('"')) or \
        (value_str.startswith("'") and value_str.endswith("'")):
        value = value_str[1:-1]
    else:
        # пытаемся определить тип
        try:
            if '.' in value_str:
                value = float(value_str)
            else:
                value = int(value_str)
        except ValueError:
            if value_str.lower() == 'true':
                value = "True"
            elif value_str.lower() == 'false':
                value = "False"
            else:
                value = value_str
    
    set_clause[column] = str(value)
    
    return set_clause

def parse_values(values_str):
    """
    Парсит значения в формате "("значение1", "значение2", ...)"
    Возвращает список значений
    """
    if values_str[0] != "(" or values_str[-1] != ")":
        print(
'Неправильный формат переданных значений. ' \
'Правильный формат:')
        print(
'<command> insert into <имя_таблицы>' \
' values (<значение1>, <значение2>, ...)')
        return None
    
    # убираем скобки
    values_str = values_str[1:-1].strip()
    
    # простой split по запятым
    values = []
    parts = values_str.split(',')
    
    for part in parts:
        part = part.strip()   
        values.append(part)
    
    return values