import shlex

import prompt

from src.primitive_db.core import (
    create_table,
    delete,
    display_table,
    drop_table,
    info,
    insert,
    list_tables,
    update,
    select,
)
from src.primitive_db.parser import parse_set_clause, parse_values, parse_where_clause
from src.primitive_db.utils import (
    load_metadata,
    save_metadata,
    save_table_data,
)

HELP_TEXT = """***Процесс работы с таблицей***
Функции:
<command> create_table <имя_таблицы> 
<столбец1:тип> <столбец2:тип> .. - создать таблицу
<command> list_tables - показать список всех таблиц
<command> drop_table <имя_таблицы> - удалить таблицу
<command> exit - выход из программы
<command> help - справочная информация"""

def print_help():
    print("""
***Операции с данными***
Функции:
<command> insert into <имя_таблицы> values \
(<значение1>, <значение2>, ...) - создать запись.
<command> select from <имя_таблицы> where \
<столбец> = <значение> - прочитать записи по условию.
<command> select from <имя_таблицы> \
- прочитать все записи.
<command> update <имя_таблицы> set <столбец1> = \
<новое_значение1> where <столбец_условия> \
= <значение_условия> - обновить запись.
<command> delete from <имя_таблицы> \
where <столбец> = <значение> - удалить запись.
<command> info <имя_таблицы> - вывести информацию о таблице.
<command> exit - выход из программы
<command> help- справочная информация
""")
    

def run():
    """
    Основной цикл программы
    """
    print("Добро пожаловать в систему управления базой данных!")
    print(HELP_TEXT)
    
    while True:
        try:
            user_input = prompt.string('Введите команду: ')
            
            if not user_input:
                continue
                
            # разбиваем команду на аргументы с помощью shlex. Получаем список
            args = shlex.split(user_input)
            command = args[0]
            
            # актуальные метаданные для дальнейшей обработки
            metadata = load_metadata()
            
            if command == "exit":
                print("Выход из программы.")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "list_tables":
                tables = list_tables(metadata)
                if tables:
                    for table in tables:
                        print(f"- {table}")
                else:
                    print("Таблицы отсутствуют.")
                    
            elif command == "create_table":
                if len(args) < 3:
                    print("Недостаточно аргументов. Правильный формат команды: \
create_table <имя_таблицы> <столбец1:тип> ...")
                    continue
                
                table_name = args[1]
                columns = args[2:]
                
                try:
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(metadata)
                    column_list = ", ".join(metadata[table_name])
                    print(f'Таблица "{table_name}" успешно создана\
 со столбцами: {column_list}')
                except ValueError as e:
                    print(f"Ошибка: {e}")
                    
            elif command == "drop_table":
                if len(args) != 2:
                    print("Неверное количество аргументов. \
Правильный формат команды: drop_table <имя_таблицы>")
                    continue
                
                table_name = args[1]

                try:#если таблицы нет выведет ValueError с текстом Ошибка: 
                    #Таблица table_name не существует.
                    metadata = drop_table(metadata, table_name) 
                    save_metadata(metadata)
                    save_table_data(table_name, data=[], data_dir="data")
                    print(f'Таблица "{table_name}" успешно удалена.')
                except ValueError as e:
                    print(f"Ошибка: {e}")


            elif user_input.startswith('insert into'):
                parts = user_input.split(' ', 3)
                if len(parts) < 4:
                    print("Ошибка: Неверный формат команды insert")
                    continue
                
                table_name = parts[2]
                values_str = parts[3]
                
                if not values_str.startswith('values'):
                    print("Ошибка: Ожидается ключевое слово values")
                    continue
                
                values_str = values_str[6:].strip()  # убираем "values"
                values = parse_values(values_str)
                
                try:
                    new_id = insert(metadata, table_name, values)
                    print(
f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
                except Exception as e:
                    print(f"Ошибка: {e}")
                    
            elif user_input.startswith('select from'):
                # select from users where age = 28

                # если не правильный вариант без условия where
                if len(args) < 3:
                    print("Ошибка: Неверный формат команды select")
                    continue

                # если прописано условие where
                if len(args) > 3 and len(args) < 7:
                    print("Ошибка: Неверный формат команды select")
                    continue

                table_name = args[2]
                print(table_name)

                if len(args) == 7:
                    where_args = args[4:7]
                    where_clause = parse_where_clause(where_args)
                    display_list = select(table_name, where_clause)
                else:
                    display_list = select(table_name)

                display_table(display_list, table_name, metadata)
            
            elif user_input.startswith('update'):
                # update users set age = 29 where name = "Sergei"
            
                if len(args) < 10:
                    print("Ошибка: Неверный формат команды update")
                    continue

                if 'set' not in args or 'where' not in args:
                    print("Ошибка: Ожидаются set и where условия")
                    continue
                
                table_name = args[1]
                where_args = args[7:10]

                set_clause = parse_set_clause(args)
                where_clause = parse_where_clause(where_args)

                try:
                    updated_id = update(metadata, table_name, set_clause, where_clause)
                    print(
f'Запись с ID={updated_id} в таблице {table_name} успешно обновлена.')
                except Exception as e:
                    print(f"Ошибка: {e}")
                    
            elif user_input.startswith('delete from'):
                # delete from users where ID = 1
                
                if len(args) < 7:
                    print("Ошибка: Неверный формат команды delete")
                    continue
                
                if "where" not in args:
                    print("Ошибка: Ожидается ключевое слово where")
                    continue

                if "where" not in args[3:7]:
                    print("Ошибка: Неверный формат команды delete")
                    continue

                table_name = args[2]
                where_args = args[4:7]
                
                where_clause = parse_where_clause(where_args)
                
                try:
                    deleted = delete(table_name, where_clause)
                    if deleted:
                        print(
f'Запись(и) с ID={deleted} успешно удалена(ы) из таблицы {table_name}.')
                    else:print(
f'Удаление не было выполнено.')
                except Exception as e:
                    print(f"Ошибка: {e}")
                    
            elif user_input.startswith('info'):
                # info users
                parts = user_input.split(' ', 1)
                if len(parts) < 2:
                    print("Ошибка: Укажите имя таблицы")
                    continue
                
                table_name = parts[1].strip()
                info(table_name, metadata)
                        
            else:
                print(f'Функции "{user_input}" нет. Попробуйте снова.')
                
        except Exception as e:
            print(f"Некорректное значение: {e}. Попробуйте снова.")