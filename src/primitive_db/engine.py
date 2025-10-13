import shlex

import prompt

from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import load_metadata, save_metadata

HELP_TEXT = """***Процесс работы с таблицей***
Функции:
<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу
<command> list_tables - показать список всех таблиц
<command> drop_table <имя_таблицы> - удалить таблицу
<command> exit - выход из программы
<command> help - справочная информация"""

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
                print(HELP_TEXT)
                
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
                    print(f'Таблица "{table_name}" успешно удалена.')
                except ValueError as e:
                    print(f"Ошибка: {e}")
                        
            else:
                print(f'Функции "{command}" нет. Попробуйте снова.')
                
        except Exception as e:
            print(f"Некорректное значение: {e}. Попробуйте снова.")