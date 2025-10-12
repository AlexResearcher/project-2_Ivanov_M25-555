import prompt

def run_project():
    print("Первая попытка запустить проект!")
    print("***")
    
    # Начало главного цикла программы
    while True:
        # Запрашиваем команду у пользователя
        command = prompt.string('Введите команду: ')
        
        # Обрабатываем введённую команду
        if command == 'exit':
            print("Выход из программы...")
            break
        elif command == 'help':
            print("\n<command> exit - выйти из программы")
            print("<command> help - справочная информация")
        else:
            print(f"Неизвестная команда: {command}")