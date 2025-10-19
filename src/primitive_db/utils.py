import json
import os


def load_metadata(filepath="db_meta.json"):
    """
    Загружает метаданные из JSON-файла
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_metadata(data, filepath="db_meta.json"):
    """
    Сохраняет метаданные в JSON-файл
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def save_table_data(table_name, data, data_dir="data"):
    """
    Сохраняет данные таблицы в JSON-файл, удаляет таблицу,
    если запрос с пустыми данными (все удалены)
    """
    if not data:
        # удаляем файл, если пришел запрос с пустыми данными
        file_path = f"data/{table_name}.json"
        if os.path.exists(file_path):
            os.remove(file_path)
    else:
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        filepath = os.path.join(data_dir, f"{table_name}.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def load_table_data(table_name, data_dir="data"):
    """
    Загружает данные таблицы из JSON-файла
    """
    filepath = os.path.join(data_dir, f"{table_name}.json")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []