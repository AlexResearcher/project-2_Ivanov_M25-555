import json


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

