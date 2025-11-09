"""Вспомогательные функции для работы с файлами."""

import json


def load_metadata(filepath):
    """
    Загрузить метаданные из JSON файла.

    Args:
        filepath: Путь к JSON файлу

    Returns:
        Словарь с метаданными или пустой словарь, если файл не найден
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath, data):
    """
    Сохранить метаданные в JSON файл.

    Args:
        filepath: Путь к JSON файлу
        data: Словарь для сохранения
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_table_data(table_name):
    """
    Загрузить данные таблицы из JSON файла.

    Args:
        table_name: Имя таблицы

    Returns:
        Список записей или пустой список, если файл не найден
    """
    filepath = f"data/{table_name}.json"
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name, data):
    """
    Сохранить данные таблицы в JSON файл.

    Args:
        table_name: Имя таблицы
        data: Список записей для сохранения
    """
    filepath = f"data/{table_name}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
