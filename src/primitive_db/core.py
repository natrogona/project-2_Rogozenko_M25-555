"""Основные операции базы данных."""

from prettytable import PrettyTable
from src.primitive_db import utils


VALID_TYPES = {'int', 'str', 'bool'}


def create_table(metadata, table_name, columns):
    """
    Создать новую таблицу в базе данных.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя создаваемой таблицы
        columns: Список столбцов в формате ["name:type", ...]

    Returns:
        Обновленный словарь метаданных

    Raises:
        ValueError: Если таблица существует или недопустимые типы данных
    """
    # Проверить, существует ли таблица
    if table_name in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' уже существует")

    # Разобрать и проверить столбцы
    parsed_columns = []

    # Автоматически добавить столбец ID
    parsed_columns.append({"name": "ID", "type": "int"})

    # Разобрать пользовательские столбцы
    for col in columns:
        if ':' not in col:
            raise ValueError(f"Ошибка: Неверный формат столбца '{col}'. "
                             "Используйте 'имя:тип'")

        name, col_type = col.split(':', 1)
        name = name.strip()
        col_type = col_type.strip()

        if col_type not in VALID_TYPES:
            raise ValueError(f"Ошибка: Недопустимый тип '{col_type}'. "
                             f"Допустимые типы: {', '.join(VALID_TYPES)}")

        parsed_columns.append({"name": name, "type": col_type})

    # Добавить таблицу в метаданные
    metadata[table_name] = {
        "columns": parsed_columns
    }

    print(f"Таблица '{table_name}' успешно создана")
    return metadata


def drop_table(metadata, table_name):
    """
    Удалить таблицу из базы данных.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя удаляемой таблицы

    Returns:
        Обновленный словарь метаданных

    Raises:
        ValueError: Если таблица не существует
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    del metadata[table_name]
    print(f"Таблица '{table_name}' успешно удалена")
    return metadata


def parse_value(value_str, expected_type):
    """
    Преобразовать строковое значение в ожидаемый тип.

    Args:
        value_str: Строковое представление значения
        expected_type: Ожидаемый тип ('int', 'str', 'bool')

    Returns:
        Преобразованное значение

    Raises:
        ValueError: Если значение невозможно преобразовать
    """
    value_str = value_str.strip().strip('"').strip("'")

    if expected_type == 'int':
        try:
            return int(value_str)
        except ValueError:
            raise ValueError(f"Ошибка: Невозможно преобразовать '{value_str}' в int")
    elif expected_type == 'str':
        return value_str
    elif expected_type == 'bool':
        if value_str.lower() in ('true', '1', 'yes', 'да', 'истина'):
            return True
        elif value_str.lower() in ('false', '0', 'no', 'нет', 'ложь'):
            return False
        else:
            raise ValueError(f"Ошибка: Невозможно преобразовать '{value_str}' в bool")
    else:
        raise ValueError(f"Ошибка: Неизвестный тип '{expected_type}'")


def insert_into(metadata, table_name, values):
    """
    Вставить запись в таблицу.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя таблицы
        values: Список значений для вставки

    Raises:
        ValueError: Если таблица не существует или неверные данные
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    columns = metadata[table_name]['columns']
    # Исключить столбец ID, так как он генерируется автоматически
    user_columns = [col for col in columns if col['name'] != 'ID']

    if len(values) != len(user_columns):
        raise ValueError(f"Ошибка: Ожидается {len(user_columns)} значений, "
                         f"получено {len(values)}")

    # Загрузить существующие данные
    data = utils.load_table_data(table_name)

    # Сгенерировать новый ID
    if data:
        new_id = max(record['ID'] for record in data) + 1
    else:
        new_id = 1

    # Разобрать и проверить значения
    record = {'ID': new_id}
    for i, col in enumerate(user_columns):
        try:
            record[col['name']] = parse_value(values[i], col['type'])
        except ValueError as e:
            raise ValueError(f"Ошибка в столбце '{col['name']}': {str(e)}")

    # Добавить запись в данные
    data.append(record)

    # Сохранить данные
    utils.save_table_data(table_name, data)

    print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')


def select_from(metadata, table_name, where_column=None, where_value=None):
    """
    Выбрать записи из таблицы.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя таблицы
        where_column: Имя столбца для фильтрации (опционально)
        where_value: Значение для фильтрации (опционально)

    Raises:
        ValueError: Если таблица не существует
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    columns = metadata[table_name]['columns']
    data = utils.load_table_data(table_name)

    # Фильтровать данные, если предоставлено условие where
    if where_column and where_value:
        # Найти тип столбца
        col_type = None
        for col in columns:
            if col['name'] == where_column:
                col_type = col['type']
                break

        if col_type is None:
            raise ValueError(f"Ошибка: Столбец '{where_column}' не существует")

        # Преобразовать значение фильтра
        parsed_value = parse_value(where_value, col_type)

        # Отфильтровать записи
        data = [record for record in data
                if record.get(where_column) == parsed_value]

    # Отобразить результаты с помощью PrettyTable
    if not data:
        print("Записи не найдены")
        return

    table = PrettyTable()
    table.field_names = [col['name'] for col in columns]

    for record in data:
        row = [record.get(col['name'], '') for col in columns]
        table.add_row(row)

    print(table)


def update_table(metadata, table_name, set_column, set_value,
                 where_column, where_value):
    """
    Обновить записи в таблице.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя таблицы
        set_column: Столбец для обновления
        set_value: Новое значение
        where_column: Столбец для фильтрации
        where_value: Значение для фильтрации

    Raises:
        ValueError: Если таблица не существует или неверные данные
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    columns = metadata[table_name]['columns']
    data = utils.load_table_data(table_name)

    # Найти типы столбцов
    set_col_type = None
    where_col_type = None
    for col in columns:
        if col['name'] == set_column:
            set_col_type = col['type']
        if col['name'] == where_column:
            where_col_type = col['type']

    if set_col_type is None:
        raise ValueError(f"Ошибка: Столбец '{set_column}' не существует")
    if where_col_type is None:
        raise ValueError(f"Ошибка: Столбец '{where_column}' не существует")

    # Преобразовать значения
    parsed_set_value = parse_value(set_value, set_col_type)
    parsed_where_value = parse_value(where_value, where_col_type)

    # Обновить подходящие записи
    updated_count = 0
    updated_ids = []
    for record in data:
        if record.get(where_column) == parsed_where_value:
            record[set_column] = parsed_set_value
            updated_count += 1
            updated_ids.append(record['ID'])

    if updated_count == 0:
        print("Подходящие записи не найдены")
        return

    # Сохранить данные
    utils.save_table_data(table_name, data)

    for record_id in updated_ids:
        print(f'Запись с ID={record_id} в таблице "{table_name}" '
              'успешно обновлена.')


def delete_from(metadata, table_name, where_column, where_value):
    """
    Удалить записи из таблицы.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя таблицы
        where_column: Столбец для фильтрации
        where_value: Значение для фильтрации

    Raises:
        ValueError: Если таблица не существует или неверные данные
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    columns = metadata[table_name]['columns']
    data = utils.load_table_data(table_name)

    # Найти тип столбца
    where_col_type = None
    for col in columns:
        if col['name'] == where_column:
            where_col_type = col['type']
            break

    if where_col_type is None:
        raise ValueError(f"Ошибка: Столбец '{where_column}' не существует")

    # Преобразовать значение фильтра
    parsed_where_value = parse_value(where_value, where_col_type)

    # Найти и удалить подходящие записи
    deleted_ids = []
    new_data = []
    for record in data:
        if record.get(where_column) == parsed_where_value:
            deleted_ids.append(record['ID'])
        else:
            new_data.append(record)

    if not deleted_ids:
        print("Подходящие записи не найдены")
        return

    # Сохранить данные
    utils.save_table_data(table_name, new_data)

    for record_id in deleted_ids:
        print(f'Запись с ID={record_id} успешно удалена из таблицы '
              f'"{table_name}".')


def info_table(metadata, table_name):
    """
    Отобразить информацию о таблице.

    Args:
        metadata: Текущий словарь метаданных
        table_name: Имя таблицы

    Raises:
        ValueError: Если таблица не существует
    """
    if table_name not in metadata:
        raise ValueError(f"Ошибка: Таблица '{table_name}' не существует")

    columns = metadata[table_name]['columns']
    col_str = ', '.join([f"{col['name']}:{col['type']}" for col in columns])

    print(f"Таблица: {table_name}")
    print(f"Столбцы: {col_str}")
