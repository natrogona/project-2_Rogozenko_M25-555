"""Движок базы данных и обработка команд."""

import re
from src.primitive_db import core, utils


METADATA_FILE = "db_meta.json"


def parse_command_parts(command):
    """
    Разобрать команду на части с учетом строк в кавычках.

    Args:
        command: Строка команды

    Returns:
        Список частей команды
    """
    # Разделить по пробелам, но сохранить строки в кавычках вместе
    parts = re.findall(r'[^\s"\']+|"[^"]*"|\'[^\']*\'', command)
    return [p.strip('"').strip("'") for p in parts]


def run():
    """Главный цикл базы данных."""
    print("Добро пожаловать в Primitive DB!")
    print("Введите 'help' для просмотра доступных команд или 'exit' для выхода")

    # Загрузить метаданные
    metadata = utils.load_metadata(METADATA_FILE)

    while True:
        try:
            command = input("\nВведите команду: ").strip()

            if not command:
                continue

            # Разобрать команду
            cmd = command.lower()

            if cmd == 'exit':
                # Сохранить метаданные перед выходом
                utils.save_metadata(METADATA_FILE, metadata)
                print("До свидания!")
                break

            if cmd == 'help':
                print_help()
                continue

            # Обработать команду INSERT
            if cmd.startswith('insert into'):
                handle_insert(command, metadata)

            # Обработать команду SELECT
            elif cmd.startswith('select from'):
                handle_select(command, metadata)

            # Обработать команду update
            elif cmd.startswith('update'):
                handle_update(command, metadata)

            # Обработать команду DELETE
            elif cmd.startswith('delete from'):
                handle_delete(command, metadata)

            # Обработать команду info
            elif cmd.startswith('info'):
                handle_info(command, metadata)

            # Обработать команду create_table
            elif cmd.startswith('create_table'):
                parts = command.split()
                if len(parts) < 3:
                    print("Ошибка: Неверный синтаксис. Используйте: create_table "
                          "имя_таблицы столбец1:тип1 столбец2:тип2 ...")
                    continue

                table_name = parts[1]
                columns = parts[2:]

                if not columns:
                    print("Ошибка: Требуется хотя бы один столбец")
                    continue

                metadata = core.create_table(metadata, table_name, columns)
                utils.save_metadata(METADATA_FILE, metadata)

            # Обработать команду drop_table
            elif cmd.startswith('drop_table'):
                parts = command.split()
                if len(parts) != 2:
                    print("Ошибка: Неверный синтаксис. Используйте: drop_table имя_таблицы")
                    continue

                table_name = parts[1]
                metadata = core.drop_table(metadata, table_name)
                utils.save_metadata(METADATA_FILE, metadata)

            # Обработать команду list_tables
            elif cmd == 'list_tables':
                show_tables(metadata)

            else:
                print("Ошибка: Неизвестная команда. Введите 'help' для "
                      "просмотра доступных команд")

        except ValueError as e:
            print(str(e))
        except KeyboardInterrupt:
            print("\nПрервано. Введите 'exit' для выхода")
        except Exception as e:
            print(f"Ошибка: {e}")


def print_help():
    """Вывести доступные команды."""
    print("\nДоступные команды:")
    print("\n*** Управление таблицами ***")
    print("  create_table имя_таблицы столбец1:тип1 столбец2:тип2 ...")
    print("    - Создать новую таблицу с указанными столбцами")
    print("    - Допустимые типы: int, str, bool")
    print("    - Столбец ID:int добавляется автоматически")
    print("\n  drop_table имя_таблицы")
    print("    - Удалить таблицу")
    print("\n  list_tables")
    print("    - Показать все таблицы")
    print("\n*** Операции с данными ***")
    print("  insert into имя_таблицы values (значение1, значение2, ...)")
    print("    - Создать запись (ID генерируется автоматически)")
    print("\n  select from имя_таблицы")
    print("    - Прочитать все записи")
    print("\n  select from имя_таблицы where столбец = значение")
    print("    - Прочитать записи по условию")
    print("\n  update имя_таблицы set столбец = новое_значение where столбец = значение")
    print("    - Обновить запись")
    print("\n  delete from имя_таблицы where столбец = значение")
    print("    - Удалить запись")
    print("\n  info имя_таблицы")
    print("    - Показать информацию о таблице")
    print("\n*** Другое ***")
    print("  help")
    print("    - Показать это справочное сообщение")
    print("\n  exit")
    print("    - Выйти из программы")


def show_tables(metadata):
    """Отобразить все таблицы."""
    if not metadata:
        print("Таблицы не найдены")
        return

    print("\nТаблицы:")
    for table_name, table_info in metadata.items():
        columns = table_info.get('columns', [])
        col_str = ', '.join([f"{col['name']}:{col['type']}"
                            for col in columns])
        print(f"  {table_name} ({col_str})")


def handle_insert(command, metadata):
    """Обработать команду INSERT."""
    # Разобрать: insert into table_name values (val1, val2, ...)
    match = re.match(r'insert\s+into\s+(\w+)\s+values\s*\((.*)\)',
                     command, re.IGNORECASE)
    if not match:
        print("Ошибка: Неверный синтаксис. Используйте: insert into имя_таблицы "
              "values (значение1, значение2, ...)")
        return

    table_name = match.group(1)
    values_str = match.group(2)

    # Разобрать значения
    values = [v.strip() for v in values_str.split(',')]

    core.insert_into(metadata, table_name, values)


def handle_select(command, metadata):
    """Обработать команду SELECT."""
    # Разобрать: select from table_name [where column = value]
    match = re.match(
        r'select\s+from\s+(\w+)(?:\s+where\s+(\w+)\s*=\s*(.+))?',
        command, re.IGNORECASE)
    if not match:
        print("Ошибка: Неверный синтаксис. Используйте: select from имя_таблицы "
              "[where столбец = значение]")
        return

    table_name = match.group(1)
    where_column = match.group(2)
    where_value = match.group(3)

    if where_column and where_value:
        where_value = where_value.strip()

    core.select_from(metadata, table_name, where_column, where_value)


def handle_update(command, metadata):
    """Обработать команду update."""
    # Разобрать: update table_name set column = value where column = value
    match = re.match(
        r'update\s+(\w+)\s+set\s+(\w+)\s*=\s*(.+?)\s+where\s+(\w+)\s*=\s*(.+)',
        command, re.IGNORECASE)
    if not match:
        print("Ошибка: Неверный синтаксис. Используйте: update имя_таблицы "
              "set столбец = значение where столбец = значение")
        return

    table_name = match.group(1)
    set_column = match.group(2)
    set_value = match.group(3).strip()
    where_column = match.group(4)
    where_value = match.group(5).strip()

    core.update_table(metadata, table_name, set_column, set_value,
                      where_column, where_value)


def handle_delete(command, metadata):
    """Обработать команду DELETE."""
    # Разобрать: delete from table_name where column = value
    match = re.match(
        r'delete\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)',
        command, re.IGNORECASE)
    if not match:
        print("Ошибка: Неверный синтаксис. Используйте: delete from имя_таблицы "
              "where столбец = значение")
        return

    table_name = match.group(1)
    where_column = match.group(2)
    where_value = match.group(3).strip()

    core.delete_from(metadata, table_name, where_column, where_value)


def handle_info(command, metadata):
    """Обработать команду info."""
    # Разобрать: info table_name
    match = re.match(r'info\s+(\w+)', command, re.IGNORECASE)
    if not match:
        print("Ошибка: Неверный синтаксис. Используйте: info имя_таблицы")
        return

    table_name = match.group(1)
    core.info_table(metadata, table_name)
