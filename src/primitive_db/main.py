"""Главная точка входа для проекта примитивной базы данных."""

from src.primitive_db import engine


def main():
    """Запустить движок базы данных."""
    engine.run()


if __name__ == "__main__":
    main()
