install:
	poetry install

project:
	poetry run project

lint:
	poetry run ruff check .
