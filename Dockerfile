# Используйте официальный образ Python как основу
FROM python:3.12

# Установите рабочую директорию внутри контейнера
WORKDIR /app

# Копируйте файлы проекта в контейнер
COPY . .

# Установите Poetry
RUN pip install poetry

# Настройте Poetry:
# - отключите создание виртуального окружения внутри Docker контейнера
# - установите зависимости проекта, используя файлы pyproject.toml и poetry.lock
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Активируйте виртуальное окружение
ENV PATH="/.venv/bin:$PATH"

# Установите uvicorn внутри контейнера
RUN pip install uvicorn

# Команда для запуска приложения
CMD ["uvicorn", "dz_fastapi.main:app", "--host", "0.0.0.0", "--reload"]
