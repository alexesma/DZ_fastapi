# Используйте официальный образ Python как основу
FROM python:3.12

# Установите рабочую директорию внутри контейнера
WORKDIR /app

# Установите необходимые системные пакеты и зависимости для Pillow
RUN apt-get update && apt-get install -y \
    nano \
    libjpeg-dev \
    libpng-dev \
    libfreetype6-dev \
  && rm -rf /var/lib/apt/lists/*

# Обновите pip до последней версии
RUN pip install --upgrade pip

# Установите Poetry
RUN pip install poetry

# Копируйте только файлы, необходимые для установки зависимостей
COPY pyproject.toml poetry.lock* ./

# Настройте Poetry для установки зависимостей в систему, а не в виртуальное окружение
RUN poetry config virtualenvs.create false

# Установите зависимости проекта
RUN poetry install --no-dev --no-interaction --no-ansi

# Явно установите Pillow, чтобы убедиться, что он доступен
RUN pip install Pillow

# Копируйте файлы проекта в контейнер
COPY . .

## Установите необходимые пакеты
#RUN apt-get update && apt-get install -y nano
#
## Установите Poetry
#RUN pip install poetry Pillow
#
## Настройте Poetry:
## - отключите создание виртуального окружения внутри Docker контейнера
## - установите зависимости проекта, используя файлы pyproject.toml и poetry.lock
#RUN poetry config virtualenvs.create false \
#    && poetry install --no-dev --no-interaction --no-ansi
#
## Активируйте виртуальное окружение
#ENV PATH="/.venv/bin:$PATH"

# Установите uvicorn внутри контейнера
RUN pip install uvicorn

# Команда для запуска приложения
CMD ["uvicorn", "dz_fastapi.main:app", "--host", "0.0.0.0", "--reload"]
