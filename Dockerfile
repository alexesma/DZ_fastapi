FROM python:3.12

ENV POETRY_VERSION=1.8.2 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1 \
    POETRY_SOLVER_LAZY_WHEEL=false \
    PIP_DEFAULT_TIMEOUT=120

WORKDIR /app

RUN apt-get update && apt-get install -y \
    nano \
    libjpeg-dev \
    libpng-dev \
    libfreetype6-dev \
    unrar-free \
    curl \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel \
  && pip install "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock ./

RUN poetry install --only main --no-root --no-ansi

COPY . .
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["/entrypoint.sh"]
