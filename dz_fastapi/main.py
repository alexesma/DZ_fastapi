import os
import logging
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI

# Настройка логгера
logger = logging.getLogger('dz_fastapi')
logger.setLevel(logging.DEBUG)

# Создание обработчика для записи логов в файл
handler = RotatingFileHandler('dz_fastapi.log', maxBytes=200000, backupCount=100)
handler.setLevel(logging.DEBUG)

# Создание формата для логов
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Добавление обработчика к логгеру
logger.addHandler(handler)
from fastapi.staticfiles import StaticFiles
from dz_fastapi.api.autopart import router as autopart_router
from dz_fastapi.api.brand import router as brand_router
from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_engine

app = FastAPI(title=settings.app_title, description=settings.app_description)

app.mount("/uploads", StaticFiles(directory=os.path.join(os.getcwd(), "uploads")), name="uploads")
app.include_router(autopart_router)
app.include_router(brand_router)
