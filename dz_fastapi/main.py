import asyncio
import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dz_fastapi.api.auth import router as auth_router
from dz_fastapi.api.autopart import router as autopart_router
from dz_fastapi.api.brand import router as brand_router
from dz_fastapi.api.customer_order import router as customer_order_router
from dz_fastapi.api.email_account import router as email_account_router
from dz_fastapi.api.order import router as order_router
from dz_fastapi.api.partner import router as partner_router
from dz_fastapi.api.webchat import router as webchat_router
from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.auth import ensure_admin_user
from dz_fastapi.services.scheduler import start_scheduler
from dz_fastapi.services.telegram_bot import start_telegram_bot

# --- Логирование ---
# Настройка логгера
logger = logging.getLogger('dz_fastapi')
logger.setLevel(logging.DEBUG)

# Создание обработчика для записи логов в файл
handler = RotatingFileHandler(
    'dz_fastapi.log', maxBytes=200000, backupCount=100
)
handler.setLevel(logging.DEBUG)

# Создание формата для логов
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
# Добавление обработчика к логгеру
logger.addHandler(handler)


# --- Используем одну общую фабрику из app.state ---
async def new_session(app: FastAPI) -> AsyncIterator[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = (
        app.state.session_factory
    )
    async with session_factory() as s:
        yield s


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Создаём одну фабрику сессий и кладём в app.state
    app.state.session_factory = get_async_session()
    try:
        async with app.state.session_factory() as session:
            await ensure_admin_user(session)
    except Exception as e:
        logger.error(f'Failed to ensure admin user: {e}')
    # 2) Стартуем планировщик и сохраняем его, чтобы потом корректно остановить
    scheduler = start_scheduler(app)
    app.state.scheduler = scheduler
    bot_task = None
    try:
        loop = asyncio.get_event_loop()
        bot_task = loop.create_task(
            asyncio.to_thread(start_telegram_bot)
        )
    except Exception as e:
        logger.error(f'Failed to start Telegram bot: {e}')
    try:
        yield
    finally:
        # 3) Аккуратно останавливаем планировщик при выключении приложения
        try:
            if app.state.scheduler:
                app.state.scheduler.shutdown(wait=True)
        except Exception as e:
            logger.exception(f'Scheduler shutdown error: {e}')
        # Отменяем задачу бота
        if bot_task:
            bot_task.cancel()


app = FastAPI(
    title=settings.app_title,
    description=settings.app_description,
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": 'ok'}


app.mount(
    '/uploads',
    StaticFiles(directory=os.path.join(os.getcwd(), 'uploads')),
    name='uploads',
)
# CORS настройка!
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        'http://localhost:5173',  # Vite dev server
        'http://localhost:3000',  # Frontend в Docker (dev)
        'http://127.0.0.1:5173',  # Локальный Vite
        'http://127.0.0.1:3000',  # Локальный Docker frontend
        'http://0.0.0.0:3000',  # Локальный Docker frontend (0.0.0.0)
        'http://90.156.158.19',  # Ваш продакшн сервер (frontend)
        'http://90.156.158.19:3000',  # Продакшн с портом
        'https://dragonzap.ru',  # Продакшн домен
    ],
    allow_credentials=True,
    allow_methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allow_headers=['*'],
)
app.include_router(autopart_router)
app.include_router(auth_router)
app.include_router(brand_router)
app.include_router(partner_router)
app.include_router(order_router)
app.include_router(customer_order_router)
app.include_router(email_account_router)
app.include_router(webchat_router)
