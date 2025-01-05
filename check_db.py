import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import text

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')


async def check_database_connection():
    try:
        engine = create_async_engine(DATABASE_URL)
        async with AsyncSession(engine) as session:
            async with session.begin():
                await session.execute(text("SELECT 1"))
        print('Соединение с базой данных успешно установлено')
    except OperationalError as e:
        print('Ошибка при соединении с базой данных:', e)

if __name__ == "__main__":
    asyncio.run(check_database_connection())
