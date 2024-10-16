import asyncio
import socket
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+asyncpg://admin:1qw2#ER$@dz_db_dev:5432/dz"

async def test_connection():
    try:
        print("Testing database connection...")
        print("Resolving host...")
        host = 'dz_db_dev'
        resolved_ip = socket.gethostbyname(host)  # This should resolve to 127.0.0.1
        print(f"Resolved IP: {resolved_ip}")

        engine = create_async_engine(DATABASE_URL, echo=True)
        async with engine.begin() as conn:
            await conn.run_sync(lambda conn: conn.execute(text("SELECT 1")))
        print("Connection successful")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())
