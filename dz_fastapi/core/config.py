import os

from pydantic import ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    app_title: str = 'Приложения для работы DragonZap'
    app_description: str = 'Проект DragonZap на FastAPI'
    database_url: str
    test_database_url: str
    asyncpg_dsn: str
    use_test_db: bool = False
    database_echo: bool = False

    model_config = SettingsConfigDict(
        extra="allow",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    def get_database_url(self, test: bool = False) -> str:
        return self.test_database_url if test else self.database_url

    def get_asyncpg_dsn(self) -> str:
        return self.asyncpg_dsn


settings = Settings()
