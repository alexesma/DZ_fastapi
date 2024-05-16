import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    app_title: str = os.getenv(
        'APP_TITLE',
        'Приложения для работы DragonZap'
    )
    app_description: str = os.getenv(
        'APP_DESCRIPTION',
        'Проект DragonZap на FastAPI'
    )
    database_url: str

    class Config:
        extra = "allow"
        env_file = '.env'


settings = Settings()
