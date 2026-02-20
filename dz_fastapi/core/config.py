from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Settings(BaseSettings):
    app_title: str = 'Приложения для работы DragonZap'
    app_description: str = 'Проект DragonZap на FastAPI'
    database_url: str = Field(..., json_schema_extra={'env': 'DATABASE_URL'})
    test_database_url: str = Field(
        ..., json_schema_extra={'env': 'TEST_DATABASE_URL'}
    )
    asyncpg_dsn: str = Field(..., json_schema_extra={'env': 'ASYNC_PG_DSN'})
    use_test_db: bool = False
    database_echo: bool = False
    jwt_secret: str = Field(
        'change-me', json_schema_extra={'env': 'JWT_SECRET'}
    )
    jwt_algorithm: str = 'HS256'
    jwt_access_token_expire_minutes: int = 60 * 24
    auth_cookie_name: str = 'access_token'
    auth_cookie_secure: bool = False
    admin_email: str | None = Field(
        None, json_schema_extra={'env': 'ADMIN_EMAIL'}
    )
    admin_password: str | None = Field(
        None, json_schema_extra={'env': 'ADMIN_PASSWORD'}
    )

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
