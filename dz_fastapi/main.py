from fastapi import FastAPI
from dz_fastapi.api.autopart import router as autopart_router
from dz_fastapi.core.config import settings


app = FastAPI(title=settings.app_title, description=settings.app_description)


app.include_router(autopart_router)