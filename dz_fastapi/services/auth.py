import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.config import settings
from dz_fastapi.models.user import User, UserRole, UserStatus

logger = logging.getLogger("dz_fastapi")
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def _normalize_password(password: str) -> str:
    password_bytes = password.encode("utf-8")
    if len(password_bytes) <= 72:
        return password
    # bcrypt ignores anything after 72 bytes; hash long passwords first
    return hashlib.sha256(password_bytes).hexdigest()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    normalized = _normalize_password(plain_password)
    return pwd_context.verify(normalized, hashed_password)


def get_password_hash(password: str) -> str:
    normalized = _normalize_password(password)
    return pwd_context.hash(normalized)


def create_access_token(
        subject: str, expires_delta: timedelta | None = None
) -> str:
    expire = datetime.now(timezone.utc) + (
        expires_delta
        if expires_delta
        else timedelta(minutes=settings.jwt_access_token_expire_minutes)
    )
    to_encode: dict[str, Any] = {"sub": subject, "exp": expire}
    return jwt.encode(
        to_encode,
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm
    )


def decode_access_token(token: str) -> dict[str, Any] | None:
    try:
        return jwt.decode(
            token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
        )
    except JWTError:
        return None


async def ensure_admin_user(session: AsyncSession) -> None:
    if not settings.admin_email or not settings.admin_password:
        logger.warning(
            'ADMIN_EMAIL or ADMIN_PASSWORD not set; admin not created'
        )
        return
    result = await session.execute(
        select(User).where(User.role == UserRole.ADMIN)
    )
    existing = result.scalars().first()
    if existing:
        logger.info('Admin already exists; bootstrap skipped')
        return
    admin = User(
        email=settings.admin_email.lower().strip(),
        password_hash=get_password_hash(settings.admin_password),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
        approved_at=datetime.now(timezone.utc),
    )
    session.add(admin)
    await session.commit()
    logger.info("Bootstrap admin created from env")
