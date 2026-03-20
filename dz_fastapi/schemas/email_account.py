from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class EmailAccountBase(BaseModel):
    name: str
    email: EmailStr
    password: str = ''
    transport: Literal['smtp', 'http_api'] = 'smtp'
    imap_host: Optional[str] = None
    imap_port: int = 993
    imap_folder: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 465
    smtp_use_ssl: bool = True
    http_api_provider: Optional[Literal['resend', 'brevo']] = None
    http_api_url: Optional[str] = None
    http_api_key: Optional[str] = None
    http_api_timeout: int = 20
    purposes: List[str] = Field(default_factory=list)
    is_active: bool = True

    @field_validator('name', mode='before')
    def name_not_empty(cls, v):
        if not str(v).strip():
            raise ValueError('Name must not be empty')
        return v

    @field_validator('password', mode='before')
    def password_to_empty_string(cls, v):
        if v is None:
            return ''
        return str(v)

    model_config = ConfigDict(from_attributes=True)


class EmailAccountCreate(EmailAccountBase):
    pass


class EmailAccountUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    transport: Optional[Literal['smtp', 'http_api']] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_folder: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_use_ssl: Optional[bool] = None
    http_api_provider: Optional[Literal['resend', 'brevo']] = None
    http_api_url: Optional[str] = None
    http_api_key: Optional[str] = None
    http_api_timeout: Optional[int] = None
    purposes: Optional[List[str]] = None
    is_active: Optional[bool] = None

    model_config = ConfigDict(from_attributes=True)


class EmailAccountResponse(EmailAccountBase):
    id: int
    oauth_provider: Optional[str] = None
    oauth_connected_at: Optional[datetime] = None


class EmailAccountTestRequest(BaseModel):
    imap: bool = True
    smtp: bool = True
    folder: Optional[str] = None
    real_send: bool = False
    to_email: Optional[EmailStr] = None


class EmailAccountTestResponse(BaseModel):
    imap_ok: Optional[bool] = None
    imap_error: Optional[str] = None
    smtp_ok: Optional[bool] = None
    smtp_error: Optional[str] = None
    outbound_transport: Optional[str] = None
    outbound_note: Optional[str] = None
