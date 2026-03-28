from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from dz_fastapi.core.email_folders import (DEFAULT_IMAP_FOLDER,
                                           normalize_imap_folder,
                                           parse_imap_additional_folders)


class EmailAccountBase(BaseModel):
    name: str
    email: EmailStr
    password: str = ''
    transport: Literal['smtp', 'gmail_api', 'resend_api'] = 'smtp'
    resend_api_key: Optional[str] = None
    resend_timeout: int = 20
    imap_host: Optional[str] = None
    imap_port: int = 993
    imap_folder: Optional[str] = DEFAULT_IMAP_FOLDER
    imap_additional_folders: List[str] = Field(default_factory=list)
    smtp_host: Optional[str] = None
    smtp_port: int = 465
    smtp_use_ssl: bool = True
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

    @field_validator('imap_host', 'smtp_host', mode='before')
    def strip_optional_mail_fields(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator('imap_folder', mode='before')
    def normalize_imap_folder_value(cls, v):
        return normalize_imap_folder(v)

    @field_validator('imap_additional_folders', mode='before')
    def normalize_imap_additional_folders(cls, v):
        return parse_imap_additional_folders(v)

    model_config = ConfigDict(from_attributes=True)


class EmailAccountCreate(EmailAccountBase):
    pass


class EmailAccountUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    transport: Optional[Literal['smtp', 'gmail_api', 'resend_api']] = None
    resend_api_key: Optional[str] = None
    resend_timeout: Optional[int] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_folder: Optional[str] = None
    imap_additional_folders: Optional[List[str]] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_use_ssl: Optional[bool] = None
    purposes: Optional[List[str]] = None
    is_active: Optional[bool] = None

    @field_validator(
        'name',
        'password',
        'imap_host',
        'smtp_host',
        mode='before',
    )
    def strip_update_mail_fields(cls, v):
        if v is None:
            return None
        return str(v).strip()

    @field_validator('imap_folder', mode='before')
    def normalize_update_imap_folder(cls, v):
        if v is None:
            return None
        return normalize_imap_folder(v)

    @field_validator('imap_additional_folders', mode='before')
    def normalize_update_imap_additional_folders(cls, v):
        if v is None:
            return None
        return parse_imap_additional_folders(v)

    model_config = ConfigDict(from_attributes=True)


class EmailAccountResponse(EmailAccountBase):
    id: int
    transport: Literal['smtp', 'gmail_api', 'resend_api'] = 'smtp'
    oauth_provider: Optional[str] = None
    oauth_connected_at: Optional[datetime] = None
    resend_last_received_at: Optional[datetime] = None


class EmailAccountTestRequest(BaseModel):
    imap: bool = True
    smtp: bool = True
    folder: Optional[str] = None
    real_send: bool = False
    to_email: Optional[EmailStr] = None


class EmailAccountTestResponse(BaseModel):
    imap_ok: Optional[bool] = None
    imap_error: Optional[str] = None
    inbound_note: Optional[str] = None
    smtp_ok: Optional[bool] = None
    smtp_error: Optional[str] = None
    outbound_transport: Optional[str] = None
    outbound_note: Optional[str] = None


class EmailAccountGoogleTokenRequest(BaseModel):
    refresh_token: str
