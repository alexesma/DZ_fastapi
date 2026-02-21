from typing import List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class EmailAccountBase(BaseModel):
    name: str
    email: EmailStr
    password: str
    imap_host: Optional[str] = None
    imap_port: int = 993
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

    model_config = ConfigDict(from_attributes=True)


class EmailAccountCreate(EmailAccountBase):
    pass


class EmailAccountUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_use_ssl: Optional[bool] = None
    purposes: Optional[List[str]] = None
    is_active: Optional[bool] = None

    model_config = ConfigDict(from_attributes=True)


class EmailAccountResponse(EmailAccountBase):
    id: int
