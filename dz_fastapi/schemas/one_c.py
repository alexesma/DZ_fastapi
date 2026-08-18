from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

ONE_C_OUTBOX_CONTRACT_VERSION = "1.0"


class OneCOutboxEvent(BaseModel):
    id: int
    event_uid: str
    entity_type: str
    entity_id: int
    event_type: str
    payload_version: int
    idempotency_key: str
    status: str
    attempt_count: int
    last_attempt_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    external_id: Optional[str] = None
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    payload: Dict[str, Any]


class OneCOutboxEnvelope(BaseModel):
    protocol: str = "dz-1c-outbox"
    contract_version: str = ONE_C_OUTBOX_CONTRACT_VERSION
    batch_uid: Optional[str] = None
    content_hash: Optional[str] = None
    formed_at: Optional[datetime] = None
    attempt: int = 0
    events: List[OneCOutboxEvent] = Field(default_factory=list)


class OneCOutboxPingResponse(BaseModel):
    protocol: str = "dz-1c-outbox"
    contract_version: str = ONE_C_OUTBOX_CONTRACT_VERSION
    status: str = "ok"
    server_time: datetime


class OneCEventAckResult(BaseModel):
    event_uid: str = Field(min_length=1, max_length=36)
    success: bool = True
    external_id: Optional[str] = Field(default=None, max_length=255)
    error: Optional[str] = Field(default=None, max_length=4000)

    @model_validator(mode="after")
    def validate_result(self):
        if self.success and not str(self.external_id or "").strip():
            raise ValueError("Для успешно обработанного события нужен external_id документа 1С")
        if not self.success and not str(self.error or "").strip():
            raise ValueError("Для ошибочного события нужен текст ошибки")
        return self


class OneCBatchAckRequest(BaseModel):
    contract_version: str = ONE_C_OUTBOX_CONTRACT_VERSION
    success: bool = True
    error: Optional[str] = Field(default=None, max_length=4000)
    external_ids: Dict[str, str] = Field(default_factory=dict)
    results: List[OneCEventAckResult] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_results(self):
        event_uids = [item.event_uid for item in self.results]
        if len(event_uids) != len(set(event_uids)):
            raise ValueError("Результат одного события не должен повторяться")
        if self.contract_version != ONE_C_OUTBOX_CONTRACT_VERSION:
            raise ValueError(f"Поддерживается контракт обмена {ONE_C_OUTBOX_CONTRACT_VERSION}")
        return self
