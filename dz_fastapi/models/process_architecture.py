from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class ProcessArchitectureAnnotation(Base):
    __tablename__ = "processarchitectureannotation"

    page_key = Column(String(64), nullable=False, index=True)
    section_key = Column(String(96), nullable=False, index=True)
    kind = Column(String(16), nullable=False, index=True)
    anchor_x = Column(Float, nullable=True)
    anchor_y = Column(Float, nullable=True)
    content = Column(Text, nullable=True)
    drawing_data = Column(JSON, nullable=True)
    parent_id = Column(
        ForeignKey("processarchitectureannotation.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    created_by_id = Column(
        ForeignKey("app_user.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    is_resolved = Column(Boolean, nullable=False, default=False)
    resolved_by_id = Column(
        ForeignKey("app_user.id", ondelete="SET NULL"),
        nullable=True,
    )
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_moscow,
        onupdate=now_moscow,
    )

    created_by = relationship("User", foreign_keys=[created_by_id], lazy="selectin")
    resolved_by = relationship("User", foreign_keys=[resolved_by_id], lazy="selectin")
