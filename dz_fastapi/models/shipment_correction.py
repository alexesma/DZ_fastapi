"""Agreed commercial changes to an already posted customer shipment.

These documents change the accounting value of the shipment. They do not
move physical stock; a physical return is a separate warehouse operation.
"""

from enum import StrEnum, unique

from sqlalchemy import DECIMAL, CheckConstraint, Column, Date, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import SyncStatus


@unique
class ShipmentCorrectionStatus(StrEnum):
    DRAFT = "draft"
    POSTED = "posted"


class ShipmentCorrection(Base):
    __tablename__ = "shipmentcorrection"

    shipment_id = Column(
        Integer, ForeignKey("shipmentdocument.id", ondelete="RESTRICT"),
        nullable=False, index=True,
    )
    doc_number = Column(String(100), nullable=False, index=True)
    doc_date = Column(Date, nullable=False)
    basis_number = Column(String(120), nullable=False)
    basis_date = Column(Date, nullable=False)
    basis_description = Column(String(255), nullable=False)
    reason = Column(Text, nullable=False)
    document_function = Column(String(16), nullable=False, default="КСЧФДИС")
    status = Column(
        SAEnum(
            ShipmentCorrectionStatus,
            name="shipmentcorrectionstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=ShipmentCorrectionStatus.DRAFT,
        index=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    posted_at = Column(DateTime(timezone=True), nullable=True)
    created_by_user_id = Column(
        Integer, ForeignKey("app_user.id", ondelete="SET NULL"), nullable=True,
    )
    diadoc_outgoing_document_id = Column(
        Integer, ForeignKey("diadocoutgoingdocument.id", ondelete="SET NULL"),
        nullable=True, index=True,
    )
    external_id = Column(String(100), nullable=True, index=True)
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
    )

    shipment = relationship("ShipmentDocument", lazy="joined")
    diadoc_outgoing_document = relationship("DiadocOutgoingDocument")
    items = relationship(
        "ShipmentCorrectionItem",
        back_populates="correction",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="ShipmentCorrectionItem.id",
    )

    __table_args__ = (
        UniqueConstraint("doc_number", name="uq_shipmentcorrection_number"),
    )


class ShipmentCorrectionItem(Base):
    __tablename__ = "shipmentcorrectionitem"

    correction_id = Column(
        Integer, ForeignKey("shipmentcorrection.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    shipment_item_id = Column(
        Integer, ForeignKey("shipmentdocumentitem.id", ondelete="RESTRICT"),
        nullable=False, index=True,
    )
    original_number = Column(Integer, nullable=False)
    product_name = Column(String(512), nullable=False)
    vendor_code = Column(String(255), nullable=True)
    quantity_before = Column(Integer, nullable=False)
    quantity_after = Column(Integer, nullable=False)
    price_before = Column(DECIMAL(12, 2), nullable=False)
    price_after = Column(DECIMAL(12, 2), nullable=False)
    vat_rate = Column(DECIMAL(5, 2), nullable=False, default=22)

    correction = relationship("ShipmentCorrection", back_populates="items")
    shipment_item = relationship("ShipmentDocumentItem", lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "correction_id", "shipment_item_id",
            name="uq_shipmentcorrection_item",
        ),
        CheckConstraint(
            "quantity_before >= 0 AND quantity_after >= 0",
            name="ck_shipmentcorrection_item_qty",
        ),
        CheckConstraint(
            "price_before >= 0 AND price_after >= 0",
            name="ck_shipmentcorrection_item_price",
        ),
    )
