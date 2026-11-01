"""ЭТрН через Диадок: справочники и перевозки.

Справочники (перевозчики, водители, ТС, адреса) нужны для титула Т1;
`EtrnWaybill` — одна перевозка, исходящая (мы грузоотправитель, Т1) или
входящая (мы грузополучатель, Т3). Группируем по УИД ГИС ЭПД (mt-id):
титулы Т3/Т4 приходят отдельным сообщением, MessageId у них другой.
"""
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class TransportCarrier(Base):
    __tablename__ = "transportcarrier"

    name = Column(String(500), nullable=False)
    inn = Column(String(32), nullable=True, index=True)
    kpp = Column(String(32), nullable=True)
    address = Column(String(1000), nullable=True)
    phone = Column(String(64), nullable=True)
    # Ящик перевозчика в Диадоке: без него Т1 отправить нельзя.
    diadoc_box_id = Column(String(64), nullable=True, index=True)
    comment = Column(String(1000), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)


class TransportDriver(Base):
    __tablename__ = "transportdriver"

    carrier_id = Column(
        Integer,
        ForeignKey("transportcarrier.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    last_name = Column(String(120), nullable=False)
    first_name = Column(String(120), nullable=False)
    middle_name = Column(String(120), nullable=True)
    license_series = Column(String(16), nullable=True)
    license_number = Column(String(32), nullable=True)
    license_date = Column(Date, nullable=True)
    phone = Column(String(64), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    carrier = relationship("TransportCarrier", lazy="selectin")


class TransportVehicle(Base):
    __tablename__ = "transportvehicle"

    carrier_id = Column(
        Integer,
        ForeignKey("transportcarrier.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    plate_number = Column(String(32), nullable=False)
    brand = Column(String(120), nullable=True)
    vehicle_type = Column(String(120), nullable=True)
    max_weight = Column(Numeric(10, 3), nullable=True)
    capacity = Column(Numeric(10, 3), nullable=True)
    trailer_number = Column(String(32), nullable=True)
    # 1 — собственное, 2 — арендованное и т.п. (ТипВлад в титуле)
    ownership = Column(String(2), nullable=False, default="1")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    carrier = relationship("TransportCarrier", lazy="selectin")


class TransportAddress(Base):
    """Адрес погрузки/выгрузки (склад компании, адрес клиента, иной)."""

    __tablename__ = "transportaddress"

    # warehouse | customer | other
    kind = Column(String(16), nullable=False, default="other")
    label = Column(String(255), nullable=False)
    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    zip_code = Column(String(6), nullable=False)
    region_code = Column(String(2), nullable=False)
    city = Column(String(255), nullable=True)
    locality = Column(String(255), nullable=True)
    street = Column(String(255), nullable=True)
    building = Column(String(20), nullable=True)
    block = Column(String(20), nullable=True)
    apartment = Column(String(20), nullable=True)
    other_info = Column(String(1000), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)


class EtrnWaybill(Base):
    __tablename__ = "etrnwaybill"

    environment = Column(String(32), nullable=False, default="staging")
    # outgoing — мы грузоотправитель (Т1); incoming — грузополучатель (Т3)
    direction = Column(String(16), nullable=False, index=True)
    # draft | sign_pending | sent | in_transit | delivered | refused | error
    status = Column(String(24), nullable=False, default="draft", index=True)
    box_id_guid = Column(String(64), nullable=True)
    counteragent_box_id = Column(String(64), nullable=True)
    number = Column(String(120), nullable=True, index=True)
    waybill_date = Column(Date, nullable=True)

    shipment_id = Column(
        Integer,
        ForeignKey("shipmentdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    carrier_id = Column(
        Integer,
        ForeignKey("transportcarrier.id", ondelete="SET NULL"),
        nullable=True,
    )
    driver_id = Column(
        Integer,
        ForeignKey("transportdriver.id", ondelete="SET NULL"),
        nullable=True,
    )
    vehicle_id = Column(
        Integer,
        ForeignKey("transportvehicle.id", ondelete="SET NULL"),
        nullable=True,
    )
    incoming_document_id = Column(
        Integer,
        ForeignKey("diadocincomingdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Снимок введённых данных Т1: груз, адреса, перевозчик «разово»,
    # водитель/ТС «разово», МЧД, время погрузки.
    payload = Column(JSON, nullable=False, default=dict)

    t1_message_id = Column(String(255), nullable=True, index=True)
    t1_entity_id = Column(String(255), nullable=True)
    mt_id = Column(String(120), nullable=True, index=True)
    kl_status = Column(String(64), nullable=True)
    gis_status_type = Column(String(32), nullable=True)
    gis_status_text = Column(String(500), nullable=True)
    # Что известно о приёмке (мы получатель) и о титулах участников
    # none | pending_sign | signed_here | signed_external
    t3_state = Column(String(24), nullable=False, default="none")
    # accepted | partial | rejected
    t3_result = Column(String(16), nullable=True)
    t3_message_id = Column(String(255), nullable=True)
    t3_data = Column(JSON, nullable=False, default=dict)

    last_error = Column(String(2000), nullable=True)
    problem_notified_key = Column(String(255), nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )

    carrier = relationship("TransportCarrier", lazy="selectin")
    driver = relationship("TransportDriver", lazy="selectin")
    vehicle = relationship("TransportVehicle", lazy="selectin")
    customer = relationship("Customer", lazy="selectin")

    __table_args__ = (
        UniqueConstraint(
            "environment",
            "direction",
            "t1_message_id",
            name="uq_etrn_waybill_t1_message",
        ),
    )
