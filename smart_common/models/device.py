from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID as UUIDType

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smart_common.core.db import Base
from smart_common.enums.device import DeviceMode


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(primary_key=True)
    uuid: Mapped[UUIDType] = mapped_column(UUID(as_uuid=True), nullable=False)
    microcontroller_id: Mapped[int] = mapped_column(
        ForeignKey("microcontrollers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scheduler_id: Mapped[int | None] = mapped_column(
        ForeignKey("schedulers.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    device_number: Mapped[int] = mapped_column(Integer, nullable=False)
    mode: Mapped[DeviceMode] = mapped_column(
        Enum(DeviceMode, name="device_mode_enum"),
        nullable=False,
    )
    manual_state: Mapped[bool | None] = mapped_column(Boolean)
    last_state_change_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    microcontroller = relationship("Microcontroller")
    scheduler = relationship("Scheduler")

