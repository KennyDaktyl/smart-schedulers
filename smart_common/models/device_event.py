from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from smart_common.core.db import Base
from smart_common.enums.device_event import DeviceEventName, DeviceEventType


class DeviceEvent(Base):
    __tablename__ = "device_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    device_id: Mapped[int] = mapped_column(
        ForeignKey("devices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[DeviceEventType] = mapped_column(
        Enum(DeviceEventType, name="device_event_type_enum"),
        nullable=False,
    )
    event_name: Mapped[DeviceEventName] = mapped_column(
        Enum(DeviceEventName, name="device_event_name_enum"),
        nullable=False,
    )
    device_state: Mapped[str | None] = mapped_column(String, nullable=True)
    pin_state: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    measured_value: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    measured_unit: Mapped[str | None] = mapped_column(String(16), nullable=True)
    trigger_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

