from __future__ import annotations

from uuid import UUID as UUIDType

from sqlalchemy import Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smart_common.core.db import Base


class Microcontroller(Base):
    __tablename__ = "microcontrollers"

    id: Mapped[int] = mapped_column(primary_key=True)
    uuid: Mapped[UUIDType] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    power_provider_id: Mapped[int | None] = mapped_column(
        ForeignKey("providers.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

