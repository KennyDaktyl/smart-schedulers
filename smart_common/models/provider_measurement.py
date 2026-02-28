from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from smart_common.core.db import Base


class ProviderMeasurement(Base):
    __tablename__ = "provider_measurements"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_id: Mapped[int] = mapped_column(
        ForeignKey("providers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    measured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    measured_value: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    measured_unit: Mapped[str | None] = mapped_column(String(length=16), nullable=True)

