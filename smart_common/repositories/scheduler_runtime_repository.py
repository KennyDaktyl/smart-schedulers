from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import func
from sqlalchemy.orm import Session

from smart_common.enums.device import DeviceMode
from smart_common.enums.scheduler import SchedulerDayOfWeek
from smart_common.models.device import Device
from smart_common.models.microcontroller import Microcontroller
from smart_common.models.provider import Provider
from smart_common.models.provider_measurement import ProviderMeasurement
from smart_common.models.scheduler import Scheduler
from smart_common.models.scheduler_slot import SchedulerSlot
from smart_common.schemas.scheduler_runtime import DueSchedulerEntry


class SchedulerRuntimeRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def fetch_due_entries(
        self,
        *,
        day_of_week: SchedulerDayOfWeek,
        hhmm: str,
    ) -> list[DueSchedulerEntry]:
        start_col = func.coalesce(SchedulerSlot.start_utc_time, SchedulerSlot.start_time)
        end_col = func.coalesce(SchedulerSlot.end_utc_time, SchedulerSlot.end_time)

        rows = (
            self.db.query(
                Device.id,
                Device.uuid,
                Device.device_number,
                Microcontroller.uuid,
                Microcontroller.power_provider_id,
                SchedulerSlot.id,
                SchedulerSlot.use_power_threshold,
                SchedulerSlot.power_provider_id,
                SchedulerSlot.power_threshold_value,
                SchedulerSlot.power_threshold_unit,
            )
            .join(Microcontroller, Device.microcontroller_id == Microcontroller.id)
            .join(Scheduler, Device.scheduler_id == Scheduler.id)
            .join(SchedulerSlot, SchedulerSlot.scheduler_id == Scheduler.id)
            .filter(
                Device.mode == DeviceMode.SCHEDULE,
                Microcontroller.enabled.is_(True),
                SchedulerSlot.day_of_week == day_of_week,
                start_col <= hhmm,
                hhmm < end_col,
            )
            .all()
        )

        result: list[DueSchedulerEntry] = []
        for row in rows:
            result.append(
                DueSchedulerEntry(
                    device_id=row[0],
                    device_uuid=row[1],
                    device_number=row[2],
                    microcontroller_uuid=row[3],
                    microcontroller_power_provider_id=row[4],
                    slot_id=row[5],
                    use_power_threshold=bool(row[6]),
                    power_provider_id=row[7],
                    power_threshold_value=_to_float(row[8]),
                    power_threshold_unit=_normalize_unit(row[9]),
                )
            )
        return result

    def get_provider(self, provider_id: int) -> Provider | None:
        return self.db.query(Provider).filter(Provider.id == provider_id).first()

    def get_latest_measurement(self, provider_id: int) -> ProviderMeasurement | None:
        return (
            self.db.query(ProviderMeasurement)
            .filter(ProviderMeasurement.provider_id == provider_id)
            .order_by(ProviderMeasurement.measured_at.desc())
            .first()
        )

    def update_device_state(
        self,
        *,
        device_id: int,
        is_on: bool,
        changed_at: datetime,
    ) -> None:
        device = self.db.query(Device).filter(Device.id == device_id).first()
        if not device:
            return
        device.manual_state = is_on
        device.last_state_change_at = changed_at


def _to_float(value: float | int | Decimal | None) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_unit(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.lower() == "kw":
        return "kW"
    if normalized.lower() == "mw":
        return "MW"
    if normalized.lower() == "w":
        return "W"
    return normalized

