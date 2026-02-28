from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from smart_common.enums.device_event import DeviceEventName, DeviceEventType
from smart_common.models.device_event import DeviceEvent


class SchedulerAuditService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create_event(
        self,
        *,
        device_id: int,
        event_name: DeviceEventName,
        trigger_reason: str,
        measured_value: float | None = None,
        measured_unit: str | None = None,
        pin_state: bool | None = None,
    ) -> None:
        self.db.add(
            DeviceEvent(
                device_id=device_id,
                event_type=DeviceEventType.SCHEDULER,
                event_name=event_name,
                device_state=_device_state_from_pin(pin_state),
                pin_state=pin_state,
                measured_value=measured_value,
                measured_unit=measured_unit,
                trigger_reason=trigger_reason,
                source="scheduler",
                created_at=datetime.now(timezone.utc),
            )
        )


def _device_state_from_pin(pin_state: bool | None) -> str | None:
    if pin_state is True:
        return "ON"
    if pin_state is False:
        return "OFF"
    return None

