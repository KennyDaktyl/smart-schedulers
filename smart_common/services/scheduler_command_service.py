from __future__ import annotations

import logging

from smart_common.enums.device import DeviceMode
from smart_common.enums.event import EventType
from smart_common.nats.event_dispatcher import EventDispatcher
from smart_common.nats.event_helpers import ack_subject_for_entity, subject_for_entity
from smart_common.nats.publisher import NatsPublisher
from smart_common.schemas.device_events import DeviceCommandPayload
from smart_common.schemas.scheduler_runtime import AckResult, DueSchedulerEntry

logger = logging.getLogger(__name__)


class SchedulerCommandService:
    def __init__(self, *, ack_timeout_sec: float):
        self._ack_timeout_sec = max(1.0, ack_timeout_sec)
        self._events = EventDispatcher(
            NatsPublisher(),
            default_source="smart-schedulers",
        )

    async def send_switch_on_command(self, *, entry: DueSchedulerEntry) -> AckResult:
        payload = DeviceCommandPayload(
            device_id=entry.device_id,
            device_uuid=str(entry.device_uuid),
            device_number=entry.device_number,
            command="SET_STATE",
            mode=DeviceMode.SCHEDULE.value,
            is_on=True,
        )

        subject = subject_for_entity(
            str(entry.microcontroller_uuid),
            EventType.DEVICE_COMMAND.value,
        )
        ack_subject = ack_subject_for_entity(
            str(entry.microcontroller_uuid),
            EventType.DEVICE_COMMAND.value,
        )

        try:
            result = await self._events.publish_event_and_wait_for_ack(
                entity_type=EventType.DEVICE_COMMAND.value,
                entity_id=str(entry.microcontroller_uuid),
                event_type=EventType.DEVICE_COMMAND.value,
                data=payload,
                predicate=lambda event: _ack_device_id(event) == entry.device_id,
                timeout=self._ack_timeout_sec,
                subject=subject,
                ack_subject=ack_subject,
            )
            data = result.get("data") or {}
            return AckResult(
                ok=bool(data.get("ok", False)),
                is_on=_ack_device_state(data),
                raw_data=data,
            )
        except Exception:
            logger.exception(
                "Scheduler command ACK failed | device_id=%s slot_id=%s",
                entry.device_id,
                entry.slot_id,
            )
            return AckResult(ok=False, is_on=None, raw_data={})


def _ack_device_id(event: dict) -> int | None:
    data = event.get("data") or {}
    if isinstance(data, dict) and "device_id" in data:
        return _to_int(data.get("device_id"))
    ack = data.get("ack")
    if isinstance(ack, dict) and "device_id" in ack:
        return _to_int(ack.get("device_id"))
    return None


def _ack_device_state(ack_data: dict) -> bool | None:
    if not isinstance(ack_data, dict):
        return None
    for key in ("is_on", "actual_state"):
        value = ack_data.get(key)
        if isinstance(value, bool):
            return value
    return None


def _to_int(value: int | str | None) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None

