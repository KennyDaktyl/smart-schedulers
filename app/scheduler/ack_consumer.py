from __future__ import annotations

import asyncio
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator
from uuid import UUID

from sqlalchemy.orm import Session

from smart_common.core.config import settings
from smart_common.core.db import get_db
from smart_common.enums.device_event import DeviceEventName
from smart_common.enums.event import EventType
from smart_common.enums.scheduler import SchedulerCommandAction, SchedulerCommandStatus
from smart_common.nats.client import nats_client
from smart_common.repositories.scheduler_command_repository import SchedulerCommandRepository
from smart_common.repositories.scheduler_runtime_repository import SchedulerRuntimeRepository
from smart_common.services.scheduler_audit_service import SchedulerAuditService

logger = logging.getLogger(__name__)


class SchedulerAckConsumer:
    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._subscription = None
        self._subject = (
            f"{settings.STREAM_NAME}.*.command.{EventType.DEVICE_COMMAND.value}.ack"
        )

    async def run(self) -> None:
        logger.info("Scheduler ACK consumer starting | subject=%s", self._subject)
        self._subscription = await nats_client.subscribe(self._subject, self._handle_ack)
        await self._stop_event.wait()
        await self._unsubscribe()
        logger.info("Scheduler ACK consumer stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _unsubscribe(self) -> None:
        if not self._subscription:
            return
        try:
            await self._subscription.unsubscribe()
        except Exception:
            logger.exception("Failed to unsubscribe ACK consumer")
        finally:
            self._subscription = None

    async def _handle_ack(self, msg) -> None:
        try:
            raw = json.loads(msg.data.decode())
            data = raw.get("data") if isinstance(raw, dict) else None
            if not isinstance(data, dict):
                logger.warning("ACK payload without data object | subject=%s", msg.subject)
                return

            command_id = _parse_uuid(data.get("command_id"))
            if command_id is None:
                logger.warning(
                    "ACK missing command_id; cannot correlate | subject=%s payload=%s",
                    msg.subject,
                    raw,
                )
                return

            transport_ok = bool(data.get("ok", False))
            actual_state = _ack_state(data)
            now_utc = datetime.now(timezone.utc)

            with _db_session() as db:
                command_repo = SchedulerCommandRepository(db)
                runtime_repo = SchedulerRuntimeRepository(db)
                audit = SchedulerAuditService(db)

                command, changed = command_repo.mark_ack(
                    command_id=command_id,
                    transport_ok=transport_ok,
                    actual_state=actual_state,
                    now_utc=now_utc,
                )
                if command is None:
                    db.rollback()
                    logger.warning(
                        "ACK for unknown command_id | command_id=%s subject=%s",
                        command_id,
                        msg.subject,
                    )
                    return
                if not changed:
                    db.rollback()
                    return

                if (
                    command.status == SchedulerCommandStatus.ACK_OK
                    and isinstance(actual_state, bool)
                ):
                    runtime_repo.update_device_state(
                        device_id=command.device_id,
                        is_on=actual_state,
                        changed_at=now_utc,
                    )

                audit.create_event(
                    device_id=command.device_id,
                    event_name=_event_name_for_ack(command.action, command.status),
                    trigger_reason=(
                        "ACK_OK"
                        if command.status == SchedulerCommandStatus.ACK_OK
                        else "ACK_FAILED"
                    ),
                    pin_state=actual_state,
                )
                db.commit()

            logger.info(
                "ACK correlated | command_id=%s status=%s transport_ok=%s actual_state=%s",
                command_id,
                command.status.value if command else "UNKNOWN",
                transport_ok,
                actual_state,
            )
        except Exception:
            logger.exception("Failed to handle scheduler ACK message")


def _parse_uuid(value: object) -> UUID | None:
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


def _ack_state(data: dict) -> bool | None:
    for key in ("actual_state", "is_on"):
        value = data.get(key)
        if isinstance(value, bool):
            return value
    return None


def _event_name_for_ack(
    action: SchedulerCommandAction,
    status: SchedulerCommandStatus,
) -> DeviceEventName:
    if status != SchedulerCommandStatus.ACK_OK:
        return DeviceEventName.SCHEDULER_ACK_FAILED
    if action == SchedulerCommandAction.ON:
        return DeviceEventName.SCHEDULER_TRIGGER_ON
    return DeviceEventName.DEVICE_OFF


@contextmanager
def _db_session() -> Iterator[Session]:
    db_gen = get_db()
    db = next(db_gen)
    try:
        yield db
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass
