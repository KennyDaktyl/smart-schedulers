from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy.orm import Session

from smart_common.core.db import get_db
from smart_common.enums.device_event import DeviceEventName
from smart_common.repositories.scheduler_command_repository import SchedulerCommandRepository
from smart_common.services.scheduler_audit_service import SchedulerAuditService

logger = logging.getLogger(__name__)


class SchedulerTimeoutSweeper:
    def __init__(
        self,
        *,
        interval_sec: float,
        batch_size: int,
    ) -> None:
        self._stop_event = asyncio.Event()
        self._interval_sec = max(0.1, interval_sec)
        self._batch_size = max(1, batch_size)

    async def run(self) -> None:
        logger.info(
            "Scheduler timeout sweeper starting | interval_sec=%s batch_size=%s",
            self._interval_sec,
            self._batch_size,
        )
        while not self._stop_event.is_set():
            timed_out = 0
            now_utc = datetime.now(timezone.utc)
            with _db_session() as db:
                repo = SchedulerCommandRepository(db)
                audit = SchedulerAuditService(db)
                commands = repo.claim_timeouts(
                    now_utc=now_utc,
                    limit=self._batch_size,
                )
                for command in commands:
                    timed_out += 1
                    audit.create_event(
                        device_id=command.device_id,
                        event_name=DeviceEventName.SCHEDULER_ACK_FAILED,
                        trigger_reason="ACK_TIMEOUT",
                        pin_state=None,
                    )
                db.commit()

            if timed_out > 0:
                logger.warning("Scheduler timeout sweep | timed_out=%s", timed_out)

            await self._sleep_or_stop(self._interval_sec)

        logger.info("Scheduler timeout sweeper stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _sleep_or_stop(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return


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
