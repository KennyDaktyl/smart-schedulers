from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator
from uuid import UUID

from sqlalchemy.orm import Session

from smart_common.core.db import get_db
from smart_common.enums.device_event import DeviceEventName
from smart_common.enums.scheduler import SchedulerCommandStatus
from smart_common.repositories.scheduler_command_repository import SchedulerCommandRepository
from smart_common.schemas.scheduler_runtime import DispatchCommandEntry
from smart_common.services.scheduler_audit_service import SchedulerAuditService
from smart_common.services.scheduler_command_service import SchedulerCommandService

logger = logging.getLogger(__name__)


class SchedulerDispatcher:
    def __init__(
        self,
        *,
        ack_timeout_sec: float,
        max_concurrency: int,
        batch_size: int,
        poll_interval_sec: float,
        max_retry: int,
        retry_backoff_sec: float,
        retry_jitter_sec: float,
        max_inflight_per_microcontroller: int,
    ) -> None:
        self._stop_event = asyncio.Event()
        self._ack_timeout_sec = max(1.0, ack_timeout_sec)
        self._batch_size = max(1, batch_size)
        self._poll_interval_sec = max(0.05, poll_interval_sec)
        self._max_retry = max(0, max_retry)
        self._retry_backoff_sec = max(0.0, retry_backoff_sec)
        self._retry_jitter_sec = max(0.0, retry_jitter_sec)
        self._max_inflight_per_microcontroller = max(1, max_inflight_per_microcontroller)
        self._max_concurrency = max(1, max_concurrency)
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._command_service = SchedulerCommandService()

    async def run(self) -> None:
        logger.info(
            (
                "Scheduler dispatcher starting | batch_size=%s max_concurrency=%s "
                "ack_timeout_sec=%s max_retry=%s inflight_per_micro=%s"
            ),
            self._batch_size,
            self._max_concurrency,
            self._ack_timeout_sec,
            self._max_retry,
            self._max_inflight_per_microcontroller,
        )
        while not self._stop_event.is_set():
            now_utc = datetime.now(timezone.utc)
            with _db_session() as db:
                repo = SchedulerCommandRepository(db)
                commands = repo.claim_pending_for_dispatch(
                    limit=self._batch_size,
                    now_utc=now_utc,
                    ack_timeout_sec=self._ack_timeout_sec,
                    max_inflight_per_microcontroller=self._max_inflight_per_microcontroller,
                )
                db.commit()

            if not commands:
                await self._sleep_or_stop(self._poll_interval_sec)
                continue

            results = await asyncio.gather(
                *[self._publish(command) for command in commands],
                return_exceptions=True,
            )

            failed_command_ids: list[UUID] = []
            for command, result in zip(commands, results):
                if isinstance(result, Exception):
                    logger.exception(
                        "Dispatcher task failed unexpectedly | command_id=%s",
                        command.command_id,
                        exc_info=result,
                    )
                    failed_command_ids.append(command.command_id)
                    continue
                if result is False:
                    failed_command_ids.append(command.command_id)

            if failed_command_ids:
                await self._handle_publish_failures(failed_command_ids)

            logger.info(
                "Scheduler dispatcher batch processed | claimed=%s failed=%s",
                len(commands),
                len(failed_command_ids),
            )

        logger.info("Scheduler dispatcher stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _publish(self, command: DispatchCommandEntry) -> bool:
        async with self._semaphore:
            try:
                await self._command_service.publish_command(command=command)
                return True
            except Exception:
                logger.exception(
                    "Scheduler dispatch publish failed | command_id=%s device_id=%s action=%s",
                    command.command_id,
                    command.device_id,
                    command.action.value,
                )
                return False

    async def _handle_publish_failures(self, command_ids: list[UUID]) -> None:
        if not command_ids:
            return

        now_utc = datetime.now(timezone.utc)
        with _db_session() as db:
            repo = SchedulerCommandRepository(db)
            audit = SchedulerAuditService(db)
            final_failures = 0

            for command_id in command_ids:
                updated = repo.mark_publish_failure(
                    command_id=command_id,
                    now_utc=now_utc,
                    max_retry=self._max_retry,
                    retry_backoff_sec=self._retry_backoff_sec,
                    retry_jitter_sec=self._retry_jitter_sec,
                )
                if not updated:
                    continue
                if updated.status == SchedulerCommandStatus.ACK_FAIL:
                    final_failures += 1
                    audit.create_event(
                        device_id=updated.device_id,
                        event_name=DeviceEventName.SCHEDULER_ACK_FAILED,
                        trigger_reason="DISPATCH_PUBLISH_FAILED",
                        pin_state=None,
                    )

            db.commit()

        if final_failures > 0:
            logger.warning(
                "Scheduler dispatcher final publish failures | count=%s",
                final_failures,
            )

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
