from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy.orm import Session

from app.scheduler.idempotency import MinuteIdempotencyStore
from smart_common.core.db import get_db
from smart_common.enums.device_event import DeviceEventName
from smart_common.enums.scheduler import SchedulerDayOfWeek
from smart_common.repositories.scheduler_runtime_repository import SchedulerRuntimeRepository
from smart_common.schemas.scheduler_runtime import DecisionKind, DueSchedulerEntry
from smart_common.services.scheduler_audit_service import SchedulerAuditService
from smart_common.services.scheduler_command_service import SchedulerCommandService
from smart_common.services.scheduler_decision_service import SchedulerDecisionService

logger = logging.getLogger(__name__)


class SchedulerEngine:
    def __init__(
        self,
        *,
        ack_timeout_sec: float,
        max_concurrency: int,
        idempotency_ttl_sec: int,
        redis_prefix: str,
    ) -> None:
        self._stop_event = asyncio.Event()
        self._last_processed_minute: datetime | None = None
        self._decision_service = SchedulerDecisionService()
        self._command_service = SchedulerCommandService(ack_timeout_sec=ack_timeout_sec)
        self._semaphore = asyncio.Semaphore(max(1, max_concurrency))
        self._idempotency = MinuteIdempotencyStore(
            prefix=redis_prefix,
            ttl_sec=idempotency_ttl_sec,
        )

    async def run(self) -> None:
        logger.info("Scheduler engine starting")
        await self._idempotency.start()

        while not self._stop_event.is_set():
            minute_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            if (
                self._last_processed_minute is None
                or minute_utc > self._last_processed_minute
            ):
                await self._process_minute(minute_utc)
                self._last_processed_minute = minute_utc

            await _sleep_until_next_tick(self._stop_event)

        await self._idempotency.close()
        logger.info("Scheduler engine stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _process_minute(self, minute_utc: datetime) -> None:
        day_of_week = _day_of_week(minute_utc)
        hhmm = minute_utc.strftime("%H:%M")

        with _db_session() as db:
            repo = SchedulerRuntimeRepository(db)
            entries = repo.fetch_due_entries(day_of_week=day_of_week, hhmm=hhmm)

        if not entries:
            logger.info("Scheduler minute processed | minute=%s due_entries=0", minute_utc.isoformat())
            return

        allow_entries: list[DueSchedulerEntry] = []

        with _db_session() as db:
            repo = SchedulerRuntimeRepository(db)
            audit = SchedulerAuditService(db)

            provider_cache: dict[int, object | None] = {}
            measurement_cache: dict[int, object | None] = {}

            for entry in entries:
                if not await self._acquire_entry_idempotency(entry=entry, minute_utc=minute_utc):
                    continue

                provider_id = entry.power_provider_id or entry.microcontroller_power_provider_id
                provider = None
                latest = None
                if provider_id is not None:
                    provider = provider_cache.get(provider_id)
                    if provider_id not in provider_cache:
                        provider = repo.get_provider(provider_id)
                        provider_cache[provider_id] = provider

                    latest = measurement_cache.get(provider_id)
                    if provider_id not in measurement_cache:
                        latest = repo.get_latest_measurement(provider_id)
                        measurement_cache[provider_id] = latest

                decision = self._decision_service.decide(
                    entry=entry,
                    now_utc=minute_utc,
                    provider=provider,
                    latest_measurement=latest,
                )

                if decision.kind == DecisionKind.ALLOW_ON:
                    allow_entries.append(entry)
                    continue

                audit.create_event(
                    device_id=entry.device_id,
                    event_name=(
                        DeviceEventName.SCHEDULER_SKIPPED_NO_POWER_DATA
                        if decision.kind == DecisionKind.SKIP_NO_POWER_DATA
                        else DeviceEventName.SCHEDULER_SKIPPED_THRESHOLD_NOT_MET
                    ),
                    trigger_reason=decision.trigger_reason,
                    measured_value=decision.measured_value,
                    measured_unit=decision.measured_unit,
                    pin_state=False,
                )

            db.commit()

        tasks = [
            asyncio.create_task(self._execute_allow_entry(entry), name=f"scheduler-device-{entry.device_id}")
            for entry in allow_entries
        ]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.exception("Allow action task failed", exc_info=result)

        logger.info(
            "Scheduler minute done | minute=%s allow_actions=%s",
            minute_utc.isoformat(),
            len(allow_entries),
        )

    async def _execute_allow_entry(self, entry: DueSchedulerEntry) -> None:
        async with self._semaphore:
            ack = await self._command_service.send_switch_on_command(entry=entry)

        with _db_session() as db:
            repo = SchedulerRuntimeRepository(db)
            audit = SchedulerAuditService(db)

            if ack.ok and isinstance(ack.is_on, bool):
                repo.update_device_state(
                    device_id=entry.device_id,
                    is_on=ack.is_on,
                    changed_at=datetime.now(timezone.utc),
                )

            audit.create_event(
                device_id=entry.device_id,
                event_name=(
                    DeviceEventName.SCHEDULER_TRIGGER_ON
                    if ack.ok
                    else DeviceEventName.SCHEDULER_ACK_FAILED
                ),
                trigger_reason="ACK_OK" if ack.ok else "ACK_FAILED",
                pin_state=ack.is_on,
            )
            db.commit()

    async def _acquire_entry_idempotency(
        self,
        *,
        entry: DueSchedulerEntry,
        minute_utc: datetime,
    ) -> bool:
        idempotency_key = f"{entry.device_id}:{entry.slot_id}:{minute_utc.isoformat()}:ON"
        return await self._idempotency.acquire(idempotency_key)


def _day_of_week(now_utc: datetime) -> SchedulerDayOfWeek:
    mapping = {
        0: SchedulerDayOfWeek.MONDAY,
        1: SchedulerDayOfWeek.TUESDAY,
        2: SchedulerDayOfWeek.WEDNESDAY,
        3: SchedulerDayOfWeek.THURSDAY,
        4: SchedulerDayOfWeek.FRIDAY,
        5: SchedulerDayOfWeek.SATURDAY,
        6: SchedulerDayOfWeek.SUNDAY,
    }
    return mapping[now_utc.weekday()]


async def _sleep_until_next_tick(stop_event: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=1.0)
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

