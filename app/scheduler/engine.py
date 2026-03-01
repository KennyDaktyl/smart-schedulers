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
from smart_common.enums.scheduler import SchedulerCommandAction, SchedulerDayOfWeek
from smart_common.repositories.scheduler_command_repository import SchedulerCommandRepository
from smart_common.repositories.scheduler_runtime_repository import SchedulerRuntimeRepository
from smart_common.schemas.scheduler_runtime import DecisionKind, DueSchedulerEntry
from smart_common.services.scheduler_audit_service import SchedulerAuditService
from smart_common.services.scheduler_decision_service import SchedulerDecisionService

logger = logging.getLogger(__name__)


class SchedulerEngine:
    def __init__(
        self,
        *,
        planner_batch_size: int,
        idempotency_ttl_sec: int,
        redis_prefix: str,
    ) -> None:
        self._stop_event = asyncio.Event()
        self._last_processed_minute: datetime | None = None
        self._planner_batch_size = max(1, planner_batch_size)
        self._decision_service = SchedulerDecisionService()
        self._idempotency = MinuteIdempotencyStore(
            prefix=redis_prefix,
            ttl_sec=idempotency_ttl_sec,
        )

    async def run(self) -> None:
        logger.info(
            "Scheduler planner starting | planner_batch_size=%s",
            self._planner_batch_size,
        )
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
        logger.info("Scheduler planner stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _process_minute(self, minute_utc: datetime) -> None:
        day_of_week = _day_of_week(minute_utc)
        hhmm = minute_utc.strftime("%H:%M")

        scanned_due = 0
        scanned_end = 0
        enqueued_on = 0
        enqueued_off = 0
        skipped = 0
        skip_reason_counts: dict[str, int] = {}

        provider_cache: dict[int, object | None] = {}
        measurement_cache: dict[int, object | None] = {}

        due_offset = 0
        while not self._stop_event.is_set():
            with _db_session() as db:
                runtime_repo = SchedulerRuntimeRepository(db)
                command_repo = SchedulerCommandRepository(db)
                audit = SchedulerAuditService(db)

                entries = runtime_repo.fetch_due_entries(
                    day_of_week=day_of_week,
                    hhmm=hhmm,
                    limit=self._planner_batch_size,
                    offset=due_offset,
                )
                if not entries:
                    break

                scanned_due += len(entries)
                for entry in entries:
                    if not await self._acquire_entry_idempotency(
                        entry=entry,
                        minute_utc=minute_utc,
                        action=SchedulerCommandAction.ON.value,
                    ):
                        continue

                    provider_id = entry.microcontroller_power_provider_id
                    provider = None
                    latest = None
                    if provider_id is not None:
                        provider = provider_cache.get(provider_id)
                        if provider_id not in provider_cache:
                            provider = runtime_repo.get_provider(provider_id)
                            provider_cache[provider_id] = provider

                        latest = measurement_cache.get(provider_id)
                        if provider_id not in measurement_cache:
                            latest = runtime_repo.get_latest_measurement(provider_id)
                            measurement_cache[provider_id] = latest

                    decision = self._decision_service.decide(
                        entry=entry,
                        now_utc=minute_utc,
                        provider=provider,
                        latest_measurement=latest,
                    )

                    if decision.kind == DecisionKind.ALLOW_ON:
                        inserted = command_repo.enqueue_command(
                            minute_key=minute_utc,
                            entry=entry,
                            action=SchedulerCommandAction.ON,
                            trigger_reason=decision.trigger_reason,
                            measured_value=decision.measured_value,
                            measured_unit=decision.measured_unit,
                            now_utc=minute_utc,
                        )
                        if inserted:
                            enqueued_on += 1
                        continue

                    skipped += 1
                    reason_key = decision.trigger_reason or decision.kind.value
                    skip_reason_counts[reason_key] = skip_reason_counts.get(reason_key, 0) + 1
                    audit.create_event(
                        device_id=entry.device_id,
                        event_name=(
                            DeviceEventName.SCHEDULER_SKIPPED_NO_POWER_DATA
                            if decision.kind == DecisionKind.SKIP_NO_POWER_DATA
                            else DeviceEventName.SCHEDULER_SKIPPED_THRESHOLD_NOT_MET
                        ),
                        trigger_reason=reason_key,
                        measured_value=decision.measured_value,
                        measured_unit=decision.measured_unit,
                        pin_state=False,
                    )

                db.commit()
                due_offset += len(entries)

        end_offset = 0
        while not self._stop_event.is_set():
            with _db_session() as db:
                runtime_repo = SchedulerRuntimeRepository(db)
                command_repo = SchedulerCommandRepository(db)
                entries = runtime_repo.fetch_end_entries(
                    day_of_week=day_of_week,
                    hhmm=hhmm,
                    limit=self._planner_batch_size,
                    offset=end_offset,
                )
                if not entries:
                    break

                scanned_end += len(entries)
                for entry in entries:
                    if not await self._acquire_entry_idempotency(
                        entry=entry,
                        minute_utc=minute_utc,
                        action=SchedulerCommandAction.OFF.value,
                    ):
                        continue

                    inserted = command_repo.enqueue_command(
                        minute_key=minute_utc,
                        entry=entry,
                        action=SchedulerCommandAction.OFF,
                        trigger_reason="SCHEDULER_END",
                        now_utc=minute_utc,
                    )
                    if inserted:
                        enqueued_off += 1

                db.commit()
                end_offset += len(entries)

        if scanned_due == 0 and scanned_end == 0:
            logger.info(
                "Scheduler minute processed | minute=%s due_entries=0 end_entries=0",
                minute_utc.isoformat(),
            )
            return

        logger.info(
            (
                "Scheduler minute planned | minute=%s scanned_due=%s scanned_end=%s "
                "enqueued_on=%s enqueued_off=%s skipped=%s"
            ),
            minute_utc.isoformat(),
            scanned_due,
            scanned_end,
            enqueued_on,
            enqueued_off,
            skipped,
        )
        if skipped > 0:
            logger.warning(
                "Scheduler minute skip summary | minute=%s skip_reasons=%s",
                minute_utc.isoformat(),
                skip_reason_counts,
            )

    async def _acquire_entry_idempotency(
        self,
        *,
        entry: DueSchedulerEntry,
        minute_utc: datetime,
        action: str,
    ) -> bool:
        idempotency_key = f"{entry.device_id}:{entry.slot_id}:{minute_utc.isoformat()}:{action}"
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
