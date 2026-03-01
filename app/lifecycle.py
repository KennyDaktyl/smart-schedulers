from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable

from app.scheduler.ack_consumer import SchedulerAckConsumer
from app.scheduler.dispatcher import SchedulerDispatcher
from app.scheduler.engine import SchedulerEngine
from app.scheduler.timeout_sweeper import SchedulerTimeoutSweeper
from smart_common.nats.client import nats_client

logger = logging.getLogger("smart-schedulers.lifecycle")


async def run() -> None:
    logger.info("=== LIFECYCLE START ===")
    logger.info("Python=%s", sys.version)
    logger.info("Event loop=%s", asyncio.get_running_loop())

    planner_enabled = _env_bool("SCHEDULER_ENABLE_PLANNER", True)
    dispatcher_enabled = _env_bool("SCHEDULER_ENABLE_DISPATCHER", True)
    ack_enabled = _env_bool("SCHEDULER_ENABLE_ACK_CONSUMER", True)
    sweeper_enabled = _env_bool("SCHEDULER_ENABLE_TIMEOUT_SWEEPER", True)

    stoppers: list[Callable[[], Awaitable[None]]] = []
    tasks: list[asyncio.Task] = []

    if planner_enabled:
        engine = SchedulerEngine(
            planner_batch_size=int(os.getenv("SCHEDULER_PLANNER_BATCH_SIZE", "1000")),
            idempotency_ttl_sec=int(os.getenv("SCHEDULER_IDEMPOTENCY_TTL_SEC", "120")),
            redis_prefix=os.getenv("SCHEDULER_REDIS_PREFIX", "smart-schedulers"),
        )
        tasks.append(asyncio.create_task(engine.run(), name="scheduler-planner"))
        stoppers.append(engine.stop)

    if dispatcher_enabled:
        dispatcher = SchedulerDispatcher(
            ack_timeout_sec=float(os.getenv("SCHEDULER_ACK_TIMEOUT_SEC", "3")),
            max_concurrency=int(os.getenv("SCHEDULER_MAX_CONCURRENCY", "25")),
            batch_size=int(os.getenv("SCHEDULER_DISPATCH_BATCH_SIZE", "500")),
            poll_interval_sec=float(os.getenv("SCHEDULER_DISPATCH_POLL_SEC", "0.2")),
            max_retry=int(os.getenv("SCHEDULER_DISPATCH_MAX_RETRY", "1")),
            retry_backoff_sec=float(os.getenv("SCHEDULER_DISPATCH_RETRY_BACKOFF_SEC", "0.25")),
            retry_jitter_sec=float(os.getenv("SCHEDULER_DISPATCH_RETRY_JITTER_SEC", "0.25")),
            max_inflight_per_microcontroller=int(
                os.getenv("SCHEDULER_MAX_INFLIGHT_PER_MICROCONTROLLER", "1")
            ),
        )
        tasks.append(asyncio.create_task(dispatcher.run(), name="scheduler-dispatcher"))
        stoppers.append(dispatcher.stop)

    if ack_enabled:
        ack_consumer = SchedulerAckConsumer()
        tasks.append(asyncio.create_task(ack_consumer.run(), name="scheduler-ack-consumer"))
        stoppers.append(ack_consumer.stop)

    if sweeper_enabled:
        sweeper = SchedulerTimeoutSweeper(
            interval_sec=float(os.getenv("SCHEDULER_TIMEOUT_SWEEPER_INTERVAL_SEC", "1.0")),
            batch_size=int(os.getenv("SCHEDULER_TIMEOUT_SWEEPER_BATCH_SIZE", "500")),
        )
        tasks.append(asyncio.create_task(sweeper.run(), name="scheduler-timeout-sweeper"))
        stoppers.append(sweeper.stop)

    if not tasks:
        raise RuntimeError("No workers enabled; enable at least one scheduler component")

    for task in tasks:
        task.add_done_callback(_task_done_callback)

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_shutdown(sig: signal.Signals) -> None:
        logger.warning(
            "Shutdown signal received",
            extra={"taskName": "lifecycle", "signal": getattr(sig, "name", sig)},
        )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda sig=sig: _handle_shutdown(sig))

    logger.info(
        (
            "smart-schedulers started | planner=%s dispatcher=%s "
            "ack_consumer=%s timeout_sweeper=%s"
        ),
        planner_enabled,
        dispatcher_enabled,
        ack_enabled,
        sweeper_enabled,
    )
    await shutdown_event.wait()
    logger.warning("smart-schedulers shutdown requested")

    for stop in stoppers:
        await stop()

    await asyncio.gather(*tasks, return_exceptions=True)
    await nats_client.close()

    logger.info("Lifecycle shutdown complete")


def _task_done_callback(task: asyncio.Task) -> None:
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        logger.warning("Worker task cancelled | name=%s", task.get_name())
        return

    if exc:
        logger.exception(
            "Worker crashed",
            exc_info=exc,
            extra={"taskName": task.get_name()},
        )
    else:
        logger.warning("Worker exited without exception | name=%s", task.get_name())


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}
