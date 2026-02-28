from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from app.scheduler.engine import SchedulerEngine
from smart_common.nats.client import nats_client

logger = logging.getLogger("smart-schedulers.lifecycle")


async def run() -> None:
    logger.info("=== LIFECYCLE START ===")
    logger.info("Python=%s", sys.version)
    logger.info("Event loop=%s", asyncio.get_running_loop())

    engine = SchedulerEngine(
        ack_timeout_sec=float(os.getenv("SCHEDULER_ACK_TIMEOUT_SEC", "10")),
        max_concurrency=int(os.getenv("SCHEDULER_MAX_CONCURRENCY", "25")),
        idempotency_ttl_sec=int(os.getenv("SCHEDULER_IDEMPOTENCY_TTL_SEC", "120")),
        redis_prefix=os.getenv("SCHEDULER_REDIS_PREFIX", "smart-schedulers"),
    )

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

    worker_task = asyncio.create_task(engine.run(), name="scheduler-engine")

    def _worker_done(task: asyncio.Task) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            logger.warning("Scheduler engine task cancelled")
            return

        if exc:
            logger.exception(
                "Scheduler engine crashed",
                exc_info=exc,
                extra={"taskName": "scheduler-engine"},
            )
        else:
            logger.warning("Scheduler engine exited without exception")

    worker_task.add_done_callback(_worker_done)

    logger.info("smart-schedulers started")
    await shutdown_event.wait()
    logger.warning("smart-schedulers shutdown requested")

    await engine.stop()
    await asyncio.gather(worker_task, return_exceptions=True)
    await nats_client.close()

    logger.info("Lifecycle shutdown complete")
