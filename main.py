from __future__ import annotations

import asyncio
import logging
import os
import platform
import sys

import sentry_sdk
from app.lifecycle import run
from smart_common.smart_logging import setup_logging
from smart_common.smart_logging.task_logging import install_task_logger

install_task_logger()
setup_logging()

logger = logging.getLogger("smart-schedulers.bootstrap")


def _init_sentry() -> None:
    sentry_dsn = os.getenv("SENTRY_DSN")
    if not sentry_dsn:
        logger.info("Sentry disabled (SENTRY_DSN is not set)")
        return

    sentry_sdk.init(
        dsn=sentry_dsn,
        send_default_pii=True,
        environment=os.getenv("ENV", "development"),
    )
    logger.info("Sentry enabled for ENV=%s", os.getenv("ENV", "development"))


def main() -> None:
    logger.info("=== SMART SCHEDULERS BOOTSTRAP START ===")
    logger.info("PID=%s", os.getpid())
    logger.info("Python=%s", sys.version)
    logger.info("Platform=%s", platform.platform())
    logger.info("CWD=%s", os.getcwd())

    for key in (
        "ENV",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_NAME",
        "REDIS_HOST",
        "REDIS_PORT",
        "NATS_URL",
        "STREAM_NAME",
        "SUBJECT",
        "SCHEDULER_ENABLE_PLANNER",
        "SCHEDULER_ENABLE_DISPATCHER",
        "SCHEDULER_ENABLE_ACK_CONSUMER",
        "SCHEDULER_ENABLE_TIMEOUT_SWEEPER",
        "SCHEDULER_PLANNER_BATCH_SIZE",
        "SCHEDULER_DISPATCH_BATCH_SIZE",
        "SCHEDULER_DISPATCH_POLL_SEC",
        "SCHEDULER_DISPATCH_MAX_RETRY",
        "SCHEDULER_DISPATCH_RETRY_BACKOFF_SEC",
        "SCHEDULER_DISPATCH_RETRY_JITTER_SEC",
        "SCHEDULER_MAX_INFLIGHT_PER_MICROCONTROLLER",
        "SCHEDULER_TIMEOUT_SWEEPER_INTERVAL_SEC",
        "SCHEDULER_TIMEOUT_SWEEPER_BATCH_SIZE",
    ):
        logger.info("ENV %s=%s", key, os.getenv(key))

    _init_sentry()

    asyncio.run(run())


if __name__ == "__main__":
    main()
