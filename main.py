from __future__ import annotations

import asyncio
import logging
import os
import platform
import sys

from app.lifecycle import run
from smart_common.smart_logging import install_task_logger, setup_logging

install_task_logger()
setup_logging()

logger = logging.getLogger("smart-schedulers.bootstrap")


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
    ):
        logger.info("ENV %s=%s", key, os.getenv(key))

    asyncio.run(run())


if __name__ == "__main__":
    main()
