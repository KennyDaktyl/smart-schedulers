from __future__ import annotations

import json
import logging

import nats

from smart_common.core.config import settings

logger = logging.getLogger(__name__)


class NATSClient:
    def __init__(self) -> None:
        self.nc = None
        self.js = None

    async def connect(self) -> None:
        if self.nc and self.nc.is_connected:
            return
        self.nc = await nats.connect(settings.NATS_URL)
        self.js = self.nc.jetstream()
        logger.info("Connected to NATS | url=%s", settings.NATS_URL)

    async def ensure_connected(self) -> None:
        if not self.nc or not self.nc.is_connected:
            await self.connect()

    async def js_publish(self, subject: str, payload: dict) -> None:
        await self.ensure_connected()
        data = json.dumps(payload).encode("utf-8")
        await self.js.publish(subject, data)

    async def close(self) -> None:
        if not self.nc:
            return
        try:
            await self.nc.drain()
        except Exception:
            pass
        try:
            await self.nc.close()
        except Exception:
            pass
        self.nc = None
        self.js = None


nats_client = NATSClient()

