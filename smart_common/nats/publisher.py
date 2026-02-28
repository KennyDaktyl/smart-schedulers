from __future__ import annotations

import asyncio
import json
from typing import Callable

from smart_common.nats.client import nats_client


class NatsPublisher:
    def __init__(self, client=nats_client):
        self.client = client

    async def publish(self, subject: str, payload: dict) -> None:
        await self.client.js_publish(subject, payload)

    async def publish_and_wait_for_ack(
        self,
        *,
        subject: str,
        ack_subject: str,
        message: dict,
        predicate: Callable[[dict], bool],
        timeout: float,
    ) -> dict:
        await self.client.ensure_connected()
        js = self.client.js
        data = json.dumps(message).encode("utf-8")
        future = asyncio.get_event_loop().create_future()

        async def ack_handler(msg):
            payload = json.loads(msg.data.decode())
            if predicate(payload) and not future.done():
                future.set_result(payload)

        sub = await self.client.nc.subscribe(ack_subject, cb=ack_handler)
        await js.publish(subject=subject, payload=data)
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            await sub.unsubscribe()

