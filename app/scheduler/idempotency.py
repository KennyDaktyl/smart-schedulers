from __future__ import annotations

import asyncio
import logging
import time

from redis.asyncio import Redis

from smart_common.core.config import settings

logger = logging.getLogger(__name__)


class MinuteIdempotencyStore:
    def __init__(
        self,
        *,
        prefix: str,
        ttl_sec: int,
    ) -> None:
        self._prefix = prefix
        self._ttl_sec = max(30, ttl_sec)
        self._redis: Redis | None = None
        self._memory: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        try:
            client = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                decode_responses=True,
            )
            await client.ping()
            self._redis = client
            logger.info(
                "Idempotency store initialized with Redis | host=%s port=%s",
                settings.REDIS_HOST,
                settings.REDIS_PORT,
            )
        except Exception:
            self._redis = None
            logger.warning("Redis unavailable, idempotency fallback set to in-memory")

    async def close(self) -> None:
        if not self._redis:
            return
        try:
            await self._redis.aclose()
        except Exception:
            logger.exception("Failed to close Redis idempotency client")
        finally:
            self._redis = None

    async def acquire(self, key: str) -> bool:
        normalized_key = f"{self._prefix}:{key}"

        if self._redis:
            try:
                result = await self._redis.set(
                    normalized_key,
                    "1",
                    nx=True,
                    ex=self._ttl_sec,
                )
                return bool(result)
            except Exception:
                logger.exception("Redis idempotency set failed, switching to memory mode")
                self._redis = None

        return await self._acquire_memory(normalized_key)

    async def _acquire_memory(self, key: str) -> bool:
        now = time.monotonic()
        expire_at = now + float(self._ttl_sec)

        async with self._lock:
            self._memory = {
                existing_key: existing_expire
                for existing_key, existing_expire in self._memory.items()
                if existing_expire > now
            }

            if key in self._memory:
                return False

            self._memory[key] = expire_at
            return True
