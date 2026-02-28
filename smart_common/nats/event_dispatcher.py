from __future__ import annotations

from typing import Callable

from pydantic import BaseModel

from smart_common.nats.event_helpers import build_event_payload


class EventDispatcher:
    def __init__(self, publisher, *, default_source: str):
        self.publisher = publisher
        self.default_source = default_source

    @staticmethod
    def _serialize_data(data: BaseModel | dict) -> dict:
        if isinstance(data, BaseModel):
            return data.model_dump(mode="json")
        return dict(data)

    async def publish_event_and_wait_for_ack(
        self,
        *,
        entity_type: str,
        entity_id: str,
        event_type: str,
        data: BaseModel | dict,
        predicate: Callable[[dict], bool],
        timeout: float,
        subject: str,
        ack_subject: str,
    ) -> dict:
        payload = build_event_payload(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            data=self._serialize_data(data),
            source=self.default_source,
            subject=subject,
        )
        payload["ack_subject"] = ack_subject

        return await self.publisher.publish_and_wait_for_ack(
            subject=subject,
            ack_subject=ack_subject,
            message=payload,
            predicate=predicate,
            timeout=timeout,
        )

