from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from smart_common.core.config import settings


def stream_name() -> str:
    return settings.STREAM_NAME


def subject_for_entity(entity_id: str, event_type: str) -> str:
    return f"{stream_name()}.{entity_id}.command.{event_type}"


def ack_subject_for_entity(entity_id: str, event_type: str) -> str:
    return f"{subject_for_entity(entity_id, event_type)}.ack"


def build_event_payload(
    *,
    event_type: str,
    entity_type: str,
    entity_id: str,
    data: dict[str, Any],
    source: str,
    subject: str,
) -> dict[str, Any]:
    return {
        "subject": subject,
        "event_type": event_type,
        "event_id": uuid4().hex,
        "source": source,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_version": "1",
        "data": data,
    }

