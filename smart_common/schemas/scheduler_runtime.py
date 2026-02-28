from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID


class DecisionKind(str, Enum):
    ALLOW_ON = "ALLOW_ON"
    SKIP_NO_POWER_DATA = "SKIP_NO_POWER_DATA"
    SKIP_THRESHOLD_NOT_MET = "SKIP_THRESHOLD_NOT_MET"


@dataclass(frozen=True)
class DueSchedulerEntry:
    device_id: int
    device_uuid: UUID
    device_number: int
    microcontroller_uuid: UUID
    microcontroller_power_provider_id: int | None
    slot_id: int
    use_power_threshold: bool
    power_provider_id: int | None
    power_threshold_value: float | None
    power_threshold_unit: str | None


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    trigger_reason: str
    measured_value: float | None = None
    measured_unit: str | None = None


@dataclass(frozen=True)
class AckResult:
    ok: bool
    is_on: bool | None
    raw_data: dict

