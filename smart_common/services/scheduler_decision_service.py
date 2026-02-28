from __future__ import annotations

from datetime import datetime, timezone

from smart_common.models.provider import Provider
from smart_common.models.provider_measurement import ProviderMeasurement
from smart_common.schemas.scheduler_runtime import Decision, DecisionKind, DueSchedulerEntry

_POWER_FACTORS = {
    "W": 1.0,
    "kW": 1000.0,
    "MW": 1_000_000.0,
}


class SchedulerDecisionService:
    def decide(
        self,
        *,
        entry: DueSchedulerEntry,
        now_utc: datetime,
        provider: Provider | None,
        latest_measurement: ProviderMeasurement | None,
    ) -> Decision:
        if not entry.use_power_threshold:
            return Decision(
                kind=DecisionKind.ALLOW_ON,
                trigger_reason="SCHEDULER_MATCH",
            )

        threshold_value = entry.power_threshold_value
        threshold_unit = _normalize_unit(entry.power_threshold_unit)
        if threshold_value is None or threshold_unit is None:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "THRESHOLD_CONFIG_MISSING")

        if not provider or not provider.enabled:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_PROVIDER_UNAVAILABLE")

        if provider.expected_interval_sec is None or provider.expected_interval_sec <= 0:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_INTERVAL_MISSING")

        if not latest_measurement:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_MISSING")

        measured_at = _to_utc_aware(latest_measurement.measured_at)
        age_sec = (now_utc - measured_at).total_seconds()
        if age_sec > provider.expected_interval_sec:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_STALE")

        if latest_measurement.measured_value is None:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_MISSING")

        value = float(latest_measurement.measured_value)
        provider_unit = _normalize_unit(provider.unit)
        measurement_unit = (
            _normalize_unit(latest_measurement.measured_unit) or provider_unit
        )

        converted = _convert_power_unit(
            value=value,
            from_unit=measurement_unit,
            to_unit=threshold_unit,
        )
        if converted is None:
            return Decision(DecisionKind.SKIP_NO_POWER_DATA, "POWER_UNIT_MISMATCH")

        if converted >= threshold_value:
            return Decision(
                kind=DecisionKind.ALLOW_ON,
                trigger_reason="SCHEDULER_MATCH",
                measured_value=converted,
                measured_unit=threshold_unit,
            )

        return Decision(
            kind=DecisionKind.SKIP_THRESHOLD_NOT_MET,
            trigger_reason="THRESHOLD_NOT_MET",
            measured_value=converted,
            measured_unit=threshold_unit,
        )


def _to_utc_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_unit(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.lower() == "kw":
        return "kW"
    if normalized.lower() == "mw":
        return "MW"
    if normalized.lower() == "w":
        return "W"
    return normalized


def _convert_power_unit(
    *,
    value: float,
    from_unit: str | None,
    to_unit: str | None,
) -> float | None:
    normalized_from = _normalize_unit(from_unit)
    normalized_to = _normalize_unit(to_unit)
    if normalized_from is None or normalized_to is None:
        return None
    from_factor = _POWER_FACTORS.get(normalized_from)
    to_factor = _POWER_FACTORS.get(normalized_to)
    if from_factor is None or to_factor is None:
        return None
    watts = value * from_factor
    return watts / to_factor

