from datetime import datetime, timezone
from uuid import uuid4

from smart_common.enums.unit import PowerUnit
from smart_common.models.provider import Provider
from smart_common.models.provider_measurement import ProviderMeasurement
from smart_common.models.provider_metric_sample import ProviderMetricSample
from smart_common.providers.enums import ProviderKind, ProviderType
from smart_common.repositories.scheduler_runtime_repository import _parse_activation_rule
from smart_common.schemas.scheduler_runtime import DecisionKind, DueSchedulerEntry
from smart_common.services.scheduler_decision_service import SchedulerDecisionService


def _build_provider() -> Provider:
    return Provider(
        id=1,
        uuid=uuid4(),
        user_id=1,
        name="Provider test",
        provider_type=ProviderType.API,
        kind=ProviderKind.PV_INVERTER,
        vendor=None,
        external_id=None,
        unit=PowerUnit.WATT,
        power_source=None,
        value_min=None,
        value_max=None,
        expected_interval_sec=60,
        has_power_meter=True,
        has_energy_storage=True,
        enabled=True,
        config={},
    )


def _build_entry() -> DueSchedulerEntry:
    rule = _parse_activation_rule(
        {
            "operator": "ANY",
            "items": [
                {
                    "operator": "ALL",
                    "items": [
                        {
                            "source": "provider_primary_power",
                            "comparator": "gte",
                            "value": 2.0,
                            "unit": "W",
                        },
                        {
                            "source": "provider_primary_power",
                            "comparator": "gte",
                            "value": 2.0,
                            "unit": "W",
                        },
                    ],
                },
                {
                    "operator": "ANY",
                    "items": [
                        {
                            "source": "provider_battery_soc",
                            "comparator": "gte",
                            "value": 30.0,
                            "unit": "%",
                        },
                        {
                            "source": "provider_battery_soc",
                            "comparator": "gte",
                            "value": 30.0,
                            "unit": "%",
                        },
                    ],
                },
            ],
        }
    )
    assert rule is not None

    return DueSchedulerEntry(
        device_id=10,
        device_uuid=uuid4(),
        device_number=1,
        microcontroller_uuid=uuid4(),
        scheduler_id=7,
        user_id=1,
        microcontroller_power_provider_id=1,
        slot_id=11,
        use_power_threshold=False,
        power_threshold_value=None,
        power_threshold_unit=None,
        activation_rule=rule,
    )


def test_nested_rule_allows_when_battery_group_matches() -> None:
    now_utc = datetime(2026, 3, 11, 8, 0, tzinfo=timezone.utc)
    service = SchedulerDecisionService()

    decision = service.decide(
        entry=_build_entry(),
        now_utc=now_utc,
        provider=_build_provider(),
        latest_measurement=ProviderMeasurement(
            provider_id=1,
            measured_at=now_utc,
            measured_value=1.0,
            measured_unit="W",
            metadata_payload={},
            extra_data={},
        ),
        latest_metric_samples={
            "battery_soc": ProviderMetricSample(
                provider_id=1,
                provider_measurement_id=1,
                metric_key="battery_soc",
                measured_at=now_utc,
                value=40.0,
                unit="%",
                metadata_payload={},
            )
        },
    )

    assert decision.kind == DecisionKind.ALLOW_ON
    assert decision.trigger_reason == "SCHEDULER_MATCH"


def test_nested_rule_skips_when_all_groups_fail() -> None:
    now_utc = datetime(2026, 3, 11, 8, 0, tzinfo=timezone.utc)
    service = SchedulerDecisionService()

    decision = service.decide(
        entry=_build_entry(),
        now_utc=now_utc,
        provider=_build_provider(),
        latest_measurement=ProviderMeasurement(
            provider_id=1,
            measured_at=now_utc,
            measured_value=1.0,
            measured_unit="W",
            metadata_payload={},
            extra_data={},
        ),
        latest_metric_samples={
            "battery_soc": ProviderMetricSample(
                provider_id=1,
                provider_measurement_id=1,
                metric_key="battery_soc",
                measured_at=now_utc,
                value=20.0,
                unit="%",
                metadata_payload={},
            )
        },
    )

    assert decision.kind == DecisionKind.SKIP_THRESHOLD_NOT_MET
    assert decision.trigger_reason == "THRESHOLD_NOT_MET"
