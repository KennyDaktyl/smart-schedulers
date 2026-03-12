import asyncio
from types import SimpleNamespace
from uuid import uuid4

from smart_common.enums.scheduler import SchedulerCommandAction
from smart_common.services.scheduler_command_service import SchedulerCommandService


def test_publish_command_builds_scheduler_policy_payload(monkeypatch) -> None:
    published: dict = {}

    async def _publish(subject: str, payload: dict, context: dict) -> None:
        published["subject"] = subject
        published["payload"] = payload
        published["context"] = context

    monkeypatch.setattr(
        "smart_common.services.scheduler_command_service.publisher",
        SimpleNamespace(publish=_publish),
    )

    service = SchedulerCommandService()
    command = SimpleNamespace(
        command_id=uuid4(),
        device_id=1,
        device_uuid=uuid4(),
        device_number=1,
        microcontroller_uuid=uuid4(),
        slot_id=10,
        scheduler_id=20,
        user_id=30,
        action=SchedulerCommandAction.ENABLE_POLICY,
        command_payload={
            "policy_type": "TEMPERATURE_HYSTERESIS",
            "sensor_id": "tank-top",
            "target_temperature_c": 65.0,
            "stop_above_target_delta_c": 0.0,
            "start_below_target_delta_c": 10.0,
            "heat_up_on_activate": True,
            "end_behavior": "FORCE_OFF",
        },
    )

    asyncio.run(service.publish_command(command=command))

    data = published["payload"]["data"]
    assert data["command"] == "SET_SCHEDULER_POLICY"
    assert data["scheduler_policy_enabled"] is True
    assert data["scheduler_policy"]["sensor_id"] == "tank-top"
