from smart_common.schemas.base import APIModel


class DeviceCommandPayload(APIModel):
    device_id: int
    device_uuid: str
    device_number: int
    mode: str
    command: str
    is_on: bool

