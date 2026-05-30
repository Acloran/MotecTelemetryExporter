from __future__ import annotations

from .models import ChannelDef


ORION_CHANNELS: list[ChannelDef] = [
    ChannelDef(
        key="motor_rpm",
        label="Motor RPM",
        table="controls",
        column="motor_speed",
        unit="rpm",
        quantity="speed",
        default=True,
        split_candidate=True,
    ),
    ChannelDef(
        key="rpm_request",
        label="RPM Request",
        table="controls",
        column="rpm_request",
        unit="rpm",
        quantity="speed",
        split_candidate=True,
    ),
    ChannelDef(key="wheel_speed", label="Wheel Speed", table="dynamics", column="wheel_speed", unit="rad/s", quantity="speed", split_candidate=True),
    ChannelDef(key="gps_speed", label="GPS Speed", table="dynamics", column="gps_speed", unit="m/s", quantity="speed", split_candidate=True),
    ChannelDef(key="gps_latitude", label="GPS Latitude", table="dynamics", column="gps[1]", unit="deg", quantity="position"),
    ChannelDef(key="gps_longitude", label="GPS Longitude", table="dynamics", column="gps[2]", unit="deg", quantity="position"),
    ChannelDef(key="apps1_travel", label="APPS 1 Travel", table="controls", column="apps1_travel", unit="%", quantity="position"),
    ChannelDef(key="apps2_travel", label="APPS 2 Travel", table="controls", column="apps2_travel", unit="%", quantity="position"),
    ChannelDef(key="brake_pressure_f", label="Brake Pressure Front", table="controls", column="brake_pressure_f", unit="psi", quantity="pressure"),
    ChannelDef(key="torque_request", label="Torque Request", table="controls", column="torque_request", unit="Nm", quantity="torque"),
    ChannelDef(key="torque_feedback", label="Torque Feedback", table="controls", column="torque_feedback", unit="Nm", quantity="torque"),
    ChannelDef(key="commanded_torque", label="Commanded Torque", table="controls", column="commanded_torque", unit="Nm", quantity="torque"),
    ChannelDef(key="steer_col_angle", label="Steering Column Angle", table="dynamics", column="steer_col_angle", unit="deg", quantity="angle"),
    ChannelDef(key="flw_speed", label="Front Left Wheel Speed", table="dynamics", column="flw_speed", unit="rad/s", quantity="speed"),
    ChannelDef(key="frw_speed", label="Front Right Wheel Speed", table="dynamics", column="frw_speed", unit="rad/s", quantity="speed"),
    ChannelDef(key="blw_speed", label="Back Left Wheel Speed", table="dynamics", column="blw_speed", unit="rad/s", quantity="speed"),
    ChannelDef(key="brw_speed", label="Back Right Wheel Speed", table="dynamics", column="brw_speed", unit="rad/s", quantity="speed"),
    ChannelDef(key="hv_pack_v", label="HV Pack Voltage", table="pack", column="hv_pack_v", unit="V", quantity="voltage"),
    ChannelDef(key="hv_c", label="HV Current", table="pack", column="hv_c", unit="A", quantity="current"),
    ChannelDef(key="hv_soc", label="HV State of Charge", table="pack", column="hv_soc", unit="%", quantity="ratio"),
    ChannelDef(key="dc_bus_v", label="DC Bus Voltage", table="pack", column="dc_bus_v", unit="V", quantity="voltage"),
    ChannelDef(key="dc_bus_current", label="DC Bus Current", table="pack", column="dc_bus_current", unit="A", quantity="current"),
    ChannelDef(key="motor_temp", label="Motor Temp", table="thermal", column="motor_temp", unit="C", quantity="temperature"),
    ChannelDef(key="inverter_temp", label="Inverter Temp", table="thermal", column="inverter_temp", unit="C", quantity="temperature"),
    ChannelDef(key="coolant_temp", label="Coolant Temp", table="thermal", column="coolant_temp", unit="C", quantity="temperature"),
    ChannelDef(key="ambient_temp", label="Ambient Temp", table="thermal", column="ambient_temp", unit="C", quantity="temperature"),
]


DEFAULT_CHANNEL_KEY = next(c.key for c in ORION_CHANNELS if c.default)
CHANNEL_BY_KEY = {channel.key: channel for channel in ORION_CHANNELS}


def get_channel(key: str | None) -> ChannelDef:
    if not key:
        return CHANNEL_BY_KEY[DEFAULT_CHANNEL_KEY]
    try:
        return CHANNEL_BY_KEY[key]
    except KeyError as exc:
        raise KeyError(f"Unknown Orion channel: {key}") from exc


def register_channels(channels: list[ChannelDef]) -> None:
    CHANNEL_BY_KEY.clear()
    CHANNEL_BY_KEY.update({channel.key: channel for channel in channels})
