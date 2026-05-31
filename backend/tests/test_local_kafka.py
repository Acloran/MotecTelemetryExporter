from motec_exporter.config import Settings
import json
import struct

from motec_exporter.live import _decode_orion_protobuf, _sample_interval_s, kafka_transport_for, normalize_live_payload, stream_kafka_samples
from motec_exporter.local_kafka import LOCAL_TOPIC_BUS


def test_local_topic_bus_streams_replay_payload():
    settings = Settings(KAFKA_MODE="local", KAFKA_CONSUMER_TIMEOUT_MS=250)
    stream = stream_kafka_samples(settings, "orion", topic="unit_local_topic")

    status = next(stream)
    assert status["ok"] is True
    assert status["transport"] == "local"

    LOCAL_TOPIC_BUS.publish(
        "unit_local_topic",
        {
            "time": 1777135260000,
            "latitude": 30.1,
            "longitude": -97.2,
            "gps_speed": 12.5,
        },
    )
    event = next(stream)
    assert event["type"] == "sample"
    assert event["topic"] == "unit_local_topic"
    assert event["sample"]["lat"] == 30.1
    assert event["sample"]["lon"] == -97.2
    assert event["sample"]["speed"] == 12.5

    stream.close()


def test_kafka_transport_defaults_to_local():
    assert kafka_transport_for(Settings(_env_file=None)) == "local"
    assert kafka_transport_for(Settings(KAFKA_MODE="kafka")) == "kafka"
    assert kafka_transport_for(Settings(KAFKA_MODE="kafka"), "local") == "local"
    assert kafka_transport_for(Settings(), "mqtt") == "mqtt"


def test_sample_interval_clamps_to_useful_bounds():
    assert _sample_interval_s(None) is None
    assert _sample_interval_s(0) is None
    assert _sample_interval_s(2) == 0.5
    assert _sample_interval_s(100) == 0.02
    assert _sample_interval_s(0.01) == 5.0


def test_live_payload_uses_inverter_dc_bus_power():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "inverter": {"dcBusVoltage": 320, "dcBusCurrent": 42}, "dynamics_gps_latitude": 30.1, "dynamics_gps_longitude": -97.2}',
        "orion",
    )

    assert sample is not None
    assert sample["lat"] == 30.1
    assert sample["lon"] == -97.2
    assert sample["hv_pack_v"] == 320
    assert sample["hv_c"] == 42
    assert sample["power_kw"] == 13.44
    assert sample["values"]["dc_bus_v"] == 320
    assert sample["values"]["dc_bus_current"] == 42
    assert sample["values"]["inverter_power_kw_signed"] == 13.44


def test_live_payload_uses_flat_inverter_dc_bus_aliases():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "inverter_dc_bus_voltage_v": 321.5, "inverter_dc_bus_current_a": -0.8}',
        "orion",
    )

    assert sample is not None
    assert sample["hv_pack_v"] == 321.5
    assert sample["hv_c"] == -0.8
    assert sample["power_kw"] == 0.2572
    assert sample["values"]["dc_bus_v"] == 321.5
    assert sample["values"]["dc_bus_current"] == -0.8
    assert sample["values"]["inverter_power_kw_signed"] == -0.2572


def test_live_payload_uses_flat_car_channel_aliases_without_gps():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "dynamics_gps_speed": 17.5, "pack_dc_bus_v": 301.2, "pack_dc_bus_current": 38.4, "controls_motor_speed": 2240}',
        "orion",
    )

    assert sample is not None
    assert sample["lat"] is None
    assert sample["lon"] is None
    assert sample["speed"] == 17.5
    assert sample["hv_pack_v"] == 301.2
    assert sample["hv_c"] == 38.4
    assert sample["power_kw"] == 11.56608
    assert sample["values"]["controls_motor_speed"] == 2240


def test_live_payload_uses_lhre_orion_bridge_fields():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "gps": [30.21842, -97.712493], "gps_speed": 13.2, "bus_voltage": 315.0, "dc_bus_v": 309.5, "dc_bus_current": 41.0, "motor_speed": 2415, "torque_feedback": 88.0, "inverter_temp": 44.5}',
        "orion",
    )

    assert sample is not None
    assert sample["lat"] == 30.21842
    assert sample["lon"] == -97.712493
    assert sample["speed"] == 13.2
    assert sample["hv_pack_v"] == 309.5
    assert sample["hv_c"] == 41.0
    assert sample["power_kw"] == 12.6895
    assert sample["values"]["motor_speed"] == 2415
    assert sample["values"]["torque_feedback"] == 88.0
    assert sample["values"]["inverter_temp"] == 44.5


def test_live_payload_falls_back_to_lhre_bus_voltage_for_power():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "wheel_speed": 9.5, "bus_voltage": 318.0, "dc_bus_current": 12.0}',
        "orion",
    )

    assert sample is not None
    assert sample["speed"] == 9.5
    assert sample["hv_pack_v"] == 318.0
    assert sample["hv_c"] == 12.0
    assert sample["power_kw"] == 3.816


def test_live_payload_uses_power_draw_magnitude_for_negative_current():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "dc_bus_v": 469.4, "dc_bus_current": -0.5, "motor_speed": 1837}',
        "orion",
    )

    assert sample is not None
    assert sample["hv_pack_v"] == 469.4
    assert sample["hv_c"] == -0.5
    assert sample["power_kw"] == 0.2347


def test_decode_lhre_orion_protobuf_payload_subset():
    payload = b"".join(
        [
            _proto_varint_field(1, 1777135260000),
            _proto_varint_field(2, 42),
            _proto_message_field(
                3,
                b"".join(
                        [
                            _proto_packed_float_field(1, [30.21842, -97.712493]),
                            _proto_float_field(3, 12.25),
                            _proto_float_field(31, 8.75),
                        ]
                ),
            ),
            _proto_message_field(4, _proto_float_field(1, 2415.0)),
            _proto_message_field(
                5,
                _proto_packed_float_field(3, [3.52, 0.0, 3.64])
                + _proto_float_field(4, 309.5)
                + _proto_float_field(12, 41.0),
            ),
        ]
    )

    decoded = _decode_orion_protobuf(payload)
    assert decoded is not None
    assert decoded["time"] == 1777135260000
    assert decoded["packet_id"] == 42
    assert decoded["latitude"] == 30.218420028686523
    assert decoded["longitude"] == -97.71249389648438
    assert decoded["gps_speed"] == 12.25
    assert decoded["wheel_speed"] == 8.75
    assert decoded["motor_speed"] == 2415.0
    assert decoded["dc_bus_v"] == 309.5
    assert decoded["dc_bus_current"] == 41.0
    assert decoded["cells_v"] == [3.5199999809265137, 0.0, 3.640000104904175]


def test_decode_lhre_orion_protobuf_payload_current_pack_cells_field():
    payload = b"".join(
        [
            _proto_varint_field(1, 1777135260000),
            _proto_message_field(
                5,
                _proto_float_field(3, 69.1)
                + _proto_packed_float_field(4, [4.02, 3.93, 4.01])
                + _proto_packed_float_field(12, [34.1, 34.0]),
            ),
        ]
    )

    decoded = _decode_orion_protobuf(payload)
    assert decoded is not None
    assert decoded["soc_estimate"] == struct.unpack("<f", struct.pack("<f", 69.1))[0]
    assert decoded["cells_v"] == [4.019999980926514, 3.930000066757202, 4.010000228881836]
    assert decoded["cells_temps"] == [34.099998474121094, 34.0]

    sample = normalize_live_payload(json.dumps(decoded), "orion")
    assert sample is not None
    assert sample["values"]["min_cell_v"] == 3.930000066757202
    assert sample["values"]["max_cell_v"] == 4.019999980926514


def test_decode_lhre_orion_protobuf_payload_repairs_current_pack_power_fields():
    payload = _proto_message_field(
        5,
        _proto_float_field(3, 62.8)
        + _proto_packed_float_field(4, [3.88, 3.92, 3.98])
        + _proto_float_field(5, 509.2)
        + _proto_float_field(6, 79.5)
        + _proto_float_field(7, -40.4)
        + _proto_float_field(13, 5.1),
    )

    decoded = _decode_orion_protobuf(payload)
    assert decoded is not None
    assert decoded["soc_estimate"] == struct.unpack("<f", struct.pack("<f", 62.8))[0]
    assert decoded["dc_bus_v"] == struct.unpack("<f", struct.pack("<f", 509.2))[0]
    assert decoded["dc_bus_current"] == struct.unpack("<f", struct.pack("<f", 5.1))[0]
    assert decoded["delta_resolver_angle"] == 79.5
    assert decoded["inverter_freq"] == struct.unpack("<f", struct.pack("<f", -40.4))[0]

    sample = normalize_live_payload(json.dumps(decoded), "orion")
    assert sample is not None
    assert sample["values"]["dc_bus_v"] == struct.unpack("<f", struct.pack("<f", 509.2))[0]
    assert sample["values"]["dc_bus_current"] == struct.unpack("<f", struct.pack("<f", 5.1))[0]
    assert sample["values"]["inverter_power_kw_signed"] == sample["values"]["dc_bus_v"] * sample["values"]["dc_bus_current"] / 1000.0


def test_decode_current_pack_uses_cell_voltage_when_inverter_voltage_is_invalid():
    payload = _proto_message_field(
        5,
        _proto_float_field(3, 62.8)
        + _proto_packed_float_field(4, [3.88, 3.92, 3.98])
        + _proto_float_field(5, 0.4)
        + _proto_float_field(13, -12.5)
        + _proto_float_field(14, -12.4)
        + _proto_float_field(15, 512.0),
    )

    decoded = _decode_orion_protobuf(payload)
    assert decoded is not None
    assert decoded["dc_bus_v"] == struct.unpack("<f", struct.pack("<f", 0.4))[0]
    assert decoded["dc_bus_current"] == struct.unpack("<f", struct.pack("<f", -12.5))[0]
    assert decoded["hv_c"] == struct.unpack("<f", struct.pack("<f", -12.4))[0]
    assert decoded["hv_pack_v"] == 512.0

    sample = normalize_live_payload(json.dumps(decoded), "orion")
    assert sample is not None
    assert sample["values"]["cell_pack_voltage_est"] == sum(decoded["cells_v"]) / len(decoded["cells_v"]) * 130
    assert sample["values"]["inverter_power_kw_signed"] == sample["values"]["cell_pack_voltage_est"] * sample["values"]["dc_bus_current"] / 1000.0


def test_live_payload_uses_mechanical_power_fallback_when_current_is_missing():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "motor_speed": 3000, "torque_feedback": -40.0}',
        "orion",
    )

    assert sample is not None
    expected_kw = -40.0 * 3000 * (2 * 3.141592653589793 / 60) / 1000.0
    assert sample["values"]["mechanical_power_kw_signed"] == expected_kw
    assert sample["values"]["estimated_power_kw_signed"] == expected_kw


def test_decode_lhre_orion_protobuf_payload_repairs_current_thermal_fields():
    payload = _proto_message_field(
        8,
        _proto_float_field(21, 0.42)
        + _proto_float_field(22, 3.93)
        + _proto_float_field(23, 35.7)
        + _proto_float_field(24, 34.9)
        + _proto_float_field(25, 35.0)
        + _proto_float_field(26, 34.2),
    )

    decoded = _decode_orion_protobuf(payload)
    assert decoded is not None
    assert "max_cell_voltage" not in decoded
    assert decoded["min_cell_voltage"] == struct.unpack("<f", struct.pack("<f", 3.93))[0]
    assert decoded["module_a_temp"] == struct.unpack("<f", struct.pack("<f", 35.7))[0]
    assert decoded["module_b_temp"] == struct.unpack("<f", struct.pack("<f", 34.9))[0]
    assert decoded["module_c_temp"] == 35.0
    assert decoded["motor_loop_inverter_temp"] == struct.unpack("<f", struct.pack("<f", 34.2))[0]


def test_live_payload_derives_cell_voltage_extremes_from_cells_v():
    sample = normalize_live_payload(
        '{"time": 1777135260000, "cells_v": [3.52, 0, 3.64, 5.2, 3.58], "dc_bus_v": 469.4, "dc_bus_current": 0.4}',
        "orion",
    )

    assert sample is not None
    assert sample["values"]["min_cell_v"] == 3.52
    assert sample["values"]["max_cell_v"] == 3.64


def _proto_key(field_number: int, wire_type: int) -> bytes:
    return _proto_varint((field_number << 3) | wire_type)


def _proto_varint(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value & 0x7F
        value >>= 7
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def _proto_varint_field(field_number: int, value: int) -> bytes:
    return _proto_key(field_number, 0) + _proto_varint(value)


def _proto_float_field(field_number: int, value: float) -> bytes:
    return _proto_key(field_number, 5) + struct.pack("<f", value)


def _proto_packed_float_field(field_number: int, values: list[float]) -> bytes:
    body = b"".join(struct.pack("<f", value) for value in values)
    return _proto_key(field_number, 2) + _proto_varint(len(body)) + body


def _proto_message_field(field_number: int, body: bytes) -> bytes:
    return _proto_key(field_number, 2) + _proto_varint(len(body)) + body
