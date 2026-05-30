from __future__ import annotations

import datetime as dt
import json
import math
import queue
import socket
import struct
import time
import uuid
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .channel_chart_store import ChannelChartStore, apply_channel_chart
from .channels import ORION_CHANNELS
from .config import Settings
from .datalog import Channel, DataLog, Sample
from .local_kafka import LOCAL_TOPIC_BUS
from .models import KafkaTransport, LiveLapExportRequest, LiveSample
from .motec_ld import write_ld, write_ldx


def kafka_topic_for(source: str, settings: Settings, requested_topic: str | None = None) -> str:
    if requested_topic:
        topic = requested_topic.strip()
        if topic:
            return topic
    prefix = settings.kafka_topic_prefix.strip() or "grafana_data"
    return f"{prefix}_{source.strip().lower()}"


def mqtt_topic_for(source: str, settings: Settings, requested_topic: str | None = None) -> str:
    if requested_topic:
        topic = requested_topic.strip()
        if topic:
            return topic
    topic = settings.live_mqtt_topic.strip()
    return topic or source.strip().lower()


def kafka_transport_for(settings: Settings, requested_transport: str | None = None) -> KafkaTransport:
    value = (requested_transport or settings.kafka_mode or "local").strip().lower()
    if value in {"kafka", "broker", "remote"}:
        return "kafka"
    if value in {"mqtt", "mqtt-broker", "aws"}:
        return "mqtt"
    return "local"


def stream_kafka_samples(
    settings: Settings,
    source: str,
    topic: str | None = None,
    transport: str | None = None,
    sample_hz: float | None = None,
) -> Iterator[dict[str, Any]]:
    transport_name = kafka_transport_for(settings, transport)
    topic_name = mqtt_topic_for(source, settings, topic) if transport_name == "mqtt" else kafka_topic_for(source, settings, topic)
    sample_interval_s = _sample_interval_s(sample_hz)
    if transport_name == "mqtt":
        yield from _stream_mqtt_samples(settings, source, topic_name, sample_interval_s)
        return

    if transport_name == "local":
        timeout_s = max(0.25, settings.kafka_consumer_timeout_ms / 1000.0)
        subscription = LOCAL_TOPIC_BUS.open_subscription(topic_name)
        yield {
            "type": "status",
            "ok": True,
            "topic": topic_name,
            "transport": transport_name,
            "message": f"Listening to local topic {topic_name}.",
        }
        last_heartbeat = time.monotonic()
        last_sample_emit = 0.0
        try:
            while True:
                try:
                    payload = subscription.get(timeout=timeout_s)
                except queue.Empty:
                    yield {"type": "heartbeat", "topic": topic_name, "transport": transport_name, "t": int(time.time() * 1000)}
                    continue
                now = time.monotonic()
                if sample_interval_s is not None and now - last_sample_emit < sample_interval_s:
                    continue
                normalized = normalize_live_payload(json.dumps(payload, separators=(",", ":")), source)
                if normalized:
                    last_sample_emit = now
                    yield {"type": "sample", "topic": topic_name, "transport": transport_name, "sample": normalized}
                if time.monotonic() - last_heartbeat > 15:
                    last_heartbeat = time.monotonic()
                    yield {"type": "heartbeat", "topic": topic_name, "transport": transport_name, "t": int(time.time() * 1000)}
        except GeneratorExit:
            raise
        except Exception as exc:
            yield {
                "type": "status",
                "ok": False,
                "topic": topic_name,
                "transport": transport_name,
                "message": "Local stream stopped.",
                "detail": str(exc),
            }
        finally:
            LOCAL_TOPIC_BUS.close_subscription(topic_name, subscription)
        return

    try:
        from kafka import KafkaConsumer
    except Exception as exc:
        yield {
            "type": "status",
            "ok": False,
            "topic": topic_name,
            "transport": transport_name,
            "message": "Kafka client is not installed in this Python environment.",
            "detail": str(exc),
        }
        return

    servers = [item.strip() for item in settings.kafka_bootstrap_servers.split(",") if item.strip()]
    if not servers:
        yield {"type": "status", "ok": False, "topic": topic_name, "transport": transport_name, "message": "KAFKA_BOOTSTRAP_SERVERS is empty."}
        return

    group_id = f"motec-live-viewer-{source}-{uuid.uuid4().hex[:10]}"
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=max(250, settings.kafka_consumer_timeout_ms),
            request_timeout_ms=max(15000, settings.kafka_consumer_timeout_ms + 4000),
            api_version_auto_timeout_ms=5000,
            reconnect_backoff_ms=250,
            reconnect_backoff_max_ms=2000,
            value_deserializer=lambda value: value.decode("utf-8", "replace"),
        )
    except Exception as exc:
        yield {
            "type": "status",
            "ok": False,
            "topic": topic_name,
            "transport": transport_name,
            "message": "Unable to connect to Kafka.",
            "detail": str(exc),
        }
        return

    yield {"type": "status", "ok": True, "topic": topic_name, "transport": transport_name, "message": f"Broker connected; waiting for samples on {topic_name}."}
    last_heartbeat = time.monotonic()
    last_unparsed_notice = 0.0
    last_sample_emit = 0.0
    unparsed_count = 0
    try:
        while True:
            got_message = False
            for message in consumer:
                got_message = True
                now = time.monotonic()
                if sample_interval_s is not None and now - last_sample_emit < sample_interval_s:
                    continue
                normalized = normalize_live_payload(message.value, source)
                if normalized:
                    last_sample_emit = now
                    yield {"type": "sample", "topic": topic_name, "transport": transport_name, "sample": normalized}
                    continue
                unparsed_count += 1
                now = time.monotonic()
                if unparsed_count == 1 or now - last_unparsed_notice > 10:
                    last_unparsed_notice = now
                    yield {
                        "type": "status",
                        "ok": True,
                        "topic": topic_name,
                        "transport": transport_name,
                        "message": (
                            f"Messages are arriving on {topic_name}, but they are not JSON live samples. "
                            f"For LHRE parsed telemetry, use {kafka_topic_for(source, settings)}."
                        ),
                    }
                if time.monotonic() - last_heartbeat > 15:
                    last_heartbeat = time.monotonic()
                    yield {"type": "heartbeat", "topic": topic_name, "transport": transport_name, "t": int(time.time() * 1000)}
            if not got_message:
                yield {"type": "heartbeat", "topic": topic_name, "transport": transport_name, "t": int(time.time() * 1000)}
    except GeneratorExit:
        raise
    except Exception as exc:
        yield {"type": "status", "ok": False, "topic": topic_name, "transport": transport_name, "message": "Kafka stream stopped.", "detail": str(exc)}
    finally:
        try:
            consumer.close()
        except Exception:
            pass


def _sample_interval_s(sample_hz: float | None) -> float | None:
    if sample_hz is None or sample_hz <= 0:
        return None
    return 1.0 / min(50.0, max(0.2, float(sample_hz)))


def _stream_mqtt_samples(settings: Settings, source: str, topic_name: str, sample_interval_s: float | None = None) -> Iterator[dict[str, Any]]:
    if source.strip().lower() != "orion":
        yield {
            "type": "status",
            "ok": False,
            "topic": topic_name,
            "transport": "mqtt",
            "message": "Direct MQTT live decoding is currently implemented for Orion only.",
        }
        return

    timeout_s = max(0.25, settings.kafka_consumer_timeout_ms / 1000.0)
    try:
        sock = socket.create_connection((settings.live_mqtt_host, settings.live_mqtt_port), timeout=5)
        sock.settimeout(timeout_s)
        client_id = f"motec-live-{source}-{uuid.uuid4().hex[:10]}"
        _mqtt_send(sock, 0x10, _mqtt_connect_body(client_id))
        packet_type, body = _mqtt_read_packet(sock)
        if packet_type != 0x20 or len(body) < 2 or body[1] != 0:
            raise RuntimeError(f"MQTT CONNACK failed: {body.hex()}")
        _mqtt_send(sock, 0x82, struct.pack("!H", 1) + _mqtt_string(topic_name) + b"\x00")
    except Exception as exc:
        yield {
            "type": "status",
            "ok": False,
            "topic": topic_name,
            "transport": "mqtt",
            "message": "Unable to connect to MQTT.",
            "detail": str(exc),
        }
        return

    yield {
        "type": "status",
        "ok": True,
        "topic": topic_name,
        "transport": "mqtt",
        "message": f"MQTT connected to {settings.live_mqtt_host}:{settings.live_mqtt_port}; waiting for {topic_name}.",
    }
    last_ping = time.monotonic()
    last_sample_emit = 0.0
    try:
        while True:
            try:
                packet_type, body = _mqtt_read_packet(sock)
            except socket.timeout:
                now = time.monotonic()
                if now - last_ping > 20:
                    _mqtt_send(sock, 0xC0, b"")
                    last_ping = now
                yield {"type": "heartbeat", "topic": topic_name, "transport": "mqtt", "t": int(time.time() * 1000)}
                continue

            now = time.monotonic()
            if now - last_ping > 20:
                _mqtt_send(sock, 0xC0, b"")
                last_ping = now

            message_type = packet_type >> 4
            if message_type == 3:
                now = time.monotonic()
                if sample_interval_s is not None and now - last_sample_emit < sample_interval_s:
                    continue
                payload = _mqtt_publish_payload(packet_type, body)
                decoded = _decode_orion_protobuf(payload)
                if not decoded:
                    continue
                normalized = normalize_live_payload(json.dumps(decoded, separators=(",", ":")), source)
                if normalized:
                    last_sample_emit = now
                    yield {"type": "sample", "topic": topic_name, "transport": "mqtt", "sample": normalized}
            elif message_type == 13:
                continue
    except GeneratorExit:
        raise
    except Exception as exc:
        yield {"type": "status", "ok": False, "topic": topic_name, "transport": "mqtt", "message": "MQTT stream stopped.", "detail": str(exc)}
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _mqtt_connect_body(client_id: str) -> bytes:
    return _mqtt_string("MQTT") + b"\x04\x02" + struct.pack("!H", 30) + _mqtt_string(client_id)


def _mqtt_send(sock: socket.socket, packet_type: int, body: bytes) -> None:
    sock.sendall(bytes([packet_type]) + _mqtt_remaining_length(len(body)) + body)


def _mqtt_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!H", len(encoded)) + encoded


def _mqtt_remaining_length(length: int) -> bytes:
    out = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length:
            digit |= 128
        out.append(digit)
        if not length:
            return bytes(out)


def _mqtt_read_packet(sock: socket.socket) -> tuple[int, bytes]:
    packet_type = _recv_exact(sock, 1)[0]
    multiplier = 1
    remaining = 0
    while True:
        digit = _recv_exact(sock, 1)[0]
        remaining += (digit & 127) * multiplier
        if not digit & 128:
            break
        multiplier *= 128
    return packet_type, _recv_exact(sock, remaining)


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise EOFError("socket closed")
        data.extend(chunk)
    return bytes(data)


def _mqtt_publish_payload(packet_type: int, body: bytes) -> bytes:
    if len(body) < 2:
        return b""
    topic_length = int.from_bytes(body[:2], "big")
    payload_start = 2 + topic_length
    qos = (packet_type >> 1) & 0x03
    if qos:
        payload_start += 2
    return body[payload_start:]


_DYNAMICS_FLOATS = {
    3: "gps_speed",
    4: "accel_pedal_travel",
    5: "steer_col_angle",
    14: "bl_ride_height",
    15: "bl_strain_gauge_v",
    16: "bl_sus_pot_v",
    17: "blw_speed",
    18: "br_ride_height",
    19: "br_strain_gauge_v",
    20: "br_sus_pot_v",
    21: "brw_speed",
    22: "fl_ride_height",
    23: "fl_strain_gauge_v",
    24: "fl_sus_pot_v",
    25: "flw_speed",
    26: "fr_ride_height",
    27: "fr_strain_gauge_v",
    28: "fr_sus_pot_v",
    29: "frw_speed",
    30: "ride_height",
    31: "wheel_speed",
}
_DYNAMICS_REPEATED_FLOATS = {
    1: "gps",
    2: "gps_imu",
    6: "bl_sprung_accel",
    7: "bl_unsprung_accel",
    8: "br_sprung_accel",
    9: "br_unsprung_accel",
    10: "fl_sprung_accel",
    11: "fl_unsprung_accel",
    12: "fr_sprung_accel",
    13: "fr_unsprung_accel",
}
_CONTROLS_FLOATS = {
    1: "motor_speed",
    2: "torque_feedback",
    3: "apps1_travel",
    4: "apps1_v",
    5: "apps2_travel",
    6: "apps2_v",
    7: "bpps1_travel",
    8: "bpps1_v",
    9: "bpps2_travel",
    10: "bpps2_v",
    11: "brake_bias",
    12: "brake_light_pct",
    13: "brake_pressure_f",
    14: "brake_pressure_rall",
    15: "brake_pressure_rbll",
    16: "bse1_v",
    17: "bse2_v",
    18: "bse3_v",
    19: "lights_current",
    20: "rpm_request",
    21: "torque_command",
    22: "torque_limit",
    23: "torque_request",
    24: "commanded_torque",
    25: "motor_angle",
    28: "torque_shudder",
}
_CONTROLS_BOOLEANS = {26: "direction", 27: "enable"}
_PACK_FLOATS = {
    1: "bus_voltage",
    2: "lv_boards_current",
    4: "dc_bus_v",
    5: "delta_resolver_angle",
    6: "inverter_freq",
    7: "neutral_output_v",
    8: "time_since_on",
    9: "vab_vq_v",
    10: "vbc_vd_v",
    12: "dc_bus_current",
    13: "hv_c",
    14: "hv_pack_v",
    15: "hv_soc",
    16: "lv_batt_c",
    17: "lv_batt_t",
    18: "lv_batt_v",
    19: "phase_a_current",
    20: "phase_b_current",
    21: "phase_c_current",
}
_PACK_REPEATED_FLOATS = {3: "cells_v", 11: "cells_temps"}
_THERMAL_FLOATS = {
    1: "batt_cooling_current",
    2: "motor_cooling_current",
    3: "ambient_temp",
    4: "motor_temp",
    5: "batt_loop_batt_temp",
    6: "batt_loop_rad_fan_speed",
    7: "batt_loop_rad_temp",
    8: "battery_fan_rpm",
    9: "bus_bar_temp1",
    10: "bus_bar_temp2",
    11: "bus_bar_temp3",
    12: "cell_bottom_temp",
    13: "cell_top_temp",
    14: "coolant_flow_lpm",
    15: "coolant_temp",
    16: "discharge_r_temp",
    17: "fan_rpm",
    18: "gate_driver_temp",
    19: "inverter_temp",
    20: "module_a_temp",
    21: "module_b_temp",
    22: "module_c_temp",
    23: "motor_loop_inverter_temp",
    24: "motor_loop_motor_temp",
    25: "motor_loop_rad_temp",
}
_BOARD_STATUS_FLOATS = {
    1: "csm_last_seen_s",
    2: "dui_last_seen_s",
    3: "hvc_last_seen_s",
    4: "inverter_last_seen_s",
    5: "pdu_last_seen_s",
    6: "tsm_last_seen_s",
    7: "usm_last_seen_s",
    8: "vcu_last_seen_s",
}


def _decode_orion_protobuf(payload: bytes) -> dict[str, Any] | None:
    try:
        out: dict[str, Any] = {}
        index = 0
        while index < len(payload):
            key, index = _read_varint(payload, index)
            field_number = key >> 3
            wire_type = key & 0x07
            if field_number in {1, 2} and wire_type == 0:
                value, index = _read_varint(payload, index)
                out["time" if field_number == 1 else "packet_id"] = value
            elif field_number in {3, 4, 5, 8, 9} and wire_type == 2:
                length, index = _read_varint(payload, index)
                child = payload[index:index + length]
                index += length
                if field_number == 3:
                    out.update(_parse_orion_child(child, _DYNAMICS_FLOATS, _DYNAMICS_REPEATED_FLOATS))
                    gps = out.get("gps")
                    if isinstance(gps, list) and len(gps) >= 2:
                        out["latitude"] = gps[0]
                        out["longitude"] = gps[1]
                elif field_number == 4:
                    out.update(_parse_orion_child(child, _CONTROLS_FLOATS, booleans=_CONTROLS_BOOLEANS))
                elif field_number == 5:
                    out.update(_parse_orion_child(child, _PACK_FLOATS, _PACK_REPEATED_FLOATS))
                elif field_number == 8:
                    out.update(_parse_orion_child(child, _THERMAL_FLOATS))
                elif field_number == 9:
                    out.update(_parse_orion_child(child, _BOARD_STATUS_FLOATS))
            else:
                index = _skip_protobuf_value(payload, index, wire_type)
        return out or None
    except Exception:
        return None


def _parse_orion_child(
    payload: bytes,
    floats: dict[int, str],
    repeated_floats: dict[int, str] | None = None,
    booleans: dict[int, str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    repeated_floats = repeated_floats or {}
    booleans = booleans or {}
    index = 0
    while index < len(payload):
        key, index = _read_varint(payload, index)
        field_number = key >> 3
        wire_type = key & 0x07
        if field_number in floats and wire_type == 5 and index + 4 <= len(payload):
            out[floats[field_number]] = struct.unpack("<f", payload[index:index + 4])[0]
            index += 4
        elif field_number in repeated_floats and wire_type == 2:
            length, index = _read_varint(payload, index)
            chunk = payload[index:index + length]
            index += length
            out[repeated_floats[field_number]] = [
                struct.unpack("<f", chunk[offset:offset + 4])[0]
                for offset in range(0, len(chunk) - len(chunk) % 4, 4)
            ]
        elif field_number in repeated_floats and wire_type == 5 and index + 4 <= len(payload):
            out.setdefault(repeated_floats[field_number], []).append(struct.unpack("<f", payload[index:index + 4])[0])
            index += 4
        elif field_number in booleans and wire_type == 0:
            value, index = _read_varint(payload, index)
            out[booleans[field_number]] = bool(value)
        else:
            index = _skip_protobuf_value(payload, index, wire_type)
    return out


def _read_varint(payload: bytes, index: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while index < len(payload):
        byte = payload[index]
        index += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, index
        shift += 7
    raise ValueError("truncated varint")


def _skip_protobuf_value(payload: bytes, index: int, wire_type: int) -> int:
    if wire_type == 0:
        _value, index = _read_varint(payload, index)
        return index
    if wire_type == 1:
        return min(len(payload), index + 8)
    if wire_type == 2:
        length, index = _read_varint(payload, index)
        return min(len(payload), index + length)
    if wire_type == 5:
        return min(len(payload), index + 4)
    raise ValueError(f"unsupported protobuf wire type {wire_type}")


def normalize_live_payload(raw_payload: str, source: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None

    values = _flatten_numeric(payload)
    t = _timestamp_ms(_first(payload, ["time", "timestamp", "packet_time", "packet.time"]))
    lat, lon = _gps(payload)
    speed = _number(
        _first(
            payload,
            [
                "speed",
                "dash_speed",
                "dashSpeed",
                "gps_speed",
                "gpsSpeed",
                "gps_velocity",
                "gpsVelocity",
                "f_gps_velocity",
                "wheel_speed",
                "dynamics_gps_speed",
                "dynamics_gps_velocity",
                "dynamics.speed",
                "dynamics.dashSpeed",
                "dynamics.gps_speed",
                "dynamics.gpsSpeed",
                "dynamics.gps_velocity",
                "dynamics.gpsVelocity",
            ],
        )
    )
    if speed is None:
        wheel_speeds = [
            _number(_first(payload, [key, f"dynamics.{key}"]))
            for key in ("flw_speed", "frw_speed", "blw_speed", "brw_speed", "flwSpeed", "frwSpeed", "blwSpeed", "brwSpeed")
        ]
        valid = [value for value in wheel_speeds if value is not None]
        speed = sum(valid) / len(valid) if valid else None

    dc_bus_v = _number(
        _first(
            payload,
            [
                "dc_bus_v",
                "bus_voltage",
                "pack_dc_bus_v",
                "pack_bus_voltage",
                "dcBusV",
                "busVoltage",
                "dc_bus_voltage",
                "pack_dc_bus_voltage",
                "dcBusVoltage",
                "pack.dc_bus_v",
                "pack.bus_voltage",
                "pack.busVoltage",
                "pack.dcBusV",
                "pack.dc_bus_voltage",
                "pack.dcBusVoltage",
                "inverter.dc_bus_v",
                "inverter.dcBusV",
                "inverter.dc_bus_voltage",
                "inverter.dcBusVoltage",
                "inverter.vdc",
                "inverter_v",
                "dynamics_inverter_v",
            ],
        )
    )
    dc_bus_current = _number(
        _first(
            payload,
            [
                "dc_bus_current",
                "pack_dc_bus_current",
                "dcBusCurrent",
                "dc_bus_c",
                "pack_dc_bus_c",
                "dcBusC",
                "pack.dc_bus_current",
                "pack.dcBusCurrent",
                "pack.dc_bus_c",
                "pack.dcBusC",
                "inverter.dc_bus_current",
                "inverter.dcBusCurrent",
                "inverter.dc_bus_c",
                "inverter.dcBusC",
                "inverter.idc",
                "inverter_c",
                "dynamics_inverter_c",
            ],
        )
    )
    hv_v_raw = _number(
        _first(
            payload,
            [
                "hv_pack_v",
                "pack_hv_pack_v",
                "hvPackV",
                "hv_pack_voltage",
                "pack.hvPackV",
                "pack.hv_pack_v",
                "pack.hv_pack_voltage",
            ],
        )
    )
    hv_c_raw = _number(_first(payload, ["hv_c", "pack_hv_c", "hvC", "hv_current", "pack.hvC", "pack.hv_c", "pack.hv_current"]))
    hv_v = hv_v_raw if hv_v_raw is not None else dc_bus_v
    hv_c = hv_c_raw if hv_c_raw is not None else dc_bus_current
    power_kw = _number(_first(payload, ["power_kw", "powerKw", "hv_power_kw", "pack_power_kw", "pack.power_kw", "pack.powerKw"]))
    if power_kw is None:
        hv_power = _number(_first(payload, ["hv_power", "hvPower", "pack.hvPower"]))
        if hv_power is not None:
            power_kw = abs(hv_power) / 1000.0
    if power_kw is None and hv_v is not None and hv_c is not None:
        power_kw = abs(hv_v * hv_c) / 1000.0
    elif power_kw is not None:
        power_kw = abs(power_kw)

    if speed is not None:
        values.setdefault("speed", float(speed))
    if hv_v is not None:
        values.setdefault("hv_pack_v", float(hv_v))
    if hv_c is not None:
        values.setdefault("hv_c", float(hv_c))
    if dc_bus_v is not None:
        values.setdefault("dc_bus_v", float(dc_bus_v))
    if dc_bus_current is not None:
        values.setdefault("dc_bus_current", float(dc_bus_current))
    if power_kw is not None:
        values.setdefault("power_kw", float(power_kw))

    return {
        "t": t,
        "source": source,
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "hv_pack_v": hv_v,
        "hv_c": hv_c,
        "power_kw": power_kw,
        "values": values,
    }


def export_live_lap(
    request: LiveLapExportRequest,
    settings: Settings,
    channel_charts: ChannelChartStore | None = None,
) -> tuple[str, Path, list[Path]]:
    if not request.samples:
        raise ValueError("No live samples were supplied.")
    samples = sorted(request.samples, key=lambda sample: sample.t)
    start_ms = samples[0].t
    end_ms = samples[-1].t
    if end_ms <= start_ms:
        raise ValueError("Live lap samples must span a positive time range.")

    export_id = _safe_name(f"live_{request.car}_{request.track_slug or 'track'}_latest")
    out_dir = settings.resolved_export_dir / export_id
    out_dir.mkdir(parents=True, exist_ok=True)

    log = _datalog_from_live_samples(samples, start_ms, request.car)
    chart = channel_charts.load(request.channel_chart_slug) if channel_charts and request.channel_chart_slug else None
    apply_channel_chart(log, chart)
    log.resample(request.frequency_hz)

    metadata = {
        "vehicle_id": request.car.title(),
        "event": "Live Telemetry",
        "session": request.lap_label,
        "short_comment": request.lap_label,
        "datetime": dt.datetime.fromtimestamp(start_ms / 1000, tz=dt.UTC).isoformat(),
        **request.metadata,
    }
    stem = _safe_name(f"{request.car.title()}__live_latest")
    ld_path = out_dir / f"{stem}.ld"
    ldx_path = out_dir / f"{stem}.ldx"
    write_ld(ld_path, log, metadata=metadata)
    write_ldx(ldx_path, [0.0, (end_ms - start_ms) / 1000.0], [])
    files = [ld_path, ldx_path]

    zip_path = settings.resolved_export_dir / f"{export_id}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, arcname=path.name)
    return export_id, zip_path, files


def _datalog_from_live_samples(samples: list[LiveSample], start_ms: int, car: str) -> DataLog:
    definitions = {channel.key: channel for channel in ORION_CHANNELS}
    values_by_key: dict[str, list[Sample]] = {}
    for sample in samples:
        elapsed = (sample.t - start_ms) / 1000.0
        for key, value in sample.values.items():
            numeric = _number(value)
            if numeric is None or not math.isfinite(numeric):
                continue
            values_by_key.setdefault(key, []).append(Sample(elapsed, numeric))

    log = DataLog(f"{car}_live", metadata={"source": "kafka_live"})
    for key, channel_samples in values_by_key.items():
        definition = definitions.get(key)
        name = definition.label if definition else _label(key)
        unit = definition.unit if definition else _unit(key)
        quantity = definition.quantity if definition else _quantity(key)
        log.channels[key] = Channel(name=name, unit=unit, quantity=quantity, samples=channel_samples)
    return log


def _flatten_numeric(payload: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                walk(f"{prefix}_{key}" if prefix else str(key), child)
            return
        number = _number(value)
        if number is not None and math.isfinite(number):
            out[_safe_key(prefix)] = number

    walk("", payload)
    return out


def _first(payload: dict[str, Any], paths: list[str]) -> Any:
    for path in paths:
        current: Any = payload
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current is not None:
            return current
    return None


def _gps(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    lat = _number(_first(payload, ["latitude", "lat", "gps_latitude", "dynamics_gps_latitude", "dynamics.latitude"]))
    lon = _number(_first(payload, ["longitude", "lon", "lng", "gps_longitude", "dynamics_gps_longitude", "dynamics.longitude"]))
    if lat is not None and lon is not None:
        return lat, lon
    for path in ["gps", "f_gps", "b_gps", "dynamics.gps", "dynamics.fGps", "dynamics.bGps"]:
        pair = _first(payload, [path])
        if isinstance(pair, list | tuple) and len(pair) >= 2:
            first = _number(pair[0])
            second = _number(pair[1])
            if first is not None and second is not None:
                return first, second
    return None, None


def _timestamp_ms(value: Any) -> int:
    number = _number(value)
    if number is None or number <= 0:
        return int(time.time() * 1000)
    if number < 10_000_000_000:
        return int(number * 1000)
    return int(number)


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_key(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_") or "value"


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value).strip("_") or "live"


def _label(key: str) -> str:
    return key.replace("_", " ").title()


def _unit(key: str) -> str:
    lower = key.lower()
    if "speed" in lower:
        return "m/s"
    if "rpm" in lower:
        return "rpm"
    if lower.endswith("_v") or "voltage" in lower:
        return "V"
    if lower.endswith("_c") or "current" in lower:
        return "A"
    if "power" in lower:
        return "kW"
    if "temp" in lower:
        return "C"
    return ""


def _quantity(key: str) -> str:
    lower = key.lower()
    if "speed" in lower or "rpm" in lower:
        return "speed"
    if lower.endswith("_v") or "voltage" in lower:
        return "voltage"
    if lower.endswith("_c") or "current" in lower:
        return "current"
    if "power" in lower:
        return "power"
    if "temp" in lower:
        return "temperature"
    return "value"
