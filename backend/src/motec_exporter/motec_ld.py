from __future__ import annotations

import datetime as dt
import math
import struct
import xml.etree.ElementTree as ET
from array import array
from pathlib import Path

from .datalog import Channel, DataLog


VEHICLE_PTR = 1762
VENUE_PTR = 5078
EVENT_PTR = 8180
HEADER_PTR = 11336
HEAD_FMT = "<I4xII20xI24xHHHI8sHHI4x16s16x16s16x64s64s64x64s64x1024xI66x64s126x64s64s"
VEHICLE_FMT = "<64s128xI32s32s"
VENUE_FMT = "<64s1034xH"
EVENT_FMT = "<64s64s1024sH"
CHAN_FMT = "<IIIIHHHHHHHh32s8s12s40x"
CHANNEL_HEADER_SIZE = struct.calcsize(CHAN_FMT)


def _fixed(text: str, size: int) -> bytes:
    encoded = str(text or "").encode("ascii", "ignore")
    return encoded[: size - 1] if len(encoded) >= size else encoded


def write_ld(path: Path, data_log: DataLog, *, metadata: dict[str, str] | None = None) -> None:
    metadata = metadata or {}
    channels = [channel for channel in data_log.channels.values() if channel.samples]
    channel_headers: list[bytes] = []
    channel_data: list[bytes] = []
    meta_ptr = HEADER_PTR
    data_ptr = HEADER_PTR + len(channels) * CHANNEL_HEADER_SIZE

    for index, channel in enumerate(channels):
        values = array("f", [_clean_float(sample.value) for sample in channel.samples])
        if values.itemsize != 4:
            values = array("f", list(values))
        data_bytes = values.tobytes()
        prev_meta = 0 if index == 0 else meta_ptr - CHANNEL_HEADER_SIZE
        next_meta = 0 if index == len(channels) - 1 else meta_ptr + CHANNEL_HEADER_SIZE
        freq = max(1, int(round(channel.average_frequency or 1)))
        channel_headers.append(
            struct.pack(
                CHAN_FMT,
                prev_meta,
                next_meta,
                data_ptr,
                len(values),
                0x2EE1 + index,
                0x07,
                4,
                freq,
                0,
                1,
                1,
                0,
                _fixed(channel.name, 32),
                _fixed(channel.quantity, 8),
                _fixed(channel.unit, 12),
            )
        )
        meta_ptr += CHANNEL_HEADER_SIZE
        data_ptr += len(data_bytes)
        channel_data.append(data_bytes)

    log_dt = _metadata_datetime(metadata)
    header = struct.pack(
        HEAD_FMT,
        0x40,
        HEADER_PTR,
        HEADER_PTR + len(channels) * CHANNEL_HEADER_SIZE,
        EVENT_PTR,
        1,
        0x4240,
        0xF,
        0x1F44,
        b"ADL",
        420,
        0xADB0,
        len(channels),
        _fixed(log_dt.strftime("%d/%m/%Y"), 16),
        _fixed(log_dt.strftime("%H:%M:%S"), 16),
        _fixed(metadata.get("driver", ""), 64),
        _fixed(metadata.get("vehicle_id", "Orion"), 64),
        _fixed(metadata.get("venue", ""), 64),
        0xC81A4,
        _fixed(metadata.get("short_comment", data_log.name), 64),
        _fixed(metadata.get("event", "Telemetry Export"), 64),
        _fixed(metadata.get("session", ""), 64),
    )
    vehicle = struct.pack(
        VEHICLE_FMT,
        _fixed(metadata.get("vehicle_id", "Orion"), 64),
        int(metadata.get("vehicle_weight", "0") or 0),
        _fixed(metadata.get("vehicle_type", "EV"), 32),
        _fixed(metadata.get("vehicle_comment", ""), 32),
    )
    venue = struct.pack(VENUE_FMT, _fixed(metadata.get("venue", ""), 64), VEHICLE_PTR)
    event = struct.pack(
        EVENT_FMT,
        _fixed(metadata.get("event", "Telemetry Export"), 64),
        _fixed(metadata.get("session", ""), 64),
        _fixed(metadata.get("long_comment", ""), 1024),
        VENUE_PTR,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        file.write(header)
        file.seek(VEHICLE_PTR)
        file.write(vehicle)
        file.seek(VENUE_PTR)
        file.write(venue)
        file.seek(EVENT_PTR)
        file.write(event)
        file.seek(HEADER_PTR)
        for header_bytes in channel_headers:
            file.write(header_bytes)
        for data_bytes in channel_data:
            file.write(data_bytes)


def write_ldx(path: Path, primary_beacons_s: list[float], split_beacons_s: list[float] | None = None) -> None:
    split_beacons_s = split_beacons_s or []
    markers = [
        ("BCN", f"Manual.{index}", seconds)
        for index, seconds in enumerate(_normal_times(primary_beacons_s), start=1)
    ]
    markers.extend(
        ("SPLTBCN", f"Split.{index}", seconds)
        for index, seconds in enumerate(_normal_times(split_beacons_s), start=1)
    )
    markers.sort(key=lambda item: (item[2], 0 if item[0] == "BCN" else 1))

    root = ET.Element("LDXFile", Version="1.6", Locale="English")
    layers = ET.SubElement(root, "Layers")
    layer = ET.SubElement(layers, "Layer")
    marker_block = ET.SubElement(layer, "MarkerBlock")
    marker_group = ET.SubElement(marker_block, "MarkerGroup", Name="Beacons", Index="3")
    for class_name, name, seconds in markers:
        ET.SubElement(
            marker_group,
            "Marker",
            Version="100",
            ClassName=class_name,
            Name=name,
            Flags="77",
            Time=f"{seconds * 1_000_000:.6f}",
        )
    ET.SubElement(layer, "RangeBlock")
    details = ET.SubElement(layer, "Details")
    ET.SubElement(details, "String", Id="Total Laps", Value=str(len(_normal_times(primary_beacons_s)) + 1))
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="utf-8", xml_declaration=True)


def _clean_float(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(-3.4e38, min(3.4e38, float(value)))


def _normal_times(values: list[float]) -> list[float]:
    output: list[float] = []
    previous: float | None = None
    for value in sorted(float(item) for item in values):
        if previous is not None and value - previous < 0.001:
            continue
        output.append(value)
        previous = value
    return output


def _metadata_datetime(metadata: dict[str, str]) -> dt.datetime:
    raw = metadata.get("datetime")
    if raw:
        try:
            parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.astimezone(dt.UTC).replace(tzinfo=None)
        except ValueError:
            pass
    return dt.datetime.now()
