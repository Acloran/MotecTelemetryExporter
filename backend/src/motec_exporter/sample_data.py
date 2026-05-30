from __future__ import annotations

import datetime as dt
import math

from .channels import ORION_CHANNELS, get_channel
from .datalog import Channel, DataLog, Sample
from .models import DriveDay, GpsPoint, SegmentSummary, SeriesPoint, SessionSummary
from .split import detect_split_ranges


BASE_MS = int(dt.datetime(2026, 4, 25, 14, 0, tzinfo=dt.UTC).timestamp() * 1000)
SESSION_LENGTH_MS = 12 * 60 * 1000
STEP_MS = 100


def _value(channel_key: str, elapsed_s: float) -> float:
    run = 60 <= elapsed_s <= 260 or 350 <= elapsed_s <= 610
    wave = math.sin(elapsed_s / 8.0)
    if channel_key == "motor_rpm":
        return (4200 + 1600 * wave + 400 * math.sin(elapsed_s / 1.7)) if run else 250 * max(0, math.sin(elapsed_s / 9.0))
    if channel_key == "rpm_request":
        return 5000 if run else 0
    if channel_key in {"wheel_speed", "gps_speed", "flw_speed", "frw_speed", "blw_speed", "brw_speed"}:
        return (18 + 5 * wave) if run else 0
    if channel_key == "apps1_travel":
        return max(0, 55 + 35 * wave) if run else 0
    if channel_key == "apps2_travel":
        return max(0, 54 + 34 * wave) if run else 0
    if channel_key == "brake_pressure_f":
        return max(0, 350 * math.sin(elapsed_s / 13.0)) if run else 0
    if "torque" in channel_key:
        return (80 + 25 * wave) if run else 0
    if channel_key == "steer_col_angle":
        return 35 * math.sin(elapsed_s / 5.5)
    if channel_key in {"hv_pack_v", "dc_bus_v"}:
        return 420 - 8 * math.sin(elapsed_s / 22.0)
    if channel_key == "hv_c":
        return 35 + 20 * wave if run else 4
    if channel_key == "hv_soc":
        return 92 - elapsed_s / 1000.0
    if "temp" in channel_key:
        return 32 + min(45, elapsed_s / 18.0) + 2 * wave
    return wave


def list_days() -> list[DriveDay]:
    return [
        DriveDay(
            date="2026-04-25",
            sessions=2,
            start_ms=BASE_MS,
            end_ms=BASE_MS + SESSION_LENGTH_MS,
            label="Sample Orion drive day",
        )
    ]


def list_sessions(date: str) -> list[SessionSummary]:
    if date != "2026-04-25":
        return []
    return [
        SessionSummary(
            id="sample-session-1",
            label="Sample Session 1",
            start_ms=BASE_MS,
            end_ms=BASE_MS + SESSION_LENGTH_MS,
            duration_s=SESSION_LENGTH_MS / 1000,
            source="sample",
        )
    ]


def series(channel_key: str, start_ms: int, end_ms: int, *, max_points: int = 5000) -> list[SeriesPoint]:
    points: list[SeriesPoint] = []
    span = max(1, end_ms - start_ms)
    step = max(STEP_MS, math.ceil(span / max_points))
    for t in range(start_ms, end_ms + 1, step):
        points.append(SeriesPoint(t=t, v=_value(channel_key, (t - BASE_MS) / 1000)))
    return points


def gps(start_ms: int, end_ms: int, *, max_points: int = 2000) -> list[GpsPoint]:
    points: list[GpsPoint] = []
    span = max(1, end_ms - start_ms)
    step = max(250, math.ceil(span / max_points))
    center_lat = 30.3922
    center_lon = -97.7287
    for t in range(start_ms, end_ms + 1, step):
        elapsed = (t - BASE_MS) / 1000
        theta = elapsed / 34.0
        lat = center_lat + 0.0011 * math.sin(theta) + 0.0002 * math.sin(theta * 3)
        lon = center_lon + 0.0018 * math.cos(theta)
        points.append(GpsPoint(t=t, lat=lat, lon=lon))
    return points


def datalog(channel_keys: list[str], start_ms: int, end_ms: int) -> DataLog:
    log = DataLog("orion_sample", metadata={"source": "sample"})
    for key in channel_keys:
        definition = get_channel(key)
        samples = [
            Sample((point.t - start_ms) / 1000.0, point.v or 0.0)
            for point in series(key, start_ms, end_ms, max_points=max(1, (end_ms - start_ms) // STEP_MS))
        ]
        log.channels[key] = Channel(definition.label, definition.unit, definition.quantity, samples)
    return log


def auto_segments(start_ms: int, end_ms: int, channel_key: str = "motor_rpm") -> list[SegmentSummary]:
    definition = get_channel(channel_key)
    samples = [
        Sample((point.t - start_ms) / 1000.0, point.v or 0.0)
        for point in series(channel_key, start_ms, end_ms, max_points=max(1, (end_ms - start_ms) // STEP_MS))
    ]
    ranges = detect_split_ranges(Channel(definition.label, definition.unit, definition.quantity, samples))
    return [
        SegmentSummary(
            id=f"auto-{index}",
            label=f"Auto split {index}",
            start_ms=start_ms + int(a * 1000),
            end_ms=start_ms + int(b * 1000),
            duration_s=b - a,
            source_channel=channel_key,
            has_gps=True,
            gps_points=max(1, int((b - a) * 4)),
        )
        for index, (a, b) in enumerate(ranges, start=1)
    ]
