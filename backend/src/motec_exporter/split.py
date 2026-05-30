from __future__ import annotations

import math

from .datalog import Channel
from .models import GateLine, GpsPoint


def split_mode(channel: Channel) -> str:
    name = channel.name.lower()
    unit = channel.unit.lower()
    if "rpm" in name or unit == "rpm":
        return "rpm"
    if "state" in name or unit in {"bool", "boolean", "state", "status"}:
        return "state"
    if "speed" in name or unit in {"m/s", "mph", "kph", "km/h", "rad/s"}:
        return "speed"
    return "generic"


def motion_threshold(channel: Channel) -> float:
    if not channel.samples:
        return 0.0
    peak = max(abs(sample.value) for sample in channel.samples)
    mode = split_mode(channel)
    unit = channel.unit.lower()
    if mode == "state":
        return 0.5
    if mode == "rpm":
        return max(1200.0, peak * 0.08)
    if unit in {"mph"}:
        return max(3.0, peak * 0.03)
    if unit in {"m/s"}:
        return max(1.0, peak * 0.03)
    return max(1.0, peak * 0.03)


def detect_active_range(channel: Channel) -> tuple[float, float] | None:
    if not channel.samples:
        return None
    threshold = motion_threshold(channel)
    mode = split_mode(channel)
    active: list[float] = []
    for sample in channel.samples:
        magnitude = abs(sample.value)
        if (magnitude > threshold if mode == "state" else magnitude >= threshold):
            active.append(sample.t)
    if not active:
        return channel.start, channel.end
    return active[0], active[-1]


def detect_split_ranges(
    channel: Channel,
    *,
    minimum_gap_s: float = 12.0,
    minimum_segment_s: float = 8.0,
) -> list[tuple[float, float]]:
    active = detect_active_range(channel)
    if active is None:
        return []
    active_start, active_end = active
    threshold = motion_threshold(channel)
    mode = split_mode(channel)
    ranges: list[tuple[float, float]] = []
    segment_start = active_start
    gap_start: float | None = None
    previous_t: float | None = None

    for sample in channel.samples:
        if sample.t < active_start:
            continue
        if sample.t > active_end:
            break
        stationary = not (abs(sample.value) > threshold if mode == "state" else abs(sample.value) >= threshold)
        if stationary and gap_start is None:
            gap_start = previous_t if previous_t is not None else sample.t
        elif not stationary and gap_start is not None:
            gap_end = previous_t if previous_t is not None else sample.t
            if gap_end - gap_start >= minimum_gap_s and gap_start - segment_start >= minimum_segment_s:
                ranges.append((segment_start, gap_start))
                segment_start = sample.t
            gap_start = None
        previous_t = sample.t

    if gap_start is not None and previous_t is not None:
        if previous_t - gap_start >= minimum_gap_s and gap_start - segment_start >= minimum_segment_s:
            ranges.append((segment_start, gap_start))
            segment_start = math.nan

    if math.isfinite(segment_start) and active_end - segment_start >= minimum_segment_s:
        ranges.append((segment_start, active_end))
    if not ranges and active_end > active_start:
        ranges = [(active_start, active_end)]
    return normalize_ranges(ranges)


def normalize_ranges(ranges: list[tuple[float, float]]) -> list[tuple[float, float]]:
    clean = sorted((float(a), float(b)) for a, b in ranges if b > a)
    merged: list[tuple[float, float]] = []
    for start, end in clean:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _orientation(ax: float, ay: float, bx: float, by: float, cx: float, cy: float) -> float:
    return (by - ay) * (cx - bx) - (bx - ax) * (cy - by)


def _segments_cross(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float], d: tuple[float, float]) -> bool:
    o1 = _orientation(*a, *b, *c)
    o2 = _orientation(*a, *b, *d)
    o3 = _orientation(*c, *d, *a)
    o4 = _orientation(*c, *d, *b)
    return o1 * o2 < 0 and o3 * o4 < 0


def gate_crossing_times(points: list[GpsPoint], gates: list[GateLine]) -> dict[str, list[int]]:
    crossings: dict[str, list[int]] = {gate.id: [] for gate in gates}
    if len(points) < 2:
        return crossings
    for prev, cur in zip(points, points[1:]):
        trace_a = (prev.lon, prev.lat)
        trace_b = (cur.lon, cur.lat)
        for gate in gates:
            gate_a = (gate.lon1, gate.lat1)
            gate_b = (gate.lon2, gate.lat2)
            if _segments_cross(trace_a, trace_b, gate_a, gate_b):
                crossings[gate.id].append(cur.t)
    return crossings

