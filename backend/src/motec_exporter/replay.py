from __future__ import annotations

import bisect
import json
import math
import threading
import time
from typing import Any

from .config import Settings
from .live import kafka_topic_for, kafka_transport_for
from .local_kafka import LOCAL_TOPIC_BUS
from .models import GpsPoint, ReplayStartRequest, ReplayStatus, SeriesPoint
from .telemetry import TelemetryService


MAX_REPLAY_FRAMES = 30_000
MAX_REPLAY_CHANNELS = 18


class ReplayManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._status = ReplayStatus()

    def status(self) -> ReplayStatus:
        with self._lock:
            return self._status.model_copy()

    def start(self, request: ReplayStartRequest, telemetry: TelemetryService) -> ReplayStatus:
        if request.end_ms <= request.start_ms:
            raise ValueError("Replay end_ms must be after start_ms.")

        self.stop("Stopped previous replay before starting a new one.")
        topic_name = kafka_topic_for(request.car, self.settings, request.topic)
        transport_name = kafka_transport_for(self.settings, request.transport)
        stop_event = threading.Event()
        with self._lock:
            self._stop_event = stop_event
            self._status = ReplayStatus(
                running=True,
                source=request.car,
                topic=topic_name,
                transport=transport_name,
                start_ms=request.start_ms,
                end_ms=request.end_ms,
                current_ms=request.start_ms,
                samples_sent=0,
                message="Loading historical telemetry for replay.",
            )
            self._thread = threading.Thread(target=self._run, args=(request, telemetry, topic_name, transport_name, stop_event), daemon=True)
            self._thread.start()
            return self._status.model_copy()

    def stop(self, message: str = "Stopped.") -> ReplayStatus:
        with self._lock:
            thread = self._thread
            self._stop_event.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        with self._lock:
            self._thread = None
            self._status = self._status.model_copy(update={"running": False, "message": message})
            return self._status.model_copy()

    def _run(
        self,
        request: ReplayStartRequest,
        telemetry: TelemetryService,
        topic_name: str,
        transport_name: str,
        stop_event: threading.Event,
    ) -> None:
        try:
            frames = _build_replay_frames(telemetry, request)
            if not frames:
                self._update(running=False, message="No historical samples were available for that range.")
                return
            if stop_event.is_set():
                self._update(running=False, message="Replay stopped before publishing.")
                return

            if transport_name == "local":
                sent = self._publish_local_replay(request, frames, topic_name, stop_event)
                self._update(
                    running=False,
                    current_ms=frames[-1]["source_time_ms"] if frames else request.end_ms,
                    samples_sent=sent,
                    message="Local replay completed." if not stop_event.is_set() else "Local replay stopped.",
                )
                return

            try:
                from kafka import KafkaProducer
            except Exception as exc:
                self._update(running=False, message=f"Kafka client is not installed in this Python environment: {exc}")
                return

            servers = [item.strip() for item in self.settings.kafka_bootstrap_servers.split(",") if item.strip()]
            if not servers:
                self._update(running=False, message="KAFKA_BOOTSTRAP_SERVERS is empty.")
                return

            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda payload: json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                linger_ms=5,
                max_block_ms=5000,
                request_timeout_ms=5000,
            )
            try:
                sent = 0
                while not stop_event.is_set():
                    previous_source_ms: int | None = None
                    for frame in frames:
                        if stop_event.is_set():
                            break
                        source_ms = int(frame["source_time_ms"])
                        if previous_source_ms is not None:
                            delay_s = max(0.0, (source_ms - previous_source_ms) / 1000.0 / _clamp(request.speed_multiplier, 0.1, 100.0))
                            _interruptible_sleep(delay_s, stop_event)
                        if stop_event.is_set():
                            break
                        live_ms = int(time.time() * 1000)
                        payload = _frame_payload(frame, request.car, live_ms, sent)
                        producer.send(
                            topic_name,
                            key=request.car,
                            value=payload,
                            headers=[("car_type", request.car.encode("utf-8")), ("replay", b"true")],
                        )
                        sent += 1
                        if sent % 25 == 0:
                            producer.flush(timeout=1)
                            self._update(current_ms=source_ms, samples_sent=sent, message=f"Publishing replay to {topic_name}.")
                        previous_source_ms = source_ms
                    if not request.loop:
                        break
                producer.flush(timeout=2)
                self._update(
                    running=False,
                    current_ms=frames[-1]["source_time_ms"] if frames else request.end_ms,
                    samples_sent=sent,
                    message="Replay completed." if not stop_event.is_set() else "Replay stopped.",
                )
            finally:
                producer.close(timeout=2)
        except Exception as exc:
            self._update(running=False, message=f"Replay failed: {exc}")

    def _update(self, **patch: Any) -> None:
        with self._lock:
            self._status = self._status.model_copy(update=patch)

    def _publish_local_replay(
        self,
        request: ReplayStartRequest,
        frames: list[dict[str, Any]],
        topic_name: str,
        stop_event: threading.Event,
    ) -> int:
        sent = 0
        while not stop_event.is_set():
            previous_source_ms: int | None = None
            for frame in frames:
                if stop_event.is_set():
                    break
                source_ms = int(frame["source_time_ms"])
                if previous_source_ms is not None:
                    delay_s = max(0.0, (source_ms - previous_source_ms) / 1000.0 / _clamp(request.speed_multiplier, 0.1, 100.0))
                    _interruptible_sleep(delay_s, stop_event)
                if stop_event.is_set():
                    break
                live_ms = int(time.time() * 1000)
                payload = _frame_payload(frame, request.car, live_ms, sent)
                LOCAL_TOPIC_BUS.publish(topic_name, payload)
                sent += 1
                if sent % 25 == 0:
                    self._update(current_ms=source_ms, samples_sent=sent, message=f"Publishing local replay to {topic_name}.")
                previous_source_ms = source_ms
            if not request.loop:
                break
        return sent


def _build_replay_frames(telemetry: TelemetryService, request: ReplayStartRequest) -> list[dict[str, Any]]:
    duration_s = (request.end_ms - request.start_ms) / 1000.0
    frequency_hz = _clamp(request.frequency_hz, 1.0, 50.0)
    frame_count = min(MAX_REPLAY_FRAMES, max(2, int(duration_s * frequency_hz) + 1))
    channel_keys = _replay_channel_keys(telemetry, request.channel_keys)

    series_by_key: dict[str, tuple[list[int], list[SeriesPoint]]] = {}
    for key in channel_keys:
        try:
            points = telemetry.series(key, request.start_ms, request.end_ms, max_points=frame_count).points
        except KeyError:
            continue
        if points:
            series_by_key[key] = ([point.t for point in points], points)

    gps_points = telemetry.gps(request.start_ms, request.end_ms, max_points=frame_count).points
    gps_times = [point.t for point in gps_points]
    if not series_by_key and not gps_points:
        return []

    times = _frame_times(request.start_ms, request.end_ms, frame_count)
    frames: list[dict[str, Any]] = []
    previous_gps: tuple[int, float, float] | None = None
    for source_ms in times:
        values = {
            key: value
            for key, (times, points) in series_by_key.items()
            if (value := _series_value_at(times, points, source_ms)) is not None
        }
        lat_lon = _gps_at(gps_times, gps_points, source_ms)
        speed = _ground_speed(values)
        if speed is None and previous_gps and lat_lon:
            previous_ms, previous_lat, previous_lon = previous_gps
            dt_s = (source_ms - previous_ms) / 1000.0
            if dt_s > 0:
                speed = _distance_m(previous_lat, previous_lon, lat_lon[0], lat_lon[1]) / dt_s
        if lat_lon:
            previous_gps = (source_ms, lat_lon[0], lat_lon[1])
        frames.append(
            {
                "source_time_ms": source_ms,
                "values": values,
                "lat": lat_lon[0] if lat_lon else None,
                "lon": lat_lon[1] if lat_lon else None,
                "speed": speed,
            }
        )
    return frames


def _default_replay_channels(telemetry: TelemetryService) -> list[str]:
    channels = telemetry.channels()
    preferred_tokens = (
        "gps_speed",
        "wheel_speed",
        "motor_speed",
        "rpm",
        "torque",
        "apps",
        "brake",
        "hv_pack_v",
        "hv_c",
        "dc_bus",
        "temp",
    )
    selected: list[str] = []
    for channel in channels:
        lower = f"{channel.key} {channel.label} {channel.column}".lower()
        if "latitude" in lower or "longitude" in lower or channel.column.startswith("gps["):
            continue
        if channel.default and channel.key not in selected:
            selected.append(channel.key)
    for channel in channels:
        lower = f"{channel.key} {channel.label} {channel.column}".lower()
        if "latitude" in lower or "longitude" in lower or channel.column.startswith("gps["):
            continue
        if any(token in lower for token in preferred_tokens) and channel.key not in selected:
            selected.append(channel.key)
        if len(selected) >= MAX_REPLAY_CHANNELS:
            break
    if len(selected) < min(8, len(channels)):
        for channel in channels:
            if channel.key not in selected and not channel.column.startswith("gps["):
                selected.append(channel.key)
            if len(selected) >= MAX_REPLAY_CHANNELS:
                break
    return selected[:MAX_REPLAY_CHANNELS]


def _replay_channel_keys(telemetry: TelemetryService, requested: list[str]) -> list[str]:
    required = _required_replay_channels(telemetry)
    candidates = requested if requested else _default_replay_channels(telemetry)
    selected: list[str] = []
    for key in [*required, *candidates]:
        if key not in selected:
            selected.append(key)
        if len(selected) >= MAX_REPLAY_CHANNELS:
            break
    return selected


def _required_replay_channels(telemetry: TelemetryService) -> list[str]:
    channels = telemetry.channels()
    gps_keys: list[str] = []
    gps_speed_key: str | None = None
    fallback_speed_key: str | None = None
    dc_bus_v_key: str | None = None
    dc_bus_current_key: str | None = None
    for channel in channels:
        lower = f"{channel.key} {channel.label} {channel.column}".lower()
        if "latitude" in lower or channel.column == "gps[1]":
            gps_keys.append(channel.key)
        elif "longitude" in lower or channel.column == "gps[2]":
            gps_keys.append(channel.key)
        elif "gps_speed" in lower:
            gps_speed_key = gps_speed_key or channel.key
        elif "dc_bus" in lower and ("current" in lower or lower.endswith("_c")):
            dc_bus_current_key = dc_bus_current_key or channel.key
        elif "dc_bus" in lower and ("voltage" in lower or lower.endswith("_v")):
            dc_bus_v_key = dc_bus_v_key or channel.key
        elif "speed" in lower:
            fallback_speed_key = fallback_speed_key or channel.key
    required = [*gps_keys[:2], *([gps_speed_key or fallback_speed_key] if gps_speed_key or fallback_speed_key else [])]
    for key in [dc_bus_v_key, dc_bus_current_key]:
        if key and key not in required:
            required.append(key)
    return required


def _frame_times(start_ms: int, end_ms: int, frame_count: int) -> list[int]:
    if frame_count <= 2:
        return [start_ms, end_ms]
    span = end_ms - start_ms
    return [round(start_ms + (span * index) / (frame_count - 1)) for index in range(frame_count)]


def _series_value_at(times: list[int], points: list[SeriesPoint], t: int) -> float | None:
    if not points:
        return None
    index = bisect.bisect_left(times, t)
    if index <= 0:
        return _finite(points[0].v)
    if index >= len(points):
        return _finite(points[-1].v)
    before = points[index - 1]
    after = points[index]
    before_value = _finite(before.v)
    after_value = _finite(after.v)
    if before_value is None:
        return after_value
    if after_value is None:
        return before_value
    if after.t <= before.t:
        return before_value
    fraction = (t - before.t) / (after.t - before.t)
    return before_value + (after_value - before_value) * fraction


def _gps_at(times: list[int], points: list[GpsPoint], t: int) -> tuple[float, float] | None:
    if not points:
        return None
    index = bisect.bisect_left(times, t)
    if index <= 0:
        return points[0].lat, points[0].lon
    if index >= len(points):
        return points[-1].lat, points[-1].lon
    before = points[index - 1]
    after = points[index]
    if after.t <= before.t:
        return before.lat, before.lon
    fraction = (t - before.t) / (after.t - before.t)
    lat = before.lat + (after.lat - before.lat) * fraction
    lon = before.lon + (after.lon - before.lon) * fraction
    return lat, lon


def _frame_payload(frame: dict[str, Any], car: str, live_ms: int, replay_index: int) -> dict[str, Any]:
    values = dict(frame["values"])
    payload: dict[str, Any] = {
        "time": live_ms,
        "timestamp": live_ms,
        "source_time_ms": frame["source_time_ms"],
        "replay_time_ms": frame["source_time_ms"],
        "packet_id": replay_index,
        "replay_index": replay_index,
        "car_type": car,
        **values,
    }
    lat = frame.get("lat")
    lon = frame.get("lon")
    if lat is not None and lon is not None:
        payload.update({"latitude": lat, "longitude": lon, "lat": lat, "lon": lon, "gps": [lat, lon]})
    speed = frame.get("speed")
    if speed is not None:
        payload.update({"speed": speed, "gps_speed": speed})
    hv_pack_v = _first_value(values, ("hv_pack_v", "pack_hv_pack_v", "dc_bus_v", "pack_dc_bus_v", "inverter_dc_bus_v", "inverter_v"))
    hv_c = _first_value(values, ("hv_c", "pack_hv_c", "dc_bus_current", "pack_dc_bus_current", "inverter_dc_bus_current", "inverter_c"))
    if hv_pack_v is not None:
        payload["hv_pack_v"] = hv_pack_v
        payload.setdefault("dc_bus_v", hv_pack_v)
    if hv_c is not None:
        payload["hv_c"] = hv_c
        payload.setdefault("dc_bus_current", hv_c)
    if hv_pack_v is not None and hv_c is not None:
        payload["power_kw"] = hv_pack_v * hv_c / 1000.0
    return payload


def _ground_speed(values: dict[str, float]) -> float | None:
    for key, value in values.items():
        lower = key.lower()
        if "gps_speed" in lower:
            return value
    wheel_values = [
        value
        for key, value in values.items()
        if "wheel_speed" in key.lower() or key.lower() in {"flw_speed", "frw_speed", "blw_speed", "brw_speed"}
    ]
    if wheel_values:
        return sum(wheel_values) / len(wheel_values)
    return None


def _first_value(values: dict[str, float], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key in values:
            return values[key]
    for key, value in values.items():
        if any(key.endswith(candidate) for candidate in keys):
            return value
    return None


def _finite(value: float | None) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _interruptible_sleep(delay_s: float, stop_event: threading.Event) -> None:
    end = time.monotonic() + delay_s
    while not stop_event.is_set():
        remaining = end - time.monotonic()
        if remaining <= 0:
            return
        stop_event.wait(min(remaining, 0.25))


def _distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
