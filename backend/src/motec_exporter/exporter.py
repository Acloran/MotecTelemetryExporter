from __future__ import annotations

import csv
import datetime as dt
import math
import re
import zipfile
from pathlib import Path

from .channel_chart_store import ChannelChartStore, apply_channel_chart
from .config import Settings
from .datalog import DataLog
from .models import ExportRequest, ExportResponse
from .motec_ld import write_ld, write_ldx
from .split import gate_crossing_times
from .telemetry import TelemetryService
from .track_store import TrackStore


SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class Exporter:
    def __init__(self, settings: Settings, telemetry: TelemetryService, tracks: TrackStore, channel_charts: ChannelChartStore | None = None):
        self.settings = settings
        self.telemetry = telemetry
        self.tracks = tracks
        self.channel_charts = channel_charts
        self.settings.resolved_export_dir.mkdir(parents=True, exist_ok=True)

    def export(self, request: ExportRequest) -> ExportResponse:
        if not request.segments:
            raise ValueError("No export segments were selected.")
        export_id = self._export_id(request)
        out_dir = self.settings.resolved_export_dir / export_id
        out_dir.mkdir(parents=True, exist_ok=True)

        track = self.tracks.load(request.track_slug) if request.export_type == "motec" and request.track_slug else None
        channel_chart = self.channel_charts.load(request.channel_chart_slug) if self.channel_charts and request.channel_chart_slug else None
        vehicle_name = request.car.title()
        files: list[Path] = []
        for index, segment in enumerate(request.segments, start=1):
            if segment.end_ms <= segment.start_ms:
                raise ValueError(f"Segment {segment.id} has an invalid time range.")
            if segment.end_ms - segment.start_ms > self.settings.max_export_seconds * 1000:
                raise ValueError("Export range is too large. Narrow the range before exporting.")

            log = self.telemetry.datalog(request.channel_keys, segment.start_ms, segment.end_ms)
            apply_channel_chart(log, channel_chart)
            log.resample(request.frequency_hz)
            stamp = dt.datetime.fromtimestamp(segment.start_ms / 1000, tz=dt.UTC).strftime("%Y_%m_%d__%H_%M_%S")
            stem = _safe_name(f"{vehicle_name}__{stamp}__part{index}")
            segment_metadata = {**request.metadata, **segment.metadata}
            metadata = {
                "vehicle_id": vehicle_name,
                "event": segment_metadata.get("event", "Telemetry Export"),
                "session": segment_metadata.get("session") or segment.label,
                "short_comment": segment_metadata.get("short_comment") or segment.id,
                "long_comment": segment_metadata.get("long_comment", ""),
                "datetime": dt.datetime.fromtimestamp(segment.start_ms / 1000, tz=dt.UTC).isoformat(),
                **segment_metadata,
            }
            metadata["session"] = segment_metadata.get("session") or segment.label
            metadata["short_comment"] = segment_metadata.get("short_comment") or segment.id
            if request.export_type == "csv":
                csv_path = out_dir / f"{stem}.csv"
                write_csv(csv_path, log, segment.start_ms)
                files.append(csv_path)
                continue

            ld_path = out_dir / f"{stem}.ld"
            write_ld(ld_path, log, metadata=metadata)
            files.append(ld_path)
            ldx_path = ld_path.with_suffix(".ldx")
            primary: list[float] = []
            splits: list[float] = []
            if track:
                gps = self.telemetry.gps(segment.start_ms, segment.end_ms, max_points=10000).points
                crossings = gate_crossing_times(gps, track.gates)
                for gate in track.gates:
                    relative = [(t - segment.start_ms) / 1000.0 for t in crossings.get(gate.id, [])]
                    if gate.role == "start_finish":
                        primary.extend(relative)
                    else:
                        splits.extend(relative)
            write_ldx(ldx_path, primary, splits)
            files.append(ldx_path)

        zip_path = self.settings.resolved_export_dir / f"{export_id}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                archive.write(path, arcname=path.name)

        return ExportResponse(
            export_id=export_id,
            zip_path=str(zip_path),
            files=[path.name for path in files],
        )

    def _export_id(self, request: ExportRequest) -> str:
        first_start = min(segment.start_ms for segment in request.segments)
        export_date = dt.datetime.fromtimestamp(first_start / 1000, tz=dt.UTC).strftime("%Y_%m_%d")
        vehicle = _safe_name(request.metadata.get("vehicle_id") or request.car.title())
        base = _safe_name(f"{vehicle}__{export_date}")
        candidate = base
        index = 2
        while (self.settings.resolved_export_dir / candidate).exists() or (self.settings.resolved_export_dir / f"{candidate}.zip").exists():
            candidate = f"{base}_{index}"
            index += 1
        return candidate


def _safe_name(value: str) -> str:
    return SAFE_NAME_RE.sub("_", value).strip("_") or "export"


def write_csv(path: Path, data_log: DataLog, start_ms: int) -> None:
    channels = [(key, channel) for key, channel in data_log.channels.items() if channel.samples]
    headers = ["timestamp_ms", "elapsed_s"] + _csv_channel_headers(channels)
    max_samples = max((len(channel.samples) for _key, channel in channels), default=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for index in range(max_samples):
            elapsed_s = _sample_time(channels, index)
            row: list[str | int | float] = [int(round(start_ms + elapsed_s * 1000)), _csv_float(elapsed_s)]
            for _key, channel in channels:
                if index < len(channel.samples):
                    row.append(_csv_float(channel.samples[index].value))
                else:
                    row.append("")
            writer.writerow(row)


def _csv_channel_headers(channels: list[tuple[str, object]]) -> list[str]:
    seen: dict[str, int] = {}
    headers: list[str] = []
    for key, channel in channels:
        name = getattr(channel, "name", key) or key
        unit = getattr(channel, "unit", "")
        base = f"{name} ({unit})" if unit else str(name)
        count = seen.get(base, 0) + 1
        seen[base] = count
        headers.append(base if count == 1 else f"{base} {count}")
    return headers


def _sample_time(channels: list[tuple[str, object]], index: int) -> float:
    for _key, channel in channels:
        samples = getattr(channel, "samples", [])
        if index < len(samples):
            return float(samples[index].t)
    return 0.0


def _csv_float(value: float) -> float | str:
    if not math.isfinite(float(value)):
        return ""
    return round(float(value), 9)
