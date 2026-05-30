from __future__ import annotations

import datetime as dt
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from . import sample_data
from .channels import DEFAULT_CHANNEL_KEY, ORION_CHANNELS
from .config import Settings
from .datalog import Channel, DataLog, Sample
from .db import ReadOnlyDatabase
from .models import ChannelDef, DayDetail, DriveDay, GpsPoint, GpsResponse, SegmentSummary, SeriesPoint, SeriesResponse, SessionSummary
from .split import detect_split_ranges


class TelemetryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = ReadOnlyDatabase(settings)
        self._channels: list[ChannelDef] | None = None

    def channels(self) -> list[ChannelDef]:
        if not self.settings.use_postgres:
            return ORION_CHANNELS
        if self._channels is None:
            self._channels = self._load_channels()
        return self._channels

    def get_channel(self, key: str | None) -> ChannelDef:
        channels = self.channels()
        lookup = {channel.key: channel for channel in channels}
        try:
            return lookup[key or DEFAULT_CHANNEL_KEY]
        except KeyError as exc:
            raise KeyError(f"Unknown telemetry channel: {key}") from exc

    def calendar(
        self,
        channel_key: str | None = None,
        *,
        threshold: float = 0.0,
        min_duration_s: float = 0.0,
        valid_only: bool = False,
    ) -> list[DriveDay]:
        if not self.settings.use_postgres:
            return sample_data.list_days()
        rows = self.db.query(
            """
            select
              start_time::bigint as start_ms,
              end_time::bigint as end_ms
            from partitions
            where end_time > start_time
            order by end_time desc
            limit 2000
            """
        )
        ranges_by_date: dict[str, list[tuple[int, int]]] = {}
        for row in rows:
            start_ms = int(row["start_ms"])
            end_ms = int(row["end_ms"])
            for date in self._dates_between_local(start_ms, end_ms):
                day_start, day_end = self._local_day_bounds_ms(date)
                clamped_start = max(start_ms, day_start)
                clamped_end = min(end_ms, day_end)
                if clamped_end > clamped_start:
                    ranges_by_date.setdefault(date, []).append((clamped_start, clamped_end))
        days: list[DriveDay] = []
        session_counts_by_date: dict[str, int] | None = None
        if valid_only and channel_key and ranges_by_date:
            all_ranges = [range_item for ranges in ranges_by_date.values() for range_item in ranges]
            session_counts_by_date = self._threshold_session_counts_by_date(
                channel_key,
                min(start for start, _end in all_ranges),
                max(end for _start, end in all_ranges),
                threshold,
                min_duration_s,
            )
        for date in sorted(ranges_by_date, reverse=True):
            ranges = _merge_ms_ranges(ranges_by_date[date])
            if not ranges:
                continue
            session_count: int | None = None
            if valid_only and channel_key:
                session_count = (session_counts_by_date or {}).get(date, 0)
                if session_count == 0:
                    continue
            days.append(
                DriveDay(
                    date=date,
                    sessions=session_count if session_count is not None else len(ranges),
                    start_ms=min(start for start, _end in ranges),
                    end_ms=max(end for _start, end in ranges),
                    label=_day_label(date, session_count if session_count is not None else len(ranges), session_count is not None),
                )
            )
        return days[:180]

    def day_detail(self, date: str, channel_key: str = DEFAULT_CHANNEL_KEY) -> DayDetail:
        sessions = self.sessions_for_day(date)
        segments: list[SegmentSummary] = []
        if self.settings.use_postgres:
            return DayDetail(date=date, sessions=sessions, segments=segments)

        for session in sessions:
            if session.preview_safe and session.duration_s <= self.settings.max_auto_split_seconds:
                segments.extend(self.auto_segments(session.start_ms, session.end_ms, channel_key))
        return DayDetail(date=date, sessions=sessions, segments=segments)

    def sessions_for_day(self, date: str) -> list[SessionSummary]:
        if not self.settings.use_postgres:
            return sample_data.list_sessions(date)
        day_start, day_end = self._local_day_bounds_ms(date)
        rows = self.db.query(
            """
            select
              partition_name,
              start_time::bigint as start_ms,
              end_time::bigint as end_ms
            from partitions
            where start_time < %(day_end)s
              and end_time > %(day_start)s
              and end_time > start_time
            order by start_time asc
            """,
            {"day_start": day_start, "day_end": day_end},
        )
        ranges = _merge_ms_ranges(
            [
                (max(int(row["start_ms"]), day_start), min(int(row["end_ms"]), day_end))
                for row in rows
            ]
        )
        sessions: list[SessionSummary] = []
        for index, (start_ms, end_ms) in enumerate(ranges, start=1):
            if end_ms <= start_ms:
                continue
            duration_s = (end_ms - start_ms) / 1000.0
            preview_safe = 0 < duration_s <= self.settings.max_preview_seconds
            warning = None
            if duration_s <= 0:
                warning = "Zero-duration source range"
            elif not preview_safe:
                warning = "Long source range; threshold sessions are generated from this local-day slice"
            sessions.append(
                SessionSummary(
                    id=f"{date}-{index}-{start_ms}-{end_ms}",
                    label=f"Source range {index}",
                    start_ms=start_ms,
                    end_ms=end_ms,
                    duration_s=duration_s,
                    source="partition_day_window",
                    preview_safe=preview_safe,
                    warning=warning,
                )
            )
        return sessions

    def threshold_segments(
        self,
        start_ms: int,
        end_ms: int,
        channel_key: str = DEFAULT_CHANNEL_KEY,
        threshold: float = 0.0,
        max_points: int = 20000,
        min_duration_s: float = 0.0,
    ) -> list[SegmentSummary]:
        if not self.settings.use_postgres:
            return [
                segment
                for segment in sample_data.auto_segments(start_ms, end_ms, channel_key)
                if segment.duration_s >= min_duration_s
            ]
        if end_ms <= start_ms:
            return []
        channel = self.get_channel(channel_key)
        points = self._postgres_series(channel, start_ms, end_ms, max_points=max_points, enforce_preview=False)
        segments: list[SegmentSummary] = []
        active_start: int | None = None
        last_t: int | None = None
        for point in points:
            is_active = point.v is not None and point.v > threshold
            if is_active and active_start is None:
                active_start = max(start_ms, point.t)
            if not is_active and active_start is not None:
                segment_end = min(end_ms, last_t or point.t)
                if segment_end > active_start and (segment_end - active_start) / 1000.0 >= min_duration_s:
                    segments.append(self._segment(channel.key, len(segments) + 1, active_start, segment_end))
                active_start = None
            last_t = point.t
        if active_start is not None:
            segment_end = min(end_ms, last_t or end_ms)
            if segment_end > active_start and (segment_end - active_start) / 1000.0 >= min_duration_s:
                segments.append(self._segment(channel.key, len(segments) + 1, active_start, segment_end))
        return self._with_gps_coverage(segments)

    def series(self, channel_key: str, start_ms: int, end_ms: int, max_points: int | None = None) -> SeriesResponse:
        channel = self.get_channel(channel_key)
        max_points = max_points or self.settings.max_preview_points
        if not self.settings.use_postgres:
            points = sample_data.series(channel.key, start_ms, end_ms, max_points=max_points)
        else:
            points = self._postgres_series(channel, start_ms, end_ms, max_points, enforce_preview=False)
        return SeriesResponse(channel=channel.key, label=channel.label, unit=channel.unit, points=points)

    def gps(self, start_ms: int, end_ms: int, max_points: int = 2000) -> GpsResponse:
        if not self.settings.use_postgres:
            return GpsResponse(points=sample_data.gps(start_ms, end_ms, max_points=max_points))
        span = max(1, end_ms - start_ms)
        step_ms = max(1, int(span / max(1, max_points)))
        rows = self.db.query(
            """
            select
              (floor(packet.time::double precision / %(step_ms)s) * %(step_ms)s)::bigint as t,
              avg(dynamics.gps[1]) as lat,
              avg(dynamics.gps[2]) as lon
            from packet
            join dynamics on dynamics.packet_id = packet.packet_id
            where packet.time between %(start_ms)s and %(end_ms)s
              and dynamics.gps is not null
              and array_length(dynamics.gps, 1) >= 2
            group by 1
            order by 1
            """,
            {"start_ms": start_ms, "end_ms": end_ms, "step_ms": step_ms},
        )
        return GpsResponse(
            points=[
                GpsPoint(t=int(row["t"]), lat=float(row["lat"]), lon=float(row["lon"]))
                for row in rows
                if row["lat"] is not None and row["lon"] is not None
            ]
        )

    def auto_segments(self, start_ms: int, end_ms: int, channel_key: str = DEFAULT_CHANNEL_KEY) -> list[SegmentSummary]:
        if not self.settings.use_postgres:
            return sample_data.auto_segments(start_ms, end_ms, channel_key)
        series = self.series(channel_key, start_ms, end_ms, max_points=20000)
        channel = self.get_channel(channel_key)
        split_channel = Channel(
            name=channel.label,
            unit=channel.unit,
            quantity=channel.quantity,
            samples=[Sample((p.t - start_ms) / 1000.0, p.v or 0.0) for p in series.points],
        )
        ranges = detect_split_ranges(split_channel)
        segments: list[SegmentSummary] = []
        for index, (a, b) in enumerate(ranges, start=1):
            segment_start = max(start_ms, start_ms + int(a * 1000))
            segment_end = min(end_ms, start_ms + int(b * 1000))
            if segment_end <= segment_start:
                continue
            segments.append(
                SegmentSummary(
                    id=f"auto-{start_ms}-{index}",
                    label=f"Auto split {index}",
                    start_ms=segment_start,
                    end_ms=segment_end,
                    duration_s=(segment_end - segment_start) / 1000.0,
                    source_channel=channel_key,
                )
            )
        return self._with_gps_coverage(segments)

    def datalog(self, channel_keys: list[str], start_ms: int, end_ms: int) -> DataLog:
        selected = channel_keys or [channel.key for channel in ORION_CHANNELS]
        if not self.settings.use_postgres:
            return sample_data.datalog(selected, start_ms, end_ms)
        channel_by_key = {channel.key: channel for channel in self.channels()}
        definitions = [channel_by_key[key] for key in selected if key in channel_by_key]
        tables = sorted({d.table for d in definitions})
        select_exprs = [f"{definition.table}.{definition.column} as {definition.key}" for definition in definitions]
        joins = " ".join(f"left join {table} on {table}.packet_id = packet.packet_id" for table in tables)
        rows = self.db.query(
            f"""
            select packet.time::bigint as t, {", ".join(select_exprs)}
            from packet
            {joins}
            where packet.time between %(start_ms)s and %(end_ms)s
            order by packet.time asc
            """,
            {"start_ms": start_ms, "end_ms": end_ms},
        )
        log = DataLog(
            f"{self.settings.orion_db_name}_postgres",
            metadata={
                "source": "postgres",
                "start_ms": str(start_ms),
                "end_ms": str(end_ms),
            },
        )
        for definition in definitions:
            samples = [
                Sample((int(row["t"]) - start_ms) / 1000.0, float(row[definition.key]))
                for row in rows
                if row.get(definition.key) is not None
            ]
            if samples:
                log.channels[definition.key] = Channel(definition.label, definition.unit, definition.quantity, samples)
        return log

    def _postgres_series(
        self,
        channel: ChannelDef,
        start_ms: int,
        end_ms: int,
        max_points: int,
        *,
        enforce_preview: bool = True,
    ) -> list[SeriesPoint]:
        if enforce_preview:
            self._assert_preview_span(start_ms, end_ms)
        span = max(1, end_ms - start_ms)
        step_ms = max(1, int(span / max(1, max_points)))
        rows = self.db.query(
            f"""
            select
              (floor(packet.time::double precision / %(step_ms)s) * %(step_ms)s)::bigint as t,
              avg({channel.table}.{channel.column}) as v
            from packet
            join {channel.table} on {channel.table}.packet_id = packet.packet_id
            where packet.time between %(start_ms)s and %(end_ms)s
            group by 1
            order by 1
            """,
            {"start_ms": start_ms, "end_ms": end_ms, "step_ms": step_ms},
        )
        return [SeriesPoint(t=int(row["t"]), v=float(row["v"]) if row["v"] is not None else None) for row in rows]

    def _load_channels(self) -> list[ChannelDef]:
        rows = self.db.query(
            """
            select table_name, column_name, data_type
            from information_schema.columns
            where table_schema='public'
              and table_name in ('controls','dynamics','pack','thermal','diagnostics','diagnostics_high','diagnostics_low')
              and column_name <> 'packet_id'
            order by table_name, ordinal_position
            """
        )
        channels: list[ChannelDef] = []
        for row in rows:
            table = str(row["table_name"])
            column = str(row["column_name"])
            data_type = str(row["data_type"])
            if data_type in {"real", "double precision", "integer", "bigint", "smallint", "numeric"}:
                key = _channel_key(table, column)
                channels.append(
                    ChannelDef(
                        key=key,
                        label=_label(column),
                        table=table,
                        column=column,
                        unit=_unit(column),
                        quantity=_quantity(column),
                        default=key in {"controls_motor_speed", "dynamics_inverter_rpm"},
                        split_candidate=_is_split_candidate(column),
                    )
                )
            elif data_type == "ARRAY" and column == "gps":
                channels.extend(
                    [
                        ChannelDef(key="dynamics_gps_latitude", label="GPS Latitude", table=table, column="gps[1]", unit="deg", quantity="position"),
                        ChannelDef(key="dynamics_gps_longitude", label="GPS Longitude", table=table, column="gps[2]", unit="deg", quantity="position"),
                    ]
                )
        if not any(channel.default for channel in channels) and channels:
            channels[0].default = True
        return channels or ORION_CHANNELS

    def _segment(self, channel_key: str, index: int, start_ms: int, end_ms: int) -> SegmentSummary:
        return SegmentSummary(
            id=f"auto-{channel_key}-{start_ms}-{index}",
            label=f"Session {index}",
            start_ms=start_ms,
            end_ms=end_ms,
            duration_s=(end_ms - start_ms) / 1000.0,
            source_channel=channel_key,
        )

    def _with_gps_coverage(self, segments: list[SegmentSummary]) -> list[SegmentSummary]:
        if not segments:
            return segments
        if not self.settings.use_postgres:
            return [segment.model_copy(update={"has_gps": True, "gps_points": max(1, segment.gps_points)}) for segment in segments]
        values: list[str] = []
        params: dict[str, int] = {}
        for index, segment in enumerate(segments):
            values.append(f"(%(idx_{index})s, %(start_{index})s, %(end_{index})s)")
            params[f"idx_{index}"] = index
            params[f"start_{index}"] = int(segment.start_ms)
            params[f"end_{index}"] = int(segment.end_ms)
        rows = self.db.query(
            f"""
            with ranges(idx, start_ms, end_ms) as (
              values {", ".join(values)}
            )
            select
              ranges.idx,
              count(dynamics.packet_id)::bigint as gps_points
            from ranges
            left join packet on packet.time between ranges.start_ms and ranges.end_ms
            left join dynamics on dynamics.packet_id = packet.packet_id
              and dynamics.gps is not null
              and array_length(dynamics.gps, 1) >= 2
              and dynamics.gps[1] is not null
              and dynamics.gps[2] is not null
            group by ranges.idx
            """,
            params,
        )
        counts = {int(row["idx"]): int(row["gps_points"]) for row in rows}
        return [
            segment.model_copy(update={"has_gps": counts.get(index, 0) > 0, "gps_points": counts.get(index, 0)})
            for index, segment in enumerate(segments)
        ]

    def _assert_preview_span(self, start_ms: int, end_ms: int) -> None:
        if end_ms <= start_ms:
            return
        if end_ms - start_ms > self.settings.max_preview_seconds * 1000:
            raise ValueError("Preview range is too large. Select a smaller range before plotting.")

    def _display_tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.settings.display_timezone)
        except ZoneInfoNotFoundError:
            return ZoneInfo("UTC")

    def _dates_between_local(self, start_ms: int, end_ms: int) -> list[str]:
        tz = self._display_tz()
        start_date = dt.datetime.fromtimestamp(start_ms / 1000.0, tz=dt.UTC).astimezone(tz).date()
        inclusive_end_ms = max(start_ms, end_ms - 1)
        end_date = dt.datetime.fromtimestamp(inclusive_end_ms / 1000.0, tz=dt.UTC).astimezone(tz).date()
        days = (end_date - start_date).days
        return [(start_date + dt.timedelta(days=offset)).isoformat() for offset in range(days + 1)]

    def _local_day_bounds_ms(self, date: str) -> tuple[int, int]:
        tz = self._display_tz()
        local_start = dt.datetime.fromisoformat(date).replace(tzinfo=tz)
        local_end = local_start + dt.timedelta(days=1)
        return (
            int(local_start.astimezone(dt.UTC).timestamp() * 1000),
            int(local_end.astimezone(dt.UTC).timestamp() * 1000),
        )

    def _threshold_session_counts_by_date(
        self,
        channel_key: str,
        start_ms: int,
        end_ms: int,
        threshold: float,
        min_duration_s: float,
    ) -> dict[str, int]:
        channel = self.get_channel(channel_key)
        step_ms = max(1, int((24 * 60 * 60 * 1000) / 20000))
        rows = self.db.query(
            f"""
            select
              to_char(to_timestamp(packet.time / 1000.0) at time zone %(timezone)s, 'YYYY-MM-DD') as date,
              (floor(packet.time::double precision / %(step_ms)s) * %(step_ms)s)::bigint as t
            from packet
            join {channel.table} on {channel.table}.packet_id = packet.packet_id
            where packet.time between %(start_ms)s and %(end_ms)s
            group by 1, 2
            having avg({channel.table}.{channel.column}) > %(threshold)s
            order by 1, 2
            """,
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "threshold": threshold,
                "timezone": self.settings.display_timezone,
                "step_ms": step_ms,
            },
        )
        counts: dict[str, int] = {}
        active_start_by_date: dict[str, int] = {}
        last_t_by_date: dict[str, int] = {}
        gap_ms = step_ms * 1.5
        for row in rows:
            date = str(row["date"])
            t = int(row["t"])
            active_start = active_start_by_date.get(date)
            last_t = last_t_by_date.get(date)
            if active_start is None or last_t is None:
                active_start_by_date[date] = t
                last_t_by_date[date] = t
                continue
            if t - last_t > gap_ms:
                if (last_t - active_start) / 1000.0 >= min_duration_s:
                    counts[date] = counts.get(date, 0) + 1
                active_start_by_date[date] = t
            last_t_by_date[date] = t
        for date, active_start in active_start_by_date.items():
            last_t = last_t_by_date[date]
            if (last_t - active_start) / 1000.0 >= min_duration_s:
                counts[date] = counts.get(date, 0) + 1
        return counts


def iso_from_ms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.UTC).isoformat()


def _merge_ms_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    clean = sorted((int(start), int(end)) for start, end in ranges if end > start)
    merged: list[tuple[int, int]] = []
    for start, end in clean:
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _day_label(date: str, count: int, sessions: bool) -> str:
    noun = "session" if sessions else "source range"
    return f"{date} ({count} {noun}{'' if count == 1 else 's'})"


def _channel_key(table: str, column: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", f"{table}__{column}".lower()).strip("_")


def _label(column: str) -> str:
    return column.replace("_", " ").upper() if column in {"rpm_request"} else column.replace("_", " ").title()


def _unit(column: str) -> str:
    lower = column.lower()
    if "rpm" in lower or lower == "motor_speed":
        return "rpm"
    if lower.endswith("_speed") and "wheel" in lower:
        return "rad/s"
    if lower.endswith("_v") or "voltage" in lower:
        return "V"
    if lower.endswith("_c") or "current" in lower:
        return "A"
    if "temp" in lower:
        return "C"
    if "pressure" in lower:
        return "psi"
    if "torque" in lower:
        return "Nm"
    if "gps" in lower and "speed" in lower:
        return "m/s"
    return ""


def _quantity(column: str) -> str:
    lower = column.lower()
    if "speed" in lower or "rpm" in lower:
        return "speed"
    if "temp" in lower:
        return "temperature"
    if "pressure" in lower:
        return "pressure"
    if "torque" in lower:
        return "torque"
    if lower.endswith("_v") or "voltage" in lower:
        return "voltage"
    if lower.endswith("_c") or "current" in lower:
        return "current"
    return "value"


def _is_split_candidate(column: str) -> bool:
    lower = column.lower()
    return any(token in lower for token in ("rpm", "speed", "torque", "apps", "gps_velocity"))
