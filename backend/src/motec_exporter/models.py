from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


CarId = Literal["orion", "angelique"]
KafkaTransport = Literal["local", "kafka", "mqtt"]


class ChannelDef(BaseModel):
    key: str
    label: str
    table: str
    column: str
    unit: str = ""
    quantity: str = ""
    default: bool = False
    split_candidate: bool = False


class SourceDef(BaseModel):
    key: CarId
    label: str


class DriveDay(BaseModel):
    date: str
    sessions: int
    start_ms: int
    end_ms: int
    label: str


class SessionSummary(BaseModel):
    id: str
    label: str
    start_ms: int
    end_ms: int
    duration_s: float
    source: str
    preview_safe: bool = True
    warning: str | None = None


class SegmentSummary(BaseModel):
    id: str
    label: str
    start_ms: int
    end_ms: int
    duration_s: float
    source_channel: str
    has_gps: bool = False
    gps_points: int = 0


class DayDetail(BaseModel):
    date: str
    sessions: list[SessionSummary]
    segments: list[SegmentSummary]


class SeriesPoint(BaseModel):
    t: int
    v: float | None


class SeriesResponse(BaseModel):
    channel: str
    label: str
    unit: str
    points: list[SeriesPoint]


class GpsPoint(BaseModel):
    t: int
    lat: float
    lon: float


class GpsResponse(BaseModel):
    points: list[GpsPoint]


class GateLine(BaseModel):
    id: str
    label: str
    lat1: float
    lon1: float
    lat2: float
    lon2: float
    role: Literal["start_finish", "split"] = "split"


class TrackDefinition(BaseModel):
    name: str
    slug: str
    notes: str = ""
    gates: list[GateLine] = Field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


class ChannelChartEntry(BaseModel):
    channel_name: str
    quantity_type: str = ""
    unit: str = ""
    notes: str = ""


class ChannelChartDefinition(BaseModel):
    name: str
    slug: str
    notes: str = ""
    entries: list[ChannelChartEntry] = Field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


class ExportSegment(BaseModel):
    id: str
    start_ms: int
    end_ms: int
    label: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)


class ExportRequest(BaseModel):
    car: CarId = "orion"
    channel_keys: list[str] = Field(default_factory=list)
    segments: list[ExportSegment]
    export_type: Literal["motec", "csv"] = "motec"
    frequency_hz: float | None = None
    track_slug: str | None = None
    channel_chart_slug: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class ExportResponse(BaseModel):
    export_id: str
    zip_path: str
    files: list[str]


class LiveSample(BaseModel):
    t: int
    values: dict[str, float] = Field(default_factory=dict)


class LiveLapExportRequest(BaseModel):
    car: CarId = "orion"
    lap_label: str = "Live Lap"
    track_slug: str | None = None
    channel_chart_slug: str | None = None
    frequency_hz: float | None = 50
    samples: list[LiveSample]
    metadata: dict[str, str] = Field(default_factory=dict)


class ReplayStartRequest(BaseModel):
    car: CarId = "orion"
    start_ms: int
    end_ms: int
    topic: str | None = None
    transport: KafkaTransport | None = None
    channel_keys: list[str] = Field(default_factory=list)
    speed_multiplier: float = 1.0
    frequency_hz: float = 10.0
    loop: bool = False


class ReplayStatus(BaseModel):
    running: bool = False
    source: CarId = "orion"
    topic: str = ""
    transport: KafkaTransport = "local"
    start_ms: int | None = None
    end_ms: int | None = None
    current_ms: int | None = None
    samples_sent: int = 0
    message: str = "Idle"
