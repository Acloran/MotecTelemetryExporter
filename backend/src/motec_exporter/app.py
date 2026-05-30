from __future__ import annotations

import json

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse

from .channel_chart_store import ChannelChartStore
from .channels import DEFAULT_CHANNEL_KEY, ORION_CHANNELS
from .config import settings
from .exporter import Exporter
from .live import export_live_lap, kafka_topic_for, kafka_transport_for, mqtt_topic_for, stream_kafka_samples
from .models import ChannelChartDefinition, ExportRequest, LiveLapExportRequest, ReplayStartRequest, SourceDef, TrackDefinition
from .replay import ReplayManager
from .telemetry import TelemetryService
from .track_store import TrackStore


app = FastAPI(title="MoTeC Telemetry Exporter", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://127.0.0.1:5174", "http://localhost:5175", "http://127.0.0.1:5175"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

tracks = TrackStore(settings.resolved_track_dir)
channel_charts = ChannelChartStore(settings.resolved_channel_chart_dir)
replay_manager = ReplayManager(settings)


def telemetry_for(source: str | None = None) -> TelemetryService:
    return TelemetryService(settings.for_source(source))


@app.get("/api/health")
def health() -> dict[str, str | bool]:
    return {
        "ok": True,
        "source": settings.telemetry_source,
        "postgres_enabled": settings.use_postgres,
        "timezone": settings.display_timezone,
    }


@app.get("/api/sources")
def sources():
    return {"sources": [SourceDef(key="orion", label="Orion").model_dump(), SourceDef(key="angelique", label="Angelique").model_dump()]}


@app.get("/api/channels")
def channels(source: str = "orion"):
    telemetry = telemetry_for(source)
    try:
        items = telemetry.channels()
    except psycopg.Error:
        items = ORION_CHANNELS
    default = next((channel.key for channel in items if channel.default), items[0].key if items else DEFAULT_CHANNEL_KEY)
    return {"channels": [channel.model_dump() for channel in items], "default": default}


@app.get("/api/calendar")
def calendar(
    source: str = "orion",
    channel: str | None = None,
    threshold: float = 0.0,
    min_duration_s: float = Query(0.0, alias="minDurationS"),
    valid_only: bool = Query(False, alias="validOnly"),
):
    try:
        return {
            "days": [
                day.model_dump()
                for day in telemetry_for(source).calendar(
                    channel,
                    threshold=threshold,
                    min_duration_s=min_duration_s,
                    valid_only=valid_only,
                )
            ]
        }
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Telemetry database is unavailable.") from exc


@app.get("/api/day/{date}")
def day_detail(date: str, channel: str = DEFAULT_CHANNEL_KEY, source: str = "orion"):
    try:
        return telemetry_for(source).day_detail(date, channel).model_dump()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Telemetry database is unavailable.") from exc


@app.get("/api/series")
def series(
    channel: str = DEFAULT_CHANNEL_KEY,
    start_ms: int = Query(..., alias="startMs"),
    end_ms: int = Query(..., alias="endMs"),
    max_points: int = Query(5000, alias="maxPoints"),
    source: str = "orion",
):
    try:
        return telemetry_for(source).series(channel, start_ms, end_ms, max_points=max_points).model_dump()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Telemetry database is unavailable.") from exc


@app.get("/api/gps")
def gps(
    start_ms: int = Query(..., alias="startMs"),
    end_ms: int = Query(..., alias="endMs"),
    max_points: int = Query(2000, alias="maxPoints"),
    source: str = "orion",
):
    try:
        return telemetry_for(source).gps(start_ms, end_ms, max_points=max_points).model_dump()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Telemetry database is unavailable.") from exc


@app.get("/api/segments")
def segments(
    channel: str = DEFAULT_CHANNEL_KEY,
    start_ms: int = Query(..., alias="startMs"),
    end_ms: int = Query(..., alias="endMs"),
    threshold: float = 0.0,
    min_duration_s: float = Query(0.0, alias="minDurationS"),
    source: str = "orion",
):
    try:
        return {
            "segments": [
                segment.model_dump()
                for segment in telemetry_for(source).threshold_segments(
                    start_ms,
                    end_ms,
                    channel,
                    threshold,
                    min_duration_s=min_duration_s,
                )
            ]
        }
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Telemetry database is unavailable.") from exc


@app.get("/api/tracks")
def list_tracks():
    return {"tracks": [track.model_dump() for track in tracks.list_tracks()]}


@app.get("/api/tracks/{slug}")
def get_track(slug: str):
    try:
        return tracks.load(slug).model_dump()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Track not found") from exc


@app.post("/api/tracks")
def save_track(track: TrackDefinition):
    return tracks.save(track).model_dump()


@app.get("/api/channel-charts")
def list_channel_charts():
    return {"charts": [chart.model_dump() for chart in channel_charts.list_charts()]}


@app.get("/api/channel-charts/{slug}")
def get_channel_chart(slug: str):
    try:
        return channel_charts.load(slug).model_dump()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Channel chart not found") from exc


@app.post("/api/channel-charts")
def save_channel_chart(chart: ChannelChartDefinition):
    return channel_charts.save(chart).model_dump()


@app.post("/api/export")
def export(request: ExportRequest):
    try:
        telemetry = telemetry_for(request.car)
        exporter = Exporter(settings.for_source(request.car), telemetry, tracks, channel_charts)
        return exporter.export(request).model_dump()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/live/config")
def live_config(source: str = "orion", topic: str | None = None, transport: str | None = None):
    transport_name = kafka_transport_for(settings, transport)
    topic_name = mqtt_topic_for(source, settings, topic) if transport_name == "mqtt" else kafka_topic_for(source, settings, topic)
    return {
        "source": source,
        "topic": topic_name,
        "transport": transport_name,
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "mqtt_host": settings.live_mqtt_host,
        "mqtt_port": settings.live_mqtt_port,
    }


@app.get("/api/live/stream")
def live_stream(
    source: str = "orion",
    topic: str | None = None,
    transport: str | None = None,
    sample_hz: float | None = Query(None, alias="sampleHz"),
):
    def events():
        for event in stream_kafka_samples(settings, source, topic, transport, sample_hz):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


@app.post("/api/live/export-lap")
def live_export_lap(request: LiveLapExportRequest):
    try:
        export_id, zip_path, files = export_live_lap(request, settings.for_source(request.car), channel_charts)
        return {
            "export_id": export_id,
            "zip_path": str(zip_path),
            "files": [path.name for path in files],
            "download_url": f"/api/export/{export_id}/download",
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/simulator/status")
def simulator_status():
    return replay_manager.status().model_dump()


@app.post("/api/simulator/start")
def simulator_start(request: ReplayStartRequest):
    try:
        return replay_manager.start(request, telemetry_for(request.car)).model_dump()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/simulator/stop")
def simulator_stop():
    return replay_manager.stop().model_dump()


@app.get("/api/export/{export_id}/download")
def download_export(export_id: str):
    zip_path = settings.resolved_export_dir / f"{export_id}.zip"
    if not zip_path.exists():
        raise HTTPException(status_code=404, detail="Export not found")
    return FileResponse(zip_path, filename=zip_path.name, media_type="application/zip")
