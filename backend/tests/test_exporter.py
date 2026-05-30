from pathlib import Path
import zipfile

from motec_exporter.config import Settings
from motec_exporter.exporter import Exporter
from motec_exporter.models import ExportRequest, ExportSegment
from motec_exporter.telemetry import TelemetryService
from motec_exporter.track_store import TrackStore


def test_sample_export_writes_zip(tmp_path):
    settings = Settings(
        TELEMETRY_SOURCE="sample",
        EXPORT_DIR=tmp_path / "exports",
        TRACK_DIR=tmp_path / "tracks",
        CACHE_DIR=tmp_path / "cache",
    )
    telemetry = TelemetryService(settings)
    exporter = Exporter(settings, telemetry, TrackStore(settings.resolved_track_dir))
    start = 1777135260000
    response = exporter.export(
        ExportRequest(
            segments=[ExportSegment(id="test", start_ms=start, end_ms=start + 30_000)],
            channel_keys=["motor_rpm", "gps_speed"],
            frequency_hz=20,
        )
    )

    assert Path(response.zip_path).exists()
    assert any(name.endswith(".ld") for name in response.files)
    assert any(name.endswith(".ldx") for name in response.files)


def test_sample_csv_export_writes_csv_without_lap_files(tmp_path):
    settings = Settings(
        TELEMETRY_SOURCE="sample",
        EXPORT_DIR=tmp_path / "exports",
        TRACK_DIR=tmp_path / "tracks",
        CACHE_DIR=tmp_path / "cache",
    )
    telemetry = TelemetryService(settings)
    exporter = Exporter(settings, telemetry, TrackStore(settings.resolved_track_dir))
    start = 1777135260000
    response = exporter.export(
        ExportRequest(
            export_type="csv",
            segments=[ExportSegment(id="test", start_ms=start, end_ms=start + 5_000)],
            channel_keys=["motor_rpm", "gps_speed"],
            frequency_hz=10,
            track_slug="ignored-for-csv",
        )
    )

    assert Path(response.zip_path).exists()
    assert any(name.endswith(".csv") for name in response.files)
    assert not any(name.endswith(".ldx") for name in response.files)
    with zipfile.ZipFile(response.zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        payload = archive.read(csv_names[0]).decode("utf-8")
    assert payload.startswith("timestamp_ms,elapsed_s,")
