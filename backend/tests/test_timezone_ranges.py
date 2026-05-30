from motec_exporter.config import Settings
from motec_exporter.telemetry import TelemetryService, _merge_ms_ranges


def test_local_day_bounds_use_configured_timezone():
    service = TelemetryService(Settings(TELEMETRY_SOURCE="sample", DISPLAY_TIMEZONE="America/Chicago"))

    start_ms, end_ms = service._local_day_bounds_ms("2026-04-25")

    assert end_ms - start_ms == 24 * 60 * 60 * 1000
    assert service._dates_between_local(start_ms, end_ms) == ["2026-04-25"]


def test_dates_between_local_includes_each_day_touched_by_long_partition():
    service = TelemetryService(Settings(TELEMETRY_SOURCE="sample", DISPLAY_TIMEZONE="America/Chicago"))
    start_ms, _ = service._local_day_bounds_ms("2026-04-24")
    _, end_ms = service._local_day_bounds_ms("2026-04-25")

    assert service._dates_between_local(start_ms, end_ms) == ["2026-04-24", "2026-04-25"]


def test_merge_ranges_deduplicates_overlapping_partition_windows():
    assert _merge_ms_ranges([(10, 20), (15, 30), (40, 50)]) == [(10, 30), (40, 50)]
